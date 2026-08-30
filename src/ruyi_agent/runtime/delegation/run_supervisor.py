"""Process-local scheduling and shutdown ownership for local Task runs."""

from __future__ import annotations

import asyncio
import logging
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeVar

from ruyi_agent.runtime.delegation.contracts import TaskAlreadyRunningError
from ruyi_agent.runtime.delegation.task_manager import TaskManager

logger = logging.getLogger(__name__)

RunFactory = Callable[[], Awaitable[None]]
MaintenanceFactory = Callable[[], Awaitable[None]]
_T = TypeVar("_T")


class RunCompletionPort(Protocol):
    def on_run_finished(self, task_id: str, task: asyncio.Task[None]) -> None: ...


class RuntimeClosingError(RuntimeError):
    """Raised when a mutation tries to schedule work during runtime shutdown."""


class InvalidRuntimePermitError(RuntimeError):
    """Raised when a lifecycle permit is forged, stolen, or reused."""


class _Permit:
    """Unforgeable-in-practice token issued only through a supervisor registry."""

    __slots__ = ("_active", "_issued", "_nonce", "_owner", "_supervisor")

    def __init__(self) -> None:
        self._supervisor: RunSupervisor | None = None
        self._nonce: str | None = None
        self._owner: asyncio.Task[Any] | None = None
        self._issued = False
        self._active = False


class _MutationPermit(_Permit):
    """Stack-local ownership of a short mutation critical section."""


class _OperationPermit(_Permit):
    """Ownership of one cancellable external-I/O operation."""


@dataclass(frozen=True, slots=True)
class _PermitEntry:
    token: _Permit
    owner: asyncio.Task[Any]
    kind: Literal["mutation", "operation"]


class RunSupervisor:
    def __init__(self, task_manager: TaskManager, *, shutdown_grace_period: float = 5.0) -> None:  # fmt: skip
        if shutdown_grace_period < 0:
            raise ValueError("shutdown_grace_period must not be negative")
        self._task_manager = task_manager
        self._shutdown_grace_period = shutdown_grace_period
        self._lifecycle_condition = asyncio.Condition()
        self._schedule_lock = asyncio.Lock()
        self._active_mutations = 0
        self._permits: dict[str, _PermitEntry] = {}
        self._runs: dict[str, asyncio.Task[None]] = {}
        self._maintenance: set[asyncio.Task[None]] = set()
        self._recovery_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._closing = False
        self._closed = False

    @property
    def is_accepting(self) -> bool:
        """Whether a new mutation may enter the runtime."""

        return not self._closing and not self._closed

    @property
    def active_run_count(self) -> int:
        """Return the number of run handles that have not actually finished."""

        return sum(not task.done() for task in self._runs.values())

    @property
    def _operations(self) -> set[asyncio.Task[Any]]:
        """Expose unique operation owners for shutdown diagnostics and tests."""

        return {
            entry.owner for entry in self._permits.values() if entry.kind == "operation"
        }

    def get_run(self, task_id: str) -> asyncio.Task[None] | None:
        """Return the supervisor-owned handle until the asyncio Task is done."""

        return self._runs.get(task_id)

    async def acquire_mutation(self) -> _MutationPermit:
        """Admit one short submission/scheduling critical section."""

        owner = self._require_current_task()
        async with self._lifecycle_condition:
            self._require_accepting()
            permit = self._mint_permit(_MutationPermit, owner, "mutation")
            self._active_mutations += 1
            return permit

    async def release_mutation(self, permit: _MutationPermit) -> None:
        """Strictly release a mutation permit exactly once."""

        await self._finish_permit_cleanup(permit, "mutation", allow_inactive=False)

    async def cleanup_mutation(self, permit: _MutationPermit) -> None:
        """Cancellation-safe, idempotent cleanup for a mutation ``finally``."""

        await self._finish_permit_cleanup(permit, "mutation", allow_inactive=True)

    async def promote_to_operation(
        self,
        permit: _MutationPermit,
    ) -> _OperationPermit:
        """Atomically replace a short mutation with tracked external I/O."""

        owner = self._require_current_task()
        async with self._lifecycle_condition:
            self._consume_permit_locked(
                permit,
                "mutation",
                owner,
                allow_inactive=False,
            )
            self._require_accepting()
            return self._mint_permit(_OperationPermit, owner, "operation")

    async def wait_for_lock(
        self,
        permit: _MutationPermit,
        lock: asyncio.Lock,
    ) -> _MutationPermit:
        """Wait for a coordination lock as cancellable work, then re-admit."""

        owner = self._require_current_task()
        operation = await self.promote_to_operation(permit)
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            async with self._lifecycle_condition:
                self._require_accepting()
                self._consume_permit_locked(
                    operation,
                    "operation",
                    owner,
                    allow_inactive=False,
                )
                mutation = self._mint_permit(_MutationPermit, owner, "mutation")
                self._active_mutations += 1
                return mutation
        except BaseException:
            if acquired:
                lock.release()
            await self.cleanup_operation(operation)
            raise

    async def release_operation(self, permit: _OperationPermit) -> None:
        """Strictly stop tracking one external operation exactly once."""

        await self._finish_permit_cleanup(permit, "operation", allow_inactive=False)

    async def cleanup_operation(self, permit: _OperationPermit) -> None:
        """Cancellation-safe, idempotent cleanup for an operation ``finally``."""

        await self._finish_permit_cleanup(permit, "operation", allow_inactive=True)

    def mutate_now(self, factory: Callable[[], _T]) -> _T:
        """Run one non-awaiting mutation at an event-loop-atomic admission point."""

        self._require_current_task()
        self._require_accepting()
        return factory()

    async def schedule(
        self,
        task_id: str,
        run_factory: RunFactory,
        *,
        completion_port: RunCompletionPort | None = None,
        permit: _MutationPermit | None = None,
    ) -> asyncio.Task[None]:
        """Persist and release one local run without an execution-before-save gap."""

        owned_permit = permit is None
        if permit is None:
            permit = await self.acquire_mutation()
        try:
            async with self._schedule_lock:
                async with self._lifecycle_condition:
                    owner = self._require_current_task()
                    self._require_accepting()
                    self._validate_permit_locked(permit, "mutation", owner)
                    current = self._runs.get(task_id)
                    if current is not None and not current.done():
                        raise TaskAlreadyRunningError(
                            f"Worker task is already running: {task_id}"
                        )

                    release = asyncio.Event()
                    run_task = asyncio.create_task(
                        self._run_after_release(release, run_factory),
                        name=f"ruyi-task-run:{task_id}",
                    )
                    try:
                        self._task_manager.mark_running(task_id, run_task)
                    except BaseException:
                        run_task.cancel()

                        async def rollback_failed_run() -> None:
                            await asyncio.gather(run_task, return_exceptions=True)
                            self._task_manager.discard_live_run(task_id)

                        await self._await_cleanup(
                            rollback_failed_run(),
                            name=f"rollback-run:{task_id}",
                        )
                        raise

                    self._runs[task_id] = run_task
                    run_task.add_done_callback(
                        lambda finished: self._run_finished(
                            task_id,
                            finished,
                            completion_port,
                        )
                    )
                    release.set()
                    return run_task
        finally:
            if owned_permit:
                await self.cleanup_mutation(permit)

    def start_recovery(self, factory: MaintenanceFactory) -> None:
        """Start the single runtime recovery loop as tracked maintenance."""

        if self._recovery_task is not None and not self._recovery_task.done():
            return
        self._require_accepting()
        task = self._create_maintenance_task(factory, name="mailbox-recovery")
        self._recovery_task = task

    def start_maintenance(
        self,
        factory: MaintenanceFactory,
        *,
        name: str,
    ) -> asyncio.Task[None] | None:
        """Start tracked non-run work, or reject it once shutdown begins."""

        if not self.is_accepting:
            return None
        return self._create_maintenance_task(factory, name=name)

    async def close(self) -> None:
        """Finish shared cleanup before propagating caller cancellation."""

        if self._close_task is None:
            self._close_task = asyncio.create_task(
                self._close_impl(),
                name="ruyi-runtime-shutdown",
            )
        await self._await_existing_task(self._close_task)

    async def _close_impl(self) -> None:
        async with self._lifecycle_condition:
            if self._closed:
                return
            self._closing = True
            current = asyncio.current_task()
            operations = tuple(task for task in self._operations if task is not current)

        await self._cancel_and_gather(operations)
        async with self._lifecycle_condition:
            self._discard_operation_permits_locked(operations)
            while self._active_mutations:
                await self._lifecycle_condition.wait()

        async with self._schedule_lock:
            maintenance = tuple(self._maintenance)
            runs = dict(self._runs)

        await self._cancel_and_gather(
            tuple(task for task in maintenance if task is not asyncio.current_task())
        )
        self._recovery_task = None

        active = tuple(task for task in runs.values() if not task.done())
        if active and self._shutdown_grace_period > 0:
            _, pending = await asyncio.wait(
                active,
                timeout=self._shutdown_grace_period,
            )
        else:
            pending = set(active)
        for task in pending:
            task.cancel()
        if runs:
            await asyncio.gather(*runs.values(), return_exceptions=True)

        for task_id in runs:
            self._finalize_interrupted(task_id)
        self._runs.clear()
        self._maintenance.clear()
        self._permits.clear()
        self._closed = True

    async def _finish_permit_cleanup(
        self,
        permit: _Permit,
        kind: Literal["mutation", "operation"],
        *,
        allow_inactive: bool,
    ) -> None:
        owner = self._require_current_task()

        async def cleanup() -> None:
            async with self._lifecycle_condition:
                self._consume_permit_locked(
                    permit,
                    kind,
                    owner,
                    allow_inactive=allow_inactive,
                )

        try:
            await self._lifecycle_condition.acquire()
        except asyncio.CancelledError:
            await self._await_cleanup(cleanup(), name=f"release-{kind}")
            raise
        try:
            self._consume_permit_locked(
                permit,
                kind,
                owner,
                allow_inactive=allow_inactive,
            )
        finally:
            self._lifecycle_condition.release()

    def _mint_permit(
        self,
        permit_type: type[_MutationPermit] | type[_OperationPermit],
        owner: asyncio.Task[Any],
        kind: Literal["mutation", "operation"],
    ) -> _MutationPermit | _OperationPermit:
        nonce = secrets.token_urlsafe(32)
        while nonce in self._permits:
            nonce = secrets.token_urlsafe(32)
        permit = permit_type()
        permit._supervisor = self
        permit._nonce = nonce
        permit._owner = owner
        permit._issued = True
        permit._active = True
        self._permits[nonce] = _PermitEntry(permit, owner, kind)
        return permit

    def _validate_permit_locked(
        self,
        permit: _Permit,
        kind: Literal["mutation", "operation"],
        owner: asyncio.Task[Any],
    ) -> _PermitEntry:
        if not isinstance(permit, _Permit) or not permit._issued:
            raise InvalidRuntimePermitError("Runtime permit was not issued")
        if permit._supervisor is not self:
            raise InvalidRuntimePermitError(
                "Runtime permit belongs to another supervisor"
            )
        if permit._owner is not owner:
            raise InvalidRuntimePermitError(
                "Runtime permit belongs to another asyncio Task"
            )
        if not permit._active or permit._nonce is None:
            raise InvalidRuntimePermitError("Runtime permit was already consumed")
        entry = self._permits.get(permit._nonce)
        if entry is None or entry.token is not permit:
            raise InvalidRuntimePermitError("Runtime permit is forged or inactive")
        if entry.owner is not owner or entry.kind != kind:
            raise InvalidRuntimePermitError(
                "Runtime permit has the wrong owner or kind"
            )
        return entry

    def _consume_permit_locked(
        self,
        permit: _Permit,
        kind: Literal["mutation", "operation"],
        owner: asyncio.Task[Any],
        *,
        allow_inactive: bool,
    ) -> bool:
        if (
            allow_inactive
            and isinstance(permit, _Permit)
            and permit._issued
            and permit._supervisor is self
            and permit._owner is owner
            and not permit._active
        ):
            return False
        self._validate_permit_locked(permit, kind, owner)
        if kind == "mutation" and self._active_mutations <= 0:
            raise RuntimeError("Mutation accounting invariant violated")
        assert permit._nonce is not None
        self._permits.pop(permit._nonce)
        permit._active = False
        if kind == "mutation":
            self._active_mutations -= 1
            if self._active_mutations == 0:
                self._lifecycle_condition.notify_all()
        return True

    def _discard_operation_permits_locked(
        self,
        owners: tuple[asyncio.Task[Any], ...],
    ) -> None:
        owner_set = set(owners)
        for nonce, entry in tuple(self._permits.items()):
            if entry.kind == "operation" and entry.owner in owner_set:
                self._permits.pop(nonce)
                entry.token._active = False

    @staticmethod
    def _require_current_task() -> asyncio.Task[Any]:
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError("Runtime admission requires an asyncio Task")
        return owner

    def _require_accepting(self) -> None:
        if not self.is_accepting:
            raise RuntimeClosingError("Agent runtime is closing")

    async def _await_cleanup(
        self,
        awaitable: Awaitable[_T],
        *,
        name: str,
    ) -> _T:
        future = asyncio.ensure_future(awaitable)
        if isinstance(future, asyncio.Task):
            future.set_name(f"ruyi-cleanup:{name}")
        return await self._await_existing_task(future)

    @staticmethod
    async def _await_existing_task(future: asyncio.Future[_T]) -> _T:
        cancelled = False
        while True:
            try:
                result = await asyncio.shield(future)
                break
            except asyncio.CancelledError:
                if future.cancelled():
                    raise
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError
        return result

    async def _run_after_release(
        self,
        release: asyncio.Event,
        run_factory: RunFactory,
    ) -> None:
        await release.wait()
        await run_factory()

    def _run_finished(
        self,
        task_id: str,
        task: asyncio.Task[None],
        completion_port: RunCompletionPort | None,
    ) -> None:
        if self._runs.get(task_id) is task:
            self._runs.pop(task_id, None)
        error = self._consume_task_result(task, label=f"Task run {task_id}")
        if error is not None:
            self._finalize_interrupted(
                task_id,
                error="Task interrupted: unhandled run failure",
            )
        if completion_port is not None:
            try:
                completion_port.on_run_finished(task_id, task)
            except Exception:
                logger.exception("Run completion callback failed: %s", task_id)

    def _create_maintenance_task(
        self,
        factory: MaintenanceFactory,
        *,
        name: str,
    ) -> asyncio.Task[None]:
        task = asyncio.create_task(factory(), name=f"ruyi-maintenance:{name}")
        self._maintenance.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            self._maintenance.discard(done)
            self._consume_task_result(done, label=f"Runtime maintenance {name}")

        task.add_done_callback(finished)
        return task

    async def _cancel_and_gather(
        self,
        tasks: tuple[asyncio.Task[Any], ...],
    ) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _finalize_interrupted(
        self,
        task_id: str,
        *,
        error: str = "Task interrupted: runtime shutdown",
    ) -> None:
        try:
            record = self._task_manager.get_task(task_id)
            if record.state == "running":
                self._task_manager.mark_interrupted(task_id, error)
        except Exception:
            logger.exception(
                "Failed to persist interrupted state during runtime shutdown: %s",
                task_id,
            )
        finally:
            self._task_manager.discard_live_run(task_id)

    @staticmethod
    def _consume_task_result(
        task: asyncio.Task[None],
        *,
        label: str,
    ) -> BaseException | None:
        if task.cancelled():
            return None
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return None
        if error is not None:
            logger.error(
                "%s failed",
                label,
                exc_info=(type(error), error, error.__traceback__),
            )
        return error


__all__ = ["RunCompletionPort", "RunSupervisor", "RuntimeClosingError"]
