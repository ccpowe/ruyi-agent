"""Process-local scheduling and shutdown ownership for local Task runs."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from ruyi_agent.runtime.delegation.contracts import TaskAlreadyRunningError

logger = logging.getLogger(__name__)

RunFactory = Callable[[], Awaitable[None]]
MaintenanceFactory = Callable[[], Awaitable[None]]


class RuntimeClosingError(RuntimeError):
    """Raised when a mutation tries to schedule work during runtime shutdown."""


class RunSupervisorHost(Protocol):
    """Narrow runtime capabilities needed by the run supervisor."""

    _mailbox: Any
    _task_manager: Any

    async def _ensure_task_awake(self, task_id: str) -> Any: ...


class MutationPermit:
    """Explicit, stack-local admission for one short mutation critical section."""

    __slots__ = ("_active", "_supervisor")

    def __init__(self, supervisor: RunSupervisor) -> None:
        self._supervisor = supervisor
        self._active = True


class OperationPermit:
    """Ownership for one cancellable caller Task performing external I/O."""

    __slots__ = ("_active", "_supervisor", "task")

    def __init__(
        self,
        supervisor: RunSupervisor,
        task: asyncio.Task[Any],
    ) -> None:
        self._supervisor = supervisor
        self.task = task
        self._active = True


class RunSupervisor:
    """Own local runs and their shutdown boundary.

    A newly created ``asyncio.Task`` waits behind an event until ``mark_running``
    commits. Mutation admission is represented by an explicit permit and is
    never inherited by a detached Task or done callback.
    """

    def __init__(
        self,
        control: RunSupervisorHost,
        *,
        shutdown_grace_period: float = 5.0,
    ) -> None:
        if shutdown_grace_period < 0:
            raise ValueError("shutdown_grace_period must not be negative")
        self._control = control
        self._shutdown_grace_period = shutdown_grace_period
        self._lifecycle_condition = asyncio.Condition()
        self._schedule_lock = asyncio.Lock()
        self._active_mutations = 0
        self._operations: set[asyncio.Task[Any]] = set()
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

    def get_run(self, task_id: str) -> asyncio.Task[None] | None:
        """Return the supervisor-owned handle until the asyncio Task is done."""

        return self._runs.get(task_id)

    async def acquire_mutation(self) -> MutationPermit:
        """Admit one short submission/scheduling critical section."""

        async with self._lifecycle_condition:
            if not self.is_accepting:
                raise RuntimeClosingError("Agent runtime is closing")
            self._active_mutations += 1
            return MutationPermit(self)

    async def release_mutation(self, permit: MutationPermit) -> None:
        """Release a mutation permit exactly once."""

        async with self._lifecycle_condition:
            if permit._supervisor is not self:
                raise RuntimeError("Mutation permit belongs to another supervisor")
            if not permit._active:
                return
            permit._active = False
            self._active_mutations -= 1
            if self._active_mutations == 0:
                self._lifecycle_condition.notify_all()

    async def promote_to_operation(
        self,
        permit: MutationPermit,
    ) -> OperationPermit:
        """Atomically replace a short mutation with tracked external I/O."""

        current = asyncio.current_task()
        if current is None:
            raise RuntimeError("Tracked operations require an asyncio Task")
        async with self._lifecycle_condition:
            self._validate_mutation_permit(permit)
            if not self.is_accepting:
                raise RuntimeClosingError("Agent runtime is closing")
            permit._active = False
            self._active_mutations -= 1
            self._operations.add(current)
            if self._active_mutations == 0:
                self._lifecycle_condition.notify_all()
            return OperationPermit(self, current)

    async def wait_for_lock(
        self,
        permit: MutationPermit,
        lock: asyncio.Lock,
    ) -> MutationPermit:
        """Wait for a coordination lock as cancellable work, then re-admit."""

        operation = await self.promote_to_operation(permit)
        acquired = False
        try:
            await lock.acquire()
            acquired = True
            async with self._lifecycle_condition:
                if operation._supervisor is not self or not operation._active:
                    raise RuntimeError("Operation permit is no longer active")
                operation._active = False
                self._operations.discard(operation.task)
                if not self.is_accepting:
                    raise RuntimeClosingError("Agent runtime is closing")
                self._active_mutations += 1
                return MutationPermit(self)
        except BaseException:
            if acquired:
                lock.release()
            await self.release_operation(operation)
            raise

    async def release_operation(self, permit: OperationPermit) -> None:
        """Stop tracking one external operation after success or cancellation."""

        async with self._lifecycle_condition:
            if permit._supervisor is not self or not permit._active:
                return
            permit._active = False
            self._operations.discard(permit.task)

    async def schedule(
        self,
        task_id: str,
        run_factory: RunFactory,
        *,
        permit: MutationPermit | None = None,
        wake_mailbox: bool = True,
    ) -> asyncio.Task[None]:
        """Persist and release one local run without an execution-before-save gap."""

        owned_permit = permit is None
        if permit is None:
            permit = await self.acquire_mutation()
        try:
            async with self._schedule_lock:
                async with self._lifecycle_condition:
                    self._validate_mutation_permit(permit)
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
                    self._control._task_manager.mark_running(task_id, run_task)
                except BaseException:
                    run_task.cancel()
                    await asyncio.gather(run_task, return_exceptions=True)
                    self._control._task_manager.discard_live_run(task_id)
                    raise

                self._runs[task_id] = run_task
                run_task.add_done_callback(
                    lambda finished: self._run_finished(
                        task_id,
                        finished,
                        wake_mailbox=wake_mailbox,
                    )
                )
                release.set()
                return run_task
        finally:
            if owned_permit:
                await self.release_mutation(permit)

    def start_recovery(self, factory: MaintenanceFactory) -> None:
        """Start the single runtime recovery loop as tracked maintenance."""

        if self._recovery_task is not None and not self._recovery_task.done():
            return
        if not self.is_accepting:
            raise RuntimeClosingError("Agent runtime is closing")
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
        close_task = self._close_task
        cancelled = False
        while True:
            try:
                await asyncio.shield(close_task)
                break
            except asyncio.CancelledError:
                if close_task.cancelled():
                    raise
                cancelled = True
                continue
        if cancelled:
            raise asyncio.CancelledError

    async def _close_impl(self) -> None:
        async with self._lifecycle_condition:
            if self._closed:
                return
            self._closing = True
            operations = tuple(
                task for task in self._operations if task is not asyncio.current_task()
            )

        # Cancel external I/O before waiting for short mutations. A remote
        # operation can own a delegation-budget lock that an admitted mutation
        # is waiting to enter; reversing this order would deadlock shutdown.
        await self._cancel_and_gather(operations)
        async with self._lifecycle_condition:
            self._operations.difference_update(operations)
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
        self._closed = True

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
        *,
        wake_mailbox: bool,
    ) -> None:
        if self._runs.get(task_id) is task:
            self._runs.pop(task_id, None)
        error = self._consume_task_result(task, label=f"Task run {task_id}")
        if error is not None:
            self._finalize_interrupted(
                task_id,
                error="Task interrupted: unhandled run failure",
            )
        if wake_mailbox and self._control._mailbox is not None and self.is_accepting:
            self.start_maintenance(
                lambda: self._control._ensure_task_awake(task_id),
                name=f"mailbox-wakeup:{task_id}",
            )

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

    def _validate_mutation_permit(self, permit: MutationPermit) -> None:
        if permit._supervisor is not self or not permit._active:
            raise RuntimeError("Mutation permit is no longer active")

    def _finalize_interrupted(
        self,
        task_id: str,
        *,
        error: str = "Task interrupted: runtime shutdown",
    ) -> None:
        try:
            record = self._control._task_manager.get_task(task_id)
            if record.state == "running":
                self._control._task_manager.mark_interrupted(task_id, error)
        except Exception:
            logger.exception(
                "Failed to persist interrupted state during runtime shutdown: %s",
                task_id,
            )
        finally:
            self._control._task_manager.discard_live_run(task_id)

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


__all__ = [
    "MutationPermit",
    "OperationPermit",
    "RunSupervisor",
    "RuntimeClosingError",
]
