"""Process-local scheduling and shutdown ownership for local Task runs."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any, Protocol, TypeVar

from ruyi_agent.runtime.delegation.contracts import TaskAlreadyRunningError

logger = logging.getLogger(__name__)

RunFactory = Callable[[], Awaitable[None]]
MaintenanceFactory = Callable[[], Awaitable[None]]
MutationResult = TypeVar("MutationResult")


class RuntimeClosingError(RuntimeError):
    """Raised when a mutation tries to schedule work during runtime shutdown."""


class RunSupervisorHost(Protocol):
    """Narrow runtime capabilities needed by the run supervisor."""

    _mailbox: Any
    _task_manager: Any

    async def _ensure_task_awake(self, task_id: str) -> Any: ...


class RunSupervisor:
    """Own every local run from gated creation through shutdown finalization.

    A newly created ``asyncio.Task`` waits behind an event until ``mark_running``
    commits. This makes the durable transition the release point for model and
    tool effects while still allowing the live handle to be stored atomically
    with that transition.
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
        self._admission_condition = asyncio.Condition()
        self._admitted: ContextVar[bool] = ContextVar(
            f"run-supervisor-admitted:{id(self)}",
            default=False,
        )
        self._active_mutations = 0
        self._mutation_lock = asyncio.Lock()
        self._close_lock = asyncio.Lock()
        self._runs: dict[str, asyncio.Task[None]] = {}
        self._maintenance: set[asyncio.Task[None]] = set()
        self._recovery_task: asyncio.Task[None] | None = None
        self._closing = False
        self._closed = False

    @property
    def is_accepting(self) -> bool:
        """Whether new local runs and maintenance work may be scheduled."""

        return not self._closing and not self._closed

    @property
    def active_run_count(self) -> int:
        """Return the number of run handles still owned by this supervisor."""

        return sum(not task.done() for task in self._runs.values())

    def ensure_accepting(self) -> None:
        """Reject a new runtime mutation after the shutdown boundary."""

        if not self.is_accepting:
            raise RuntimeClosingError("Agent runtime is closing")

    async def mutate(
        self,
        factory: Callable[[], Awaitable[MutationResult]],
    ) -> MutationResult:
        """Admit one mutation before shutdown and keep its dependencies open."""

        if self._admitted.get():
            return await factory()
        async with self._admission_condition:
            self.ensure_accepting()
            self._active_mutations += 1
        token = self._admitted.set(True)
        try:
            return await factory()
        finally:
            self._admitted.reset(token)
            async with self._admission_condition:
                self._active_mutations -= 1
                if self._active_mutations == 0:
                    self._admission_condition.notify_all()

    async def schedule(
        self,
        task_id: str,
        run_factory: RunFactory,
        *,
        wake_mailbox: bool = True,
    ) -> asyncio.Task[None]:
        """Persist and release one local run without an execution-before-save gap."""

        async with self._mutation_lock:
            if not self.is_accepting and not self._admitted.get():
                raise RuntimeClosingError("Agent runtime is closing")
            if self._control._task_manager.has_active_run(task_id):
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
                # The gated coroutine has not invoked run_factory, so cancelling
                # and awaiting it cannot execute model, tool, or payload code.
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
        """Stop recovery, drain runs, cancel stragglers, and consume failures."""

        async with self._close_lock:
            if self._closed:
                return
            async with self._admission_condition:
                self._closing = True
                while self._active_mutations:
                    await self._admission_condition.wait()
            async with self._mutation_lock:
                recovery = self._recovery_task
                maintenance = tuple(self._maintenance)
                runs = dict(self._runs)

            await self._cancel_and_gather(
                tuple(
                    task
                    for task in maintenance
                    if task is not asyncio.current_task()
                )
            )
            if recovery is not None:
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

            # A run normally persists ``interrupted`` from its CancelledError
            # handler. Retry while stores are still open if that transition
            # failed, then release the process-local handle regardless.
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

    async def _cancel_and_gather(self, tasks: tuple[asyncio.Task[None], ...]) -> None:
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
            record = self._control._task_manager.get_task(task_id)
            if record.state == "running":
                self._control._task_manager.mark_interrupted(
                    task_id,
                    error,
                )
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


__all__ = ["RunSupervisor", "RuntimeClosingError"]
