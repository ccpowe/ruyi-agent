"""Process-local ownership for active Task run handles."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass(slots=True)
class LiveRun:
    """Runtime-only state for one currently scheduled local Task run."""

    task: asyncio.Task[None]
    cancel_requested: bool = False


class LiveRunRegistry:
    """Keep non-serializable execution handles outside durable Task records."""

    def __init__(self) -> None:
        self._runs: dict[str, LiveRun] = {}

    def register(self, task_id: str, task: asyncio.Task[None]) -> None:
        current = self._runs.get(task_id)
        if current is not None and not current.task.done():
            raise RuntimeError(f"Task run is already active: {task_id}")
        run = LiveRun(task=task)
        self._runs[task_id] = run

        def discard_finished(_: asyncio.Task[None]) -> None:
            if self._runs.get(task_id) is run:
                self._runs.pop(task_id, None)

        task.add_done_callback(discard_finished)

    def get_task(self, task_id: str) -> asyncio.Task[None] | None:
        run = self._runs.get(task_id)
        return run.task if run is not None else None

    def is_active(self, task_id: str) -> bool:
        task = self.get_task(task_id)
        return task is not None and not task.done()

    def request_cancel(self, task_id: str) -> asyncio.Task[None] | None:
        run = self._runs.get(task_id)
        if run is None:
            return None
        run.cancel_requested = True
        run.task.cancel()
        return run.task

    def was_cancel_requested(self, task_id: str) -> bool:
        run = self._runs.get(task_id)
        return run is not None and run.cancel_requested

    def discard(self, task_id: str) -> None:
        self._runs.pop(task_id, None)

    def snapshot(self, task_id: str) -> tuple[LiveRun | None, bool]:
        """Capture one registry entry for strong exception rollback."""

        run = self._runs.get(task_id)
        return run, run.cancel_requested if run is not None else False

    def restore(
        self,
        task_id: str,
        snapshot: tuple[LiveRun | None, bool],
    ) -> None:
        """Restore a previously captured registry entry without replacing its Task."""

        run, cancel_requested = snapshot
        if run is None:
            self._runs.pop(task_id, None)
            return
        run.cancel_requested = cancel_requested
        self._runs[task_id] = run
