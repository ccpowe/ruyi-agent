from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from ruyi_agent.channels.gateway_client import (
    GatewayTaskClient,
    gateway_task_from_payload,
)
from ruyi_agent.channels.gateway_dto import GatewayTask


TERMINAL_TASK_STATES = {"completed", "failed", "cancelled", "interrupted"}

TaskHook = Callable[[GatewayTask], Awaitable[None]]
ErrorHook = Callable[[Exception], Awaitable[None]]


@dataclass(slots=True)
class TaskWatchHooks:
    on_pending_review: TaskHook
    on_terminal: TaskHook
    on_superseded: TaskHook | None = None
    on_error: ErrorHook | None = None


class TaskWatchManager:
    """Observe Gateway Task runs while keeping channel delivery platform-specific."""

    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        poll_interval: float = 2.0,
        terminal_review_grace_checks: int = 3,
    ) -> None:
        self._gateway_client = gateway_client
        self._poll_interval = poll_interval
        self._terminal_review_grace_checks = max(0, terminal_review_grace_checks)
        self._watches: dict[tuple[str, int], asyncio.Task[None]] = {}

    def ensure(
        self,
        *,
        task_id: str,
        run_count: int,
        hooks: TaskWatchHooks,
    ) -> None:
        key = (task_id, run_count)
        existing = self._watches.get(key)
        if existing is not None and not existing.done():
            return
        self._watches[key] = asyncio.create_task(
            self._watch(
                task_id=task_id,
                expected_run_count=run_count,
                hooks=hooks,
            )
        )

    def is_active(self, *, task_id: str, run_count: int) -> bool:
        watch = self._watches.get((task_id, run_count))
        return watch is not None and not watch.done()

    async def wait(self) -> None:
        active = [watch for watch in self._watches.values() if not watch.done()]
        if active:
            await asyncio.gather(*active)

    async def _watch(
        self,
        *,
        task_id: str,
        expected_run_count: int,
        hooks: TaskWatchHooks,
    ) -> None:
        key = (task_id, expected_run_count)
        terminal_sent = False
        grace_checks_remaining = self._terminal_review_grace_checks
        try:
            while True:
                task = gateway_task_from_payload(
                    await self._gateway_client.get_task(task_id=task_id)
                )
                if task.run_count > expected_run_count:
                    if hooks.on_superseded is not None:
                        await hooks.on_superseded(task)
                    return
                if task.has_pending_review:
                    await hooks.on_pending_review(task)
                    return
                if task.status in TERMINAL_TASK_STATES:
                    if not terminal_sent:
                        await hooks.on_terminal(task)
                        terminal_sent = True
                    if grace_checks_remaining <= 0:
                        return
                    grace_checks_remaining -= 1
                await asyncio.sleep(self._poll_interval)
        except Exception as exc:
            if hooks.on_error is not None:
                await hooks.on_error(exc)
            raise
        finally:
            self._watches.pop(key, None)
