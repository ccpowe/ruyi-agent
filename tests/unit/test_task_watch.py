from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Any

import pytest

from ruyi_agent.channels.task_watch import TaskWatchHooks, TaskWatchManager


def async_test(function: Callable[[], Awaitable[None]]) -> Callable[[], None]:
    @wraps(function)
    def wrapper() -> None:
        asyncio.run(function())

    return wrapper


class SequenceGateway:
    def __init__(self, tasks: list[dict[str, Any] | Exception]) -> None:
        self.tasks = list(tasks)
        self.calls = 0

    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        del task_id
        self.calls += 1
        item = self.tasks.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def task(status: str, *, run_count: int = 1, review: bool = False) -> dict[str, Any]:
    return {
        "task_id": "task-1",
        "status": status,
        "run_count": run_count,
        "pending_review": {"review_id": "review-1"} if review else None,
    }


@async_test
async def test_watch_reports_pending_review_and_stops() -> None:
    gateway = SequenceGateway([task("running"), task("waiting_for_human", review=True)])
    events: list[str] = []
    manager = TaskWatchManager(gateway_client=gateway, poll_interval=0)

    manager.ensure(
        task_id="task-1",
        run_count=1,
        hooks=TaskWatchHooks(
            on_pending_review=lambda _: append(events, "review"),
            on_terminal=lambda _: append(events, "terminal"),
        ),
    )
    await manager.wait()

    assert events == ["review"]
    assert gateway.calls == 2
    assert not manager.is_active(task_id="task-1", run_count=1)


@async_test
async def test_watch_sends_terminal_once_during_grace_checks() -> None:
    gateway = SequenceGateway([task("completed"), task("completed"), task("completed")])
    events: list[str] = []
    manager = TaskWatchManager(
        gateway_client=gateway,
        poll_interval=0,
        terminal_review_grace_checks=2,
    )

    manager.ensure(
        task_id="task-1",
        run_count=1,
        hooks=TaskWatchHooks(
            on_pending_review=lambda _: append(events, "review"),
            on_terminal=lambda _: append(events, "terminal"),
        ),
    )
    await manager.wait()

    assert events == ["terminal"]
    assert gateway.calls == 3


@async_test
async def test_watch_can_find_review_during_terminal_grace_period() -> None:
    gateway = SequenceGateway([task("completed"), task("waiting_for_human", review=True)])
    events: list[str] = []
    manager = TaskWatchManager(
        gateway_client=gateway,
        poll_interval=0,
        terminal_review_grace_checks=2,
    )

    manager.ensure(
        task_id="task-1",
        run_count=1,
        hooks=TaskWatchHooks(
            on_pending_review=lambda _: append(events, "review"),
            on_terminal=lambda _: append(events, "terminal"),
        ),
    )
    await manager.wait()

    assert events == ["terminal", "review"]


@async_test
async def test_watch_stops_when_newer_run_supersedes_it() -> None:
    gateway = SequenceGateway([task("running", run_count=2)])
    events: list[str] = []
    manager = TaskWatchManager(gateway_client=gateway, poll_interval=0)

    manager.ensure(
        task_id="task-1",
        run_count=1,
        hooks=TaskWatchHooks(
            on_pending_review=lambda _: append(events, "review"),
            on_terminal=lambda _: append(events, "terminal"),
            on_superseded=lambda _: append(events, "superseded"),
        ),
    )
    await manager.wait()

    assert events == ["superseded"]


@async_test
async def test_ensure_deduplicates_active_watch() -> None:
    gateway = SequenceGateway([task("completed")])
    events: list[str] = []
    manager = TaskWatchManager(
        gateway_client=gateway,
        poll_interval=0,
        terminal_review_grace_checks=0,
    )
    hooks = TaskWatchHooks(
        on_pending_review=lambda _: append(events, "review"),
        on_terminal=lambda _: append(events, "terminal"),
    )

    manager.ensure(task_id="task-1", run_count=1, hooks=hooks)
    manager.ensure(task_id="task-1", run_count=1, hooks=hooks)
    await manager.wait()

    assert gateway.calls == 1
    assert events == ["terminal"]


@async_test
async def test_watch_calls_error_hook_and_propagates() -> None:
    gateway = SequenceGateway([RuntimeError("gateway unavailable")])
    events: list[str] = []
    manager = TaskWatchManager(gateway_client=gateway, poll_interval=0)

    manager.ensure(
        task_id="task-1",
        run_count=1,
        hooks=TaskWatchHooks(
            on_pending_review=lambda _: append(events, "review"),
            on_terminal=lambda _: append(events, "terminal"),
            on_error=lambda exc: append(events, str(exc)),
        ),
    )

    with pytest.raises(RuntimeError, match="gateway unavailable"):
        await manager.wait()
    assert events == ["gateway unavailable"]


async def append(items: list[str], value: str) -> None:
    items.append(value)
