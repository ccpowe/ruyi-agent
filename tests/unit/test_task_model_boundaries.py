from __future__ import annotations

import asyncio
from dataclasses import fields
from pathlib import Path

import pytest

from ruyi_agent.runtime.delegation.async_runtime import (
    PublishedArtifact as RuntimePublishedArtifact,
)
from ruyi_agent.runtime.delegation.async_runtime import TaskRecord as RuntimeTaskRecord
from ruyi_agent.runtime.delegation.live_runs import LiveRunRegistry
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import (
    ACTIVE_TASK_STATES,
    SETTLED_TASK_STATES,
    TASK_STATES,
    PublishedArtifact,
    TaskRecord,
    parse_task_state,
)


def test_task_record_contains_only_persistence_safe_fields() -> None:
    field_names = {item.name for item in fields(TaskRecord)}

    assert "active_run" not in field_names
    assert "cancel_requested" not in field_names


def test_async_runtime_keeps_legacy_model_imports_compatible() -> None:
    assert RuntimeTaskRecord is TaskRecord
    assert RuntimePublishedArtifact is PublishedArtifact


def test_task_state_parser_and_sets_share_one_canonical_vocabulary() -> None:
    assert ACTIVE_TASK_STATES | SETTLED_TASK_STATES == TASK_STATES
    for state in TASK_STATES:
        assert parse_task_state(state) is state

    with pytest.raises(ValueError, match="status must be one of"):
        parse_task_state("unknown", path="status")


def test_storage_and_event_modules_do_not_import_async_runtime() -> None:
    source_root = Path(__file__).parents[2] / "src" / "ruyi_agent"
    for relative_path in ("storage/task_store.py", "runtime/task_events.py"):
        source = (source_root / relative_path).read_text(encoding="utf-8")
        assert "runtime.delegation.async_runtime" not in source


def test_live_run_registry_releases_completed_handle() -> None:
    async def scenario() -> None:
        registry = LiveRunRegistry()
        release = asyncio.Event()
        task = asyncio.create_task(release.wait())
        registry.register("task-1", task)

        assert registry.get_task("task-1") is task
        assert registry.is_active("task-1") is True

        release.set()
        await task
        await asyncio.sleep(0)

        assert registry.get_task("task-1") is None

    asyncio.run(scenario())


def test_live_run_registry_rejects_replacing_active_run() -> None:
    async def scenario() -> None:
        registry = LiveRunRegistry()
        first = asyncio.create_task(asyncio.Event().wait())
        replacement = asyncio.create_task(asyncio.Event().wait())
        try:
            registry.register("task-1", first)

            with pytest.raises(RuntimeError, match="already active"):
                registry.register("task-1", replacement)

            assert registry.get_task("task-1") is first
        finally:
            first.cancel()
            replacement.cancel()
            await asyncio.gather(first, replacement, return_exceptions=True)

    asyncio.run(scenario())


def test_old_done_callback_cannot_discard_replacement_run() -> None:
    async def scenario() -> None:
        registry = LiveRunRegistry()
        replacement_release = asyncio.Event()
        replacement = asyncio.create_task(replacement_release.wait())
        old = asyncio.create_task(asyncio.sleep(0))

        # asyncio runs done callbacks in registration order. Install the
        # replacement immediately after old settles, before the registry's old
        # cleanup callback gets its turn.
        old.add_done_callback(lambda _: registry.register("task-1", replacement))
        registry.register("task-1", old)

        await old
        await asyncio.sleep(0)

        assert registry.get_task("task-1") is replacement
        assert registry.is_active("task-1") is True

        replacement_release.set()
        await replacement
        await asyncio.sleep(0)
        assert registry.get_task("task-1") is None

    asyncio.run(scenario())


def test_cancel_requested_does_not_leak_into_replacement_run() -> None:
    async def scenario() -> None:
        registry = LiveRunRegistry()
        first = asyncio.create_task(asyncio.Event().wait())
        replacement = asyncio.create_task(asyncio.Event().wait())
        registry.register("task-1", first)

        assert registry.request_cancel("task-1") is first
        assert registry.was_cancel_requested("task-1") is True

        registry.discard("task-1")
        registry.register("task-1", replacement)
        assert registry.was_cancel_requested("task-1") is False

        await asyncio.gather(first, return_exceptions=True)
        assert registry.get_task("task-1") is replacement

        replacement.cancel()
        await asyncio.gather(replacement, return_exceptions=True)

    asyncio.run(scenario())


def test_request_cancel_targets_only_requested_run() -> None:
    async def scenario() -> None:
        registry = LiveRunRegistry()
        first = asyncio.create_task(asyncio.Event().wait())
        second = asyncio.create_task(asyncio.Event().wait())
        registry.register("task-1", first)
        registry.register("task-2", second)

        cancelled = registry.request_cancel("task-1")

        assert cancelled is first
        assert first.cancelling() == 1
        assert second.cancelling() == 0
        assert registry.was_cancel_requested("task-1") is True
        assert registry.was_cancel_requested("task-2") is False

        await asyncio.gather(first, return_exceptions=True)
        second.cancel()
        await asyncio.gather(second, return_exceptions=True)

    asyncio.run(scenario())


def test_task_store_returns_core_task_record(tmp_path: Path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    try:
        assert store.list_tasks() == []
    finally:
        store.close()
