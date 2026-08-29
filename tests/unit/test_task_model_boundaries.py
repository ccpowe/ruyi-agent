from __future__ import annotations

import asyncio
from dataclasses import fields
from pathlib import Path

from ruyi_agent.runtime.delegation.async_runtime import (
    PublishedArtifact as RuntimePublishedArtifact,
)
from ruyi_agent.runtime.delegation.async_runtime import TaskRecord as RuntimeTaskRecord
from ruyi_agent.runtime.delegation.live_runs import LiveRunRegistry
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import PublishedArtifact, TaskRecord


def test_task_record_contains_only_persistence_safe_fields() -> None:
    field_names = {item.name for item in fields(TaskRecord)}

    assert "active_run" not in field_names
    assert "cancel_requested" not in field_names


def test_async_runtime_keeps_legacy_model_imports_compatible() -> None:
    assert RuntimeTaskRecord is TaskRecord
    assert RuntimePublishedArtifact is PublishedArtifact


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


def test_task_store_returns_core_task_record(tmp_path: Path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    try:
        assert store.list_tasks() == []
    finally:
        store.close()
