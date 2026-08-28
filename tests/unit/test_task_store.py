from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from ruyi_agent.runtime.delegation.async_runtime import PublishedArtifact, TaskRecord
from ruyi_agent.storage.task_store import TaskStore


def test_task_store_persists_skill_view_fields(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    record = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="completed",
        thread_id="thread-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 5, 23, tzinfo=UTC),
        updated_at=datetime(2026, 5, 23, tzinfo=UTC),
        effective_skill_names=("frontend", "repo-workflow"),
        skill_view_path="/.ruyi_agent/runtime/skill-views/abc",
        skill_view_hash="abc",
    )

    store.save_task(record)
    loaded = store.get_task("task-1")

    assert loaded is not None
    assert loaded.effective_skill_names == ("frontend", "repo-workflow")
    assert loaded.skill_view_path == "/.ruyi_agent/runtime/skill-views/abc"
    assert loaded.skill_view_hash == "abc"
    store.close()


def test_task_store_persists_published_artifacts(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    record = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="completed",
        thread_id="thread-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 5, 23, tzinfo=UTC),
        updated_at=datetime(2026, 5, 23, tzinfo=UTC),
        artifacts=[
            PublishedArtifact(
                artifact_id="art_1",
                path="/workspace/out/report.xlsx",
                name="report.xlsx",
                caption="Report",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                size=123,
                run_count=1,
            )
        ],
    )

    store.save_task(record)
    loaded = store.get_task("task-1")

    assert loaded is not None
    assert loaded.artifacts == record.artifacts
    store.close()


def test_task_store_insert_rejects_duplicate_identity_without_overwrite(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    original = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="completed",
        thread_id="task-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 8, 28, tzinfo=UTC),
        updated_at=datetime(2026, 8, 28, tzinfo=UTC),
        result="original",
    )
    try:
        store.insert_task(original)
        with pytest.raises(sqlite3.IntegrityError):
            store.insert_task(replace(original, agent_name="other", result="overwritten"))

        loaded = store.get_task("task-1")
        assert loaded is not None
        assert loaded.agent_name == "main"
        assert loaded.result == "original"
    finally:
        store.close()


def test_task_store_update_requires_existing_identity(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    record = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="pending",
        thread_id="task-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 8, 28, tzinfo=UTC),
        updated_at=datetime(2026, 8, 28, tzinfo=UTC),
    )
    try:
        with pytest.raises(KeyError):
            store.update_task(record)

        store.insert_task(record)
        record.state = "completed"
        record.result = "done"
        store.update_task(record)
        loaded = store.get_task("task-1")
        assert loaded is not None
        assert loaded.state == "completed"
        assert loaded.result == "done"
    finally:
        store.close()


def test_task_store_rolls_back_task_update_when_event_append_fails(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    record = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="running",
        thread_id="task-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 8, 28, tzinfo=UTC),
        updated_at=datetime(2026, 8, 28, tzinfo=UTC),
        run_count=1,
    )
    try:
        store.insert_task(record)
        record.state = "completed"
        record.result = "must roll back"

        def fail_append(**kwargs):
            del kwargs
            raise RuntimeError("event append failed")

        monkeypatch.setattr(store, "_append_task_event_locked", fail_append)
        with pytest.raises(RuntimeError, match="event append failed"):
            store.update_task_with_event(
                record,
                event_type="task.completed",
                event_data={"status": "completed"},
                event_created_at=record.updated_at,
            )

        loaded = store.get_task(record.task_id)
        assert loaded is not None
        assert loaded.state == "running"
        assert loaded.result is None
        assert store.list_task_events(
            task_id=record.task_id,
            run_count=1,
            after_event_id=0,
        ) == []
    finally:
        store.close()
