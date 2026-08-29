from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from ruyi_agent.storage.task_store import (
    StoredTaskAlreadyExistsError,
    TaskRootBudgetExceededError,
    TaskStore,
)
from ruyi_agent.task_models import PublishedArtifact, TaskRecord


def _budgeted_record(
    task_id: str,
    *,
    root_task_id: str,
    parent_task_id: str | None,
    depth: int,
    limit: int,
) -> TaskRecord:
    timestamp = datetime(2026, 8, 29, tzinfo=UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name="main",
        state="pending",
        thread_id=task_id,
        parent_task_id=parent_task_id,
        root_task_id=root_task_id,
        depth=depth,
        created_at=timestamp,
        updated_at=timestamp,
        delegation_root_id=f"node-a:{root_task_id}",
        delegation_max_depth=5,
        delegation_max_tasks_per_root=limit,
        delegation_visited_nodes=("node-a",),
    )


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


def test_task_store_budget_counts_root_and_reports_persisted_count(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    root = _budgeted_record(
        "root",
        root_task_id="root",
        parent_task_id=None,
        depth=1,
        limit=2,
    )
    child = _budgeted_record(
        "child",
        root_task_id="root",
        parent_task_id="root",
        depth=2,
        limit=2,
    )
    denied = _budgeted_record(
        "denied",
        root_task_id="root",
        parent_task_id="root",
        depth=2,
        limit=2,
    )
    try:
        store.insert_task(root)
        store.insert_task(child)

        with pytest.raises(TaskRootBudgetExceededError) as exc_info:
            store.insert_task(denied)

        assert exc_info.value.root_task_id == "root"
        assert exc_info.value.current_count == 2
        assert exc_info.value.max_tasks_per_root == 2
        assert store.count_tasks_under_root("root") == 2
        assert store.get_task("denied") is None
    finally:
        store.close()


def test_task_store_duplicate_identity_is_checked_before_full_budget(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    root = _budgeted_record(
        "root",
        root_task_id="root",
        parent_task_id=None,
        depth=1,
        limit=1,
    )
    try:
        store.insert_task(root)

        with pytest.raises(StoredTaskAlreadyExistsError) as exc_info:
            store.insert_task(root)

        assert exc_info.value.record.task_id == "root"
        assert store.count_tasks_under_root("root") == 1
    finally:
        store.close()


def test_independent_task_stores_atomically_compete_for_last_root_slot(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "tasks.sqlite")
    setup_store = TaskStore(db_path)
    setup_store.insert_task(
        _budgeted_record(
            "root",
            root_task_id="root",
            parent_task_id=None,
            depth=1,
            limit=2,
        )
    )
    setup_store.close()
    stores = [TaskStore(db_path), TaskStore(db_path)]
    barrier = threading.Barrier(2)

    def insert_child(index: int) -> Exception | None:
        barrier.wait()
        try:
            stores[index].insert_task(
                _budgeted_record(
                    f"child-{index}",
                    root_task_id="root",
                    parent_task_id="root",
                    depth=2,
                    limit=2,
                )
            )
        except Exception as exc:  # pragma: no branch - result is asserted below
            return exc
        return None

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(insert_child, range(2)))

        assert sum(result is None for result in results) == 1
        failures = [result for result in results if result is not None]
        assert len(failures) == 1
        assert isinstance(failures[0], TaskRootBudgetExceededError)
        assert failures[0].current_count == 2
        assert stores[0].count_tasks_under_root("root") == 2
    finally:
        for store in stores:
            store.close()


def test_task_store_creates_root_task_id_query_index(tmp_path) -> None:
    db_path = tmp_path / "tasks.sqlite"
    store = TaskStore(str(db_path))
    store.close()

    connection = sqlite3.connect(db_path)
    try:
        indexes = {
            row[1]
            for row in connection.execute("PRAGMA index_list(agent_tasks)").fetchall()
        }
    finally:
        connection.close()

    assert "idx_agent_tasks_root_task_id" in indexes
