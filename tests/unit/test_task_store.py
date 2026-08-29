from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from ruyi_agent.storage.task_store import (
    StoredTaskAlreadyExistsError,
    TaskRootBudgetExceededError,
    TaskStore,
)
from ruyi_agent.task_models import PendingReviewRecord, PublishedArtifact, TaskRecord


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


def test_task_store_review_transition_is_atomic_with_task_and_root_projection(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    created_at = datetime(2026, 8, 29, tzinfo=UTC)
    root = TaskRecord(
        task_id="root",
        agent_name="main",
        state="completed",
        thread_id="root",
        parent_task_id=None,
        root_task_id="root",
        depth=1,
        created_at=created_at,
        updated_at=created_at,
    )
    child = TaskRecord(
        task_id="child",
        agent_name="worker",
        state="pending",
        thread_id="child",
        parent_task_id="root",
        root_task_id="root",
        depth=2,
        created_at=created_at,
        updated_at=created_at,
    )
    payload = {"review_id": "review-1", "action_requests": []}
    review = PendingReviewRecord(
        review_id="review-1",
        task_id="child",
        root_task_id="root",
        payload=payload,
        created_at=created_at,
        updated_at=created_at,
    )
    try:
        store.insert_task(root)
        store.insert_task(child)
        child.state = "waiting_for_human"
        child.pending_review = payload
        root.pending_review = {**payload, "source_task_id": "child"}

        original_append = store._append_task_event_locked

        def fail_append(**kwargs):
            del kwargs
            raise RuntimeError("event append failed")

        monkeypatch.setattr(store, "_append_task_event_locked", fail_append)
        with pytest.raises(RuntimeError, match="event append failed"):
            store.update_review_transition(
                child,
                pending_review=review,
                root_record=root,
                events=[
                    (
                        child,
                        "task.review_requested",
                        {"status": "waiting_for_human"},
                        created_at,
                    ),
                    (
                        root,
                        "task.review_requested",
                        {"status": "completed"},
                        created_at,
                    ),
                ],
            )

        persisted_child = store.get_task("child")
        persisted_root = store.get_task("root")
        assert persisted_child is not None
        assert persisted_child.state == "pending"
        assert persisted_child.pending_review is None
        assert persisted_root is not None
        assert persisted_root.pending_review is None
        assert store.get_pending_review("review-1") is None

        monkeypatch.setattr(store, "_append_task_event_locked", original_append)
        store.update_review_transition(
            child,
            pending_review=review,
            root_record=root,
            events=[
                (
                    child,
                    "task.review_requested",
                    {"status": "waiting_for_human"},
                    created_at,
                ),
            ],
        )
        child.state = "running"
        child.pending_review = None
        root.pending_review = None
        monkeypatch.setattr(store, "_append_task_event_locked", fail_append)
        with pytest.raises(RuntimeError, match="event append failed"):
            store.update_review_transition(
                child,
                pending_review=None,
                root_record=root,
                events=[
                    (
                        child,
                        "task.running",
                        {"status": "running"},
                        created_at,
                    )
                ],
            )

        persisted_child = store.get_task("child")
        persisted_root = store.get_task("root")
        assert persisted_child is not None
        assert persisted_child.state == "waiting_for_human"
        assert persisted_child.pending_review == payload
        assert persisted_root is not None
        assert persisted_root.pending_review == {
            **payload,
            "source_task_id": "child",
        }
        assert store.get_pending_review("review-1") == replace(
            review,
            ingest_sequence=1,
            cursor_order_updated_at=created_at,
        )
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


def test_task_store_backfills_all_legacy_reviews_and_rebuilds_root_projection(
    tmp_path,
) -> None:
    db_path = tmp_path / "tasks.sqlite"
    created_at = datetime(2026, 8, 29, tzinfo=UTC)
    legacy = TaskStore(str(db_path))
    payload_a = {"review_id": "review-a", "action_requests": []}
    payload_b = {"review_id": "review-b", "action_requests": []}
    root = TaskRecord(
        task_id="root",
        agent_name="main",
        state="completed",
        thread_id="root",
        parent_task_id=None,
        root_task_id="root",
        depth=1,
        created_at=created_at,
        updated_at=created_at,
        pending_review={**payload_b, "source_task_id": "child-b"},
    )
    child_a = TaskRecord(
        task_id="child-a",
        agent_name="worker",
        state="waiting_for_human",
        thread_id="child-a",
        parent_task_id="root",
        root_task_id="root",
        depth=2,
        created_at=created_at,
        updated_at=created_at,
        pending_review=payload_a,
    )
    child_b = replace(
        child_a,
        task_id="child-b",
        thread_id="child-b",
        pending_review=payload_b,
    )
    legacy.insert_task(root)
    legacy.insert_task(child_a)
    legacy.insert_task(child_b)
    legacy.close()

    reopened = TaskStore(str(db_path))
    try:
        reviews = reopened.list_pending_reviews(root_task_id="root")
        assert [review.review_id for review in reviews] == ["review-a", "review-b"]
        restored_root = reopened.get_task("root")
        assert restored_root is not None
        assert restored_root.pending_review == {
            **payload_a,
            "source_task_id": "child-a",
        }
    finally:
        reopened.close()


def test_task_store_migrates_legacy_review_ingest_order_idempotently(
    tmp_path,
) -> None:
    db_path = tmp_path / "tasks.sqlite"
    created_at = datetime(2026, 8, 29, tzinfo=UTC)
    store = TaskStore(str(db_path))
    for task_id in ("task-a", "task-b"):
        store.insert_task(
            TaskRecord(
                task_id=task_id,
                agent_name="main",
                state="waiting_for_human",
                thread_id=task_id,
                parent_task_id=None,
                root_task_id=task_id,
                depth=0,
                created_at=created_at,
                updated_at=created_at,
            )
        )
    store.close()
    connection = sqlite3.connect(db_path)
    try:
        connection.execute("DROP TABLE agent_task_pending_reviews")
        connection.execute(
            """
            CREATE TABLE agent_task_pending_reviews (
                review_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL UNIQUE,
                root_task_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES agent_tasks(task_id)
            )
            """
        )
        for suffix in ("b", "a"):
            connection.execute(
                """
                INSERT INTO agent_task_pending_reviews (
                    review_id, task_id, root_task_id, payload_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    f"review-{suffix}",
                    f"task-{suffix}",
                    f"task-{suffix}",
                    f'{{"review_id":"review-{suffix}"}}',
                    created_at.isoformat(),
                    created_at.isoformat(),
                ),
            )
        connection.commit()
    finally:
        connection.close()

    reopened = TaskStore(str(db_path))
    first = reopened.list_pending_reviews()
    reopened.close()
    reopened_again = TaskStore(str(db_path))
    try:
        second = reopened_again.list_pending_reviews()
        assert [(item.review_id, item.ingest_sequence) for item in first] == [
            ("review-a", 1),
            ("review-b", 2),
        ]
        assert [(item.review_id, item.ingest_sequence) for item in second] == [
            ("review-a", 1),
            ("review-b", 2),
        ]
        assert all(item.cursor_order_updated_at == created_at for item in first)
        assert all(item.cursor_order_updated_at == created_at for item in second)
        connection = sqlite3.connect(db_path)
        try:
            columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(agent_task_pending_reviews)"
                ).fetchall()
            }
            indexes = {
                row[1]
                for row in connection.execute(
                    "PRAGMA index_list(agent_task_pending_reviews)"
                ).fetchall()
            }
        finally:
            connection.close()
        assert "ingest_sequence" in columns
        assert "idx_agent_task_pending_reviews_ingest" in indexes
    finally:
        reopened_again.close()


def test_task_store_preserves_review_sequence_and_allocates_replacement_order(
    tmp_path,
) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    created_at = datetime(2026, 8, 29, tzinfo=UTC)
    task = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="waiting_for_human",
        thread_id="task-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=created_at,
        updated_at=created_at,
    )
    store.insert_task(task)
    first = PendingReviewRecord(
        review_id="review-1",
        task_id=task.task_id,
        root_task_id=task.root_task_id,
        payload={"review_id": "review-1"},
        created_at=created_at,
        updated_at=created_at,
    )
    try:
        store.update_review_transition(
            task,
            pending_review=first,
            root_record=None,
            events=[],
        )
        persisted = store.get_pending_review("review-1")
        assert persisted is not None
        assert persisted.ingest_sequence == 1
        assert persisted.cursor_order_updated_at == created_at

        store.update_review_transition(
            task,
            pending_review=replace(
                first,
                updated_at=created_at + timedelta(minutes=1),
            ),
            root_record=None,
            events=[],
        )
        unchanged = store.get_pending_review("review-1")
        assert unchanged is not None
        assert unchanged.ingest_sequence == 1
        assert unchanged.updated_at == created_at + timedelta(minutes=1)
        assert unchanged.cursor_order_updated_at == created_at

        replacement = replace(
            first,
            review_id="review-2",
            payload={"review_id": "review-2"},
        )
        store.update_review_transition(
            task,
            pending_review=replacement,
            root_record=None,
            events=[],
        )
        persisted_replacement = store.get_pending_review("review-2")
        assert persisted_replacement is not None
        assert persisted_replacement.ingest_sequence == 2

        store.update_review_transition(
            task,
            pending_review=None,
            root_record=None,
            events=[],
        )
        store.close()
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        restored_task = store.get_task(task.task_id)
        assert restored_task is not None
        task = restored_task
        third = replace(
            first,
            review_id="review-3",
            payload={"review_id": "review-3"},
        )
        store.update_review_transition(
            task,
            pending_review=third,
            root_record=None,
            events=[],
        )
        persisted_third = store.get_pending_review("review-3")
        assert persisted_third is not None
        assert persisted_third.ingest_sequence == 3
    finally:
        store.close()
