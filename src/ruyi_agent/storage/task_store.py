from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.storage.task_event_repository import (
    StoredTaskEvent,
    TaskEventRepository,
)
from ruyi_agent.storage.task_repository import (
    StoredTaskAlreadyExistsError,
    TaskRepository,
    TaskRootBudgetExceededError,
)
from ruyi_agent.storage.task_review_uow import TaskReviewUnitOfWork
from ruyi_agent.storage.task_schema import initialize_task_database
from ruyi_agent.storage.task_unit_of_work import TaskLifecycleUnitOfWork
from ruyi_agent.task_models import PendingReviewRecord, TaskRecord

__all__ = [
    "StoredTaskAlreadyExistsError",
    "StoredTaskEvent",
    "TaskRootBudgetExceededError",
    "TaskStore",
    "task_record_for_restart",
]


class TaskStore:
    """Stable facade over focused Task persistence components.

    ``TaskDatabase`` owns locking and commit/rollback. Repositories own one
    resource's SQL; units of work own cross-resource atomicity. Callers retain
    the original API without depending on SQLite schema details.
    """

    def __init__(self, db_path: str) -> None:
        self._database = TaskDatabase(db_path)
        initialize_task_database(self._database)
        self._tasks = TaskRepository(self._database)
        self._events = TaskEventRepository(self._database)
        self._lifecycle = TaskLifecycleUnitOfWork(
            self._database,
            self._tasks,
            self._events,
        )
        self._reviews = TaskReviewUnitOfWork(self._database, self._tasks)

    def save_task(self, record: TaskRecord) -> None:
        """Upsert a task without SQLite's delete-and-reinsert semantics."""

        self._tasks.save(record)

    def insert_task(self, record: TaskRecord) -> None:
        """Insert a newly allocated task identity and reject duplicates."""

        self._tasks.insert(record)

    def insert_task_with_event(
        self,
        record: TaskRecord,
        *,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Insert a Task and its first lifecycle event atomically."""

        return self._lifecycle.insert_with_event(
            record,
            event_type=event_type,
            event_data=event_data,
            event_created_at=event_created_at,
            append_event=self._append_task_event_locked,
        )

    def update_task(self, record: TaskRecord) -> None:
        """Update an existing task without ever recreating its row."""

        self._tasks.update(record)

    def update_task_with_event(
        self,
        record: TaskRecord,
        *,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Update a Task and append its public lifecycle event atomically."""

        return self._lifecycle.update_with_event(
            record,
            event_type=event_type,
            event_data=event_data,
            event_created_at=event_created_at,
            append_event=self._append_task_event_locked,
        )

    def update_review_transition(
        self,
        record: TaskRecord,
        *,
        pending_review: PendingReviewRecord | None,
        root_record: TaskRecord | None,
        events: list[tuple[TaskRecord, str, dict[str, Any], datetime]],
    ) -> list[StoredTaskEvent]:
        """Atomically persist review ownership, projections, and events."""

        return self._reviews.update_transition(
            record,
            pending_review=pending_review,
            root_record=root_record,
            events=events,
            append_event=self._append_task_event_locked,
        )

    def append_task_event(
        self,
        *,
        task_id: str,
        run_count: int,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Append an event without changing the persisted Task row."""

        return self._lifecycle.append_event(
            task_id=task_id,
            run_count=run_count,
            event_type=event_type,
            event_data=event_data,
            event_created_at=event_created_at,
            append_event=self._append_task_event_locked,
        )

    def get_task_event(self, event_id: int) -> StoredTaskEvent | None:
        return self._events.get(event_id)

    def list_task_events(
        self,
        *,
        task_id: str,
        run_count: int,
        after_event_id: int,
        limit: int = 100,
    ) -> list[StoredTaskEvent]:
        return self._events.list(
            task_id=task_id,
            run_count=run_count,
            after_event_id=after_event_id,
            limit=limit,
        )

    def get_task_with_event_anchor(
        self,
        *,
        task_id: str,
        run_count: int,
        build_anchor: Callable[[TaskRecord], tuple[str, dict[str, Any], datetime]],
    ) -> tuple[TaskRecord, StoredTaskEvent, bool]:
        """Read a Task and idempotently ensure its run has a durable anchor."""

        return self._lifecycle.get_with_event_anchor(
            task_id=task_id,
            run_count=run_count,
            build_anchor=build_anchor,
            append_event=self._append_task_event_locked,
        )

    def get_task(self, task_id: str) -> TaskRecord | None:
        return self._tasks.get(task_id)

    def get_pending_review(self, review_id: str) -> PendingReviewRecord | None:
        return self._reviews.get(review_id)

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        return self._reviews.list(root_task_id=root_task_id, task_id=task_id)

    def get_task_by_parent_thread_id(
        self,
        *,
        task_id: str,
        parent_thread_id: str,
    ) -> TaskRecord | None:
        return self._tasks.get_by_parent_thread_id(
            task_id=task_id,
            parent_thread_id=parent_thread_id,
        )

    def list_tasks(self) -> list[TaskRecord]:
        return self._tasks.list_all()

    def list_tasks_by_parent_thread_id(
        self,
        parent_thread_id: str,
    ) -> list[TaskRecord]:
        return self._tasks.list_by_parent_thread_id(parent_thread_id)

    def count_tasks_under_root(self, root_task_id: str) -> int:
        return self._tasks.count_under_root(root_task_id)

    def close(self) -> None:
        self._database.close()

    def _append_task_event_locked(
        self,
        *,
        task_id: str,
        run_count: int,
        event_type: str,
        encoded_data: str,
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Compatibility injection seam used within an active unit of work."""

        with self._database.locked_connection() as connection:
            return self._events.append_locked(
                connection,
                task_id=task_id,
                run_count=run_count,
                event_type=event_type,
                encoded_data=encoded_data,
                event_created_at=event_created_at,
            )


def task_record_for_restart(record: TaskRecord) -> TaskRecord:
    if record.route_kind == "local" and record.state in {"pending", "running"}:
        return replace(
            record,
            state="interrupted",
            updated_at=datetime.now(UTC),
            error=record.error or "Task interrupted: local process restarted.",
        )
    return replace(record)
