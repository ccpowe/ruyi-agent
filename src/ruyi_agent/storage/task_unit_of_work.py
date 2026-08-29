from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from typing import Any

from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.storage.task_event_repository import (
    StoredTaskEvent,
    TaskEventRepository,
    row_to_task_event,
    serialize_event_data,
)
from ruyi_agent.storage.task_repository import TaskRepository
from ruyi_agent.storage.settled_outbox import (
    SettledOutboxIntent,
    SettledOutboxRepository,
)
from ruyi_agent.task_models import TaskRecord


AppendEvent = Callable[..., StoredTaskEvent]


class TaskLifecycleUnitOfWork:
    """Atomically compose Task state and its durable lifecycle ledger."""

    def __init__(
        self,
        database: TaskDatabase,
        tasks: TaskRepository,
        events: TaskEventRepository,
        settled_outbox: SettledOutboxRepository,
    ) -> None:
        self._database = database
        self._tasks = tasks
        self._events = events
        self._settled_outbox = settled_outbox

    def insert_with_event(
        self,
        record: TaskRecord,
        *,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
        append_event: AppendEvent,
    ) -> StoredTaskEvent:
        encoded_data = serialize_event_data(event_data)
        with self._database.transaction(immediate=True) as connection:
            self._tasks.ensure_new_identity_locked(connection, record.task_id)
            self._tasks.enforce_root_budget_locked(connection, record)
            self._tasks.insert_locked(connection, record)
            return append_event(
                task_id=record.task_id,
                run_count=record.run_count,
                event_type=event_type,
                encoded_data=encoded_data,
                event_created_at=event_created_at,
            )

    def update_with_event(
        self,
        record: TaskRecord,
        *,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
        append_event: AppendEvent,
        settled_outbox_intent: SettledOutboxIntent | None = None,
    ) -> StoredTaskEvent:
        encoded_data = serialize_event_data(event_data)
        with self._database.transaction() as connection:
            self._tasks.update_locked(connection, record)
            event = append_event(
                task_id=record.task_id,
                run_count=record.run_count,
                event_type=event_type,
                encoded_data=encoded_data,
                event_created_at=event_created_at,
            )
            if settled_outbox_intent is not None:
                self._settled_outbox.insert_locked(
                    connection,
                    settled_outbox_intent,
                )
            return event

    def append_event(
        self,
        *,
        task_id: str,
        run_count: int,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
        append_event: AppendEvent,
    ) -> StoredTaskEvent:
        encoded_data = serialize_event_data(event_data)
        with self._database.transaction() as connection:
            if not self._tasks.exists_locked(connection, task_id):
                raise KeyError(f"Task does not exist: {task_id}")
            return append_event(
                task_id=task_id,
                run_count=run_count,
                event_type=event_type,
                encoded_data=encoded_data,
                event_created_at=event_created_at,
            )

    def get_with_event_anchor(
        self,
        *,
        task_id: str,
        run_count: int,
        build_anchor: Callable[
            [TaskRecord], tuple[str, dict[str, Any], datetime]
        ],
        append_event: AppendEvent,
    ) -> tuple[TaskRecord, StoredTaskEvent, bool]:
        with self._database.transaction() as connection:
            record = self._tasks.get_locked(connection, task_id)
            if record is None:
                raise KeyError(f"Task does not exist: {task_id}")
            if record.run_count != run_count:
                raise ValueError(
                    f"Task run mismatch: expected {run_count}, current {record.run_count}"
                )

            event_row = self._events.latest_row_locked(
                connection,
                task_id=task_id,
                run_count=run_count,
            )
            if event_row is not None:
                return record, row_to_task_event(event_row), False

            event_type, event_data, event_created_at = build_anchor(record)
            event = append_event(
                task_id=task_id,
                run_count=run_count,
                event_type=event_type,
                encoded_data=serialize_event_data(event_data),
                event_created_at=event_created_at,
            )
            return record, event, True
