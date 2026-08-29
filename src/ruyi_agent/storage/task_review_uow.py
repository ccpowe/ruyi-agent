from __future__ import annotations

import sqlite3
from collections.abc import Callable
from datetime import datetime
from typing import Any

from ruyi_agent.storage.task_codecs import (
    row_to_pending_review,
    serialize_datetime,
    serialize_json_object,
)
from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.storage.task_event_repository import StoredTaskEvent, serialize_event_data
from ruyi_agent.storage.task_repository import TaskRepository
from ruyi_agent.task_models import PendingReviewRecord, TaskRecord


AppendEvent = Callable[..., StoredTaskEvent]


class TaskReviewUnitOfWork:
    """Own the atomic Task, Pending Review, root projection, and event write."""

    def __init__(self, database: TaskDatabase, tasks: TaskRepository) -> None:
        self._database = database
        self._tasks = tasks

    def update_transition(
        self,
        record: TaskRecord,
        *,
        pending_review: PendingReviewRecord | None,
        root_record: TaskRecord | None,
        events: list[tuple[TaskRecord, str, dict[str, Any], datetime]],
        append_event: AppendEvent,
    ) -> list[StoredTaskEvent]:
        if pending_review is not None and pending_review.task_id != record.task_id:
            raise ValueError("Pending review owner does not match Task record")
        encoded_events = [
            (event_record, event_type, serialize_event_data(data), created_at)
            for event_record, event_type, data, created_at in events
        ]
        with self._database.transaction() as connection:
            self._tasks.update_locked(connection, record)
            self._replace_review_locked(connection, record.task_id, pending_review)
            if root_record is not None and root_record.task_id != record.task_id:
                self._tasks.update_locked(connection, root_record)
            return [
                append_event(
                    task_id=event_record.task_id,
                    run_count=event_record.run_count,
                    event_type=event_type,
                    encoded_data=encoded_data,
                    event_created_at=created_at,
                )
                for event_record, event_type, encoded_data, created_at in encoded_events
            ]

    def get(self, review_id: str) -> PendingReviewRecord | None:
        with self._database.locked_connection() as connection:
            row = connection.execute(
                """
                SELECT review_id, task_id, root_task_id, payload_json,
                       created_at, updated_at
                FROM agent_task_pending_reviews
                WHERE review_id = ?
                """,
                (review_id,),
            ).fetchone()
        return row_to_pending_review(row) if row is not None else None

    def list(
        self,
        *,
        root_task_id: str | None,
        task_id: str | None,
    ) -> list[PendingReviewRecord]:
        clauses: list[str] = []
        params: list[str] = []
        if root_task_id is not None:
            clauses.append("root_task_id = ?")
            params.append(root_task_id)
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._database.locked_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT review_id, task_id, root_task_id, payload_json,
                       created_at, updated_at
                FROM agent_task_pending_reviews
                {where}
                ORDER BY created_at ASC, review_id ASC
                """,
                params,
            ).fetchall()
        return [row_to_pending_review(row) for row in rows]

    @staticmethod
    def _replace_review_locked(
        connection: sqlite3.Connection,
        task_id: str,
        pending_review: PendingReviewRecord | None,
    ) -> None:
        connection.execute(
            "DELETE FROM agent_task_pending_reviews WHERE task_id = ?",
            (task_id,),
        )
        if pending_review is None:
            return
        connection.execute(
            """
            INSERT INTO agent_task_pending_reviews (
                review_id, task_id, root_task_id, payload_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                pending_review.review_id,
                pending_review.task_id,
                pending_review.root_task_id,
                serialize_json_object(pending_review.payload),
                serialize_datetime(pending_review.created_at),
                serialize_datetime(pending_review.updated_at),
            ),
        )
