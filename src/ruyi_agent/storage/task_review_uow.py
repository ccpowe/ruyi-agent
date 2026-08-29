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
from ruyi_agent.storage.task_event_repository import (
    StoredTaskEvent,
    serialize_event_data,
)
from ruyi_agent.storage.task_repository import TaskRepository
from ruyi_agent.storage.settled_outbox import (
    SettledOutboxIntent,
    SettledOutboxRepository,
)
from ruyi_agent.task_models import PendingReviewRecord, TaskRecord


AppendEvent = Callable[..., StoredTaskEvent]


class TaskReviewUnitOfWork:
    """Own the atomic Task, Pending Review, root projection, and event write."""

    def __init__(
        self,
        database: TaskDatabase,
        tasks: TaskRepository,
        settled_outbox: SettledOutboxRepository,
    ) -> None:
        self._database = database
        self._tasks = tasks
        self._settled_outbox = settled_outbox

    def update_transition(
        self,
        record: TaskRecord,
        *,
        pending_review: PendingReviewRecord | None,
        root_record: TaskRecord | None,
        events: list[tuple[TaskRecord, str, dict[str, Any], datetime]],
        append_event: AppendEvent,
        settled_outbox_intent: SettledOutboxIntent | None = None,
    ) -> list[StoredTaskEvent]:
        if pending_review is not None and pending_review.task_id != record.task_id:
            raise ValueError("Pending review owner does not match Task record")
        encoded_events = [
            (event_record, event_type, serialize_event_data(data), created_at)
            for event_record, event_type, data, created_at in events
        ]
        with self._database.transaction(immediate=True) as connection:
            self._tasks.update_locked(connection, record)
            self._replace_review_locked(connection, record.task_id, pending_review)
            if root_record is not None and root_record.task_id != record.task_id:
                self._tasks.update_locked(connection, root_record)
            stored_events = [
                append_event(
                    task_id=event_record.task_id,
                    run_count=event_record.run_count,
                    event_type=event_type,
                    encoded_data=encoded_data,
                    event_created_at=created_at,
                )
                for event_record, event_type, encoded_data, created_at in encoded_events
            ]
            if settled_outbox_intent is not None:
                self._settled_outbox.insert_locked(
                    connection,
                    settled_outbox_intent,
                )
            return stored_events

    def get(self, review_id: str) -> PendingReviewRecord | None:
        with self._database.locked_connection() as connection:
            row = connection.execute(
                """
                SELECT review_id, task_id, root_task_id, payload_json,
                       created_at, updated_at, ingest_sequence,
                       cursor_order_updated_at
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
                       created_at, updated_at, ingest_sequence,
                       cursor_order_updated_at
                FROM agent_task_pending_reviews
                {where}
                ORDER BY ingest_sequence ASC, review_id ASC
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
        existing = connection.execute(
            """
            SELECT review_id, ingest_sequence, cursor_order_updated_at
            FROM agent_task_pending_reviews
            WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        if pending_review is None:
            connection.execute(
                "DELETE FROM agent_task_pending_reviews WHERE task_id = ?",
                (task_id,),
            )
            return
        if existing is not None and existing[0] == pending_review.review_id:
            ingest_sequence = int(existing[1])
            cursor_order_updated_at = str(existing[2])
        else:
            state = connection.execute(
                """
                SELECT last_sequence
                FROM agent_task_review_ingest_state
                WHERE singleton = 1
                """
            ).fetchone()
            if state is None:
                raise RuntimeError("Pending Review ingest state is missing")
            ingest_sequence = int(state[0]) + 1
            if ingest_sequence > 2**63 - 1:
                raise OverflowError("Pending Review ingest sequence is exhausted")
            connection.execute(
                """
                UPDATE agent_task_review_ingest_state
                SET last_sequence = ?
                WHERE singleton = 1
                """,
                (ingest_sequence,),
            )
            cursor_order_updated_at = serialize_datetime(
                pending_review.cursor_order_updated_at or pending_review.updated_at
            )
        connection.execute(
            "DELETE FROM agent_task_pending_reviews WHERE task_id = ?",
            (task_id,),
        )
        connection.execute(
            """
            INSERT INTO agent_task_pending_reviews (
                review_id, task_id, root_task_id, payload_json, created_at, updated_at,
                ingest_sequence, cursor_order_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                pending_review.review_id,
                pending_review.task_id,
                pending_review.root_task_id,
                serialize_json_object(pending_review.payload),
                serialize_datetime(pending_review.created_at),
                serialize_datetime(pending_review.updated_at),
                ingest_sequence,
                cursor_order_updated_at,
            ),
        )
