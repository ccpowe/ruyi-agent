from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ruyi_agent.storage.task_codecs import parse_datetime, serialize_datetime
from ruyi_agent.storage.task_database import TaskDatabase


@dataclass(frozen=True, slots=True)
class StoredTaskEvent:
    """One durable, ordered public lifecycle event for a Gateway Task."""

    event_id: int
    task_id: str
    run_count: int
    event_type: str
    created_at: datetime
    data: dict[str, Any]


class TaskEventRepository:
    """Append and replay durable Task lifecycle events in event-id order."""

    def __init__(self, database: TaskDatabase) -> None:
        self._database = database

    def get(self, event_id: int) -> StoredTaskEvent | None:
        with self._database.locked_connection() as connection:
            row = connection.execute(
                """
                SELECT event_id, task_id, run_count, event_type, created_at, data_json
                FROM agent_task_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
        return row_to_task_event(row) if row is not None else None

    def list(
        self,
        *,
        task_id: str,
        run_count: int,
        after_event_id: int,
        limit: int,
    ) -> list[StoredTaskEvent]:
        if limit <= 0:
            raise ValueError("Task event limit must be positive")
        with self._database.locked_connection() as connection:
            rows = connection.execute(
                """
                SELECT event_id, task_id, run_count, event_type, created_at, data_json
                FROM agent_task_events
                WHERE task_id = ? AND run_count = ? AND event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (task_id, run_count, after_event_id, limit),
            ).fetchall()
        return [row_to_task_event(row) for row in rows]

    @staticmethod
    def append_locked(
        connection: sqlite3.Connection,
        *,
        task_id: str,
        run_count: int,
        event_type: str,
        encoded_data: str,
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        cursor = connection.execute(
            """
            INSERT INTO agent_task_events (
                task_id, run_count, event_type, created_at, data_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                task_id,
                run_count,
                event_type,
                serialize_datetime(event_created_at),
                encoded_data,
            ),
        )
        event_id = cursor.lastrowid
        if not isinstance(event_id, int):
            raise RuntimeError("SQLite did not allocate a Task event id")
        return StoredTaskEvent(
            event_id=event_id,
            task_id=task_id,
            run_count=run_count,
            event_type=event_type,
            created_at=event_created_at,
            data=json.loads(encoded_data),
        )

    @staticmethod
    def latest_row_locked(
        connection: sqlite3.Connection,
        *,
        task_id: str,
        run_count: int,
    ) -> tuple[Any, ...] | None:
        return connection.execute(
            """
            SELECT event_id, task_id, run_count, event_type, created_at, data_json
            FROM agent_task_events
            WHERE task_id = ? AND run_count = ?
            ORDER BY event_id DESC
            LIMIT 1
            """,
            (task_id, run_count),
        ).fetchone()


def serialize_event_data(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):
        raise TypeError("Task event data must encode to a JSON object")
    return encoded


def row_to_task_event(row: tuple[Any, ...]) -> StoredTaskEvent:
    data = json.loads(row[5])
    if not isinstance(data, dict):
        raise ValueError("Stored Task event data is not a JSON object")
    return StoredTaskEvent(
        event_id=int(row[0]),
        task_id=str(row[1]),
        run_count=int(row[2]),
        event_type=str(row[3]),
        created_at=parse_datetime(str(row[4])),
        data=data,
    )
