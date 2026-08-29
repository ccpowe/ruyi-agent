from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ruyi_agent.task_models import PendingReviewRecord, PublishedArtifact, TaskRecord


@dataclass(frozen=True, slots=True)
class StoredTaskEvent:
    """One durable, ordered public lifecycle event for a Gateway Task."""

    event_id: int
    task_id: str
    run_count: int
    event_type: str
    created_at: datetime
    data: dict[str, Any]


class TaskRootBudgetExceededError(ValueError):
    """A persisted delegation tree has reached its cumulative Task limit."""

    def __init__(
        self,
        *,
        root_task_id: str,
        current_count: int,
        max_tasks_per_root: int,
    ) -> None:
        self.root_task_id = root_task_id
        self.current_count = current_count
        self.max_tasks_per_root = max_tasks_per_root
        super().__init__(
            "root_task_id="
            f"{root_task_id} current_count={current_count} "
            f"max_tasks_per_root={max_tasks_per_root}"
        )


class StoredTaskAlreadyExistsError(sqlite3.IntegrityError):
    """A stable Task identity already exists in persistent storage."""

    def __init__(self, record: TaskRecord) -> None:
        self.record = record
        super().__init__(f"Task already exists: {record.task_id}")


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _serialize_event_data(value: dict[str, Any]) -> str:
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


def _row_to_task_event(row: tuple[Any, ...]) -> StoredTaskEvent:
    data = json.loads(row[5])
    if not isinstance(data, dict):
        raise ValueError("Stored Task event data is not a JSON object")
    return StoredTaskEvent(
        event_id=int(row[0]),
        task_id=str(row[1]),
        run_count=int(row[2]),
        event_type=str(row[3]),
        created_at=_parse_datetime(str(row[4])),
        data=data,
    )


def _row_to_pending_review(row: tuple[Any, ...]) -> PendingReviewRecord:
    payload = json.loads(row[3])
    if not isinstance(payload, dict):
        raise ValueError("Stored pending review payload is not a JSON object")
    return PendingReviewRecord(
        review_id=str(row[0]),
        task_id=str(row[1]),
        root_task_id=str(row[2]),
        payload=payload,
        created_at=_parse_datetime(str(row[4])),
        updated_at=_parse_datetime(str(row[5])),
    )


def _artifact_to_dict(value: Any) -> dict[str, Any]:
    return {
        "artifact_id": value.artifact_id,
        "path": value.path,
        "name": value.name,
        "caption": value.caption,
        "content_type": value.content_type,
        "size": value.size,
        "run_count": value.run_count,
    }


def _parse_artifacts(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        raw_items = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(raw_items, list):
        return []
    artifacts = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        artifact_id = item.get("artifact_id")
        path = item.get("path")
        name = item.get("name")
        content_type = item.get("content_type")
        size = item.get("size")
        run_count = item.get("run_count")
        if not all(
            [
                isinstance(artifact_id, str) and artifact_id,
                isinstance(path, str) and path,
                isinstance(name, str) and name,
                isinstance(content_type, str) and content_type,
                isinstance(size, int),
                isinstance(run_count, int),
            ]
        ):
            continue
        caption = item.get("caption")
        artifacts.append(
            PublishedArtifact(
                artifact_id=artifact_id,
                path=path,
                name=name,
                caption=caption if isinstance(caption, str) and caption else None,
                content_type=content_type,
                size=size,
                run_count=run_count,
            )
        )
    return artifacts


class TaskStore:
    """SQLite-backed store for task control-plane state."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self._init_db()

    def save_task(self, record: TaskRecord) -> None:
        """Upsert a task without SQLite's delete-and-reinsert REPLACE semantics."""

        with self._lock:
            try:
                self._begin_write_locked()
                if not self._task_exists_locked(record.task_id):
                    self._enforce_root_budget_locked(record)
                self._conn.execute(
                    self._insert_sql(upsert=True),
                    self._record_values(record),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def insert_task(self, record: TaskRecord) -> None:
        """Insert a newly allocated task identity and reject duplicates."""

        with self._lock:
            try:
                self._begin_write_locked()
                self._ensure_new_task_identity_locked(record.task_id)
                self._enforce_root_budget_locked(record)
                self._conn.execute(
                    self._insert_sql(upsert=False),
                    self._record_values(record),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def insert_task_with_event(
        self,
        record: TaskRecord,
        *,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Insert a Task and its first lifecycle event atomically."""

        encoded_data = _serialize_event_data(event_data)
        with self._lock:
            try:
                self._begin_write_locked()
                self._ensure_new_task_identity_locked(record.task_id)
                self._enforce_root_budget_locked(record)
                self._conn.execute(
                    self._insert_sql(upsert=False),
                    self._record_values(record),
                )
                event = self._append_task_event_locked(
                    task_id=record.task_id,
                    run_count=record.run_count,
                    event_type=event_type,
                    encoded_data=encoded_data,
                    event_created_at=event_created_at,
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return event

    def update_task(self, record: TaskRecord) -> None:
        """Update an existing task without ever recreating its row."""

        columns = self._write_columns()
        assignments = ", ".join(f"{column} = ?" for column in columns[1:])
        values = self._record_values(record)
        with self._lock:
            cursor = self._conn.execute(
                f"UPDATE agent_tasks SET {assignments} WHERE task_id = ?",
                (*values[1:], record.task_id),
            )
            self._conn.commit()
            if cursor.rowcount != 1:
                raise KeyError(f"Task does not exist: {record.task_id}")

    def update_task_with_event(
        self,
        record: TaskRecord,
        *,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Update a Task and append its public lifecycle event atomically."""

        encoded_data = _serialize_event_data(event_data)
        columns = self._write_columns()
        assignments = ", ".join(f"{column} = ?" for column in columns[1:])
        values = self._record_values(record)
        with self._lock:
            try:
                cursor = self._conn.execute(
                    f"UPDATE agent_tasks SET {assignments} WHERE task_id = ?",
                    (*values[1:], record.task_id),
                )
                if cursor.rowcount != 1:
                    raise KeyError(f"Task does not exist: {record.task_id}")
                event = self._append_task_event_locked(
                    task_id=record.task_id,
                    run_count=record.run_count,
                    event_type=event_type,
                    encoded_data=encoded_data,
                    event_created_at=event_created_at,
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return event

    def update_review_transition(
        self,
        record: TaskRecord,
        *,
        pending_review: PendingReviewRecord | None,
        root_record: TaskRecord | None,
        events: list[tuple[TaskRecord, str, dict[str, Any], datetime]],
    ) -> list[StoredTaskEvent]:
        """Persist a Task/review transition and compatibility projection atomically.

        ``pending_review`` is the authoritative pending resource for ``record``;
        passing ``None`` removes any pending review owned by that Task.  A
        distinct ``root_record`` carries the legacy single-review projection.
        All Task rows, the review row, and lifecycle events share one SQLite
        transaction so a restart cannot observe a half-applied review state.
        """

        if pending_review is not None and pending_review.task_id != record.task_id:
            raise ValueError("Pending review owner does not match Task record")
        encoded_events = [
            (event_record, event_type, _serialize_event_data(data), created_at)
            for event_record, event_type, data, created_at in events
        ]
        with self._lock:
            try:
                self._update_task_locked(record)
                self._conn.execute(
                    "DELETE FROM agent_task_pending_reviews WHERE task_id = ?",
                    (record.task_id,),
                )
                if pending_review is not None:
                    self._conn.execute(
                        """
                        INSERT INTO agent_task_pending_reviews (
                            review_id,
                            task_id,
                            root_task_id,
                            payload_json,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            pending_review.review_id,
                            pending_review.task_id,
                            pending_review.root_task_id,
                            json.dumps(
                                pending_review.payload,
                                ensure_ascii=True,
                                sort_keys=True,
                            ),
                            _serialize_datetime(pending_review.created_at),
                            _serialize_datetime(pending_review.updated_at),
                        ),
                    )
                if root_record is not None and root_record.task_id != record.task_id:
                    self._update_task_locked(root_record)

                stored_events = [
                    self._append_task_event_locked(
                        task_id=event_record.task_id,
                        run_count=event_record.run_count,
                        event_type=event_type,
                        encoded_data=encoded_data,
                        event_created_at=created_at,
                    )
                    for event_record, event_type, encoded_data, created_at in encoded_events
                ]
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return stored_events

    def append_task_event(
        self,
        *,
        task_id: str,
        run_count: int,
        event_type: str,
        event_data: dict[str, Any],
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        """Append an event without changing the already-persisted Task row."""

        encoded_data = _serialize_event_data(event_data)
        with self._lock:
            try:
                if not self._task_exists_locked(task_id):
                    raise KeyError(f"Task does not exist: {task_id}")
                event = self._append_task_event_locked(
                    task_id=task_id,
                    run_count=run_count,
                    event_type=event_type,
                    encoded_data=encoded_data,
                    event_created_at=event_created_at,
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return event

    def get_task_event(self, event_id: int) -> StoredTaskEvent | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT event_id, task_id, run_count, event_type, created_at, data_json
                FROM agent_task_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
        return _row_to_task_event(row) if row is not None else None

    def list_task_events(
        self,
        *,
        task_id: str,
        run_count: int,
        after_event_id: int,
        limit: int = 100,
    ) -> list[StoredTaskEvent]:
        if limit <= 0:
            raise ValueError("Task event limit must be positive")
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT event_id, task_id, run_count, event_type, created_at, data_json
                FROM agent_task_events
                WHERE task_id = ? AND run_count = ? AND event_id > ?
                ORDER BY event_id ASC
                LIMIT ?
                """,
                (task_id, run_count, after_event_id, limit),
            ).fetchall()
        return [_row_to_task_event(row) for row in rows]

    def get_task_with_event_anchor(
        self,
        *,
        task_id: str,
        run_count: int,
        build_anchor: Callable[[TaskRecord], tuple[str, dict[str, Any], datetime]],
    ) -> tuple[TaskRecord, StoredTaskEvent, bool]:
        """Read a Task and idempotently ensure its run has a durable anchor.

        This is the upgrade reconciliation used by a first SSE subscription for
        Tasks created before the event ledger existed. The persisted Task row,
        anchor check, optional insert, and high-water read share one SQLite lock
        and transaction view.
        """

        with self._lock:
            try:
                task_row = self._conn.execute(
                    f"""
                    SELECT
                        {self._select_columns()}
                    FROM agent_tasks
                    WHERE task_id = ?
                    """,
                    (task_id,),
                ).fetchone()
                if task_row is None:
                    raise KeyError(f"Task does not exist: {task_id}")
                record = self._row_to_task_record(task_row)
                if record.run_count != run_count:
                    raise ValueError(
                        f"Task run mismatch: expected {run_count}, current {record.run_count}"
                    )

                event_row = self._latest_task_event_row_locked(
                    task_id=task_id,
                    run_count=run_count,
                )
                created = False
                if event_row is None:
                    event_type, event_data, event_created_at = build_anchor(record)
                    event = self._append_task_event_locked(
                        task_id=task_id,
                        run_count=run_count,
                        event_type=event_type,
                        encoded_data=_serialize_event_data(event_data),
                        event_created_at=event_created_at,
                    )
                    created = True
                else:
                    event = _row_to_task_event(event_row)
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return record, event, created

    def get_task(self, task_id: str) -> TaskRecord | None:
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT
                    {self._select_columns()}
                FROM agent_tasks
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_task_record(row)

    def get_pending_review(self, review_id: str) -> PendingReviewRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT review_id, task_id, root_task_id, payload_json,
                       created_at, updated_at
                FROM agent_task_pending_reviews
                WHERE review_id = ?
                """,
                (review_id,),
            ).fetchone()
        return _row_to_pending_review(row) if row is not None else None

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
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
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT review_id, task_id, root_task_id, payload_json,
                       created_at, updated_at
                FROM agent_task_pending_reviews
                {where}
                ORDER BY created_at ASC, review_id ASC
                """,
                params,
            ).fetchall()
        return [_row_to_pending_review(row) for row in rows]

    def get_task_by_parent_thread_id(
        self,
        *,
        task_id: str,
        parent_thread_id: str,
    ) -> TaskRecord | None:
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT
                    {self._select_columns()}
                FROM agent_tasks
                WHERE task_id = ? AND parent_thread_id = ?
                """,
                (task_id, parent_thread_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_task_record(row)

    def list_tasks(self) -> list[TaskRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT
                    {self._select_columns()}
                FROM agent_tasks
                """
            ).fetchall()
        return [self._row_to_task_record(row) for row in rows]

    def list_tasks_by_parent_thread_id(
        self,
        parent_thread_id: str,
    ) -> list[TaskRecord]:
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT
                    {self._select_columns()}
                FROM agent_tasks
                WHERE parent_thread_id = ?
                """,
                (parent_thread_id,),
            ).fetchall()
        return [self._row_to_task_record(row) for row in rows]

    def count_tasks_under_root(self, root_task_id: str) -> int:
        """Count every persisted Task in a delegation tree, including its root."""

        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM agent_tasks WHERE root_task_id = ?",
                (root_task_id,),
            ).fetchone()
        assert row is not None
        return int(row[0])

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _ensure_parent_dir(self) -> None:
        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            self._conn.execute("PRAGMA foreign_keys = ON")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_tasks (
                    task_id TEXT PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    thread_id TEXT NOT NULL,
                    parent_task_id TEXT,
                    root_task_id TEXT NOT NULL,
                    depth INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    result TEXT,
                    error TEXT,
                    run_count INTEGER NOT NULL,
                    route_kind TEXT NOT NULL,
                    upstream_task_id TEXT,
                    parent_thread_id TEXT,
                    mailbox_suppressed INTEGER NOT NULL DEFAULT 0,
                    mailbox_delivered INTEGER NOT NULL DEFAULT 0,
                    webhook_json TEXT,
                    delegation_root_id TEXT,
                    delegation_max_depth INTEGER,
                    delegation_max_tasks_per_root INTEGER,
                    delegation_visited_nodes_json TEXT NOT NULL DEFAULT '[]',
                    permission_profile TEXT NOT NULL DEFAULT '',
                    effective_skill_names_json TEXT NOT NULL DEFAULT '[]',
                    skill_view_path TEXT,
                    skill_view_hash TEXT,
                    pending_review_json TEXT,
                    artifacts_json TEXT NOT NULL DEFAULT '[]'
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_task_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    run_count INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    data_json TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_task_pending_reviews (
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
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_task_pending_reviews_root
                ON agent_task_pending_reviews(root_task_id, created_at, review_id)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_task_events_task_run_event
                ON agent_task_events(task_id, run_count, event_id)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_tasks_root_task_id
                ON agent_tasks(root_task_id)
                """
            )
            self._ensure_column(
                table="agent_tasks",
                column="permission_profile",
                definition="TEXT NOT NULL DEFAULT ''",
            )
            self._ensure_column(
                table="agent_tasks",
                column="pending_review_json",
                definition="TEXT",
            )
            self._ensure_column(
                table="agent_tasks",
                column="effective_skill_names_json",
                definition="TEXT NOT NULL DEFAULT '[]'",
            )
            self._ensure_column(
                table="agent_tasks",
                column="skill_view_path",
                definition="TEXT",
            )
            self._ensure_column(
                table="agent_tasks",
                column="skill_view_hash",
                definition="TEXT",
            )
            self._ensure_column(
                table="agent_tasks",
                column="artifacts_json",
                definition="TEXT NOT NULL DEFAULT '[]'",
            )
            self._backfill_pending_reviews_locked()
            self._conn.commit()

    def _backfill_pending_reviews_locked(self) -> None:
        """Upgrade legacy waiting Tasks and rebuild root compatibility views."""

        rows = self._conn.execute(
            """
            SELECT task_id, root_task_id, pending_review_json, updated_at
            FROM agent_tasks
            WHERE state = 'waiting_for_human' AND pending_review_json IS NOT NULL
            """
        ).fetchall()
        for task_id, root_task_id, payload_json, updated_at in rows:
            try:
                payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict):
                continue
            review_id = payload.get("review_id")
            if not isinstance(review_id, str) or not review_id:
                continue
            self._conn.execute(
                """
                INSERT OR IGNORE INTO agent_task_pending_reviews (
                    review_id,
                    task_id,
                    root_task_id,
                    payload_json,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    task_id,
                    root_task_id,
                    json.dumps(payload, ensure_ascii=True, sort_keys=True),
                    updated_at,
                    updated_at,
                ),
            )

        review_roots = {
            str(row[0])
            for row in self._conn.execute(
                "SELECT DISTINCT root_task_id FROM agent_task_pending_reviews"
            ).fetchall()
        }
        for root_task_id in review_roots:
            selected = self._conn.execute(
                """
                SELECT task_id, payload_json
                FROM agent_task_pending_reviews
                WHERE root_task_id = ?
                ORDER BY created_at ASC, review_id ASC
                LIMIT 1
                """,
                (root_task_id,),
            ).fetchone()
            if selected is None:
                continue
            task_id, payload_json = selected
            payload = json.loads(payload_json)
            if task_id != root_task_id:
                payload["source_task_id"] = task_id
            self._conn.execute(
                "UPDATE agent_tasks SET pending_review_json = ? WHERE task_id = ?",
                (
                    json.dumps(payload, ensure_ascii=True, sort_keys=True),
                    root_task_id,
                ),
            )

        root_rows = self._conn.execute(
            """
            SELECT task_id, state, pending_review_json
            FROM agent_tasks
            WHERE task_id = root_task_id AND pending_review_json IS NOT NULL
            """
        ).fetchall()
        for task_id, state, payload_json in root_rows:
            if task_id in review_roots or state == "waiting_for_human":
                continue
            try:
                payload = json.loads(payload_json)
            except (TypeError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict) and "source_task_id" in payload:
                self._conn.execute(
                    "UPDATE agent_tasks SET pending_review_json = NULL WHERE task_id = ?",
                    (task_id,),
                )

    def _update_task_locked(self, record: TaskRecord) -> None:
        columns = self._write_columns()
        assignments = ", ".join(f"{column} = ?" for column in columns[1:])
        values = self._record_values(record)
        cursor = self._conn.execute(
            f"UPDATE agent_tasks SET {assignments} WHERE task_id = ?",
            (*values[1:], record.task_id),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"Task does not exist: {record.task_id}")

    def _task_exists_locked(self, task_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM agent_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        return row is not None

    def _begin_write_locked(self) -> None:
        """Acquire SQLite's writer reservation before reading a Task budget."""

        self._conn.execute("BEGIN IMMEDIATE")

    def _ensure_new_task_identity_locked(self, task_id: str) -> None:
        row = self._conn.execute(
            f"""
            SELECT
                {self._select_columns()}
            FROM agent_tasks
            WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        if row is not None:
            raise StoredTaskAlreadyExistsError(self._row_to_task_record(row))

    def _enforce_root_budget_locked(self, record: TaskRecord) -> None:
        """Check a tree's durable count inside the Task insertion transaction."""

        candidate_limit = record.delegation_max_tasks_per_root
        if candidate_limit is None:
            return
        if candidate_limit < 1:
            raise ValueError("delegation_max_tasks_per_root must be at least 1")
        stored_limit_row = self._conn.execute(
            """
            SELECT MIN(delegation_max_tasks_per_root)
            FROM agent_tasks
            WHERE root_task_id = ?
              AND delegation_max_tasks_per_root IS NOT NULL
            """,
            (record.root_task_id,),
        ).fetchone()
        stored_limit = (
            int(stored_limit_row[0])
            if stored_limit_row is not None and stored_limit_row[0] is not None
            else candidate_limit
        )
        effective_limit = min(candidate_limit, stored_limit)
        count_row = self._conn.execute(
            "SELECT COUNT(*) FROM agent_tasks WHERE root_task_id = ?",
            (record.root_task_id,),
        ).fetchone()
        assert count_row is not None
        current_count = int(count_row[0])
        if current_count >= effective_limit:
            raise TaskRootBudgetExceededError(
                root_task_id=record.root_task_id,
                current_count=current_count,
                max_tasks_per_root=effective_limit,
            )

    def _append_task_event_locked(
        self,
        *,
        task_id: str,
        run_count: int,
        event_type: str,
        encoded_data: str,
        event_created_at: datetime,
    ) -> StoredTaskEvent:
        cursor = self._conn.execute(
            """
            INSERT INTO agent_task_events (
                task_id,
                run_count,
                event_type,
                created_at,
                data_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                task_id,
                run_count,
                event_type,
                _serialize_datetime(event_created_at),
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

    def _latest_task_event_row_locked(
        self,
        *,
        task_id: str,
        run_count: int,
    ) -> tuple[Any, ...] | None:
        return self._conn.execute(
            """
            SELECT event_id, task_id, run_count, event_type, created_at, data_json
            FROM agent_task_events
            WHERE task_id = ? AND run_count = ?
            ORDER BY event_id DESC
            LIMIT 1
            """,
            (task_id, run_count),
        ).fetchone()

    def _ensure_column(self, *, table: str, column: str, definition: str) -> None:
        columns = {
            row[1]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _insert_sql(self, *, upsert: bool) -> str:
        columns = self._write_columns()
        column_list = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        sql = f"INSERT INTO agent_tasks ({column_list}) VALUES ({placeholders})"
        if not upsert:
            return sql
        assignments = ", ".join(
            f"{column} = excluded.{column}" for column in columns[1:]
        )
        return f"{sql} ON CONFLICT(task_id) DO UPDATE SET {assignments}"

    def _write_columns(self) -> tuple[str, ...]:
        return (
            "task_id",
            "agent_name",
            "state",
            "thread_id",
            "parent_task_id",
            "root_task_id",
            "depth",
            "created_at",
            "updated_at",
            "result",
            "error",
            "run_count",
            "route_kind",
            "upstream_task_id",
            "parent_thread_id",
            "mailbox_suppressed",
            "mailbox_delivered",
            "webhook_json",
            "delegation_root_id",
            "delegation_max_depth",
            "delegation_max_tasks_per_root",
            "delegation_visited_nodes_json",
            "permission_profile",
            "effective_skill_names_json",
            "skill_view_path",
            "skill_view_hash",
            "pending_review_json",
            "artifacts_json",
        )

    def _record_values(self, record: TaskRecord) -> tuple[Any, ...]:
        return (
            record.task_id,
            record.agent_name,
            record.state,
            record.thread_id,
            record.parent_task_id,
            record.root_task_id,
            record.depth,
            _serialize_datetime(record.created_at),
            _serialize_datetime(record.updated_at),
            record.result,
            record.error,
            record.run_count,
            record.route_kind,
            record.upstream_task_id,
            record.parent_thread_id,
            int(record.mailbox_suppressed),
            int(record.mailbox_delivered),
            (
                json.dumps(record.webhook, ensure_ascii=True, sort_keys=True)
                if record.webhook is not None
                else None
            ),
            record.delegation_root_id,
            record.delegation_max_depth,
            record.delegation_max_tasks_per_root,
            json.dumps(
                list(record.delegation_visited_nodes),
                ensure_ascii=True,
            ),
            record.permission_profile,
            json.dumps(
                list(record.effective_skill_names),
                ensure_ascii=True,
            ),
            record.skill_view_path,
            record.skill_view_hash,
            (
                json.dumps(
                    record.pending_review,
                    ensure_ascii=True,
                    sort_keys=True,
                )
                if record.pending_review is not None
                else None
            ),
            json.dumps(
                [_artifact_to_dict(item) for item in record.artifacts],
                ensure_ascii=True,
                sort_keys=True,
            ),
        )

    def _select_columns(self) -> str:
        return """
                    task_id,
                    agent_name,
                    state,
                    thread_id,
                    parent_task_id,
                    root_task_id,
                    depth,
                    created_at,
                    updated_at,
                    result,
                    error,
                    run_count,
                    route_kind,
                    upstream_task_id,
                    parent_thread_id,
                    mailbox_suppressed,
                    mailbox_delivered,
                    webhook_json,
                    delegation_root_id,
                    delegation_max_depth,
                    delegation_max_tasks_per_root,
                    delegation_visited_nodes_json,
                    permission_profile,
                    effective_skill_names_json,
                    skill_view_path,
                    skill_view_hash,
                    pending_review_json,
                    artifacts_json
        """

    def _row_to_task_record(self, row: tuple[Any, ...]) -> TaskRecord:
        webhook_json = row[17]
        webhook = json.loads(webhook_json) if webhook_json else None
        if not isinstance(webhook, dict):
            webhook = None

        visited_nodes_json = row[21]
        visited_nodes_raw = json.loads(visited_nodes_json) if visited_nodes_json else []
        visited_nodes = tuple(
            item for item in visited_nodes_raw if isinstance(item, str)
        )

        return TaskRecord(
            task_id=row[0],
            agent_name=row[1],
            state=row[2],
            thread_id=row[3],
            parent_task_id=row[4],
            root_task_id=row[5],
            depth=row[6],
            created_at=_parse_datetime(row[7]),
            updated_at=_parse_datetime(row[8]),
            result=row[9],
            error=row[10],
            run_count=row[11],
            route_kind=row[12],
            upstream_task_id=row[13],
            parent_thread_id=row[14],
            mailbox_suppressed=bool(row[15]),
            mailbox_delivered=bool(row[16]),
            webhook=webhook,
            delegation_root_id=row[18],
            delegation_max_depth=row[19],
            delegation_max_tasks_per_root=row[20],
            delegation_visited_nodes=visited_nodes,
            permission_profile=row[22],
            effective_skill_names=tuple(
                item
                for item in (json.loads(row[23]) if row[23] else [])
                if isinstance(item, str)
            ),
            skill_view_path=row[24],
            skill_view_hash=row[25],
            pending_review=json.loads(row[26]) if row[26] else None,
            artifacts=_parse_artifacts(row[27]),
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
