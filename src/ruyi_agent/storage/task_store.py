from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ruyi_agent.runtime.delegation.async_runtime import TaskRecord


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def _parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


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
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO agent_tasks (
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
                    pending_review_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
                    (
                        json.dumps(
                            record.pending_review,
                            ensure_ascii=True,
                            sort_keys=True,
                        )
                        if record.pending_review is not None
                        else None
                    ),
                ),
            )
            self._conn.commit()

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

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _ensure_parent_dir(self) -> None:
        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
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
                    pending_review_json TEXT
                )
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
            self._conn.commit()

    def _ensure_column(self, *, table: str, column: str, definition: str) -> None:
        columns = {
            row[1]
            for row in self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                    pending_review_json
        """

    def _row_to_task_record(self, row: tuple[Any, ...]) -> TaskRecord:
        from ruyi_agent.runtime.delegation.async_runtime import TaskRecord

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
            active_run=None,
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
            pending_review=json.loads(row[23]) if row[23] else None,
        )


def task_record_for_restart(record: TaskRecord) -> TaskRecord:
    if record.route_kind == "local" and record.state in {"pending", "running"}:
        return replace(
            record,
            state="interrupted",
            active_run=None,
            updated_at=datetime.now(UTC),
            error=record.error or "Task interrupted: local process restarted.",
        )
    return replace(record, active_run=None)
