from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class MailboxStore:
    """SQLite-backed durable storage for task mailbox messages."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        if db_path != ":memory:" and not db_path.startswith("file:"):
            Path(db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._owner_id = f"mailbox-owner-{uuid.uuid4().hex}"
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
            uri=db_path.startswith("file:"),
        )
        self._conn.row_factory = sqlite3.Row
        self._init_db()

    def publish(self, values: dict[str, Any]) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                """
                INSERT OR IGNORE INTO agent_mailbox_messages (
                    message_id, idempotency_key, recipient_task_id,
                    recipient_thread_id, sender_task_id, sender_agent_name,
                    child_task_id, child_agent_name, child_run_count,
                    settled_status, content, trigger_run, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    values["message_id"],
                    values.get("idempotency_key"),
                    values.get("recipient_task_id"),
                    values["recipient_thread_id"],
                    values.get("sender_task_id"),
                    values.get("sender_agent_name"),
                    values.get("child_task_id"),
                    values.get("child_agent_name"),
                    values.get("child_run_count"),
                    values.get("settled_status"),
                    values["content"],
                    int(values.get("trigger_run", True)),
                    values["created_at"].isoformat(),
                ),
            )
            self._conn.commit()
            return cursor.rowcount == 1

    def claim(
        self,
        *,
        recipient_task_id: str | None,
        recipient_thread_id: str,
        lease_seconds: int = 300,
    ) -> list[dict[str, Any]]:
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=lease_seconds)
        claim_token = uuid.uuid4().hex
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._release_expired_claims_locked(now, commit=False)
                if recipient_task_id:
                    rows = self._conn.execute(
                    """
                    SELECT * FROM agent_mailbox_messages
                    WHERE status = 'pending'
                      AND (recipient_task_id = ? OR (
                           recipient_task_id IS NULL AND recipient_thread_id = ?))
                    ORDER BY created_at, message_id
                    """,
                    (recipient_task_id, recipient_thread_id),
                    ).fetchall()
                else:
                    rows = self._conn.execute(
                    """
                    SELECT * FROM agent_mailbox_messages
                    WHERE status = 'pending' AND recipient_thread_id = ?
                    ORDER BY created_at, message_id
                    """,
                    (recipient_thread_id,),
                    ).fetchall()
                ids = [row["message_id"] for row in rows]
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    self._conn.execute(
                    f"""
                    UPDATE agent_mailbox_messages
                    SET status = 'claimed', claimed_at = ?, claim_expires_at = ?,
                        claimed_by = ?, claim_token = ?
                    WHERE message_id IN ({placeholders}) AND status = 'pending'
                    """,
                        (
                            now.isoformat(),
                            expires_at.isoformat(),
                            self._owner_id,
                            claim_token,
                            *ids,
                        ),
                    )
                claimed_rows = self._conn.execute(
                    """
                    SELECT * FROM agent_mailbox_messages
                    WHERE claimed_by = ? AND claim_token = ?
                    ORDER BY created_at, message_id
                    """,
                    (self._owner_id, claim_token),
                ).fetchall()
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
            return [dict(row) for row in claimed_rows]

    def acknowledge(self, message_ids: list[str]) -> None:
        if not message_ids:
            return
        with self._lock:
            placeholders = ",".join("?" for _ in message_ids)
            self._conn.execute(
                f"""
                UPDATE agent_mailbox_messages
                SET status = 'delivered', delivered_at = ?, claim_expires_at = NULL,
                    claim_token = NULL
                WHERE message_id IN ({placeholders}) AND status = 'claimed'
                  AND claimed_by = ?
                """,
                (datetime.now(UTC).isoformat(), *message_ids, self._owner_id),
            )
            self._conn.commit()

    def acknowledge_task(
        self,
        recipient_task_id: str,
        recipient_thread_id: str,
    ) -> None:
        """Acknowledge this runtime's claims after graph execution is durable."""
        with self._lock:
            self._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET status = 'delivered', delivered_at = ?, claim_expires_at = NULL,
                    claim_token = NULL
                WHERE status = 'claimed' AND claimed_by = ?
                  AND (recipient_task_id = ? OR (
                       recipient_task_id IS NULL AND recipient_thread_id = ?))
                """,
                (
                    datetime.now(UTC).isoformat(),
                    self._owner_id,
                    recipient_task_id,
                    recipient_thread_id,
                ),
            )
            self._conn.commit()

    def retract_settled(
        self,
        *,
        recipient_thread_id: str,
        child_task_id: str,
        child_run_count: int,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET status = 'retracted', claim_expires_at = NULL
                WHERE recipient_thread_id = ? AND child_task_id = ?
                  AND child_run_count = ? AND status IN ('pending', 'claimed')
                """,
                (recipient_thread_id, child_task_id, child_run_count),
            )
            self._conn.commit()

    def has_triggering(self, recipient_task_id: str) -> bool:
        now = datetime.now(UTC)
        with self._lock:
            self._release_expired_claims_locked(now)
            row = self._conn.execute(
                """
                SELECT 1 FROM agent_mailbox_messages
                WHERE recipient_task_id = ? AND trigger_run = 1
                  AND status = 'pending'
                LIMIT 1
                """,
                (recipient_task_id,),
            ).fetchone()
            return row is not None

    def recover_claims(self) -> None:
        """Release only expired claims; live replicas retain their leases."""
        with self._lock:
            now = datetime.now(UTC)
            self._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET claim_expires_at = ?
                WHERE status = 'claimed' AND claimed_by = ?
                """,
                ((now + timedelta(seconds=300)).isoformat(), self._owner_id),
            )
            self._release_expired_claims_locked(now)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _release_expired_claims_locked(
        self,
        now: datetime,
        *,
        commit: bool = True,
    ) -> None:
        self._conn.execute(
            """
            UPDATE agent_mailbox_messages
            SET status = 'pending', claimed_at = NULL, claim_expires_at = NULL,
                claimed_by = NULL, claim_token = NULL
            WHERE status = 'claimed' AND claim_expires_at <= ?
            """,
            (now.isoformat(),),
        )
        if commit:
            self._conn.commit()

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS agent_mailbox_messages (
                    message_id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE,
                    recipient_task_id TEXT,
                    recipient_thread_id TEXT NOT NULL,
                    sender_task_id TEXT,
                    sender_agent_name TEXT,
                    child_task_id TEXT,
                    child_agent_name TEXT,
                    child_run_count INTEGER,
                    settled_status TEXT,
                    content TEXT NOT NULL,
                    trigger_run INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    claimed_at TEXT,
                    claim_expires_at TEXT,
                    delivered_at TEXT
                )
                """
            )
            self._ensure_column("claimed_by", "TEXT")
            self._ensure_column("claim_token", "TEXT")
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_mailbox_pending_recipient
                ON agent_mailbox_messages (
                    recipient_task_id, status, trigger_run, created_at
                )
                """
            )
            self._conn.commit()

    def _ensure_column(self, column: str, definition: str) -> None:
        columns = {
            row[1]
            for row in self._conn.execute(
                "PRAGMA table_info(agent_mailbox_messages)"
            ).fetchall()
        }
        if column not in columns:
            self._conn.execute(
                f"ALTER TABLE agent_mailbox_messages ADD COLUMN {column} {definition}"
            )
