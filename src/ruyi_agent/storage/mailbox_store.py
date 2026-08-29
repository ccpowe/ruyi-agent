from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ruyi_agent.storage.settled_outbox import SettledOutboxIntent
from ruyi_agent.storage.task_schema import sanitize_legacy_remote_public_projections


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

    @property
    def db_path(self) -> str:
        return self._db_path

    def shares_database(self, db_path: str) -> bool:
        """Return whether another store addresses this durable SQLite file."""

        if self._db_path == ":memory:" or db_path == ":memory:":
            return False
        if self._db_path.startswith("file:") or db_path.startswith("file:"):
            return self._db_path == db_path
        return (
            Path(self._db_path).expanduser().resolve()
            == Path(db_path).expanduser().resolve()
        )

    def publish(self, values: dict[str, Any]) -> bool:
        with self._lock:
            try:
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
            except BaseException:
                self._conn.rollback()
                raise

    def publish_claimed_settled_outbox(
        self,
        intent: SettledOutboxIntent,
    ) -> bool:
        """Atomically publish and acknowledge one fenced settled intent."""

        if intent.claim_token is None:
            return False
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                claimed = self._conn.execute(
                    """
                    SELECT 1
                    FROM agent_task_settled_outbox AS outbox
                    WHERE outbox.outbox_key = ? AND outbox.status = 'claimed'
                      AND outbox.claim_token = ?
                      AND NOT EXISTS (
                          SELECT 1
                          FROM agent_tasks AS task
                          WHERE task.task_id = outbox.task_id
                            AND task.run_count = outbox.run_count
                            AND task.mailbox_suppressed = 1
                      )
                    """,
                    (intent.outbox_key, intent.claim_token),
                ).fetchone()
                if claimed is None:
                    self._conn.commit()
                    return False
                self._resolve_settled_message_locked(intent)
                delivered_at = datetime.now(UTC).isoformat()
                cursor = self._conn.execute(
                    """
                    UPDATE agent_task_settled_outbox
                    SET status = 'delivered', delivered_at = ?, claimed_by = NULL,
                        claim_token = NULL, claimed_at = NULL, claim_expires_at = NULL,
                        last_error = NULL
                    WHERE outbox_key = ? AND status = 'claimed' AND claim_token = ?
                    """,
                    (delivered_at, intent.outbox_key, intent.claim_token),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("Settled outbox lease was lost during delivery")
                self._conn.execute(
                    """
                    UPDATE agent_tasks
                    SET mailbox_delivered = 1
                    WHERE task_id = ? AND run_count = ? AND mailbox_suppressed = 0
                    """,
                    (intent.task_id, intent.run_count),
                )
                self._conn.commit()
                return True
            except BaseException:
                self._conn.rollback()
                raise

    def retract_settled_outbox(self, intent: SettledOutboxIntent) -> bool:
        """Atomically retract a suppressed message and confirm reconciliation."""

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                suppressed = self._conn.execute(
                    """
                    SELECT 1 FROM agent_task_settled_outbox
                    WHERE outbox_key = ? AND status = 'suppressed'
                      AND retracted_at IS NULL
                    """,
                    (intent.outbox_key,),
                ).fetchone()
                if suppressed is None:
                    self._conn.commit()
                    return False
                self._conn.execute(
                    """
                    UPDATE agent_mailbox_messages
                    SET status = 'retracted', claim_expires_at = NULL,
                        claimed_by = NULL, claim_token = NULL
                    WHERE idempotency_key = ? AND status IN ('pending', 'claimed')
                    """,
                    (intent.outbox_key,),
                )
                self._conn.execute(
                    """
                    UPDATE agent_task_settled_outbox
                    SET retracted_at = ?
                    WHERE outbox_key = ? AND status = 'suppressed'
                    """,
                    (datetime.now(UTC).isoformat(), intent.outbox_key),
                )
                self._conn.commit()
                return True
            except BaseException:
                self._conn.rollback()
                raise

    def settled_outbox_needs_wake(self, intent: SettledOutboxIntent) -> bool:
        """Return whether the adopted mailbox row still needs recipient work."""

        with self._lock:
            suppression = self._authoritative_suppression_clause_locked("message")
            row = self._conn.execute(
                f"""
                SELECT 1 FROM agent_mailbox_messages AS message
                WHERE message.idempotency_key = ? AND message.status = 'pending'
                  AND message.trigger_run = 1
                  {suppression}
                LIMIT 1
                """,
                (intent.outbox_key,),
            ).fetchone()
        return row is not None

    def list_pending_trigger_recipient_task_ids(self) -> list[str]:
        """List Task identities whose durable input still needs a wakeup."""

        now = datetime.now(UTC)
        with self._lock:
            self._release_expired_claims_locked(now)
            suppression = self._authoritative_suppression_clause_locked("message")
            rows = self._conn.execute(
                f"""
                SELECT DISTINCT message.recipient_task_id
                FROM agent_mailbox_messages AS message
                WHERE message.recipient_task_id IS NOT NULL
                  AND message.trigger_run = 1 AND message.status = 'pending'
                  {suppression}
                ORDER BY message.recipient_task_id
                """
            ).fetchall()
        return [str(row[0]) for row in rows]

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
                suppression = self._authoritative_suppression_clause_locked("message")
                if recipient_task_id:
                    rows = self._conn.execute(
                        f"""
                        SELECT message.* FROM agent_mailbox_messages AS message
                        WHERE message.status = 'pending'
                          AND (message.recipient_task_id = ? OR (
                               message.recipient_task_id IS NULL
                               AND message.recipient_thread_id = ?))
                          {suppression}
                        ORDER BY message.created_at, message.message_id
                        """,
                        (recipient_task_id, recipient_thread_id),
                    ).fetchall()
                else:
                    rows = self._conn.execute(
                        f"""
                        SELECT message.* FROM agent_mailbox_messages AS message
                        WHERE message.status = 'pending'
                          AND message.recipient_thread_id = ?
                          {suppression}
                        ORDER BY message.created_at, message.message_id
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
                SET status = 'retracted', claimed_at = NULL,
                    claim_expires_at = NULL, claimed_by = NULL, claim_token = NULL
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
            suppression = self._authoritative_suppression_clause_locked("message")
            row = self._conn.execute(
                f"""
                SELECT 1 FROM agent_mailbox_messages AS message
                WHERE message.recipient_task_id = ? AND message.trigger_run = 1
                  AND message.status = 'pending'
                  {suppression}
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
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_agent_mailbox_settled_run_status
                ON agent_mailbox_messages (
                    child_task_id, child_run_count, status, idempotency_key
                )
                """
            )
            sanitize_legacy_remote_public_projections(self._conn)
            self._conn.commit()

    def _resolve_settled_message_locked(self, intent: SettledOutboxIntent) -> None:
        """Bind a true legacy row or insert the deterministic mailbox identity."""

        keyed = self._conn.execute(
            "SELECT * FROM agent_mailbox_messages WHERE idempotency_key = ?",
            (intent.outbox_key,),
        ).fetchall()
        if keyed:
            if len(keyed) != 1:
                raise RuntimeError("Settled mailbox idempotency key is not unique")
            self._validate_settled_message_identity(keyed[0], intent)
            return

        legacy = self._conn.execute(
            """
            SELECT * FROM agent_mailbox_messages
            WHERE idempotency_key IS NULL
              AND recipient_thread_id = ?
              AND child_task_id = ? AND child_run_count = ?
              AND status IN ('pending', 'claimed', 'delivered')
            ORDER BY created_at, message_id
            """,
            (intent.recipient_thread_id, intent.task_id, intent.run_count),
        ).fetchall()
        if legacy:
            if len(legacy) != 1:
                raise RuntimeError(
                    "Multiple legacy mailbox rows conflict with one settled intent"
                )
            self._validate_settled_message_identity(legacy[0], intent)
            self._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET idempotency_key = ?
                WHERE message_id = ? AND idempotency_key IS NULL
                """,
                (intent.outbox_key, legacy[0]["message_id"]),
            )
            return

        try:
            self._conn.execute(
                """
                INSERT INTO agent_mailbox_messages (
                    message_id, idempotency_key, recipient_task_id,
                    recipient_thread_id, sender_task_id, sender_agent_name,
                    child_task_id, child_agent_name, child_run_count,
                    settled_status, content, trigger_run, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 'pending', ?)
                """,
                (
                    intent.message_id,
                    intent.outbox_key,
                    intent.recipient_task_id,
                    intent.recipient_thread_id,
                    intent.task_id,
                    intent.child_agent_name,
                    intent.task_id,
                    intent.child_agent_name,
                    intent.run_count,
                    intent.settled_status,
                    intent.content,
                    intent.created_at.isoformat(),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise RuntimeError(
                "Settled mailbox identity conflicts with an unrelated message"
            ) from exc

    @staticmethod
    def _validate_settled_message_identity(
        row: sqlite3.Row,
        intent: SettledOutboxIntent,
    ) -> None:
        stored = (
            row["recipient_task_id"],
            row["recipient_thread_id"],
            row["sender_task_id"],
            row["sender_agent_name"],
            row["child_task_id"],
            row["child_agent_name"],
            row["child_run_count"],
            row["settled_status"],
            row["content"],
            int(row["trigger_run"]),
        )
        expected = (
            intent.recipient_task_id,
            intent.recipient_thread_id,
            intent.task_id,
            intent.child_agent_name,
            intent.task_id,
            intent.child_agent_name,
            intent.run_count,
            intent.settled_status,
            intent.content,
            1,
        )
        if stored != expected or row["status"] not in {
            "pending",
            "claimed",
            "delivered",
        }:
            raise RuntimeError(
                "Settled mailbox logical identity conflicts with outbox intent"
            )

    def _authoritative_suppression_clause_locked(self, alias: str) -> str:
        tables = {
            str(row[0])
            for row in self._conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table'
                  AND name IN ('agent_tasks', 'agent_task_settled_outbox')
                """
            ).fetchall()
        }
        clauses: list[str] = []
        if "agent_tasks" in tables:
            clauses.append(
                f"""
                AND NOT EXISTS (
                    SELECT 1 FROM agent_tasks AS task
                    WHERE task.task_id = {alias}.child_task_id
                      AND task.run_count = {alias}.child_run_count
                      AND task.mailbox_suppressed = 1
                )
                """
            )
        if "agent_task_settled_outbox" in tables:
            clauses.append(
                f"""
                AND NOT EXISTS (
                    SELECT 1 FROM agent_task_settled_outbox AS suppressed_outbox
                    WHERE suppressed_outbox.task_id = {alias}.child_task_id
                      AND suppressed_outbox.run_count = {alias}.child_run_count
                      AND suppressed_outbox.status = 'suppressed'
                )
                """
            )
        return "\n".join(clauses)

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
