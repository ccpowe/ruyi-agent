from __future__ import annotations

import sqlite3
import threading
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4


ReceiptKey = str | int
ReceiptClaimStatus = Literal["claimed", "processed", "busy"]
SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ChannelEventReceiptSchema:
    """Existing platform table shape consumed by the shared lease store."""

    table_name: str
    key_column: str
    key_sql_type: Literal["TEXT", "INTEGER"]

    def __post_init__(self) -> None:
        for identifier in (self.table_name, self.key_column):
            if SQL_IDENTIFIER_PATTERN.fullmatch(identifier) is None:
                raise ValueError(f"Invalid SQLite identifier: {identifier!r}")


@dataclass(frozen=True, slots=True)
class ChannelEventReceipt:
    event_key: ReceiptKey
    channel_id: str
    message_id: str


@dataclass(frozen=True, slots=True)
class ChannelEventReceiptClaim:
    status: ReceiptClaimStatus
    event_key: ReceiptKey
    claimed_at: str | None = None
    claim_token: str | None = None


class ChannelEventReceiptStore:
    """Durable ownership leases for idempotent platform events.

    Platform adapters provide only their stable event key and the legacy table
    schema. Claim ownership, expiry, fencing, migration, and SQLite concurrency
    are deliberately shared so the two channels cannot drift independently.
    """

    def __init__(
        self,
        db_path: str,
        *,
        schema: ChannelEventReceiptSchema,
        claim_timeout_seconds: float = 300.0,
    ) -> None:
        self._db_path = db_path
        self._schema = schema
        self._claim_timeout_seconds = max(0.0, claim_timeout_seconds)
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self._init_db()

    def claim(
        self,
        receipt: ChannelEventReceipt,
        *,
        now: datetime | None = None,
    ) -> ChannelEventReceiptClaim:
        now_dt = now or datetime.now(UTC)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=UTC)
        now_text = now_dt.isoformat()
        new_claim_token = uuid4().hex
        table = self._schema.table_name
        key_column = self._schema.key_column
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    f"""
                    SELECT processed_at, claimed_at, claim_token
                    FROM {table}
                    WHERE {key_column} = ?
                    """,
                    (receipt.event_key,),
                ).fetchone()
                if row is None:
                    self._conn.execute(
                        f"""
                        INSERT INTO {table} (
                            {key_column},
                            chat_id,
                            message_id,
                            first_seen_at,
                            claimed_at,
                            claim_token,
                            processed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, NULL)
                        """,
                        (
                            receipt.event_key,
                            receipt.channel_id,
                            receipt.message_id,
                            now_text,
                            now_text,
                            new_claim_token,
                        ),
                    )
                    self._conn.commit()
                    return ChannelEventReceiptClaim(
                        status="claimed",
                        event_key=receipt.event_key,
                        claimed_at=now_text,
                        claim_token=new_claim_token,
                    )

                processed_at, claimed_at, claim_token = row
                if processed_at:
                    self._conn.commit()
                    return ChannelEventReceiptClaim(
                        status="processed",
                        event_key=receipt.event_key,
                    )
                if claim_token and not self._claim_expired(claimed_at, now_dt):
                    self._conn.commit()
                    return ChannelEventReceiptClaim(
                        status="busy",
                        event_key=receipt.event_key,
                    )

                self._conn.execute(
                    f"""
                    UPDATE {table}
                    SET chat_id = ?, message_id = ?, claimed_at = ?, claim_token = ?
                    WHERE {key_column} = ? AND processed_at IS NULL
                    """,
                    (
                        receipt.channel_id,
                        receipt.message_id,
                        now_text,
                        new_claim_token,
                        receipt.event_key,
                    ),
                )
                self._conn.commit()
                return ChannelEventReceiptClaim(
                    status="claimed",
                    event_key=receipt.event_key,
                    claimed_at=now_text,
                    claim_token=new_claim_token,
                )
            except BaseException:
                self._conn.rollback()
                raise

    def mark_processed(
        self,
        event_key: ReceiptKey,
        *,
        claim_token: str,
        now: datetime | None = None,
    ) -> bool:
        now_dt = now or datetime.now(UTC)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=UTC)
        table = self._schema.table_name
        key_column = self._schema.key_column
        with self._lock:
            cursor = self._conn.execute(
                f"""
                UPDATE {table}
                SET claimed_at = NULL, claim_token = NULL, processed_at = ?
                WHERE {key_column} = ?
                    AND claim_token = ?
                    AND processed_at IS NULL
                """,
                (now_dt.isoformat(), event_key, claim_token),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def release(self, event_key: ReceiptKey, *, claim_token: str) -> bool:
        table = self._schema.table_name
        key_column = self._schema.key_column
        with self._lock:
            cursor = self._conn.execute(
                f"""
                UPDATE {table}
                SET claimed_at = NULL, claim_token = NULL
                WHERE {key_column} = ?
                    AND claim_token = ?
                    AND processed_at IS NULL
                """,
                (event_key, claim_token),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:":
            return
        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        table = self._schema.table_name
        key_column = self._schema.key_column
        key_sql_type = self._schema.key_sql_type
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {key_column} {key_sql_type} PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    claimed_at TEXT,
                    claim_token TEXT,
                    processed_at TEXT
                )
                """
            )
            columns = {
                row[1]
                for row in self._conn.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            if "claimed_at" not in columns:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN claimed_at TEXT")
            if "claim_token" not in columns:
                self._conn.execute(f"ALTER TABLE {table} ADD COLUMN claim_token TEXT")
            self._conn.commit()

    def _claim_expired(self, claimed_at: str | None, now: datetime) -> bool:
        if not claimed_at:
            return True
        try:
            claimed_at_dt = datetime.fromisoformat(claimed_at)
        except ValueError:
            return True
        if claimed_at_dt.tzinfo is None:
            claimed_at_dt = claimed_at_dt.replace(tzinfo=UTC)
        return (now - claimed_at_dt).total_seconds() >= self._claim_timeout_seconds

    def close(self) -> None:
        with self._lock:
            self._conn.close()
