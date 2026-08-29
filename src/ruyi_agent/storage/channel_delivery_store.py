from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from asyncio import to_thread
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


FINAL_DELIVERY_STATES = {"delivered", "superseded"}
RECOVERABLE_DELIVERY_STATES = {
    "watching",
    "retry_wait",
    "error",
    "delivering",
    "review_waiting",
    "terminal_grace",
}


@dataclass(frozen=True, slots=True)
class ChannelDeliveryIntent:
    intent_id: str
    platform: str
    session_key: str
    chat_id: str
    task_id: str
    run_count: int
    delivery_kind: str
    review_id: str | None
    state: str
    cursor: int
    attempt_count: int
    next_attempt_at: float | None
    last_error: str | None
    lease_owner: str | None
    lease_token: str | None
    lease_until: float | None
    fence: int
    created_at: float
    updated_at: float


def delivery_intent_id(
    *,
    platform: str,
    session_key: str,
    task_id: str,
    run_count: int,
) -> str:
    return f"{platform}\x1f{session_key}\x1f{task_id}\x1f{run_count}"


class ChannelDeliveryStore:
    """Durable Task Watch and channel-delivery ledger with fenced leases."""

    def __init__(
        self,
        db_path: str,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._db_path = db_path
        self._clock = clock
        self._lock = threading.RLock()
        self._ensure_parent_dir()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, timeout=30.0)
        self._init_db()

    def ensure_watch(
        self,
        *,
        platform: str,
        session_key: str,
        chat_id: str,
        task_id: str,
        run_count: int,
    ) -> ChannelDeliveryIntent:
        intent_id = delivery_intent_id(
            platform=platform,
            session_key=session_key,
            task_id=task_id,
            run_count=run_count,
        )
        now = self._clock()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO channel_delivery_intents (
                    intent_id, platform, session_key, chat_id, task_id, run_count,
                    delivery_kind, state, cursor, attempt_count, fence,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'watch', 'watching', 0, 0, 0, ?, ?)
                ON CONFLICT(intent_id) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    updated_at = excluded.updated_at
                """,
                (
                    intent_id,
                    platform,
                    session_key,
                    chat_id,
                    task_id,
                    run_count,
                    now,
                    now,
                ),
            )
            self._conn.commit()
        intent = self.get(intent_id)
        if intent is None:  # pragma: no cover - SQLite invariant
            raise RuntimeError(f"Channel delivery intent '{intent_id}' was not saved")
        return intent

    def get(self, intent_id: str) -> ChannelDeliveryIntent | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT intent_id, platform, session_key, chat_id, task_id,
                    run_count, delivery_kind, review_id, state, cursor,
                    attempt_count, next_attempt_at, last_error, lease_owner,
                    lease_token, lease_until, fence, created_at, updated_at
                FROM channel_delivery_intents WHERE intent_id = ?
                """,
                (intent_id,),
            ).fetchone()
        return self._row(row) if row is not None else None

    def list_recoverable(self, *, platform: str) -> list[ChannelDeliveryIntent]:
        placeholders = ", ".join("?" for _ in RECOVERABLE_DELIVERY_STATES)
        values = (platform, *sorted(RECOVERABLE_DELIVERY_STATES))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT intent_id, platform, session_key, chat_id, task_id,
                    run_count, delivery_kind, review_id, state, cursor,
                    attempt_count, next_attempt_at, last_error, lease_owner,
                    lease_token, lease_until, fence, created_at, updated_at
                FROM channel_delivery_intents
                WHERE platform = ? AND state IN ({placeholders})
                ORDER BY created_at, intent_id
                """,
                values,
            ).fetchall()
        return [self._row(row) for row in rows]

    async def alist_recoverable(self, *, platform: str) -> list[ChannelDeliveryIntent]:
        return await to_thread(self.list_recoverable, platform=platform)

    def claim(
        self,
        intent_id: str,
        *,
        owner: str,
        lease_seconds: float,
    ) -> str | None:
        if lease_seconds <= 0:
            raise ValueError("delivery lease seconds must be positive")
        now = self._clock()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT state, lease_owner, lease_until, fence
                    FROM channel_delivery_intents WHERE intent_id = ?
                    """,
                    (intent_id,),
                ).fetchone()
                if row is None or row[0] in FINAL_DELIVERY_STATES:
                    self._conn.rollback()
                    return None
                lease_owner = row[1]
                lease_until = row[2]
                if (
                    lease_owner is not None
                    and lease_owner != owner
                    and lease_until is not None
                    and float(lease_until) > now
                ):
                    self._conn.rollback()
                    return None
                fence = int(row[3]) + 1
                token = f"{owner}:{fence}:{uuid.uuid4().hex}"
                self._conn.execute(
                    """
                    UPDATE channel_delivery_intents
                    SET lease_owner = ?, lease_token = ?, lease_until = ?,
                        fence = ?, updated_at = ?
                    WHERE intent_id = ?
                    """,
                    (owner, token, now + lease_seconds, fence, now, intent_id),
                )
                self._conn.commit()
                return token
            except BaseException:
                self._conn.rollback()
                raise

    def renew(
        self,
        intent_id: str,
        *,
        token: str,
        lease_seconds: float,
    ) -> bool:
        now = self._clock()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE channel_delivery_intents
                SET lease_until = ?, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                """,
                (now + lease_seconds, now, intent_id, token, now),
            )
            self._conn.commit()
        return cursor.rowcount == 1

    def mark_retry(
        self,
        intent_id: str,
        *,
        token: str,
        attempt: int,
        delay: float,
        error: str,
    ) -> bool:
        now = self._clock()
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "state = 'retry_wait', attempt_count = ?, cursor = cursor + 1, "
                "next_attempt_at = ?, last_error = ?, updated_at = ?"
            ),
            values=(attempt, now + max(0.0, delay), error, now),
        )

    def mark_watching(self, intent_id: str, *, token: str) -> bool:
        now = self._clock()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE channel_delivery_intents
                SET state = CASE
                        WHEN state IN ('review_waiting', 'terminal_grace') THEN state
                        ELSE 'watching'
                    END,
                    attempt_count = 0,
                    next_attempt_at = NULL, last_error = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state NOT IN ('delivered', 'superseded')
                """,
                (now, intent_id, token, now),
            )
            self._conn.commit()
        return cursor.rowcount == 1

    def mark_delivering(
        self,
        intent_id: str,
        *,
        token: str,
        delivery_kind: str,
        review_id: str | None,
    ) -> bool:
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "delivery_kind = ?, review_id = ?, state = 'delivering', "
                "attempt_count = 0, next_attempt_at = NULL, last_error = NULL, "
                "updated_at = ?"
            ),
            values=(delivery_kind, review_id, self._clock()),
        )

    def mark_error(self, intent_id: str, *, token: str, error: str) -> bool:
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "state = 'error', next_attempt_at = NULL, last_error = ?, "
                "lease_owner = NULL, lease_token = NULL, lease_until = NULL, "
                "updated_at = ?"
            ),
            values=(error, self._clock()),
        )

    def mark_review_waiting(self, intent_id: str, *, token: str) -> bool:
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "state = 'review_waiting', attempt_count = 0, "
                "next_attempt_at = NULL, last_error = NULL, updated_at = ?"
            ),
            values=(self._clock(),),
        )

    def mark_terminal_steps_done(self, intent_id: str, *, token: str) -> bool:
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "state = 'terminal_grace', attempt_count = 0, "
                "next_attempt_at = NULL, last_error = NULL, updated_at = ?"
            ),
            values=(self._clock(),),
        )

    def mark_delivered(self, intent_id: str, *, token: str) -> bool:
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "state = 'delivered', attempt_count = 0, next_attempt_at = NULL, "
                "last_error = NULL, updated_at = ?"
            ),
            values=(self._clock(),),
        )

    def mark_superseded(self, intent_id: str, *, token: str) -> bool:
        return self._owned_update(
            intent_id,
            token=token,
            assignments=(
                "state = 'superseded', lease_owner = NULL, lease_token = NULL, "
                "lease_until = NULL, updated_at = ?"
            ),
            values=(self._clock(),),
        )

    def step_delivered(self, intent_id: str, *, step_key: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1 FROM channel_delivery_steps
                WHERE intent_id = ? AND step_key = ?
                """,
                (intent_id, step_key),
            ).fetchone()
        return row is not None

    def mark_step_delivered(
        self,
        intent_id: str,
        *,
        token: str,
        step_key: str,
    ) -> bool:
        now = self._clock()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                owned = self._conn.execute(
                    """
                    SELECT 1 FROM channel_delivery_intents
                    WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    """,
                    (intent_id, token, now),
                ).fetchone()
                if owned is None:
                    self._conn.rollback()
                    return False
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO channel_delivery_steps (
                        intent_id, step_key, delivered_at
                    ) VALUES (?, ?, ?)
                    """,
                    (intent_id, step_key, now),
                )
                self._conn.execute(
                    """
                    UPDATE channel_delivery_intents
                    SET cursor = cursor + 1, updated_at = ?
                    WHERE intent_id = ? AND lease_token = ?
                    """,
                    (now, intent_id, token),
                )
                self._conn.commit()
                return True
            except BaseException:
                self._conn.rollback()
                raise

    def release(self, intent_id: str, *, token: str) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE channel_delivery_intents
                SET lease_owner = NULL, lease_token = NULL, lease_until = NULL,
                    updated_at = ?
                WHERE intent_id = ? AND lease_token = ?
                """,
                (self._clock(), intent_id, token),
            )
            self._conn.commit()
        return cursor.rowcount == 1

    def release_owner(self, owner: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE channel_delivery_intents
                SET lease_owner = NULL, lease_token = NULL, lease_until = NULL,
                    updated_at = ?
                WHERE lease_owner = ?
                """,
                (self._clock(), owner),
            )
            self._conn.commit()

    def _owned_update(
        self,
        intent_id: str,
        *,
        token: str,
        assignments: str,
        values: tuple[object, ...],
    ) -> bool:
        now = self._clock()
        with self._lock:
            cursor = self._conn.execute(
                f"""
                UPDATE channel_delivery_intents SET {assignments}
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                """,
                (*values, intent_id, token, now),
            )
            self._conn.commit()
        return cursor.rowcount == 1

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:":
            return
        Path(self._db_path).expanduser().resolve().parent.mkdir(
            parents=True, exist_ok=True
        )

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_delivery_intents (
                    intent_id TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    session_key TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    run_count INTEGER NOT NULL CHECK (run_count >= 0),
                    delivery_kind TEXT NOT NULL,
                    review_id TEXT,
                    state TEXT NOT NULL,
                    cursor INTEGER NOT NULL DEFAULT 0,
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL,
                    last_error TEXT,
                    lease_owner TEXT,
                    lease_token TEXT,
                    lease_until REAL,
                    fence INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(platform, session_key, task_id, run_count)
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_channel_delivery_recovery
                ON channel_delivery_intents(platform, state, next_attempt_at)
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_delivery_steps (
                    intent_id TEXT NOT NULL,
                    step_key TEXT NOT NULL,
                    delivered_at REAL NOT NULL,
                    PRIMARY KEY(intent_id, step_key),
                    FOREIGN KEY(intent_id) REFERENCES channel_delivery_intents(intent_id)
                        ON DELETE CASCADE
                )
                """
            )
            self._conn.commit()

    @staticmethod
    def _row(row: tuple[object, ...]) -> ChannelDeliveryIntent:
        return ChannelDeliveryIntent(
            intent_id=str(row[0]),
            platform=str(row[1]),
            session_key=str(row[2]),
            chat_id=str(row[3]),
            task_id=str(row[4]),
            run_count=int(row[5]),
            delivery_kind=str(row[6]),
            review_id=str(row[7]) if row[7] is not None else None,
            state=str(row[8]),
            cursor=int(row[9]),
            attempt_count=int(row[10]),
            next_attempt_at=float(row[11]) if row[11] is not None else None,
            last_error=str(row[12]) if row[12] is not None else None,
            lease_owner=str(row[13]) if row[13] is not None else None,
            lease_token=str(row[14]) if row[14] is not None else None,
            lease_until=float(row[15]) if row[15] is not None else None,
            fence=int(row[16]),
            created_at=float(row[17]),
            updated_at=float(row[18]),
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
