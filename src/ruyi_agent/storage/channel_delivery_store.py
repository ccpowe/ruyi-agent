from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from asyncio import to_thread
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeAlias, cast, get_args

from ruyi_agent.storage.task_database import (
    configure_connection_for_initialization,
    database_initialization_lock,
)


ChannelDeliveryKind: TypeAlias = Literal["watch", "review", "terminal"]
ChannelDeliveryState: TypeAlias = Literal[
    "watching",
    "retry_wait",
    "error",
    "delivering",
    "review_waiting",
    "terminal_grace",
    "delivered",
    "superseded",
]

DELIVERY_KINDS: tuple[ChannelDeliveryKind, ...] = get_args(ChannelDeliveryKind)
DELIVERY_STATES: tuple[ChannelDeliveryState, ...] = get_args(ChannelDeliveryState)
FINAL_DELIVERY_STATES: frozenset[ChannelDeliveryState] = frozenset(DELIVERY_STATES[-2:])
RECOVERABLE_DELIVERY_STATES: frozenset[ChannelDeliveryState] = frozenset(
    DELIVERY_STATES[:-2]
)

DELIVERY_KIND_WATCH: ChannelDeliveryKind = DELIVERY_KINDS[0]
DELIVERY_KIND_REVIEW: ChannelDeliveryKind = DELIVERY_KINDS[1]
DELIVERY_KIND_TERMINAL: ChannelDeliveryKind = DELIVERY_KINDS[2]
DELIVERY_STATE_WATCHING: ChannelDeliveryState = DELIVERY_STATES[0]
DELIVERY_STATE_RETRY_WAIT: ChannelDeliveryState = DELIVERY_STATES[1]
DELIVERY_STATE_ERROR: ChannelDeliveryState = DELIVERY_STATES[2]
DELIVERY_STATE_DELIVERING: ChannelDeliveryState = DELIVERY_STATES[3]
DELIVERY_STATE_REVIEW_WAITING: ChannelDeliveryState = DELIVERY_STATES[4]
DELIVERY_STATE_TERMINAL_GRACE: ChannelDeliveryState = DELIVERY_STATES[5]
DELIVERY_STATE_DELIVERED: ChannelDeliveryState = DELIVERY_STATES[6]
DELIVERY_STATE_SUPERSEDED: ChannelDeliveryState = DELIVERY_STATES[7]

DeliveryKind: TypeAlias = ChannelDeliveryKind
DeliveryState: TypeAlias = ChannelDeliveryState


def _parse_delivery_value(
    value: object,
    *,
    path: str,
    allowed: tuple[str, ...],
) -> str:
    if type(value) is not str or value not in allowed:
        allowed_text = ", ".join(repr(item) for item in allowed)
        raise ValueError(
            f"{path} has invalid value {value!r}; expected one of ({allowed_text})"
        )
    return value


def parse_channel_delivery_kind(
    value: object,
    *,
    path: str = "delivery_kind",
) -> ChannelDeliveryKind:
    return cast(
        ChannelDeliveryKind,
        _parse_delivery_value(value, path=path, allowed=DELIVERY_KINDS),
    )


def parse_channel_delivery_state(
    value: object,
    *,
    path: str = "state",
) -> ChannelDeliveryState:
    return cast(
        ChannelDeliveryState,
        _parse_delivery_value(value, path=path, allowed=DELIVERY_STATES),
    )


parse_delivery_kind = parse_channel_delivery_kind
parse_delivery_state = parse_channel_delivery_state


_LEGACY_INTENT_COLUMNS = (
    "intent_id",
    "platform",
    "session_key",
    "chat_id",
    "task_id",
    "run_count",
    "delivery_kind",
    "review_id",
    "state",
    "cursor",
    "attempt_count",
    "next_attempt_at",
    "last_error",
    "lease_owner",
    "lease_token",
    "lease_until",
    "fence",
    "created_at",
    "updated_at",
)
_CURRENT_INTENT_COLUMNS = (
    *_LEGACY_INTENT_COLUMNS[:11],
    "redrive_count",
    *_LEGACY_INTENT_COLUMNS[11:],
)
_INTENT_SELECT_COLUMNS = ", ".join(_CURRENT_INTENT_COLUMNS)
_KIND_CHECK_SQL = ", ".join(repr(value) for value in DELIVERY_KINDS)
_STATE_CHECK_SQL = ", ".join(repr(value) for value in DELIVERY_STATES)
_RECOVERABLE_STATE_VALUES = (
    DELIVERY_STATE_WATCHING,
    DELIVERY_STATE_RETRY_WAIT,
    DELIVERY_STATE_ERROR,
    DELIVERY_STATE_DELIVERING,
    DELIVERY_STATE_REVIEW_WAITING,
    DELIVERY_STATE_TERMINAL_GRACE,
)
_RECOVERABLE_STATE_SQL = ", ".join("?" for _ in _RECOVERABLE_STATE_VALUES)


@dataclass(frozen=True, slots=True)
class ChannelDeliveryIntent:
    intent_id: str
    platform: str
    session_key: str
    chat_id: str
    task_id: str
    run_count: int
    delivery_kind: ChannelDeliveryKind
    review_id: str | None
    state: ChannelDeliveryState
    cursor: int
    attempt_count: int
    redrive_count: int
    next_attempt_at: float | None
    last_error: str | None
    lease_owner: str | None
    lease_token: str | None
    lease_until: float | None
    fence: int
    created_at: float
    updated_at: float


@dataclass(frozen=True, slots=True)
class ChannelDeliverySchedule:
    due_errors: tuple[ChannelDeliveryIntent, ...]
    earliest_due_at: float | None
    observed_at: float


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
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
            uri=db_path.startswith("file:"),
        )
        try:
            with database_initialization_lock(self._db_path):
                self._init_db()
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass
            raise

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
                """INSERT INTO channel_delivery_intents
                    (intent_id, platform, session_key, chat_id, task_id, run_count,
                     delivery_kind, state, cursor, attempt_count, redrive_count, fence,
                     created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0, ?, ?)
                    ON CONFLICT(intent_id) DO UPDATE SET chat_id = excluded.chat_id,
                    updated_at = excluded.updated_at""",
                (
                    intent_id,
                    platform,
                    session_key,
                    chat_id,
                    task_id,
                    run_count,
                    DELIVERY_KIND_WATCH,
                    DELIVERY_STATE_WATCHING,
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
                f"SELECT {_INTENT_SELECT_COLUMNS} FROM channel_delivery_intents "
                "WHERE intent_id = ?",
                (intent_id,),
            ).fetchone()
        return self._row(row) if row is not None else None

    def list_recoverable(self, *, platform: str) -> list[ChannelDeliveryIntent]:
        placeholders = ", ".join("?" for _ in RECOVERABLE_DELIVERY_STATES)
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_INTENT_SELECT_COLUMNS} FROM channel_delivery_intents "
                f"WHERE platform = ? AND state IN ({placeholders}) ORDER BY created_at, intent_id",
                (platform, *_RECOVERABLE_STATE_VALUES),
            ).fetchall()
        return [self._row(row) for row in rows]

    def list_due_errors(
        self,
        *,
        platform: str,
        limit: int = 100,
        now: float | None = None,
    ) -> list[ChannelDeliveryIntent]:
        if limit <= 0:
            raise ValueError("delivery reconciliation limit must be positive")
        current_time = self._clock() if now is None else now
        with self._lock:
            rows = self._conn.execute(
                f"SELECT {_INTENT_SELECT_COLUMNS} FROM channel_delivery_intents "
                "WHERE platform = ? AND state = ? AND next_attempt_at IS NOT NULL "
                "AND next_attempt_at <= ? AND (lease_owner IS NULL OR lease_until IS NULL "
                "OR lease_until <= ?) ORDER BY next_attempt_at, created_at, intent_id LIMIT ?",
                (platform, DELIVERY_STATE_ERROR, current_time, current_time, limit),
            ).fetchall()
        return [self._row(row) for row in rows]

    def schedule_snapshot(
        self,
        *,
        platform: str,
        limit: int = 100,
        now: float | None = None,
    ) -> ChannelDeliverySchedule:
        if limit <= 0:
            raise ValueError("delivery reconciliation limit must be positive")
        current_time = self._clock() if now is None else now
        with self._lock:
            rows = self._conn.execute(
                f"""WITH candidates AS (
                    SELECT {_INTENT_SELECT_COLUMNS}, CASE WHEN next_attempt_at <= ? THEN 1 ELSE 0 END AS is_due
                    FROM channel_delivery_intents
                    WHERE platform = ? AND state = ? AND next_attempt_at IS NOT NULL
                      AND (lease_owner IS NULL OR lease_until IS NULL OR lease_until <= ?)
                ), ranked AS (
                    SELECT candidates.*, ROW_NUMBER() OVER (
                        PARTITION BY is_due ORDER BY next_attempt_at, created_at, intent_id
                    ) AS schedule_rank FROM candidates
                ) SELECT {_INTENT_SELECT_COLUMNS}, is_due FROM ranked
                WHERE (is_due = 1 AND schedule_rank <= ?) OR (is_due = 0 AND schedule_rank = 1)
                ORDER BY is_due DESC, next_attempt_at, created_at, intent_id""",
                (current_time, platform, DELIVERY_STATE_ERROR, current_time, limit),
            ).fetchall()
        due: list[ChannelDeliveryIntent] = []
        earliest: float | None = None
        for row in rows:
            if row[20] == 1:
                due.append(self._row(row[:20]))
            elif earliest is None:
                earliest = float(row[12])
        return ChannelDeliverySchedule(tuple(due), earliest, current_time)

    async def aschedule_snapshot(
        self,
        *,
        platform: str,
        limit: int = 100,
        now: float | None = None,
    ) -> ChannelDeliverySchedule:
        return await to_thread(
            self.schedule_snapshot, platform=platform, limit=limit, now=now
        )

    async def alist_due_errors(
        self,
        *,
        platform: str,
        limit: int = 100,
        now: float | None = None,
    ) -> list[ChannelDeliveryIntent]:
        return await to_thread(
            self.list_due_errors, platform=platform, limit=limit, now=now
        )

    def next_due_error_at(
        self,
        *,
        platform: str,
        now: float | None = None,
    ) -> float | None:
        current_time = self._clock() if now is None else now
        with self._lock:
            row = self._conn.execute(
                """SELECT MIN(next_attempt_at) FROM channel_delivery_intents
                   WHERE platform = ? AND state = ? AND next_attempt_at > ?
                     AND (lease_owner IS NULL OR lease_until IS NULL OR lease_until <= ?)""",
                (platform, DELIVERY_STATE_ERROR, current_time, current_time),
            ).fetchone()
        return float(row[0]) if row is not None and row[0] is not None else None

    async def anext_due_error_at(
        self,
        *,
        platform: str,
        now: float | None = None,
    ) -> float | None:
        return await to_thread(self.next_due_error_at, platform=platform, now=now)

    def now(self) -> float:
        return self._clock()

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
        token = f"{owner}:{uuid.uuid4().hex}"
        final_states = tuple(FINAL_DELIVERY_STATES)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.execute(
                    """UPDATE channel_delivery_intents SET lease_owner = ?, lease_token = ?,
                       lease_until = ?, fence = fence + 1, updated_at = ?
                       WHERE intent_id = ? AND state NOT IN (?, ?)
                       AND (lease_owner IS NULL OR lease_until IS NULL OR lease_until <= ?
                            OR lease_owner = ?)""",
                    (
                        owner,
                        token,
                        now + lease_seconds,
                        now,
                        intent_id,
                        *final_states,
                        now,
                        owner,
                    ),
                )
                if cursor.rowcount != 1:
                    self._conn.rollback()
                    return None
                self._conn.commit()
                return token
            except BaseException:
                self._conn.rollback()
                raise

    def claim_due_error(
        self,
        intent_id: str,
        *,
        owner: str,
        lease_seconds: float,
        now: float | None = None,
    ) -> str | None:
        if lease_seconds <= 0:
            raise ValueError("delivery lease seconds must be positive")
        current_time = self._clock() if now is None else now
        token = f"{owner}:{uuid.uuid4().hex}"
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT state, next_attempt_at, lease_owner, lease_until, fence "
                    "FROM channel_delivery_intents WHERE intent_id = ?",
                    (intent_id,),
                ).fetchone()
                if row is None:
                    self._conn.rollback()
                    return None
                if (
                    parse_channel_delivery_state(row[0]) != DELIVERY_STATE_ERROR
                    or row[1] is None
                ):
                    self._conn.rollback()
                    return None
                if float(row[1]) > current_time:
                    self._conn.rollback()
                    return None
                if (
                    row[2] is not None
                    and row[2] != owner
                    and row[3] is not None
                    and float(row[3]) > current_time
                ):
                    self._conn.rollback()
                    return None
                old_fence = int(row[4])
                fence = old_fence + 1
                cursor = self._conn.execute(
                    """UPDATE channel_delivery_intents SET lease_owner = ?, lease_token = ?,
                       lease_until = ?, fence = ?, updated_at = ?
                       WHERE intent_id = ? AND state = ? AND next_attempt_at <= ?
                       AND fence = ? AND (lease_owner IS NULL OR lease_until IS NULL
                       OR lease_until <= ? OR lease_owner = ?)""",
                    (
                        owner,
                        token,
                        current_time + lease_seconds,
                        fence,
                        current_time,
                        intent_id,
                        DELIVERY_STATE_ERROR,
                        current_time,
                        old_fence,
                        current_time,
                        owner,
                    ),
                )
                if cursor.rowcount != 1:
                    self._conn.rollback()
                    return None
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
        return self._fenced_update(
            f"""UPDATE channel_delivery_intents
                SET state = CASE WHEN state = ? THEN state ELSE ? END,
                    attempt_count = ?, cursor = cursor + 1,
                    next_attempt_at = ?, last_error = ?, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN ({_RECOVERABLE_STATE_SQL})""",
            (
                DELIVERY_STATE_TERMINAL_GRACE,
                DELIVERY_STATE_RETRY_WAIT,
                attempt,
                now + max(0.0, delay),
                error,
                now,
                intent_id,
                token,
                now,
                *_RECOVERABLE_STATE_VALUES,
            ),
        )

    def mark_watching(self, intent_id: str, *, token: str) -> bool:
        now = self._clock()
        return self._fenced_update(
            f"""UPDATE channel_delivery_intents
                SET state = CASE WHEN state IN (?, ?) THEN state ELSE ? END,
                    attempt_count = 0, redrive_count = 0,
                    next_attempt_at = NULL, last_error = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN ({_RECOVERABLE_STATE_SQL})""",
            (
                DELIVERY_STATE_REVIEW_WAITING,
                DELIVERY_STATE_TERMINAL_GRACE,
                DELIVERY_STATE_WATCHING,
                now,
                intent_id,
                token,
                now,
                *_RECOVERABLE_STATE_VALUES,
            ),
        )

    def mark_delivering(
        self,
        intent_id: str,
        *,
        token: str,
        delivery_kind: ChannelDeliveryKind,
        review_id: str | None,
    ) -> bool:
        delivery_kind = parse_channel_delivery_kind(delivery_kind, path="delivery_kind")
        now = self._clock()
        return self._fenced_update(
            f"""UPDATE channel_delivery_intents
                SET delivery_kind = ?, review_id = ?, state = ?,
                    attempt_count = 0, next_attempt_at = NULL,
                    last_error = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN ({_RECOVERABLE_STATE_SQL})""",
            (
                delivery_kind,
                review_id,
                DELIVERY_STATE_DELIVERING,
                now,
                intent_id,
                token,
                now,
                *_RECOVERABLE_STATE_VALUES,
            ),
        )

    def mark_error(
        self,
        intent_id: str,
        *,
        token: str,
        error: str,
        redrive_delay: float | None = None,
    ) -> bool:
        if redrive_delay is not None and redrive_delay <= 0:
            raise ValueError("delivery redrive delay must be positive")
        now = self._clock()
        return self._fenced_update(
            f"""UPDATE channel_delivery_intents
                SET state = ?, next_attempt_at = ?, last_error = ?,
                    redrive_count = redrive_count + ?,
                    lease_owner = NULL, lease_token = NULL, lease_until = NULL,
                    updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN ({_RECOVERABLE_STATE_SQL})""",
            (
                DELIVERY_STATE_ERROR,
                now + redrive_delay if redrive_delay is not None else None,
                error,
                1 if redrive_delay is not None else 0,
                now,
                intent_id,
                token,
                now,
                *_RECOVERABLE_STATE_VALUES,
            ),
        )

    def mark_review_waiting(self, intent_id: str, *, token: str) -> bool:
        now = self._clock()
        return self._fenced_update(
            """UPDATE channel_delivery_intents
                SET state = ?, attempt_count = 0,
                    next_attempt_at = NULL, last_error = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN (?, ?)""",
            (
                DELIVERY_STATE_REVIEW_WAITING,
                now,
                intent_id,
                token,
                now,
                DELIVERY_STATE_DELIVERING,
                DELIVERY_STATE_REVIEW_WAITING,
            ),
        )

    def mark_terminal_steps_done(self, intent_id: str, *, token: str) -> bool:
        now = self._clock()
        return self._fenced_update(
            """UPDATE channel_delivery_intents
                SET state = ?, attempt_count = 0,
                    next_attempt_at = NULL, last_error = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN (?, ?)""",
            (
                DELIVERY_STATE_TERMINAL_GRACE,
                now,
                intent_id,
                token,
                now,
                DELIVERY_STATE_DELIVERING,
                DELIVERY_STATE_TERMINAL_GRACE,
            ),
        )

    def mark_delivered(self, intent_id: str, *, token: str) -> bool:
        now = self._clock()
        return self._fenced_update(
            """UPDATE channel_delivery_intents
                SET state = ?, attempt_count = 0, next_attempt_at = NULL,
                    last_error = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state = ?""",
            (
                DELIVERY_STATE_DELIVERED,
                now,
                intent_id,
                token,
                now,
                DELIVERY_STATE_TERMINAL_GRACE,
            ),
        )

    def mark_superseded(self, intent_id: str, *, token: str) -> bool:
        now = self._clock()
        return self._fenced_update(
            f"""UPDATE channel_delivery_intents
                SET state = ?, lease_owner = NULL, lease_token = NULL,
                    lease_until = NULL, updated_at = ?
                WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    AND state IN ({_RECOVERABLE_STATE_SQL})""",
            (
                DELIVERY_STATE_SUPERSEDED,
                now,
                intent_id,
                token,
                now,
                *_RECOVERABLE_STATE_VALUES,
            ),
        )

    def _fenced_update(self, statement: str, values: tuple[object, ...]) -> bool:
        with self._lock:
            try:
                cursor = self._conn.execute(statement, values)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return cursor.rowcount == 1

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
                    SELECT state FROM channel_delivery_intents
                    WHERE intent_id = ? AND lease_token = ? AND lease_until > ?
                    """,
                    (intent_id, token, now),
                ).fetchone()
                if owned is None:
                    self._conn.rollback()
                    return False
                parse_channel_delivery_state(
                    owned[0], path="channel_delivery_intents.state"
                )
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

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:" or self._db_path.startswith("file:"):
            return
        Path(self._db_path).expanduser().resolve().parent.mkdir(
            parents=True, exist_ok=True
        )

    def _init_db(self) -> None:
        with self._lock:
            configure_connection_for_initialization(
                self._conn,
                db_path=self._db_path,
                foreign_keys=True,
            )
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                if self._table_exists("channel_delivery_intents"):
                    if not self._is_current_intent_schema():
                        self._rebuild_legacy_schema()
                else:
                    self._create_intent_table("channel_delivery_intents")
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_channel_delivery_recovery "
                    "ON channel_delivery_intents(platform, state, next_attempt_at)"
                )
                self._conn.execute(
                    """CREATE TABLE IF NOT EXISTS channel_delivery_steps (
                        intent_id TEXT NOT NULL, step_key TEXT NOT NULL,
                        delivered_at REAL NOT NULL, PRIMARY KEY(intent_id, step_key),
                        FOREIGN KEY(intent_id) REFERENCES channel_delivery_intents(intent_id)
                        ON DELETE CASCADE)"""
                )
                self._conn.commit()
            except BaseException:
                try:
                    self._conn.rollback()
                except BaseException:
                    pass
                raise

    def _create_intent_table(self, table_name: str) -> None:
        if table_name not in (
            "channel_delivery_intents",
            "channel_delivery_intents__m03_new",
        ):
            raise ValueError(f"Unexpected channel delivery table {table_name!r}")
        self._conn.execute(
            f"""CREATE TABLE {table_name} (
                intent_id TEXT PRIMARY KEY,
                platform TEXT NOT NULL,
                session_key TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                run_count INTEGER NOT NULL CHECK (run_count >= 0),
                delivery_kind TEXT NOT NULL CHECK (delivery_kind IN ({_KIND_CHECK_SQL})),
                review_id TEXT,
                state TEXT NOT NULL CHECK (state IN ({_STATE_CHECK_SQL})),
                cursor INTEGER NOT NULL DEFAULT 0,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                redrive_count INTEGER NOT NULL DEFAULT 0 CHECK (redrive_count >= 0),
                next_attempt_at REAL,
                last_error TEXT,
                lease_owner TEXT,
                lease_token TEXT,
                lease_until REAL,
                fence INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(platform, session_key, task_id, run_count)
            )"""
        )

    def _table_exists(self, table_name: str) -> bool:
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
        return any(row[0] == table_name for row in rows)

    def _is_current_intent_schema(self) -> bool:
        table_info = self._conn.execute(
            "PRAGMA table_info(channel_delivery_intents)"
        ).fetchall()
        columns = tuple(row[1] for row in table_info)
        if columns != _CURRENT_INTENT_COLUMNS:
            return False
        not_null = {row[1]: row[3] for row in table_info}
        if any(
            not_null.get(column) != 1
            for column in ("delivery_kind", "state", "redrive_count")
        ):
            return False
        row = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'channel_delivery_intents'"
        ).fetchone()
        sql = row[0] if row is not None else None
        if not isinstance(sql, str):
            return False
        normalized = " ".join(sql.lower().split())
        return (
            "check (delivery_kind in (" in normalized
            and "check (state in (" in normalized
            and "check (redrive_count >= 0)" in normalized
            and all(repr(value) in normalized for value in DELIVERY_KINDS)
            and all(repr(value) in normalized for value in DELIVERY_STATES)
        )

    def _rebuild_legacy_schema(self) -> None:
        columns = {
            row[1]
            for row in self._conn.execute(
                "PRAGMA table_info(channel_delivery_intents)"
            ).fetchall()
        }
        missing = [column for column in _LEGACY_INTENT_COLUMNS if column not in columns]
        if missing:
            raise ValueError(
                "Legacy channel delivery schema is missing columns: "
                + ", ".join(missing)
            )
        self._validate_legacy_rows()

        current_columns = ", ".join(_CURRENT_INTENT_COLUMNS)
        redrive_expression = "redrive_count" if "redrive_count" in columns else "0"
        source_values = ", ".join(
            (
                *_LEGACY_INTENT_COLUMNS[:11],
                redrive_expression,
                *_LEGACY_INTENT_COLUMNS[11:],
            )
        )
        new_intents = "channel_delivery_intents__m03_new"
        new_steps = "channel_delivery_steps__m03_new"
        self._conn.execute(f"DROP TABLE IF EXISTS {new_steps}")
        self._conn.execute(f"DROP TABLE IF EXISTS {new_intents}")
        self._create_intent_table(new_intents)
        self._conn.execute(
            f"INSERT INTO {new_intents} ({current_columns}) SELECT {source_values} FROM channel_delivery_intents"
        )
        if self._table_exists("channel_delivery_steps"):
            self._conn.execute(
                f"""CREATE TABLE {new_steps} (
                    intent_id TEXT NOT NULL, step_key TEXT NOT NULL,
                    delivered_at REAL NOT NULL, PRIMARY KEY(intent_id, step_key),
                    FOREIGN KEY(intent_id) REFERENCES {new_intents}(intent_id)
                    ON DELETE CASCADE)"""
            )
            self._conn.execute(
                f"INSERT INTO {new_steps} (intent_id, step_key, delivered_at) "
                "SELECT intent_id, step_key, delivered_at FROM channel_delivery_steps"
            )
        self._conn.execute("DROP INDEX IF EXISTS idx_channel_delivery_recovery")
        if self._table_exists("channel_delivery_steps"):
            self._conn.execute("DROP TABLE channel_delivery_steps")
        self._conn.execute("DROP TABLE channel_delivery_intents")
        self._conn.execute(
            "ALTER TABLE " + new_intents + " RENAME TO channel_delivery_intents"
        )
        if self._table_exists(new_steps):
            self._conn.execute(
                "ALTER TABLE " + new_steps + " RENAME TO channel_delivery_steps"
            )

    def _validate_legacy_rows(self) -> None:
        rows = self._conn.execute(
            "SELECT intent_id, delivery_kind, state FROM channel_delivery_intents"
        ).fetchall()
        for intent_id, delivery_kind, state in rows:
            path = f"channel_delivery_intents[{intent_id!r}]"
            parse_channel_delivery_kind(delivery_kind, path=f"{path}.delivery_kind")
            parse_channel_delivery_state(state, path=f"{path}.state")

    @staticmethod
    def _row(row: tuple[object, ...]) -> ChannelDeliveryIntent:
        return ChannelDeliveryIntent(
            intent_id=str(row[0]),
            platform=str(row[1]),
            session_key=str(row[2]),
            chat_id=str(row[3]),
            task_id=str(row[4]),
            run_count=int(row[5]),
            delivery_kind=parse_channel_delivery_kind(
                row[6], path="channel_delivery_intents.delivery_kind"
            ),
            review_id=str(row[7]) if row[7] is not None else None,
            state=parse_channel_delivery_state(
                row[8], path="channel_delivery_intents.state"
            ),
            cursor=int(row[9]),
            attempt_count=int(row[10]),
            redrive_count=int(row[11]),
            next_attempt_at=float(row[12]) if row[12] is not None else None,
            last_error=str(row[13]) if row[13] is not None else None,
            lease_owner=str(row[14]) if row[14] is not None else None,
            lease_token=str(row[15]) if row[15] is not None else None,
            lease_until=float(row[16]) if row[16] is not None else None,
            fence=int(row[17]),
            created_at=float(row[18]),
            updated_at=float(row[19]),
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
