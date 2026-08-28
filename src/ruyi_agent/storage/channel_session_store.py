from __future__ import annotations

import json
import sqlite3
import threading
from asyncio import to_thread
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(slots=True)
class ChannelSessionRecord:
    session_key: str
    platform: str
    agent_name: str
    current_task_id: str | None
    chat_id: str | None = None
    user_id: str | None = None
    thread_id: str | None = None
    created_at: str = ""
    updated_at: str = ""


@dataclass(frozen=True, slots=True)
class ChannelTurnReceipt:
    idempotency_key: str
    platform: str
    session_key: str
    operation: str
    request_hash: str
    task_id: str
    response: dict[str, Any]
    created_at: str


class ChannelSessionStore:
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

    def get_session(self, session_key: str) -> ChannelSessionRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT session_key, platform, agent_name, current_task_id,
                    chat_id, user_id, thread_id, created_at, updated_at
                FROM channel_sessions
                WHERE session_key = ?
                """,
                (session_key,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_record(row)

    async def aget_session(self, session_key: str) -> ChannelSessionRecord | None:
        return await to_thread(self.get_session, session_key)

    def get_turn_receipt(
        self,
        idempotency_key: str,
    ) -> ChannelTurnReceipt | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT idempotency_key, platform, session_key, operation,
                    request_hash, task_id, response_json, created_at
                FROM channel_turn_receipts
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
        if row is None:
            return None
        response = json.loads(row[6])
        if not isinstance(response, dict):
            raise RuntimeError(
                f"Channel turn receipt '{idempotency_key}' has an invalid response"
            )
        return ChannelTurnReceipt(
            idempotency_key=row[0],
            platform=row[1],
            session_key=row[2],
            operation=row[3],
            request_hash=row[4],
            task_id=row[5],
            response=response,
            created_at=row[7],
        )

    async def aget_turn_receipt(
        self,
        idempotency_key: str,
    ) -> ChannelTurnReceipt | None:
        return await to_thread(self.get_turn_receipt, idempotency_key)

    def bind_session(
        self,
        *,
        session_key: str,
        platform: str,
        agent_name: str,
        current_task_id: str,
        chat_id: str | None = None,
        user_id: str | None = None,
        thread_id: str | None = None,
        turn_idempotency_key: str | None = None,
        turn_operation: str | None = None,
        turn_request_hash: str | None = None,
        turn_response: dict[str, Any] | None = None,
    ) -> ChannelSessionRecord:
        if turn_idempotency_key is None:
            if (
                turn_operation is not None
                or turn_request_hash is not None
                or turn_response is not None
            ):
                raise ValueError("Turn receipt fields require an idempotency key")
        elif (
            turn_operation not in {"create", "send"}
            or not turn_request_hash
            or turn_response is None
        ):
            raise ValueError(
                "Turn receipt requires operation, request hash, and response"
            )
        now = _utc_now_iso()
        response_json = (
            json.dumps(turn_response, ensure_ascii=True, sort_keys=True)
            if turn_response is not None
            else None
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO channel_sessions (
                        session_key,
                        platform,
                        agent_name,
                        current_task_id,
                        chat_id,
                        user_id,
                        thread_id,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_key) DO UPDATE SET
                        platform = excluded.platform,
                        agent_name = excluded.agent_name,
                        current_task_id = excluded.current_task_id,
                        chat_id = excluded.chat_id,
                        user_id = excluded.user_id,
                        thread_id = excluded.thread_id,
                        updated_at = excluded.updated_at
                    """,
                    (
                        session_key,
                        platform,
                        agent_name,
                        current_task_id,
                        chat_id,
                        user_id,
                        thread_id,
                        now,
                        now,
                    ),
                )
                if turn_idempotency_key is not None:
                    self._insert_or_validate_turn_receipt(
                        idempotency_key=turn_idempotency_key,
                        platform=platform,
                        session_key=session_key,
                        operation=str(turn_operation),
                        request_hash=str(turn_request_hash),
                        task_id=current_task_id,
                        response_json=str(response_json),
                        created_at=now,
                    )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        record = self.get_session(session_key)
        if record is None:
            raise RuntimeError(f"Channel session '{session_key}' was not saved")
        return record

    async def abind_session(
        self,
        *,
        session_key: str,
        platform: str,
        agent_name: str,
        current_task_id: str,
        chat_id: str | None = None,
        user_id: str | None = None,
        thread_id: str | None = None,
        turn_idempotency_key: str | None = None,
        turn_operation: str | None = None,
        turn_request_hash: str | None = None,
        turn_response: dict[str, Any] | None = None,
    ) -> ChannelSessionRecord:
        return await to_thread(
            self.bind_session,
            session_key=session_key,
            platform=platform,
            agent_name=agent_name,
            current_task_id=current_task_id,
            chat_id=chat_id,
            user_id=user_id,
            thread_id=thread_id,
            turn_idempotency_key=turn_idempotency_key,
            turn_operation=turn_operation,
            turn_request_hash=turn_request_hash,
            turn_response=turn_response,
        )

    def unbind_session(self, session_key: str) -> ChannelSessionRecord | None:
        existing = self.get_session(session_key)
        if existing is None:
            return None
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE channel_sessions
                SET current_task_id = NULL, updated_at = ?
                WHERE session_key = ?
                """,
                (now, session_key),
            )
            self._conn.commit()
        return self.get_session(session_key)

    async def aunbind_session(self, session_key: str) -> ChannelSessionRecord | None:
        return await to_thread(self.unbind_session, session_key)

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:":
            return
        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_sessions (
                    session_key TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    agent_name TEXT NOT NULL,
                    current_task_id TEXT,
                    chat_id TEXT,
                    user_id TEXT,
                    thread_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_turn_receipts (
                    idempotency_key TEXT PRIMARY KEY,
                    platform TEXT NOT NULL,
                    session_key TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            self._conn.commit()

    def _insert_or_validate_turn_receipt(
        self,
        *,
        idempotency_key: str,
        platform: str,
        session_key: str,
        operation: str,
        request_hash: str,
        task_id: str,
        response_json: str,
        created_at: str,
    ) -> None:
        existing = self._conn.execute(
            """
            SELECT platform, session_key, operation, request_hash, task_id,
                response_json
            FROM channel_turn_receipts
            WHERE idempotency_key = ?
            """,
            (idempotency_key,),
        ).fetchone()
        requested = (
            platform,
            session_key,
            operation,
            request_hash,
            task_id,
            response_json,
        )
        if existing is not None:
            if tuple(existing) != requested:
                raise ValueError(
                    f"Channel turn receipt conflict for key '{idempotency_key}'"
                )
            return
        self._conn.execute(
            """
            INSERT INTO channel_turn_receipts (
                idempotency_key, platform, session_key, operation, request_hash,
                task_id, response_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                idempotency_key,
                platform,
                session_key,
                operation,
                request_hash,
                task_id,
                response_json,
                created_at,
            ),
        )

    def _row_to_record(self, row: tuple[str, ...]) -> ChannelSessionRecord:
        return ChannelSessionRecord(
            session_key=row[0],
            platform=row[1],
            agent_name=row[2],
            current_task_id=row[3],
            chat_id=row[4],
            user_id=row[5],
            thread_id=row[6],
            created_at=row[7],
            updated_at=row[8],
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
