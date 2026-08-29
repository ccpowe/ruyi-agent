from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from asyncio import to_thread
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from ruyi_agent.storage.task_database import (
    configure_connection_for_initialization,
    database_initialization_lock,
)


CommandClaimStatus = Literal["acquired", "busy", "replay", "terminal"]


class GatewayCommandConflictError(ValueError):
    """Raised when one idempotency key is reused for a different command."""


class GatewayCommandStateError(RuntimeError):
    """Raised when a command cannot make the requested durable transition."""


@dataclass(frozen=True, slots=True)
class GatewayCommandClaim:
    status: CommandClaimStatus
    command_id: str
    task_id: str
    mailbox_message_id: str | None
    operation: str
    target: str
    claim_token: str | None = None
    response_json: str | None = None
    error_json: str | None = None


class GatewayCommandStore:
    """Durable idempotency ledger for Gateway Task mutation commands.

    The Gateway currently has a single-process deployment contract. On opening the
    store, replay-safe interrupted claims are released. Claims that may have reached
    a non-idempotent downstream create become terminal and retain their public Task
    identity instead of replaying the effect.
    """

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
            uri=db_path.startswith("file:"),
        )
        self._conn.row_factory = sqlite3.Row
        try:
            with database_initialization_lock(self._db_path):
                self._init_db()
                self._recover_interrupted_claims()
        except BaseException:
            try:
                self.close()
            except BaseException:
                pass
            raise

    def claim(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
        operation: str,
        target: str,
        request_hash: str,
        proposed_task_id: str,
        proposed_mailbox_message_id: str | None = None,
    ) -> GatewayCommandClaim:
        now = datetime.now(UTC).isoformat()
        claim_token = uuid.uuid4().hex
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """
                    SELECT command_id, operation, target, request_hash, state,
                        task_id, mailbox_message_id, claim_token, response_json,
                        error_json
                    FROM gateway_commands
                    WHERE principal_id = ? AND idempotency_key = ?
                    """,
                    (principal_id, idempotency_key),
                ).fetchone()
                if row is None:
                    command_id = str(uuid.uuid4())
                    self._conn.execute(
                        """
                        INSERT INTO gateway_commands (
                            command_id, principal_id, idempotency_key, operation,
                            target, request_hash, state, task_id,
                            mailbox_message_id, claim_token, response_json,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'processing', ?, ?, ?, NULL, ?, ?)
                        """,
                        (
                            command_id,
                            principal_id,
                            idempotency_key,
                            operation,
                            target,
                            request_hash,
                            proposed_task_id,
                            proposed_mailbox_message_id,
                            claim_token,
                            now,
                            now,
                        ),
                    )
                    self._conn.commit()
                    return GatewayCommandClaim(
                        status="acquired",
                        command_id=command_id,
                        task_id=proposed_task_id,
                        mailbox_message_id=proposed_mailbox_message_id,
                        operation=operation,
                        target=target,
                        claim_token=claim_token,
                    )

                self._validate_existing_command(
                    row,
                    operation=operation,
                    target=target,
                    request_hash=request_hash,
                )
                command_id = str(row["command_id"])
                task_id = str(row["task_id"])
                mailbox_message_id = (
                    str(row["mailbox_message_id"])
                    if row["mailbox_message_id"] is not None
                    else None
                )
                if row["state"] == "succeeded":
                    response_json = row["response_json"]
                    if not isinstance(response_json, str) or not response_json:
                        raise GatewayCommandStateError(
                            f"Succeeded Gateway command '{command_id}' has no response"
                        )
                    self._conn.commit()
                    return GatewayCommandClaim(
                        status="replay",
                        command_id=command_id,
                        task_id=task_id,
                        mailbox_message_id=mailbox_message_id,
                        operation=operation,
                        target=target,
                        response_json=response_json,
                    )
                if row["state"] == "failed":
                    error_json = row["error_json"]
                    if not isinstance(error_json, str) or not error_json:
                        raise GatewayCommandStateError(
                            f"Failed Gateway command '{command_id}' has no error"
                        )
                    self._conn.commit()
                    return GatewayCommandClaim(
                        status="terminal",
                        command_id=command_id,
                        task_id=task_id,
                        mailbox_message_id=mailbox_message_id,
                        operation=operation,
                        target=target,
                        error_json=error_json,
                    )
                if row["state"] == "processing":
                    self._conn.commit()
                    return GatewayCommandClaim(
                        status="busy",
                        command_id=command_id,
                        task_id=task_id,
                        mailbox_message_id=mailbox_message_id,
                        operation=operation,
                        target=target,
                    )
                if row["state"] != "pending":
                    raise GatewayCommandStateError(
                        f"Gateway command '{command_id}' has invalid state={row['state']!r}"
                    )
                self._conn.execute(
                    """
                    UPDATE gateway_commands
                    SET state = 'processing', claim_token = ?, updated_at = ?
                    WHERE command_id = ? AND state = 'pending'
                    """,
                    (claim_token, now, command_id),
                )
                self._conn.commit()
                return GatewayCommandClaim(
                    status="acquired",
                    command_id=command_id,
                    task_id=task_id,
                    mailbox_message_id=mailbox_message_id,
                    operation=operation,
                    target=target,
                    claim_token=claim_token,
                )
            except BaseException:
                self._conn.rollback()
                raise

    async def aclaim(self, **kwargs: str | None) -> GatewayCommandClaim:
        return await to_thread(self.claim, **kwargs)

    def complete(
        self,
        *,
        command_id: str,
        claim_token: str,
        response_json: str,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'succeeded', response_json = ?, claim_token = NULL,
                    error_json = NULL, updated_at = ?
                WHERE command_id = ? AND state = 'processing' AND claim_token = ?
                """,
                (response_json, now, command_id, claim_token),
            )
            self._conn.commit()
            if cursor.rowcount != 1:
                raise GatewayCommandStateError(
                    f"Gateway command '{command_id}' is no longer owned by this claim"
                )

    async def acomplete(self, **kwargs: str) -> None:
        await to_thread(self.complete, **kwargs)

    def mark_effect_started(
        self,
        *,
        command_id: str,
        claim_token: str,
        replay_safe: bool,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE gateway_commands
                SET effect_started = 1, replay_safe = ?, updated_at = ?
                WHERE command_id = ? AND state = 'processing' AND claim_token = ?
                """,
                (int(replay_safe), now, command_id, claim_token),
            )
            self._conn.commit()
            if cursor.rowcount != 1:
                raise GatewayCommandStateError(
                    f"Gateway command '{command_id}' is no longer owned by this claim"
                )

    async def amark_effect_started(
        self,
        *,
        command_id: str,
        claim_token: str,
        replay_safe: bool,
    ) -> None:
        await to_thread(
            self.mark_effect_started,
            command_id=command_id,
            claim_token=claim_token,
            replay_safe=replay_safe,
        )

    def fail(
        self,
        *,
        command_id: str,
        claim_token: str,
        error_json: str,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'failed', error_json = ?, claim_token = NULL,
                    updated_at = ?
                WHERE command_id = ? AND state = 'processing' AND claim_token = ?
                """,
                (error_json, now, command_id, claim_token),
            )
            self._conn.commit()
            if cursor.rowcount != 1:
                raise GatewayCommandStateError(
                    f"Gateway command '{command_id}' is no longer owned by this claim"
                )

    async def afail(self, **kwargs: str) -> None:
        await to_thread(self.fail, **kwargs)

    def release(self, *, command_id: str, claim_token: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'pending', claim_token = NULL, effect_started = 0,
                    updated_at = ?
                WHERE command_id = ? AND state = 'processing' AND claim_token = ?
                    AND NOT (effect_started = 1 AND replay_safe = 0)
                """,
                (now, command_id, claim_token),
            )
            self._conn.commit()

    async def arelease(self, **kwargs: str) -> None:
        await to_thread(self.release, **kwargs)

    def release_not_dispatched(self, *, command_id: str, claim_token: str) -> None:
        """Release an unsafe claim after route evidence proves zero dispatch."""

        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'pending', claim_token = NULL, effect_started = 0,
                    replay_safe = 0, error_json = NULL, updated_at = ?
                WHERE command_id = ? AND operation = 'create_task'
                    AND state = 'processing' AND claim_token = ?
                """,
                (now, command_id, claim_token),
            )
            self._conn.commit()
            if cursor.rowcount != 1:
                raise GatewayCommandStateError(
                    f"Gateway command '{command_id}' is no longer owned by this claim"
                )

    async def arelease_not_dispatched(self, **kwargs: str) -> None:
        await to_thread(self.release_not_dispatched, **kwargs)

    def reopen_not_dispatched(
        self,
        *,
        command_id: str,
        expected_error_json: str,
    ) -> bool:
        """Reopen a terminal create when separate route evidence proves no send."""

        now = datetime.now(UTC).isoformat()
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'pending', claim_token = NULL, effect_started = 0,
                    replay_safe = 0, error_json = NULL, updated_at = ?
                WHERE command_id = ? AND operation = 'create_task'
                    AND state = 'failed' AND error_json = ?
                """,
                (now, command_id, expected_error_json),
            )
            self._conn.commit()
        return cursor.rowcount == 1

    async def areopen_not_dispatched(self, **kwargs: str) -> bool:
        return await to_thread(self.reopen_not_dispatched, **kwargs)

    def count_commands(self) -> int:
        with self._lock:
            row = self._conn.execute("SELECT COUNT(*) FROM gateway_commands").fetchone()
        return int(row[0]) if row is not None else 0

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _validate_existing_command(
        self,
        row: sqlite3.Row,
        *,
        operation: str,
        target: str,
        request_hash: str,
    ) -> None:
        if (
            row["operation"] == operation
            and row["target"] == target
            and row["request_hash"] == request_hash
        ):
            return
        raise GatewayCommandConflictError(
            "Idempotency key was already used for a different Gateway command"
        )

    def _recover_interrupted_claims(self) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """
                    SELECT command_id, task_id
                    FROM gateway_commands
                    WHERE state = 'processing' AND effect_started = 1
                        AND replay_safe = 0
                    """
                ).fetchall()
                for row in rows:
                    error_json = json.dumps(
                        {
                            "code": "idempotency_outcome_uncertain",
                            "message": (
                                "The previous Gateway command may have reached a "
                                "non-idempotent downstream service"
                            ),
                            "details": {
                                "task_id": str(row["task_id"]),
                                "create_retryable": False,
                                "effect_outcome": "uncertain",
                            },
                        },
                        sort_keys=True,
                    )
                    self._conn.execute(
                        """
                        UPDATE gateway_commands
                        SET state = 'failed', claim_token = NULL, error_json = ?,
                            updated_at = ?
                        WHERE command_id = ? AND state = 'processing'
                        """,
                        (error_json, now, str(row["command_id"])),
                    )
                self._conn.execute(
                    """
                    UPDATE gateway_commands
                    SET state = 'pending', claim_token = NULL, effect_started = 0,
                        updated_at = ?
                    WHERE state = 'processing'
                    """,
                    (now,),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:" or self._db_path.startswith("file:"):
            return
        Path(self._db_path).expanduser().resolve().parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _init_db(self) -> None:
        with self._lock:
            configure_connection_for_initialization(
                self._conn,
                db_path=self._db_path,
            )
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                _initialize_gateway_command_schema(self._conn)
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise


def _initialize_gateway_command_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS gateway_commands (
            command_id TEXT PRIMARY KEY,
            principal_id TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            operation TEXT NOT NULL,
            target TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            state TEXT NOT NULL,
            task_id TEXT NOT NULL,
            mailbox_message_id TEXT,
            claim_token TEXT,
            response_json TEXT,
            error_json TEXT,
            effect_started INTEGER NOT NULL DEFAULT 0,
            replay_safe INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(principal_id, idempotency_key)
        )
        """
    )
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(gateway_commands)").fetchall()
    }
    additions = {
        "error_json": "TEXT",
        "effect_started": "INTEGER NOT NULL DEFAULT 0",
        "replay_safe": "INTEGER NOT NULL DEFAULT 1",
    }
    for column, declaration in additions.items():
        if column not in columns:
            connection.execute(
                f"ALTER TABLE gateway_commands ADD COLUMN {column} {declaration}"
            )
