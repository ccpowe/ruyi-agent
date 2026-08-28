from __future__ import annotations

import sqlite3
import threading
import uuid
from asyncio import to_thread
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal


CommandClaimStatus = Literal["acquired", "busy", "replay"]


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
    claim_token: str | None = None
    response_json: str | None = None


class GatewayCommandStore:
    """Durable idempotency ledger for Gateway Task mutation commands.

    The Gateway currently has a single-process deployment contract. On opening the
    store, claims left by the previous process are released so the same client
    request can drive the persisted command again. Effects remain independently
    deduplicated by their reserved task/message identities.
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
        self._init_db()
        self._recover_interrupted_claims()

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
                        task_id, mailbox_message_id, claim_token, response_json
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
                        response_json=response_json,
                    )
                if row["state"] == "processing":
                    self._conn.commit()
                    return GatewayCommandClaim(
                        status="busy",
                        command_id=command_id,
                        task_id=task_id,
                        mailbox_message_id=mailbox_message_id,
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
                    updated_at = ?
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

    def release(self, *, command_id: str, claim_token: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'pending', claim_token = NULL, updated_at = ?
                WHERE command_id = ? AND state = 'processing' AND claim_token = ?
                """,
                (now, command_id, claim_token),
            )
            self._conn.commit()

    async def arelease(self, **kwargs: str) -> None:
        await to_thread(self.release, **kwargs)

    def count_commands(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM gateway_commands"
            ).fetchone()
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
            self._conn.execute(
                """
                UPDATE gateway_commands
                SET state = 'pending', claim_token = NULL, updated_at = ?
                WHERE state = 'processing'
                """,
                (now,),
            )
            self._conn.commit()

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:" or self._db_path.startswith("file:"):
            return
        Path(self._db_path).expanduser().resolve().parent.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
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
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(principal_id, idempotency_key)
                )
                """
            )
            self._conn.commit()
