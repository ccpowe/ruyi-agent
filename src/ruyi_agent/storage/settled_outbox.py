from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from ruyi_agent.storage.task_codecs import TASK_SELECT_COLUMNS, row_to_task_record
from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.task_models import SETTLED_TASK_STATES, TaskRecord, TaskState


@dataclass(frozen=True, slots=True)
class SettledOutboxIntent:
    """Durable intent to publish one child Task run settlement."""

    outbox_key: str
    message_id: str
    task_id: str
    run_count: int
    recipient_task_id: str | None
    recipient_thread_id: str
    child_agent_name: str
    settled_status: TaskState
    content: str
    created_at: datetime
    claim_token: str | None = None


def settled_outbox_key(
    *,
    recipient_thread_id: str,
    task_id: str,
    run_count: int,
) -> str:
    return f"settled:{recipient_thread_id}:{task_id}:{run_count}"


def build_settled_outbox_intent(record: TaskRecord) -> SettledOutboxIntent | None:
    """Project one eligible settled Task run into a deterministic intent."""

    if (
        record.state not in SETTLED_TASK_STATES
        or record.parent_thread_id is None
        or record.mailbox_suppressed
        or record.mailbox_delivered
    ):
        return None
    key = settled_outbox_key(
        recipient_thread_id=record.parent_thread_id,
        task_id=record.task_id,
        run_count=record.run_count,
    )
    return SettledOutboxIntent(
        outbox_key=key,
        message_id=str(uuid.uuid5(uuid.NAMESPACE_URL, key)),
        task_id=record.task_id,
        run_count=record.run_count,
        recipient_task_id=record.parent_task_id,
        recipient_thread_id=record.parent_thread_id,
        child_agent_name=record.agent_name,
        settled_status=record.state,
        content=(
            record.result
            or record.error
            or f"Task run ended with state={record.state}"
        ),
        created_at=record.updated_at,
    )


class SettledOutboxRepository:
    """Own settled-notification outbox rows and their fenced delivery leases."""

    def __init__(self, database: TaskDatabase) -> None:
        self._database = database
        self._owner_id = f"settled-outbox-{uuid.uuid4().hex}"

    @staticmethod
    def insert_locked(
        connection: sqlite3.Connection,
        intent: SettledOutboxIntent,
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO agent_task_settled_outbox (
                outbox_key, message_id, task_id, run_count, recipient_task_id,
                recipient_thread_id, child_agent_name, settled_status, content,
                status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            """,
            (
                intent.outbox_key,
                intent.message_id,
                intent.task_id,
                intent.run_count,
                intent.recipient_task_id,
                intent.recipient_thread_id,
                intent.child_agent_name,
                intent.settled_status,
                intent.content,
                intent.created_at.isoformat(),
            ),
        )

    def reconcile_legacy_settlements(self) -> int:
        """Create missing intents for pre-outbox settled Task rows."""

        inserted = 0
        with self._database.transaction(immediate=True) as connection:
            rows = connection.execute(
                f"""
                SELECT {TASK_SELECT_COLUMNS}
                FROM agent_tasks
                WHERE state IN ('completed', 'failed', 'cancelled', 'interrupted')
                  AND parent_thread_id IS NOT NULL
                  AND mailbox_suppressed = 0
                  AND mailbox_delivered = 0
                ORDER BY updated_at, task_id
                """
            ).fetchall()
            for row in rows:
                intent = build_settled_outbox_intent(row_to_task_record(row))
                if intent is None:
                    continue
                before = connection.total_changes
                self.insert_locked(connection, intent)
                inserted += connection.total_changes - before
        return inserted

    def claim_pending(
        self,
        *,
        limit: int = 100,
        lease_seconds: float = 30.0,
    ) -> list[SettledOutboxIntent]:
        if limit < 1:
            raise ValueError("Settled outbox claim limit must be positive")
        if lease_seconds <= 0:
            raise ValueError("Settled outbox lease must be positive")
        now = datetime.now(UTC)
        expires_at = now + timedelta(seconds=lease_seconds)
        claim_token = uuid.uuid4().hex
        with self._database.transaction(immediate=True) as connection:
            connection.execute(
                """
                UPDATE agent_task_settled_outbox
                SET status = 'pending', claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, claim_expires_at = NULL
                WHERE status = 'claimed' AND claim_expires_at <= ?
                """,
                (now.isoformat(),),
            )
            rows = connection.execute(
                """
                SELECT outbox_key
                FROM agent_task_settled_outbox
                WHERE status = 'pending'
                ORDER BY created_at, outbox_key
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            keys = [str(row[0]) for row in rows]
            if keys:
                placeholders = ",".join("?" for _ in keys)
                connection.execute(
                    f"""
                    UPDATE agent_task_settled_outbox
                    SET status = 'claimed', claimed_by = ?, claim_token = ?,
                        claimed_at = ?, claim_expires_at = ?,
                        attempt_count = attempt_count + 1, last_error = NULL
                    WHERE outbox_key IN ({placeholders}) AND status = 'pending'
                    """,
                    (
                        self._owner_id,
                        claim_token,
                        now.isoformat(),
                        expires_at.isoformat(),
                        *keys,
                    ),
                )
            claimed = connection.execute(
                """
                SELECT outbox_key, message_id, task_id, run_count,
                       recipient_task_id, recipient_thread_id, child_agent_name,
                       settled_status, content, created_at, claim_token
                FROM agent_task_settled_outbox
                WHERE claimed_by = ? AND claim_token = ? AND status = 'claimed'
                ORDER BY created_at, outbox_key
                """,
                (self._owner_id, claim_token),
            ).fetchall()
        return [self._row_to_intent(row) for row in claimed]

    def release_claim(
        self,
        intent: SettledOutboxIntent,
        *,
        error: str,
    ) -> bool:
        if intent.claim_token is None:
            return False
        with self._database.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE agent_task_settled_outbox
                SET status = 'pending', claimed_by = NULL, claim_token = NULL,
                    claimed_at = NULL, claim_expires_at = NULL, last_error = ?
                WHERE outbox_key = ? AND status = 'claimed'
                  AND claimed_by = ? AND claim_token = ?
                """,
                (error[:2000], intent.outbox_key, self._owner_id, intent.claim_token),
            )
        return cursor.rowcount == 1

    def suppress_for_task_run_locked(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        run_count: int,
    ) -> None:
        connection.execute(
            """
            UPDATE agent_task_settled_outbox
            SET status = 'suppressed', claimed_by = NULL, claim_token = NULL,
                claimed_at = NULL, claim_expires_at = NULL, retracted_at = NULL
            WHERE task_id = ? AND run_count = ?
              AND status IN ('pending', 'claimed', 'delivered')
            """,
            (task_id, run_count),
        )

    def list_suppressed_unretracted(self, *, limit: int = 100) -> list[SettledOutboxIntent]:
        with self._database.locked_connection() as connection:
            rows = connection.execute(
                """
                SELECT outbox_key, message_id, task_id, run_count,
                       recipient_task_id, recipient_thread_id, child_agent_name,
                       settled_status, content, created_at, NULL
                FROM agent_task_settled_outbox
                WHERE status = 'suppressed' AND retracted_at IS NULL
                ORDER BY created_at, outbox_key
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_intent(row) for row in rows]

    def list_all(self) -> list[dict[str, object]]:
        """Return diagnostic rows for tests and operational inspection."""

        with self._database.locked_connection() as connection:
            connection.row_factory = sqlite3.Row
            try:
                rows = connection.execute(
                    "SELECT * FROM agent_task_settled_outbox ORDER BY created_at, outbox_key"
                ).fetchall()
                return [dict(row) for row in rows]
            finally:
                connection.row_factory = None

    @staticmethod
    def _row_to_intent(row: sqlite3.Row | tuple[object, ...]) -> SettledOutboxIntent:
        return SettledOutboxIntent(
            outbox_key=str(row[0]),
            message_id=str(row[1]),
            task_id=str(row[2]),
            run_count=int(row[3]),
            recipient_task_id=str(row[4]) if row[4] is not None else None,
            recipient_thread_id=str(row[5]),
            child_agent_name=str(row[6]),
            settled_status=str(row[7]),  # type: ignore[arg-type]
            content=str(row[8]),
            created_at=datetime.fromisoformat(str(row[9])),
            claim_token=str(row[10]) if row[10] is not None else None,
        )
