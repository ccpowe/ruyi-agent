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


@dataclass(frozen=True, slots=True)
class LegacySettlementMigrationBatch:
    """One bounded, restartable legacy settlement migration step."""

    inserted: int
    completed: bool


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
        or record.external_operation is not None
        or record.external_outcome_uncertain
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
    ) -> bool:
        cursor = connection.execute(
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

        rows = connection.execute(
            """
            SELECT outbox_key, message_id, task_id, run_count,
                   recipient_task_id, recipient_thread_id, child_agent_name,
                   settled_status, content
            FROM agent_task_settled_outbox
            WHERE outbox_key = ? OR message_id = ?
               OR (task_id = ? AND run_count = ?)
            """,
            (
                intent.outbox_key,
                intent.message_id,
                intent.task_id,
                intent.run_count,
            ),
        ).fetchall()
        expected = (
            intent.outbox_key,
            intent.message_id,
            intent.task_id,
            intent.run_count,
            intent.recipient_task_id,
            intent.recipient_thread_id,
            intent.child_agent_name,
            intent.settled_status,
            intent.content,
        )
        if len(rows) != 1 or tuple(rows[0]) != expected:
            raise RuntimeError(
                "Settled outbox identity conflicts with the existing Task run intent"
            )
        return cursor.rowcount == 1

    @staticmethod
    def discard_invalid_uncertain_locked(
        connection: sqlite3.Connection,
        *,
        task_id: str | None = None,
    ) -> dict[tuple[str, int], bool]:
        """Fence legacy settlements that projected an uncertain remote effect.

        Pending and claimed mailbox rows have not crossed the durable delivery
        boundary and can be removed. A delivered mailbox row is irreversible;
        its Task run remains consumed so a later authoritative sync cannot emit
        a conflicting second settlement for the same identity.
        """

        mailbox_exists = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_mailbox_messages'
            """
        ).fetchone()
        task_filter = "AND task.task_id = ?" if task_id is not None else ""
        params: tuple[str, ...] = (task_id,) if task_id is not None else ()
        rows = connection.execute(
            f"""
            SELECT outbox.outbox_key, outbox.message_id,
                   outbox.task_id, outbox.run_count
            FROM agent_task_settled_outbox AS outbox
            JOIN agent_tasks AS task ON task.task_id = outbox.task_id
            WHERE task.external_operation IS NOT NULL
              AND task.external_outcome_uncertain = 1
              AND outbox.run_count = task.run_count
              {task_filter}
            """,
            params,
        ).fetchall()
        results: dict[tuple[str, int], bool] = {}
        for outbox_key, message_id, row_task_id, run_count in rows:
            mailbox_delivered = False
            if mailbox_exists is not None:
                delivered = connection.execute(
                    """
                    SELECT 1 FROM agent_mailbox_messages
                    WHERE (idempotency_key = ? OR message_id = ?)
                      AND status = 'delivered'
                    LIMIT 1
                    """,
                    (str(outbox_key), str(message_id)),
                ).fetchone()
                mailbox_delivered = delivered is not None
            key = (str(row_task_id), int(run_count))
            results[key] = mailbox_delivered
            if mailbox_delivered:
                connection.execute(
                    """
                    UPDATE agent_task_settled_outbox
                    SET status = 'delivered', claimed_by = NULL,
                        claim_token = NULL, claimed_at = NULL,
                        claim_expires_at = NULL
                    WHERE outbox_key = ?
                    """,
                    (str(outbox_key),),
                )
                connection.execute(
                    """
                    UPDATE agent_tasks SET mailbox_delivered = 1
                    WHERE task_id = ? AND run_count = ?
                    """,
                    key,
                )
                continue
            if mailbox_exists is not None:
                connection.execute(
                    """
                    DELETE FROM agent_mailbox_messages
                    WHERE (idempotency_key = ? OR message_id = ?)
                      AND status IN ('pending', 'claimed', 'retracted')
                    """,
                    (str(outbox_key), str(message_id)),
                )
            connection.execute(
                "DELETE FROM agent_task_settled_outbox WHERE outbox_key = ?",
                (str(outbox_key),),
            )
            connection.execute(
                """
                UPDATE agent_tasks SET mailbox_delivered = 0
                WHERE task_id = ? AND run_count = ?
                """,
                key,
            )
        return results

    def reconcile_legacy_settlements(
        self,
        *,
        limit: int = 250,
    ) -> LegacySettlementMigrationBatch:
        """Advance the indexed, durable migration watermark by one bounded page."""

        if limit < 1:
            raise ValueError("Legacy settlement migration limit must be positive")
        inserted = 0
        with self._database.transaction(immediate=True) as connection:
            migration = connection.execute(
                """
                SELECT cursor, completed
                FROM agent_storage_migrations
                WHERE name = 'settled_outbox_v1'
                """
            ).fetchone()
            if migration is not None and bool(migration[1]):
                return LegacySettlementMigrationBatch(inserted=0, completed=True)
            cursor = str(migration[0]) if migration is not None else ""
            rows = connection.execute(
                f"""
                SELECT {TASK_SELECT_COLUMNS}
                FROM agent_tasks
                WHERE task_id > ?
                ORDER BY task_id
                LIMIT ?
                """,
                (cursor, limit),
            ).fetchall()
            for row in rows:
                record = row_to_task_record(row)
                if record.mailbox_suppressed:
                    self.suppress_for_task_run_locked(
                        connection,
                        task_id=record.task_id,
                        run_count=record.run_count,
                    )
                    self.retract_mailbox_for_task_run_locked(
                        connection,
                        task_id=record.task_id,
                        run_count=record.run_count,
                    )
                    continue
                if record.mailbox_delivered:
                    self._confirm_delivered_for_task_run_locked(
                        connection,
                        task_id=record.task_id,
                        run_count=record.run_count,
                    )
                    continue
                intent = build_settled_outbox_intent(record)
                if intent is None:
                    continue
                inserted += int(self.insert_locked(connection, intent))
            completed = len(rows) < limit
            next_cursor = str(rows[-1][0]) if rows else cursor
            connection.execute(
                """
                INSERT INTO agent_storage_migrations (name, cursor, completed)
                VALUES ('settled_outbox_v1', ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    cursor = excluded.cursor,
                    completed = excluded.completed
                """,
                (next_cursor, int(completed)),
            )
        return LegacySettlementMigrationBatch(
            inserted=inserted,
            completed=completed,
        )

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
            self.discard_invalid_uncertain_locked(connection)
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
                SELECT outbox.outbox_key
                FROM agent_task_settled_outbox AS outbox
                WHERE outbox.status = 'pending'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM agent_tasks AS task
                      WHERE task.task_id = outbox.task_id
                        AND task.run_count = outbox.run_count
                        AND (task.mailbox_suppressed = 1
                             OR task.mailbox_delivered = 1)
                  )
                ORDER BY outbox.created_at, outbox.outbox_key
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

    @staticmethod
    def retract_mailbox_for_task_run_locked(
        connection: sqlite3.Connection,
        *,
        task_id: str,
        run_count: int,
    ) -> bool:
        """Retract current-run legacy mailbox rows when that table is present."""

        mailbox_exists = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'table' AND name = 'agent_mailbox_messages'
            """
        ).fetchone()
        if mailbox_exists is None:
            return False
        connection.execute(
            """
            UPDATE agent_mailbox_messages
            SET status = 'retracted', claimed_at = NULL,
                claim_expires_at = NULL, claimed_by = NULL, claim_token = NULL
            WHERE child_task_id = ? AND child_run_count = ?
              AND status IN ('pending', 'claimed')
            """,
            (task_id, run_count),
        )
        connection.execute(
            """
            UPDATE agent_task_settled_outbox
            SET retracted_at = COALESCE(retracted_at, ?)
            WHERE task_id = ? AND run_count = ? AND status = 'suppressed'
            """,
            (datetime.now(UTC).isoformat(), task_id, run_count),
        )
        return True

    @staticmethod
    def _confirm_delivered_for_task_run_locked(
        connection: sqlite3.Connection,
        *,
        task_id: str,
        run_count: int,
    ) -> None:
        connection.execute(
            """
            UPDATE agent_task_settled_outbox
            SET status = 'delivered', claimed_by = NULL, claim_token = NULL,
                claimed_at = NULL, claim_expires_at = NULL
            WHERE task_id = ? AND run_count = ?
              AND status IN ('pending', 'claimed')
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
