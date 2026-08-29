from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime

from ruyi_agent.storage.task_database import (
    TaskDatabase,
    configure_connection_for_initialization,
)


REMOTE_TASK_PUBLIC_ERROR = "Remote Gateway Task failed"
REMOTE_PUBLIC_PROJECTION_MIGRATION = "remote_public_projection_v1"


def initialize_task_database(database: TaskDatabase) -> None:
    """Create the current schema and run compatible, idempotent migrations."""

    with database.initialization():
        with database.locked_connection() as connection:
            configure_connection_for_initialization(
                connection,
                db_path=database.db_path,
                foreign_keys=True,
            )
        with database.transaction(immediate=True) as connection:
            _create_tables(connection)
            _ensure_legacy_columns(connection)
            sanitize_legacy_remote_public_projections(connection)
            backfill_pending_reviews(connection)
            _backfill_pending_review_cursor_order(connection)
            _backfill_pending_review_ingest_sequences(connection)
            _create_indexes(connection)


def _create_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_tasks (
            task_id TEXT PRIMARY KEY,
            agent_name TEXT NOT NULL,
            state TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            parent_task_id TEXT,
            root_task_id TEXT NOT NULL,
            depth INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            result TEXT,
            error TEXT,
            run_count INTEGER NOT NULL,
            route_kind TEXT NOT NULL,
            upstream_task_id TEXT,
            parent_thread_id TEXT,
            mailbox_suppressed INTEGER NOT NULL DEFAULT 0,
            mailbox_delivered INTEGER NOT NULL DEFAULT 0,
            webhook_json TEXT,
            delegation_root_id TEXT,
            delegation_max_depth INTEGER,
            delegation_max_tasks_per_root INTEGER,
            delegation_visited_nodes_json TEXT NOT NULL DEFAULT '[]',
            permission_profile TEXT NOT NULL DEFAULT '',
            effective_skill_names_json TEXT NOT NULL DEFAULT '[]',
            skill_view_path TEXT,
            skill_view_hash TEXT,
            pending_review_json TEXT,
            artifacts_json TEXT NOT NULL DEFAULT '[]',
            external_operation TEXT,
            external_operation_identity TEXT,
            external_operation_run_count INTEGER,
            external_outcome_uncertain INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_task_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            run_count INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            data_json TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_task_pending_reviews (
            review_id TEXT PRIMARY KEY,
            task_id TEXT NOT NULL UNIQUE,
            root_task_id TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            ingest_sequence INTEGER,
            cursor_order_updated_at TEXT,
            FOREIGN KEY(task_id) REFERENCES agent_tasks(task_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_task_review_ingest_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            last_sequence INTEGER NOT NULL CHECK(last_sequence >= 0)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_task_settled_outbox (
            outbox_key TEXT PRIMARY KEY,
            message_id TEXT NOT NULL UNIQUE,
            task_id TEXT NOT NULL,
            run_count INTEGER NOT NULL,
            recipient_task_id TEXT,
            recipient_thread_id TEXT NOT NULL,
            child_agent_name TEXT NOT NULL,
            settled_status TEXT NOT NULL,
            content TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            claimed_at TEXT,
            claim_expires_at TEXT,
            claimed_by TEXT,
            claim_token TEXT,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            delivered_at TEXT,
            retracted_at TEXT,
            UNIQUE(task_id, run_count),
            FOREIGN KEY(task_id) REFERENCES agent_tasks(task_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_storage_migrations (
            name TEXT PRIMARY KEY,
            cursor TEXT NOT NULL DEFAULT '',
            completed INTEGER NOT NULL DEFAULT 0
        )
        """
    )


def _create_indexes(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_task_pending_reviews_ingest
        ON agent_task_pending_reviews(ingest_sequence)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_task_pending_reviews_root_ingest
        ON agent_task_pending_reviews(root_task_id, ingest_sequence, review_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_task_pending_reviews_root
        ON agent_task_pending_reviews(root_task_id, created_at, review_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_task_events_task_run_event
        ON agent_task_events(task_id, run_count, event_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_tasks_root_task_id
        ON agent_tasks(root_task_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_task_settled_outbox_delivery
        ON agent_task_settled_outbox(status, claim_expires_at, created_at)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_agent_task_settled_outbox_task_run_status
        ON agent_task_settled_outbox(task_id, run_count, status)
        """
    )


def _ensure_legacy_columns(connection: sqlite3.Connection) -> None:
    for column, definition in (
        ("permission_profile", "TEXT NOT NULL DEFAULT ''"),
        ("pending_review_json", "TEXT"),
        ("effective_skill_names_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("skill_view_path", "TEXT"),
        ("skill_view_hash", "TEXT"),
        ("artifacts_json", "TEXT NOT NULL DEFAULT '[]'"),
        ("external_operation", "TEXT"),
        ("external_operation_identity", "TEXT"),
        ("external_operation_run_count", "INTEGER"),
        ("external_outcome_uncertain", "INTEGER NOT NULL DEFAULT 0"),
    ):
        _ensure_column(
            connection,
            table="agent_tasks",
            column=column,
            definition=definition,
        )
    _ensure_column(
        connection,
        table="agent_task_pending_reviews",
        column="ingest_sequence",
        definition="INTEGER",
    )
    _ensure_column(
        connection,
        table="agent_task_pending_reviews",
        column="cursor_order_updated_at",
        definition="TEXT",
    )


def _ensure_column(
    connection: sqlite3.Connection,
    *,
    table: str,
    column: str,
    definition: str,
) -> None:
    columns = {
        row[1] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in columns:
        connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def sanitize_legacy_remote_public_projections(
    connection: sqlite3.Connection,
) -> None:
    """Idempotently remove pre-trust-boundary remote data from public storage.

    The migration runs on every open so a database that was only partly upgraded,
    or whose mailbox table was created after ``TaskStore``, is repaired before a
    notification lease can be acquired. ``upstream_task_id`` remains the private
    routing binding; every public Task and notification identity uses ``task_id``.
    """

    tables = _table_names(connection)
    if "agent_tasks" not in tables:
        return
    remote_rows = connection.execute(
        """
        SELECT task_id, upstream_task_id, state, run_count, error,
               pending_review_json
        FROM agent_tasks
        WHERE route_kind = 'remote_ref'
        """
    ).fetchall()
    if not remote_rows:
        _record_remote_projection_migration(connection, tables=tables)
        return

    remote_errors: dict[str, str] = {}
    for (
        task_id_value,
        _upstream_task_id,
        _state,
        _run_count,
        error,
        pending_review_json,
    ) in remote_rows:
        task_id = str(task_id_value)
        if error is not None:
            remote_errors[task_id] = str(error)
        connection.execute(
            """
            UPDATE agent_tasks
            SET thread_id = task_id,
                error = CASE
                    WHEN error IS NULL THEN NULL
                    ELSE ?
                END,
                pending_review_json = ?
            WHERE task_id = ? AND route_kind = 'remote_ref'
            """,
            (
                REMOTE_TASK_PUBLIC_ERROR,
                _public_pending_review_json(pending_review_json, task_id=task_id),
                task_id,
            ),
        )

    if "agent_task_pending_reviews" in tables:
        _sanitize_remote_pending_reviews(connection)
    if "agent_task_events" in tables:
        _sanitize_remote_task_events(connection)
    if "agent_task_settled_outbox" in tables:
        _sanitize_remote_settled_outbox(
            connection,
            remote_errors=remote_errors,
        )
    if "agent_mailbox_messages" in tables:
        _sanitize_remote_mailbox_messages(
            connection,
            remote_errors=remote_errors,
        )
    _record_remote_projection_migration(connection, tables=tables)


def _sanitize_remote_pending_reviews(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        """
        SELECT review.review_id, review.task_id, review.payload_json
        FROM agent_task_pending_reviews AS review
        JOIN agent_tasks AS task ON task.task_id = review.task_id
        WHERE task.route_kind = 'remote_ref'
        """
    ).fetchall()
    for review_id, task_id_value, payload_json in rows:
        task_id = str(task_id_value)
        connection.execute(
            """
            UPDATE agent_task_pending_reviews
            SET payload_json = ?
            WHERE review_id = ?
            """,
            (
                _public_pending_review_json(payload_json, task_id=task_id),
                review_id,
            ),
        )


def _sanitize_remote_task_events(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        """
        SELECT event.event_id, event.task_id, event.data_json
        FROM agent_task_events AS event
        JOIN agent_tasks AS task ON task.task_id = event.task_id
        WHERE task.route_kind = 'remote_ref'
        """
    ).fetchall()
    for event_id, task_id_value, data_json in rows:
        task_id = str(task_id_value)
        public_json = _public_remote_event_json(data_json, task_id=task_id)
        connection.execute(
            "UPDATE agent_task_events SET data_json = ? WHERE event_id = ?",
            (public_json, event_id),
        )


def _sanitize_remote_settled_outbox(
    connection: sqlite3.Connection,
    *,
    remote_errors: dict[str, str],
) -> None:
    rows = connection.execute(
        """
        SELECT outbox.outbox_key, outbox.task_id, outbox.run_count,
               outbox.content, outbox.status, outbox.settled_status,
               task.state, task.run_count
        FROM agent_task_settled_outbox AS outbox
        JOIN agent_tasks AS task ON task.task_id = outbox.task_id
        WHERE task.route_kind = 'remote_ref'
          AND outbox.status != 'delivered'
        """
    ).fetchall()
    for (
        outbox_key,
        task_id_value,
        outbox_run_count,
        content,
        status,
        settled_status,
        task_state,
        task_run_count,
    ) in rows:
        task_id = str(task_id_value)
        public_content = _public_remote_settlement_content(
            content=str(content),
            old_error=remote_errors.get(task_id),
            settled_status=str(settled_status),
            task_state=str(task_state),
            is_current_run=int(outbox_run_count) == int(task_run_count),
        )
        connection.execute(
            """
            UPDATE agent_task_settled_outbox
            SET content = ?,
                status = CASE WHEN status = 'claimed' THEN 'pending' ELSE status END,
                claimed_at = NULL, claim_expires_at = NULL,
                claimed_by = NULL, claim_token = NULL, last_error = NULL
            WHERE outbox_key = ? AND status = ?
            """,
            (public_content, outbox_key, status),
        )


def _sanitize_remote_mailbox_messages(
    connection: sqlite3.Connection,
    *,
    remote_errors: dict[str, str],
) -> None:
    rows = connection.execute(
        """
        SELECT message_id, idempotency_key, recipient_task_id,
               recipient_thread_id, sender_task_id, sender_agent_name,
               child_task_id, child_agent_name, child_run_count,
               settled_status, content, status
        FROM agent_mailbox_messages
        WHERE status IN ('pending', 'claimed', 'retracted')
          AND child_run_count IS NOT NULL
          AND settled_status IN ('completed', 'failed', 'cancelled', 'interrupted')
        """
    ).fetchall()
    resolved: list[dict[str, object]] = []
    for row in rows:
        message = _legacy_mailbox_message(row)
        candidates = _legacy_remote_task_candidates(connection, message=message)
        (
            binding,
            linked_outbox_keys,
            authoritative_local,
        ) = _resolve_legacy_remote_mailbox_binding(
            connection,
            message=message,
            candidates=candidates,
            remote_errors=remote_errors,
        )
        if authoritative_local:
            continue
        if not candidates and not linked_outbox_keys:
            continue
        if message["status"] == "retracted" or binding is None:
            _isolate_legacy_remote_mailbox(
                connection,
                message=message,
                linked_outbox_keys=linked_outbox_keys,
            )
            continue
        resolved.append(binding)

    expected_key_counts: dict[str, int] = {}
    for binding in resolved:
        key = str(binding["outbox_key"])
        expected_key_counts[key] = expected_key_counts.get(key, 0) + 1
    for binding in resolved:
        message_id = str(binding["message_id"])
        outbox_key = str(binding["outbox_key"])
        key_owners = connection.execute(
            """
            SELECT message_id
            FROM agent_mailbox_messages
            WHERE idempotency_key = ? AND message_id != ?
            """,
            (outbox_key, message_id),
        ).fetchall()
        if expected_key_counts[outbox_key] != 1 or key_owners:
            _isolate_legacy_remote_mailbox(
                connection,
                message=binding,
                linked_outbox_keys=tuple(binding["linked_outbox_keys"]),
            )
            continue
        connection.execute(
            """
            UPDATE agent_mailbox_messages
            SET idempotency_key = ?, sender_task_id = ?, sender_agent_name = ?,
                child_task_id = ?, child_agent_name = ?, content = ?,
                status = CASE WHEN status = 'claimed' THEN 'pending' ELSE status END,
                claimed_at = NULL, claim_expires_at = NULL,
                claimed_by = NULL, claim_token = NULL
            WHERE message_id = ? AND status IN ('pending', 'claimed')
            """,
            (
                outbox_key,
                binding["task_id"],
                binding["agent_name"],
                binding["task_id"],
                binding["agent_name"],
                binding["content"],
                message_id,
            ),
        )


def _legacy_mailbox_message(row: tuple[object, ...]) -> dict[str, object]:
    return {
        "message_id": str(row[0]),
        "idempotency_key": str(row[1]) if row[1] is not None else None,
        "recipient_task_id": str(row[2]) if row[2] is not None else None,
        "recipient_thread_id": str(row[3]),
        "sender_task_id": str(row[4]) if row[4] is not None else None,
        "sender_agent_name": str(row[5]) if row[5] is not None else None,
        "child_task_id": str(row[6]) if row[6] is not None else None,
        "child_agent_name": str(row[7]) if row[7] is not None else None,
        "child_run_count": int(row[8]),
        "settled_status": str(row[9]),
        "content": str(row[10]),
        "status": str(row[11]),
    }


def _legacy_remote_task_candidates(
    connection: sqlite3.Connection,
    *,
    message: dict[str, object],
) -> list[dict[str, object]]:
    rows = connection.execute(
        """
        SELECT task_id, upstream_task_id, agent_name, state, run_count,
               parent_task_id, parent_thread_id, mailbox_suppressed
        FROM agent_tasks
        WHERE route_kind = 'remote_ref'
          AND (? IN (task_id, upstream_task_id)
               OR ? IN (task_id, upstream_task_id))
        ORDER BY task_id
        """,
        (message["child_task_id"], message["sender_task_id"]),
    ).fetchall()
    return [
        {
            "task_id": str(row[0]),
            "upstream_task_id": str(row[1]) if row[1] is not None else None,
            "agent_name": str(row[2]),
            "state": str(row[3]),
            "run_count": int(row[4]),
            "parent_task_id": str(row[5]) if row[5] is not None else None,
            "parent_thread_id": str(row[6]) if row[6] is not None else None,
            "mailbox_suppressed": bool(row[7]),
        }
        for row in rows
    ]


def _resolve_legacy_remote_mailbox_binding(
    connection: sqlite3.Connection,
    *,
    message: dict[str, object],
    candidates: list[dict[str, object]],
    remote_errors: dict[str, str],
) -> tuple[dict[str, object] | None, tuple[str, ...], bool]:
    outbox_rows = connection.execute(
        """
        SELECT outbox.outbox_key, outbox.task_id, outbox.run_count,
               outbox.recipient_task_id, outbox.recipient_thread_id,
               outbox.child_agent_name, outbox.settled_status, outbox.content,
               outbox.status, task.route_kind
        FROM agent_task_settled_outbox AS outbox
        JOIN agent_tasks AS task ON task.task_id = outbox.task_id
        WHERE (outbox.message_id = ?
               OR (? IS NOT NULL AND outbox.outbox_key = ?))
        ORDER BY outbox.outbox_key
        """,
        (
            message["message_id"],
            message["idempotency_key"],
            message["idempotency_key"],
        ),
    ).fetchall()
    linked_outbox_keys = tuple(str(row[0]) for row in outbox_rows)
    if len(outbox_rows) > 1:
        return None, linked_outbox_keys, False
    if len(outbox_rows) == 1 and str(outbox_rows[0][9]) != "remote_ref":
        return None, linked_outbox_keys, True

    matching = [
        candidate
        for candidate in candidates
        if _legacy_mailbox_matches_task(message=message, task=candidate)
    ]
    outbox = outbox_rows[0] if outbox_rows else None
    if outbox is not None:
        matching = [
            candidate
            for candidate in matching
            if str(outbox[9]) == "remote_ref"
            and candidate["task_id"] == str(outbox[1])
            and _legacy_mailbox_matches_outbox(message=message, outbox=outbox)
        ]
    if len(matching) != 1:
        return None, linked_outbox_keys, False

    task = matching[0]
    task_id = str(task["task_id"])
    run_count = int(message["child_run_count"])
    outbox_key = (
        str(outbox[0])
        if outbox is not None
        else (
            f"settled:{message['recipient_thread_id']}:{task_id}:{run_count}"
        )
    )
    content = _public_remote_settlement_content(
        content=str(message["content"]),
        old_error=remote_errors.get(task_id),
        settled_status=str(message["settled_status"]),
        task_state=str(task["state"]),
        is_current_run=run_count == int(task["run_count"]),
    )
    return (
        {
            **message,
            "task_id": task_id,
            "agent_name": task["agent_name"],
            "outbox_key": outbox_key,
            "content": content,
            "linked_outbox_keys": linked_outbox_keys,
        },
        linked_outbox_keys,
        False,
    )


def _legacy_mailbox_matches_task(
    *,
    message: dict[str, object],
    task: dict[str, object],
) -> bool:
    identities = {task["task_id"], task["upstream_task_id"]} - {None}
    return (
        message["child_task_id"] in identities
        and message["sender_task_id"] in identities
        and message["child_agent_name"] == task["agent_name"]
        and message["sender_agent_name"] == task["agent_name"]
        and message["recipient_task_id"] == task["parent_task_id"]
        and message["recipient_thread_id"] == task["parent_thread_id"]
    )


def _legacy_mailbox_matches_outbox(
    *,
    message: dict[str, object],
    outbox: tuple[object, ...],
) -> bool:
    return (
        int(outbox[2]) == message["child_run_count"]
        and (str(outbox[3]) if outbox[3] is not None else None)
        == message["recipient_task_id"]
        and str(outbox[4]) == message["recipient_thread_id"]
        and str(outbox[5]) == message["child_agent_name"]
        and str(outbox[5]) == message["sender_agent_name"]
        and str(outbox[6]) == message["settled_status"]
    )


def _isolate_legacy_remote_mailbox(
    connection: sqlite3.Connection,
    *,
    message: dict[str, object],
    linked_outbox_keys: tuple[str, ...] = (),
) -> None:
    connection.execute(
        """
        UPDATE agent_mailbox_messages
        SET idempotency_key = NULL,
            sender_task_id = NULL, sender_agent_name = NULL,
            child_task_id = NULL, child_agent_name = NULL,
            content = ?, status = 'retracted',
            claimed_at = NULL, claim_expires_at = NULL,
            claimed_by = NULL, claim_token = NULL
        WHERE message_id = ? AND status != 'delivered'
        """,
        (REMOTE_TASK_PUBLIC_ERROR, message["message_id"]),
    )
    if not linked_outbox_keys:
        return
    placeholders = ",".join("?" for _ in linked_outbox_keys)
    connection.execute(
        f"""
        UPDATE agent_task_settled_outbox
        SET status = 'suppressed', claimed_at = NULL, claim_expires_at = NULL,
            claimed_by = NULL, claim_token = NULL, last_error = NULL,
            retracted_at = COALESCE(retracted_at, ?)
        WHERE outbox_key IN ({placeholders}) AND status != 'delivered'
        """,
        (datetime.now(UTC).isoformat(), *linked_outbox_keys),
    )


def _public_remote_settlement_content(
    *,
    content: str,
    old_error: str | None,
    settled_status: str | None,
    task_state: str,
    is_current_run: bool,
) -> str:
    if old_error is not None and content == old_error:
        return REMOTE_TASK_PUBLIC_ERROR
    if settled_status in {"failed", "interrupted"}:
        return REMOTE_TASK_PUBLIC_ERROR
    if is_current_run and task_state in {"failed", "interrupted"}:
        return REMOTE_TASK_PUBLIC_ERROR
    return content


def _public_remote_event_json(value: object, *, task_id: str) -> str:
    try:
        payload = json.loads(value) if isinstance(value, str) else None
    except json.JSONDecodeError:
        return json.dumps({}, ensure_ascii=True, sort_keys=True)
    if not isinstance(payload, dict):
        return json.dumps({}, ensure_ascii=True, sort_keys=True)
    if payload.get("error") is not None:
        payload["error"] = REMOTE_TASK_PUBLIC_ERROR
        payload.pop("error_truncated", None)
    if "task_id" in payload:
        payload["task_id"] = task_id
    if "thread_id" in payload:
        payload["thread_id"] = task_id
    if "pending_review" in payload:
        payload["pending_review"] = _public_pending_review(
            payload.get("pending_review"),
            task_id=task_id,
        )
    return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def _public_pending_review_json(value: object, *, task_id: str) -> str | None:
    if value is None:
        return None
    try:
        payload = json.loads(value) if isinstance(value, str) else None
    except json.JSONDecodeError:
        return None
    public = _public_pending_review(payload, task_id=task_id)
    if public is None:
        return None
    return json.dumps(public, ensure_ascii=True, sort_keys=True)


def _public_pending_review(
    value: object,
    *,
    task_id: str,
) -> dict[str, object] | None:
    if not isinstance(value, dict):
        return None
    public = dict(value)
    if "source_task_id" in public:
        public["source_task_id"] = task_id
    return public


def _record_remote_projection_migration(
    connection: sqlite3.Connection,
    *,
    tables: set[str],
) -> None:
    if "agent_storage_migrations" not in tables:
        return
    connection.execute(
        """
        INSERT INTO agent_storage_migrations (name, cursor, completed)
        VALUES (?, '', 1)
        ON CONFLICT(name) DO UPDATE SET completed = 1
        """,
        (REMOTE_PUBLIC_PROJECTION_MIGRATION,),
    )


def _table_names(connection: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def backfill_pending_reviews(connection: sqlite3.Connection) -> None:
    """Upgrade legacy review mirrors and rebuild root compatibility views."""

    rows = connection.execute(
        """
        SELECT task_id, root_task_id, pending_review_json, updated_at
        FROM agent_tasks
        WHERE state = 'waiting_for_human' AND pending_review_json IS NOT NULL
        """
    ).fetchall()
    for task_id, root_task_id, payload_json, updated_at in rows:
        try:
            payload = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        review_id = payload.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            continue
        connection.execute(
            """
            INSERT OR IGNORE INTO agent_task_pending_reviews (
                review_id, task_id, root_task_id, payload_json, created_at, updated_at,
                ingest_sequence
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                review_id,
                task_id,
                root_task_id,
                json.dumps(payload, ensure_ascii=True, sort_keys=True),
                updated_at,
                updated_at,
            ),
        )

    review_roots = {
        str(row[0])
        for row in connection.execute(
            "SELECT DISTINCT root_task_id FROM agent_task_pending_reviews"
        ).fetchall()
    }
    for root_task_id in review_roots:
        selected = connection.execute(
            """
            SELECT task_id, payload_json
            FROM agent_task_pending_reviews
            WHERE root_task_id = ?
            ORDER BY created_at ASC, review_id ASC
            LIMIT 1
            """,
            (root_task_id,),
        ).fetchone()
        if selected is None:
            continue
        task_id, payload_json = selected
        payload = json.loads(payload_json)
        if task_id != root_task_id:
            payload["source_task_id"] = task_id
        connection.execute(
            "UPDATE agent_tasks SET pending_review_json = ? WHERE task_id = ?",
            (json.dumps(payload, ensure_ascii=True, sort_keys=True), root_task_id),
        )

    root_rows = connection.execute(
        """
        SELECT task_id, state, pending_review_json
        FROM agent_tasks
        WHERE task_id = root_task_id AND pending_review_json IS NOT NULL
        """
    ).fetchall()
    for task_id, state, payload_json in root_rows:
        if task_id in review_roots or state == "waiting_for_human":
            continue
        try:
            payload = json.loads(payload_json)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and "source_task_id" in payload:
            connection.execute(
                "UPDATE agent_tasks SET pending_review_json = NULL WHERE task_id = ?",
                (task_id,),
            )


def _backfill_pending_review_ingest_sequences(
    connection: sqlite3.Connection,
) -> None:
    """Assign stable local order to legacy reviews before adding uniqueness."""

    rows = connection.execute(
        """
        SELECT review_id, ingest_sequence
        FROM agent_task_pending_reviews
        ORDER BY created_at ASC, review_id ASC
        """
    ).fetchall()
    positive_sequences = [
        int(row[1]) for row in rows if isinstance(row[1], int) and int(row[1]) > 0
    ]
    state = connection.execute(
        """
        SELECT last_sequence
        FROM agent_task_review_ingest_state
        WHERE singleton = 1
        """
    ).fetchone()
    persisted_high_water = int(state[0]) if state is not None else 0
    next_sequence = (
        max(
            max(positive_sequences, default=0),
            persisted_high_water,
        )
        + 1
    )
    used: set[int] = set()
    for review_id, raw_sequence in rows:
        sequence = int(raw_sequence) if isinstance(raw_sequence, int) else 0
        if sequence > 0 and sequence not in used:
            used.add(sequence)
            continue
        if next_sequence > 2**63 - 1:
            raise OverflowError("Pending Review ingest sequence is exhausted")
        connection.execute(
            """
            UPDATE agent_task_pending_reviews
            SET ingest_sequence = ?
            WHERE review_id = ?
            """,
            (next_sequence, review_id),
        )
        used.add(next_sequence)
        next_sequence += 1
    high_water = max(max(used, default=0), persisted_high_water)
    connection.execute(
        """
        INSERT INTO agent_task_review_ingest_state (singleton, last_sequence)
        VALUES (1, ?)
        ON CONFLICT(singleton) DO UPDATE SET
            last_sequence = MAX(last_sequence, excluded.last_sequence)
        """,
        (high_water,),
    )


def _backfill_pending_review_cursor_order(connection: sqlite3.Connection) -> None:
    """Freeze the legacy v2 ordering timestamp without changing existing values."""

    connection.execute(
        """
        UPDATE agent_task_pending_reviews
        SET cursor_order_updated_at = updated_at
        WHERE cursor_order_updated_at IS NULL OR cursor_order_updated_at = ''
        """
    )
