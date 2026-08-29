from __future__ import annotations

import json
import sqlite3

from ruyi_agent.storage.task_database import TaskDatabase


def initialize_task_database(database: TaskDatabase) -> None:
    """Create the current schema and run compatible, idempotent migrations."""

    with database.locked_connection() as connection:
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        if database.db_path != ":memory:":
            connection.execute("PRAGMA journal_mode = WAL")
        _create_tables(connection)
        _ensure_legacy_columns(connection)
        backfill_pending_reviews(connection)
        _backfill_pending_review_ingest_sequences(connection)
        _create_indexes(connection)
        connection.commit()


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
            artifacts_json TEXT NOT NULL DEFAULT '[]'
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
