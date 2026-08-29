from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import sqlite3
import threading
from pathlib import Path
from typing import Callable

import pytest

import ruyi_agent.storage.gateway_command_store as gateway_command_store_module
import ruyi_agent.storage.task_schema as task_schema_module
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.storage.task_store import TaskStore


NOW = "2026-08-30T00:00:00+00:00"
THREAD_COUNT = 12
REPEAT_COUNT = 3


def _create_legacy_task_database(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            CREATE TABLE agent_tasks (
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
            );
            CREATE TABLE agent_task_pending_reviews (
                review_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL UNIQUE,
                root_task_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES agent_tasks(task_id)
            );
            """
        )
        for suffix in ("a", "b"):
            task_id = f"task-{suffix}"
            private_id = f"private-{suffix}"
            review_id = f"review-{suffix}"
            review_json = json.dumps(
                {
                    "review_id": review_id,
                    "source_task_id": private_id,
                },
                sort_keys=True,
            )
            connection.execute(
                """
                INSERT INTO agent_tasks (
                    task_id, agent_name, state, thread_id, parent_task_id,
                    root_task_id, depth, created_at, updated_at, result, error,
                    run_count, route_kind, upstream_task_id, parent_thread_id,
                    mailbox_suppressed, mailbox_delivered, webhook_json,
                    delegation_root_id, delegation_max_depth,
                    delegation_max_tasks_per_root, delegation_visited_nodes_json,
                    permission_profile, effective_skill_names_json,
                    skill_view_path, skill_view_hash, pending_review_json,
                    artifacts_json
                ) VALUES (?, 'remote', 'waiting_for_human', ?, NULL, ?, 0, ?, ?,
                          NULL, ?, 1, 'remote_ref', ?, NULL, 0, 0, NULL, NULL,
                          NULL, NULL, '[]', '', '[]', NULL, NULL, ?, '[]')
                """,
                (
                    task_id,
                    private_id,
                    task_id,
                    NOW,
                    NOW,
                    f"private failure {private_id}",
                    private_id,
                    review_json,
                ),
            )
            if suffix == "a":
                connection.execute(
                    """
                    INSERT INTO agent_task_pending_reviews (
                        review_id, task_id, root_task_id, payload_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (review_id, task_id, task_id, review_json, NOW, NOW),
                )
        connection.commit()
    finally:
        connection.close()


def _create_legacy_command_database(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            CREATE TABLE gateway_commands (
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
            );
            INSERT INTO gateway_commands (
                command_id, principal_id, idempotency_key, operation, target,
                request_hash, state, task_id, mailbox_message_id, claim_token,
                response_json, created_at, updated_at
            ) VALUES (
                'command-1', 'principal-1', 'key-1', 'create_task', 'main',
                'hash-1', 'processing', 'task-1', NULL, 'claim-1', NULL,
                '2026-08-30T00:00:00+00:00', '2026-08-30T00:00:00+00:00'
            );
            """
        )
        connection.commit()
    finally:
        connection.close()


def _open_stores_concurrently(factory: Callable[[], object]) -> None:
    barrier = threading.Barrier(THREAD_COUNT)

    def open_repeatedly(_index: int) -> None:
        for _ in range(REPEAT_COUNT):
            barrier.wait(timeout=30)
            store = factory()
            try:
                count = getattr(store, "count_commands", None)
                if count is not None:
                    assert count() == 1
            finally:
                getattr(store, "close")()

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        list(executor.map(open_repeatedly, range(THREAD_COUNT)))


def _column_names(connection: sqlite3.Connection, table: str) -> list[str]:
    return [
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
    ]


def _index_names(connection: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in connection.execute(f"PRAGMA index_list({table})").fetchall()
    }


def test_legacy_task_schema_initializes_concurrently_without_partial_migration(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-tasks.sqlite"
    _create_legacy_task_database(db_path)

    _open_stores_concurrently(lambda: TaskStore(str(db_path)))

    connection = sqlite3.connect(db_path)
    try:
        review_columns = _column_names(connection, "agent_task_pending_reviews")
        review_indexes = _index_names(connection, "agent_task_pending_reviews")
        reviews = connection.execute(
            """
            SELECT review_id, task_id, payload_json, ingest_sequence,
                   cursor_order_updated_at
            FROM agent_task_pending_reviews
            ORDER BY ingest_sequence
            """
        ).fetchall()
        tasks = connection.execute(
            """
            SELECT task_id, thread_id, error, pending_review_json
            FROM agent_tasks
            ORDER BY task_id
            """
        ).fetchall()
        high_water = connection.execute(
            """
            SELECT last_sequence FROM agent_task_review_ingest_state
            WHERE singleton = 1
            """
        ).fetchone()
        migration = connection.execute(
            """
            SELECT COUNT(*), MIN(completed), MAX(completed)
            FROM agent_storage_migrations
            WHERE name = 'remote_public_projection_v1'
            """
        ).fetchone()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    finally:
        connection.close()

    assert len(review_columns) == len(set(review_columns))
    assert {"ingest_sequence", "cursor_order_updated_at"} <= set(review_columns)
    assert {
        "idx_agent_task_pending_reviews_ingest",
        "idx_agent_task_pending_reviews_root_ingest",
        "idx_agent_task_pending_reviews_root",
    } <= review_indexes
    assert [(row[0], row[3], row[4]) for row in reviews] == [
        ("review-a", 1, NOW),
        ("review-b", 2, NOW),
    ]
    assert len({row[3] for row in reviews}) == 2
    for review_id, task_id, payload_json, _sequence, _cursor_order in reviews:
        assert json.loads(payload_json) == {
            "review_id": review_id,
            "source_task_id": task_id,
        }
    for task_id, thread_id, error, pending_review_json in tasks:
        assert thread_id == task_id
        assert error == "Remote Gateway Task failed"
        assert json.loads(pending_review_json)["source_task_id"] == task_id
    assert high_water == (2,)
    assert migration == (1, 1, 1)
    assert integrity == ("ok",)


def test_legacy_command_schema_initializes_concurrently_without_duplicate_columns(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-commands.sqlite"
    _create_legacy_command_database(db_path)

    _open_stores_concurrently(lambda: GatewayCommandStore(str(db_path)))

    connection = sqlite3.connect(db_path)
    try:
        columns = _column_names(connection, "gateway_commands")
        indexes = connection.execute("PRAGMA index_list(gateway_commands)").fetchall()
        command = connection.execute(
            """
            SELECT state, claim_token, error_json, effect_started, replay_safe
            FROM gateway_commands WHERE command_id = 'command-1'
            """
        ).fetchone()
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO gateway_commands (
                    command_id, principal_id, idempotency_key, operation,
                    target, request_hash, state, task_id, created_at, updated_at
                ) VALUES ('duplicate', 'principal-1', 'key-1', 'create_task',
                          'main', 'hash-1', 'pending', 'task-2', ?, ?)
                """,
                (NOW, NOW),
            )
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
    finally:
        connection.close()

    assert len(columns) == len(set(columns))
    assert {"error_json", "effect_started", "replay_safe"} <= set(columns)
    assert any(int(row[2]) == 1 for row in indexes)
    assert command == ("pending", None, None, 0, 1)
    assert integrity == ("ok",)


def test_task_schema_migration_rolls_back_as_one_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "task-rollback.sqlite"
    _create_legacy_task_database(db_path)
    database = TaskDatabase(str(db_path))
    original_create_indexes = task_schema_module._create_indexes

    with monkeypatch.context() as scoped:
        scoped.setattr(
            task_schema_module,
            "_create_indexes",
            lambda _connection: (_ for _ in ()).throw(RuntimeError("injected")),
        )
        with pytest.raises(RuntimeError, match="injected"):
            task_schema_module.initialize_task_database(database)

    with database.locked_connection() as connection:
        assert "ingest_sequence" not in _column_names(
            connection, "agent_task_pending_reviews"
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_task_pending_reviews"
        ).fetchone() == (1,)
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name = 'agent_task_review_ingest_state'"
            ).fetchone()
            is None
        )

    assert task_schema_module._create_indexes is original_create_indexes
    task_schema_module.initialize_task_database(database)
    with database.locked_connection() as connection:
        assert {"ingest_sequence", "cursor_order_updated_at"} <= set(
            _column_names(connection, "agent_task_pending_reviews")
        )
        assert connection.execute(
            "SELECT COUNT(*) FROM agent_task_pending_reviews"
        ).fetchone() == (2,)
    database.close()


def test_command_schema_migration_rolls_back_as_one_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "command-rollback.sqlite"
    _create_legacy_command_database(db_path)
    store = GatewayCommandStore.__new__(GatewayCommandStore)
    store._db_path = str(db_path)
    store._lock = threading.RLock()
    store._conn = sqlite3.connect(
        db_path,
        check_same_thread=False,
        timeout=30.0,
    )
    original_initialize = (
        gateway_command_store_module._initialize_gateway_command_schema
    )

    def fail_after_schema(connection: sqlite3.Connection) -> None:
        original_initialize(connection)
        raise RuntimeError("injected")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            gateway_command_store_module,
            "_initialize_gateway_command_schema",
            fail_after_schema,
        )
        with pytest.raises(RuntimeError, match="injected"):
            store._init_db()

    assert "error_json" not in _column_names(store._conn, "gateway_commands")
    store._init_db()
    assert {"error_json", "effect_started", "replay_safe"} <= set(
        _column_names(store._conn, "gateway_commands")
    )
    store.close()
