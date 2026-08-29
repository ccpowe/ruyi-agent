from __future__ import annotations

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable, cast
from urllib.parse import quote

import pytest

import ruyi_agent.storage.gateway_command_store as gateway_command_store_module
import ruyi_agent.storage.task_database as task_database_module
import ruyi_agent.storage.task_schema as task_schema_module
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.storage.task_store import TaskStore


NOW = "2026-08-30T00:00:00+00:00"
THREAD_COUNT = 12
STRESS_ROUND_COUNT = 60
URI_STRESS_ROUND_COUNT = 6


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


def _open_stores_concurrently(
    factory: Callable[[], object],
    *,
    repeat_count: int = 1,
) -> None:
    barrier = threading.Barrier(THREAD_COUNT)
    failure_lock = threading.Lock()
    first_failure: list[BaseException] = []

    def record_failure(exc: BaseException) -> None:
        with failure_lock:
            if not first_failure:
                first_failure.append(exc)
        barrier.abort()

    def open_repeatedly(_index: int) -> None:
        try:
            for _ in range(repeat_count):
                try:
                    barrier.wait(timeout=30)
                except threading.BrokenBarrierError as exc:
                    with failure_lock:
                        already_failed = bool(first_failure)
                    if already_failed:
                        return
                    record_failure(exc)
                    return
                store = factory()
                try:
                    count = getattr(store, "count_commands", None)
                    if count is not None:
                        assert count() == 1
                finally:
                    getattr(store, "close")()
        except BaseException as exc:
            record_failure(exc)

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [
            executor.submit(open_repeatedly, index) for index in range(THREAD_COUNT)
        ]
        for future in futures:
            future.result()

    if first_failure:
        raise first_failure[0]


def _open_aliases_concurrently(
    aliases: list[str],
    factory: Callable[[str], object],
) -> None:
    barrier = threading.Barrier(len(aliases))

    def open_store(alias: str) -> None:
        barrier.wait(timeout=30)
        store = factory(alias)
        try:
            count = getattr(store, "count_commands", None)
            if count is not None:
                assert count() == 1
        finally:
            getattr(store, "close")()

    with ThreadPoolExecutor(max_workers=len(aliases)) as executor:
        futures = [executor.submit(open_store, alias) for alias in aliases]
        for future in futures:
            future.result()


class _PeakTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active = 0
        self.peak = 0

    @contextmanager
    def hold(self) -> Iterator[None]:
        with self._lock:
            self._active += 1
            self.peak = max(self.peak, self._active)
        try:
            time.sleep(0.02)
            yield
        finally:
            with self._lock:
                self._active -= 1


def _disk_uri_aliases(db_path: Path) -> list[str]:
    encoded_path = quote(str(db_path), safe="/")
    dot_directory = db_path.parent / "uri-dot"
    dot_directory.mkdir(exist_ok=True)
    dotted_path = quote(f"{dot_directory}/../{db_path.name}", safe="/")
    return [
        f"file:{encoded_path}?mode=rwc&cache=shared",
        f"file://{encoded_path}?cache=shared&mode=rwc",
        f"file://localhost{encoded_path}?cache=shared&mode=rwc",
        f"file:{dotted_path}?cache=shared&mode=rwc",
    ]


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


def test_wal_negotiation_retries_only_locked_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class LockedThenReadyConnection:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, statement: str) -> None:
            assert statement == "PRAGMA journal_mode = WAL"
            self.calls += 1
            if self.calls < 3:
                raise sqlite3.OperationalError("database is locked")

    connection = LockedThenReadyConnection()
    delays: list[float] = []
    monkeypatch.setattr(task_database_module.time, "sleep", delays.append)

    task_database_module._enable_write_ahead_log(cast(sqlite3.Connection, connection))

    assert connection.calls == 3
    assert delays == [0.005, 0.01]


def test_wal_negotiation_preserves_non_lock_sqlite_error() -> None:
    class BrokenConnection:
        calls = 0

        def execute(self, statement: str) -> None:
            assert statement == "PRAGMA journal_mode = WAL"
            self.calls += 1
            raise sqlite3.OperationalError("disk I/O error")

    connection = BrokenConnection()
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        task_database_module._enable_write_ahead_log(
            cast(sqlite3.Connection, connection)
        )

    assert connection.calls == 1


def test_database_identity_canonicalizes_equivalent_disk_uri_spellings(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_directory = tmp_path / "uri identity"
    database_directory.mkdir()
    dot_directory = database_directory / "dot"
    dot_directory.mkdir()
    db_path = database_directory / "tasks #1.sqlite"
    encoded_path = quote(str(db_path), safe="/")
    dotted_path = quote(f"{dot_directory}/../{db_path.name}", safe="/")
    identities = {
        task_database_module._database_identity(candidate)
        for candidate in (
            str(db_path),
            f"file:{encoded_path}",
            f"file://{encoded_path}",
            f"file://localhost{encoded_path}",
            f"file:{dotted_path}",
        )
    }

    assert len(identities) == 1

    monkeypatch.chdir(database_directory)
    relative_identities = {
        task_database_module._database_identity(candidate)
        for candidate in (
            db_path.name,
            f"file:{quote(db_path.name)}",
            f"file:./{quote(db_path.name)}",
            f"file:dot/../{quote(db_path.name)}",
        )
    }
    assert relative_identities == identities

    tilde_directory = database_directory / "~"
    tilde_directory.mkdir()
    assert task_database_module._database_identity(
        "~/tilde.sqlite"
    ) == task_database_module._database_identity("file:~/tilde.sqlite")
    assert task_database_module._database_identity(
        f"file:{encoded_path}#ignored"
    ) == task_database_module._database_identity(str(db_path))


def test_database_identity_canonicalizes_query_without_losing_semantics(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "query.sqlite"
    uri = f"file:{quote(str(db_path), safe='/')}?"

    assert task_database_module._database_identity(
        f"{uri}mode=rwc&ca%63he=shared&label=a%2Bb"
    ) == task_database_module._database_identity(
        f"{uri}label=a+b&cache=shared&mode=rwc"
    )
    assert task_database_module._database_identity(
        f"{uri}label=a+b"
    ) != task_database_module._database_identity(f"{uri}label=a%20b")
    assert task_database_module._database_identity(
        f"{uri}mode=rwc&cache=private&label=x&cache=shared"
    ) == task_database_module._database_identity(
        f"{uri}cache=private&cache=shared&label=x&mode=rwc"
    )
    assert task_database_module._database_identity(
        f"{uri}cache=private&cache=shared"
    ) != task_database_module._database_identity(f"{uri}cache=shared&cache=private")
    assert task_database_module._database_identity(
        f"{uri}mode=ro"
    ) != task_database_module._database_identity(f"{uri}mode=rwc")


def test_database_identity_preserves_sqlite_named_memory_names() -> None:
    shared_aliases = (
        "file:round8-memory?mode=memory&cache=shared",
        "file:round8%2Dmemory?cache=shared&mode=memory",
    )
    absolute_aliases = (
        "file:/round8-memory?mode=memory&cache=shared",
        "file:///round8%2Dmemory?cache=shared&mode=memory",
        "file://localhost/round8-memory?cache=shared&mode=memory",
    )

    assert (
        len(
            {task_database_module._database_identity(alias) for alias in shared_aliases}
        )
        == 1
    )
    assert (
        len(
            {
                task_database_module._database_identity(alias)
                for alias in absolute_aliases
            }
        )
        == 1
    )
    assert task_database_module._database_identity(
        shared_aliases[0]
    ) != task_database_module._database_identity(
        "file:./round8-memory?mode=memory&cache=shared"
    )
    assert task_database_module._database_identity(
        shared_aliases[0]
    ) != task_database_module._database_identity(
        "file:other-memory?mode=memory&cache=shared"
    )


def test_task_store_uri_aliases_share_complete_initialization_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _PeakTracker()
    original_lock = task_database_module.database_initialization_lock

    @contextmanager
    def tracked_lock(db_path: str) -> Iterator[None]:
        with original_lock(db_path):
            with tracker.hold():
                yield

    monkeypatch.setattr(
        task_database_module,
        "database_initialization_lock",
        tracked_lock,
    )

    for round_index in range(URI_STRESS_ROUND_COUNT):
        db_path = tmp_path / "task uri alias" / f"legacy-{round_index}.sqlite"
        db_path.parent.mkdir(exist_ok=True)
        _create_legacy_task_database(db_path)
        aliases = _disk_uri_aliases(db_path) * 2
        _open_aliases_concurrently(aliases, TaskStore)

    assert tracker.peak == 1


def test_command_store_uri_aliases_share_recovery_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _PeakTracker()
    original_lock = gateway_command_store_module.database_initialization_lock

    @contextmanager
    def tracked_lock(db_path: str) -> Iterator[None]:
        with original_lock(db_path):
            with tracker.hold():
                yield

    monkeypatch.setattr(
        gateway_command_store_module,
        "database_initialization_lock",
        tracked_lock,
    )

    for round_index in range(URI_STRESS_ROUND_COUNT):
        db_path = tmp_path / "command uri alias" / f"legacy-{round_index}.sqlite"
        db_path.parent.mkdir(exist_ok=True)
        _create_legacy_command_database(db_path)
        aliases = _disk_uri_aliases(db_path) * 2
        _open_aliases_concurrently(aliases, GatewayCommandStore)

    assert tracker.peak == 1


def test_distinct_disk_databases_initialize_in_parallel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _PeakTracker()
    original_lock = task_database_module.database_initialization_lock

    @contextmanager
    def tracked_lock(db_path: str) -> Iterator[None]:
        with original_lock(db_path):
            with tracker.hold():
                yield

    monkeypatch.setattr(
        task_database_module,
        "database_initialization_lock",
        tracked_lock,
    )
    paths = [tmp_path / f"distinct-{index}.sqlite" for index in range(2)]
    for db_path in paths:
        _create_legacy_task_database(db_path)

    _open_aliases_concurrently([str(path) for path in paths], TaskStore)

    assert tracker.peak >= 2


def test_named_memory_aliases_serialize_but_distinct_names_do_not(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tracker = _PeakTracker()
    original_lock = task_database_module.database_initialization_lock

    @contextmanager
    def tracked_lock(db_path: str) -> Iterator[None]:
        with original_lock(db_path):
            with tracker.hold():
                yield

    monkeypatch.setattr(
        task_database_module,
        "database_initialization_lock",
        tracked_lock,
    )
    name = f"round8-{tmp_path.name}"
    aliases = [
        f"file:{name}?mode=memory&cache=shared",
        f"file:{name.replace('-', '%2D', 1)}?cache=shared&mode=memory",
    ] * 4
    anchor = sqlite3.connect(aliases[0], uri=True)
    try:
        _open_aliases_concurrently(aliases, TaskStore)
        assert anchor.execute(
            "SELECT name FROM sqlite_master WHERE name = 'agent_tasks'"
        ).fetchone() == ("agent_tasks",)
    finally:
        anchor.close()
    assert tracker.peak == 1

    tracker.peak = 0
    distinct_names = [
        f"file:{name}-{index}?mode=memory&cache=shared" for index in range(2)
    ]
    _open_aliases_concurrently(distinct_names, TaskStore)

    assert tracker.peak >= 2


def test_legacy_task_schema_initializes_concurrently_without_partial_migration(
    tmp_path: Path,
) -> None:
    for round_index in range(STRESS_ROUND_COUNT):
        db_path = tmp_path / f"legacy-tasks-{round_index}.sqlite"
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
    for round_index in range(STRESS_ROUND_COUNT):
        db_path = tmp_path / f"legacy-commands-{round_index}.sqlite"
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


def test_task_store_constructor_preserves_first_concurrent_failure_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "task-constructor-failure.sqlite"
    _create_legacy_task_database(db_path)
    original_create_indexes = task_schema_module._create_indexes
    original_close = TaskDatabase.close
    injection_lock = threading.Lock()
    close_lock = threading.Lock()
    should_fail = True
    close_count = 0

    def fail_first_initialization(connection: sqlite3.Connection) -> None:
        nonlocal should_fail
        original_create_indexes(connection)
        with injection_lock:
            if should_fail:
                should_fail = False
                connection.execute("SELECT * FROM sentinel_task_initialization_failure")

    def track_close(database: TaskDatabase) -> None:
        nonlocal close_count
        original_close(database)
        with close_lock:
            close_count += 1

    monkeypatch.setattr(
        task_schema_module,
        "_create_indexes",
        fail_first_initialization,
    )
    monkeypatch.setattr(TaskDatabase, "close", track_close)

    with pytest.raises(
        sqlite3.OperationalError,
        match="no such table: sentinel_task_initialization_failure",
    ):
        _open_stores_concurrently(
            lambda: TaskStore(str(db_path)),
            repeat_count=1,
        )

    assert close_count == THREAD_COUNT


def test_command_store_constructor_preserves_first_concurrent_failure_and_closes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "command-constructor-failure.sqlite"
    _create_legacy_command_database(db_path)
    original_initialize = (
        gateway_command_store_module._initialize_gateway_command_schema
    )
    original_close = GatewayCommandStore.close
    injection_lock = threading.Lock()
    close_lock = threading.Lock()
    should_fail = True
    close_count = 0

    def fail_first_initialization(connection: sqlite3.Connection) -> None:
        nonlocal should_fail
        original_initialize(connection)
        with injection_lock:
            if should_fail:
                should_fail = False
                connection.execute(
                    "SELECT * FROM sentinel_command_initialization_failure"
                )

    def track_close(store: GatewayCommandStore) -> None:
        nonlocal close_count
        original_close(store)
        with close_lock:
            close_count += 1

    monkeypatch.setattr(
        gateway_command_store_module,
        "_initialize_gateway_command_schema",
        fail_first_initialization,
    )
    monkeypatch.setattr(GatewayCommandStore, "close", track_close)

    with pytest.raises(
        sqlite3.OperationalError,
        match="no such table: sentinel_command_initialization_failure",
    ):
        _open_stores_concurrently(
            lambda: GatewayCommandStore(str(db_path)),
            repeat_count=1,
        )

    assert close_count == THREAD_COUNT


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
