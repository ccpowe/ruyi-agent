from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import sqlite3
from pathlib import Path
from typing import cast
from urllib.parse import quote

import pytest

import ruyi_agent.storage.gateway_command_store as gateway_command_store_module
import ruyi_agent.storage.task_database as task_database_module
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.task_store import TaskStore


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
        f"{uri}mode=rwc&cache=private&label=old&cache=shared&la%62el=a%2Bb"
    ) == task_database_module._database_identity(
        f"{uri}label=a+b&cache=shared&mode=rwc"
    )
    assert task_database_module._database_identity(
        f"{uri}cache=private&cache=shared"
    ) == task_database_module._database_identity(f"{uri}cache=shared")
    assert task_database_module._database_identity(
        f"{uri}cache=private&cache=shared"
    ) != task_database_module._database_identity(f"{uri}cache=shared&cache=private")
    assert task_database_module._database_identity(
        f"{uri}mode=ro"
    ) != task_database_module._database_identity(f"{uri}mode=rwc")


def test_sqlite_cache_and_vfs_control_parameters_use_final_decoded_value(
    tmp_path: Path,
) -> None:
    memory_name = quote(f"control-{tmp_path.name}", safe="")
    shared_override = f"file:{memory_name}?mode=memory&cache=private&ca%63he=shared"
    shared_final = f"file:{memory_name}?cache=shared&mode=memory"
    first_shared = sqlite3.connect(shared_override, uri=True)
    second_shared = sqlite3.connect(shared_final, uri=True)
    try:
        first_shared.execute("CREATE TABLE shared_cache_evidence (value INTEGER)")
        first_shared.commit()
        assert second_shared.execute(
            "SELECT COUNT(*) FROM shared_cache_evidence"
        ).fetchone() == (0,)
    finally:
        first_shared.close()
        second_shared.close()

    private_uri = f"file:{memory_name}-private?mode=memory&cache=shared&ca%63he=private"
    first_private = sqlite3.connect(private_uri, uri=True)
    second_private = sqlite3.connect(private_uri, uri=True)
    try:
        first_private.execute("CREATE TABLE private_cache_evidence (value INTEGER)")
        first_private.commit()
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            second_private.execute("SELECT * FROM private_cache_evidence")
    finally:
        first_private.close()
        second_private.close()

    db_path = tmp_path / "vfs.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE vfs_evidence (value INTEGER)")
    connection.commit()
    connection.close()
    uri = f"file:{quote(str(db_path), safe='/')}?"
    vfs_override = f"{uri}v%66s=no_such_round10&vfs=unix"
    vfs_final = f"{uri}vfs=unix"
    assert task_database_module._database_identity(
        vfs_override
    ) == task_database_module._database_identity(vfs_final)
    connection = sqlite3.connect(vfs_override, uri=True)
    try:
        assert connection.execute("SELECT * FROM vfs_evidence").fetchall() == []
    finally:
        connection.close()
    with pytest.raises(sqlite3.OperationalError, match="no such vfs"):
        sqlite3.connect(f"{uri}vfs=unix&vfs=no_such_round10", uri=True)


def test_sqlite_ro_to_memory_uses_writable_shared_final_target(tmp_path: Path) -> None:
    memory_name = quote(f"mode-memory-{tmp_path.name}", safe="")
    override_uri = f"file:{memory_name}?mode=ro&mode=memory&cache=shared&label=current"
    final_uri = f"file:{memory_name}?label=current&cache=shared&mode=memory"
    assert task_database_module._database_identity(
        override_uri
    ) == task_database_module._database_identity(final_uri)

    anchor = sqlite3.connect(final_uri, uri=True)
    override = sqlite3.connect(override_uri, uri=True)
    second_final = sqlite3.connect(final_uri, uri=True)
    try:
        override.execute("CREATE TABLE memory_mode_evidence (value INTEGER)")
        override.execute("INSERT INTO memory_mode_evidence VALUES (1)")
        override.commit()
        assert anchor.execute("SELECT value FROM memory_mode_evidence").fetchall() == [
            (1,)
        ]
        second_final.execute("INSERT INTO memory_mode_evidence VALUES (2)")
        second_final.commit()
        assert anchor.execute(
            "SELECT value FROM memory_mode_evidence ORDER BY value"
        ).fetchall() == [(1,), (2,)]
    finally:
        anchor.close()
        override.close()
        second_final.close()


def test_sqlite_memory_to_disk_modes_use_final_filesystem_target(
    tmp_path: Path,
) -> None:
    for mode in ("ro", "rw"):
        db_path = tmp_path / f"memory-to-{mode}.sqlite"
        setup = sqlite3.connect(db_path)
        setup.execute("CREATE TABLE disk_mode_evidence (value INTEGER)")
        setup.execute("INSERT INTO disk_mode_evidence VALUES (1)")
        setup.commit()
        setup.close()
        uri = f"file:{quote(str(db_path), safe='/')}?"
        override_uri = f"{uri}mode=memory&mode={mode}"
        final_uri = f"{uri}mode={mode}"
        assert task_database_module._database_identity(
            override_uri
        ) == task_database_module._database_identity(final_uri)

        override = sqlite3.connect(override_uri, uri=True)
        final = sqlite3.connect(final_uri, uri=True)
        try:
            assert override.execute(
                "SELECT value FROM disk_mode_evidence"
            ).fetchall() == [(1,)]
            assert final.execute("SELECT value FROM disk_mode_evidence").fetchall() == [
                (1,)
            ]
            if mode == "ro":
                with pytest.raises(
                    sqlite3.OperationalError,
                    match="attempt to write a readonly database",
                ):
                    override.execute("INSERT INTO disk_mode_evidence VALUES (2)")
                override.rollback()
            else:
                override.execute("INSERT INTO disk_mode_evidence VALUES (2)")
                override.commit()
                assert final.execute(
                    "SELECT value FROM disk_mode_evidence ORDER BY value"
                ).fetchall() == [(1,), (2,)]
        finally:
            override.close()
            final.close()

    rwc_path = tmp_path / "memory-to-rwc.sqlite"
    uri = f"file:{quote(str(rwc_path), safe='/')}?"
    override_uri = f"{uri}mode=memory&mode=rwc"
    final_uri = f"{uri}mode=rwc"
    assert not rwc_path.exists()
    assert task_database_module._database_identity(
        override_uri
    ) == task_database_module._database_identity(final_uri)
    override = sqlite3.connect(override_uri, uri=True)
    final = sqlite3.connect(final_uri, uri=True)
    try:
        override.execute("CREATE TABLE disk_mode_evidence (value INTEGER)")
        override.execute("INSERT INTO disk_mode_evidence VALUES (1)")
        override.commit()
        assert rwc_path.is_file()
        assert final.execute("SELECT value FROM disk_mode_evidence").fetchall() == [
            (1,)
        ]
        final.execute("INSERT INTO disk_mode_evidence VALUES (2)")
        final.commit()
        assert override.execute(
            "SELECT value FROM disk_mode_evidence ORDER BY value"
        ).fetchall() == [(1,), (2,)]
    finally:
        override.close()
        final.close()


def test_sqlite_disk_mode_escalation_fails_before_initialization_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "mode.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE mode_evidence (value INTEGER)")
    connection.commit()
    connection.close()
    uri = f"file:{quote(str(db_path), safe='/')}?"

    for override, final in (
        ("mode=rwc&mode=rw", "mode=rw"),
        ("mode=rw&mode=ro", "mode=ro"),
    ):
        assert task_database_module._database_identity(
            f"{uri}{override}"
        ) == task_database_module._database_identity(f"{uri}{final}")
        connection = sqlite3.connect(f"{uri}{override}", uri=True)
        try:
            assert connection.execute("SELECT * FROM mode_evidence").fetchall() == []
        finally:
            connection.close()

    lock_calls = {"task": 0, "command": 0}

    @contextmanager
    def track_task_lock(_db_path: str) -> Iterator[None]:
        lock_calls["task"] += 1
        yield

    @contextmanager
    def track_command_lock(_db_path: str) -> Iterator[None]:
        lock_calls["command"] += 1
        yield

    monkeypatch.setattr(
        task_database_module,
        "database_initialization_lock",
        track_task_lock,
    )
    monkeypatch.setattr(
        gateway_command_store_module,
        "database_initialization_lock",
        track_command_lock,
    )
    with pytest.raises(
        sqlite3.OperationalError,
        match="access mode not allowed: rwc",
    ):
        TaskStore(f"{uri}mode=rw&mode=rwc")
    with pytest.raises(
        sqlite3.OperationalError,
        match="access mode not allowed: rw",
    ):
        GatewayCommandStore(f"{uri}mode=ro&mode=rw")

    assert lock_calls == {"task": 0, "command": 0}


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
