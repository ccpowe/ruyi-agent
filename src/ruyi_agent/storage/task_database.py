from __future__ import annotations

import os
import sqlite3
import threading
import time
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote_from_bytes, unquote_to_bytes, urlsplit


_INITIALIZATION_LOCKS_GUARD = threading.Lock()
_INITIALIZATION_LOCKS: weakref.WeakValueDictionary[str, threading.RLock] = (
    weakref.WeakValueDictionary()
)
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_WAL_RETRY_INITIAL_DELAY_SECONDS = 0.005
_WAL_RETRY_MAX_DELAY_SECONDS = 0.1
_URI_PATH_SAFE = "/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"


@contextmanager
def database_initialization_lock(db_path: str) -> Iterator[None]:
    """Serialize complete startup for connections to the same SQLite database."""

    if _is_private_database(db_path):
        yield
        return
    identity = _database_identity(db_path)
    with _INITIALIZATION_LOCKS_GUARD:
        lock = _INITIALIZATION_LOCKS.get(identity)
        if lock is None:
            lock = threading.RLock()
            _INITIALIZATION_LOCKS[identity] = lock
    with lock:
        yield


def configure_connection_for_initialization(
    connection: sqlite3.Connection,
    *,
    db_path: str,
    foreign_keys: bool = False,
) -> None:
    """Apply connection policy before an idempotent schema transaction."""

    connection.execute(f"PRAGMA busy_timeout = {_SQLITE_BUSY_TIMEOUT_MS}")
    if foreign_keys:
        connection.execute("PRAGMA foreign_keys = ON")
    if _is_memory_database(db_path):
        return
    _enable_write_ahead_log(connection)


def _database_identity(db_path: str) -> str:
    if not db_path.startswith("file:"):
        if db_path in {"", ":memory:"}:
            return "sqlite-private"
        return f"sqlite-file:{Path(db_path).resolve()}"

    authority, path, query = _file_uri_parts(db_path)
    query_identity = _canonical_query(query)
    if _uri_is_memory(path, query):
        identity = f"sqlite-memory:{quote_from_bytes(path, safe='/:')}"
    elif authority in {"", "localhost"}:
        filesystem_path = os.fsdecode(path)
        identity = f"sqlite-file:{Path(filesystem_path).resolve()}"
    else:
        identity = (
            f"sqlite-uri:{authority}:{quote_from_bytes(path, safe=_URI_PATH_SAFE)}"
        )
    if query_identity:
        return f"{identity}?{query_identity}"
    return identity


def _file_uri_parts(db_path: str) -> tuple[str, bytes, list[tuple[bytes, bytes]]]:
    parsed = urlsplit(db_path)
    path = unquote_to_bytes(parsed.path).split(b"\x00", 1)[0]
    query = []
    if parsed.query:
        for parameter in parsed.query.split("&"):
            key, separator, value = parameter.partition("=")
            query.append(
                (
                    unquote_to_bytes(key),
                    unquote_to_bytes(value if separator else ""),
                )
            )
    return parsed.netloc, path, query


def _canonical_query(query: list[tuple[bytes, bytes]]) -> str:
    # SQLite gives repeated parameters order-sensitive semantics. Python's sort is
    # stable, so this normalizes distinct parameter order without reordering values
    # for the same decoded key.
    ordered = sorted(query, key=lambda parameter: parameter[0])
    return "&".join(
        f"{quote_from_bytes(key, safe='')}={quote_from_bytes(value, safe='')}"
        for key, value in ordered
    )


def _last_uri_parameter(
    query: list[tuple[bytes, bytes]],
    key: bytes,
) -> bytes | None:
    values = [value for candidate, value in query if candidate == key]
    return values[-1] if values else None


def _uri_is_memory(path: bytes, query: list[tuple[bytes, bytes]]) -> bool:
    return path == b":memory:" or _last_uri_parameter(query, b"mode") == b"memory"


def _is_private_database(db_path: str) -> bool:
    if db_path in {"", ":memory:"}:
        return True
    if not db_path.startswith("file:"):
        return False
    _authority, path, query = _file_uri_parts(db_path)
    if not path:
        return True
    return _uri_is_memory(path, query) and (
        _last_uri_parameter(query, b"cache") != b"shared"
    )


def _is_memory_database(db_path: str) -> bool:
    if db_path == ":memory:":
        return True
    if not db_path.startswith("file:"):
        return False
    _authority, path, query = _file_uri_parts(db_path)
    return _uri_is_memory(path, query)


def _enable_write_ahead_log(connection: sqlite3.Connection) -> None:
    deadline = time.monotonic() + (_SQLITE_BUSY_TIMEOUT_MS / 1000)
    delay = _WAL_RETRY_INITIAL_DELAY_SECONDS
    while True:
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            return
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            time.sleep(min(delay, remaining))
            delay = min(delay * 2, _WAL_RETRY_MAX_DELAY_SECONDS)


class TaskDatabase:
    """Own the single connection and process-local lock for Task persistence.

    Repositories never commit independently while participating in a unit of
    work.  This object is the only owner of commit/rollback and lock ordering.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        if db_path not in {"", ":memory:"} and not db_path.startswith("file:"):
            parent = Path(db_path).expanduser().resolve().parent
            parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            timeout=30.0,
            uri=db_path.startswith("file:"),
        )

    @contextmanager
    def locked_connection(self) -> Iterator[sqlite3.Connection]:
        """Serialize a read or schema operation on the shared connection."""

        with self._lock:
            yield self._conn

    @contextmanager
    def initialization(self) -> Iterator[None]:
        """Hold the process-local startup lock for this database identity."""

        with database_initialization_lock(self.db_path):
            yield

    @contextmanager
    def transaction(
        self,
        *,
        immediate: bool = False,
    ) -> Iterator[sqlite3.Connection]:
        """Run one atomic write and roll it back on every failure path."""

        with self._lock:
            try:
                if immediate:
                    self._conn.execute("BEGIN IMMEDIATE")
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def close(self) -> None:
        with self._lock:
            self._conn.close()
