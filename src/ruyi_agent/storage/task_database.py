from __future__ import annotations

import sqlite3
import threading
import time
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


_INITIALIZATION_LOCKS_GUARD = threading.Lock()
_INITIALIZATION_LOCKS: weakref.WeakValueDictionary[str, threading.RLock] = (
    weakref.WeakValueDictionary()
)
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_WAL_RETRY_INITIAL_DELAY_SECONDS = 0.005
_WAL_RETRY_MAX_DELAY_SECONDS = 0.1


@contextmanager
def database_initialization_lock(db_path: str) -> Iterator[None]:
    """Serialize complete startup for connections to the same SQLite database."""

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
    if db_path.startswith("file:") or db_path == ":memory:":
        return db_path
    return str(Path(db_path).expanduser().resolve())


def _is_memory_database(db_path: str) -> bool:
    normalized = db_path.lower()
    return normalized == ":memory:" or (
        normalized.startswith("file:")
        and (normalized.startswith("file::memory:") or "mode=memory" in normalized)
    )


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
