from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


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
