from __future__ import annotations

import sqlite3
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable

import pytest

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


@pytest.fixture(autouse=True)
def close_sqlite_connections(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Close SQLite connections opened on the main test thread.

    Production stores expose ``close()``, but tests construct them through many
    different paths (including nested application factories). Tracking the
    common connection factory keeps teardown deterministic without duplicating
    finalizers throughout the suite. Connections created by aiosqlite's worker
    thread retain their native thread affinity and are closed by its async
    context manager.
    """

    original_connect = sqlite3.connect
    connections: list[tuple[int, sqlite3.Connection]] = []

    def tracked_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = original_connect(*args, **kwargs)
        connections.append((threading.get_ident(), connection))
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)

    yield

    current_thread = threading.get_ident()
    for owner_thread, connection in reversed(connections):
        if owner_thread == current_thread:
            connection.close()


@pytest.fixture
def run_http_server() -> Iterator[
    Callable[[type[BaseHTTPRequestHandler]], ThreadingHTTPServer]
]:
    """Start threaded test servers and close both their thread and socket."""

    running: list[tuple[ThreadingHTTPServer, threading.Thread]] = []

    def start(handler: type[BaseHTTPRequestHandler]) -> ThreadingHTTPServer:
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        running.append((server, thread))
        return server

    yield start

    for server, thread in reversed(running):
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
