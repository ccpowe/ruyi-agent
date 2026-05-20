from __future__ import annotations

import json
import sqlite3
import threading
from asyncio import to_thread
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ruyi_agent.channels.http.api import MetadataScalar, TaskRouteRecord


class GatewayRouteStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self._init_db()

    def save_route(self, route: TaskRouteRecord) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO gateway_task_routes (
                    task_id,
                    agent_name,
                    metadata_json,
                    route_kind,
                    upstream_task_id,
                    webhook_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    route.task_id,
                    route.agent_name,
                    json.dumps(route.metadata, ensure_ascii=True, sort_keys=True),
                    route.route_kind,
                    route.upstream_task_id,
                    (
                        json.dumps(route.webhook, ensure_ascii=True, sort_keys=True)
                        if route.webhook is not None
                        else None
                    ),
                ),
            )
            self._conn.commit()

    async def asave_route(self, route: TaskRouteRecord) -> None:
        await to_thread(self.save_route, route)

    def get_route(self, task_id: str) -> TaskRouteRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT task_id, agent_name, metadata_json, route_kind,
                    upstream_task_id, webhook_json
                FROM gateway_task_routes
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_route(row)

    async def aget_route(self, task_id: str) -> TaskRouteRecord | None:
        return await to_thread(self.get_route, task_id)

    def list_routes(self) -> list[TaskRouteRecord]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT task_id, agent_name, metadata_json, route_kind,
                    upstream_task_id, webhook_json
                FROM gateway_task_routes
                """
            ).fetchall()
        return [self._row_to_route(row) for row in rows]

    async def alist_routes(self) -> list[TaskRouteRecord]:
        return await to_thread(self.list_routes)

    def get_route_by_upstream_task_id(
        self,
        upstream_task_id: str,
    ) -> TaskRouteRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT task_id, agent_name, metadata_json, route_kind,
                    upstream_task_id, webhook_json
                FROM gateway_task_routes
                WHERE upstream_task_id = ?
                """,
                (upstream_task_id,),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_route(row)

    async def aget_route_by_upstream_task_id(
        self,
        upstream_task_id: str,
    ) -> TaskRouteRecord | None:
        return await to_thread(self.get_route_by_upstream_task_id, upstream_task_id)

    def _ensure_parent_dir(self) -> None:
        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS gateway_task_routes (
                    task_id TEXT PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    route_kind TEXT NOT NULL,
                    upstream_task_id TEXT NOT NULL,
                    webhook_json TEXT
                )
                """
            )
            columns = {
                row[1]
                for row in self._conn.execute(
                    "PRAGMA table_info(gateway_task_routes)"
                ).fetchall()
            }
            if "webhook_json" not in columns:
                self._conn.execute(
                    "ALTER TABLE gateway_task_routes ADD COLUMN webhook_json TEXT"
                )
            self._conn.commit()

    def _row_to_route(
        self,
        row: tuple[str, str, str, str, str, str | None],
    ) -> TaskRouteRecord:
        from ruyi_agent.channels.http.api import TaskRouteRecord

        metadata_json = row[2]
        metadata = json.loads(metadata_json)
        webhook_json = row[5]
        webhook = json.loads(webhook_json) if webhook_json else None
        if not isinstance(webhook, dict):
            webhook = None
        return TaskRouteRecord(
            task_id=row[0],
            agent_name=row[1],
            metadata=metadata,
            route_kind=row[3],
            upstream_task_id=row[4],
            webhook=webhook,
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
