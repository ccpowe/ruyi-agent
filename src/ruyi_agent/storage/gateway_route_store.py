from __future__ import annotations

import json
import sqlite3
import threading
from asyncio import to_thread
from datetime import UTC, datetime
from pathlib import Path
from ruyi_agent.task_models import (
    TASK_ROUTE_STATES,
    TaskRouteRecord,
    TaskRouteState,
)


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
        self._validate_route_state(route.route_state)
        route.updated_at = datetime.now(UTC)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                existing = self._conn.execute(
                    """
                    SELECT agent_name, route_kind, upstream_task_id, route_state,
                        created_at
                    FROM gateway_task_routes
                    WHERE task_id = ?
                    """,
                    (route.task_id,),
                ).fetchone()
                metadata_json = json.dumps(
                    route.metadata,
                    ensure_ascii=True,
                    sort_keys=True,
                )
                webhook_json = (
                    json.dumps(route.webhook, ensure_ascii=True, sort_keys=True)
                    if route.webhook is not None
                    else None
                )
                if existing is None:
                    self._conn.execute(
                        """
                        INSERT INTO gateway_task_routes (
                            task_id, agent_name, metadata_json, route_kind,
                            upstream_task_id, webhook_json, route_state, route_error,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            route.task_id,
                            route.agent_name,
                            metadata_json,
                            route.route_kind,
                            route.upstream_task_id,
                            webhook_json,
                            route.route_state,
                            route.route_error,
                            route.created_at.isoformat(),
                            route.updated_at.isoformat(),
                        ),
                    )
                else:
                    route.created_at = datetime.fromisoformat(str(existing[4]))
                    stable_binding = (
                        str(existing[0]),
                        str(existing[1]),
                    )
                    requested_binding = (
                        route.agent_name,
                        route.route_kind,
                    )
                    if stable_binding != requested_binding:
                        raise ValueError(
                            f"Gateway route binding conflict for task '{route.task_id}'"
                        )
                    if str(existing[3]) == "active" and route.route_state != "active":
                        raise ValueError(
                            f"Active Gateway route '{route.task_id}' cannot be downgraded"
                        )
                    existing_upstream = (
                        str(existing[2]) if existing[2] is not None else None
                    )
                    if (
                        existing_upstream is not None
                        and route.upstream_task_id is not None
                        and existing_upstream != route.upstream_task_id
                        and not (
                            str(existing[3]) != "active"
                            and route.route_kind == "remote_ref"
                            and existing_upstream == route.task_id
                        )
                    ):
                        raise ValueError(
                            f"Gateway route binding conflict for task '{route.task_id}'"
                        )
                    self._conn.execute(
                        """
                        UPDATE gateway_task_routes
                        SET metadata_json = ?, webhook_json = ?,
                            upstream_task_id = COALESCE(?, upstream_task_id),
                            route_state = ?, route_error = ?, updated_at = ?
                        WHERE task_id = ?
                        """,
                        (
                            metadata_json,
                            webhook_json,
                            route.upstream_task_id,
                            route.route_state,
                            route.route_error,
                            route.updated_at.isoformat(),
                            route.task_id,
                        ),
                    )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    async def asave_route(self, route: TaskRouteRecord) -> None:
        await to_thread(self.save_route, route)

    def reserve_route(self, route: TaskRouteRecord) -> TaskRouteRecord:
        """Persist a stable Gateway identity before starting its effect."""

        reservation = TaskRouteRecord(
            task_id=route.task_id,
            agent_name=route.agent_name,
            metadata=dict(route.metadata),
            route_kind=route.route_kind,
            upstream_task_id=route.task_id,
            webhook=dict(route.webhook) if route.webhook is not None else None,
            route_state="pending",
        )
        with self._lock:
            existing = self.get_route(route.task_id)
            if existing is not None:
                if (
                    existing.agent_name != reservation.agent_name
                    or existing.route_kind != reservation.route_kind
                ):
                    raise ValueError(
                        f"Gateway route binding conflict for task '{route.task_id}'"
                    )
                return existing
            self.save_route(reservation)
        return reservation

    async def areserve_route(self, route: TaskRouteRecord) -> TaskRouteRecord:
        return await to_thread(self.reserve_route, route)

    def transition_route(
        self,
        task_id: str,
        *,
        route_state: TaskRouteState,
        upstream_task_id: str | None = None,
        route_error: str | None = None,
    ) -> TaskRouteRecord:
        """Transition a reservation without allowing identity rebinding."""

        self._validate_route_state(route_state)
        with self._lock:
            route = self.get_route(task_id)
            if route is None:
                raise KeyError(task_id)
            if (
                route.upstream_task_id is not None
                and upstream_task_id is not None
                and route.upstream_task_id != upstream_task_id
                and not (
                    route.route_kind == "remote_ref"
                    and route.route_state != "active"
                    and route.upstream_task_id == route.task_id
                )
            ):
                raise ValueError(
                    f"Gateway route binding conflict for task '{task_id}'"
                )
            route.upstream_task_id = upstream_task_id or route.upstream_task_id
            route.route_state = route_state
            route.route_error = route_error
            self.save_route(route)
            return route

    async def atransition_route(
        self,
        task_id: str,
        *,
        route_state: TaskRouteState,
        upstream_task_id: str | None = None,
        route_error: str | None = None,
    ) -> TaskRouteRecord:
        return await to_thread(
            self.transition_route,
            task_id,
            route_state=route_state,
            upstream_task_id=upstream_task_id,
            route_error=route_error,
        )

    def get_route(self, task_id: str) -> TaskRouteRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT task_id, agent_name, metadata_json, route_kind,
                    upstream_task_id, webhook_json, route_state, route_error,
                    created_at, updated_at
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
                    upstream_task_id, webhook_json, route_state, route_error,
                    created_at, updated_at
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
                    upstream_task_id, webhook_json, route_state, route_error,
                    created_at, updated_at
                FROM gateway_task_routes
                WHERE upstream_task_id = ? AND route_state = 'active'
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
                    webhook_json TEXT,
                    route_state TEXT NOT NULL DEFAULT 'active'
                        CHECK (route_state IN ('pending', 'active', 'failed', 'uncertain')),
                    route_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
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
            if "route_state" not in columns:
                self._conn.execute(
                    "ALTER TABLE gateway_task_routes "
                    "ADD COLUMN route_state TEXT NOT NULL DEFAULT 'active'"
                )
            if "route_error" not in columns:
                self._conn.execute(
                    "ALTER TABLE gateway_task_routes ADD COLUMN route_error TEXT"
                )
            now = datetime.now(UTC).isoformat()
            if "created_at" not in columns:
                self._conn.execute(
                    "ALTER TABLE gateway_task_routes ADD COLUMN created_at TEXT"
                )
                self._conn.execute(
                    "UPDATE gateway_task_routes SET created_at = ?",
                    (now,),
                )
            if "updated_at" not in columns:
                self._conn.execute(
                    "ALTER TABLE gateway_task_routes ADD COLUMN updated_at TEXT"
                )
                self._conn.execute(
                    "UPDATE gateway_task_routes SET updated_at = ?",
                    (now,),
                )
            self._conn.commit()

    def _row_to_route(
        self,
        row: tuple[
            str,
            str,
            str,
            str,
            str | None,
            str | None,
            str,
            str | None,
            str,
            str,
        ],
    ) -> TaskRouteRecord:
        metadata_json = row[2]
        metadata = json.loads(metadata_json)
        webhook_json = row[5]
        webhook = json.loads(webhook_json) if webhook_json else None
        if not isinstance(webhook, dict):
            webhook = None
        route_state = str(row[6])
        self._validate_route_state(route_state)
        return TaskRouteRecord(
            task_id=row[0],
            agent_name=row[1],
            metadata=metadata,
            route_kind=row[3],
            upstream_task_id=row[4],
            webhook=webhook,
            route_state=route_state,
            route_error=row[7],
            created_at=datetime.fromisoformat(row[8]),
            updated_at=datetime.fromisoformat(row[9]),
        )

    def _validate_route_state(self, route_state: object) -> None:
        if route_state not in TASK_ROUTE_STATES:
            raise ValueError(f"Invalid Gateway route state: {route_state!r}")

    def close(self) -> None:
        with self._lock:
            self._conn.close()
