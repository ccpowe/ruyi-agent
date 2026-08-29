from __future__ import annotations

import json
import sqlite3
import threading
from asyncio import to_thread
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from ruyi_agent.task_models import (
    TASK_ROUTE_STATES,
    TaskRouteRecord,
    TaskRouteState,
)

_ROUTE_COLUMNS = (
    "task_id, agent_name, metadata_json, route_kind, upstream_task_id, "
    "webhook_json, route_state, route_error, created_at, updated_at, "
    "create_key_scope, create_replay_policy, create_effect_boundary"
)
_ALLOWED_ROUTE_TRANSITIONS: dict[TaskRouteState, frozenset[TaskRouteState]] = {
    "pending": frozenset({"pending", "active", "failed", "uncertain"}),
    "active": frozenset({"active", "uncertain"}),
    "failed": frozenset({"failed"}),
    "uncertain": frozenset({"uncertain"}),
}

_CREATE_KEY_SCOPES = frozenset({"none", "external", "generated", "legacy_unknown"})
_CREATE_REPLAY_POLICIES = frozenset(
    {"never", "local_task_identity", "ruyi_gateway_v1", "legacy_unknown"}
)
_CREATE_EFFECT_BOUNDARIES = frozenset({"reserved", "started", "legacy_unknown"})


@dataclass(frozen=True, slots=True)
class GatewayCreateEvidence:
    """Non-secret facts needed to classify an interrupted create safely."""

    task_id: str
    key_scope: str
    replay_policy: str
    effect_boundary: str

    @property
    def permits_remote_replay(self) -> bool:
        return (
            self.key_scope == "external"
            and self.replay_policy == "ruyi_gateway_v1"
            and self.effect_boundary == "started"
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
        self._validate_active_binding(route)
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
                    existing_state = str(existing[3])
                    if (
                        route.route_state
                        not in _ALLOWED_ROUTE_TRANSITIONS[existing_state]
                    ):
                        raise ValueError(
                            f"Gateway route '{route.task_id}' cannot transition "
                            f"from {existing_state} to {route.route_state}"
                        )
                    existing_upstream = (
                        str(existing[2]) if existing[2] is not None else None
                    )
                    if (
                        existing_upstream is not None
                        and route.upstream_task_id is not None
                        and existing_upstream != route.upstream_task_id
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

    def reserve_route(
        self,
        route: TaskRouteRecord,
        *,
        create_key_scope: str = "legacy_unknown",
        create_replay_policy: str = "legacy_unknown",
    ) -> TaskRouteRecord:
        """Persist a stable Gateway identity before starting its effect."""

        self._validate_create_evidence(
            key_scope=create_key_scope,
            replay_policy=create_replay_policy,
            effect_boundary="reserved",
        )
        reservation = TaskRouteRecord(
            task_id=route.task_id,
            agent_name=route.agent_name,
            metadata=dict(route.metadata),
            route_kind=route.route_kind,
            upstream_task_id=(route.task_id if route.route_kind == "local" else None),
            webhook=dict(route.webhook) if route.webhook is not None else None,
            route_state="pending",
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    f"SELECT {_ROUTE_COLUMNS} FROM gateway_task_routes "
                    "WHERE task_id = ?",
                    (route.task_id,),
                ).fetchone()
                if row is not None:
                    existing = self._row_to_route(row)
                    if (
                        existing.agent_name != reservation.agent_name
                        or existing.route_kind != reservation.route_kind
                    ):
                        raise ValueError(
                            f"Gateway route binding conflict for task '{route.task_id}'"
                        )
                    self._conn.commit()
                    return existing
                now = datetime.now(UTC)
                reservation.created_at = now
                reservation.updated_at = now
                self._conn.execute(
                    """
                    INSERT INTO gateway_task_routes (
                        task_id, agent_name, metadata_json, route_kind,
                        upstream_task_id, webhook_json, route_state, route_error,
                        created_at, updated_at, create_key_scope,
                        create_replay_policy, create_effect_boundary
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'reserved')
                    """,
                    (
                        reservation.task_id,
                        reservation.agent_name,
                        json.dumps(
                            reservation.metadata, ensure_ascii=True, sort_keys=True
                        ),
                        reservation.route_kind,
                        reservation.upstream_task_id,
                        (
                            json.dumps(
                                reservation.webhook,
                                ensure_ascii=True,
                                sort_keys=True,
                            )
                            if reservation.webhook is not None
                            else None
                        ),
                        reservation.route_state,
                        reservation.route_error,
                        reservation.created_at.isoformat(),
                        reservation.updated_at.isoformat(),
                        create_key_scope,
                        create_replay_policy,
                    ),
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        return reservation

    async def areserve_route(
        self,
        route: TaskRouteRecord,
        *,
        create_key_scope: str = "legacy_unknown",
        create_replay_policy: str = "legacy_unknown",
    ) -> TaskRouteRecord:
        return await to_thread(
            self.reserve_route,
            route,
            create_key_scope=create_key_scope,
            create_replay_policy=create_replay_policy,
        )

    def mark_create_effect_started(self, task_id: str) -> GatewayCreateEvidence:
        """Commit the last local boundary before invoking the create effect."""

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cursor = self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET create_effect_boundary = 'started', updated_at = ?
                    WHERE task_id = ? AND route_state = 'pending'
                        AND create_effect_boundary IN ('reserved', 'started')
                    """,
                    (datetime.now(UTC).isoformat(), task_id),
                )
                if cursor.rowcount != 1:
                    raise ValueError(
                        f"Gateway route '{task_id}' cannot start its create effect"
                    )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise
        evidence = self.get_create_evidence(task_id)
        if evidence is None:  # pragma: no cover - protected by the transaction
            raise KeyError(task_id)
        return evidence

    async def amark_create_effect_started(
        self,
        task_id: str,
    ) -> GatewayCreateEvidence:
        return await to_thread(self.mark_create_effect_started, task_id)

    def get_create_evidence(self, task_id: str) -> GatewayCreateEvidence | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT create_key_scope, create_replay_policy,
                    create_effect_boundary
                FROM gateway_task_routes
                WHERE task_id = ?
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            return None
        self._validate_create_evidence(
            key_scope=str(row[0]),
            replay_policy=str(row[1]),
            effect_boundary=str(row[2]),
        )
        return GatewayCreateEvidence(
            task_id=task_id,
            key_scope=str(row[0]),
            replay_policy=str(row[1]),
            effect_boundary=str(row[2]),
        )

    async def aget_create_evidence(
        self,
        task_id: str,
    ) -> GatewayCreateEvidence | None:
        return await to_thread(self.get_create_evidence, task_id)

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
                    and route.route_state == "pending"
                    and route.upstream_task_id is None
                )
            ):
                raise ValueError(f"Gateway route binding conflict for task '{task_id}'")
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
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                table_names = {
                    str(row[0])
                    for row in self._conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    ).fetchall()
                }
                if (
                    "gateway_task_routes" not in table_names
                    and "gateway_task_routes_migrating" in table_names
                ):
                    self._conn.execute(
                        "ALTER TABLE gateway_task_routes_migrating "
                        "RENAME TO gateway_task_routes"
                    )
                self._create_route_table("gateway_task_routes")
                self._conn.execute("DROP TABLE IF EXISTS gateway_task_routes_migrating")
                columns = {
                    str(row[1]): row
                    for row in self._conn.execute(
                        "PRAGMA table_info(gateway_task_routes)"
                    ).fetchall()
                }
                additions = {
                    "webhook_json": "TEXT",
                    "route_state": "TEXT NOT NULL DEFAULT 'active'",
                    "route_error": "TEXT",
                    "created_at": "TEXT",
                    "updated_at": "TEXT",
                    "create_key_scope": "TEXT NOT NULL DEFAULT 'legacy_unknown'",
                    "create_replay_policy": ("TEXT NOT NULL DEFAULT 'legacy_unknown'"),
                    "create_effect_boundary": (
                        "TEXT NOT NULL DEFAULT 'legacy_unknown'"
                    ),
                }
                for column, declaration in additions.items():
                    if column not in columns:
                        self._conn.execute(
                            f"ALTER TABLE gateway_task_routes "
                            f"ADD COLUMN {column} {declaration}"
                        )
                now = datetime.now(UTC).isoformat()
                self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET created_at = ?
                    WHERE created_at IS NULL OR trim(created_at) = ''
                    """,
                    (now,),
                )
                self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET updated_at = COALESCE(NULLIF(trim(updated_at), ''), created_at)
                    WHERE updated_at IS NULL OR trim(updated_at) = ''
                    """
                )
                columns = {
                    str(row[1]): row
                    for row in self._conn.execute(
                        "PRAGMA table_info(gateway_task_routes)"
                    ).fetchall()
                }
                if int(columns["upstream_task_id"][3]) != 0:
                    self._rebuild_route_table()
                self._normalize_create_evidence()
                self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET route_state = 'uncertain',
                        route_error = COALESCE(
                            route_error,
                            'Remote route has no durable upstream binding'
                        )
                    WHERE route_kind = 'remote_ref'
                        AND route_state = 'active'
                        AND (
                            upstream_task_id IS NULL
                            OR trim(upstream_task_id) = ''
                        )
                    """
                )
                self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET upstream_task_id = NULL
                    WHERE route_kind = 'remote_ref'
                        AND route_state != 'active'
                        AND upstream_task_id = task_id
                    """
                )
                self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET route_state = 'failed',
                        route_error = 'Gateway Task creation did not start'
                    WHERE route_state = 'pending'
                        AND create_effect_boundary = 'reserved'
                    """
                )
                self._conn.execute(
                    """
                    UPDATE gateway_task_routes
                    SET route_state = 'uncertain',
                        route_error = 'Remote Task creation was interrupted'
                    WHERE route_kind = 'remote_ref'
                        AND route_state = 'pending'
                        AND NOT (
                            create_key_scope = 'external'
                            AND create_replay_policy = 'ruyi_gateway_v1'
                            AND create_effect_boundary = 'started'
                        )
                    """
                )
                self._conn.commit()
            except BaseException:
                self._conn.rollback()
                raise

    def _create_route_table(self, table_name: str) -> None:
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                task_id TEXT PRIMARY KEY,
                agent_name TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                route_kind TEXT NOT NULL,
                upstream_task_id TEXT,
                webhook_json TEXT,
                route_state TEXT NOT NULL DEFAULT 'active'
                    CHECK (route_state IN ('pending', 'active', 'failed', 'uncertain')),
                route_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                create_key_scope TEXT NOT NULL DEFAULT 'legacy_unknown'
                    CHECK (create_key_scope IN (
                        'none', 'external', 'generated', 'legacy_unknown'
                    )),
                create_replay_policy TEXT NOT NULL DEFAULT 'legacy_unknown'
                    CHECK (create_replay_policy IN (
                        'never', 'local_task_identity', 'ruyi_gateway_v1',
                        'legacy_unknown'
                    )),
                create_effect_boundary TEXT NOT NULL DEFAULT 'legacy_unknown'
                    CHECK (create_effect_boundary IN (
                        'reserved', 'started', 'legacy_unknown'
                    ))
            )
            """
        )

    def _rebuild_route_table(self) -> None:
        temporary = "gateway_task_routes_migrating"
        self._conn.execute(f"DROP TABLE IF EXISTS {temporary}")
        self._create_route_table(temporary)
        self._conn.execute(
            f"""
            INSERT INTO {temporary} ({_ROUTE_COLUMNS})
            SELECT {_ROUTE_COLUMNS} FROM gateway_task_routes
            """
        )
        self._conn.execute("DROP TABLE gateway_task_routes")
        self._conn.execute(f"ALTER TABLE {temporary} RENAME TO gateway_task_routes")

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
        route = TaskRouteRecord(
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
        self._validate_active_binding(route)
        return route

    def _validate_route_state(self, route_state: object) -> None:
        if route_state not in TASK_ROUTE_STATES:
            raise ValueError(f"Invalid Gateway route state: {route_state!r}")

    def _normalize_create_evidence(self) -> None:
        self._conn.execute(
            """
            UPDATE gateway_task_routes
            SET create_key_scope = 'legacy_unknown'
            WHERE create_key_scope IS NULL OR create_key_scope NOT IN (
                'none', 'external', 'generated', 'legacy_unknown'
            )
            """
        )
        self._conn.execute(
            """
            UPDATE gateway_task_routes
            SET create_replay_policy = 'legacy_unknown'
            WHERE create_replay_policy IS NULL OR create_replay_policy NOT IN (
                'never', 'local_task_identity', 'ruyi_gateway_v1',
                'legacy_unknown'
            )
            """
        )
        self._conn.execute(
            """
            UPDATE gateway_task_routes
            SET create_effect_boundary = 'legacy_unknown'
            WHERE create_effect_boundary IS NULL OR create_effect_boundary NOT IN (
                'reserved', 'started', 'legacy_unknown'
            )
            """
        )

    def _validate_create_evidence(
        self,
        *,
        key_scope: str,
        replay_policy: str,
        effect_boundary: str,
    ) -> None:
        if key_scope not in _CREATE_KEY_SCOPES:
            raise ValueError(f"Invalid create key scope: {key_scope!r}")
        if replay_policy not in _CREATE_REPLAY_POLICIES:
            raise ValueError(f"Invalid create replay policy: {replay_policy!r}")
        if effect_boundary not in _CREATE_EFFECT_BOUNDARIES:
            raise ValueError(f"Invalid create effect boundary: {effect_boundary!r}")

    def _validate_active_binding(self, route: TaskRouteRecord) -> None:
        if (
            route.route_state == "active"
            and route.route_kind == "remote_ref"
            and not route.upstream_task_id
        ):
            raise ValueError(
                f"Active remote Gateway route '{route.task_id}' has no upstream binding"
            )

    def close(self) -> None:
        with self._lock:
            self._conn.close()
