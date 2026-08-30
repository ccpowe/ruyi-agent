from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import TaskRouteRecord
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.contracts import UnknownWorkerTaskError
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import TaskRecord


def _record(
    task_id: str,
    *,
    agent_name: str = "main",
    route_kind: str = "local",
    upstream_task_id: str | None = None,
    parent_task_id: str | None = None,
    root_task_id: str | None = None,
    depth: int = 1,
    state: str = "completed",
    run_count: int = 1,
) -> TaskRecord:
    now = datetime.now(UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name=agent_name,
        state=state,  # type: ignore[arg-type]
        thread_id=task_id,
        parent_task_id=parent_task_id,
        root_task_id=root_task_id or task_id,
        depth=depth,
        created_at=now,
        updated_at=now,
        run_count=run_count,
        route_kind=route_kind,
        upstream_task_id=upstream_task_id,
    )


class RecordingControl:
    def __init__(self) -> None:
        self.records: dict[str, TaskRecord] = {}
        self.ensure_calls: list[dict[str, Any]] = []
        self.refresh_calls: list[str] = []

    async def spawn_task(self, agent_name: str, task: str, **kwargs: Any) -> TaskRecord:
        del task
        is_remote = "metadata" in kwargs
        task_id = str(
            kwargs.get("task_id") or ("remote-local-id" if is_remote else "local-id")
        )
        record = _record(
            task_id,
            agent_name=agent_name,
            route_kind="remote_ref" if is_remote else "local",
            upstream_task_id="upstream-id" if is_remote else None,
        )
        self.records[task_id] = record
        return record

    def get_task_record(self, task_id: str) -> TaskRecord:
        try:
            return self.records[task_id]
        except KeyError as exc:
            raise UnknownWorkerTaskError(task_id) from exc

    def list_persisted_task_records(self) -> list[TaskRecord]:
        return list(self.records.values())

    def ensure_remote_task_record(self, **kwargs: Any) -> TaskRecord:
        self.ensure_calls.append(kwargs)
        record = _record(
            kwargs["task_id"],
            agent_name=kwargs["agent_name"],
            route_kind="remote_ref",
            upstream_task_id=kwargs["upstream_task_id"],
        )
        self.records[record.task_id] = record
        return record

    async def refresh_task(self, task_id: str) -> TaskRecord:
        self.refresh_calls.append(task_id)
        return self.records[task_id]


class FailingSpawnControl(RecordingControl):
    def __init__(self, error: Exception) -> None:
        super().__init__()
        self.error = error
        self.spawn_calls = 0

    async def spawn_task(self, agent_name: str, task: str, **kwargs: Any) -> TaskRecord:
        del agent_name, task, kwargs
        self.spawn_calls += 1
        raise self.error

    def get_task_record(self, task_id: str) -> TaskRecord:
        raise UnknownWorkerTaskError(task_id)


class IdempotentRecordingControl(RecordingControl):
    def __init__(self) -> None:
        super().__init__()
        self.spawn_calls = 0
        self.effect_calls = 0

    async def spawn_task(self, agent_name: str, task: str, **kwargs: Any) -> TaskRecord:
        del task
        self.spawn_calls += 1
        task_id = str(kwargs["task_id"])
        existing = self.records.get(task_id)
        if existing is not None:
            return existing
        self.effect_calls += 1
        route_kind = "remote_ref" if "metadata" in kwargs else "local"
        record = _record(
            task_id,
            agent_name=agent_name,
            route_kind=route_kind,
            upstream_task_id="upstream-id" if route_kind == "remote_ref" else None,
        )
        self.records[task_id] = record
        return record


class RecordThenFailControl(RecordingControl):
    async def spawn_task(self, agent_name: str, task: str, **kwargs: Any) -> TaskRecord:
        del task
        task_id = str(kwargs["task_id"])
        now = datetime.now(UTC)
        record = TaskRecord(
            task_id=task_id,
            agent_name=agent_name,
            state="pending",
            thread_id=task_id,
            parent_task_id=None,
            root_task_id=task_id,
            depth=1,
            created_at=now,
            updated_at=now,
            run_count=0,
            route_kind="local",
        )
        self.records[task_id] = record
        raise RuntimeError("crashed before durable initial run")


class TransientReadControl(RecordingControl):
    def __init__(self) -> None:
        super().__init__()
        self.fail_reads = True
        self.spawn_calls = 0
        self.send_calls = 0
        self.cancel_calls = 0

    def get_task_record(self, task_id: str) -> TaskRecord:
        if self.fail_reads:
            raise RuntimeError("task database temporarily unavailable")
        return super().get_task_record(task_id)

    async def spawn_task(self, *args: Any, **kwargs: Any) -> TaskRecord:
        self.spawn_calls += 1
        return await super().spawn_task(*args, **kwargs)

    async def send_task_input(
        self,
        task_id: str,
        input_content: str,
        **kwargs: Any,
    ) -> TaskRecord:
        del input_content, kwargs
        self.send_calls += 1
        return self.get_task_record(task_id)

    async def cancel_task(self, task_id: str) -> TaskRecord:
        self.cancel_calls += 1
        return self.get_task_record(task_id)


class FailingReservationStore:
    def __init__(self) -> None:
        self.reserve_calls = 0

    async def areserve_route(
        self, route: TaskRouteRecord, **kwargs: object
    ) -> TaskRouteRecord:
        del route, kwargs
        self.reserve_calls += 1
        raise sqlite3.OperationalError("route database unavailable")


class FlakyActivationStore(GatewayRouteStore):
    def __init__(self) -> None:
        super().__init__(":memory:")
        self.active_failures = 1

    async def atransition_route(
        self, task_id: str, **kwargs: object
    ) -> TaskRouteRecord:
        if kwargs.get("route_state") == "active" and self.active_failures:
            self.active_failures -= 1
            raise sqlite3.OperationalError("activation commit failed")
        return await super().atransition_route(task_id, **kwargs)


class UnavailableFallbackStore(GatewayRouteStore):
    async def atransition_route(
        self, task_id: str, **kwargs: object
    ) -> TaskRouteRecord:
        del task_id, kwargs
        raise sqlite3.OperationalError("transition unavailable")

    async def aget_route(self, task_id: str) -> TaskRouteRecord | None:
        del task_id
        raise sqlite3.OperationalError("fallback read unavailable")


def test_router_creates_and_persists_local_route() -> None:
    asyncio.run(_create_and_persist_local_route())


async def _create_and_persist_local_route() -> None:
    control = RecordingControl()
    store = GatewayRouteStore(":memory:")
    try:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        routed = await router.create_task(
            agent_name="main",
            route_kind="local",
            input_content="hello",
            metadata={"source": "test"},
            webhook=None,
            delegation_context=None,
            task_id="local-id",
        )

        assert routed.route == await router.get_route("local-id")
        assert routed.route.route_kind == "local"
        assert routed.route.upstream_task_id == "local-id"
        assert routed.route.metadata == {"source": "test"}
    finally:
        store.close()


def test_route_store_rejects_identity_rebinding_without_overwrite() -> None:
    store = GatewayRouteStore(":memory:")
    original = TaskRouteRecord(
        task_id="task-1",
        agent_name="main",
        metadata={"version": "original"},
        route_kind="local",
        upstream_task_id="task-1",
    )
    try:
        store.save_route(original)
        with pytest.raises(ValueError, match="binding conflict"):
            store.save_route(
                TaskRouteRecord(
                    task_id="task-1",
                    agent_name="remote",
                    metadata={"version": "replacement"},
                    route_kind="remote_ref",
                    upstream_task_id="upstream-2",
                )
            )
        degraded = store.transition_route("task-1", route_state="uncertain")
        assert degraded.route_state == "uncertain"
        with pytest.raises(ValueError, match="cannot transition"):
            store.transition_route("task-1", route_state="active")

        assert store.get_route("task-1") == degraded
    finally:
        store.close()


def test_route_store_updates_metadata_for_same_identity_binding() -> None:
    store = GatewayRouteStore(":memory:")
    original = TaskRouteRecord(
        task_id="task-1",
        agent_name="main",
        metadata={"version": "original"},
        route_kind="local",
        upstream_task_id="task-1",
    )
    updated = TaskRouteRecord(
        task_id="task-1",
        agent_name="main",
        metadata={"version": "updated"},
        route_kind="local",
        upstream_task_id="task-1",
        webhook={"url": "https://example.test/hook"},
    )
    try:
        store.save_route(original)
        store.save_route(updated)
        assert store.get_route("task-1") == updated
    finally:
        store.close()


@pytest.mark.parametrize("route_kind", ["local", "remote_ref"])
def test_route_reservation_failure_prevents_spawn_effect(route_kind: str) -> None:
    async def scenario() -> None:
        control = RecordingControl()
        spawn_calls = 0

        async def spawn_task(*args: Any, **kwargs: Any) -> TaskRecord:
            nonlocal spawn_calls
            del args, kwargs
            spawn_calls += 1
            return _record("should-not-exist")

        control.spawn_task = spawn_task  # type: ignore[method-assign]
        store = FailingReservationStore()
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        with pytest.raises(GatewayTaskError) as caught:
            await router.create_task(
                agent_name="main",
                route_kind=route_kind,  # type: ignore[arg-type]
                input_content="hello",
                metadata={},
                webhook=None,
                delegation_context=None,
                task_id="gateway-id",
            )

        assert spawn_calls == 0
        assert caught.value.code == "route_persistence_failed"
        assert caught.value.details == {
            "task_id": "gateway-id",
            "route_state": "unpersisted",
            "task_queryable": False,
            "create_retryable": True,
            "effect_outcome": "not_started",
        }

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status_code", "expected_state"),
    [(400, "failed"), (502, "uncertain")],
)
def test_remote_effect_failure_retains_queryable_route_identity(
    status_code: int,
    expected_state: str,
) -> None:
    async def scenario() -> None:
        control = FailingSpawnControl(
            A2AClientError(
                status_code=status_code,
                code="upstream_gateway_error",
                message="remote create failed",
            )
        )
        store = GatewayRouteStore(":memory:")
        try:
            router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
            with pytest.raises(GatewayTaskError) as caught:
                await router.create_task(
                    agent_name="remote",
                    route_kind="remote_ref",
                    input_content="hello",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id="gateway-id",
                    idempotency_key="same-command",
                )

            route = await router.get_route("gateway-id")
            record = await router.get_record(route)
            assert control.spawn_calls == 1
            assert route.route_state == expected_state
            assert record.task_id == "gateway-id"
            assert record.state == (
                "failed" if expected_state == "failed" else "interrupted"
            )
            assert caught.value.details == {
                "task_id": "gateway-id",
                "task_url": "/tasks/gateway-id",
                "route_state": expected_state,
                "task_queryable": True,
                "create_retryable": False,
                "effect_outcome": (
                    "uncertain" if expected_state == "uncertain" else "not_started"
                ),
            }
            with pytest.raises(GatewayTaskError) as unavailable:
                await router.cancel(route)
            assert unavailable.value.code == "task_route_unavailable"
        finally:
            store.close()

    asyncio.run(scenario())


def test_create_error_reports_actual_unknown_fallback_persistence_state() -> None:
    async def scenario() -> None:
        control = FailingSpawnControl(
            A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message="remote response lost",
            )
        )
        store = UnavailableFallbackStore(":memory:")
        try:
            router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
            with pytest.raises(GatewayTaskError) as caught:
                await router.create_task(
                    agent_name="remote",
                    route_kind="remote_ref",
                    input_content="hello",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id="gateway-id",
                )

            assert caught.value.details == {
                "task_id": "gateway-id",
                "route_state": "pending",
                "task_queryable": False,
                "create_retryable": False,
                "effect_outcome": "uncertain",
            }
            assert store.get_route("gateway-id") is not None
        finally:
            store.close()

    asyncio.run(scenario())


def test_effectless_local_reservation_remains_queryable_but_not_routable() -> None:
    async def scenario() -> None:
        control = FailingSpawnControl(RuntimeError("not used"))
        store = GatewayRouteStore(":memory:")
        try:
            reservation = store.reserve_route(
                TaskRouteRecord(
                    task_id="reserved-only",
                    agent_name="main",
                    metadata={"request": "visible"},
                    route_kind="local",
                    upstream_task_id="reserved-only",
                    route_state="pending",
                )
            )
            router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

            record = await router.get_record(reservation)
            assert record.task_id == "reserved-only"
            assert record.state == "interrupted"
            assert reservation.route_state == "pending"
            with pytest.raises(GatewayTaskError) as caught:
                await router.send_input(reservation, "unsafe retry")
            assert caught.value.code == "task_route_unavailable"
            assert control.spawn_calls == 0
        finally:
            store.close()

    asyncio.run(scenario())


def test_local_record_without_durable_run_never_promotes_uncertain_route() -> None:
    async def scenario() -> None:
        control = RecordThenFailControl()
        store = GatewayRouteStore(":memory:")
        try:
            router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
            with pytest.raises(GatewayTaskError):
                await router.create_task(
                    agent_name="main",
                    route_kind="local",
                    input_content="hello",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id="created-not-started",
                )

            route = await router.get_route("created-not-started")
            queried = await router.get_record(route)
            listed = await router.list_routes()
            assert route.route_state == "uncertain"
            assert queried.state == "interrupted"
            assert listed[0].route_state == "uncertain"
            assert store.get_route("created-not-started").route_state == "uncertain"  # type: ignore[union-attr]
        finally:
            store.close()

    asyncio.run(scenario())


def test_active_local_transient_record_read_failure_does_not_degrade_route() -> None:
    async def scenario() -> None:
        control = TransientReadControl()
        control.records["active-local"] = _record("active-local")
        store = GatewayRouteStore(":memory:")
        store.save_route(
            TaskRouteRecord(
                task_id="active-local",
                agent_name="main",
                metadata={},
                route_kind="local",
                upstream_task_id="active-local",
                route_state="active",
            )
        )
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
        before = store.get_route("active-local")
        try:
            with pytest.raises(RuntimeError, match="temporarily unavailable"):
                await router.get_route("active-local")
            assert store.get_route("active-local") == before

            control.fail_reads = False
            route = await router.get_route("active-local")
            assert route.route_state == "active"
            assert (await router.get_record(route)).run_count == 1
            assert (await router.send_input(route, "continue")).task_id == route.task_id
            assert (await router.cancel(route)).task_id == route.task_id
            assert store.get_route("active-local").route_state == "active"  # type: ignore[union-attr]
            assert control.spawn_calls == 0
            assert control.send_calls == control.cancel_calls == 1
        finally:
            store.close()

    asyncio.run(scenario())


def test_pending_local_transient_record_read_failure_does_not_terminalize() -> None:
    async def scenario() -> None:
        control = TransientReadControl()
        store = GatewayRouteStore(":memory:")
        store.reserve_route(
            TaskRouteRecord(
                task_id="pending-local",
                agent_name="main",
                metadata={},
                route_kind="local",
                upstream_task_id="pending-local",
                route_state="pending",
            ),
            create_key_scope="external",
            create_replay_policy="local_task_identity",
        )
        store.mark_create_effect_started("pending-local")
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
        before = store.get_route("pending-local")
        try:
            with pytest.raises(RuntimeError, match="temporarily unavailable"):
                await router.get_route("pending-local")
            assert store.get_route("pending-local") == before
            assert before is not None and before.route_state == "pending"

            with pytest.raises(RuntimeError, match="temporarily unavailable"):
                await router.create_task(
                    agent_name="main",
                    route_kind="local",
                    input_content="must not respawn",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id="pending-local",
                    idempotency_key="same-command",
                )
            assert store.get_route("pending-local") == before
            assert control.spawn_calls == 0

            control.records["pending-local"] = _record("pending-local")
            control.fail_reads = False
            recovered = await router.get_route("pending-local")
            assert recovered.route_state == "active"
            assert control.spawn_calls == 0
        finally:
            store.close()

    asyncio.run(scenario())


def test_effect_then_activation_failure_recovers_same_identity_without_respawn() -> (
    None
):
    async def scenario() -> None:
        control = RecordingControl()
        spawn_calls = 0
        original_spawn = control.spawn_task

        async def counted_spawn(*args: Any, **kwargs: Any) -> TaskRecord:
            nonlocal spawn_calls
            spawn_calls += 1
            return await original_spawn(*args, **kwargs)

        control.spawn_task = counted_spawn  # type: ignore[method-assign]
        store = FlakyActivationStore()
        try:
            router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
            with pytest.raises(GatewayTaskError) as caught:
                await router.create_task(
                    agent_name="main",
                    route_kind="local",
                    input_content="hello",
                    metadata={"source": "test"},
                    webhook=None,
                    delegation_context=None,
                    task_id="gateway-id",
                )

            uncertain = await router.get_route("gateway-id")
            assert uncertain.route_state == "uncertain"
            assert caught.value.code == "route_persistence_failed"
            assert caught.value.details == {
                "task_id": "gateway-id",
                "task_url": "/tasks/gateway-id",
                "route_state": "uncertain",
                "task_queryable": True,
                "create_retryable": False,
                "effect_outcome": "completed",
            }

            record = await router.get_record(uncertain)
            recovered = await router.get_route("gateway-id")
            assert record.task_id == "gateway-id"
            assert record.state == "interrupted"
            assert recovered.route_state == "uncertain"
            assert spawn_calls == 1
        finally:
            store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("route_kind", ["local", "remote_ref"])
def test_restart_after_effect_before_binding_activates_without_duplicate_effect(
    tmp_path: Path,
    route_kind: str,
) -> None:
    db_path = tmp_path / f"{route_kind}-routes.sqlite"
    first_store = GatewayRouteStore(str(db_path))
    first_store.reserve_route(
        TaskRouteRecord(
            task_id="gateway-id",
            agent_name="main",
            metadata={"request": "stable"},
            route_kind=route_kind,  # type: ignore[arg-type]
            upstream_task_id="gateway-id",
            route_state="pending",
        ),
        create_key_scope="external",
        create_replay_policy=(
            "ruyi_gateway_v1" if route_kind == "remote_ref" else "local_task_identity"
        ),
    )
    first_store.mark_create_effect_started("gateway-id")
    first_store.close()

    control = IdempotentRecordingControl()
    control.records["gateway-id"] = _record(
        "gateway-id",
        route_kind=route_kind,
        upstream_task_id="upstream-id" if route_kind == "remote_ref" else None,
    )
    second_store = GatewayRouteStore(str(db_path))

    async def scenario() -> None:
        router = TaskRouter(control=control, route_store=second_store)  # type: ignore[arg-type]
        routed = await router.create_task(
            agent_name="main",
            route_kind=route_kind,  # type: ignore[arg-type]
            input_content="same request",
            metadata={"request": "stable"},
            webhook=None,
            delegation_context=None,
            task_id="gateway-id",
            idempotency_key="same-command",
        )

        assert routed.record.task_id == "gateway-id"
        assert routed.route.route_state == "active"
        assert routed.route.upstream_task_id == (
            "upstream-id" if route_kind == "remote_ref" else "gateway-id"
        )
        assert control.spawn_calls == (1 if route_kind == "remote_ref" else 0)
        assert control.effect_calls == 0

    try:
        asyncio.run(scenario())
    finally:
        second_store.close()


def test_route_store_migrates_legacy_rows_as_active_idempotently(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "legacy-routes.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE gateway_task_routes (
            task_id TEXT PRIMARY KEY,
            agent_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            route_kind TEXT NOT NULL,
            upstream_task_id TEXT NOT NULL,
            webhook_json TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gateway_task_routes VALUES (
            'task-1', 'main', '{}', 'local', 'task-1', NULL
        )
        """
    )
    connection.commit()
    connection.close()

    for _ in range(2):
        store = GatewayRouteStore(str(db_path))
        try:
            route = store.get_route("task-1")
            assert route is not None
            assert route.route_state == "active"
            assert route.route_error is None
        finally:
            store.close()


def test_route_store_repairs_partial_migration_and_empty_timestamps_each_open(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "partial-routes.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE gateway_task_routes_migrating (
            task_id TEXT PRIMARY KEY,
            agent_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            route_kind TEXT NOT NULL,
            upstream_task_id TEXT NOT NULL,
            webhook_json TEXT,
            route_state TEXT NOT NULL DEFAULT 'active',
            route_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gateway_task_routes_migrating VALUES (
            'task-1', 'remote', '{}', 'remote_ref', 'task-1', NULL,
            'uncertain', 'lost response', '', NULL
        )
        """
    )
    connection.commit()
    connection.close()

    for _ in range(3):
        store = GatewayRouteStore(str(db_path))
        try:
            route = store.get_route("task-1")
            assert route is not None
            assert route.route_state == "uncertain"
            assert route.upstream_task_id is None
            with sqlite3.connect(db_path) as check:
                columns = check.execute(
                    "PRAGMA table_info(gateway_task_routes)"
                ).fetchall()
                assert (
                    next(row for row in columns if row[1] == "upstream_task_id")[3] == 0
                )
                assert check.execute(
                    "SELECT created_at, updated_at FROM gateway_task_routes"
                ).fetchone() == (
                    route.created_at.isoformat(),
                    route.updated_at.isoformat(),
                )
                assert (
                    check.execute(
                        "SELECT name FROM sqlite_master WHERE name = "
                        "'gateway_task_routes_migrating'"
                    ).fetchone()
                    is None
                )
        finally:
            store.close()


def test_route_migration_never_keeps_remote_without_binding_active(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "remote-null-routes.sqlite"
    connection = sqlite3.connect(db_path)
    connection.execute(
        """
        CREATE TABLE gateway_task_routes (
            task_id TEXT PRIMARY KEY,
            agent_name TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            route_kind TEXT NOT NULL,
            upstream_task_id TEXT,
            webhook_json TEXT,
            route_state TEXT NOT NULL,
            route_error TEXT,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    connection.execute(
        """
        INSERT INTO gateway_task_routes VALUES (
            'remote-child', 'remote', '{}', 'remote_ref', NULL, NULL,
            'active', NULL, NULL, ''
        )
        """
    )
    connection.commit()
    connection.close()

    for _ in range(2):
        store = GatewayRouteStore(str(db_path))
        try:
            route = store.get_route("remote-child")
            assert route is not None
            assert route.route_state == "uncertain"
            assert route.upstream_task_id is None
            assert route.route_error == "Remote route has no durable upstream binding"
        finally:
            store.close()


def test_router_rebuilds_and_refreshes_remote_record() -> None:
    asyncio.run(_rebuild_and_refresh_remote_record())


async def _rebuild_and_refresh_remote_record() -> None:
    control = RecordingControl()
    store = GatewayRouteStore(":memory:")
    route = TaskRouteRecord(
        task_id="local-remote-id",
        agent_name="research",
        metadata={},
        route_kind="remote_ref",
        upstream_task_id="upstream-id",
    )
    store.save_route(route)
    try:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        record = await router.get_record(await router.get_route(route.task_id))

        assert record.task_id == route.task_id
        assert control.ensure_calls[0]["upstream_task_id"] == "upstream-id"
        assert control.refresh_calls == [route.task_id]
    finally:
        store.close()


def test_router_discovers_only_descendants_of_gateway_roots() -> None:
    asyncio.run(_discover_only_gateway_descendants())


async def _discover_only_gateway_descendants() -> None:
    control = RecordingControl()
    root = _record("root-id")
    child = _record(
        "child-id",
        agent_name="research",
        parent_task_id=root.task_id,
        root_task_id=root.task_id,
        depth=2,
    )
    orphan = _record("orphan-id")
    control.records = {record.task_id: record for record in (root, child, orphan)}
    store = GatewayRouteStore(":memory:")
    store.save_route(
        TaskRouteRecord(
            task_id=root.task_id,
            agent_name=root.agent_name,
            metadata={"channel_user": "alice"},
            route_kind="local",
            upstream_task_id=root.task_id,
        )
    )
    try:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        routes = await router.list_routes()

        assert {route.task_id for route in routes} == {"root-id", "child-id"}
        child_route = await router.get_route("child-id")
        assert child_route.agent_name == "research"
        assert child_route.metadata == {"channel_user": "alice"}
    finally:
        store.close()


@pytest.mark.parametrize(
    ("child_kind", "child_upstream", "child_state", "run_count", "root_state"),
    [
        ("remote_ref", None, "completed", 1, "active"),
        ("local", None, "pending", 0, "active"),
        ("remote_ref", "upstream-child", "completed", 1, "uncertain"),
    ],
)
def test_descendant_recovery_requires_active_ancestor_and_durable_binding(
    child_kind: str,
    child_upstream: str | None,
    child_state: str,
    run_count: int,
    root_state: str,
) -> None:
    async def scenario() -> None:
        control = RecordingControl()
        root = _record("root-id")
        child = _record(
            "child-id",
            agent_name="remote" if child_kind == "remote_ref" else "worker",
            route_kind=child_kind,
            upstream_task_id=child_upstream,
            parent_task_id=root.task_id,
            root_task_id=root.task_id,
            depth=2,
            state=child_state,
            run_count=run_count,
        )
        control.records = {root.task_id: root, child.task_id: child}
        store = GatewayRouteStore(":memory:")
        store.save_route(
            TaskRouteRecord(
                task_id=root.task_id,
                agent_name=root.agent_name,
                metadata={},
                route_kind="local",
                upstream_task_id=root.task_id,
                route_state=root_state,  # type: ignore[arg-type]
                route_error="ancestor unavailable" if root_state != "active" else None,
            )
        )
        try:
            router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
            recovered = await router.get_route(child.task_id)

            assert recovered.route_state == "uncertain"
            assert recovered.upstream_task_id == (
                child_upstream if child_kind == "remote_ref" else child.task_id
            )
            record = await router.get_record(recovered)
            assert record.state == "interrupted"
            assert control.ensure_calls == []
            assert control.refresh_calls == []
            with pytest.raises(GatewayTaskError, match="cannot be routed safely"):
                await router.cancel(recovered)
            with pytest.raises(GatewayTaskError, match="cannot be routed safely"):
                await router.send_input(recovered, "never dispatch")
        finally:
            store.close()

    asyncio.run(scenario())
