from __future__ import annotations

import asyncio
import multiprocessing
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.runtime.delegation.async_runtime import UnknownWorkerTaskError
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import TaskRecord
from tests.unit.gateway_http_support import auth_headers, build_app


class _ForbiddenEffectRemote:
    def __init__(self) -> None:
        self.calls = 0

    async def create_task(self, *args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        self.calls += 1
        raise AssertionError("recovery must not invoke the remote create effect")

    async def send_input(self, *args: object, **kwargs: object) -> dict[str, object]:
        del args, kwargs
        self.calls += 1
        raise AssertionError("an unavailable route must not invoke remote input")


class _NoEffectControl:
    def __init__(self) -> None:
        self.spawn_calls = 0
        self.send_calls = 0

    def get_task_record(self, task_id: str) -> TaskRecord:
        raise UnknownWorkerTaskError(task_id)

    async def spawn_task(self, *args: object, **kwargs: object) -> TaskRecord:
        del args, kwargs
        self.spawn_calls += 1
        raise AssertionError("route recovery must not spawn")

    async def send_task_input(self, *args: object, **kwargs: object) -> TaskRecord:
        del args, kwargs
        self.send_calls += 1
        raise AssertionError("an unavailable route must reject input")


class _DurableThenBlockControl(_NoEffectControl):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.records: dict[str, TaskRecord] = {}

    def get_task_record(self, task_id: str) -> TaskRecord:
        try:
            return self.records[task_id]
        except KeyError as exc:
            raise UnknownWorkerTaskError(task_id) from exc

    async def spawn_task(
        self,
        agent_name: str,
        input_content: str,
        **kwargs: object,
    ) -> TaskRecord:
        del input_content
        self.spawn_calls += 1
        task_id = str(kwargs["task_id"])
        now = datetime.now(UTC)
        self.records[task_id] = TaskRecord(
            task_id=task_id,
            agent_name=agent_name,
            state="running",
            thread_id=task_id,
            parent_task_id=None,
            root_task_id=task_id,
            depth=1,
            created_at=now,
            updated_at=now,
            run_count=1,
            route_kind="local",
        )
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _BlockingEffectBoundaryStore(GatewayRouteStore):
    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.boundary_entered = asyncio.Event()

    async def amark_create_effect_started(self, task_id: str):
        del task_id
        self.boundary_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _crashing_remote_http_process(
    route_path: str,
    marker_path: str,
    ready: Any,
) -> None:
    class CrashAfterEffectBoundaryRemote:
        async def create_task(
            self,
            *args: object,
            **kwargs: object,
        ) -> dict[str, object]:
            del args, kwargs
            marker = Path(marker_path)
            calls = int(marker.read_text() or "0") if marker.exists() else 0
            marker.write_text(str(calls + 1))
            ready.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    patcher = pytest.MonkeyPatch()
    routes = GatewayRouteStore(route_path)
    app, _ = build_app(
        patcher,
        a2a_client=CrashAfterEffectBoundaryRemote(),  # type: ignore[arg-type]
        route_store=routes,
        remote_create_idempotency="ruyi_gateway_v1",
    )

    async def request() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            await client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "crash after effect"}, "metadata": {}},
            )

    asyncio.run(request())


def test_no_key_remote_process_kill_reopens_uncertain_without_replaying_effect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    route_path = str(tmp_path / "killed-remote-routes.sqlite")
    marker_path = str(tmp_path / "remote-effect-count")
    process = context.Process(
        target=_crashing_remote_http_process,
        args=(route_path, marker_path, ready),
    )
    process.start()
    try:
        assert ready.wait(timeout=15)
        process.kill()
        process.join(timeout=10)
        assert not process.is_alive()
        assert process.exitcode is not None and process.exitcode < 0
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=10)

    assert Path(marker_path).read_text() == "1"
    routes = GatewayRouteStore(route_path)
    [route] = routes.list_routes()
    evidence = routes.get_create_evidence(route.task_id)
    assert route.route_state == "uncertain"
    assert evidence is not None
    assert (evidence.key_scope, evidence.replay_policy, evidence.effect_boundary) == (
        "generated",
        "ruyi_gateway_v1",
        "started",
    )

    remote = _ForbiddenEffectRemote()
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=routes,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    try:
        with TestClient(app) as client:
            queried = client.get(f"/tasks/{route.task_id}", headers=auth_headers())
            sent = client.post(
                f"/tasks/{route.task_id}/input",
                headers=auth_headers(),
                json={"input": {"content": "must not replay"}},
            )
        assert queried.status_code == 200
        assert queried.json()["status"] == "interrupted"
        assert sent.status_code == 409
        assert remote.calls == 0
        assert Path(marker_path).read_text() == "1"
    finally:
        routes.close()


@pytest.mark.parametrize("schema_version", ["9ca29e3", "8bea2c6"])
def test_exact_pre_evidence_route_db_crash_fixture_never_dispatches_effect(
    tmp_path: Path,
    schema_version: str,
) -> None:
    db_path = tmp_path / f"routes-{schema_version}.sqlite"
    connection = sqlite3.connect(db_path)
    if schema_version == "9ca29e3":
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
            "INSERT INTO gateway_task_routes VALUES "
            "('legacy-local', 'main', '{}', 'local', 'legacy-local', NULL)"
        )
    else:
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
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            "INSERT INTO gateway_task_routes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "legacy-local",
                "main",
                "{}",
                "local",
                "legacy-local",
                None,
                "pending",
                None,
                "2026-08-30T00:00:00+00:00",
                "2026-08-30T00:00:01+00:00",
            ),
        )
    connection.commit()
    connection.close()

    store = GatewayRouteStore(str(db_path))
    control = _NoEffectControl()

    async def recover() -> None:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
        route = await router.get_route("legacy-local")
        assert route.route_state == "uncertain"
        record = await router.get_record(route)
        assert record.state == "interrupted"
        with pytest.raises(GatewayTaskError, match="cannot be routed safely"):
            await router.send_input(route, "must not dispatch")

    try:
        asyncio.run(recover())
        evidence = store.get_create_evidence("legacy-local")
        assert evidence is not None
        assert evidence.effect_boundary == "legacy_unknown"
        assert control.spawn_calls == control.send_calls == 0
    finally:
        store.close()


def test_local_http_cancel_before_durable_effect_is_failed_across_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    route_path = str(tmp_path / "cancelled-local-routes.sqlite")
    first_routes = _BlockingEffectBoundaryStore(route_path)
    first_app, factory = build_app(monkeypatch, route_store=first_routes)
    assert factory.control is not None
    spawn_calls = 0

    async def forbidden_spawn(*args: object, **kwargs: object) -> TaskRecord:
        nonlocal spawn_calls
        del args, kwargs
        spawn_calls += 1
        raise AssertionError("the local effect boundary was not crossed")

    factory.control.spawn_task = forbidden_spawn  # type: ignore[method-assign]

    async def cancel_request() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=first_app),
            base_url="http://gateway.test",
        ) as client:
            request = asyncio.create_task(
                client.post(
                    "/agents/main/tasks",
                    headers=auth_headers(),
                    json={"input": {"content": "cancel before run"}, "metadata": {}},
                )
            )
            await asyncio.wait_for(first_routes.boundary_entered.wait(), timeout=2)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request

    asyncio.run(cancel_request())
    [cancelled] = first_routes.list_routes()
    assert cancelled.route_state == "failed"
    assert cancelled.route_error == "Gateway Task creation did not start"
    evidence = first_routes.get_create_evidence(cancelled.task_id)
    assert evidence is not None and evidence.effect_boundary == "reserved"
    assert spawn_calls == 0
    first_routes.close()

    second_routes = GatewayRouteStore(route_path)
    second_app, second_factory = build_app(monkeypatch, route_store=second_routes)
    assert second_factory.control is not None
    recovery_effects = 0

    async def forbidden(*args: object, **kwargs: object) -> TaskRecord:
        nonlocal recovery_effects
        del args, kwargs
        recovery_effects += 1
        raise AssertionError("startup/read/send must not invoke an effect")

    second_factory.control.spawn_task = forbidden  # type: ignore[method-assign]
    second_factory.control.send_task_input = forbidden  # type: ignore[method-assign]
    try:
        with TestClient(second_app) as client:
            queried = client.get(f"/tasks/{cancelled.task_id}", headers=auth_headers())
            sent = client.post(
                f"/tasks/{cancelled.task_id}/input",
                headers=auth_headers(),
                json={"input": {"content": "must stay stopped"}},
            )
        assert queried.status_code == 200
        assert queried.json()["status"] == "failed"
        assert sent.status_code == 409
        assert recovery_effects == 0
    finally:
        second_routes.close()


def test_cancel_after_durable_local_run_recovers_active_without_respawn() -> None:
    async def scenario() -> None:
        control = _DurableThenBlockControl()
        store = GatewayRouteStore(":memory:")
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]
        request = asyncio.create_task(
            router.create_task(
                agent_name="main",
                route_kind="local",
                input_content="durable before response",
                metadata={},
                webhook=None,
                delegation_context=None,
                task_id="durable-local",
            )
        )
        await asyncio.wait_for(control.started.wait(), timeout=2)
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request

        route = await router.get_route("durable-local")
        assert route.route_state == "active"
        assert (await router.get_record(route)).run_count == 1
        assert control.spawn_calls == 1
        store.close()

    asyncio.run(scenario())
