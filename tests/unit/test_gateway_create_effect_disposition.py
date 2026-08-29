from __future__ import annotations

import asyncio
import multiprocessing
import sqlite3
import threading
from pathlib import Path
from typing import Any

import httpx
from fastapi.testclient import TestClient
import pytest

from ruyi_agent.gateway.commands import command_request_hash
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import TaskRouteRecord
from tests.unit.gateway_http_support import auth_headers, build_app


_REMOTE_URL = "https://effect-boundary.test/a2a"
_HEADERS = {**auth_headers(), "Idempotency-Key": "stable-create-command"}
_BODY = {"input": {"content": "create exactly once"}, "metadata": {}}


class _BlockingResetRouteStore(GatewayRouteStore):
    def __init__(self, db_path: str, *, fail: bool = False) -> None:
        super().__init__(db_path)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.fail = fail

    def restore_create_not_dispatched(self, task_id: str):
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise TimeoutError("reset test gate timed out")
        if self.fail:
            raise RuntimeError("injected reset rollback")
        return super().restore_create_not_dispatched(task_id)


class _CancelAfterResetRouteStore(GatewayRouteStore):
    async def arestore_create_not_dispatched(self, task_id: str):
        route = await super().arestore_create_not_dispatched(task_id)
        current = asyncio.current_task()
        assert current is not None
        current.cancel("cancel after route reset commit")
        await asyncio.sleep(0)
        return route  # pragma: no cover - cancellation is injected above


class _BlockingReleaseCommandStore(GatewayCommandStore):
    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def arelease_not_dispatched(self, **kwargs: str) -> None:
        self.entered.set()
        await self.release.wait()
        await super().arelease_not_dispatched(**kwargs)


def _leave_committed_reset_before_command_release(
    route_path: str,
    command_path: str,
    ready: Any,
) -> None:
    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    routes.reserve_route(
        TaskRouteRecord(
            task_id="process-reset-task",
            agent_name="remote_code_wiki",
            metadata={},
            route_kind="remote_ref",
            upstream_task_id=None,
            route_state="pending",
        ),
        create_key_scope="external",
        create_replay_policy="never",
    )
    claim = commands.claim(
        principal_id="gateway-bearer",
        idempotency_key=_HEADERS["Idempotency-Key"],
        operation="create_task",
        target="remote_code_wiki",
        request_hash=command_request_hash(
            operation="create_task",
            target="remote_code_wiki",
            body={
                "input": {"content": "create exactly once", "attachments": []},
                "metadata": {},
                "webhook": None,
            },
        ),
        proposed_task_id="process-reset-task",
    )
    assert claim.claim_token is not None
    commands.mark_effect_started(
        command_id=claim.command_id,
        claim_token=claim.claim_token,
        replay_safe=False,
    )
    routes.mark_create_effect_started(claim.task_id)
    routes.restore_create_not_dispatched(claim.task_id)
    ready.set()
    multiprocessing.Event().wait()


def _command_row(path: str) -> sqlite3.Row:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT state, task_id, effect_started, replay_safe, error_json "
            "FROM gateway_commands"
        ).fetchone()
        assert row is not None
        return row
    finally:
        connection.close()


def _success_response(request: httpx.Request) -> httpx.Response:
    return httpx.Response(
        201,
        request=request,
        json={
            "task_id": "private-upstream-id",
            "agent_name": "code_wiki",
            "status": "completed",
            "last_result": "created",
            "error": None,
            "run_count": 1,
            "created_at": "2026-08-30T00:00:00Z",
            "updated_at": "2026-08-30T00:00:01Z",
        },
    )


async def _cancel_while_waiting(
    app: object,
    *,
    entered: threading.Event | asyncio.Event,
    release: threading.Event | asyncio.Event,
) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),  # type: ignore[arg-type]
        base_url="http://gateway.test",
    ) as client:
        request = asyncio.create_task(
            client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )
        )
        if isinstance(entered, threading.Event):
            assert await asyncio.to_thread(entered.wait, 2)
        else:
            await asyncio.wait_for(entered.wait(), timeout=2)
        request.cancel("client disconnected during durable cleanup")
        await asyncio.sleep(0)
        request.cancel("shutdown repeated cancellation")
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await request


def _assert_reserved_retry_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    *,
    route_path: str,
    command_path: str,
    task_id: str,
    effect_requests: list[httpx.Request],
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def repaired(request: httpx.Request) -> httpx.Response:
        effect_requests.append(request)
        return _success_response(request)

    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(repaired)}
        ),
        route_store=routes,
        command_store=commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    try:
        with TestClient(app) as client:
            recovered = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )
        assert recovered.status_code == replay.status_code == 201
        assert recovered.json()["task_id"] == task_id
        assert replay.json() == recovered.json()
        assert replay.headers["idempotency-replayed"] == "true"
        assert len(effect_requests) == 1
    finally:
        routes.close()
        commands.close()


def _assert_pending_reserved(
    routes: GatewayRouteStore,
    *,
    command_path: str,
) -> str:
    [route] = routes.list_routes()
    evidence = routes.get_create_evidence(route.task_id)
    command = _command_row(command_path)
    assert route.route_state == "pending"
    assert evidence is not None and evidence.effect_boundary == "reserved"
    assert tuple(command[key] for key in ("state", "effect_started")) == (
        "pending",
        0,
    )
    return route.task_id


def test_cancel_during_route_reset_waits_for_commit_before_command_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("REMOTE_CODE_WIKI_TOKEN", raising=False)
    route_path = str(tmp_path / "cancel-mid-reset-routes.sqlite")
    command_path = str(tmp_path / "cancel-mid-reset-commands.sqlite")
    effects: list[httpx.Request] = []

    def forbidden(request: httpx.Request) -> httpx.Response:
        effects.append(request)
        return _success_response(request)

    routes = _BlockingResetRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(forbidden)}
        ),
        route_store=routes,
        command_store=commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    asyncio.run(
        _cancel_while_waiting(
            app,
            entered=routes.entered,
            release=routes.release,
        )
    )
    task_id = _assert_pending_reserved(routes, command_path=command_path)
    assert effects == []
    routes.close()
    commands.close()
    _assert_reserved_retry_after_restart(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        task_id=task_id,
        effect_requests=effects,
    )


def test_cancel_after_route_reset_commit_cannot_terminalize_reserved_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("REMOTE_CODE_WIKI_TOKEN", raising=False)
    route_path = str(tmp_path / "cancel-after-reset-routes.sqlite")
    command_path = str(tmp_path / "cancel-after-reset-commands.sqlite")
    effects: list[httpx.Request] = []
    routes = _CancelAfterResetRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(_success_response)}
        ),
        route_store=routes,
        command_store=commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )

    async def request() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway.test",
        ) as client:
            with pytest.raises(asyncio.CancelledError):
                await client.post(
                    "/agents/remote_code_wiki/tasks",
                    headers=_HEADERS,
                    json=_BODY,
                )

    asyncio.run(request())
    task_id = _assert_pending_reserved(routes, command_path=command_path)
    routes.close()
    commands.close()
    _assert_reserved_retry_after_restart(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        task_id=task_id,
        effect_requests=effects,
    )


def test_cancel_before_command_release_finishes_release_then_propagates(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("REMOTE_CODE_WIKI_TOKEN", raising=False)
    route_path = str(tmp_path / "cancel-before-release-routes.sqlite")
    command_path = str(tmp_path / "cancel-before-release-commands.sqlite")
    effects: list[httpx.Request] = []
    routes = GatewayRouteStore(route_path)
    commands = _BlockingReleaseCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(_success_response)}
        ),
        route_store=routes,
        command_store=commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    asyncio.run(
        _cancel_while_waiting(
            app,
            entered=commands.entered,
            release=commands.release,
        )
    )
    task_id = _assert_pending_reserved(routes, command_path=command_path)
    routes.close()
    commands.close()
    _assert_reserved_retry_after_restart(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        task_id=task_id,
        effect_requests=effects,
    )


def test_cancelled_reset_rollback_keeps_started_create_conservative(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("REMOTE_CODE_WIKI_TOKEN", raising=False)
    route_path = str(tmp_path / "cancel-rollback-routes.sqlite")
    command_path = str(tmp_path / "cancel-rollback-commands.sqlite")
    effects: list[httpx.Request] = []
    routes = _BlockingResetRouteStore(route_path, fail=True)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(_success_response)}
        ),
        route_store=routes,
        command_store=commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    asyncio.run(
        _cancel_while_waiting(
            app,
            entered=routes.entered,
            release=routes.release,
        )
    )
    [route] = routes.list_routes()
    evidence = routes.get_create_evidence(route.task_id)
    assert route.route_state == "uncertain"
    assert evidence is not None and evidence.effect_boundary == "started"
    assert _command_row(command_path)["state"] == "failed"
    routes.close()
    commands.close()

    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def forbidden_replay(request: httpx.Request) -> httpx.Response:
        effects.append(request)
        return _success_response(request)

    recovered_routes = GatewayRouteStore(route_path)
    recovered_commands = GatewayCommandStore(command_path)
    recovered_app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(forbidden_replay)}
        ),
        route_store=recovered_routes,
        command_store=recovered_commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    try:
        with TestClient(recovered_app) as client:
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )
        assert replay.status_code == 409
        assert replay.json()["error"]["details"]["effect_outcome"] == "uncertain"
        assert effects == []
    finally:
        recovered_routes.close()
        recovered_commands.close()


def test_process_exit_after_route_reset_reopens_command_on_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    route_path = str(tmp_path / "process-reset-routes.sqlite")
    command_path = str(tmp_path / "process-reset-commands.sqlite")
    context = multiprocessing.get_context("fork")
    ready = context.Event()
    process = context.Process(
        target=_leave_committed_reset_before_command_release,
        args=(route_path, command_path, ready),
    )
    process.start()
    try:
        assert ready.wait(timeout=5)
        process.kill()
        process.join(timeout=5)
        assert process.exitcode is not None and process.exitcode != 0
    finally:
        if process.is_alive():
            process.kill()
            process.join(timeout=5)

    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    route = routes.get_route("process-reset-task")
    evidence = routes.get_create_evidence("process-reset-task")
    command = _command_row(command_path)
    assert route is not None and route.route_state == "pending"
    assert evidence is not None and evidence.effect_boundary == "reserved"
    assert command["state"] == "failed"
    routes.close()
    commands.close()

    effects: list[httpx.Request] = []
    _assert_reserved_retry_after_restart(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        task_id="process-reset-task",
        effect_requests=effects,
    )


@pytest.mark.parametrize("failure", ["missing_token", "invalid_url", "connect"])
def test_real_a2a_not_dispatched_create_retries_same_identity_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure: str,
) -> None:
    route_path = str(tmp_path / f"{failure}-routes.sqlite")
    command_path = str(tmp_path / f"{failure}-commands.sqlite")
    effect_requests: list[httpx.Request] = []
    connect_attempts: list[httpx.Request] = []

    def forbidden_effect(request: httpx.Request) -> httpx.Response:
        effect_requests.append(request)
        return _success_response(request)

    def connect_failure(request: httpx.Request) -> httpx.Response:
        connect_attempts.append(request)
        raise httpx.ConnectError("connection refused before send", request=request)

    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    first_url = _REMOTE_URL
    first_transport: httpx.AsyncBaseTransport | None = httpx.MockTransport(
        forbidden_effect
    )
    if failure == "missing_token":
        monkeypatch.delenv("REMOTE_CODE_WIKI_TOKEN")
    elif failure == "invalid_url":
        first_url = "not-a-valid-http-url"
        first_transport = None
    else:
        first_transport = httpx.MockTransport(connect_failure)

    first_routes = GatewayRouteStore(route_path)
    first_commands = GatewayCommandStore(command_path)
    first_client = A2AClient(
        transports=({first_url: first_transport} if first_transport is not None else {})
    )
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=first_client,
        route_store=first_routes,
        command_store=first_commands,
        remote_url=_REMOTE_URL,
        remote_ref_url=first_url,
        remote_create_idempotency="none",
    )
    with TestClient(first_app) as client:
        failed = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=_HEADERS,
            json=_BODY,
        )
        failed_task_id = failed.json()["error"]["details"]["task_id"]
        queried = client.get(
            f"/tasks/{failed_task_id}",
            headers=auth_headers(),
        )

    assert failed.status_code == 502
    details = failed.json()["error"]["details"]
    task_id = details["task_id"]
    assert queried.status_code == 200
    assert queried.json()["status"] == "interrupted"
    assert details == {
        "task_id": task_id,
        "task_url": f"/tasks/{task_id}",
        "route_state": "pending",
        "task_queryable": True,
        "create_retryable": True,
        "effect_outcome": "not_started",
    }
    evidence = first_routes.get_create_evidence(task_id)
    route = first_routes.get_route(task_id)
    command = _command_row(command_path)
    assert route is not None and route.route_state == "pending"
    assert evidence is not None and evidence.effect_boundary == "reserved"
    assert tuple(command[key] for key in ("state", "effect_started", "replay_safe")) == (
        "pending",
        0,
        0,
    )
    assert command["task_id"] == task_id
    assert command["error_json"] is None
    assert effect_requests == []
    assert len(connect_attempts) == (1 if failure == "connect" else 0)
    first_routes.close()
    first_commands.close()

    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def repaired_effect(request: httpx.Request) -> httpx.Response:
        effect_requests.append(request)
        return _success_response(request)

    second_routes = GatewayRouteStore(route_path)
    second_commands = GatewayCommandStore(command_path)
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(repaired_effect)}
        ),
        route_store=second_routes,
        command_store=second_commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    try:
        with TestClient(second_app) as client:
            recovered = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )

        assert recovered.status_code == replay.status_code == 201
        assert recovered.json()["task_id"] == task_id
        assert replay.json() == recovered.json()
        assert replay.headers["idempotency-replayed"] == "true"
        assert len(effect_requests) == 1
        assert effect_requests[0].headers["idempotency-key"] == _HEADERS[
            "Idempotency-Key"
        ]
        assert second_routes.get_route(task_id).route_state == "active"  # type: ignore[union-attr]
        assert _command_row(command_path)["state"] == "succeeded"
    finally:
        second_routes.close()
        second_commands.close()


@pytest.mark.parametrize(
    ("status_code", "expected_state", "expected_outcome"),
    [(400, "failed", "not_started"), (503, "uncertain", "uncertain")],
)
def test_real_a2a_remote_response_remains_terminal_by_disposition(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status_code: int,
    expected_state: str,
    expected_outcome: str,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    requests: list[httpx.Request] = []

    def reject(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            status_code,
            request=request,
            json={
                "error": {
                    "code": "invalid_request",
                    "message": "untrusted remote response",
                }
            },
        )

    route_path = str(tmp_path / f"response-{status_code}-routes.sqlite")
    command_path = str(tmp_path / f"response-{status_code}-commands.sqlite")
    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(reject)}
        ),
        route_store=routes,
        command_store=commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    with TestClient(app) as client:
        first = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=_HEADERS,
            json=_BODY,
        )
    routes.close()
    commands.close()

    recovered_routes = GatewayRouteStore(route_path)
    recovered_commands = GatewayCommandStore(command_path)
    recovered_app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(
            transports={_REMOTE_URL: httpx.MockTransport(reject)}
        ),
        route_store=recovered_routes,
        command_store=recovered_commands,
        remote_url=_REMOTE_URL,
        remote_create_idempotency="none",
    )
    try:
        with TestClient(recovered_app) as client:
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=_HEADERS,
                json=_BODY,
            )
        assert first.status_code == replay.status_code == 502
        assert replay.json() == first.json()
        details = first.json()["error"]["details"]
        assert details["route_state"] == expected_state
        assert details["effect_outcome"] == expected_outcome
        assert details["create_retryable"] is False
        assert len(requests) == 1
        assert _command_row(command_path)["state"] == "failed"
    finally:
        recovered_routes.close()
        recovered_commands.close()
