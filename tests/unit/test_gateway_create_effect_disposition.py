from __future__ import annotations

import sqlite3
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
import pytest

from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from tests.unit.gateway_http_support import auth_headers, build_app


_REMOTE_URL = "https://effect-boundary.test/a2a"
_HEADERS = {**auth_headers(), "Idempotency-Key": "stable-create-command"}
_BODY = {"input": {"content": "create exactly once"}, "metadata": {}}


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
