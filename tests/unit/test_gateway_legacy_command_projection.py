from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from tests.unit.gateway_http_support import (
    StaticRemoteA2AClient,
    auth_headers,
    build_app,
)


PRIVATE_ID = "private-upstream-legacy-77"
PRIVATE_URL = f"https://remote.invalid/tasks/{PRIVATE_ID}"


def _paths(tmp_path: Path, stem: str) -> tuple[str, str]:
    return (
        str(tmp_path / f"{stem}-routes.sqlite"),
        str(tmp_path / f"{stem}-commands.sqlite"),
    )


def _create_keyed_remote_task(
    monkeypatch: pytest.MonkeyPatch,
    *,
    route_path: str,
    command_path: str,
    key: str,
) -> tuple[dict[str, object], StaticRemoteA2AClient]:
    remote = StaticRemoteA2AClient()
    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=routes,
        command_store=commands,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    with TestClient(app) as client:
        response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers={**auth_headers(), "Idempotency-Key": key},
            json={"input": {"content": "legacy projection"}, "metadata": {}},
        )
    assert response.status_code == 201
    payload = response.json()
    routes.close()
    commands.close()
    return payload, remote


def _replay(
    monkeypatch: pytest.MonkeyPatch,
    *,
    route_path: str,
    command_path: str,
    key: str,
    remote: StaticRemoteA2AClient,
):
    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=routes,
        command_store=commands,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    try:
        with TestClient(app) as client:
            response = client.post(
                "/agents/remote_code_wiki/tasks",
                headers={**auth_headers(), "Idempotency-Key": key},
                json={"input": {"content": "legacy projection"}, "metadata": {}},
            )
        route = routes.get_route(response.json().get("task_id", ""))
        return response, route
    finally:
        routes.close()
        commands.close()


def test_legacy_succeeded_command_response_is_reprojected_to_public_route(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    route_path, command_path = _paths(tmp_path, "legacy-success")
    key = "legacy-success-key"
    original, remote = _create_keyed_remote_task(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        key=key,
    )
    public_id = str(original["task_id"])
    malicious = {
        **original,
        "task_id": PRIVATE_ID,
        "parent_task_id": PRIVATE_ID,
        "root_task_id": PRIVATE_ID,
        "error": f"failed {PRIVATE_ID} at {PRIVATE_URL}",
        "metadata": {"private_task_url": PRIVATE_URL},
        "pending_review": {
            "review_id": "legacy-review",
            "source_task_id": PRIVATE_ID,
            "action_requests": [],
            "review_configs": [],
        },
        "artifacts": [
            {
                "artifact_id": PRIVATE_ID,
                "path": f"/tasks/{PRIVATE_ID}",
                "name": "private",
                "content_type": "text/plain",
                "size": 1,
                "run_count": 1,
            }
        ],
    }
    with sqlite3.connect(command_path) as connection:
        connection.execute(
            "UPDATE gateway_commands SET response_json = ? WHERE idempotency_key = ?",
            (json.dumps(malicious), key),
        )

    replay, _ = _replay(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        key=key,
        remote=remote,
    )

    assert replay.status_code == 201
    assert replay.headers["idempotency-replayed"] == "true"
    assert replay.json()["task_id"] == public_id
    assert replay.json()["root_task_id"] == public_id
    assert replay.json()["parent_task_id"] is None
    assert replay.json()["pending_review"]["source_task_id"] == public_id
    assert replay.json()["artifacts"] == []
    assert PRIVATE_ID not in replay.text
    assert "remote.invalid" not in replay.text
    assert remote.created_inputs == ["legacy projection"]


def test_legacy_terminal_command_error_and_route_write_are_public_only(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    route_path, command_path = _paths(tmp_path, "legacy-terminal")
    key = "legacy-terminal-key"
    original, remote = _create_keyed_remote_task(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        key=key,
    )
    public_id = str(original["task_id"])
    legacy_error = {
        "kind": "upstream_failure",
        "code": "upstream_gateway_error",
        "message": f"failed {PRIVATE_ID} at {PRIVATE_URL}",
        "details": {
            "task_id": PRIVATE_ID,
            "task_url": f"/tasks/{PRIVATE_ID}",
            "nested": {"upstream": PRIVATE_URL},
        },
    }
    with sqlite3.connect(command_path) as connection:
        connection.execute(
            """
            UPDATE gateway_commands
            SET state = 'failed', response_json = NULL, error_json = ?,
                claim_token = NULL
            WHERE idempotency_key = ?
            """,
            (json.dumps(legacy_error), key),
        )
    with sqlite3.connect(route_path) as connection:
        connection.execute(
            """
            UPDATE gateway_task_routes
            SET route_state = 'pending', upstream_task_id = NULL, route_error = ?
            WHERE task_id = ?
            """,
            (f"raw {PRIVATE_ID} {PRIVATE_URL}", public_id),
        )

    replay, _ = _replay(
        monkeypatch,
        route_path=route_path,
        command_path=command_path,
        key=key,
        remote=remote,
    )

    assert replay.status_code == 502
    assert replay.json()["error"] == {
        "code": "upstream_gateway_error",
        "message": "Remote Gateway Task creation failed",
        "details": {
            "task_id": public_id,
            "task_queryable": True,
            "task_url": f"/tasks/{public_id}",
            "route_state": "uncertain",
            "create_retryable": False,
            "effect_outcome": "uncertain",
        },
    }
    assert PRIVATE_ID not in replay.text
    assert "remote.invalid" not in replay.text
    routes = GatewayRouteStore(route_path)
    try:
        route = routes.get_route(public_id)
        assert route is not None
        assert route.route_state == "uncertain"
        assert route.route_error == "Remote Gateway Task creation failed"
    finally:
        routes.close()
    assert remote.created_inputs == ["legacy projection"]
