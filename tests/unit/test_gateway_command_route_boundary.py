from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient
import pytest

from ruyi_agent.gateway.commands import command_request_hash
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import TaskRouteRecord
from tests.unit.gateway_http_support import (
    StaticRemoteA2AClient,
    auth_headers,
    build_app,
)


def _row(db_path: str, query: str) -> sqlite3.Row:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(query).fetchone()
        assert row is not None
        return row
    finally:
        connection.close()


def test_command_marker_before_route_boundary_replays_authoritative_not_started(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    route_path = str(tmp_path / "routes.sqlite")
    command_path = str(tmp_path / "commands.sqlite")
    task_id = "public-reserved-create"
    idempotency_key = "crash-before-route-boundary"
    input_content = "must never dispatch"

    routes = GatewayRouteStore(route_path)
    routes.reserve_route(
        TaskRouteRecord(
            task_id=task_id,
            agent_name="remote_code_wiki",
            metadata={},
            route_kind="remote_ref",
            upstream_task_id=None,
            route_state="pending",
        ),
        create_key_scope="external",
        create_replay_policy="never",
    )
    commands = GatewayCommandStore(command_path)
    claim = commands.claim(
        principal_id="gateway-bearer",
        idempotency_key=idempotency_key,
        operation="create_task",
        target="remote_code_wiki",
        request_hash=command_request_hash(
            operation="create_task",
            target="remote_code_wiki",
            body={
                "input": {"content": input_content, "attachments": []},
                "metadata": {},
                "webhook": None,
            },
        ),
        proposed_task_id=task_id,
    )
    assert claim.claim_token is not None
    commands.mark_effect_started(
        command_id=claim.command_id,
        claim_token=claim.claim_token,
        replay_safe=False,
    )

    route_before = routes.get_route(task_id)
    evidence_before = routes.get_create_evidence(task_id)
    command_before = _row(
        command_path,
        "SELECT state, effect_started, replay_safe FROM gateway_commands",
    )
    assert route_before is not None and route_before.route_state == "pending"
    assert evidence_before is not None
    assert evidence_before.effect_boundary == "reserved"
    assert tuple(command_before) == ("processing", 1, 0)
    routes.close()
    commands.close()

    # Startup recovery sees both durable facts.  The command ledger remains
    # conservatively terminal, while the route boundary proves no effect began.
    recovered_routes = GatewayRouteStore(route_path)
    recovered_commands = GatewayCommandStore(command_path)
    recovered_route = recovered_routes.get_route(task_id)
    recovered_command = _row(
        command_path,
        "SELECT state, error_json FROM gateway_commands",
    )
    assert recovered_route is not None and recovered_route.route_state == "failed"
    assert recovered_routes.get_create_evidence(task_id) == evidence_before
    assert recovered_command["state"] == "failed"
    assert (
        json.loads(str(recovered_command["error_json"]))["code"]
        == "idempotency_outcome_uncertain"
    )

    remote = StaticRemoteA2AClient()
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=recovered_routes,
        command_store=recovered_commands,
    )
    try:
        with TestClient(app) as client:
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers={**auth_headers(), "Idempotency-Key": idempotency_key},
                json={"input": {"content": input_content}, "metadata": {}},
            )
            queried = client.get(f"/tasks/{task_id}", headers=auth_headers())

        assert replay.status_code == 409
        assert replay.json() == {
            "error": {
                "code": "task_creation_not_retryable",
                "message": "Gateway Task creation did not start",
                "details": {
                    "task_id": task_id,
                    "task_queryable": True,
                    "task_url": f"/tasks/{task_id}",
                    "route_state": "failed",
                    "create_retryable": False,
                    "effect_outcome": "not_started",
                },
            }
        }
        assert "uncertain" not in replay.text
        assert "may have reached" not in replay.text
        assert queried.status_code == 200
        assert queried.json()["status"] == "failed"
        assert remote.created_inputs == []
    finally:
        recovered_routes.close()
        recovered_commands.close()
