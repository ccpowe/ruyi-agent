from __future__ import annotations

import asyncio
import base64
import json
import multiprocessing
import time
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from ruyi_agent.runtime.delegation.async_runtime import AgentControl
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.runtime.delegation.context import (
    CONTEXT_VERSION,
    CONTEXT_VERSION_FIELD,
    DEPTH_FIELD,
    MAX_DEPTH_FIELD,
    MAX_TASKS_PER_ROOT_FIELD,
    ROOT_ID_FIELD,
    VISITED_NODES_FIELD,
)
from ruyi_agent.task_models import TaskRouteRecord
from ruyi_agent.gateway.commands import command_request_hash
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from tests.unit.gateway_http_support import (
    DelayedAgentFactory,
    FailOnceGatewayCommandStore,
    RemoteBackDelegatingAgentFactory,
    ReviewInterruptingAgentFactory,
    ReviewRemoteA2AClient,
    StaticRemoteA2AClient,
    UnhashableStatusRemoteA2AClient,
    auth_headers,
    build_agent_configs,
    build_app,
    build_local_agent_config,
    build_specs,
)
from tests.support.async_subagent_runtime import wait_for_task_state


class ResponseLostRemoteA2AClient(StaticRemoteA2AClient):
    async def create_task(self, *args, **kwargs):
        await super().create_task(*args, **kwargs)
        await asyncio.sleep(0.02)
        raise A2AClientError(
            status_code=502,
            code="upstream_gateway_error",
            message="remote response was lost",
        )


class RejectedRemoteA2AClient(StaticRemoteA2AClient):
    async def create_task(self, *args, **kwargs):
        await super().create_task(*args, **kwargs)
        raise A2AClientError(
            status_code=400,
            code="invalid_request",
            message="remote rejected create",
        )


class CancelledRemoteA2AClient(StaticRemoteA2AClient):
    def __init__(self) -> None:
        super().__init__()
        self.effect_started = asyncio.Event()

    async def create_task(self, remote_ref, *, input_content, metadata, **kwargs):
        del remote_ref, metadata, kwargs
        self.created_inputs.append(input_content)
        self.effect_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


def _leave_started_active_create_in_killed_process(
    route_path: str,
    command_path: str,
    ready,
) -> None:
    routes = GatewayRouteStore(route_path)
    commands = GatewayCommandStore(command_path)
    routes.save_route(
        TaskRouteRecord(
            task_id="reserved-before-process-loss",
            agent_name="remote_code_wiki",
            metadata={},
            route_kind="remote_ref",
            upstream_task_id="private-upstream-task",
            route_state="active",
        )
    )
    claim = commands.claim(
        principal_id="gateway-bearer",
        idempotency_key="process-loss-create",
        operation="create_task",
        target="remote_code_wiki",
        request_hash=command_request_hash(
            operation="create_task",
            target="remote_code_wiki",
            body={
                "input": {"content": "unknown outcome", "attachments": []},
                "metadata": {},
                "webhook": None,
            },
        ),
        proposed_task_id="reserved-before-process-loss",
    )
    assert claim.claim_token is not None
    commands.mark_effect_started(
        command_id=claim.command_id,
        claim_token=claim.claim_token,
        replay_safe=False,
    )
    ready.set()
    multiprocessing.Event().wait()


def test_review_cursor_malicious_base64_json_returns_http_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    cursor = base64.urlsafe_b64encode(
        b'{"fallback_updated_at":"2026-08-29T00:00:00",'
        b'"resume_review_id":"review-1","version":2}'
    ).decode()

    with TestClient(app) as client:
        response = client.get(
            f"/reviews?cursor={cursor}",
            headers=auth_headers(),
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"


def test_cancelled_remote_http_create_is_terminal_across_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote = CancelledRemoteA2AClient()
    route_path = str(tmp_path / "cancelled-routes.sqlite")
    command_path = str(tmp_path / "cancelled-commands.sqlite")
    headers = {**auth_headers(), "Idempotency-Key": "cancelled-create"}
    body = {"input": {"content": "create once"}, "metadata": {}}
    first_routes = GatewayRouteStore(route_path)
    first_commands = GatewayCommandStore(command_path)
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=first_routes,
        command_store=first_commands,
    )

    async def cancel_request() -> None:
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gateway.test",
        ) as client:
            request = asyncio.create_task(
                client.post(
                    "/agents/remote_code_wiki/tasks",
                    headers=headers,
                    json=body,
                )
            )
            await asyncio.wait_for(remote.effect_started.wait(), timeout=1)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request

    asyncio.run(cancel_request())
    first_routes.close()
    first_commands.close()

    second_routes = GatewayRouteStore(route_path)
    second_commands = GatewayCommandStore(command_path)
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=second_routes,
        command_store=second_commands,
    )
    try:
        with TestClient(second_app) as client:
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=body,
            )
            task_id = replay.json()["error"]["details"]["task_id"]
            queried = client.get(f"/tasks/{task_id}", headers=auth_headers())

        assert replay.status_code == 409
        assert replay.json()["error"]["code"] == "idempotency_outcome_uncertain"
        assert replay.json()["error"]["details"]["route_state"] == "uncertain"
        assert queried.status_code == 200
        assert queried.json()["status"] == "interrupted"
        assert remote.created_inputs == ["create once"]
    finally:
        second_routes.close()
        second_commands.close()


def test_remote_ref_forwards_via_a2a(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    remote_factory = DelayedAgentFactory(delay=0.03)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", remote_factory)

    remote_db = str(tmp_path / "remote-tasks.sqlite")
    remote_task_store = TaskStore(remote_db)
    remote_mailbox_store = MailboxStore(remote_db)
    remote_control = AgentControl(
        {
            "code_wiki": LocalWorkerSpec(
                name="code_wiki",
                description="remote code wiki",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=["/sandbox/home/AGENTS.md"],
                skills=["frontend-skill"],
            )
        },
        {},
        checkpointer=object(),
        backend=object(),
        task_store=remote_task_store,
        mailbox=AgentMailbox(remote_mailbox_store),
    )
    remote_service = GatewayTaskModule(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": build_local_agent_config(
                "code_wiki",
                "remote code wiki",
            )
        },
        control=remote_control,
    )
    remote_app = create_gateway_app(
        service=remote_service,
        bearer_token="remote-secret",
    )
    remote_root_app = FastAPI()
    remote_root_app.mount("/a2a", remote_app)
    transport = httpx.ASGITransport(app=remote_root_app)

    app, _ = build_app(
        monkeypatch,
        delay=0.03,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": transport},
        ),
    )
    with TestClient(app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "explain repo"},
                "metadata": {"channel": "telegram"},
            },
        )
        assert create_response.status_code == 201, create_response.json()
        create_payload = create_response.json()
        assert create_payload["agent_name"] == "remote_code_wiki"
        proxy_task_id = create_payload["task_id"]
        assert create_payload["depth"] == 1

        time.sleep(0.08)
        get_response = client.get(f"/tasks/{proxy_task_id}", headers=auth_headers())
        assert get_response.status_code == 200
        assert get_response.json()["status"] == "completed"
        assert get_response.json()["agent_name"] == "remote_code_wiki"
        assert get_response.json()["last_result"] == "done: explain repo"

        send_response = client.post(
            f"/tasks/{proxy_task_id}/input",
            headers=auth_headers(),
            json={"input": {"content": "follow up"}},
        )
        assert send_response.status_code == 202
        assert send_response.json()["task_id"] == proxy_task_id
        assert send_response.json()["agent_name"] == "remote_code_wiki"

        time.sleep(0.08)
        cancel_response = client.post(
            f"/tasks/{proxy_task_id}/cancel",
            headers=auth_headers(),
        )
        assert cancel_response.status_code == 202
        assert cancel_response.json()["task_id"] == proxy_task_id

        list_response = client.get(
            "/tasks?agent_name=remote_code_wiki&metadata.channel=telegram",
            headers=auth_headers(),
        )
        assert list_response.status_code == 200
        items = list_response.json()["items"]
        assert len(items) == 1
        assert items[0]["task_id"] == proxy_task_id
        assert items[0]["agent_name"] == "remote_code_wiki"
    remote_task_store.close()
    remote_mailbox_store.close()


def test_public_remote_ref_create_injects_delegation_context_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a2a_client = StaticRemoteA2AClient()
    app, _ = build_app(
        monkeypatch,
        a2a_client=a2a_client,  # type: ignore[arg-type]
        node_id="node-a",
    )

    with TestClient(app) as client:
        response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "explain repo"},
                "metadata": {"channel": "tg"},
            },
        )

    assert response.status_code == 201
    assert len(a2a_client.created_metadata) == 1
    metadata = a2a_client.created_metadata[0]
    assert metadata["channel"] == "tg"
    assert metadata[CONTEXT_VERSION_FIELD] == CONTEXT_VERSION
    assert metadata[ROOT_ID_FIELD].startswith("node-a:")
    assert metadata[DEPTH_FIELD] == 1
    assert metadata[MAX_DEPTH_FIELD] == 3
    assert metadata[MAX_TASKS_PER_ROOT_FIELD] == 20
    assert metadata[VISITED_NODES_FIELD] == '["node-a"]'


def test_declared_remote_create_capability_generates_downstream_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = StaticRemoteA2AClient()
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        remote_create_idempotency="ruyi_gateway_v1",
    )

    with TestClient(app) as client:
        response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "safe create"}, "metadata": {}},
        )

    assert response.status_code == 201
    assert len(remote.created_idempotency_keys) == 1
    assert remote.created_idempotency_keys[0] == (
        f"gateway-create:{response.json()['task_id']}"
    )


def test_public_remote_ref_maps_unhashable_status_to_upstream_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(
        monkeypatch,
        a2a_client=UnhashableStatusRemoteA2AClient(),  # type: ignore[arg-type]
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "invalid remote"}, "metadata": {}},
        )

    assert response.status_code == 502
    error = response.json()["error"]
    assert error["code"] == "upstream_gateway_error"
    task_id = error["details"]["task_id"]
    assert error["details"] == {
        "task_id": task_id,
        "task_url": f"/tasks/{task_id}",
        "route_state": "uncertain",
        "task_queryable": True,
        "create_retryable": False,
        "effect_outcome": "uncertain",
    }
    with TestClient(app) as client:
        query = client.get(f"/tasks/{task_id}", headers=auth_headers())
    assert query.status_code == 200
    assert query.json()["task_id"] == task_id
    assert query.json()["status"] == "interrupted"


def test_remote_response_lost_is_terminal_across_restart_and_queryable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote = ResponseLostRemoteA2AClient()
    route_path = str(tmp_path / "routes.sqlite")
    command_path = str(tmp_path / "commands.sqlite")
    headers = {**auth_headers(), "Idempotency-Key": "response-lost-create"}
    body = {"input": {"content": "create once"}, "metadata": {}}

    first_routes = GatewayRouteStore(route_path)
    first_commands = GatewayCommandStore(command_path)
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=first_routes,
        command_store=first_commands,
    )
    with TestClient(first_app) as client:
        first = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=headers,
            json=body,
        )
    first_routes.close()
    first_commands.close()

    second_routes = GatewayRouteStore(route_path)
    second_commands = GatewayCommandStore(command_path)
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=second_routes,
        command_store=second_commands,
    )
    try:
        with TestClient(second_app) as client:
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=body,
            )
            task_id = replay.json()["error"]["details"]["task_id"]
            queried = client.get(f"/tasks/{task_id}", headers=auth_headers())
            listed = client.get("/tasks", headers=auth_headers())

        assert first.status_code == replay.status_code == 502
        assert replay.json() == first.json()
        assert replay.json()["error"]["details"] == {
            "task_id": task_id,
            "task_url": f"/tasks/{task_id}",
            "route_state": "uncertain",
            "task_queryable": True,
            "create_retryable": False,
            "effect_outcome": "uncertain",
        }
        assert queried.status_code == 200
        assert queried.json()["status"] == "interrupted"
        assert [item["task_id"] for item in listed.json()["items"]] == [task_id]
        assert remote.created_inputs == ["create once"]
    finally:
        second_routes.close()
        second_commands.close()


def test_process_restart_terminalizes_started_remote_create_without_replay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    route_path = str(tmp_path / "routes.sqlite")
    command_path = str(tmp_path / "commands.sqlite")
    task_id = "reserved-before-process-loss"
    idempotency_key = "process-loss-create"
    process_context = multiprocessing.get_context("fork")
    ready = process_context.Event()
    writer = process_context.Process(
        target=_leave_started_active_create_in_killed_process,
        args=(route_path, command_path, ready),
    )
    writer.start()
    assert ready.wait(timeout=5)
    writer.kill()
    writer.join(timeout=5)
    assert writer.exitcode is not None and writer.exitcode != 0

    remote = StaticRemoteA2AClient()
    second_routes = GatewayRouteStore(route_path)
    second_commands = GatewayCommandStore(command_path)
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=second_routes,
        command_store=second_commands,
    )
    try:
        with TestClient(app) as client:
            response = client.post(
                "/agents/remote_code_wiki/tasks",
                headers={
                    **auth_headers(),
                    "Idempotency-Key": idempotency_key,
                },
                json={"input": {"content": "unknown outcome"}, "metadata": {}},
            )
            queried = client.get(f"/tasks/{task_id}", headers=auth_headers())

        assert response.status_code == 409
        assert response.json() == {
            "error": {
                "code": "idempotency_outcome_uncertain",
                "message": (
                    "The previous Gateway command may have reached a "
                    "non-idempotent downstream service"
                ),
                "details": {
                    "task_id": task_id,
                    "task_url": f"/tasks/{task_id}",
                    "task_queryable": True,
                    "route_state": "active",
                    "create_retryable": False,
                    "effect_outcome": "completed",
                },
            }
        }
        assert queried.status_code == 200
        assert queried.json()["status"] == "completed"
        assert "private-upstream-task" not in response.text
        assert remote.created_inputs == []
    finally:
        second_routes.close()
        second_commands.close()


def test_concurrent_remote_response_lost_reuses_one_terminal_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = ResponseLostRemoteA2AClient()
    app, _ = build_app(monkeypatch, a2a_client=remote)  # type: ignore[arg-type]
    headers = {**auth_headers(), "Idempotency-Key": "concurrent-lost-create"}
    body = {"input": {"content": "create once"}, "metadata": {}}

    async def scenario() -> list[httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gateway.test",
        ) as client:
            return await asyncio.gather(
                *[
                    client.post(
                        "/agents/remote_code_wiki/tasks",
                        headers=headers,
                        json=body,
                    )
                    for _ in range(20)
                ]
            )

    responses = asyncio.run(scenario())

    assert {response.status_code for response in responses} == {502}
    assert (
        len({response.json()["error"]["details"]["task_id"] for response in responses})
        == 1
    )
    assert remote.created_inputs == ["create once"]


def test_remote_4xx_create_is_terminal_for_same_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = RejectedRemoteA2AClient()
    app, _ = build_app(monkeypatch, a2a_client=remote)  # type: ignore[arg-type]
    headers = {**auth_headers(), "Idempotency-Key": "remote-rejected-create"}
    body = {"input": {"content": "rejected"}, "metadata": {}}

    with TestClient(app) as client:
        first = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=headers,
            json=body,
        )
        replay = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=headers,
            json=body,
        )

    assert first.status_code == replay.status_code == 502
    assert replay.json() == first.json()
    details = replay.json()["error"]["details"]
    assert details["route_state"] == "failed"
    assert details["effect_outcome"] == "not_started"
    assert details["create_retryable"] is False
    assert remote.created_inputs == ["rejected"]


def test_remote_command_completion_crash_reuses_active_route_without_create(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote = StaticRemoteA2AClient()
    route_path = str(tmp_path / "routes.sqlite")
    command_path = str(tmp_path / "commands.sqlite")
    headers = {**auth_headers(), "Idempotency-Key": "remote-completion-crash"}
    body = {"input": {"content": "create once"}, "metadata": {}}

    first_routes = GatewayRouteStore(route_path)
    first_commands = FailOnceGatewayCommandStore(command_path)
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=first_routes,
        command_store=first_commands,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    with TestClient(first_app, raise_server_exceptions=False) as client:
        failed = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=headers,
            json=body,
        )
    first_routes.close()
    first_commands.close()

    second_routes = GatewayRouteStore(route_path)
    second_commands = GatewayCommandStore(command_path)
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=second_routes,
        command_store=second_commands,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    try:
        with TestClient(second_app) as client:
            recovered = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=body,
            )
            replay = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=body,
            )

        assert failed.status_code == 500
        assert recovered.status_code == replay.status_code == 201
        assert recovered.json() == replay.json()
        assert replay.headers["idempotency-replayed"] == "true"
        assert remote.created_inputs == ["create once"]
    finally:
        second_routes.close()
        second_commands.close()


@pytest.mark.parametrize("route_state", ["pending", "failed", "uncertain"])
def test_nonactive_remote_route_never_dispatches_http_operations(
    monkeypatch: pytest.MonkeyPatch,
    route_state: str,
) -> None:
    remote = StaticRemoteA2AClient()
    routes = GatewayRouteStore(":memory:")
    routes.save_route(
        TaskRouteRecord(
            task_id="nonactive-task",
            agent_name="remote_code_wiki",
            metadata={},
            route_kind="remote_ref",
            upstream_task_id=None,
            route_state=route_state,  # type: ignore[arg-type]
            route_error="response lost",
        )
    )
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=routes,
    )
    webhook = {
        "event_id": "event-1",
        "event_type": "task.completed",
        "task_id": "nonactive-task",
        "agent_name": "remote_code_wiki",
        "status": "completed",
        "last_result": "should not bind",
        "error": None,
        "run_count": 1,
        "created_at": "2026-04-23T00:00:00Z",
        "updated_at": "2026-04-23T00:00:01Z",
    }
    try:
        with TestClient(app) as client:
            queried = client.get("/tasks/nonactive-task", headers=auth_headers())
            listed = client.get("/tasks", headers=auth_headers())
            sent = client.post(
                "/tasks/nonactive-task/input",
                headers=auth_headers(),
                json={"input": {"content": "unsafe"}},
            )
            cancelled = client.post(
                "/tasks/nonactive-task/cancel",
                headers=auth_headers(),
            )
            messages = client.get(
                "/tasks/nonactive-task/messages",
                headers=auth_headers(),
            )
            events = client.get(
                "/tasks/nonactive-task/events?run_count=0",
                headers=auth_headers(),
            )
            decision = client.post(
                "/tasks/nonactive-task/reviews/review-1/decision",
                headers=auth_headers(),
                json={"decisions": [{"type": "approve"}]},
            )
            task_reviews = client.get(
                "/tasks/nonactive-task/reviews",
                headers=auth_headers(),
            )
            delivered = client.post(
                "/webhooks/tasks",
                headers=auth_headers(),
                json=webhook,
            )

        assert queried.status_code == listed.status_code == 200
        assert queried.json()["status"] == (
            "failed" if route_state == "failed" else "interrupted"
        )
        assert listed.json()["items"][0]["task_id"] == "nonactive-task"
        assert {sent.status_code, cancelled.status_code, messages.status_code} == {409}
        assert events.status_code == decision.status_code == 409
        assert task_reviews.status_code == 200
        assert task_reviews.json()["items"] == []
        assert delivered.status_code == 202
        assert delivered.json() == {"delivered": 0}
        assert remote.created_inputs == []
        assert remote.sent_inputs == []
        assert remote.cancelled == []
    finally:
        routes.close()


def test_public_remote_ref_forwards_attachments_to_remote_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    a2a_client = StaticRemoteA2AClient()
    app, _ = build_app(
        monkeypatch,
        a2a_client=a2a_client,  # type: ignore[arg-type]
        node_id="node-a",
    )
    encoded = base64.b64encode(b"remote file").decode("ascii")

    with TestClient(app) as client:
        response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={
                "input": {
                    "content": "explain file",
                    "attachments": [
                        {
                            "name": "remote.txt",
                            "content_type": "text/plain",
                            "kind": "document",
                            "data_base64": encoded,
                        }
                    ],
                },
                "metadata": {"channel": "tg"},
            },
        )

    assert response.status_code == 201
    assert a2a_client.created_inputs == ["explain file"]
    assert a2a_client.created_attachments == [
        [
            {
                "name": "remote.txt",
                "content_type": "text/plain",
                "kind": "document",
                "data_base64": encoded,
            }
        ]
    ]


def test_remote_ref_review_is_exposed_and_forwarded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    a2a_client = ReviewRemoteA2AClient()
    app, _ = build_app(
        monkeypatch,
        a2a_client=a2a_client,  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "needs approval"}},
        )
        assert create_response.status_code == 201, create_response.json()
        create_payload = create_response.json()
        proxy_task_id = create_payload["task_id"]
        assert create_payload["status"] == "waiting_for_human"
        assert create_payload["pending_review"]["review_id"] == "remote-review-1"
        assert create_payload["pending_review"]["source_task_id"] == proxy_task_id

        task_response = client.get(f"/tasks/{proxy_task_id}", headers=auth_headers())
        assert task_response.status_code == 200, task_response.json()
        assert task_response.json()["pending_review"]["source_task_id"] == proxy_task_id

        task_list_response = client.get("/tasks", headers=auth_headers())
        assert task_list_response.status_code == 200, task_list_response.json()
        listed_task = next(
            item
            for item in task_list_response.json()["items"]
            if item["task_id"] == proxy_task_id
        )
        assert listed_task["pending_review"]["source_task_id"] == proxy_task_id

        reviews_response = client.get("/reviews", headers=auth_headers())
        assert reviews_response.status_code == 200, reviews_response.json()
        reviews_payload = reviews_response.json()
        assert reviews_payload["items"][0]["review_id"] == "remote-review-1"
        assert reviews_payload["items"][0]["task_id"] == proxy_task_id
        assert reviews_payload["items"][0]["thread_id"] == proxy_task_id
        assert reviews_payload["items"][0]["route_kind"] == "remote_ref"

        review_response = client.get(
            "/reviews/remote-review-1",
            headers=auth_headers(),
        )
        assert review_response.status_code == 200, review_response.json()
        assert review_response.json()["task_id"] == proxy_task_id
        assert review_response.json()["thread_id"] == proxy_task_id

        task_reviews_response = client.get(
            f"/tasks/{proxy_task_id}/reviews",
            headers=auth_headers(),
        )
        assert task_reviews_response.status_code == 200
        assert (
            task_reviews_response.json()["items"][0]["review_id"] == "remote-review-1"
        )
        assert task_reviews_response.json()["items"][0]["thread_id"] == proxy_task_id
        assert "upstream-review-task" not in json.dumps(
            {
                "created": create_payload,
                "task": task_response.json(),
                "tasks": task_list_response.json(),
                "reviews": reviews_payload,
                "review": review_response.json(),
                "task_reviews": task_reviews_response.json(),
            }
        )

        submit_response = client.post(
            f"/tasks/{proxy_task_id}/reviews/remote-review-1/decision",
            headers=auth_headers(),
            json={"decisions": [{"type": "approve"}]},
        )
        assert submit_response.status_code == 202, submit_response.json()
        submit_payload = submit_response.json()

    assert submit_payload["status"] == "completed"
    assert submit_payload["last_result"] == "remote review resumed"
    assert submit_payload["pending_review"] is None
    assert a2a_client.submitted_reviews == [
        {
            "task_id": "upstream-review-task",
            "review_id": "remote-review-1",
            "decisions": [{"type": "approve"}],
        }
    ]


def test_review_submit_rejects_review_not_owned_by_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    a2a_client = ReviewRemoteA2AClient()
    app, _factory = build_app(
        monkeypatch,
        a2a_client=a2a_client,  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "needs approval"}},
        )
        assert create_response.status_code == 201, create_response.json()
        proxy_task_id = create_response.json()["task_id"]

        submit_response = client.post(
            f"/tasks/{proxy_task_id}/reviews/wrong-review/decision",
            headers=auth_headers(),
            json={"decisions": [{"type": "approve"}]},
        )

    assert submit_response.status_code == 404
    assert submit_response.json()["error"]["code"] == "review_not_found"
    assert a2a_client.submitted_reviews == []


def test_review_submit_accepts_root_task_mirrored_review(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    interrupt_factory = ReviewInterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", interrupt_factory)
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )
    route_store = GatewayRouteStore(":memory:")
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
        route_store=route_store,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")

    child_review_id: str | None = None
    root_task_id: str | None = None

    async def seed_review() -> None:
        nonlocal child_review_id, root_task_id
        root = await control.spawn_task("background_research", "root task")
        await wait_for_task_state(control, root.task_id, states={"completed", "failed"})
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        await wait_for_task_state(control, child.task_id, states={"waiting_for_human"})
        root_task_id = root.task_id
        child_review_id = control.get_task_record(child.task_id).pending_review[
            "review_id"
        ]

    asyncio.run(seed_review())

    assert root_task_id is not None
    assert child_review_id is not None
    asyncio.run(
        route_store.asave_route(
            TaskRouteRecord(
                task_id=root_task_id,
                agent_name="background_research",
                metadata={},
                route_kind="local",
                upstream_task_id=root_task_id,
            )
        )
    )

    with TestClient(app) as client:
        submit_response = client.post(
            f"/tasks/{root_task_id}/reviews/{child_review_id}/decision",
            headers=auth_headers(),
            json={"decisions": [{"type": "approve"}]},
        )

    assert submit_response.status_code == 202, submit_response.json()
    payload = submit_response.json()
    assert payload["task_id"] == root_task_id
    assert payload["pending_review"] is None


def test_root_task_review_api_enumerates_and_decides_sibling_reviews(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    interrupt_factory = ReviewInterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", interrupt_factory)
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )
    route_store = GatewayRouteStore(":memory:")
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
        route_store=route_store,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")

    async def seed_reviews() -> tuple[str, list[str]]:
        root = await control.spawn_task("background_research", "root task")
        await wait_for_task_state(control, root.task_id, states={"completed", "failed"})
        review_ids = []
        for _index in range(2):
            child = await control.spawn_task(
                "background_research",
                "needs review",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
            await wait_for_task_state(
                control, child.task_id, states={"waiting_for_human"}
            )
            review_ids.append(child.pending_review["review_id"])
        await route_store.asave_route(
            TaskRouteRecord(
                task_id=root.task_id,
                agent_name="background_research",
                metadata={"channel": "test"},
                route_kind="local",
                upstream_task_id=root.task_id,
            )
        )
        return root.task_id, review_ids

    root_task_id, review_ids = asyncio.run(seed_reviews())

    with TestClient(app) as client:
        listed = client.get("/reviews", headers=auth_headers())
        assert listed.status_code == 200, listed.json()
        assert {item["review_id"] for item in listed.json()["items"]} == set(review_ids)

        root_reviews = client.get(
            f"/tasks/{root_task_id}/reviews",
            headers=auth_headers(),
        )
        assert root_reviews.status_code == 200, root_reviews.json()
        assert {item["review_id"] for item in root_reviews.json()["items"]} == set(
            review_ids
        )

        fetched = client.get(
            f"/reviews/{review_ids[1]}",
            headers=auth_headers(),
        )
        assert fetched.status_code == 200, fetched.json()
        assert fetched.json()["review_id"] == review_ids[1]

        decided_second = client.post(
            f"/tasks/{root_task_id}/reviews/{review_ids[1]}/decision",
            headers=auth_headers(),
            json={"decisions": [{"type": "approve"}]},
        )
        assert decided_second.status_code == 202, decided_second.json()
        assert decided_second.json()["pending_review"]["review_id"] == review_ids[0]

        remaining = client.get(
            f"/tasks/{root_task_id}/reviews",
            headers=auth_headers(),
        )
        assert [item["review_id"] for item in remaining.json()["items"]] == [
            review_ids[0]
        ]

        decided_first = client.post(
            f"/tasks/{root_task_id}/reviews/{review_ids[0]}/decision",
            headers=auth_headers(),
            json={"decisions": [{"type": "approve"}]},
        )
        assert decided_first.status_code == 202, decided_first.json()
        assert decided_first.json()["pending_review"] is None


def test_remote_a_to_b_to_a_loop_is_rejected_by_visited_nodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOP_GATEWAY_TOKEN", "secret-token")
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        RemoteBackDelegatingAgentFactory(),
    )
    a_to_b_ref = RemoteRef(
        name="remote_code_wiki",
        description="remote helper",
        url="https://node-b.example/a2a",
        remote_agent_name="code_wiki",
        auth={"type": "bearer", "token_env": "LOOP_GATEWAY_TOKEN"},
    )
    b_to_a_ref = RemoteRef(
        name="back_to_a",
        description="back to node a",
        url="https://node-a.example/a2a",
        remote_agent_name="main",
        auth={"type": "bearer", "token_env": "LOOP_GATEWAY_TOKEN"},
    )
    transports_a: dict[str, httpx.AsyncBaseTransport] = {}
    transports_b: dict[str, httpx.AsyncBaseTransport] = {}

    a_control = AgentControl(
        {
            "main": LocalWorkerSpec(
                name="main",
                description="node a main",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=[],
                skills=[],
            )
        },
        {"remote_code_wiki": a_to_b_ref},
        checkpointer=object(),
        backend=object(),
        a2a_client=A2AClient(transports=transports_a),
        node_id="node-a",
    )
    a_service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs={
            "main": build_local_agent_config("main", "node a main"),
            "remote_code_wiki": {
                "kind": "remote_ref",
                "public": True,
                "name": "remote_code_wiki",
                "description": "remote helper",
                "url": "https://node-b.example/a2a",
                "remote_agent_name": "code_wiki",
            },
        },
        control=a_control,
    )
    a_app = create_gateway_app(service=a_service, bearer_token="secret-token")
    a_root_app = FastAPI()
    a_root_app.mount("/a2a", a_app)

    b_spec = LocalWorkerSpec(
        name="code_wiki",
        description="node b code wiki",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_targets=("back_to_a",),
    )
    b_control = AgentControl(
        {"code_wiki": b_spec},
        {"back_to_a": b_to_a_ref},
        checkpointer=object(),
        backend=object(),
        a2a_client=A2AClient(transports=transports_b),
        node_id="node-b",
    )
    b_service = GatewayTaskModule(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": build_local_agent_config(
                "code_wiki",
                "node b code wiki",
            )
        },
        control=b_control,
    )
    b_app = create_gateway_app(service=b_service, bearer_token="secret-token")
    b_root_app = FastAPI()
    b_root_app.mount("/a2a", b_app)
    transports_a["https://node-b.example/a2a"] = httpx.ASGITransport(app=b_root_app)
    transports_b["https://node-a.example/a2a"] = httpx.ASGITransport(app=a_root_app)

    with TestClient(a_app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "start loop"}},
        )
        assert create_response.status_code == 201, create_response.json()
        proxy_task_id = create_response.json()["task_id"]

        time.sleep(0.08)
        get_response = client.get(f"/tasks/{proxy_task_id}", headers=auth_headers())
        assert get_response.status_code == 200
        payload = get_response.json()

    assert payload["status"] == "completed"
    assert "already appears in route" in payload["last_result"]


def test_public_remote_ref_not_registered_in_runtime_returns_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = DelayedAgentFactory(delay=0.03)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = AgentControl(
        build_specs(),
        {},
        checkpointer=object(),
        backend=object(),
    )
    a2a_client = StaticRemoteA2AClient()
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")

    with TestClient(app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "explain repo"}, "metadata": {"channel": "tg"}},
        )
        assert create_response.status_code == 503
        assert create_response.json()["error"]["code"] == "runtime_unavailable"

    assert a2a_client.created_inputs == []
    assert a2a_client.sent_inputs == []
    assert a2a_client.cancelled == []


def test_remote_route_persists_across_service_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    remote_factory = DelayedAgentFactory(delay=0.03)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", remote_factory)

    remote_control = AgentControl(
        {
            "code_wiki": LocalWorkerSpec(
                name="code_wiki",
                description="remote code wiki",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=["/sandbox/home/AGENTS.md"],
                skills=["frontend-skill"],
            )
        },
        {},
        checkpointer=object(),
        backend=object(),
    )
    remote_service = GatewayTaskModule(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": build_local_agent_config(
                "code_wiki",
                "remote code wiki",
            )
        },
        control=remote_control,
    )
    remote_app = create_gateway_app(
        service=remote_service, bearer_token="remote-secret"
    )
    remote_root_app = FastAPI()
    remote_root_app.mount("/a2a", remote_app)
    transport = httpx.ASGITransport(app=remote_root_app)
    route_db = tmp_path / "gateway-routes.sqlite"

    first_store = GatewayRouteStore(str(route_db))
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(transports={"https://example.com/a2a": transport}),
        route_store=first_store,
    )
    with TestClient(first_app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "persist me"}, "metadata": {"channel": "tg"}},
        )
        assert create_response.status_code == 201
        proxy_task_id = create_response.json()["task_id"]
    first_store.close()

    second_store = GatewayRouteStore(str(route_db))
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=A2AClient(transports={"https://example.com/a2a": transport}),
        route_store=second_store,
    )
    with TestClient(second_app) as client:
        time.sleep(0.08)
        get_response = client.get(f"/tasks/{proxy_task_id}", headers=auth_headers())
        assert get_response.status_code == 200
        assert get_response.json()["task_id"] == proxy_task_id
        assert get_response.json()["agent_name"] == "remote_code_wiki"
    second_store.close()


def test_remote_route_webhook_persists_across_service_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[dict[str, object]] = []

    class RunningRemoteA2AClient(StaticRemoteA2AClient):
        async def create_task(self, *args, **kwargs):
            payload = await super().create_task(*args, **kwargs)
            payload["status"] = "running"
            payload["last_result"] = None
            return payload

    class CapturingAsyncClient:
        def __init__(self, *, timeout: float) -> None:
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url, *, headers, json):
            calls.append(
                {
                    "url": url,
                    "headers": headers,
                    "json": json,
                }
            )

    monkeypatch.setattr(
        httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )
    route_db = tmp_path / "gateway-routes.sqlite"
    first_store = GatewayRouteStore(str(route_db))
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=RunningRemoteA2AClient(),  # type: ignore[arg-type]
        route_store=first_store,
    )
    with TestClient(first_app) as client:
        create_response = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "persist webhook"},
                "metadata": {"channel": "tg"},
                "webhook": {
                    "url": "https://client.example/hooks",
                    "token": "client-secret",
                },
            },
        )
        assert create_response.status_code == 201
        proxy_task_id = create_response.json()["task_id"]
    first_store.close()

    second_store = GatewayRouteStore(str(route_db))
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=StaticRemoteA2AClient(),  # type: ignore[arg-type]
        route_store=second_store,
    )
    with TestClient(second_app) as client:
        webhook_response = client.post(
            "/webhooks/tasks",
            headers=auth_headers(),
            json={
                "event_id": "evt-1",
                "event_type": "task.completed",
                "task_id": "upstream-1",
                "agent_name": "remote_code_wiki",
                "status": "completed",
                "last_result": "remote done after restart",
                "error": None,
                "run_count": 1,
                "created_at": "2026-04-23T00:00:00Z",
                "updated_at": "2026-04-23T00:00:01Z",
            },
        )
        assert webhook_response.status_code == 202
        assert webhook_response.json()["delivered"] == 1
    second_store.close()

    assert len(calls) == 1
    assert calls[0]["url"] == "https://client.example/hooks"
    assert calls[0]["headers"] == {
        "Content-Type": "application/json",
        "Authorization": "Bearer client-secret",
    }
    payload = calls[0]["json"]
    assert isinstance(payload, dict)
    assert payload["task_id"] == proxy_task_id
    assert payload["agent_name"] == "remote_code_wiki"
    assert payload["status"] == "completed"
    assert payload["last_result"] == "remote done after restart"


def test_gateway_route_store_async_methods_handle_concurrent_access(
    tmp_path: Path,
) -> None:
    route_db = tmp_path / "gateway-routes.sqlite"
    store = GatewayRouteStore(str(route_db))

    async def write_and_read(route_number: int) -> None:
        route = TaskRouteRecord(
            task_id=f"task-{route_number}",
            agent_name="code_wiki",
            metadata={"route": route_number},
            route_kind="local",
            upstream_task_id=f"upstream-{route_number}",
        )
        await store.asave_route(route)
        loaded = await store.aget_route(route.task_id)
        assert loaded == route
        loaded_by_upstream = await store.aget_route_by_upstream_task_id(
            route.upstream_task_id
        )
        assert loaded_by_upstream == route

    async def run_concurrent_access() -> None:
        await asyncio.gather(*(write_and_read(index) for index in range(25)))
        routes = await store.alist_routes()
        assert len(routes) == 25

    try:
        asyncio.run(run_concurrent_access())
    finally:
        store.close()
