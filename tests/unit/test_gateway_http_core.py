from __future__ import annotations

import asyncio
import base64
import sqlite3
import time
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
import pytest

from ruyi_agent.task_models import TaskRecord
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.config.loader import LocalWorkerSpec
from ruyi_agent.runtime.delegation.context import (
    CONTEXT_VERSION,
    CONTEXT_VERSION_FIELD,
    DEPTH_FIELD,
    MAX_DEPTH_FIELD,
    MAX_TASKS_PER_ROOT_FIELD,
    ROOT_ID_FIELD,
    VISITED_NODES_FIELD,
)
from ruyi_agent.gateway_protocol.dto import TaskResponse
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from tests.unit.gateway_http_support import (
    DelayedAgentFactory,
    DelegatingAgentFactory,
    FailOnceGatewayCommandStore,
    MemoryBackend,
    auth_headers,
    build_agent_configs,
    build_app,
    build_specs,
    build_test_remote_refs,
)
from tests.support.async_subagent_runtime import wait_for_task_state


def test_gateway_exposes_subagent_task_created_by_public_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        DelegatingAgentFactory(),
    )
    child_spec = build_specs()["background_research"]
    parent_spec = LocalWorkerSpec(
        name="main",
        description="main entry agent",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_targets=("background_research",),
    )
    control = AgentControl(
        {
            "main": parent_spec,
            "background_research": child_spec,
        },
        {},
        checkpointer=object(),
        backend=object(),
    )
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
    )

    async def scenario() -> tuple[
        list[TaskRecord],
        TaskResponse,
        TaskResponse,
        list[TaskResponse],
    ]:
        response = await service.create_task(
            agent_name="main",
            input_content="gateway task",
            metadata={"channel_user": "alice"},
        )
        parent = control.get_task_record(response.task_id)
        # Route persistence is now executed via asyncio.to_thread to avoid blocking
        # the FastAPI event loop. That yields control and allows very fast tasks to
        # complete before this assertion runs.
        await wait_for_task_state(control, parent.task_id, states={"completed"})
        child_records = [
            record
            for record in control.list_persisted_task_records()
            if record.agent_name == "background_research"
        ]
        for record in child_records:
            await wait_for_task_state(control, record.task_id, states={"completed"})
        child_response = await service.get_task(child_records[0].task_id)
        continued_child = await service.send_input(
            child_records[0].task_id,
            "follow-up",
        )
        continued_record = control.get_task_record(child_records[0].task_id)
        await wait_for_task_state(
            control, continued_record.task_id, states={"completed"}
        )
        task_tree = await service.list_tasks(
            agent_name=None,
            status=None,
            metadata_filters={},
            cursor=None,
            limit=10,
            root_task_id=response.task_id,
        )
        return child_records, child_response, continued_child, task_tree.items

    child_records, child_response, continued_child, task_tree = asyncio.run(scenario())

    assert len(child_records) == 1
    assert child_records[0].parent_task_id is not None
    assert child_records[0].depth == 2
    assert child_response.task_id == child_records[0].task_id
    assert child_response.parent_task_id == child_records[0].parent_task_id
    assert child_response.root_task_id == child_records[0].root_task_id
    assert child_response.depth == 2
    assert child_response.metadata == {"channel_user": "alice"}
    assert continued_child.run_count == 2
    assert {task.task_id for task in task_tree} == {
        child_records[0].root_task_id,
        child_records[0].task_id,
    }


def test_gateway_requires_bearer_token(monkeypatch: pytest.MonkeyPatch) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/agents")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


def test_team_console_requires_a_browser_session_for_shell_and_assets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        page = client.get("/debug/team", follow_redirects=False)
        styles = client.get("/debug/team/app.css")
        script = client.get("/debug/team/app.js")

    assert page.status_code == 303
    assert page.headers["location"] == "/debug/team/login"
    assert page.headers["cache-control"] == "no-store"
    assert styles.status_code == 401
    assert styles.headers["cache-control"] == "no-store"
    assert styles.headers["www-authenticate"].startswith("Bearer ")
    assert script.status_code == 401
    assert script.headers["cache-control"] == "no-store"


def test_team_console_login_issues_protected_session_and_removes_stored_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    browser_headers = {
        "Origin": "http://127.0.0.1",
        "Sec-Fetch-Site": "same-origin",
    }
    with TestClient(app, base_url="http://127.0.0.1") as client:
        login_page = client.get("/debug/team/login")
        invalid = client.post(
            "/debug/team/login",
            data={"token": "do-not-reflect-this"},
            headers=browser_headers,
            follow_redirects=False,
        )
        logged_in = client.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            headers=browser_headers,
            follow_redirects=False,
        )
        page = client.get("/debug/team")
        styles = client.get("/debug/team/app.css")
        script = client.get("/debug/team/app.js")
        console_headers = {
            "X-Ruyi-Team-Console": "1",
            "Sec-Fetch-Site": "same-origin",
            "Referer": "http://127.0.0.1/debug/team",
        }
        agents = client.get("/agents", headers=console_headers)
        error = client.get("/agents/does-not-exist", headers=console_headers)
        invalid_payload = client.post(
            "/agents/main/tasks",
            json={},
            headers=console_headers,
        )
        missing_marker = client.get(
            "/agents",
            headers={"Sec-Fetch-Site": "same-origin"},
        )
        invalid_bearer = client.get(
            "/agents",
            headers={**console_headers, "Authorization": "Bearer wrong"},
        )

    assert login_page.status_code == 200
    assert login_page.headers["cache-control"] == "no-store"
    assert "form-action 'self'" in login_page.headers["content-security-policy"]
    assert "secret-token" not in login_page.text
    assert invalid.status_code == 401
    assert "do-not-reflect-this" not in invalid.text
    assert "set-cookie" not in invalid.headers
    assert logged_in.status_code == 303
    assert logged_in.headers["location"] == "/debug/team"
    set_cookie = logged_in.headers["set-cookie"]
    assert "ruyi_team_console_session=" in set_cookie
    assert "HttpOnly" in set_cookie
    assert "Max-Age=28800" in set_cookie
    assert "Path=/" in set_cookie
    assert "SameSite=strict" in set_cookie
    assert "Secure" not in set_cookie
    assert page.status_code == 200
    assert "RUYI / ARCHITECTURE DESK" in page.text
    assert page.headers["cache-control"] == "no-store"
    assert "script-src 'self'" in page.headers["content-security-policy"]
    assert styles.status_code == 200
    assert styles.headers["cache-control"] == "no-store"
    assert "fonts.googleapis.com" not in styles.text
    assert script.status_code == 200
    assert 'localStorage.removeItem("ruyi.gatewayToken")' in script.text
    assert '"X-Ruyi-Team-Console": "1"' in script.text
    assert agents.status_code == 200
    assert agents.headers["cache-control"] == "no-store"
    assert error.status_code == 404
    assert error.headers["cache-control"] == "no-store"
    assert invalid_payload.status_code == 422
    assert invalid_payload.headers["cache-control"] == "no-store"
    assert missing_marker.status_code == 401
    assert invalid_bearer.status_code == 401


def test_team_console_logout_clears_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    login_headers = {
        "Origin": "http://localhost",
        "Sec-Fetch-Site": "same-origin",
    }
    console_headers = {
        "X-Ruyi-Team-Console": "1",
        "Origin": "http://localhost",
        "Sec-Fetch-Site": "same-origin",
    }
    with TestClient(app, base_url="http://localhost") as client:
        client.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            headers=login_headers,
        )
        logout = client.post(
            "/debug/team/logout",
            headers=console_headers,
            follow_redirects=False,
        )
        page = client.get("/debug/team", follow_redirects=False)

    assert logout.status_code == 303
    assert logout.headers["location"] == "/debug/team/login"
    assert "Max-Age=0" in logout.headers["set-cookie"]
    assert "HttpOnly" in logout.headers["set-cookie"]
    assert page.status_code == 303


def test_team_console_login_rejects_cross_site_query_and_oversized_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app, base_url="http://127.0.0.1") as client:
        missing_source = client.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            follow_redirects=False,
        )
        cross_site = client.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            headers={
                "Origin": "https://attacker.example",
                "Sec-Fetch-Site": "cross-site",
            },
            follow_redirects=False,
        )
        query_token = client.get(
            "/debug/team/login?token=secret-token",
            follow_redirects=False,
        )
        oversized = client.post(
            "/debug/team/login",
            content=b"token=" + (b"x" * 9_000),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "http://127.0.0.1",
                "Sec-Fetch-Site": "same-origin",
            },
            follow_redirects=False,
        )
        wrong_media_type = client.post(
            "/debug/team/login",
            json={"token": "secret-token"},
            headers={
                "Origin": "http://127.0.0.1",
                "Sec-Fetch-Site": "same-origin",
            },
            follow_redirects=False,
        )
        malformed_encoding = client.post(
            "/debug/team/login",
            content="token=%ZZ",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "http://127.0.0.1",
                "Sec-Fetch-Site": "same-origin",
            },
            follow_redirects=False,
        )
        duplicate_token = client.post(
            "/debug/team/login",
            content="token=secret-token&token=secret-token",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "http://127.0.0.1",
                "Sec-Fetch-Site": "same-origin",
            },
            follow_redirects=False,
        )

    assert missing_source.status_code == 403
    assert cross_site.status_code == 403
    assert query_token.status_code == 400
    assert "secret-token" not in query_token.text
    assert oversized.status_code == 413
    assert wrong_media_type.status_code == 415
    assert malformed_encoding.status_code == 400
    assert duplicate_token.status_code == 400


def test_team_console_cookie_security_uses_asgi_scheme_not_forwarded_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app, base_url="http://gateway.example") as plaintext:
        rejected = plaintext.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            headers={
                "Origin": "http://gateway.example",
                "Sec-Fetch-Site": "same-origin",
                "X-Forwarded-Proto": "https",
            },
            follow_redirects=False,
        )
    with TestClient(app, base_url="https://gateway.example") as tls_client:
        accepted = tls_client.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            headers={
                "Origin": "https://gateway.example",
                "Sec-Fetch-Site": "same-origin",
            },
            follow_redirects=False,
        )

    assert rejected.status_code == 400
    assert "set-cookie" not in rejected.headers
    assert accepted.status_code == 303
    assert "Secure" in accepted.headers["set-cookie"]


def test_team_console_cookie_authenticated_unexpected_error_is_not_cached() -> None:
    class ExplodingService:
        def list_agents(self) -> None:
            raise RuntimeError("sensitive internal failure")

    app = create_gateway_app(
        service=ExplodingService(),  # type: ignore[arg-type]
        bearer_token="secret-token",
    )
    browser_headers = {
        "Origin": "http://127.0.0.1",
        "Sec-Fetch-Site": "same-origin",
    }
    with TestClient(
        app,
        base_url="http://127.0.0.1",
        raise_server_exceptions=False,
    ) as client:
        client.post(
            "/debug/team/login",
            data={"token": "secret-token"},
            headers=browser_headers,
        )
        response = client.get(
            "/agents",
            headers={
                "X-Ruyi-Team-Console": "1",
                "Referer": "http://127.0.0.1/debug/team",
                "Sec-Fetch-Site": "same-origin",
            },
        )

    assert response.status_code == 500
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {
        "error": {
            "code": "internal_error",
            "message": "Internal gateway error",
        }
    }
    assert "sensitive internal failure" not in response.text


def test_get_agents_returns_public_targets_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/agents", headers=auth_headers())

    assert response.status_code == 200
    names = [item["name"] for item in response.json()["items"]]
    assert names == ["main", "remote_code_wiki"]
    default_agent = next(
        item for item in response.json()["items"] if item["name"] == "main"
    )
    assert default_agent["is_default"] is True


def test_unavailable_agent_does_not_block_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(
        monkeypatch,
        unavailable_agents={"main": "missing provider credential"},
    )
    with TestClient(app) as client:
        listed = client.get("/agents", headers=auth_headers())
        created = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "hello"}},
        )

    main = next(item for item in listed.json()["items"] if item["name"] == "main")
    assert main["available"] is False
    assert main["unavailable_reason"] == "missing provider credential"
    assert created.status_code == 503
    assert created.json()["error"]["code"] == "agent_unavailable"


def test_get_private_agent_returns_documented_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get(
            "/agents/background_research",
            headers=auth_headers(),
        )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "agent_not_public"


def test_create_task_returns_201_and_running_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, factory = build_app(monkeypatch, delay=0.1)
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "research react"},
                "metadata": {"channel": "tg"},
            },
        )

    assert response.status_code == 201
    assert response.headers["location"].startswith("/tasks/")
    payload = response.json()
    assert payload["status"] == "running"
    assert payload["run_count"] == 1
    assert payload["metadata"] == {"channel": "tg"}
    assert len(factory.created) == 1


def test_create_task_idempotency_replays_same_response(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    command_store = GatewayCommandStore(str(tmp_path / "commands.sqlite"))
    app, factory = build_app(
        monkeypatch,
        delay=0.1,
        command_store=command_store,
    )
    headers = {**auth_headers(), "Idempotency-Key": "create-request-1"}
    body = {
        "input": {"content": "research react"},
        "metadata": {"channel": "tg"},
    }
    try:
        with TestClient(app) as client:
            first = client.post("/agents/main/tasks", headers=headers, json=body)
            replay = client.post("/agents/main/tasks", headers=headers, json=body)

        assert first.status_code == 201
        assert replay.status_code == 201
        assert replay.json() == first.json()
        assert replay.headers["idempotency-replayed"] == "true"
        assert replay.headers["location"] == first.headers["location"]
        assert "idempotency-replayed" not in first.headers
        assert command_store.count_commands() == 1
        assert len(factory.created) == 1
    finally:
        command_store.close()


def test_create_replay_does_not_depend_on_current_agent_availability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = DelayedAgentFactory(delay=0.1)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=MemoryBackend(),
        workspace_root="/workspace",
    )
    command_store = GatewayCommandStore(str(tmp_path / "commands.sqlite"))
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
        command_store=command_store,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    headers = {**auth_headers(), "Idempotency-Key": "availability-change"}
    body = {"input": {"content": "hello"}, "metadata": {}}
    try:
        with TestClient(app) as client:
            first = client.post("/agents/main/tasks", headers=headers, json=body)
            service._unavailable_agents["main"] = "maintenance"  # noqa: SLF001
            replay = client.post("/agents/main/tasks", headers=headers, json=body)

        assert first.status_code == 201
        assert replay.status_code == 201
        assert replay.json() == first.json()
        assert replay.headers["idempotency-replayed"] == "true"
    finally:
        command_store.close()


def test_create_idempotency_hash_ignores_json_object_key_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.1)
    headers = {**auth_headers(), "Idempotency-Key": "canonical-json"}
    with TestClient(app) as client:
        first = client.post(
            "/agents/main/tasks",
            headers=headers,
            json={
                "input": {"content": "hello"},
                "metadata": {"source": "test", "sequence": 1},
            },
        )
        replay = client.post(
            "/agents/main/tasks",
            headers=headers,
            json={
                "metadata": {"sequence": 1, "source": "test"},
                "input": {"content": "hello"},
            },
        )

    assert first.status_code == 201
    assert replay.status_code == 201
    assert replay.json() == first.json()
    assert replay.headers["idempotency-replayed"] == "true"


def test_create_without_idempotency_key_preserves_non_idempotent_behavior(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.1)
    body = {"input": {"content": "hello"}, "metadata": {}}
    with TestClient(app) as client:
        first = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json=body,
        )
        second = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json=body,
        )

    assert first.status_code == 201
    assert second.status_code == 201
    assert first.json()["task_id"] != second.json()["task_id"]


def test_idempotency_key_reuse_with_different_request_returns_409(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.1)
    headers = {**auth_headers(), "Idempotency-Key": "globally-unique-key"}
    with TestClient(app) as client:
        first = client.post(
            "/agents/main/tasks",
            headers=headers,
            json={"input": {"content": "first"}, "metadata": {}},
        )
        different_body = client.post(
            "/agents/main/tasks",
            headers=headers,
            json={"input": {"content": "second"}, "metadata": {}},
        )
        different_operation = client.post(
            f"/tasks/{first.json()['task_id']}/input",
            headers=headers,
            json={"input": {"content": "first"}},
        )
        nonexistent_target = client.post(
            "/tasks/does-not-exist/input",
            headers=headers,
            json={"input": {"content": "first"}},
        )

    assert first.status_code == 201
    assert different_body.status_code == 409
    assert different_body.json()["error"]["code"] == "idempotency_key_reused"
    assert different_operation.status_code == 409
    assert different_operation.json()["error"]["code"] == "idempotency_key_reused"
    assert nonexistent_target.status_code == 409
    assert nonexistent_target.json()["error"]["code"] == "idempotency_key_reused"


def test_invalid_idempotency_key_returns_400(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers={**auth_headers(), "Idempotency-Key": "contains whitespace"},
            json={"input": {"content": "hello"}, "metadata": {}},
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"


def test_idempotent_input_route_failure_releases_command_for_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    headers = {**auth_headers(), "Idempotency-Key": "missing-task-input"}
    with TestClient(app) as client:
        first = client.post(
            "/tasks/does-not-exist/input",
            headers=headers,
            json={"input": {"content": "hello"}},
        )
        retry = client.post(
            "/tasks/does-not-exist/input",
            headers=headers,
            json={"input": {"content": "hello"}},
        )

    assert first.status_code == 404
    assert first.json()["error"]["code"] == "task_not_found"
    assert retry.status_code == 404
    assert retry.json()["error"]["code"] == "task_not_found"


def test_concurrent_create_requests_share_one_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, factory = build_app(monkeypatch, delay=0.1)
    headers = {**auth_headers(), "Idempotency-Key": "concurrent-create"}
    body = {"input": {"content": "hello"}, "metadata": {}}

    async def scenario() -> list[httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gateway.test",
        ) as client:
            return await asyncio.gather(
                *[
                    client.post("/agents/main/tasks", headers=headers, json=body)
                    for _ in range(100)
                ]
            )

    responses = asyncio.run(scenario())

    assert {response.status_code for response in responses} == {201}
    assert len({response.json()["task_id"] for response in responses}) == 1
    assert (
        sum("idempotency-replayed" in response.headers for response in responses) == 99
    )
    assert len(factory.created) == 1


def test_create_retry_recovers_after_effect_before_command_completion(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    command_store = FailOnceGatewayCommandStore(str(tmp_path / "commands.sqlite"))
    app, factory = build_app(
        monkeypatch,
        delay=0.1,
        command_store=command_store,
    )
    headers = {**auth_headers(), "Idempotency-Key": "create-after-crash"}
    body = {"input": {"content": "hello"}, "metadata": {}}
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            failed = client.post("/agents/main/tasks", headers=headers, json=body)
            recovered = client.post("/agents/main/tasks", headers=headers, json=body)
            replay = client.post("/agents/main/tasks", headers=headers, json=body)

        assert failed.status_code == 500
        assert recovered.status_code == 201
        assert replay.status_code == 201
        assert replay.json() == recovered.json()
        assert replay.headers["idempotency-replayed"] == "true"
        assert len(factory.created) == 1
        assert command_store.count_commands() == 1
    finally:
        command_store.close()


def test_create_task_uploads_attachments_and_injects_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace")
    app, factory = build_app(
        monkeypatch,
        delay=0.0,
        backend=backend,
        workspace_root="/workspace",
    )
    encoded = base64.b64encode(b"hello file").decode("ascii")
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {
                    "content": "read this",
                    "attachments": [
                        {
                            "name": "../report.txt",
                            "content_type": "text/plain",
                            "kind": "document",
                            "data_base64": encoded,
                        }
                    ],
                },
                "metadata": {"channel": "telegram"},
            },
        )

    assert response.status_code == 201, response.json()
    assert len(backend.files) == 1
    uploaded_path = next(iter(backend.files))
    assert uploaded_path.startswith("/workspace/inbox/gateway/")
    assert uploaded_path.endswith("/01-report.txt")
    assert backend.files[uploaded_path] == b"hello file"
    assert len(factory.created) == 1
    content = factory.created[0].calls[0]["payload"]["messages"][0]["content"]
    assert "Uploaded attachments:" in content
    assert f"path={uploaded_path}" in content
    assert "attachments" in response.json()["metadata"]


def test_create_task_accepts_attachment_without_text_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace")
    app, factory = build_app(
        monkeypatch,
        delay=0.0,
        backend=backend,
        workspace_root="/workspace",
    )
    encoded = base64.b64encode(b"hello file").decode("ascii")
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {
                    "content": "",
                    "attachments": [
                        {
                            "name": "report.txt",
                            "content_type": "text/plain",
                            "kind": "document",
                            "data_base64": encoded,
                        }
                    ],
                }
            },
        )

    assert response.status_code == 201, response.json()
    assert len(backend.files) == 1
    content = factory.created[0].calls[0]["payload"]["messages"][0]["content"]
    assert "Uploaded attachments:" in content
    assert "report.txt" in content


def test_create_task_rejects_empty_input_without_attachments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)

    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": ""}},
        )

    assert response.status_code == 422


def test_artifact_download_reads_from_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace")
    backend.files["/workspace/out/report.txt"] = b"artifact bytes"
    app, _ = build_app(
        monkeypatch,
        backend=backend,
        workspace_root="/workspace",
    )

    with TestClient(app) as client:
        response = client.post(
            "/artifacts/download",
            headers=auth_headers(),
            json={"path": "/workspace/out/report.txt"},
        )

    assert response.status_code == 200
    assert response.content == b"artifact bytes"
    assert response.headers["x-artifact-path"] == "/workspace/out/report.txt"


def test_task_response_includes_published_artifacts_and_downloads_by_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace")
    backend.files["/workspace/out/report.txt"] = b"artifact bytes"
    app, factory = build_app(
        monkeypatch,
        backend=backend,
        workspace_root="/workspace",
    )

    with TestClient(app) as client:
        create_response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "write report"}},
        )
        task_id = create_response.json()["task_id"]
        for _ in range(100):
            if factory.compile_kwargs:
                break
            time.sleep(0.01)
        assert factory.compile_kwargs
        register_artifact = factory.compile_kwargs[0]["register_artifact"]
        assert callable(register_artifact)
        artifact = register_artifact(
            task_id=task_id,
            artifact={
                "path": "/workspace/out/report.txt",
                "name": "report.txt",
                "caption": "Report",
                "content_type": "text/plain",
                "size": len(b"artifact bytes"),
            },
        )
        task_response = client.get(f"/tasks/{task_id}", headers=auth_headers())
        download_response = client.get(
            f"/tasks/{task_id}/artifacts/{artifact['artifact_id']}/download",
            headers=auth_headers(),
        )

    assert task_response.status_code == 200
    assert task_response.json()["artifacts"] == [
        {
            "artifact_id": artifact["artifact_id"],
            "path": "/workspace/out/report.txt",
            "name": "report.txt",
            "caption": "Report",
            "content_type": "text/plain",
            "size": len(b"artifact bytes"),
            "run_count": 1,
        }
    ]
    assert download_response.status_code == 200
    assert download_response.content == b"artifact bytes"
    assert download_response.headers["x-artifact-id"] == artifact["artifact_id"]


def test_task_artifact_download_encodes_non_ascii_filename_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace")
    backend.files["/workspace/out/intro.txt"] = b"artifact bytes"
    app, factory = build_app(
        monkeypatch,
        backend=backend,
        workspace_root="/workspace",
    )

    with TestClient(app) as client:
        create_response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "write report"}},
        )
        task_id = create_response.json()["task_id"]
        for _ in range(100):
            if factory.compile_kwargs:
                break
            time.sleep(0.01)
        assert factory.compile_kwargs
        register_artifact = factory.compile_kwargs[0]["register_artifact"]
        assert callable(register_artifact)
        artifact = register_artifact(
            task_id=task_id,
            artifact={
                "path": "/workspace/out/intro.txt",
                "name": "自我介绍.txt",
                "caption": "Report",
                "content_type": "text/plain",
                "size": len(b"artifact bytes"),
            },
        )
        response = client.get(
            f"/tasks/{task_id}/artifacts/{artifact['artifact_id']}/download",
            headers=auth_headers(),
        )

    assert response.status_code == 200
    assert response.content == b"artifact bytes"
    assert response.headers["content-disposition"] == (
        'attachment; filename="artifact.txt"; '
        "filename*=UTF-8''%E8%87%AA%E6%88%91%E4%BB%8B%E7%BB%8D.txt"
    )


def test_artifact_download_rejects_path_outside_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace")
    backend.files["/etc/passwd"] = b"root:x"
    app, _ = build_app(
        monkeypatch,
        backend=backend,
        workspace_root="/workspace",
    )

    with TestClient(app) as client:
        response = client.post(
            "/artifacts/download",
            headers=auth_headers(),
            json={"path": "/etc/passwd"},
        )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "workspace_path_forbidden"


def test_create_task_rejects_untrusted_workspace_root_for_attachments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(
        monkeypatch,
        backend=MemoryBackend(root="/workspace/.."),
        workspace_root="/workspace/..",
    )
    encoded = base64.b64encode(b"hello").decode("ascii")

    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {
                    "content": "read this",
                    "attachments": [
                        {
                            "name": "report.txt",
                            "content_type": "text/plain",
                            "kind": "document",
                            "data_base64": encoded,
                        }
                    ],
                }
            },
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "runtime_unavailable"


def test_create_task_rejects_incomplete_attachment_upload_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    backend = MemoryBackend(root="/workspace", truncate_upload_results=True)
    app, _ = build_app(
        monkeypatch,
        backend=backend,
        workspace_root="/workspace",
    )
    encoded = base64.b64encode(b"hello").decode("ascii")

    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {
                    "content": "read this",
                    "attachments": [
                        {
                            "name": "report.txt",
                            "content_type": "text/plain",
                            "kind": "document",
                            "data_base64": encoded,
                        }
                    ],
                }
            },
        )

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "attachment_upload_failed"


def test_create_task_accepts_inbound_delegation_context_and_strips_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.1, node_id="node-b")
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "research react"},
                "metadata": {
                    "channel": "tg",
                    CONTEXT_VERSION_FIELD: CONTEXT_VERSION,
                    ROOT_ID_FIELD: "node-a:root-1",
                    DEPTH_FIELD: 2,
                    MAX_DEPTH_FIELD: 3,
                    MAX_TASKS_PER_ROOT_FIELD: 20,
                    VISITED_NODES_FIELD: '["node-a"]',
                },
            },
        )

    assert response.status_code == 201
    payload = response.json()
    assert payload["metadata"] == {"channel": "tg"}
    assert payload["root_task_id"] == "node-a:root-1"
    assert payload["depth"] == 2


def test_create_task_rejects_inbound_delegation_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, node_id="node-b")
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "research react"},
                "metadata": {
                    CONTEXT_VERSION_FIELD: CONTEXT_VERSION,
                    ROOT_ID_FIELD: "node-a:root-1",
                    DEPTH_FIELD: 2,
                    MAX_DEPTH_FIELD: 3,
                    MAX_TASKS_PER_ROOT_FIELD: 20,
                    VISITED_NODES_FIELD: '["node-a","node-b"]',
                },
            },
        )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "delegation_loop_detected"


def test_create_task_rejects_inbound_delegation_depth_exceeded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, node_id="node-b")
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={
                "input": {"content": "research react"},
                "metadata": {
                    CONTEXT_VERSION_FIELD: CONTEXT_VERSION,
                    ROOT_ID_FIELD: "node-a:root-1",
                    DEPTH_FIELD: 4,
                    MAX_DEPTH_FIELD: 3,
                    MAX_TASKS_PER_ROOT_FIELD: 20,
                    VISITED_NODES_FIELD: '["node-a"]',
                },
            },
        )

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "delegation_depth_exceeded"


def test_send_input_and_list_tasks_follow_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.03)
    with TestClient(app) as client:
        create_response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "first"}, "metadata": {"channel": "telegram"}},
        )
        task_id = create_response.json()["task_id"]

        time.sleep(0.08)
        completed_response = client.get(f"/tasks/{task_id}", headers=auth_headers())
        assert completed_response.status_code == 200
        assert completed_response.json()["status"] == "completed"
        assert completed_response.json()["last_result"] == "done: first"

        send_response = client.post(
            f"/tasks/{task_id}/input",
            headers=auth_headers(),
            json={"input": {"content": "second"}},
        )
        assert send_response.status_code == 202
        assert send_response.json()["status"] == "running"
        assert send_response.json()["run_count"] == 2
        assert send_response.json()["last_result"] == "done: first"

        time.sleep(0.08)
        list_response = client.get(
            "/tasks?metadata.channel=telegram&limit=20",
            headers=auth_headers(),
        )
        assert list_response.status_code == 200
        items = list_response.json()["items"]
        assert len(items) == 1
        assert items[0]["task_id"] == task_id
        assert list_response.json()["next_cursor"] is None


def test_idempotent_send_requires_durable_mailbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.03)
    with TestClient(app) as client:
        created = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "first"}, "metadata": {}},
        )
        task_id = created.json()["task_id"]
        time.sleep(0.08)

        headers = {**auth_headers(), "Idempotency-Key": "send-request-1"}
        first = client.post(
            f"/tasks/{task_id}/input",
            headers=headers,
            json={"input": {"content": "second"}},
        )
        retry = client.post(
            f"/tasks/{task_id}/input",
            headers=headers,
            json={"input": {"content": "second"}},
        )
        current = client.get(f"/tasks/{task_id}", headers=auth_headers())

    assert first.status_code == 503
    assert first.json()["error"]["code"] == "idempotency_unavailable"
    assert retry.status_code == 503
    assert retry.json()["error"]["code"] == "idempotency_unavailable"
    assert current.status_code == 200
    assert current.json()["run_count"] == 1


def test_send_retry_after_command_completion_failure_publishes_one_mailbox_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = DelayedAgentFactory(delay=0.03)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    db_path = str(tmp_path / "tasks.sqlite")
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    route_store = GatewayRouteStore(str(tmp_path / "routes.sqlite"))
    command_store = FailOnceGatewayCommandStore(db_path)
    command_store.fail_next_complete = False
    control = AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=MemoryBackend(),
        mailbox=mailbox,
        task_store=task_store,
        workspace_root="/workspace",
    )
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
        route_store=route_store,
        command_store=command_store,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    replay_route_store: GatewayRouteStore | None = None
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            created = client.post(
                "/agents/main/tasks",
                headers=auth_headers(),
                json={"input": {"content": "first"}, "metadata": {}},
            )
            task_id = created.json()["task_id"]
            time.sleep(0.08)
            command_store.fail_next_complete = True
            headers = {**auth_headers(), "Idempotency-Key": "durable-send-1"}

            failed = client.post(
                f"/tasks/{task_id}/input",
                headers=headers,
                json={"input": {"content": "second"}},
            )
            recovered = client.post(
                f"/tasks/{task_id}/input",
                headers=headers,
                json={"input": {"content": "second"}},
            )
            replay = client.post(
                f"/tasks/{task_id}/input",
                headers=headers,
                json={"input": {"content": "second"}},
            )
            claimed = mailbox.claim(
                recipient_task_id=task_id,
                recipient_thread_id=task_id,
            )
            mailbox.acknowledge([message.message_id for message in claimed])
            time.sleep(0.08)

        replay_route_store = GatewayRouteStore(":memory:")
        replay_service = GatewayTaskModule(
            main_agent_name="main",
            agent_configs=build_agent_configs(),
            control=control,
            route_store=replay_route_store,
            command_store=command_store,
        )
        replay_app = create_gateway_app(
            service=replay_service,
            bearer_token="secret-token",
        )
        with TestClient(replay_app) as client:
            replay_without_route = client.post(
                f"/tasks/{task_id}/input",
                headers=headers,
                json={"input": {"content": "second"}},
            )

        assert failed.status_code == 500
        assert recovered.status_code == 202
        assert replay.status_code == 202
        assert replay.json() == recovered.json()
        assert replay.headers["idempotency-replayed"] == "true"
        assert replay_without_route.status_code == 202
        assert replay_without_route.json() == recovered.json()
        assert replay_without_route.headers["idempotency-replayed"] == "true"
        with sqlite3.connect(db_path) as conn:
            mailbox_rows = conn.execute(
                "SELECT message_id, idempotency_key, content "
                "FROM agent_mailbox_messages"
            ).fetchall()
        assert len(mailbox_rows) == 1
        assert str(mailbox_rows[0][1]).startswith("gateway-input:")
        assert mailbox_rows[0][2] == "second"
        assert command_store.count_commands() == 1
    finally:
        time.sleep(0.08)
        if replay_route_store is not None:
            replay_route_store.close()
        command_store.close()
        route_store.close()
        task_store.close()
        mailbox_store.close()


def test_non_public_agent_returns_documented_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        private_response = client.post(
            "/agents/background_research/tasks",
            headers=auth_headers(),
            json={"input": {"content": "x"}},
        )
    assert private_response.status_code == 403
    assert private_response.json()["error"]["code"] == "agent_not_public"


def test_send_input_while_running_returns_409(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.2)
    with TestClient(app) as client:
        create_response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "first"}},
        )
        task_id = create_response.json()["task_id"]

        send_response = client.post(
            f"/tasks/{task_id}/input",
            headers=auth_headers(),
            json={"input": {"content": "second"}},
        )

    assert send_response.status_code == 409
    assert send_response.json()["error"]["code"] == "task_already_running"
