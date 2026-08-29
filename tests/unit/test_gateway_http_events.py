from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from starlette.requests import ClientDisconnect

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.config.loader import LocalWorkerSpec
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.runtime.task_events import TaskStreamEvent
from ruyi_agent.storage.task_store import TaskStore
from tests.unit.gateway_http_support import (
    DelayedAgentFactory,
    MemoryBackend,
    ProbeTaskEventService,
    ProbeTaskEvents,
    _task_event_scope,
    auth_headers,
    build_agent_configs,
    build_app,
    build_local_agent_config,
    build_specs,
    build_test_remote_refs,
    parse_sse_records,
)


def test_task_event_stream_closes_context_when_response_start_fails() -> None:
    async def scenario() -> tuple[ProbeTaskEventService, ProbeTaskEvents]:
        events = ProbeTaskEvents()
        service = ProbeTaskEventService(events)
        app = create_gateway_app(
            service=service,  # type: ignore[arg-type]
            bearer_token="secret-token",
        )

        async def receive() -> dict[str, str]:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def send(message: dict[str, object]) -> None:
            assert message["type"] == "http.response.start"
            raise OSError("client disconnected before response start")

        with pytest.raises(ClientDisconnect):
            await app(_task_event_scope(spec_version="2.4"), receive, send)
        return service, events

    service, events = asyncio.run(scenario())
    assert service.entered == service.exited == 1
    assert events.closed is True
    assert events.started is False


def test_task_event_stream_closes_context_on_pre_iteration_disconnect() -> None:
    async def scenario() -> tuple[ProbeTaskEventService, ProbeTaskEvents]:
        events = ProbeTaskEvents()
        service = ProbeTaskEventService(events)
        app = create_gateway_app(
            service=service,  # type: ignore[arg-type]
            bearer_token="secret-token",
        )
        response_started = asyncio.Event()

        async def receive() -> dict[str, str]:
            await response_started.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, object]) -> None:
            assert message["type"] == "http.response.start"
            response_started.set()
            await asyncio.Event().wait()

        await app(_task_event_scope(spec_version="2.0"), receive, send)
        return service, events

    service, events = asyncio.run(scenario())
    assert service.entered == service.exited == 1
    assert events.closed is True
    assert events.started is False


def test_task_event_stream_closes_context_when_body_send_fails() -> None:
    async def scenario() -> tuple[
        ProbeTaskEventService,
        ProbeTaskEvents,
        list[str],
    ]:
        first = TaskStreamEvent(
            event_type="assistant.delta",
            task_id="task-1",
            run_count=1,
            created_at=datetime(2026, 8, 28, 12, 0, tzinfo=UTC),
            data={"content": "token"},
        )
        events = ProbeTaskEvents(first)
        service = ProbeTaskEventService(events)
        app = create_gateway_app(
            service=service,  # type: ignore[arg-type]
            bearer_token="secret-token",
        )
        seen: list[str] = []

        async def receive() -> dict[str, str]:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def send(message: dict[str, object]) -> None:
            seen.append(str(message["type"]))
            if message["type"] == "http.response.body":
                raise OSError("client disconnected while sending body")

        with pytest.raises(ClientDisconnect):
            await app(_task_event_scope(spec_version="2.4"), receive, send)
        return service, events, seen

    service, events, seen = asyncio.run(scenario())
    assert service.entered == service.exited == 1
    assert events.closed is True
    assert events.started is True
    assert seen == ["http.response.start", "http.response.body"]


def test_task_event_endpoint_auth_validation_replay_and_headers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = DelayedAgentFactory(delay=0.2)
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        {},
        checkpointer=object(),
        backend=MemoryBackend(),
        task_store=store,
    )
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/main/tasks",
                headers=auth_headers(),
                json={"input": {"content": "stream me"}, "metadata": {}},
            )
            assert created.status_code == 201
            task_id = created.json()["task_id"]
            run_count = created.json()["run_count"]

            unauthorized = client.get(f"/tasks/{task_id}/events?run_count={run_count}")
            assert unauthorized.status_code == 401
            missing_run = client.get(f"/tasks/{task_id}/events", headers=auth_headers())
            assert missing_run.status_code == 400
            invalid_run = client.get(
                f"/tasks/{task_id}/events?run_count=old", headers=auth_headers()
            )
            assert invalid_run.status_code == 400
            mismatch = client.get(
                f"/tasks/{task_id}/events?run_count=0", headers=auth_headers()
            )
            assert mismatch.status_code == 409
            assert mismatch.json()["error"]["code"] == "task_run_mismatch"
            bad_cursor = client.get(
                f"/tasks/{task_id}/events?run_count={run_count}",
                headers={**auth_headers(), "Last-Event-ID": "not-base64"},
            )
            assert bad_cursor.status_code == 400

            streamed = client.get(
                f"/tasks/{task_id}/events?run_count={run_count}",
                headers=auth_headers(),
            )
            assert streamed.status_code == 200
            assert streamed.headers["content-type"].startswith("text/event-stream")
            assert streamed.headers["cache-control"] == "no-cache, no-transform"
            assert streamed.headers["x-accel-buffering"] == "no"
            records = parse_sse_records(streamed.text)
            assert [item["event"] for item in records] == [
                "task.snapshot",
                "task.completed",
                "stream.end",
            ]
            assert records[0]["data"]["task_id"] == task_id
            assert records[0]["data"]["status"] == "running"
            assert records[1]["data"]["last_result"] == "done: stream me"
            assert records[2]["data"] == {
                "task_id": task_id,
                "run_count": run_count,
                "created_at": records[2]["data"]["created_at"],
                "reason": "completed",
            }
            assert "id" not in records[2]

            replayed = client.get(
                f"/tasks/{task_id}/events?run_count={run_count}",
                headers={
                    **auth_headers(),
                    "Last-Event-ID": str(records[0]["id"]),
                },
            )
            replay_records = parse_sse_records(replayed.text)
            assert [item["event"] for item in replay_records] == [
                "task.completed",
                "stream.end",
            ]
    finally:
        asyncio.run(control.close())
        store.close()


def test_task_event_endpoint_requires_durable_task_store(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch, delay=0.01)
    with TestClient(app) as client:
        created = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "no ledger"}, "metadata": {}},
        )
        payload = created.json()
        response = client.get(
            f"/tasks/{payload['task_id']}/events?run_count={payload['run_count']}",
            headers=auth_headers(),
        )
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "task_events_unavailable"


def test_remote_task_event_stream_rewrites_task_id_and_replays_cursor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    factory = DelayedAgentFactory(delay=0.15)
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    remote_store = TaskStore(str(tmp_path / "remote-tasks.sqlite"))
    remote_control = async_subagent_runtime.AgentControl(
        {
            "code_wiki": LocalWorkerSpec(
                name="code_wiki",
                description="remote code wiki",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=[],
                skills=[],
            )
        },
        {},
        checkpointer=object(),
        backend=MemoryBackend(),
        task_store=remote_store,
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
    remote_root = FastAPI()
    remote_root.mount("/a2a", remote_app)

    proxy_store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
    proxy_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=MemoryBackend(),
        task_store=proxy_store,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": httpx.ASGITransport(app=remote_root)}
        ),
    )
    proxy_service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=proxy_control,
    )
    proxy_app = create_gateway_app(
        service=proxy_service,
        bearer_token="secret-token",
    )
    try:
        with TestClient(proxy_app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "remote stream"}, "metadata": {}},
            )
            assert created.status_code == 201, created.json()
            proxy_task_id = created.json()["task_id"]
            run_count = created.json()["run_count"]
            downstream_task_id = proxy_control.get_task_record(
                proxy_task_id
            ).upstream_task_id
            assert downstream_task_id and downstream_task_id != proxy_task_id

            streamed = client.get(
                f"/tasks/{proxy_task_id}/events?run_count={run_count}",
                headers=auth_headers(),
            )
            assert streamed.status_code == 200
            records = parse_sse_records(streamed.text)
            assert [item["event"] for item in records] == [
                "task.snapshot",
                "task.completed",
                "stream.end",
            ]
            assert all(item["data"]["task_id"] == proxy_task_id for item in records)
            assert all(
                downstream_task_id not in json.dumps(item["data"]) for item in records
            )

            replay = client.get(
                f"/tasks/{proxy_task_id}/events?run_count={run_count}",
                headers={
                    **auth_headers(),
                    "Last-Event-ID": str(records[0]["id"]),
                },
            )
            assert replay.status_code == 200
            replay_records = parse_sse_records(replay.text)
            assert [item["event"] for item in replay_records] == [
                "task.completed",
                "stream.end",
            ]
            assert replay_records[0]["id"] == records[1]["id"]
    finally:
        asyncio.run(proxy_control.close())
        asyncio.run(remote_control.close())
        proxy_store.close()
        remote_store.close()


def test_established_remote_stream_fault_emits_error_then_end(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith(
            "/agents/code_wiki/tasks"
        ):
            return httpx.Response(
                201,
                json={
                    "task_id": "downstream-1",
                    "agent_name": "code_wiki",
                    "status": "running",
                    "last_result": None,
                    "error": None,
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            )
        if request.method == "GET" and request.url.path.endswith(
            "/tasks/downstream-1/events"
        ):
            body = (
                "id: cursor-1\n"
                "event: task.snapshot\n"
                'data: {"task_id":"downstream-1","run_count":1,'
                '"created_at":"2026-08-28T12:00:00+00:00",'
                '"status":"running","last_result":null,"error":null,'
                '"updated_at":"2026-08-28T12:00:00+00:00",'
                '"pending_review":null,"artifacts":[]}\n\n'
                "event: task.completed\ndata: not-json\n\n"
            ).encode()
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": httpx.MockTransport(handler)}
        ),
    )
    service = GatewayTaskModule(
        main_agent_name="remote_code_wiki",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "remote"}, "metadata": {}},
            )
            assert created.status_code == 201
            payload = created.json()
            response = client.get(
                f"/tasks/{payload['task_id']}/events?run_count=1",
                headers=auth_headers(),
            )
            assert response.status_code == 200
            records = parse_sse_records(response.text)
            assert [item["event"] for item in records] == [
                "task.snapshot",
                "stream.error",
                "stream.end",
            ]
            assert records[1]["data"]["code"] == "task_stream_error"
            assert records[2]["data"]["reason"] == "error"
            assert "id" not in records[1]
            assert "id" not in records[2]
    finally:
        asyncio.run(control.close())
        store.close()


@pytest.mark.parametrize(
    "first_record",
    [
        (
            "event: assistant.delta\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:00+00:00",'
            '"content":"too early"}\n\n'
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:00+00:00",'
            '"reason":"completed"}\n\n'
        ),
    ],
)
def test_fresh_remote_stream_requires_snapshot_before_any_other_event(
    first_record: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith(
            "/agents/code_wiki/tasks"
        ):
            return httpx.Response(
                201,
                json={
                    "task_id": "downstream-1",
                    "agent_name": "code_wiki",
                    "status": "running",
                    "last_result": None,
                    "error": None,
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            )
        if request.method == "GET" and request.url.path.endswith(
            "/tasks/downstream-1/events"
        ):
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=first_record.encode(),
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": httpx.MockTransport(handler)}
        ),
    )
    service = GatewayTaskModule(
        main_agent_name="remote_code_wiki",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "remote"}, "metadata": {}},
            )
            assert created.status_code == 201
            task_id = created.json()["task_id"]
            response = client.get(
                f"/tasks/{task_id}/events?run_count=1",
                headers=auth_headers(),
            )

        assert response.status_code == 200
        records = parse_sse_records(response.text)
        assert [record["event"] for record in records] == [
            "stream.error",
            "stream.end",
        ]
        assert records[1]["data"]["reason"] == "error"
    finally:
        asyncio.run(control.close())
        store.close()


@pytest.mark.parametrize(
    "tail",
    [
        (
            "event: stream.error\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"code":"downstream_error","message":"failed"}\n\n'
        ),
        (
            "event: stream.error\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"code":"downstream_error","message":"failed"}\n\n'
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:02+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"reason":"error"}\n\n'
        ),
        (
            "id: cursor-2\n"
            "event: task.snapshot\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"status":"running","last_result":null,"error":null,'
            '"updated_at":"2026-08-28T12:00:01+00:00",'
            '"pending_review":null,"artifacts":[]}\n\n'
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:02+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            "id: cursor-2\n"
            "event: task.completed\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"status":"completed","last_result":"done","error":null,'
            '"updated_at":"2026-08-28T12:00:01+00:00",'
            '"pending_review":null,"artifacts":[]}\n\n'
            "event: stream.end\n"
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:02+00:00",'
            '"reason":"failed"}\n\n'
        ),
    ],
)
def test_remote_stream_requires_one_atomic_error_end_pair(
    tail: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    snapshot = (
        "id: cursor-1\n"
        "event: task.snapshot\n"
        'data: {"task_id":"downstream-1","run_count":1,'
        '"created_at":"2026-08-28T12:00:00+00:00",'
        '"status":"running","last_result":null,"error":null,'
        '"updated_at":"2026-08-28T12:00:00+00:00",'
        '"pending_review":null,"artifacts":[]}\n\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith(
            "/agents/code_wiki/tasks"
        ):
            return httpx.Response(
                201,
                json={
                    "task_id": "downstream-1",
                    "agent_name": "code_wiki",
                    "status": "running",
                    "last_result": None,
                    "error": None,
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            )
        if request.method == "GET" and request.url.path.endswith(
            "/tasks/downstream-1/events"
        ):
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=(snapshot + tail).encode(),
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": httpx.MockTransport(handler)}
        ),
    )
    service = GatewayTaskModule(
        main_agent_name="remote_code_wiki",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "remote"}, "metadata": {}},
            )
            task_id = created.json()["task_id"]
            response = client.get(
                f"/tasks/{task_id}/events?run_count=1",
                headers=auth_headers(),
            )

        records = parse_sse_records(response.text)
        event_types = [record["event"] for record in records]
        assert event_types[0] == "task.snapshot"
        assert event_types[-2:] == ["stream.error", "stream.end"]
        assert sum(record["event"] == "stream.error" for record in records) == 1
        assert records[-2]["data"]["code"] == "task_stream_error"
        assert records[-1]["data"]["reason"] == "error"
    finally:
        asyncio.run(control.close())
        store.close()


def test_resumed_remote_stream_rejects_late_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    resumed_body = (
        "id: cursor-2\n"
        "event: task.running\n"
        'data: {"task_id":"downstream-1","run_count":1,'
        '"created_at":"2026-08-28T12:00:01+00:00",'
        '"status":"running","last_result":null,"error":null,'
        '"updated_at":"2026-08-28T12:00:01+00:00",'
        '"pending_review":null,"artifacts":[]}\n\n'
        "id: cursor-3\n"
        "event: task.snapshot\n"
        'data: {"task_id":"downstream-1","run_count":1,'
        '"created_at":"2026-08-28T12:00:02+00:00",'
        '"status":"running","last_result":null,"error":null,'
        '"updated_at":"2026-08-28T12:00:02+00:00",'
        '"pending_review":null,"artifacts":[]}\n\n'
        "event: stream.end\n"
        'data: {"task_id":"downstream-1","run_count":1,'
        '"created_at":"2026-08-28T12:00:03+00:00",'
        '"reason":"completed"}\n\n'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "task_id": "downstream-1",
                    "agent_name": "code_wiki",
                    "status": "running",
                    "last_result": None,
                    "error": None,
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            )
        assert request.headers["last-event-id"] == "downstream-cursor"
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=resumed_body.encode(),
        )

    store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": httpx.MockTransport(handler)}
        ),
    )
    service = GatewayTaskModule(
        main_agent_name="remote_code_wiki",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "remote"}, "metadata": {}},
            )
            task_id = created.json()["task_id"]
            response = client.get(
                f"/tasks/{task_id}/events?run_count=1",
                headers={
                    **auth_headers(),
                    "Last-Event-ID": "downstream-cursor",
                },
            )

        records = parse_sse_records(response.text)
        assert [record["event"] for record in records] == [
            "task.running",
            "stream.error",
            "stream.end",
        ]
        assert records[2]["data"]["reason"] == "error"
    finally:
        asyncio.run(control.close())
        store.close()


def test_resumed_remote_stream_allows_end_without_replayed_full_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            return httpx.Response(
                201,
                json={
                    "task_id": "downstream-1",
                    "agent_name": "code_wiki",
                    "status": "running",
                    "last_result": None,
                    "error": None,
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            )
        assert request.headers["last-event-id"] == "terminal-cursor"
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=(
                "event: stream.end\n"
                'data: {"task_id":"downstream-1","run_count":1,'
                '"created_at":"2026-08-28T12:00:01+00:00",'
                '"reason":"completed"}\n\n'
            ).encode(),
        )

    store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=A2AClient(
            transports={"https://example.com/a2a": httpx.MockTransport(handler)}
        ),
    )
    service = GatewayTaskModule(
        main_agent_name="remote_code_wiki",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "remote"}, "metadata": {}},
            )
            response = client.get(
                f"/tasks/{created.json()['task_id']}/events?run_count=1",
                headers={
                    **auth_headers(),
                    "Last-Event-ID": "terminal-cursor",
                },
            )

        records = parse_sse_records(response.text)
        assert [record["event"] for record in records] == ["stream.end"]
        assert records[0]["data"]["reason"] == "completed"
    finally:
        asyncio.run(control.close())
        store.close()
