from __future__ import annotations

import asyncio
import base64
import json
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from starlette.requests import ClientDisconnect

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.integrations.a2a.client import A2AClient
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
from ruyi_agent.gateway.models import TaskResponse, TaskRouteRecord
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.task_events import TaskStreamEvent
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore


class DelayedFakeAgent:
    def __init__(self, delay: float = 0.05) -> None:
        self.delay = delay
        self.calls: list[dict[str, object]] = []

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await async_subagent_runtime.asyncio.sleep(self.delay)
        content = payload["messages"][0]["content"]
        return {"messages": [{"role": "assistant", "content": f"done: {content}"}]}


class DelayedAgentFactory:
    def __init__(self, delay: float = 0.05) -> None:
        self.delay = delay
        self.created: list[DelayedFakeAgent] = []
        self.control: async_subagent_runtime.AgentControl | None = None

    def __call__(self, **kwargs):
        agent = DelayedFakeAgent(delay=self.delay)
        self.created.append(agent)
        return agent


class FailOnceGatewayCommandStore(GatewayCommandStore):
    def __init__(self, db_path: str) -> None:
        super().__init__(db_path)
        self.fail_next_complete = True

    async def acomplete(self, **kwargs: str) -> None:
        if self.fail_next_complete:
            self.fail_next_complete = False
            raise RuntimeError("simulated command completion failure")
        await super().acomplete(**kwargs)


class ReviewInterruptingAgent:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await asyncio.sleep(0)
        content = payload["messages"][0]["content"]
        if content == "needs review":
            return {
                "__interrupt__": [
                    {
                        "value": {
                            "action_requests": [
                                {
                                    "name": "execute",
                                    "args": {"command": "python -V"},
                                }
                            ],
                            "review_configs": [
                                {
                                    "action_name": "execute",
                                    "allowed_decisions": ["approve", "reject"],
                                }
                            ],
                        }
                    }
                ]
            }
        return {"messages": [{"role": "assistant", "content": f"done: {content}"}]}


class ReviewInterruptingAgentFactory:
    def __init__(self) -> None:
        self.created: list[ReviewInterruptingAgent] = []

    def __call__(self, **kwargs):
        agent = ReviewInterruptingAgent()
        self.created.append(agent)
        return agent


class DelegatingFakeAgent:
    def __init__(self, build_worker_tools=None) -> None:
        self.build_worker_tools = build_worker_tools

    async def ainvoke(self, payload, *, config, version):
        if self.build_worker_tools is None:
            return {"messages": [{"role": "assistant", "content": "child done"}]}
        tools = self.build_worker_tools()
        spawn_tool = next(tool for tool in tools if tool.name == "spawn_agent")
        result = await spawn_tool.ainvoke(
            {
                "agent_name": "background_research",
                "task": "delegated from gateway public agent",
            },
            config=config,
        )
        return {"messages": [{"role": "assistant", "content": result}]}


class DelegatingAgentFactory:
    def __call__(self, **kwargs):
        return DelegatingFakeAgent(kwargs.get("build_worker_tools"))


class RemoteBackDelegatingFakeAgent:
    def __init__(self, build_worker_tools=None) -> None:
        self.build_worker_tools = build_worker_tools

    async def ainvoke(self, payload, *, config, version):
        if self.build_worker_tools is None:
            return {"messages": [{"role": "assistant", "content": "no tools"}]}
        tools = self.build_worker_tools()
        spawn_tool = next(tool for tool in tools if tool.name == "spawn_agent")
        result = await spawn_tool.ainvoke(
            {
                "agent_name": "back_to_a",
                "task": "return to node a",
            },
            config=config,
        )
        return {"messages": [{"role": "assistant", "content": result}]}


class RemoteBackDelegatingAgentFactory:
    def __call__(self, **kwargs):
        return RemoteBackDelegatingFakeAgent(kwargs.get("build_worker_tools"))


class MemoryBackend:
    def __init__(
        self, *, root: str = "/workspace", truncate_upload_results: bool = False
    ) -> None:
        self.root = root
        self.files: dict[str, bytes] = {}
        self.truncate_upload_results = truncate_upload_results

    def upload_files(self, files: list[tuple[str, bytes]]):
        from deepagents.backends.protocol import FileUploadResponse

        responses = []
        for path, content in files:
            self.files[path] = content
            responses.append(FileUploadResponse(path=path, error=None))
        if self.truncate_upload_results and responses:
            return responses[:-1]
        return responses

    def download_files(self, paths: list[str]):
        from deepagents.backends.protocol import FileDownloadResponse

        responses = []
        for path in paths:
            if path in self.files:
                responses.append(
                    FileDownloadResponse(
                        path=path,
                        content=self.files[path],
                        error=None,
                    )
                )
            else:
                responses.append(
                    FileDownloadResponse(
                        path=path,
                        content=None,
                        error="file_not_found",
                    )
                )
        return responses


class StaticRemoteA2AClient:
    def __init__(self) -> None:
        self.created_inputs: list[str] = []
        self.created_metadata: list[dict[str, object]] = []
        self.created_attachments: list[list[dict[str, object]] | None] = []
        self.sent_inputs: list[str] = []
        self.sent_attachments: list[list[dict[str, object]] | None] = []
        self.cancelled: list[str] = []

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict[str, object],
        attachments: list[dict[str, object]] | None = None,
        webhook: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.created_inputs.append(input_content)
        self.created_metadata.append(dict(metadata))
        self.created_attachments.append(attachments)
        return {
            "task_id": "upstream-1",
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": f"remote done: {input_content}",
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
        }

    async def get_task(self, remote_ref, *, task_id: str) -> dict[str, object]:
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": "remote done: refreshed",
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:02Z",
        }

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        self.sent_inputs.append(input_content)
        self.sent_attachments.append(attachments)
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": f"remote follow-up: {input_content}",
            "error": None,
            "run_count": 2,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:03Z",
        }

    async def cancel_task(self, remote_ref, *, task_id: str) -> dict[str, object]:
        self.cancelled.append(task_id)
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "cancelled",
            "last_result": None,
            "error": None,
            "run_count": 2,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:04Z",
        }

    async def submit_review_decision(
        self,
        remote_ref,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": "remote review resumed",
            "error": None,
            "run_count": 2,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:05Z",
            "pending_review": None,
        }


class UnhashableStatusRemoteA2AClient(StaticRemoteA2AClient):
    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict[str, object],
        attachments: list[dict[str, object]] | None = None,
        webhook: dict[str, object] | None = None,
    ) -> dict[str, object]:
        payload = await super().create_task(
            remote_ref,
            input_content=input_content,
            metadata=metadata,
            attachments=attachments,
            webhook=webhook,
        )
        payload["status"] = []
        return payload


class ReviewRemoteA2AClient(StaticRemoteA2AClient):
    def __init__(self) -> None:
        super().__init__()
        self.submitted_reviews: list[dict[str, object]] = []

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict[str, object],
        attachments: list[dict[str, object]] | None = None,
        webhook: dict[str, object] | None = None,
    ) -> dict[str, object]:
        self.created_inputs.append(input_content)
        self.created_metadata.append(dict(metadata))
        self.created_attachments.append(attachments)
        return {
            "task_id": "upstream-review-task",
            "agent_name": remote_ref.name,
            "status": "waiting_for_human",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
            "pending_review": {
                "review_id": "remote-review-1",
                "action_requests": [
                    {
                        "name": "execute",
                        "args": {"command": "python -V"},
                    }
                ],
                "review_configs": [
                    {
                        "action_name": "execute",
                        "allowed_decisions": ["approve", "reject"],
                    }
                ],
            },
        }

    async def get_task(self, remote_ref, *, task_id: str) -> dict[str, object]:
        if self.submitted_reviews:
            return {
                "task_id": task_id,
                "agent_name": remote_ref.name,
                "status": "completed",
                "last_result": "remote review resumed",
                "error": None,
                "run_count": 2,
                "created_at": "2026-04-23T00:00:00Z",
                "updated_at": "2026-04-23T00:00:02Z",
                "pending_review": None,
            }
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "waiting_for_human",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
            "pending_review": {
                "review_id": "remote-review-1",
                "action_requests": [
                    {
                        "name": "execute",
                        "args": {"command": "python -V"},
                    }
                ],
                "review_configs": [
                    {
                        "action_name": "execute",
                        "allowed_decisions": ["approve", "reject"],
                    }
                ],
            },
        }

    async def submit_review_decision(
        self,
        remote_ref,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, object]],
    ) -> dict[str, object]:
        self.submitted_reviews.append(
            {
                "task_id": task_id,
                "review_id": review_id,
                "decisions": decisions,
            }
        )
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": "remote review resumed",
            "error": None,
            "run_count": 2,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:02Z",
            "pending_review": None,
        }


def build_specs() -> dict[str, LocalWorkerSpec]:
    return {
        "main": LocalWorkerSpec(
            name="main",
            description="main entry agent",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=["/sandbox/home/AGENTS.md"],
            skills=["frontend-skill"],
        ),
        "background_research": LocalWorkerSpec(
            name="background_research",
            description="background helper",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=["/sandbox/home/AGENTS.md"],
            skills=["frontend-skill"],
        ),
    }


def build_test_remote_refs() -> dict[str, RemoteRef]:
    return {
        "remote_code_wiki": RemoteRef(
            name="remote_code_wiki",
            description="remote helper",
            url="https://example.com/a2a",
            remote_agent_name="code_wiki",
            auth={"type": "bearer", "token_env": "REMOTE_CODE_WIKI_TOKEN"},
        )
    }


def build_agent_configs() -> dict[str, dict[str, object]]:
    return {
        "main": {
            "kind": "local",
            "public": True,
            "name": "main",
            "description": "main entry agent",
        },
        "background_research": {
            "kind": "local",
            "public": False,
            "name": "background_research",
            "description": "background helper",
        },
        "remote_code_wiki": {
            "kind": "remote_ref",
            "public": True,
            "name": "remote_code_wiki",
            "description": "remote helper",
            "url": "https://example.com/a2a",
            "remote_agent_name": "code_wiki",
            "auth": {"type": "bearer", "token_env": "REMOTE_CODE_WIKI_TOKEN"},
        },
    }


def build_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    delay: float = 0.05,
    a2a_client: A2AClient | None = None,
    route_store: GatewayRouteStore | None = None,
    command_store: GatewayCommandStore | None = None,
    node_id: str | None = None,
    backend: object | None = None,
    workspace_root: str = "/workspace",
    unavailable_agents: dict[str, str] | None = None,
) -> tuple[object, DelayedAgentFactory]:
    factory = DelayedAgentFactory(delay=delay)
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=backend or MemoryBackend(root=workspace_root),
        a2a_client=a2a_client,
        node_id=node_id,
        workspace_root=workspace_root,
    )
    factory.control = control
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
        route_store=route_store,
        command_store=command_store,
        unavailable_agents=unavailable_agents,
    )
    return create_gateway_app(service=service, bearer_token="secret-token"), factory


def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer secret-token"}


def parse_sse_records(body: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for block in body.split("\n\n"):
        if not block or block.startswith(":"):
            continue
        record: dict[str, object] = {}
        data_lines: list[str] = []
        for line in block.splitlines():
            field, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if field == "data":
                data_lines.append(value)
            elif field in {"event", "id"}:
                record[field] = value
        record["data"] = json.loads("\n".join(data_lines))
        records.append(record)
    return records


class ProbeTaskEvents:
    def __init__(self, first: TaskStreamEvent | None = None) -> None:
        self.first = first
        self.started = False
        self.closed = False
        self._first_yielded = False
        self._wait_forever = asyncio.Event()

    def __aiter__(self) -> "ProbeTaskEvents":
        return self

    async def __anext__(self) -> TaskStreamEvent:
        self.started = True
        if self.first is not None and not self._first_yielded:
            self._first_yielded = True
            return self.first
        await self._wait_forever.wait()
        raise StopAsyncIteration

    async def aclose(self) -> None:
        self.closed = True


class ProbeTaskEventService:
    def __init__(self, events: ProbeTaskEvents) -> None:
        self.events = events
        self.entered = 0
        self.exited = 0

    @asynccontextmanager
    async def open_task_event_stream(self, *args, **kwargs):
        del args, kwargs
        self.entered += 1
        try:
            yield self.events
        finally:
            self.exited += 1
            await self.events.aclose()


def _task_event_scope(*, spec_version: str) -> dict[str, object]:
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": spec_version},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/tasks/task-1/events",
        "raw_path": b"/tasks/task-1/events",
        "query_string": b"run_count=1",
        "headers": [(b"authorization", b"Bearer secret-token")],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "root_path": "",
        "state": {},
    }


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

            unauthorized = client.get(
                f"/tasks/{task_id}/events?run_count={run_count}"
            )
            assert unauthorized.status_code == 401
            missing_run = client.get(
                f"/tasks/{task_id}/events", headers=auth_headers()
            )
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
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote code wiki",
            }
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
            transports={
                "https://example.com/a2a": httpx.ASGITransport(app=remote_root)
            }
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
            assert all(
                item["data"]["task_id"] == proxy_task_id for item in records
            )
            assert all(
                downstream_task_id not in json.dumps(item["data"])
                for item in records
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
                'id: cursor-1\n'
                'event: task.snapshot\n'
                'data: {"task_id":"downstream-1","run_count":1,'
                '"created_at":"2026-08-28T12:00:00+00:00",'
                '"status":"running","last_result":null,"error":null,'
                '"updated_at":"2026-08-28T12:00:00+00:00",'
                '"pending_review":null,"artifacts":[]}\n\n'
                'event: task.completed\ndata: not-json\n\n'
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
            transports={
                "https://example.com/a2a": httpx.MockTransport(handler)
            }
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
            'event: assistant.delta\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:00+00:00",'
            '"content":"too early"}\n\n'
            'event: stream.end\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            'event: stream.end\n'
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
            transports={
                "https://example.com/a2a": httpx.MockTransport(handler)
            }
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
            'event: stream.error\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"code":"downstream_error","message":"failed"}\n\n'
        ),
        (
            'event: stream.error\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"code":"downstream_error","message":"failed"}\n\n'
            'event: stream.end\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:02+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            'event: stream.end\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"reason":"error"}\n\n'
        ),
        (
            'id: cursor-2\n'
            'event: task.snapshot\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"status":"running","last_result":null,"error":null,'
            '"updated_at":"2026-08-28T12:00:01+00:00",'
            '"pending_review":null,"artifacts":[]}\n\n'
            'event: stream.end\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:02+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            'event: stream.end\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"reason":"completed"}\n\n'
        ),
        (
            'id: cursor-2\n'
            'event: task.completed\n'
            'data: {"task_id":"downstream-1","run_count":1,'
            '"created_at":"2026-08-28T12:00:01+00:00",'
            '"status":"completed","last_result":"done","error":null,'
            '"updated_at":"2026-08-28T12:00:01+00:00",'
            '"pending_review":null,"artifacts":[]}\n\n'
            'event: stream.end\n'
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
        'id: cursor-1\n'
        'event: task.snapshot\n'
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
            transports={
                "https://example.com/a2a": httpx.MockTransport(handler)
            }
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
        'id: cursor-2\n'
        'event: task.running\n'
        'data: {"task_id":"downstream-1","run_count":1,'
        '"created_at":"2026-08-28T12:00:01+00:00",'
        '"status":"running","last_result":null,"error":null,'
        '"updated_at":"2026-08-28T12:00:01+00:00",'
        '"pending_review":null,"artifacts":[]}\n\n'
        'id: cursor-3\n'
        'event: task.snapshot\n'
        'data: {"task_id":"downstream-1","run_count":1,'
        '"created_at":"2026-08-28T12:00:02+00:00",'
        '"status":"running","last_result":null,"error":null,'
        '"updated_at":"2026-08-28T12:00:02+00:00",'
        '"pending_review":null,"artifacts":[]}\n\n'
        'event: stream.end\n'
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
            transports={
                "https://example.com/a2a": httpx.MockTransport(handler)
            }
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
                'event: stream.end\n'
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
            transports={
                "https://example.com/a2a": httpx.MockTransport(handler)
            }
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


def test_gateway_exposes_subagent_task_created_by_public_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        async_subagent_runtime,
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
        delegation_local_worker_specs={"background_research": child_spec},
    )
    control_ref: dict[str, async_subagent_runtime.AgentControl] = {}
    parent_spec.build_delegation_tools = lambda: control_ref["control"].build_tools_for(
        "main"
    )
    control = async_subagent_runtime.AgentControl(
        {
            "main": parent_spec,
            "background_research": child_spec,
        },
        {},
        checkpointer=object(),
        backend=object(),
    )
    control_ref["control"] = control
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        control=control,
    )

    async def scenario() -> tuple[
        list[async_subagent_runtime.TaskRecord],
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
        if parent.active_run is not None:
            await parent.active_run
        child_records = [
            record
            for record in control.list_task_records()
            if record.agent_name == "background_research"
        ]
        for record in child_records:
            if record.active_run is not None:
                await record.active_run
        child_response = await service.get_task(child_records[0].task_id)
        continued_child = await service.send_input(
            child_records[0].task_id,
            "follow-up",
        )
        continued_record = control.get_task_record(child_records[0].task_id)
        if continued_record.active_run is not None:
            await continued_record.active_run
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


def test_team_console_shell_is_served_without_embedding_gateway_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        page = client.get("/debug/team")
        styles = client.get("/debug/team/app.css")
        script = client.get("/debug/team/app.js")

    assert page.status_code == 200
    assert "RUYI / ARCHITECTURE DESK" in page.text
    assert "dev-token" not in page.text
    assert styles.status_code == 200
    assert "--paper" in styles.text
    assert script.status_code == 200
    assert 'api("/agents")' in script.text


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
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
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
    assert sum("idempotency-replayed" in response.headers for response in responses) == 99
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
        artifact = factory.control.register_artifact(
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
        artifact = factory.control.register_artifact(
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
    app, factory = build_app(monkeypatch, delay=0.1, node_id="node-b")
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
    assert factory.control is not None
    record = factory.control.get_task_record(payload["task_id"])
    assert record.root_task_id == "node-a:root-1"
    assert record.depth == 2
    assert record.delegation_root_id == "node-a:root-1"
    assert record.delegation_visited_nodes == ("node-a", "node-b")
    assert record.delegation_max_depth == 3
    assert record.delegation_max_tasks_per_root == 20


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
    app, factory = build_app(monkeypatch, delay=0.03)
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

    assert first.status_code == 503
    assert first.json()["error"]["code"] == "idempotency_unavailable"
    assert retry.status_code == 503
    assert retry.json()["error"]["code"] == "idempotency_unavailable"
    assert factory.control is not None
    assert factory.control.get_task_record(task_id).run_count == 1


def test_send_retry_after_command_completion_failure_publishes_one_mailbox_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = DelayedAgentFactory(delay=0.03)
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    db_path = str(tmp_path / "tasks.sqlite")
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    route_store = GatewayRouteStore(str(tmp_path / "routes.sqlite"))
    command_store = FailOnceGatewayCommandStore(db_path)
    command_store.fail_next_complete = False
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=MemoryBackend(),
        mailbox=mailbox,
        task_store=task_store,
        workspace_root="/workspace",
    )
    factory.control = control
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


def test_remote_ref_forwards_via_a2a(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")
    remote_factory = DelayedAgentFactory(delay=0.03)
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", remote_factory)

    remote_control = async_subagent_runtime.AgentControl(
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
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote code wiki",
            }
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

    app, factory = build_app(
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
        assert factory.control is not None
        proxy_record = factory.control.get_task_record(proxy_task_id)
        assert proxy_record.route_kind == "remote_ref"
        assert proxy_record.agent_name == "remote_code_wiki"
        assert proxy_record.upstream_task_id is not None
        assert proxy_record.depth == 1

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


def test_public_remote_ref_maps_unhashable_status_to_upstream_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, factory = build_app(
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
    assert response.json()["error"]["code"] == "upstream_gateway_error"
    assert factory.control is not None
    assert factory.control.list_task_records() == []


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
    app, factory = build_app(
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

        reviews_response = client.get("/reviews", headers=auth_headers())
        assert reviews_response.status_code == 200, reviews_response.json()
        reviews_payload = reviews_response.json()
        assert reviews_payload["items"][0]["review_id"] == "remote-review-1"
        assert reviews_payload["items"][0]["task_id"] == proxy_task_id
        assert reviews_payload["items"][0]["route_kind"] == "remote_ref"

        review_response = client.get(
            "/reviews/remote-review-1",
            headers=auth_headers(),
        )
        assert review_response.status_code == 200, review_response.json()
        assert review_response.json()["task_id"] == proxy_task_id

        task_reviews_response = client.get(
            f"/tasks/{proxy_task_id}/reviews",
            headers=auth_headers(),
        )
        assert task_reviews_response.status_code == 200
        assert (
            task_reviews_response.json()["items"][0]["review_id"] == "remote-review-1"
        )

        assert factory.control is not None
        pending = factory.control.list_pending_review_records()
        assert [item.task_id for item in pending] == [proxy_task_id]

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
    monkeypatch.setattr(
        async_subagent_runtime, "create_runtime_agent", interrupt_factory
    )
    control = async_subagent_runtime.AgentControl(
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
        if root.active_run is not None:
            await root.active_run
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if child.active_run is not None:
            await child.active_run
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


def test_remote_a_to_b_to_a_loop_is_rejected_by_visited_nodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOP_GATEWAY_TOKEN", "secret-token")
    monkeypatch.setattr(
        async_subagent_runtime,
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

    a_control = async_subagent_runtime.AgentControl(
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
            "main": {
                "kind": "local",
                "public": True,
                "name": "main",
                "description": "node a main",
            },
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

    b_control_ref: dict[str, async_subagent_runtime.AgentControl] = {}
    b_spec = LocalWorkerSpec(
        name="code_wiki",
        description="node b code wiki",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_remote_refs={"back_to_a": b_to_a_ref},
    )
    b_spec.build_delegation_tools = lambda: b_control_ref["control"].build_tools_for(
        "code_wiki"
    )
    b_control = async_subagent_runtime.AgentControl(
        {"code_wiki": b_spec},
        {"back_to_a": b_to_a_ref},
        checkpointer=object(),
        backend=object(),
        a2a_client=A2AClient(transports=transports_b),
        node_id="node-b",
    )
    b_control_ref["control"] = b_control
    b_service = GatewayTaskModule(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "node b code wiki",
            }
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
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
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
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", remote_factory)

    remote_control = async_subagent_runtime.AgentControl(
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
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote code wiki",
            }
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
        async_subagent_runtime.httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )
    route_db = tmp_path / "gateway-routes.sqlite"
    first_store = GatewayRouteStore(str(route_db))
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=StaticRemoteA2AClient(),  # type: ignore[arg-type]
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
