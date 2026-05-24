from __future__ import annotations

import asyncio
import base64
import time
from pathlib import Path

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

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
from ruyi_agent.channels.http.api import (
    AgentControlGatewayRuntime,
    GatewayService,
    TaskRouteRecord,
    create_gateway_app,
)
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore


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
    def __init__(self, *, root: str = "/workspace", truncate_upload_results: bool = False) -> None:
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
    node_id: str | None = None,
    backend: object | None = None,
    workspace_root: str = "/workspace",
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
    service = GatewayService(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        runtime=AgentControlGatewayRuntime(control),
        route_store=route_store,
    )
    return create_gateway_app(service=service, bearer_token="secret-token"), factory


def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer secret-token"}


def test_gateway_public_local_agent_can_delegate_to_its_workers(
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
    parent_spec.build_delegation_tools = (
        lambda: control_ref["control"].build_tools_for("main")
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
    service = GatewayService(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        runtime=AgentControlGatewayRuntime(control),
    )

    async def scenario() -> list[async_subagent_runtime.TaskRecord]:
        response = await service.create_task(
            agent_name="main",
            input_content="gateway task",
            metadata={},
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
        return child_records

    child_records = asyncio.run(scenario())

    assert len(child_records) == 1
    assert child_records[0].parent_task_id is not None
    assert child_records[0].depth == 2


def test_gateway_requires_bearer_token(monkeypatch: pytest.MonkeyPatch) -> None:
    app, _ = build_app(monkeypatch)
    with TestClient(app) as client:
        response = client.get("/agents")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


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


def test_create_task_returns_201_and_running_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, factory = build_app(monkeypatch, delay=0.1)
    with TestClient(app) as client:
        response = client.post(
            "/agents/main/tasks",
            headers=auth_headers(),
            json={"input": {"content": "research react"}, "metadata": {"channel": "tg"}},
        )

    assert response.status_code == 201
    assert response.headers["location"].startswith("/tasks/")
    payload = response.json()
    assert payload["status"] == "running"
    assert payload["run_count"] == 1
    assert payload["metadata"] == {"channel": "tg"}
    assert len(factory.created) == 1


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
    remote_service = GatewayService(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote code wiki",
            }
        },
        runtime=AgentControlGatewayRuntime(remote_control),
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
        assert task_reviews_response.json()["items"][0]["review_id"] == "remote-review-1"

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
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", interrupt_factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )
    service = GatewayService(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        runtime=AgentControlGatewayRuntime(control),
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
        child_review_id = control.get_task_record(child.task_id).pending_review["review_id"]

    asyncio.run(seed_review())

    assert root_task_id is not None
    assert child_review_id is not None
    asyncio.run(
        service._route_store.asave_route(
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
    a_service = GatewayService(
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
        runtime=AgentControlGatewayRuntime(a_control),
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
    b_spec.build_delegation_tools = (
        lambda: b_control_ref["control"].build_tools_for("code_wiki")
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
    b_service = GatewayService(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "node b code wiki",
            }
        },
        runtime=AgentControlGatewayRuntime(b_control),
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
    service = GatewayService(
        main_agent_name="main",
        agent_configs=build_agent_configs(),
        runtime=AgentControlGatewayRuntime(control),
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
    remote_service = GatewayService(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote code wiki",
            }
        },
        runtime=AgentControlGatewayRuntime(remote_control),
    )
    remote_app = create_gateway_app(service=remote_service, bearer_token="remote-secret")
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
