from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager

import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.runtime.task_events import TaskStreamEvent
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
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
    def __init__(self, worker_tools=None) -> None:
        self.worker_tools = worker_tools

    async def ainvoke(self, payload, *, config, version):
        if self.worker_tools is None:
            return {"messages": [{"role": "assistant", "content": "child done"}]}
        spawn_tool = next(
            tool for tool in self.worker_tools if tool.name == "spawn_agent"
        )
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
        return DelegatingFakeAgent(kwargs.get("worker_tools"))


class RemoteBackDelegatingFakeAgent:
    def __init__(self, worker_tools=None) -> None:
        self.worker_tools = worker_tools

    async def ainvoke(self, payload, *, config, version):
        if self.worker_tools is None:
            return {"messages": [{"role": "assistant", "content": "no tools"}]}
        spawn_tool = next(
            tool for tool in self.worker_tools if tool.name == "spawn_agent"
        )
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
        return RemoteBackDelegatingFakeAgent(kwargs.get("worker_tools"))


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
        self.created_idempotency_keys: list[str | None] = []
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
        idempotency_key: str | None = None,
    ) -> dict[str, object]:
        self.created_inputs.append(input_content)
        self.created_metadata.append(dict(metadata))
        self.created_attachments.append(attachments)
        self.created_idempotency_keys.append(idempotency_key)
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
        idempotency_key: str | None = None,
    ) -> dict[str, object]:
        payload = await super().create_task(
            remote_ref,
            input_content=input_content,
            metadata=metadata,
            attachments=attachments,
            webhook=webhook,
            idempotency_key=idempotency_key,
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
        idempotency_key: str | None = None,
    ) -> dict[str, object]:
        self.created_inputs.append(input_content)
        self.created_metadata.append(dict(metadata))
        self.created_attachments.append(attachments)
        self.created_idempotency_keys.append(idempotency_key)
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
