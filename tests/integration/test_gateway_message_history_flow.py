from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Sequence

import httpx
from deepagents.backends import StateBackend
from fastapi import FastAPI
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from pydantic import Field

from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.gateway.models import TaskRouteRecord
from ruyi_agent.gateway.routing import _encode_task_message_cursor
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.task_store import TaskStore


@tool
def transcript_echo(value: str) -> str:
    """Echo a value so the checkpoint contains a real tool result."""

    return f"tool-result:{value}"


class TranscriptModel(BaseChatModel):
    tool_call_count: int = Field(default=0)

    @property
    def _llm_type(self) -> str:
        return "transcript-model"

    def bind_tools(
        self,
        tools: Sequence[Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> "TranscriptModel":
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages,
        stop=None,
        run_manager=None,
        **kwargs,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        last_message = messages[-1]
        if isinstance(last_message, ToolMessage):
            response = AIMessage(content=f"answer:{last_message.content}")
        else:
            latest_user = next(
                message
                for message in reversed(messages)
                if isinstance(message, HumanMessage)
            )
            self.tool_call_count += 1
            response = AIMessage(
                content="",
                tool_calls=[
                    {
                        "id": f"call-{self.tool_call_count}",
                        "name": "transcript_echo",
                        "args": {"value": str(latest_user.content)},
                    }
                ],
            )
        return ChatResult(generations=[ChatGeneration(message=response)])

    async def _agenerate(
        self,
        messages,
        stop=None,
        run_manager=None,
        **kwargs,
    ) -> ChatResult:
        return self._generate(
            messages,
            stop=stop,
            run_manager=run_manager,
            **kwargs,
        )


def _local_spec(name: str, *, model: Any) -> LocalWorkerSpec:
    return LocalWorkerSpec(
        name=name,
        description=name,
        system_prompt="Test transcript agent",
        model=model,
        tools=[transcript_echo],
        memory=[],
        skills=[],
    )


def _agent_config(name: str, *, kind: str = "local") -> dict[str, object]:
    return {
        "kind": kind,
        "public": True,
        "name": name,
        "description": name,
    }


async def _wait_for_run(control: AgentControl, task_id: str) -> None:
    record = control.get_task_record(task_id)
    active_run = control.get_live_run(record.task_id)
    if active_run is not None:
        await active_run


def test_local_message_history_is_snapshot_consistent_and_survives_restart(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        checkpoint_path = str(tmp_path / "checkpoints.sqlite")
        task_path = str(tmp_path / "tasks.sqlite")
        route_path = str(tmp_path / "routes.sqlite")
        task_id: str

        async with AsyncSqliteSaver.from_conn_string(checkpoint_path) as saver:
            task_store = TaskStore(task_path)
            route_store = GatewayRouteStore(route_path)
            control = AgentControl(
                {"main": _local_spec("main", model=TranscriptModel())},
                checkpointer=saver,
                backend=StateBackend(),
                task_store=task_store,
            )
            service = GatewayTaskModule(
                main_agent_name="main",
                agent_configs={"main": _agent_config("main")},
                control=control,
                route_store=route_store,
            )
            app = create_gateway_app(service=service, bearer_token="secret")
            client = GatewayHTTPClient(
                base_url="http://gateway.test",
                bearer_token="secret",
                transport=httpx.ASGITransport(app=app),
            )
            try:
                pending_task_id = "pending-without-checkpoint"
                control._task_manager.create_task_record(
                    pending_task_id,
                    "main",
                    parent_task_id=None,
                    root_task_id=pending_task_id,
                    depth=1,
                )
                await route_store.asave_route(
                    TaskRouteRecord(
                        task_id=pending_task_id,
                        agent_name="main",
                        metadata={},
                        route_kind="local",
                        upstream_task_id=pending_task_id,
                    )
                )
                pending_page = await client.list_task_messages(
                    task_id=pending_task_id,
                    limit=20,
                )
                assert pending_page == {
                    "task_id": pending_task_id,
                    "items": [],
                    "next_cursor": None,
                }

                created = await client.create_task(
                    agent_name="main",
                    content="first",
                    metadata={},
                )
                task_id = str(created["task_id"])
                await _wait_for_run(control, task_id)

                first_page = await client.list_task_messages(
                    task_id=task_id,
                    limit=2,
                )
                frozen_cursor = first_page["next_cursor"]
                assert isinstance(frozen_cursor, str)
                assert [item["role"] for item in first_page["items"]] == [
                    "user",
                    "assistant",
                ]
                assert first_page["items"][1]["tool_calls"] == [
                    {
                        "tool_call_id": "call-1",
                        "name": "transcript_echo",
                        "arguments": {"value": "first"},
                    }
                ]

                await client.send_input(task_id=task_id, content="second")
                await _wait_for_run(control, task_id)

                frozen_tail = await client.list_task_messages(
                    task_id=task_id,
                    cursor=frozen_cursor,
                    limit=100,
                )
                assert frozen_tail["next_cursor"] is None
                assert [item["role"] for item in frozen_tail["items"]] == [
                    "tool",
                    "assistant",
                ]
                assert frozen_tail["items"][0]["content"] == "tool-result:first"
                assert frozen_tail["items"][0]["status"] == "success"

                latest = await client.list_task_messages(
                    task_id=task_id,
                    limit=100,
                )
                assert [item["role"] for item in latest["items"]] == [
                    "user",
                    "assistant",
                    "tool",
                    "assistant",
                    "user",
                    "assistant",
                    "tool",
                    "assistant",
                ]
                assert [item["sequence"] for item in latest["items"]] == list(range(8))

                raw_transport = httpx.ASGITransport(app=app)
                missing_cursor = _encode_task_message_cursor(
                    task_id=task_id,
                    checkpoint_id="missing-checkpoint",
                    offset=1,
                )
                cross_task_cursor = _encode_task_message_cursor(
                    task_id="another-task",
                    checkpoint_id="missing-checkpoint",
                    offset=1,
                )
                async with httpx.AsyncClient(
                    transport=raw_transport,
                    base_url="http://gateway.test",
                ) as raw_client:
                    unauthorized = await raw_client.get(f"/tasks/{task_id}/messages")
                    malformed = await raw_client.get(
                        f"/tasks/{task_id}/messages?cursor=not-base64",
                        headers={"Authorization": "Bearer secret"},
                    )
                    missing_snapshot = await raw_client.get(
                        f"/tasks/{task_id}/messages?cursor={missing_cursor}",
                        headers={"Authorization": "Bearer secret"},
                    )
                    cross_task = await raw_client.get(
                        f"/tasks/{task_id}/messages?cursor={cross_task_cursor}",
                        headers={"Authorization": "Bearer secret"},
                    )
                    invalid_limit = await raw_client.get(
                        f"/tasks/{task_id}/messages?limit=0",
                        headers={"Authorization": "Bearer secret"},
                    )
                    non_integer_limit = await raw_client.get(
                        f"/tasks/{task_id}/messages?limit=many",
                        headers={"Authorization": "Bearer secret"},
                    )
                    missing_task = await raw_client.get(
                        "/tasks/not-found/messages",
                        headers={"Authorization": "Bearer secret"},
                    )
                assert unauthorized.status_code == 401
                assert malformed.status_code == 400
                assert malformed.json()["error"]["code"] == "invalid_request"
                assert missing_snapshot.status_code == 400
                assert cross_task.status_code == 400
                assert invalid_limit.status_code == 400
                assert non_integer_limit.status_code == 400
                assert missing_task.status_code == 404
            finally:
                route_store.close()
                task_store.close()

        # A new control reads the saved transcript without compiling the old model.
        async with AsyncSqliteSaver.from_conn_string(checkpoint_path) as saver:
            task_store = TaskStore(task_path)
            route_store = GatewayRouteStore(route_path)
            control = AgentControl(
                {"main": _local_spec("main", model=object())},
                checkpointer=saver,
                backend=object(),
                task_store=task_store,
            )
            service = GatewayTaskModule(
                main_agent_name="main",
                agent_configs={"main": _agent_config("main")},
                control=control,
                route_store=route_store,
            )
            client = GatewayHTTPClient(
                base_url="http://gateway.test",
                bearer_token="secret",
                transport=httpx.ASGITransport(
                    app=create_gateway_app(service=service, bearer_token="secret")
                ),
            )
            try:
                restored = await client.list_task_messages(
                    task_id=task_id,
                    limit=100,
                )
                assert len(restored["items"]) == 8
                assert restored["items"][0]["content"] == "first"
                assert restored["items"][-1]["content"] == ("answer:tool-result:second")
            finally:
                route_store.close()
                task_store.close()

    asyncio.run(scenario())


def test_remote_message_history_preserves_downstream_snapshot_cursor(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("REMOTE_HISTORY_TOKEN", "downstream-secret")

    async def scenario() -> None:
        downstream_task_store = TaskStore(str(tmp_path / "downstream-tasks.sqlite"))
        downstream_route_store = GatewayRouteStore(
            str(tmp_path / "downstream-routes.sqlite")
        )
        upstream_task_store = TaskStore(str(tmp_path / "upstream-tasks.sqlite"))
        upstream_route_store = GatewayRouteStore(
            str(tmp_path / "upstream-routes.sqlite")
        )
        async with AsyncSqliteSaver.from_conn_string(
            str(tmp_path / "downstream-checkpoints.sqlite")
        ) as downstream_saver:
            downstream_control = AgentControl(
                {"worker": _local_spec("worker", model=TranscriptModel())},
                checkpointer=downstream_saver,
                backend=StateBackend(),
                task_store=downstream_task_store,
                node_id="node-downstream",
            )
            downstream_service = GatewayTaskModule(
                main_agent_name="worker",
                agent_configs={"worker": _agent_config("worker")},
                control=downstream_control,
                route_store=downstream_route_store,
            )
            downstream_app = create_gateway_app(
                service=downstream_service,
                bearer_token="downstream-secret",
            )
            downstream_root = FastAPI()
            downstream_root.mount("/a2a", downstream_app)
            downstream_transport = httpx.ASGITransport(app=downstream_root)

            remote_ref = RemoteRef(
                name="remote_worker",
                description="remote worker",
                url="https://downstream.test/a2a",
                remote_agent_name="worker",
                auth={
                    "type": "bearer",
                    "token_env": "REMOTE_HISTORY_TOKEN",
                },
            )
            upstream_control = AgentControl(
                {"main": _local_spec("main", model=object())},
                {"remote_worker": remote_ref},
                checkpointer=object(),
                backend=object(),
                task_store=upstream_task_store,
                a2a_client=A2AClient(
                    transports={
                        "https://downstream.test/a2a": downstream_transport,
                    }
                ),
                node_id="node-upstream",
            )
            upstream_service = GatewayTaskModule(
                main_agent_name="main",
                agent_configs={
                    "main": _agent_config("main"),
                    "remote_worker": _agent_config(
                        "remote_worker",
                        kind="remote_ref",
                    ),
                },
                control=upstream_control,
                route_store=upstream_route_store,
            )
            upstream_app = create_gateway_app(
                service=upstream_service,
                bearer_token="upstream-secret",
            )
            upstream_client = GatewayHTTPClient(
                base_url="http://upstream.test",
                bearer_token="upstream-secret",
                transport=httpx.ASGITransport(app=upstream_app),
            )
            downstream_client = GatewayHTTPClient(
                base_url="http://downstream.test/a2a",
                bearer_token="downstream-secret",
                transport=downstream_transport,
            )
            try:
                created = await upstream_client.create_task(
                    agent_name="remote_worker",
                    content="first",
                    metadata={},
                )
                proxy_task_id = str(created["task_id"])
                proxy_record = upstream_control.get_task_record(proxy_task_id)
                downstream_task_id = str(proxy_record.upstream_task_id)
                await _wait_for_run(downstream_control, downstream_task_id)

                direct_page = await downstream_client.list_task_messages(
                    task_id=downstream_task_id,
                    limit=2,
                )
                proxy_page = await upstream_client.list_task_messages(
                    task_id=proxy_task_id,
                    limit=2,
                )
                frozen_cursor = proxy_page["next_cursor"]
                assert proxy_page["task_id"] == proxy_task_id
                assert direct_page["task_id"] == downstream_task_id
                assert frozen_cursor == direct_page["next_cursor"]

                await upstream_client.send_input(
                    task_id=proxy_task_id,
                    content="second",
                )
                await _wait_for_run(downstream_control, downstream_task_id)

                frozen_tail = await upstream_client.list_task_messages(
                    task_id=proxy_task_id,
                    cursor=str(frozen_cursor),
                    limit=100,
                )
                assert frozen_tail["task_id"] == proxy_task_id
                assert [item["role"] for item in frozen_tail["items"]] == [
                    "tool",
                    "assistant",
                ]
                latest = await upstream_client.list_task_messages(
                    task_id=proxy_task_id,
                    limit=100,
                )
                assert len(latest["items"]) == 8

                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=upstream_app),
                    base_url="http://upstream.test",
                    headers={"Authorization": "Bearer upstream-secret"},
                ) as raw_client:
                    invalid_remote_cursor = await raw_client.get(
                        f"/tasks/{proxy_task_id}/messages?cursor=invalid"
                    )
                assert invalid_remote_cursor.status_code == 400
                assert invalid_remote_cursor.json()["error"]["code"] == (
                    "invalid_request"
                )
            finally:
                upstream_route_store.close()
                upstream_task_store.close()
                downstream_route_store.close()
                downstream_task_store.close()

    asyncio.run(scenario())
