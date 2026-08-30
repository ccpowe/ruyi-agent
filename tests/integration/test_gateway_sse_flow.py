from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

import httpx
from langchain_core.messages import AIMessage, AIMessageChunk
import pytest
import uvicorn

import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.gateway_protocol.sse import GatewayTaskEvent, iter_gateway_task_events
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.storage.task_store import TaskStore


class ControlledStreamingAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.emit_delta = asyncio.Event()
        self.finish = asyncio.Event()
        self.invoke_calls = 0

    async def astream(self, payload, *, config, stream_mode, version):
        del payload, config
        assert stream_mode == ["messages", "values"]
        assert version == "v2"
        self.started.set()
        await self.emit_delta.wait()
        yield {
            "type": "messages",
            "ns": (),
            "data": (
                AIMessageChunk(content="live token"),
                {
                    "provider": "hidden",
                    "langgraph_node": "model",
                    "langgraph_path": ("__pregel_pull", "model"),
                },
            ),
        }
        await self.finish.wait()
        yield {
            "type": "values",
            "data": {"messages": [AIMessage(content="final answer")]},
            "interrupts": (),
        }

    async def ainvoke(self, payload, *, config, version):
        del payload, config, version
        self.invoke_calls += 1
        raise AssertionError("stream-capable Agent must not be invoked twice")


class ControlledStreamingFactory:
    def __init__(self) -> None:
        self.created: list[ControlledStreamingAgent] = []

    def __call__(self, **kwargs):
        del kwargs
        agent = ControlledStreamingAgent()
        self.created.append(agent)
        return agent


def _spec(name: str) -> LocalWorkerSpec:
    return LocalWorkerSpec(
        name=name,
        description=f"{name} agent",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
    )


def _local_service(control: AgentControl, *, name: str) -> GatewayTaskModule:
    return GatewayTaskModule(
        main_agent_name=name,
        agent_configs={
            name: {
                "kind": "local",
                "public": True,
                "name": name,
                "description": f"{name} agent",
            }
        },
        control=control,
    )


@asynccontextmanager
async def _serve(app) -> AsyncIterator[str]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    host, port = listener.getsockname()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="error",
            lifespan="off",
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(500):
            if server.started:
                break
            if server_task.done():
                await server_task
            await asyncio.sleep(0.01)
        else:
            raise RuntimeError("Uvicorn did not start")
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        with suppress(TimeoutError):
            await asyncio.wait_for(server_task, timeout=5)
        listener.close()


async def _wait_for_agent(
    factory: ControlledStreamingFactory,
) -> ControlledStreamingAgent:
    for _ in range(500):
        if factory.created:
            agent = factory.created[0]
            await agent.started.wait()
            return agent
        await asyncio.sleep(0.01)
    raise RuntimeError("Streaming Agent did not start")


async def _wait_for_completed(
    client: httpx.AsyncClient,
    task_id: str,
) -> dict[str, object]:
    for _ in range(500):
        response = await client.get(f"/tasks/{task_id}")
        response.raise_for_status()
        payload = response.json()
        if payload["status"] == "completed":
            return payload
        await asyncio.sleep(0.01)
    raise RuntimeError("Task did not complete")


async def _next_event(
    events: AsyncIterator[GatewayTaskEvent],
) -> GatewayTaskEvent:
    return await asyncio.wait_for(anext(events), timeout=5)


def test_real_http_sse_disconnect_does_not_cancel_run_and_cursor_replays(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    async def scenario() -> None:
        factory = ControlledStreamingFactory()
        monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
        store = TaskStore(str(tmp_path / "local-tasks.sqlite"))
        control = AgentControl(
            {"main": _spec("main")},
            {},
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        app = create_gateway_app(
            service=_local_service(control, name="main"),
            bearer_token="secret",
        )
        agent: ControlledStreamingAgent | None = None
        try:
            async with _serve(app) as base_url:
                async with httpx.AsyncClient(
                    base_url=base_url,
                    headers={"Authorization": "Bearer secret"},
                    timeout=5,
                ) as client:
                    created = await client.post(
                        "/agents/main/tasks",
                        json={"input": {"content": "stream"}, "metadata": {}},
                    )
                    created.raise_for_status()
                    task = created.json()
                    task_id = task["task_id"]
                    run_count = task["run_count"]
                    agent = await _wait_for_agent(factory)

                    async with client.stream(
                        "GET",
                        f"/tasks/{task_id}/events",
                        params={"run_count": str(run_count)},
                    ) as response:
                        assert response.status_code == 200
                        events = iter_gateway_task_events(response.aiter_lines())
                        snapshot = await _next_event(events)
                        assert snapshot.event_type == "task.snapshot"
                        assert snapshot.event_id is not None
                        agent.emit_delta.set()
                        delta = await _next_event(events)
                        assert delta.event_type == "assistant.delta"
                        assert delta.data["content"] == "live token"
                        running = await client.get(f"/tasks/{task_id}")
                        assert running.json()["status"] == "running"
                        cursor = snapshot.event_id

                    # Closing only the HTTP subscription must leave graph execution alive.
                    still_running = await client.get(f"/tasks/{task_id}")
                    assert still_running.json()["status"] == "running"
                    agent.finish.set()
                    completed = await _wait_for_completed(client, task_id)
                    assert completed["last_result"] == "final answer"

                    async with client.stream(
                        "GET",
                        f"/tasks/{task_id}/events",
                        params={"run_count": str(run_count)},
                        headers={"Last-Event-ID": cursor},
                    ) as replay_response:
                        replay = iter_gateway_task_events(replay_response.aiter_lines())
                        terminal = await _next_event(replay)
                        ended = await _next_event(replay)
                        assert terminal.event_type == "task.completed"
                        assert terminal.data["last_result"] == "final answer"
                        assert ended.event_type == "stream.end"
                        assert ended.data["reason"] == "completed"
                    assert agent.invoke_calls == 0
        finally:
            if agent is not None:
                agent.emit_delta.set()
                agent.finish.set()
            await control.close()
            store.close()

    asyncio.run(scenario())


def test_real_two_gateway_sse_proxy_streams_and_replays(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    async def scenario() -> None:
        monkeypatch.setenv("DOWNSTREAM_GATEWAY_TOKEN", "downstream-secret")
        factory = ControlledStreamingFactory()
        monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
        downstream_store = TaskStore(str(tmp_path / "downstream-tasks.sqlite"))
        downstream_control = AgentControl(
            {"worker": _spec("worker")},
            {},
            checkpointer=object(),
            backend=object(),
            task_store=downstream_store,
        )
        downstream_app = create_gateway_app(
            service=_local_service(downstream_control, name="worker"),
            bearer_token="downstream-secret",
        )
        proxy_store: TaskStore | None = None
        proxy_control: AgentControl | None = None
        agent: ControlledStreamingAgent | None = None
        try:
            async with _serve(downstream_app) as downstream_url:
                remote_ref = RemoteRef(
                    name="remote_worker",
                    description="remote worker",
                    url=downstream_url,
                    remote_agent_name="worker",
                    auth={
                        "type": "bearer",
                        "token_env": "DOWNSTREAM_GATEWAY_TOKEN",
                    },
                )
                proxy_store = TaskStore(str(tmp_path / "proxy-tasks.sqlite"))
                proxy_control = AgentControl(
                    {},
                    {"remote_worker": remote_ref},
                    checkpointer=object(),
                    backend=object(),
                    task_store=proxy_store,
                )
                proxy_service = GatewayTaskModule(
                    main_agent_name="remote_worker",
                    agent_configs={
                        "remote_worker": {
                            "kind": "remote_ref",
                            "public": True,
                            "name": "remote_worker",
                            "description": "remote worker",
                        }
                    },
                    control=proxy_control,
                )
                proxy_app = create_gateway_app(
                    service=proxy_service,
                    bearer_token="proxy-secret",
                )
                async with _serve(proxy_app) as proxy_url:
                    async with httpx.AsyncClient(
                        base_url=proxy_url,
                        headers={"Authorization": "Bearer proxy-secret"},
                        timeout=5,
                    ) as client:
                        created = await client.post(
                            "/agents/remote_worker/tasks",
                            json={"input": {"content": "proxy stream"}, "metadata": {}},
                        )
                        created.raise_for_status()
                        task = created.json()
                        proxy_task_id = task["task_id"]
                        run_count = task["run_count"]
                        agent = await _wait_for_agent(factory)

                        async with client.stream(
                            "GET",
                            f"/tasks/{proxy_task_id}/events",
                            params={"run_count": str(run_count)},
                        ) as response:
                            assert response.status_code == 200
                            events = iter_gateway_task_events(response.aiter_lines())
                            snapshot = await _next_event(events)
                            assert snapshot.event_type == "task.snapshot"
                            assert snapshot.data["task_id"] == proxy_task_id
                            cursor = snapshot.event_id
                            assert cursor is not None
                            agent.emit_delta.set()
                            delta = await _next_event(events)
                            assert delta.event_type == "assistant.delta"
                            assert delta.data == {
                                "task_id": proxy_task_id,
                                "run_count": run_count,
                                "created_at": delta.data["created_at"],
                                "content": "live token",
                            }
                            agent.finish.set()
                            terminal = await _next_event(events)
                            ended = await _next_event(events)
                            assert terminal.event_type == "task.completed"
                            assert terminal.data["task_id"] == proxy_task_id
                            assert terminal.data["last_result"] == "final answer"
                            assert ended.data["reason"] == "completed"

                        async with client.stream(
                            "GET",
                            f"/tasks/{proxy_task_id}/events",
                            params={"run_count": str(run_count)},
                            headers={"Last-Event-ID": cursor},
                        ) as replay_response:
                            replay = iter_gateway_task_events(
                                replay_response.aiter_lines()
                            )
                            assert (await _next_event(replay)).event_type == (
                                "task.completed"
                            )
                            assert (await _next_event(replay)).data["reason"] == (
                                "completed"
                            )
        finally:
            if agent is not None:
                agent.emit_delta.set()
                agent.finish.set()
            if proxy_control is not None:
                await proxy_control.close()
            await downstream_control.close()
            if proxy_store is not None:
                proxy_store.close()
            downstream_store.close()

    asyncio.run(scenario())
