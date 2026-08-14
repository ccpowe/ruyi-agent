from __future__ import annotations

import asyncio
import httpx

import ruyi_agent.runtime.delegation.async_runtime as async_runtime
from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.channels.telegram.adapter import (
    TelegramAdapter,
    TelegramMessage,
)
from ruyi_agent.config.loader import LocalWorkerSpec
from ruyi_agent.storage.channel_session_store import ChannelSessionStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.task_store import TaskStore


class RecordingAgent:
    def __init__(self) -> None:
        self.inputs: list[str] = []

    async def ainvoke(self, payload, *, config, version):
        content = str(payload["messages"][0]["content"])
        self.inputs.append(content)
        await asyncio.sleep(0)
        return {"messages": [{"role": "assistant", "content": f"done: {content}"}]}


class RecordingTelegramClient:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send_message(self, **kwargs) -> None:
        self.messages.append(str(kwargs["text"]))


def test_telegram_channel_turn_reuses_gateway_task_through_real_http_and_sqlite(
    monkeypatch,
    tmp_path,
) -> None:
    agent = RecordingAgent()
    monkeypatch.setattr(
        async_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    route_store = GatewayRouteStore(str(tmp_path / "routes.sqlite"))
    session_store = ChannelSessionStore(str(tmp_path / "sessions.sqlite"))
    control = async_runtime.AgentControl(
        {
            "main": LocalWorkerSpec(
                name="main",
                description="main",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=[],
                skills=[],
            )
        },
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
    )
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs={
            "main": {
                "kind": "local",
                "public": True,
                "name": "main",
                "description": "main",
            }
        },
        control=control,
        route_store=route_store,
    )
    app = create_gateway_app(service=service, bearer_token="test-token")
    gateway_client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="test-token",
        transport=httpx.ASGITransport(app=app),
    )
    telegram = RecordingTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway_client,
        telegram_client=telegram,  # type: ignore[arg-type]
        default_agent_name="main",
        session_store=session_store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
        message_parse_mode=None,
    )

    async def scenario() -> tuple[str, int]:
        first = TelegramMessage(
            update_id=1,
            chat_id=100,
            user_id=200,
            text="first",
            message_id=1,
            chat_type="private",
        )
        await adapter.handle_message(first)
        await adapter.wait_for_watchers()
        session = session_store.get_session("agent:main:telegram:dm:100")
        assert session is not None
        task_id = str(session.current_task_id)

        second = TelegramMessage(
            update_id=2,
            chat_id=100,
            user_id=200,
            text="second",
            message_id=2,
            chat_type="private",
        )
        await adapter.handle_message(second)
        await adapter.wait_for_watchers()
        task = await gateway_client.get_task(task_id=task_id)
        return task_id, int(task["run_count"])

    try:
        task_id, run_count = asyncio.run(scenario())
        persisted = task_store.get_task(task_id)
        assert persisted is not None
        assert persisted.thread_id == task_id
        assert run_count == 2
        assert agent.inputs == ["first", "second"]
        assert any("done: first" in message for message in telegram.messages)
        assert any("done: second" in message for message in telegram.messages)
    finally:
        session_store.close()
        route_store.close()
        task_store.close()
