from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from ruyi_agent.gateway_protocol.dto import GatewayTask
from ruyi_agent.storage.channel_delivery_store import ChannelDeliveryStore
from ruyi_agent.storage.channel_session_store import ChannelSessionStore
from tests.unit._feishu_adapter_support import (
    FakeGatewayClient as FeishuGateway,
    FakeFeishuClient,
    build_message as build_feishu_message,
)
from tests.unit._telegram_adapter_support import (
    FakeGatewayClient as TelegramGateway,
    FakeTelegramClient,
    build_message as build_telegram_message,
)
from ruyi_agent.channels.feishu.adapter import FeishuAdapter
from ruyi_agent.channels.telegram.adapter import TelegramAdapter

def gateway_task(
    status: str,
    *,
    task_id: str = "task-1",
    run_count: int = 1,
    review_id: str | None = None,
    artifacts: bool = False,
) -> GatewayTask:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "status": status,
        "run_count": run_count,
        "last_result": "done",
    }
    if review_id is not None:
        payload["pending_review"] = {"review_id": review_id}
    if artifacts:
        payload["artifacts"] = [
            {
                "artifact_id": "artifact-1",
                "path": "/workspace/artifact.txt",
                "name": "artifact.txt",
                "content_type": "text/plain",
                "size": 4,
                "run_count": run_count,
            }
        ]
    return GatewayTask.model_validate(payload)

@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_adapter_start_recovers_durable_active_watch(
    tmp_path: Path,
    platform: str,
) -> None:
    async def scenario() -> None:
        path = str(tmp_path / f"{platform}-adapter.sqlite3")
        first_store = ChannelDeliveryStore(path)
        if platform == "telegram":
            first_gateway = TelegramGateway()
            first_gateway.tasks["task-1"] = gateway_task("running").to_payload()
            first_adapter = TelegramAdapter(
                gateway_client=first_gateway,
                telegram_client=FakeTelegramClient(),
                default_agent_name="main",
                delivery_store=first_store,
                task_poll_interval=60,
            )
            first_adapter._ensure_watcher(
                task_id="task-1",
                chat_id=100,
                run_count=1,
                session_key="telegram:session-1",
            )
        else:
            first_gateway = FeishuGateway()
            first_gateway.tasks["task-1"] = gateway_task("running").to_payload()
            first_adapter = FeishuAdapter(
                gateway_client=first_gateway,
                feishu_client=FakeFeishuClient(),
                default_agent_name="main",
                delivery_store=first_store,
                task_poll_interval=60,
            )
            first_adapter._ensure_watcher(
                task_id="task-1",
                chat_id="chat-1",
                run_count=1,
                session_key="feishu:session-1",
            )
        await asyncio.sleep(0)
        await first_adapter.close()
        first_store.close()

        second_store = ChannelDeliveryStore(path)
        if platform == "telegram":
            gateway = TelegramGateway()
            gateway.tasks["task-1"] = gateway_task("completed").to_payload()
            transport = FakeTelegramClient()
            second_adapter = TelegramAdapter(
                gateway_client=gateway,
                telegram_client=transport,
                default_agent_name="main",
                delivery_store=second_store,
                task_poll_interval=0,
                terminal_review_grace_checks=0,
            )
        else:
            gateway = FeishuGateway()
            gateway.tasks["task-1"] = gateway_task("completed").to_payload()
            transport = FakeFeishuClient()
            second_adapter = FeishuAdapter(
                gateway_client=gateway,
                feishu_client=transport,
                default_agent_name="main",
                delivery_store=second_store,
                task_poll_interval=0,
                terminal_review_grace_checks=0,
            )
        assert await second_adapter.start() == 1
        await second_adapter.wait_for_watchers()
        assert len(transport.sent_messages) == 1
        assert "done" in transport.sent_messages[0]["text"]
        await second_adapter.close()
        second_store.close()

    asyncio.run(scenario())

@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_adapter_start_failure_compensates_and_can_retry(
    tmp_path: Path,
    platform: str,
) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / f"{platform}-recover.sqlite3"))
        intents = [
            store.ensure_watch(
                platform=platform,
                session_key=f"{platform}:session-{index}",
                chat_id=str(index),
                task_id=f"task-{index}",
                run_count=1,
            )
            for index in (1, 2)
        ]
        if platform == "telegram":
            gateway = TelegramGateway()
            transport = FakeTelegramClient()
            gateway.tasks = {
                f"task-{index}": gateway_task(
                    "completed", task_id=f"task-{index}"
                ).to_payload()
                for index in (1, 2)
            }
            adapter: TelegramAdapter | FeishuAdapter = TelegramAdapter(
                gateway_client=gateway,
                telegram_client=transport,
                default_agent_name="main",
                delivery_store=store,
                task_poll_interval=0,
                terminal_review_grace_checks=0,
            )
        else:
            gateway = FeishuGateway()
            transport = FakeFeishuClient()
            gateway.tasks = {
                f"task-{index}": gateway_task(
                    "completed", task_id=f"task-{index}"
                ).to_payload()
                for index in (1, 2)
            }
            adapter = FeishuAdapter(
                gateway_client=gateway,
                feishu_client=transport,
                default_agent_name="main",
                delivery_store=store,
                task_poll_interval=0,
                terminal_review_grace_checks=0,
                ack_mode="off",
            )

        original_hooks = adapter._recovery_hooks
        original_list = store.alist_recoverable
        listing_started = asyncio.Event()
        allow_listing = asyncio.Event()
        hook_calls = 0

        async def gated_list(*, platform: str):
            listing_started.set()
            await allow_listing.wait()
            return await original_list(platform=platform)

        def fail_second(intent):
            nonlocal hook_calls
            hook_calls += 1
            if hook_calls == 2:
                raise RuntimeError("recovery hook failed")
            return original_hooks(intent)

        adapter._recovery_hooks = fail_second  # type: ignore[method-assign]
        store.alist_recoverable = gated_list  # type: ignore[method-assign]
        first_start = asyncio.create_task(adapter.start())
        await listing_started.wait()
        second_start = asyncio.create_task(adapter.start())
        await asyncio.sleep(0)
        allow_listing.set()
        outcomes = await asyncio.gather(
            first_start, second_start, return_exceptions=True
        )
        assert all(
            isinstance(outcome, RuntimeError) and str(outcome) == "recovery hook failed"
            for outcome in outcomes
        )
        assert hook_calls == 2
        assert not adapter._lifecycle.started
        assert all(
            store.get(intent.intent_id).lease_token is None  # type: ignore[union-attr]
            for intent in intents
        )
        assert all(
            not adapter._delivery.is_active(task_id=f"task-{index}", run_count=1)
            for index in (1, 2)
        )

        adapter._recovery_hooks = original_hooks  # type: ignore[method-assign]
        listing_started.clear()
        allow_listing.clear()
        first_retry = asyncio.create_task(adapter.start())
        await listing_started.wait()
        second_retry = asyncio.create_task(adapter.start())
        await asyncio.sleep(0)
        allow_listing.set()
        assert await asyncio.gather(first_retry, second_retry) == [2, 2]
        store.alist_recoverable = original_list  # type: ignore[method-assign]
        assert await adapter.start() == 0
        await adapter.wait_for_watchers()
        assert len(transport.sent_messages) == 2
        await adapter.close()
        await adapter.close()
        store.close()

    asyncio.run(scenario())

@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_adapter_close_wins_race_with_inflight_startup(
    tmp_path: Path,
    platform: str,
) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / f"{platform}-close.sqlite3"))
        intent = store.ensure_watch(
            platform=platform,
            session_key=f"{platform}:session-1",
            chat_id="1",
            task_id="task-1",
            run_count=1,
        )
        running = gateway_task("running").to_payload()
        if platform == "telegram":
            gateway = TelegramGateway()
            gateway.tasks["task-1"] = running
            adapter: TelegramAdapter | FeishuAdapter = TelegramAdapter(
                gateway_client=gateway,
                telegram_client=FakeTelegramClient(),
                default_agent_name="main",
                delivery_store=store,
                task_poll_interval=60,
            )
        else:
            gateway = FeishuGateway()
            gateway.tasks["task-1"] = running
            adapter = FeishuAdapter(
                gateway_client=gateway,
                feishu_client=FakeFeishuClient(),
                default_agent_name="main",
                delivery_store=store,
                task_poll_interval=60,
                ack_mode="off",
            )

        original_recover = adapter._delivery.recover
        recovery_started = asyncio.Event()

        async def blocked_after_recover(hooks_for):
            recovered = await original_recover(hooks_for)
            recovery_started.set()
            await asyncio.Event().wait()
            return recovered

        adapter._delivery.recover = blocked_after_recover  # type: ignore[method-assign]
        starting = asyncio.create_task(adapter.start())
        await recovery_started.wait()
        assert adapter._delivery.is_active(task_id="task-1", run_count=1)
        await adapter.close()
        with pytest.raises(RuntimeError, match="closed during startup"):
            await starting

        assert adapter._lifecycle.closed
        assert not adapter._lifecycle.started
        assert not adapter._delivery.is_active(task_id="task-1", run_count=1)
        assert store.get(intent.intent_id).lease_token is None  # type: ignore[union-attr]
        with pytest.raises(RuntimeError, match="is closed"):
            await adapter.start()
        await adapter.close()
        store.close()

    asyncio.run(scenario())

@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_settled_input_reuses_pending_durable_terminal_delivery_once(
    platform: str,
) -> None:
    async def scenario() -> None:
        if platform == "telegram":
            gateway = TelegramGateway()
            transport = FakeTelegramClient()
            sessions = ChannelSessionStore(":memory:")
            task = gateway_task("completed").to_payload()
            task["last_result"] = "old result"
            task["metadata"] = {
                "channel": "telegram",
                "chat_id": "100",
                "user_id": "200",
                "chat_type": "private",
                "channel_session_key": "agent:main:telegram:dm:100",
            }
            gateway.tasks["task-1"] = task
            gateway.list_items = [task]
            sessions.bind_session(
                session_key="agent:main:telegram:dm:100",
                platform="telegram",
                agent_name="main",
                current_task_id="task-1",
                chat_id="100",
                user_id="200",
            )
            adapter: TelegramAdapter | FeishuAdapter = TelegramAdapter(
                gateway_client=gateway,
                telegram_client=transport,
                default_agent_name="main",
                session_store=sessions,
                task_poll_interval=60,
                terminal_review_grace_checks=1,
            )
            adapter._ensure_watcher(
                task_id="task-1",
                chat_id=100,
                run_count=1,
                session_key="agent:main:telegram:dm:100",
            )
            await adapter.handle_message(build_telegram_message("follow up"))
        else:
            gateway = FeishuGateway()
            transport = FakeFeishuClient()
            sessions = ChannelSessionStore(":memory:")
            task = gateway_task("completed").to_payload()
            task["last_result"] = "old result"
            task["metadata"] = {
                "channel": "feishu",
                "chat_id": "chat-1",
                "user_id": "user-1",
                "chat_type": "p2p",
                "channel_session_key": "agent:main:feishu:dm:chat-1",
            }
            gateway.tasks["task-1"] = task
            sessions.bind_session(
                session_key="agent:main:feishu:dm:chat-1",
                platform="feishu",
                agent_name="main",
                current_task_id="task-1",
                chat_id="chat-1",
                user_id="user-1",
            )
            adapter = FeishuAdapter(
                gateway_client=gateway,
                feishu_client=transport,
                default_agent_name="main",
                session_store=sessions,
                task_poll_interval=60,
                terminal_review_grace_checks=1,
                ack_mode="off",
            )
            adapter._ensure_watcher(
                task_id="task-1",
                chat_id="chat-1",
                run_count=1,
                session_key="agent:main:feishu:dm:chat-1",
            )
            await adapter.handle_message(
                build_feishu_message(
                    "follow up", event_id="event-follow", message_id="message-follow"
                )
            )

        terminal_messages = [
            item for item in transport.sent_messages if "old result" in item["text"]
        ]
        assert len(terminal_messages) == 1
        assert gateway.sent
        await adapter.close()
        sessions.close()

    asyncio.run(scenario())

@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_deprecated_media_root_constructor_keyword_is_ignored(
    tmp_path: Path,
    platform: str,
) -> None:
    with pytest.warns(DeprecationWarning, match="media_root is deprecated"):
        if platform == "telegram":
            adapter: TelegramAdapter | FeishuAdapter = TelegramAdapter(
                gateway_client=TelegramGateway(),
                telegram_client=FakeTelegramClient(),
                default_agent_name="main",
                media_root=tmp_path,
            )
        else:
            adapter = FeishuAdapter(
                gateway_client=FeishuGateway(),
                feishu_client=FakeFeishuClient(),
                default_agent_name="main",
                media_root=tmp_path,
            )
    asyncio.run(adapter.close())
