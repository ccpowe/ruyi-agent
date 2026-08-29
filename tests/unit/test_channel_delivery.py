from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from ruyi_agent.channels.gateway_client import GatewayClientError
from ruyi_agent.channels.gateway_dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.presentation import (
    ChannelDeliveryCoordinator,
    ChannelDeliveryHooks,
)
from ruyi_agent.channels.task_watch import TaskWatchManager, WatchRetryPolicy
from ruyi_agent.storage.channel_delivery_store import (
    ChannelDeliveryStore,
    delivery_intent_id,
)
from tests.unit._feishu_adapter_support import (
    FakeFeishuClient,
    FakeGatewayClient as FeishuGateway,
)
from tests.unit._telegram_adapter_support import (
    FakeGatewayClient as TelegramGateway,
    FakeTelegramClient,
)
from ruyi_agent.channels.feishu.adapter import FeishuAdapter
from ruyi_agent.channels.telegram.adapter import TelegramAdapter


class SequenceGateway:
    def __init__(self, items: list[GatewayTask | Exception]) -> None:
        self.items = list(items)
        self.calls = 0

    async def get_task(self, *, task_id: str) -> GatewayTask:
        del task_id
        self.calls += 1
        item = self.items.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def gateway_task(
    status: str,
    *,
    review_id: str | None = None,
    artifacts: bool = False,
) -> GatewayTask:
    payload: dict[str, Any] = {
        "task_id": "task-1",
        "status": status,
        "run_count": 1,
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
                "run_count": 1,
            }
        ]
    return GatewayTask.model_validate(payload)


def hooks(events: list[str]) -> ChannelDeliveryHooks:
    async def record(name: str) -> None:
        events.append(name)

    return ChannelDeliveryHooks(
        send_review=lambda _: record("review"),
        send_terminal_message=lambda _: record("terminal"),
        send_artifact=lambda _, artifact: record(f"artifact:{artifact.artifact_id}"),
    )


@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_restart_recovers_exhausted_watch_for_each_platform(
    tmp_path: Path,
    platform: str,
) -> None:
    async def scenario() -> None:
        db_path = str(tmp_path / f"{platform}.sqlite3")
        first_store = ChannelDeliveryStore(db_path)
        first_gateway = SequenceGateway(
            [GatewayClientError(status_code=503, code="down", message="temporary")]
        )
        first_manager = TaskWatchManager(
            gateway_client=first_gateway,
            poll_interval=0,
            retry_policy=WatchRetryPolicy(max_attempts=0),
        )
        first = ChannelDeliveryCoordinator(
            task_watch=first_manager,
            store=first_store,
            platform=platform,
        )
        first.ensure_delivery(
            session_key=f"{platform}:session-1",
            chat_id="chat-1",
            task_id="task-1",
            run_count=1,
            hooks=hooks([]),
        )
        await first.wait()
        intent_id = delivery_intent_id(
            platform=platform,
            session_key=f"{platform}:session-1",
            task_id="task-1",
            run_count=1,
        )
        failed = first_store.get(intent_id)
        assert failed is not None and failed.state == "error"
        await first.close()
        first_store.close()

        events: list[str] = []
        second_store = ChannelDeliveryStore(db_path)
        second_manager = TaskWatchManager(
            gateway_client=SequenceGateway([gateway_task("completed")]),
            poll_interval=0,
            terminal_review_grace_checks=0,
        )
        second = ChannelDeliveryCoordinator(
            task_watch=second_manager,
            store=second_store,
            platform=platform,
        )
        assert await second.recover(lambda _: hooks(events)) == 1
        await second.wait()

        delivered = second_store.get(intent_id)
        assert delivered is not None and delivered.state == "delivered"
        assert events == ["terminal"]
        await second.close()
        second_store.close()

    asyncio.run(scenario())


def test_partial_artifact_failure_retries_without_duplicate_message(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / "delivery.sqlite3"))
        task = gateway_task("completed", artifacts=True)
        manager = TaskWatchManager(
            gateway_client=SequenceGateway([task, task]),
            poll_interval=0,
            terminal_review_grace_checks=0,
            retry_policy=WatchRetryPolicy(max_attempts=2, base_delay=0),
        )
        coordinator = ChannelDeliveryCoordinator(
            task_watch=manager,
            store=store,
            platform="telegram",
        )
        messages = 0
        artifact_attempts = 0

        async def send_message(_: GatewayTask) -> None:
            nonlocal messages
            messages += 1

        async def send_artifact(
            _: GatewayTask, artifact: GatewayPublishedArtifact
        ) -> None:
            nonlocal artifact_attempts
            assert artifact.artifact_id == "artifact-1"
            artifact_attempts += 1
            if artifact_attempts == 1:
                raise RuntimeError("platform upload interrupted")

        delivery_hooks = ChannelDeliveryHooks(
            send_review=lambda _: asyncio.sleep(0),
            send_terminal_message=send_message,
            send_artifact=send_artifact,
        )
        assert coordinator.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=delivery_hooks,
        )
        await coordinator.wait()

        assert messages == 1
        assert artifact_attempts == 2
        intent_id = delivery_intent_id(
            platform="telegram",
            session_key="telegram:session-1",
            task_id="task-1",
            run_count=1,
        )
        delivered = store.get(intent_id)
        assert delivered is not None and delivered.state == "delivered"
        assert store.step_delivered(
            intent_id, step_key="terminal:1:message"
        )
        assert store.step_delivered(
            intent_id, step_key="terminal:1:artifact:artifact-1"
        )
        assert not coordinator.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=delivery_hooks,
        )
        assert messages == 1
        await coordinator.close()
        store.close()

    asyncio.run(scenario())


def test_review_delivery_uses_deterministic_durable_step(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / "review.sqlite3"))
        manager = TaskWatchManager(
            gateway_client=SequenceGateway(
                [gateway_task("waiting_for_human", review_id="review-1")]
            ),
            poll_interval=0,
        )
        coordinator = ChannelDeliveryCoordinator(
            task_watch=manager,
            store=store,
            platform="feishu",
        )
        events: list[str] = []
        coordinator.ensure_delivery(
            session_key="feishu:session-1",
            chat_id="chat-1",
            task_id="task-1",
            run_count=1,
            hooks=hooks(events),
        )
        await coordinator.wait()

        intent_id = delivery_intent_id(
            platform="feishu",
            session_key="feishu:session-1",
            task_id="task-1",
            run_count=1,
        )
        assert store.step_delivered(intent_id, step_key="review:review-1:message")
        assert store.get(intent_id).state == "delivered"  # type: ignore[union-attr]
        assert events == ["review"]
        assert not coordinator.ensure_delivery(
            session_key="feishu:session-1",
            chat_id="chat-1",
            task_id="task-1",
            run_count=1,
            hooks=hooks(events),
        )
        assert events == ["review"]
        await coordinator.close()
        store.close()

    asyncio.run(scenario())


def test_delivery_claim_is_fenced_across_processes(tmp_path: Path) -> None:
    now = [100.0]
    path = str(tmp_path / "claims.sqlite3")
    first = ChannelDeliveryStore(path, clock=lambda: now[0])
    second = ChannelDeliveryStore(path, clock=lambda: now[0])
    intent = first.ensure_watch(
        platform="telegram",
        session_key="telegram:session-1",
        chat_id="100",
        task_id="task-1",
        run_count=1,
    )

    first_token = first.claim(intent.intent_id, owner="process-1", lease_seconds=10)
    assert first_token is not None
    assert second.claim(intent.intent_id, owner="process-2", lease_seconds=10) is None

    now[0] = 111.0
    second_token = second.claim(intent.intent_id, owner="process-2", lease_seconds=10)
    assert second_token is not None
    assert not first.mark_step_delivered(
        intent.intent_id,
        token=first_token,
        step_key="terminal:1:message",
    )
    assert second.mark_step_delivered(
        intent.intent_id,
        token=second_token,
        step_key="terminal:1:message",
    )
    first.close()
    second.close()


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
