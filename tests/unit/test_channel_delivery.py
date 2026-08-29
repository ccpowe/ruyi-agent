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
from ruyi_agent.storage.channel_session_store import ChannelSessionStore
from tests.unit._feishu_adapter_support import (
    FakeFeishuClient,
    FakeGatewayClient as FeishuGateway,
    build_message as build_feishu_message,
)
from tests.unit._telegram_adapter_support import (
    FakeGatewayClient as TelegramGateway,
    FakeTelegramClient,
    build_message as build_telegram_message,
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
        assert store.get(intent_id).state == "review_waiting"  # type: ignore[union-attr]
        assert events == ["review"]
        gateway = manager._gateway_client
        assert isinstance(gateway, SequenceGateway)
        gateway.items.append(gateway_task("waiting_for_human", review_id="review-2"))
        assert coordinator.ensure_delivery(
            session_key="feishu:session-1",
            chat_id="chat-1",
            task_id="task-1",
            run_count=1,
            hooks=hooks(events),
        )
        await coordinator.wait()
        assert events == ["review", "review"]
        assert store.step_delivered(intent_id, step_key="review:review-2:message")
        assert store.get(intent_id).state == "review_waiting"  # type: ignore[union-attr]
        await coordinator.close()
        store.close()

    asyncio.run(scenario())


def test_concurrent_reopen_request_does_not_swallow_next_review(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / "concurrent-reviews.sqlite3"))
        events: list[str] = []
        coordinator = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway(
                    [
                        gateway_task("waiting_for_human", review_id="review-1"),
                        gateway_task("waiting_for_human", review_id="review-2"),
                    ]
                ),
                poll_interval=0,
            ),
            store=store,
            platform="feishu",
        )
        arguments = {
            "session_key": "feishu:session-1",
            "chat_id": "chat-1",
            "task_id": "task-1",
            "run_count": 1,
            "hooks": hooks(events),
        }
        assert coordinator.ensure_delivery(**arguments)
        assert not coordinator.ensure_delivery(**arguments)
        await coordinator.wait()

        intent_id = delivery_intent_id(
            platform="feishu",
            session_key="feishu:session-1",
            task_id="task-1",
            run_count=1,
        )
        assert events == ["review", "review"]
        assert store.step_delivered(intent_id, step_key="review:review-1:message")
        assert store.step_delivered(intent_id, step_key="review:review-2:message")
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


def test_restart_during_terminal_grace_skips_terminal_and_delivers_late_review(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = str(tmp_path / "grace.sqlite3")
        first_store = ChannelDeliveryStore(path)
        first_events: list[str] = []
        first = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway([gateway_task("completed")]),
                poll_interval=60,
                terminal_review_grace_checks=2,
            ),
            store=first_store,
            platform="telegram",
        )
        first.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=hooks(first_events),
        )
        intent_id = delivery_intent_id(
            platform="telegram",
            session_key="telegram:session-1",
            task_id="task-1",
            run_count=1,
        )
        for _ in range(20):
            current = first_store.get(intent_id)
            if current is not None and current.state == "terminal_grace":
                break
            await asyncio.sleep(0.001)
        assert first_store.get(intent_id).state == "terminal_grace"  # type: ignore[union-attr]
        assert first_events == ["terminal"]
        await first.close()
        first_store.close()

        second_store = ChannelDeliveryStore(path)
        second_events: list[str] = []
        second = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway(
                    [
                        gateway_task("completed"),
                        gateway_task("waiting_for_human", review_id="review-late"),
                    ]
                ),
                poll_interval=0,
                terminal_review_grace_checks=2,
            ),
            store=second_store,
            platform="telegram",
        )
        assert await second.recover(lambda _: hooks(second_events)) == 1
        await second.wait()

        assert second_events == ["review"]
        assert second_store.step_delivered(
            intent_id, step_key="terminal:1:message"
        )
        assert second_store.step_delivered(
            intent_id, step_key="review:review-late:message"
        )
        assert second_store.get(intent_id).state == "review_waiting"  # type: ignore[union-attr]
        await second.close()
        second_store.close()

    asyncio.run(scenario())


def test_lease_heartbeat_blocks_second_instance_during_slow_send(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = str(tmp_path / "heartbeat.sqlite3")
        first_store = ChannelDeliveryStore(path)
        second_store = ChannelDeliveryStore(path)
        send_started = asyncio.Event()
        release_send = asyncio.Event()
        effects: list[str] = []

        async def slow_send(_: GatewayTask) -> None:
            send_started.set()
            await release_send.wait()
            effects.append("first")

        first = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway([gateway_task("completed")]),
                poll_interval=0,
                terminal_review_grace_checks=0,
            ),
            store=first_store,
            platform="telegram",
            lease_seconds=0.06,
            lease_heartbeat_interval=0.01,
        )
        second = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway([gateway_task("completed")]),
                poll_interval=0,
                terminal_review_grace_checks=0,
            ),
            store=second_store,
            platform="telegram",
            lease_seconds=0.06,
            lease_heartbeat_interval=0.01,
        )
        first_hooks = ChannelDeliveryHooks(
            send_review=lambda _: asyncio.sleep(0),
            send_terminal_message=slow_send,
            send_artifact=lambda _task, _artifact: asyncio.sleep(0),
        )
        first.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=first_hooks,
        )
        await send_started.wait()
        await asyncio.sleep(0.09)
        assert not second.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=hooks(effects),
        )
        assert effects == []
        release_send.set()
        await first.wait()
        assert effects == ["first"]
        await first.close()
        await second.close()
        first_store.close()
        second_store.close()

    asyncio.run(scenario())


def test_fenced_lease_loss_cancels_effect_and_stops_later_steps(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        now = [100.0]
        path = str(tmp_path / "lease-loss.sqlite3")
        first_store = ChannelDeliveryStore(path, clock=lambda: now[0])
        second_store = ChannelDeliveryStore(path, clock=lambda: now[0])
        first_started = asyncio.Event()
        first_cancelled = asyncio.Event()
        effects: list[str] = []

        async def record(value: str) -> None:
            effects.append(value)

        async def blocked_send(_: GatewayTask) -> None:
            first_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                first_cancelled.set()
                raise

        first = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway(
                    [gateway_task("completed", artifacts=True)]
                ),
                poll_interval=0,
                terminal_review_grace_checks=0,
                retry_policy=WatchRetryPolicy(max_attempts=0),
            ),
            store=first_store,
            platform="telegram",
            lease_seconds=10,
            lease_heartbeat_interval=0.01,
        )
        second = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway(
                    [gateway_task("completed", artifacts=True)]
                ),
                poll_interval=0,
                terminal_review_grace_checks=0,
            ),
            store=second_store,
            platform="telegram",
            lease_seconds=10,
            lease_heartbeat_interval=0.01,
        )
        first.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=ChannelDeliveryHooks(
                send_review=lambda _: asyncio.sleep(0),
                send_terminal_message=blocked_send,
                send_artifact=lambda _task, _artifact: record("stale-artifact"),
            ),
        )
        await first_started.wait()
        now[0] = 111.0

        async def second_send(_: GatewayTask) -> None:
            await first_cancelled.wait()
            effects.append("second-message")

        assert second.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=ChannelDeliveryHooks(
                send_review=lambda _: asyncio.sleep(0),
                send_terminal_message=second_send,
                send_artifact=lambda _task, _artifact: record("second-artifact"),
            ),
        )
        await asyncio.wait_for(first_cancelled.wait(), timeout=1)
        await asyncio.gather(first.wait(), second.wait())

        assert effects == ["second-message", "second-artifact"]
        await first.close()
        await second.close()
        first_store.close()
        second_store.close()

    asyncio.run(scenario())


def test_coordinator_close_cancels_and_consumes_blocked_delivery(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / "close.sqlite3"))
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def blocked(_: GatewayTask) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        coordinator = ChannelDeliveryCoordinator(
            task_watch=TaskWatchManager(
                gateway_client=SequenceGateway([gateway_task("completed")]),
                poll_interval=0,
                terminal_review_grace_checks=0,
            ),
            store=store,
            platform="telegram",
            lease_seconds=1,
            lease_heartbeat_interval=0.01,
        )
        coordinator.ensure_delivery(
            session_key="telegram:session-1",
            chat_id="100",
            task_id="task-1",
            run_count=1,
            hooks=ChannelDeliveryHooks(
                send_review=lambda _: asyncio.sleep(0),
                send_terminal_message=blocked,
                send_artifact=lambda _task, _artifact: asyncio.sleep(0),
            ),
        )
        await started.wait()
        await coordinator.close()
        await coordinator.close()

        intent_id = delivery_intent_id(
            platform="telegram",
            session_key="telegram:session-1",
            task_id="task-1",
            run_count=1,
        )
        assert cancelled.is_set()
        assert store.get(intent_id).lease_token is None  # type: ignore[union-attr]
        store.close()

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
        hook_calls = 0

        def fail_second(intent):
            nonlocal hook_calls
            hook_calls += 1
            if hook_calls == 2:
                raise RuntimeError("recovery hook failed")
            return original_hooks(intent)

        adapter._recovery_hooks = fail_second  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="recovery hook failed"):
            await adapter.start()
        assert not adapter._started
        assert all(
            store.get(intent.intent_id).lease_token is None  # type: ignore[union-attr]
            for intent in intents
        )
        assert all(
            not adapter._delivery.is_active(task_id=f"task-{index}", run_count=1)
            for index in (1, 2)
        )

        adapter._recovery_hooks = original_hooks  # type: ignore[method-assign]
        assert await adapter.start() == 2
        await adapter.wait_for_watchers()
        assert len(transport.sent_messages) == 2
        await adapter.close()
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
