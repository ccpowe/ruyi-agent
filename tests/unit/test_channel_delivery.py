from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from ruyi_agent.channels.gateway_client import GatewayClientError
from ruyi_agent.gateway_protocol.dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.presentation import (
    ChannelDeliveryCoordinator,
    ChannelDeliveryHooks,
    ChannelDeliveryRedrivePolicy,
)
from ruyi_agent.channels.task_watch import TaskWatchManager, WatchRetryPolicy
from ruyi_agent.storage.channel_delivery_store import (
    ChannelDeliveryStore,
    delivery_intent_id,
    parse_channel_delivery_kind,
    parse_channel_delivery_state,
)


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


@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_exhausted_query_redrives_in_same_process(
    tmp_path: Path,
    platform: str,
) -> None:
    async def scenario() -> None:
        store = ChannelDeliveryStore(str(tmp_path / f"{platform}-redrive.sqlite3"))
        gateway = SequenceGateway(
            [
                GatewayClientError(status_code=503, code="down", message="temporary"),
                gateway_task("completed"),
            ]
        )
        manager = TaskWatchManager(
            gateway_client=gateway,
            poll_interval=0,
            terminal_review_grace_checks=0,
            retry_policy=WatchRetryPolicy(max_attempts=0),
        )
        coordinator = ChannelDeliveryCoordinator(
            task_watch=manager,
            store=store,
            platform=platform,
            redrive_policy=ChannelDeliveryRedrivePolicy(
                base_delay=0.02,
                max_delay=0.02,
                jitter_ratio=0,
                scan_interval=0.01,
            ),
        )
        events: list[str] = []
        assert coordinator.ensure_delivery(
            session_key=f"{platform}:session-1",
            chat_id="chat-1",
            task_id="task-1",
            run_count=1,
            hooks=hooks(events),
        )
        await coordinator.wait()
        intent_id = delivery_intent_id(
            platform=platform,
            session_key=f"{platform}:session-1",
            task_id="task-1",
            run_count=1,
        )
        for _ in range(100):
            current = store.get(intent_id)
            if current is not None and current.state == "delivered":
                break
            await asyncio.sleep(0.005)
        delivered = store.get(intent_id)
        assert delivered is not None and delivered.state == "delivered"
        assert delivered.redrive_count == 0
        assert gateway.calls == 2
        assert events == ["terminal"]
        await coordinator.close()
        store.close()

    asyncio.run(scenario())


def test_channel_delivery_contract_and_legacy_migration(tmp_path: Path) -> None:
    for value in (" watch", "WATCH", ""):
        with pytest.raises(ValueError):
            parse_channel_delivery_kind(value)
    for value in (" delivered ", "DELIVERED", None):
        with pytest.raises(ValueError):
            parse_channel_delivery_state(value)

    legacy_columns = (
        "intent_id",
        "platform",
        "session_key",
        "chat_id",
        "task_id",
        "run_count",
        "delivery_kind",
        "review_id",
        "state",
        "cursor",
        "attempt_count",
        "next_attempt_at",
        "last_error",
        "lease_owner",
        "lease_token",
        "lease_until",
        "fence",
        "created_at",
        "updated_at",
    )
    legacy_schema = """
        CREATE TABLE channel_delivery_intents (
            intent_id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            session_key TEXT NOT NULL,
            chat_id TEXT NOT NULL,
            task_id TEXT NOT NULL,
            run_count INTEGER NOT NULL CHECK (run_count >= 0),
            delivery_kind TEXT NOT NULL,
            review_id TEXT,
            state TEXT NOT NULL,
            cursor INTEGER NOT NULL DEFAULT 0,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at REAL,
            last_error TEXT,
            lease_owner TEXT,
            lease_token TEXT,
            lease_until REAL,
            fence INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL,
            UNIQUE(platform, session_key, task_id, run_count)
        );
        CREATE INDEX idx_channel_delivery_recovery
            ON channel_delivery_intents(platform, state, next_attempt_at);
        CREATE TABLE channel_delivery_steps (
            intent_id TEXT NOT NULL,
            step_key TEXT NOT NULL,
            delivered_at REAL NOT NULL,
            PRIMARY KEY(intent_id, step_key),
            FOREIGN KEY(intent_id) REFERENCES channel_delivery_intents(intent_id)
                ON DELETE CASCADE
        );
    """

    def create_legacy(
        path: Path,
        *,
        state: str,
        kind: str = "watch",
    ) -> tuple[tuple[object, ...], ...]:
        connection = sqlite3.connect(path)
        connection.executescript(legacy_schema)
        values: tuple[object, ...] = (
            "legacy-1",
            "telegram",
            "telegram:session-1",
            "chat-1",
            "task-1",
            1,
            kind,
            None,
            state,
            7,
            3,
            None,
            "gateway unavailable",
            None,
            None,
            None,
            4,
            10.0,
            20.0,
        )
        connection.execute(
            f"INSERT INTO channel_delivery_intents ({', '.join(legacy_columns)}) "
            f"VALUES ({', '.join('?' for _ in legacy_columns)})",
            values,
        )
        connection.execute(
            "INSERT INTO channel_delivery_steps VALUES (?, ?, ?)",
            ("legacy-1", "review:r1:message", 19.0),
        )
        connection.commit()
        before = tuple(
            connection.execute(
                f"SELECT {', '.join(legacy_columns)} FROM channel_delivery_intents "
                "ORDER BY intent_id"
            ).fetchall()
        )
        connection.close()
        return before

    valid_path = tmp_path / "legacy-valid.sqlite3"
    before = create_legacy(valid_path, state="error")
    connection = sqlite3.connect(valid_path)
    all_states = (
        "watching",
        "retry_wait",
        "error",
        "delivering",
        "review_waiting",
        "terminal_grace",
        "delivered",
        "superseded",
    )
    for kind_index, kind in enumerate(("watch", "review", "terminal")):
        for state_index, state in enumerate(all_states):
            if kind == "watch" and state == "error":
                continue
            run_count = 100 + kind_index * len(all_states) + state_index
            intent_id = f"legacy-{kind}-{state}"
            values = (
                intent_id,
                "telegram",
                f"telegram:session-{run_count}",
                f"chat-{run_count}",
                f"task-{run_count}",
                run_count,
                kind,
                None,
                state,
                run_count,
                2,
                None,
                None,
                None,
                None,
                None,
                run_count,
                1000.0 + run_count,
                2000.0 + run_count,
            )
            connection.execute(
                f"INSERT INTO channel_delivery_intents ({', '.join(legacy_columns)}) "
                f"VALUES ({', '.join('?' for _ in legacy_columns)})",
                values,
            )
            connection.execute(
                "INSERT INTO channel_delivery_steps VALUES (?, ?, ?)",
                (intent_id, f"step:{intent_id}", 3000.0 + run_count),
            )
    connection.commit()
    before = tuple(
        connection.execute(
            f"SELECT {', '.join(legacy_columns)} FROM channel_delivery_intents "
            "ORDER BY intent_id"
        ).fetchall()
    )
    connection.close()
    store = ChannelDeliveryStore(str(valid_path))
    intent = store.get("legacy-1")
    assert intent is not None
    assert intent.delivery_kind == "watch"
    assert intent.state == "error"
    assert intent.next_attempt_at is None
    assert intent.redrive_count == 0
    assert store.step_delivered("legacy-1", step_key="review:r1:message")
    store.close()

    connection = sqlite3.connect(valid_path)
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute(
            "UPDATE channel_delivery_intents SET delivery_kind = 'invalid'"
        )
    connection.rollback()
    with pytest.raises(sqlite3.IntegrityError):
        connection.execute("UPDATE channel_delivery_intents SET state = 'invalid'")
    connection.rollback()
    migrated = tuple(
        connection.execute(
            f"SELECT {', '.join(legacy_columns)} FROM channel_delivery_intents "
            "ORDER BY intent_id"
        ).fetchall()
    )
    assert migrated == before
    table_info = connection.execute(
        "PRAGMA table_info(channel_delivery_intents)"
    ).fetchall()
    assert [row[1] for row in table_info] == [
        *legacy_columns[:11],
        "redrive_count",
        *legacy_columns[11:],
    ]
    assert all(
        row[3] == 1
        for row in table_info
        if row[1] in {"delivery_kind", "state", "redrive_count"}
    )
    table_sql = connection.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'channel_delivery_intents'"
    ).fetchone()[0]
    assert all(
        value in table_sql for value in all_states + ("watch", "review", "terminal")
    )
    assert connection.execute("SELECT COUNT(*) FROM channel_delivery_steps").fetchone()[
        0
    ] == len(before)
    assert {
        row[2]
        for row in connection.execute("PRAGMA foreign_key_list(channel_delivery_steps)")
    } == {"channel_delivery_intents"}
    connection.close()
    idempotent = ChannelDeliveryStore(str(valid_path))
    idempotent.close()
    connection = sqlite3.connect(valid_path)
    assert (
        tuple(
            connection.execute(
                f"SELECT {', '.join(legacy_columns)} FROM channel_delivery_intents "
                "ORDER BY intent_id"
            ).fetchall()
        )
        == before
    )
    connection.close()

    for suffix, kind, state in (
        ("state", "watch", "not-a-state"),
        ("kind", "not-a-kind", "error"),
    ):
        invalid_path = tmp_path / f"legacy-invalid-{suffix}.sqlite3"
        invalid_before = create_legacy(
            invalid_path,
            kind=kind,
            state=state,
        )
        with pytest.raises(ValueError):
            ChannelDeliveryStore(str(invalid_path))
        connection = sqlite3.connect(invalid_path)
        assert (
            tuple(
                connection.execute(
                    f"SELECT {', '.join(legacy_columns)} "
                    "FROM channel_delivery_intents ORDER BY intent_id"
                ).fetchall()
            )
            == invalid_before
        )
        assert connection.execute(
            "SELECT intent_id, step_key FROM channel_delivery_steps"
        ).fetchall() == [("legacy-1", "review:r1:message")]
        connection.close()


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
        assert store.step_delivered(intent_id, step_key="terminal:1:message")
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
        assert second_store.step_delivered(intent_id, step_key="terminal:1:message")
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


def test_accepted_then_blocked_send_may_repeat_after_fenced_lease_loss(
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
            effects.append("first-accepted")
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

        assert effects == [
            "first-accepted",
            "second-message",
            "second-artifact",
        ]
        intent_id = delivery_intent_id(
            platform="telegram",
            session_key="telegram:session-1",
            task_id="task-1",
            run_count=1,
        )
        assert second_store.step_delivered(intent_id, step_key="terminal:1:message")
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
            redrive_policy=ChannelDeliveryRedrivePolicy(scan_interval=60),
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
        assert not any(
            task.get_name().startswith("channel-delivery-reconciler:")
            for task in asyncio.all_tasks()
            if not task.done()
        )
        store.close()

    asyncio.run(scenario())
