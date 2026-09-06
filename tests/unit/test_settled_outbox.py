from __future__ import annotations

import asyncio
import sqlite3
import threading
import uuid
from datetime import UTC, datetime

import httpx
import pytest

import ruyi_agent.runtime.agent_factory as agent_factory_module
import ruyi_agent.runtime.delegation.task_runtime as task_runtime_module
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.delegation.notifications import SettledRunNotifier
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import TaskRecord
from tests.support.async_subagent_runtime import (
    FakeAgent,
    FakeAgentFactory,
    build_specs,
    build_test_remote_refs,
    wait_for_task_state,
)


def _control(
    monkeypatch: pytest.MonkeyPatch,
    db_path: str,
    *,
    task_store: TaskStore | None = None,
    mailbox_store: MailboxStore | None = None,
    agent_factory: object | None = None,
) -> tuple[AgentControl, TaskStore, MailboxStore, AgentMailbox]:
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        agent_factory if agent_factory is not None else FakeAgentFactory(),
    )
    task_store = task_store or TaskStore(db_path)
    mailbox_store = mailbox_store or MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    control = AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
        task_store=task_store,
    )
    return control, task_store, mailbox_store, mailbox


def _notifier(
    task_store: TaskStore,
    mailbox: AgentMailbox,
    *,
    manager: TaskManager | None = None,
) -> SettledRunNotifier:
    return SettledRunNotifier(
        manager or TaskManager(task_store, settled_outbox_enabled=True),
        mailbox,
    )


def _record(
    task_id: str,
    *,
    state: str = "completed",
    run_count: int = 1,
    parent_task_id: str | None = None,
    parent_thread_id: str | None = "parent-thread",
    mailbox_suppressed: bool = False,
    mailbox_delivered: bool = False,
) -> TaskRecord:
    now = datetime.now(UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name="background_research",
        state=state,  # type: ignore[arg-type]
        thread_id=task_id,
        parent_task_id=parent_task_id,
        root_task_id=parent_task_id or task_id,
        depth=2 if parent_task_id else 1,
        created_at=now,
        updated_at=now,
        result="done" if state == "completed" else None,
        error="failed" if state in {"failed", "interrupted"} else None,
        run_count=run_count,
        parent_thread_id=parent_thread_id,
        mailbox_suppressed=mailbox_suppressed,
        mailbox_delivered=mailbox_delivered,
    )




@pytest.mark.parametrize(
    ("state", "transition"),
    [
        ("completed", lambda manager, task_id: manager.mark_completed(task_id, "done")),
        ("failed", lambda manager, task_id: manager.mark_failed(task_id, "boom")),
        ("cancelled", lambda manager, task_id: manager.mark_cancelled(task_id)),
        (
            "interrupted",
            lambda manager, task_id: manager.mark_interrupted(task_id, "restart"),
        ),
    ],
)
def test_every_settled_transition_commits_deterministic_outbox(
    tmp_path,
    state: str,
    transition,
) -> None:
    store = TaskStore(str(tmp_path / f"{state}.sqlite"))
    manager = TaskManager(store, settled_outbox_enabled=True)

    async def scenario() -> None:
        record = manager.create_task_record(
            f"task-{state}",
            "background_research",
            parent_task_id=None,
            root_task_id=f"task-{state}",
            depth=1,
            parent_thread_id="parent-thread",
        )
        manager.mark_running(record.task_id, asyncio.current_task())  # type: ignore[arg-type]
        transition(manager, record.task_id)

    try:
        asyncio.run(scenario())
        stored = store.get_task(f"task-{state}")
        rows = store.list_settled_outbox()
    finally:
        store.close()

    assert stored is not None
    assert stored.state == state
    assert stored.run_count == 1
    assert len(rows) == 1
    assert rows[0]["outbox_key"] == f"settled:parent-thread:task-{state}:1"
    assert rows[0]["settled_status"] == state
    assert rows[0]["status"] == "pending"


def test_settlement_rolls_back_task_event_and_outbox_together(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    store = TaskStore(str(tmp_path / "atomic.sqlite"))
    manager = TaskManager(store, settled_outbox_enabled=True)

    async def scenario() -> tuple[TaskRecord, asyncio.Task[None]]:
        record = manager.create_task_record(
            "task-atomic",
            "background_research",
            parent_task_id=None,
            root_task_id="task-atomic",
            depth=1,
            parent_thread_id="parent-thread",
        )
        sleeper = asyncio.create_task(asyncio.sleep(60))
        manager.mark_running(record.task_id, sleeper)

        def fail_insert(*args: object, **kwargs: object) -> None:
            del args, kwargs
            raise RuntimeError("injected outbox failure")

        monkeypatch.setattr(store._settled_outbox, "insert_locked", fail_insert)
        with pytest.raises(RuntimeError, match="injected outbox failure"):
            manager.mark_completed(record.task_id, "must roll back")
        return manager.get_task(record.task_id), sleeper

    record, sleeper = asyncio.run(scenario())
    try:
        durable = store.get_task("task-atomic")
        events = store.list_task_events(
            task_id="task-atomic",
            run_count=1,
            after_event_id=0,
        )
        rows = store.list_settled_outbox()
    finally:
        store.close()

    assert record.state == "running"
    assert record.result is None
    assert sleeper.cancelled()
    assert durable is not None and durable.state == "running"
    assert [event.event_type for event in events] == ["task.running"]
    assert rows == []


def test_direct_mailbox_retry_does_not_poison_seen_key(
    tmp_path,
) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    mailbox = AgentMailbox(store)
    with store._lock:
        store._conn.execute(
            """
            CREATE TRIGGER fail_mailbox_insert
            BEFORE INSERT ON agent_mailbox_messages
            BEGIN
                SELECT RAISE(FAIL, 'injected mailbox write failure');
            END
            """
        )
        store._conn.commit()
    try:
        with pytest.raises(
            sqlite3.IntegrityError, match="injected mailbox write failure"
        ):
            mailbox.publish_settled(
                recipient_thread_id="parent-thread",
                child_task_id="child-1",
                child_agent_name="worker",
                run_count=1,
                status="completed",
                content="done",
            )
        with store._lock:
            store._conn.execute("DROP TRIGGER fail_mailbox_insert")
            store._conn.commit()
        retried = mailbox.publish_settled(
            recipient_thread_id="parent-thread",
            child_task_id="child-1",
            child_agent_name="worker",
            run_count=1,
            status="completed",
            content="done",
        )
        messages = mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )
    finally:
        store.close()

    assert retried is not None
    assert [(message.child_task_id, message.run_count) for message in messages] == [
        ("child-1", 1)
    ]


def test_first_outbox_write_failure_retries_and_does_not_block_webhook(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_path = str(tmp_path / "retry.sqlite")
    setup_store = TaskStore(db_path)
    parent = _record("parent-task", parent_thread_id=None)
    setup_store.insert_task(parent)
    setup_store.close()
    active_mailbox: list[AgentMailbox] = []

    class ParentWakeAgent(FakeAgent):
        def __init__(self) -> None:
            super().__init__()
            self.parent_messages: list = []

        async def ainvoke(self, payload, *, config, version):
            self.calls.append(
                {"payload": payload, "config": config, "version": version}
            )
            if payload == {"messages": []}:
                task_id = config["configurable"]["task_id"]
                thread_id = config["configurable"]["thread_id"]
                self.parent_messages.extend(
                    active_mailbox[0].claim(
                        recipient_task_id=task_id,
                        recipient_thread_id=thread_id,
                    )
                )
                active_mailbox[0].acknowledge(
                    [message.message_id for message in self.parent_messages]
                )
            return {"messages": [{"role": "assistant", "content": "done"}]}

    agent = ParentWakeAgent()

    def factory(**kwargs):
        del kwargs
        return agent

    control, task_store, mailbox_store, mailbox = _control(
        monkeypatch,
        db_path,
        agent_factory=factory,
    )
    active_mailbox.append(mailbox)
    original_publish = MailboxStore.publish_claimed_settled_outbox
    publish_attempts = 0
    webhook_calls: list[str] = []

    def flaky_publish(store: MailboxStore, intent) -> bool:
        nonlocal publish_attempts
        publish_attempts += 1
        if publish_attempts == 1:
            raise RuntimeError("mailbox unavailable")
        return original_publish(store, intent)

    class CapturingClient:
        def __init__(self, *, timeout: float) -> None:
            del timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb

        async def post(self, url, *, headers, json):
            del headers, json
            webhook_calls.append(url)

    monkeypatch.setattr(
        MailboxStore,
        "publish_claimed_settled_outbox",
        flaky_publish,
    )
    monkeypatch.setattr(httpx, "AsyncClient", CapturingClient)

    async def scenario() -> tuple[TaskRecord, list, list, list]:
        record = await control.spawn_task(
            "background_research",
            "run",
            task_id="child-task",
            parent_task_id=parent.task_id,
            parent_thread_id="parent-thread",
            webhook={"url": "https://client.example/settled"},
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        assert task_store.list_settled_outbox()[0]["status"] == "pending"
        await control.close()
        mailbox_store.close()
        task_store.close()

        reopened_tasks = TaskStore(db_path)
        reopened_mailbox_store = MailboxStore(db_path)
        reopened_mailbox = AgentMailbox(reopened_mailbox_store)
        active_mailbox[0] = reopened_mailbox
        reopened, _, _, _ = _control(
            monkeypatch,
            db_path,
            task_store=reopened_tasks,
            mailbox_store=reopened_mailbox_store,
            agent_factory=factory,
        )
        try:
            await reopened.wake_pending_mailbox_tasks()
            parent_after = await wait_for_task_state(
                reopened,
                parent.task_id,
                states={"completed"},
            )
            messages = list(agent.parent_messages)
            empty = reopened_mailbox.claim(
                recipient_task_id=parent.task_id,
                recipient_thread_id=parent.thread_id,
            )
            rows = reopened_tasks.list_settled_outbox()
            return parent_after, messages, empty, rows
        finally:
            await reopened.close()
            reopened_mailbox_store.close()
            reopened_tasks.close()

    parent_after, messages, empty, rows = asyncio.run(scenario())

    assert webhook_calls == ["https://client.example/settled"]
    assert publish_attempts == 2
    assert "mailbox unavailable" in caplog.text
    assert "dispatch settled:parent-thread:child-task:1" in caplog.text
    assert len(messages) == 1
    assert messages[0].child_task_id == "child-task"
    assert messages[0].run_count == 1
    assert parent_after.run_count == 2
    assert empty == []
    assert len(rows) == 1 and rows[0]["status"] == "delivered"


def test_webhook_failure_does_not_lose_mailbox_delivery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "webhook-failure.sqlite")
    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_path)

    class FailingClient:
        def __init__(self, *, timeout: float) -> None:
            del timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            del exc_type, exc, tb

        async def post(self, url, *, headers, json):
            del url, headers, json
            raise httpx.ConnectError("webhook unavailable")

    monkeypatch.setattr(httpx, "AsyncClient", FailingClient)

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "run",
            parent_thread_id="parent-thread",
            webhook={"url": "https://client.example/settled"},
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        return mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(scenario())
        row = task_store.list_settled_outbox()[0]
    finally:
        mailbox_store.close()
        task_store.close()

    assert len(messages) == 1
    assert messages[0].status == "completed"
    assert row["status"] == "delivered"


def test_restart_reconciles_pending_and_legacy_settlements(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "restart.sqlite")
    setup = TaskStore(db_path)
    setup.insert_task(_record("legacy-child"))
    setup.close()

    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_path)

    async def scenario() -> list:
        await control.wake_pending_mailbox_tasks()
        return mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(scenario())
        rows = task_store.list_settled_outbox()
    finally:
        mailbox_store.close()
        task_store.close()

    assert [(message.child_task_id, message.run_count) for message in messages] == [
        ("legacy-child", 1)
    ]
    assert rows[0]["status"] == "delivered"


def test_preexisting_mailbox_row_is_acked_without_duplicate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "ack-window.sqlite")
    task_store = TaskStore(db_path)
    manager = TaskManager(task_store, settled_outbox_enabled=True)

    async def settle() -> None:
        record = manager.create_task_record(
            "child-ack",
            "background_research",
            parent_task_id=None,
            root_task_id="child-ack",
            depth=1,
            parent_thread_id="parent-thread",
        )
        manager.mark_running(record.task_id, asyncio.current_task())  # type: ignore[arg-type]
        manager.mark_completed(record.task_id, "done")

    asyncio.run(settle())
    row = task_store.list_settled_outbox()[0]
    mailbox_store = MailboxStore(db_path)
    mailbox_store.publish(
        {
            "message_id": row["message_id"],
            "idempotency_key": row["outbox_key"],
            "recipient_task_id": row["recipient_task_id"],
            "recipient_thread_id": row["recipient_thread_id"],
            "sender_task_id": row["task_id"],
            "sender_agent_name": row["child_agent_name"],
            "child_task_id": row["task_id"],
            "child_agent_name": row["child_agent_name"],
            "child_run_count": row["run_count"],
            "settled_status": row["settled_status"],
            "content": row["content"],
            "trigger_run": True,
            "created_at": datetime.fromisoformat(str(row["created_at"])),
        }
    )
    control, _, _, mailbox = _control(
        monkeypatch,
        db_path,
        task_store=task_store,
        mailbox_store=mailbox_store,
    )

    async def reconcile() -> list:
        await _notifier(task_store, mailbox).reconcile()
        return mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(reconcile())
        outbox = task_store.list_settled_outbox()
    finally:
        mailbox_store.close()
        task_store.close()

    assert len(messages) == 1
    assert messages[0].message_id == row["message_id"]
    assert outbox[0]["status"] == "delivered"


def test_two_reconcilers_deliver_one_message(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "concurrent.sqlite")
    setup = TaskStore(db_path)
    setup.insert_task(_record("concurrent-child"))
    assert setup.reconcile_settled_outbox() == 1
    setup.close()

    first, first_tasks, first_mailboxes, first_mailbox = _control(monkeypatch, db_path)
    second, second_tasks, second_mailboxes, _ = _control(monkeypatch, db_path)
    start = threading.Event()

    def dispatch(tasks: TaskStore, mailbox: AgentMailbox) -> list[str]:
        start.wait(timeout=5)
        return asyncio.run(_notifier(tasks, mailbox).reconcile())

    async def scenario() -> list:
        first_dispatch = asyncio.create_task(
            asyncio.to_thread(dispatch, first_tasks, first_mailbox)
        )
        second_dispatch = asyncio.create_task(
            asyncio.to_thread(dispatch, second_tasks, first_mailbox)
        )
        await asyncio.sleep(0)
        start.set()
        await asyncio.gather(first_dispatch, second_dispatch)
        return first_mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(scenario())
        rows = first_tasks.list_settled_outbox()
    finally:
        second_mailboxes.close()
        second_tasks.close()
        first_mailboxes.close()
        first_tasks.close()

    assert [(message.child_task_id, message.run_count) for message in messages] == [
        ("concurrent-child", 1)
    ]
    assert len(rows) == 1 and rows[0]["status"] == "delivered"


def test_expired_claim_is_reclaimed_and_claim_arguments_are_validated(tmp_path) -> None:
    db_path = str(tmp_path / "expired-claim.sqlite")
    first = TaskStore(db_path)
    first.insert_task(_record("expired-child"))
    first.reconcile_settled_outbox()
    with pytest.raises(ValueError, match="limit must be positive"):
        first.claim_settled_outbox(limit=0)
    with pytest.raises(ValueError, match="lease must be positive"):
        first.claim_settled_outbox(lease_seconds=0)
    claimed = first.claim_settled_outbox(lease_seconds=0.001)
    assert len(claimed) == 1
    second = TaskStore(db_path)

    async def reclaim() -> list:
        await asyncio.sleep(0.01)
        return second.claim_settled_outbox()

    try:
        reclaimed = asyncio.run(reclaim())
    finally:
        second.close()
        first.close()

    assert len(reclaimed) == 1
    assert reclaimed[0].outbox_key == claimed[0].outbox_key
    assert reclaimed[0].claim_token != claimed[0].claim_token


def test_background_reconciler_starts_and_closes_cleanly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "background.sqlite")
    control, task_store, mailbox_store, _ = _control(monkeypatch, db_path)

    class ControlledSleep:
        def __init__(self) -> None:
            self.entered = {
                1: [asyncio.Event(), asyncio.Event()],
                5: [asyncio.Event(), asyncio.Event()],
            }
            self.released = {
                1: [asyncio.Event(), asyncio.Event()],
                5: [asyncio.Event(), asyncio.Event()],
            }
            self.counts = {1: 0, 5: 0}

        async def __call__(self, delay: int) -> None:
            assert delay in self.entered
            index = self.counts[delay]
            self.counts[delay] += 1
            if index < 2:
                self.entered[delay][index].set()
                await self.released[delay][index].wait()
            else:
                await asyncio.Event().wait()

    controlled_sleep = ControlledSleep()
    monkeypatch.setattr(task_runtime_module.asyncio, "sleep", controlled_sleep)

    async def scenario() -> None:
        control.start_mailbox_recovery()
        await controlled_sleep.entered[1][0].wait()
        await controlled_sleep.entered[5][0].wait()
        assert controlled_sleep.counts == {1: 1, 5: 1}
        controlled_sleep.released[1][0].set()
        await controlled_sleep.entered[1][1].wait()
        assert controlled_sleep.counts[5] == 1
        controlled_sleep.released[5][0].set()
        await controlled_sleep.entered[5][1].wait()
        assert controlled_sleep.counts[1] == 2
        controlled_sleep.released[1][1].set()
        controlled_sleep.released[5][1].set()
        await control.close()

    try:
        asyncio.run(scenario())
    finally:
        mailbox_store.close()
        task_store.close()


def test_parent_wake_retries_after_targeted_trigger_probe_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db_path = str(tmp_path / "retry.sqlite")
    setup_store = TaskStore(db_path)
    parent = _record("parent-task", parent_thread_id=None)
    setup_store.insert_task(parent)
    setup_store.close()
    active_mailbox: list[AgentMailbox] = []
    first_probe_failed = asyncio.Event()
    parent_message_claimed = asyncio.Event()
    parent_finish = asyncio.Event()

    class ParentWakeAgent(FakeAgent):
        def __init__(self) -> None:
            super().__init__()
            self.parent_messages: list = []

        async def ainvoke(self, payload, *, config, version):
            self.calls.append(
                {"payload": payload, "config": config, "version": version}
            )
            if payload == {"messages": []}:
                task_id = config["configurable"]["task_id"]
                thread_id = config["configurable"]["thread_id"]
                self.parent_messages.extend(
                    active_mailbox[0].claim(
                        recipient_task_id=task_id,
                        recipient_thread_id=thread_id,
                    )
                )
                active_mailbox[0].acknowledge(
                    [message.message_id for message in self.parent_messages]
                )
                parent_message_claimed.set()
                await parent_finish.wait()
            return {"messages": [{"role": "assistant", "content": "done"}]}

    agent = ParentWakeAgent()

    def factory(**kwargs):
        del kwargs
        return agent

    control, task_store, mailbox_store, mailbox = _control(
        monkeypatch,
        db_path,
        agent_factory=factory,
    )
    active_mailbox.append(mailbox)
    original_trigger_sequence = AgentMailbox.max_triggering_sequence
    probe_attempts = 0

    def fail_first_target_probe(
        store: AgentMailbox,
        recipient_task_id: str,
    ) -> int:
        nonlocal probe_attempts
        if recipient_task_id == parent.task_id and not parent_message_claimed.is_set():
            probe_attempts += 1
            if probe_attempts == 1:
                first_probe_failed.set()
                raise RuntimeError("target mailbox probe unavailable")
        return original_trigger_sequence(store, recipient_task_id)

    monkeypatch.setattr(
        AgentMailbox,
        "max_triggering_sequence",
        fail_first_target_probe,
    )

    async def scenario() -> tuple[TaskRecord, list, list, list]:
        await control.spawn_task(
            "background_research",
            "run",
            task_id="child-task",
            parent_task_id=parent.task_id,
            parent_thread_id="parent-thread",
        )
        await asyncio.wait_for(first_probe_failed.wait(), timeout=1)
        await control.close()
        mailbox_store.close()
        task_store.close()

        reopened_tasks = TaskStore(db_path)
        reopened_mailbox_store = MailboxStore(db_path)
        reopened_mailbox = AgentMailbox(reopened_mailbox_store)
        active_mailbox[0] = reopened_mailbox
        reopened, _, _, _ = _control(
            monkeypatch,
            db_path,
            task_store=reopened_tasks,
            mailbox_store=reopened_mailbox_store,
            agent_factory=factory,
        )
        try:
            await reopened.wake_pending_mailbox_tasks()
            await asyncio.wait_for(parent_message_claimed.wait(), timeout=1)
            parent_before_finish = reopened.get_task_record(parent.task_id)
            assert parent_before_finish.run_count == 2
            empty = reopened_mailbox.claim(
                recipient_task_id=parent.task_id,
                recipient_thread_id=parent.thread_id,
            )
            parent_finish.set()
            parent_after = await wait_for_task_state(
                reopened,
                parent.task_id,
                states={"completed"},
            )
            messages = list(agent.parent_messages)
            rows = reopened_tasks.list_settled_outbox()
            return parent_after, messages, empty, rows
        finally:
            await reopened.close()
            reopened_mailbox_store.close()
            reopened_tasks.close()

    parent_after, messages, empty, rows = asyncio.run(scenario())

    assert probe_attempts == 2
    assert "target mailbox probe unavailable" in caplog.text
    assert "Non-authoritative local run tail failed: child-task" in caplog.text
    assert len(messages) == 1
    assert messages[0].child_task_id == "child-task"
    assert messages[0].run_count == 1
    assert parent_after.run_count == 2
    assert empty == []
    assert len(rows) == 1 and rows[0]["status"] == "delivered"


def test_claim_then_suppress_fences_publish_and_reconciles_retraction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "suppress.sqlite")
    task_store = TaskStore(db_path)
    manager = TaskManager(task_store, settled_outbox_enabled=True)
    task_store.insert_task(_record("child-suppress"))
    manager.load_task_by_id("child-suppress")
    task_store.reconcile_settled_outbox()
    claimed = task_store.claim_settled_outbox()
    assert len(claimed) == 1
    manager.mark_mailbox_suppressed("child-suppress")

    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    assert mailbox.publish_claimed_settled_outbox(claimed[0]) is False
    control, _, _, _ = _control(
        monkeypatch,
        db_path,
        task_store=task_store,
        mailbox_store=mailbox_store,
    )

    async def reconcile() -> list:
        await _notifier(task_store, mailbox).reconcile()
        return mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(reconcile())
        row = task_store.list_settled_outbox()[0]
    finally:
        mailbox_store.close()
        task_store.close()

    assert messages == []
    assert row["status"] == "suppressed"
    assert row["retracted_at"] is not None


def test_wait_after_delivery_atomically_suppresses_and_retracts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "wait-retract.sqlite")
    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_path)

    async def scenario() -> tuple[str, list]:
        record = await control.spawn_task(
            "background_research",
            "run",
            parent_thread_id="parent-thread",
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        result = control.get_task_record(record.task_id)
        _notifier(task_store, mailbox).suppress_mailbox_delivery(result)
        messages = mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )
        return f"state={result.state}", messages

    try:
        result, messages = asyncio.run(scenario())
        task = task_store.get_task(task_store.list_settled_outbox()[0]["task_id"])
        outbox = task_store.list_settled_outbox()[0]
    finally:
        mailbox_store.close()
        task_store.close()

    assert "state=completed" in result
    assert messages == []
    assert task is not None and task.mailbox_suppressed is True
    assert outbox["status"] == "suppressed"
    assert outbox["retracted_at"] is not None


def test_new_run_gets_a_distinct_parent_message(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "runs.sqlite")
    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_path)

    async def scenario() -> tuple[list, list]:
        record = await control.spawn_task(
            "background_research",
            "first",
            parent_thread_id="parent-thread",
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        first = mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )
        mailbox.acknowledge([message.message_id for message in first])
        await control.send_task_input(record.task_id, "second")
        claimed_input = mailbox.claim(
            recipient_task_id=record.task_id,
            recipient_thread_id=record.thread_id,
        )
        mailbox.acknowledge([message.message_id for message in claimed_input])
        await wait_for_task_state(control, record.task_id, states={"completed"})
        second = mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )
        return first, second

    try:
        first, second = asyncio.run(scenario())
        rows = task_store.list_settled_outbox()
    finally:
        mailbox_store.close()
        task_store.close()

    assert [message.run_count for message in first] == [1]
    assert [message.run_count for message in second] == [2]
    assert len({str(row["message_id"]) for row in rows}) == 2


def test_shared_memory_uri_uses_the_same_atomic_outbox_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_uri = f"file:settled-{uuid.uuid4().hex}?mode=memory&cache=shared"
    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_uri)

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "run",
            parent_thread_id="parent-thread",
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        return mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(scenario())
    finally:
        mailbox_store.close()
        task_store.close()

    assert len(messages) == 1
    assert messages[0].status == "completed"


def test_no_parent_thread_or_suppressed_task_produces_no_intent(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "eligibility.sqlite"))
    try:
        store.insert_task(_record("no-parent", parent_thread_id=None))
        store.insert_task(_record("suppressed", mailbox_suppressed=True))
        store.insert_task(_record("already-delivered", mailbox_delivered=True))
        assert store.reconcile_settled_outbox() == 0
        assert store.list_settled_outbox() == []
    finally:
        store.close()


def test_task_store_without_mailbox_does_not_create_undeliverable_intent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    store = TaskStore(str(tmp_path / "no-mailbox.sqlite"))
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", FakeAgentFactory()
    )
    control = AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
    )

    async def scenario() -> str:
        record = await control.spawn_task(
            "background_research",
            "run",
            parent_thread_id="parent-thread",
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        return control.get_task_record(record.task_id).state

    try:
        state = asyncio.run(scenario())
        rows = store.list_settled_outbox()
    finally:
        store.close()

    assert state == "completed"
    assert rows == []


def test_remote_run_count_reset_and_settlement_create_outbox(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "remote.sqlite"))
    manager = TaskManager(store, settled_outbox_enabled=True)
    record = manager.create_task_record(
        "remote-child",
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id="remote-child",
        depth=1,
        route_kind="remote_ref",
        upstream_task_id="upstream-1",
        parent_thread_id="parent-thread",
    )
    record.mailbox_suppressed = True
    record.mailbox_delivered = True
    store.update_task(record)

    try:
        synced = manager.sync_remote_task(
            record.task_id,
            {
                "task_id": "upstream-1",
                "status": "completed",
                "last_result": "remote done",
                "error": None,
                "run_count": 1,
                "created_at": "2026-08-30T00:00:00Z",
                "updated_at": "2026-08-30T00:00:01Z",
            },
        )
        rows = store.list_settled_outbox()
    finally:
        store.close()

    assert synced.mailbox_suppressed is False
    assert synced.mailbox_delivered is False
    assert len(rows) == 1
    assert rows[0]["outbox_key"] == "settled:parent-thread:remote-child:1"


def test_remote_settlement_rolls_back_when_outbox_insert_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    store = TaskStore(str(tmp_path / "remote-rollback.sqlite"))
    manager = TaskManager(store, settled_outbox_enabled=True)
    record = manager.create_task_record(
        "remote-rollback",
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id="remote-rollback",
        depth=1,
        route_kind="remote_ref",
        upstream_task_id="upstream-rollback",
        parent_thread_id="parent-thread",
    )
    running_payload = {
        "task_id": "upstream-rollback",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "created_at": "2026-08-30T00:00:00Z",
        "updated_at": "2026-08-30T00:00:01Z",
    }
    manager.sync_remote_task(record.task_id, running_payload)

    def fail_insert(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("injected remote outbox failure")

    monkeypatch.setattr(store._settled_outbox, "insert_locked", fail_insert)
    with pytest.raises(RuntimeError, match="injected remote outbox failure"):
        manager.sync_remote_task(
            record.task_id,
            {
                **running_payload,
                "status": "completed",
                "last_result": "must roll back",
                "updated_at": "2026-08-30T00:00:02Z",
            },
        )

    try:
        durable = store.get_task(record.task_id)
        rows = store.list_settled_outbox()
    finally:
        store.close()

    assert record.state == "running"
    assert record.result is None
    assert durable is not None and durable.state == "running"
    assert rows == []






@pytest.mark.parametrize("mailbox_kind", ["different-file", "memory", "volatile"])
def test_agent_control_fails_fast_without_one_shared_sqlite_database(
    tmp_path,
    mailbox_kind: str,
) -> None:
    task_path = ":memory:" if mailbox_kind == "memory" else str(tmp_path / "tasks.db")
    task_store = TaskStore(task_path)
    mailbox_store = None
    if mailbox_kind == "different-file":
        mailbox_store = MailboxStore(str(tmp_path / "mailbox.db"))
        mailbox = AgentMailbox(mailbox_store)
    elif mailbox_kind == "memory":
        mailbox_store = MailboxStore(":memory:")
        mailbox = AgentMailbox(mailbox_store)
    else:
        mailbox = AgentMailbox()
    try:
        with pytest.raises(RuntimeError, match="share one SQLite database"):
            AgentControl(
                build_specs(),
                build_test_remote_refs(),
                checkpointer=object(),
                backend=object(),
                mailbox=mailbox,
                task_store=task_store,
            )
    finally:
        if mailbox_store is not None:
            mailbox_store.close()
        task_store.close()


def test_outbox_conflict_rolls_back_task_event_and_remote_duplicate_is_quiet(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "intent-conflict.sqlite")
    store = TaskStore(db_path)
    manager = TaskManager(store, settled_outbox_enabled=True)
    record = manager.create_task_record(
        "remote-conflict",
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id="remote-conflict",
        depth=1,
        route_kind="remote_ref",
        upstream_task_id="upstream-conflict",
        parent_thread_id="parent-thread",
    )
    payload = {
        "task_id": "upstream-conflict",
        "status": "completed",
        "last_result": "stable result",
        "error": None,
        "run_count": 1,
        "created_at": "2026-08-30T00:00:00Z",
        "updated_at": "2026-08-30T00:00:01Z",
    }
    manager.sync_remote_task(record.task_id, payload)
    events_before = store.list_task_events(
        task_id=record.task_id,
        run_count=1,
        after_event_id=0,
    )
    manager.sync_remote_task(
        record.task_id,
        {**payload, "updated_at": "2026-08-30T00:00:02Z"},
    )
    events_after_duplicate = store.list_task_events(
        task_id=record.task_id,
        run_count=1,
        after_event_id=0,
    )
    with pytest.raises(RuntimeError, match="outbox identity conflicts"):
        manager.sync_remote_task(
            record.task_id,
            {
                **payload,
                "status": "failed",
                "last_result": None,
                "error": "conflicting terminal refresh",
                "updated_at": "2026-08-30T00:00:03Z",
            },
        )
    durable = store.get_task(record.task_id)
    events_after_conflict = store.list_task_events(
        task_id=record.task_id,
        run_count=1,
        after_event_id=0,
    )
    outbox = store.list_settled_outbox()
    store.close()

    assert len(events_before) == len(events_after_duplicate)
    assert len(events_after_conflict) == len(events_before)
    assert durable is not None and durable.state == "completed"
    assert durable.result == "stable result"
    assert manager.get_task(record.task_id).state == "completed"
    assert len(outbox) == 1 and outbox[0]["settled_status"] == "completed"
