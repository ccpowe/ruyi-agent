from __future__ import annotations

import asyncio
import sqlite3
import threading
import uuid
from datetime import UTC, datetime

import httpx
import pytest

import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.delegation.notifications import SettledRunNotifier
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import TaskRecord
from tests.support.async_subagent_runtime import (
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
) -> tuple[AgentControl, TaskStore, MailboxStore, AgentMailbox]:
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        FakeAgentFactory(),
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


def _legacy_mailbox_values(
    record: TaskRecord,
    *,
    message_id: str,
    idempotency_key: str | None,
    run_count: int | None = None,
    content: str | None = None,
) -> dict[str, object]:
    return {
        "message_id": message_id,
        "idempotency_key": idempotency_key,
        "recipient_task_id": record.parent_task_id,
        "recipient_thread_id": record.parent_thread_id or "legacy-parent-thread",
        "sender_task_id": record.task_id,
        "sender_agent_name": record.agent_name,
        "child_task_id": record.task_id,
        "child_agent_name": record.agent_name,
        "child_run_count": run_count or record.run_count,
        "settled_status": record.state,
        "content": content or record.result or record.error or "done",
        "trigger_run": True,
        "created_at": record.updated_at,
    }


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
) -> None:
    db_path = str(tmp_path / "retry.sqlite")
    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_path)
    original_publish = mailbox_store.publish_claimed_settled_outbox
    publish_calls = 0
    webhook_calls: list[str] = []

    def flaky_publish(intent) -> bool:
        nonlocal publish_calls
        publish_calls += 1
        if publish_calls == 1:
            raise RuntimeError("mailbox unavailable")
        return original_publish(intent)

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

    monkeypatch.setattr(mailbox_store, "publish_claimed_settled_outbox", flaky_publish)
    monkeypatch.setattr(httpx, "AsyncClient", CapturingClient)

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "run",
            parent_thread_id="parent-thread",
            webhook={"url": "https://client.example/settled"},
        )
        await wait_for_task_state(control, record.task_id, states={"completed"})
        assert task_store.list_settled_outbox()[0]["status"] == "pending"
        await _notifier(task_store, mailbox).reconcile()
        return mailbox.claim(
            recipient_task_id=None,
            recipient_thread_id="parent-thread",
        )

    try:
        messages = asyncio.run(scenario())
    finally:
        mailbox_store.close()
        task_store.close()

    assert webhook_calls == ["https://client.example/settled"]
    assert publish_calls == 2
    assert [(message.child_task_id, message.run_count) for message in messages] == [
        (messages[0].child_task_id, 1)
    ]


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

    async def scenario() -> None:
        control.start_mailbox_recovery()
        await asyncio.sleep(0)
        await control.close()

    try:
        asyncio.run(scenario())
    finally:
        mailbox_store.close()
        task_store.close()


def test_parent_wake_failure_is_consumed_and_retried_from_durable_mailbox(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "parent-wake.sqlite")
    control, task_store, mailbox_store, mailbox = _control(monkeypatch, db_path)
    manager = TaskManager(task_store, settled_outbox_enabled=True)

    async def scenario() -> tuple[list, list[str]]:
        parent = manager.create_task_record(
            "parent-task",
            "background_research",
            parent_task_id=None,
            root_task_id="parent-task",
            depth=1,
        )
        child = manager.create_task_record(
            "child-task",
            "background_research",
            parent_task_id=parent.task_id,
            root_task_id=parent.task_id,
            depth=2,
            parent_thread_id=parent.thread_id,
        )
        manager.mark_running(child.task_id, asyncio.current_task())  # type: ignore[arg-type]
        manager.mark_completed(child.task_id, "done")
        notifier = _notifier(task_store, mailbox, manager=manager)
        assert notifier.publish_settled_message(child.task_id) == [parent.task_id]
        wake_ids = await notifier.reconcile()
        messages = mailbox.claim(
            recipient_task_id=parent.task_id,
            recipient_thread_id=parent.thread_id,
        )
        return messages, wake_ids

    try:
        messages, wake_ids = asyncio.run(scenario())
    finally:
        mailbox_store.close()
        task_store.close()

    assert wake_ids == ["parent-task"]
    assert [message.child_task_id for message in messages] == ["child-task"]


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


@pytest.mark.parametrize("mailbox_status", ["pending", "claimed", "delivered"])
def test_restart_adopts_random_id_legacy_mailbox_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    mailbox_status: str,
) -> None:
    db_path = str(tmp_path / f"legacy-{mailbox_status}.sqlite")
    setup_tasks = TaskStore(db_path)
    record = _record(f"legacy-{mailbox_status}")
    setup_tasks.insert_task(record)
    assert setup_tasks.reconcile_settled_outbox() == 1
    outbox = setup_tasks.list_settled_outbox()[0]
    setup_mailbox = MailboxStore(db_path)
    random_message_id = str(uuid.uuid4())
    assert setup_mailbox.publish(
        _legacy_mailbox_values(
            record,
            message_id=random_message_id,
            idempotency_key=str(outbox["outbox_key"]),
        )
    )
    with setup_mailbox._lock:
        if mailbox_status == "claimed":
            setup_mailbox._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET status = 'claimed', claimed_by = 'legacy-owner',
                    claim_token = 'legacy-token',
                    claimed_at = ?, claim_expires_at = ?
                WHERE message_id = ?
                """,
                (
                    record.updated_at.isoformat(),
                    "2099-01-01T00:00:00+00:00",
                    random_message_id,
                ),
            )
        elif mailbox_status == "delivered":
            setup_mailbox._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET status = 'delivered', delivered_at = ?
                WHERE message_id = ?
                """,
                (record.updated_at.isoformat(), random_message_id),
            )
        setup_mailbox._conn.commit()
    setup_mailbox.close()
    setup_tasks.close()

    _, tasks, mailboxes, mailbox = _control(monkeypatch, db_path)
    try:
        asyncio.run(_notifier(tasks, mailbox).reconcile())
        outbox_after = tasks.list_settled_outbox()[0]
        with mailboxes._lock:
            messages = mailboxes._conn.execute(
                """
                SELECT message_id, idempotency_key, status
                FROM agent_mailbox_messages
                """
            ).fetchall()
    finally:
        mailboxes.close()
        tasks.close()

    assert outbox_after["status"] == "delivered"
    assert [tuple(row) for row in messages] == [
        (random_message_id, outbox["outbox_key"], mailbox_status)
    ]


def test_restart_binds_unkeyed_legacy_row_and_rejects_payload_conflict(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "legacy-unkeyed.sqlite")
    tasks = TaskStore(db_path)
    record = _record("legacy-unkeyed")
    tasks.insert_task(record)
    tasks.reconcile_settled_outbox()
    mailbox = MailboxStore(db_path)
    random_message_id = str(uuid.uuid4())
    assert mailbox.publish(
        _legacy_mailbox_values(
            record,
            message_id=random_message_id,
            idempotency_key=None,
        )
    )
    claimed = tasks.claim_settled_outbox()
    assert mailbox.publish_claimed_settled_outbox(claimed[0]) is True
    with mailbox._lock:
        bound = mailbox._conn.execute(
            """
            SELECT message_id, idempotency_key
            FROM agent_mailbox_messages
            """
        ).fetchall()
    assert [tuple(row) for row in bound] == [(random_message_id, claimed[0].outbox_key)]
    mailbox.close()
    tasks.close()

    conflict_db = str(tmp_path / "legacy-conflict.sqlite")
    conflict_tasks = TaskStore(conflict_db)
    conflict_record = _record("legacy-conflict")
    conflict_tasks.insert_task(conflict_record)
    conflict_tasks.reconcile_settled_outbox()
    conflict_mailbox = MailboxStore(conflict_db)
    assert conflict_mailbox.publish(
        _legacy_mailbox_values(
            conflict_record,
            message_id=str(uuid.uuid4()),
            idempotency_key=None,
            content="different payload",
        )
    )
    conflict_intent = conflict_tasks.claim_settled_outbox()[0]
    with pytest.raises(RuntimeError, match="logical identity conflicts"):
        conflict_mailbox.publish_claimed_settled_outbox(conflict_intent)
    assert len(conflict_tasks.list_settled_outbox()) == 1
    conflict_mailbox.close()
    conflict_tasks.close()


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


def test_suppression_win_blocks_cross_connection_mailbox_claim(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "suppression-win.sqlite")
    tasks = TaskStore(db_path)
    manager = TaskManager(tasks, settled_outbox_enabled=True)
    record = _record(
        "suppression-win",
        parent_task_id="parent-task",
    )
    tasks.insert_task(record)
    tasks.reconcile_settled_outbox()
    publisher = MailboxStore(db_path)
    assert publisher.publish_claimed_settled_outbox(tasks.claim_settled_outbox()[0])
    claimant = MailboxStore(db_path)
    entered = threading.Event()
    release = threading.Event()
    original_retract = tasks._settled_outbox.retract_mailbox_for_task_run_locked

    def pause_retract(connection, *, task_id: str, run_count: int) -> bool:
        entered.set()
        assert release.wait(timeout=5)
        return original_retract(
            connection,
            task_id=task_id,
            run_count=run_count,
        )

    monkeypatch.setattr(
        tasks._settled_outbox,
        "retract_mailbox_for_task_run_locked",
        pause_retract,
    )
    suppress_thread = threading.Thread(
        target=manager.mark_mailbox_suppressed,
        args=(record.task_id,),
    )
    claimed_rows: list[list[dict[str, object]]] = []
    claim_thread = threading.Thread(
        target=lambda: claimed_rows.append(
            claimant.claim(
                recipient_task_id="parent-task",
                recipient_thread_id="parent-thread",
            )
        )
    )
    suppress_thread.start()
    assert entered.wait(timeout=5)
    claim_thread.start()
    release.set()
    suppress_thread.join(timeout=5)
    claim_thread.join(timeout=5)
    with publisher._lock:
        status = publisher._conn.execute(
            "SELECT status FROM agent_mailbox_messages"
        ).fetchone()[0]
    assert claimed_rows == [[]]
    assert status == "retracted"
    assert claimant.list_pending_trigger_recipient_task_ids() == []
    assert claimant.has_triggering("parent-task") is False
    claimant.close()
    publisher.close()
    tasks.close()


def test_mailbox_claim_win_is_retracted_by_later_suppression(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "claim-win.sqlite")
    tasks = TaskStore(db_path)
    manager = TaskManager(tasks, settled_outbox_enabled=True)
    record = _record("claim-win", parent_task_id="parent-task")
    tasks.insert_task(record)
    tasks.reconcile_settled_outbox()
    publisher = MailboxStore(db_path)
    assert publisher.publish_claimed_settled_outbox(tasks.claim_settled_outbox()[0])
    claimant = MailboxStore(db_path)
    entered = threading.Event()
    release = threading.Event()
    original_release = claimant._release_expired_claims_locked

    def pause_claim(now, *, commit: bool = True) -> None:
        entered.set()
        assert release.wait(timeout=5)
        original_release(now, commit=commit)

    monkeypatch.setattr(claimant, "_release_expired_claims_locked", pause_claim)
    claimed_rows: list[list[dict[str, object]]] = []
    claim_thread = threading.Thread(
        target=lambda: claimed_rows.append(
            claimant.claim(
                recipient_task_id="parent-task",
                recipient_thread_id="parent-thread",
            )
        )
    )
    suppress_thread = threading.Thread(
        target=manager.mark_mailbox_suppressed,
        args=(record.task_id,),
    )
    claim_thread.start()
    assert entered.wait(timeout=5)
    suppress_thread.start()
    release.set()
    claim_thread.join(timeout=5)
    suppress_thread.join(timeout=5)
    with publisher._lock:
        status = publisher._conn.execute(
            "SELECT status FROM agent_mailbox_messages"
        ).fetchone()[0]
    assert len(claimed_rows) == 1 and len(claimed_rows[0]) == 1
    assert status == "retracted"
    assert publisher.list_pending_trigger_recipient_task_ids() == []
    publisher.close()
    claimant.close()
    tasks.close()


def test_startup_migration_retracts_only_current_suppressed_legacy_run(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "legacy-suppressed.sqlite")
    tasks = TaskStore(db_path)
    record = _record(
        "suppressed-no-parent",
        run_count=2,
        parent_thread_id=None,
        mailbox_suppressed=True,
    )
    tasks.insert_task(record)
    mailbox = MailboxStore(db_path)
    message_ids: dict[str, str] = {}
    for label, run_count in (
        ("current-pending", 2),
        ("current-claimed", 2),
        ("current-delivered", 2),
        ("old-pending", 1),
    ):
        message_id = str(uuid.uuid4())
        message_ids[label] = message_id
        assert mailbox.publish(
            _legacy_mailbox_values(
                record,
                message_id=message_id,
                idempotency_key=f"legacy:{label}",
                run_count=run_count,
            )
        )
    with mailbox._lock:
        mailbox._conn.execute(
            """
            UPDATE agent_mailbox_messages
            SET status = 'claimed', claimed_by = 'old', claim_token = 'old',
                claimed_at = ?, claim_expires_at = ?
            WHERE message_id = ?
            """,
            (
                record.updated_at.isoformat(),
                "2099-01-01T00:00:00+00:00",
                message_ids["current-claimed"],
            ),
        )
        mailbox._conn.execute(
            """
            UPDATE agent_mailbox_messages
            SET status = 'delivered', delivered_at = ?
            WHERE message_id = ?
            """,
            (record.updated_at.isoformat(), message_ids["current-delivered"]),
        )
        mailbox._conn.commit()
    assert tasks.reconcile_settled_outbox() == 0
    with mailbox._lock:
        rows = mailbox._conn.execute(
            "SELECT message_id, status FROM agent_mailbox_messages"
        ).fetchall()
    statuses = {str(row[0]): str(row[1]) for row in rows}
    assert statuses[message_ids["current-pending"]] == "retracted"
    assert statuses[message_ids["current-claimed"]] == "retracted"
    assert statuses[message_ids["current-delivered"]] == "delivered"
    assert statuses[message_ids["old-pending"]] == "pending"
    assert tasks.list_settled_outbox() == []
    mailbox.close()
    tasks.close()


def test_legacy_migration_is_bounded_restartable_and_releases_writer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "large-migration.sqlite")
    migrating = TaskStore(db_path)
    for index in range(300):
        migrating.insert_task(_record(f"legacy-{index:04d}"))
    writer = TaskStore(db_path)
    entered = threading.Event()
    release = threading.Event()
    writer_started = threading.Event()
    writer_done = threading.Event()
    original_insert = migrating._settled_outbox.insert_locked
    first = True

    def pause_first_insert(connection, intent) -> bool:
        nonlocal first
        if first:
            first = False
            entered.set()
            assert release.wait(timeout=5)
        return original_insert(connection, intent)

    monkeypatch.setattr(
        migrating._settled_outbox,
        "insert_locked",
        pause_first_insert,
    )
    batches = []
    migration_thread = threading.Thread(
        target=lambda: batches.append(
            migrating.reconcile_settled_outbox_batch(limit=100)
        )
    )

    def write_concurrently() -> None:
        writer_started.set()
        writer.insert_task(_record("modern-writer", state="pending"))
        writer_done.set()

    writer_thread = threading.Thread(target=write_concurrently)
    migration_thread.start()
    assert entered.wait(timeout=5)
    writer_thread.start()
    assert writer_started.wait(timeout=5)
    assert writer_done.wait(timeout=0.05) is False
    release.set()
    migration_thread.join(timeout=5)
    writer_thread.join(timeout=5)
    assert writer_done.is_set()
    assert batches[0].inserted == 100
    assert batches[0].completed is False
    migrating.close()
    resumed = TaskStore(db_path)
    while True:
        batch = resumed.reconcile_settled_outbox_batch(limit=100)
        if batch.completed:
            break
    statements: list[str] = []
    with resumed._database.locked_connection() as connection:
        connection.set_trace_callback(statements.append)
    resumed.claim_settled_outbox(limit=1)
    with resumed._database.locked_connection() as connection:
        connection.set_trace_callback(None)
    normalized = " ".join(statements).lower()
    assert "from agent_tasks where task_id >" not in normalized
    assert resumed.reconcile_settled_outbox_batch().completed is True
    writer.close()
    resumed.close()


def test_pending_wake_query_excludes_more_than_one_cleanup_page_of_suppression(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "suppressed-page.sqlite")
    tasks = TaskStore(db_path)
    mailbox = MailboxStore(db_path)
    with tasks._database.transaction(immediate=True) as connection:
        for index in range(101):
            record = _record(
                f"suppressed-page-{index:03d}",
                run_count=2,
                parent_task_id=f"parent-{index:03d}",
            )
            tasks._tasks.insert_locked(connection, record)
            connection.execute(
                """
                INSERT INTO agent_task_settled_outbox (
                    outbox_key, message_id, task_id, run_count,
                    recipient_task_id, recipient_thread_id, child_agent_name,
                    settled_status, content, status, created_at
                ) VALUES (?, ?, ?, 1, ?, ?, ?, 'completed', 'old',
                          'suppressed', ?)
                """,
                (
                    f"settled:parent-thread:{record.task_id}:1",
                    str(uuid.uuid4()),
                    record.task_id,
                    record.parent_task_id,
                    record.parent_thread_id,
                    record.agent_name,
                    record.created_at.isoformat(),
                ),
            )
            connection.execute(
                """
                INSERT INTO agent_mailbox_messages (
                    message_id, idempotency_key, recipient_task_id,
                    recipient_thread_id, sender_task_id, sender_agent_name,
                    child_task_id, child_agent_name, child_run_count,
                    settled_status, content, trigger_run, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 'completed', 'old',
                          1, 'pending', ?)
                """,
                (
                    str(uuid.uuid4()),
                    f"settled:parent-thread:{record.task_id}:1",
                    record.parent_task_id,
                    record.parent_thread_id,
                    record.task_id,
                    record.agent_name,
                    record.task_id,
                    record.agent_name,
                    record.created_at.isoformat(),
                ),
            )
    assert mailbox.list_pending_trigger_recipient_task_ids() == []
    assert (
        mailbox.claim(
            recipient_task_id="parent-100",
            recipient_thread_id="parent-thread",
        )
        == []
    )
    mailbox.close()
    tasks.close()


def test_periodic_dispatch_stops_polling_legacy_task_table_after_watermark(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "migration-complete.sqlite")
    _, tasks, mailboxes, mailbox = _control(monkeypatch, db_path)
    asyncio.run(_notifier(tasks, mailbox).reconcile())

    def unexpected_legacy_scan():
        raise AssertionError("completed legacy migration must not be polled")

    manager = TaskManager(tasks, settled_outbox_enabled=True)
    monkeypatch.setattr(
        manager, "reconcile_settled_outbox_batch", unexpected_legacy_scan
    )
    asyncio.run(_notifier(tasks, mailbox, manager=manager).reconcile())
    mailboxes.close()
    tasks.close()
