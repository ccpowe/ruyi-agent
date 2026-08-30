from __future__ import annotations

import asyncio
import threading
import uuid
from datetime import UTC, datetime

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
