from datetime import UTC, datetime
import sqlite3

import pytest

from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import TaskRecord


def _publish(mailbox, key):
    return mailbox.publish_input(
        recipient_task_id="task", recipient_thread_id="thread", content=key,
        idempotency_key=key,
    )


def test_wakeup_sequence_survives_duplicate_deletion_vacuum_and_reopen(tmp_path):
    path = str(tmp_path / "mailbox.sqlite")
    store = MailboxStore(path)
    mailbox = AgentMailbox(store)
    assert _publish(mailbox, "one") is not None
    first = store.max_triggering_sequence("task")
    assert first > 0
    assert _publish(mailbox, "one") is None
    assert store.max_triggering_sequence("task") == first
    assert not store.has_triggering("task", after_sequence=first)
    assert store.list_pending_trigger_recipient_task_ids(after_sequence=first) == []
    store.close()
    with sqlite3.connect(path) as conn:
        conn.execute("DELETE FROM agent_mailbox_messages")
        conn.commit()
        conn.execute("VACUUM")
    store = MailboxStore(path)
    try:
        assert _publish(AgentMailbox(store), "two") is not None
        assert store.max_triggering_sequence("task") > first
        assert store.has_triggering("task", after_sequence=first)
        assert store.list_pending_trigger_recipient_task_ids(after_sequence=first) == ["task"]
    finally:
        store.close()


@pytest.mark.parametrize("mailbox_first", [True, False])
@pytest.mark.parametrize("state", ["failed", "cancelled", "interrupted", "running", "completed"])
def test_legacy_wakeup_migration_fences_unsuccessful_runs(tmp_path, mailbox_first, state):
    path = str(tmp_path / "tasks.sqlite")
    tasks = TaskStore(path)
    now = datetime.now(UTC)
    tasks.save_task(TaskRecord(
        task_id="task", agent_name="gone", state=state, thread_id="thread",
        parent_task_id=None, root_task_id="task", depth=0, created_at=now, updated_at=now,
    ))
    tasks.close()
    mailbox = MailboxStore(path)
    _publish(AgentMailbox(mailbox), "old-one")
    _publish(AgentMailbox(mailbox), "old-two")
    mailbox.close()
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TRIGGER agent_mailbox_assign_wakeup_sequence")
        conn.execute("DROP INDEX idx_agent_mailbox_wakeup_sequence")
        conn.execute("DROP TABLE agent_mailbox_sequence")
        conn.execute("ALTER TABLE agent_mailbox_messages DROP COLUMN wakeup_sequence")
        conn.execute("ALTER TABLE agent_tasks DROP COLUMN mailbox_wakeup_sequence")
    if mailbox_first:
        mailbox = MailboxStore(path)
        tasks = TaskStore(path)
    else:
        tasks = TaskStore(path)
        mailbox = MailboxStore(path)
    try:
        record = tasks.get_task("task")
        sequence = mailbox.max_triggering_sequence("task")
        assert sequence == 2
        assert record.mailbox_wakeup_sequence == (0 if state == "completed" else sequence)
        assert _publish(AgentMailbox(mailbox), "new") is not None
        assert mailbox.max_triggering_sequence("task") > max(sequence, record.mailbox_wakeup_sequence)
        record.mailbox_wakeup_sequence = mailbox.max_triggering_sequence("task")
        tasks.save_task(record)
    finally:
        tasks.close()
        mailbox.close()
    tasks = TaskStore(path)
    mailbox = MailboxStore(path)
    try:
        assert tasks.get_task("task").mailbox_wakeup_sequence == mailbox.max_triggering_sequence("task")
    finally:
        tasks.close()
        mailbox.close()
