from __future__ import annotations

from ruyi_agent.runtime.mailbox.service import AgentMailbox, render_mailbox_messages
import ruyi_agent.runtime.middleware.mailbox as mailbox_middleware_module
from ruyi_agent.runtime.middleware.mailbox import MailboxMiddleware
from ruyi_agent.storage.mailbox_store import MailboxStore


def test_durable_mailbox_survives_reopen(tmp_path) -> None:
    db_path = tmp_path / "tasks.sqlite"
    first_store = MailboxStore(str(db_path))
    first_mailbox = AgentMailbox(first_store)
    message = first_mailbox.publish_input(
        recipient_task_id="task-1",
        recipient_thread_id="thread-1",
        sender_task_id="task-parent",
        sender_agent_name="moderator",
        content="Please reconsider the migration order.",
    )
    assert message is not None
    first_store.close()

    second_store = MailboxStore(str(db_path))
    try:
        second_mailbox = AgentMailbox(second_store)
        claimed = second_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )
        assert [item.content for item in claimed] == [
            "Please reconsider the migration order."
        ]
        assert second_mailbox.has_triggering_messages("task-1") is False
        second_mailbox.acknowledge([claimed[0].message_id])
        assert second_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        ) == []
    finally:
        second_store.close()


def test_mailbox_idempotency_key_deduplicates_network_retry(tmp_path) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        first = mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="same logical input",
            idempotency_key="request-1",
            message_id="message-1",
        )
        duplicate = mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="same logical input",
            idempotency_key="request-1",
            message_id="message-1",
        )
        assert first is not None
        assert first.message_id == "message-1"
        assert duplicate is None
        assert len(
            mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
        ) == 1
    finally:
        store.close()


def test_in_memory_mailbox_deduplicates_preallocated_input_identity() -> None:
    mailbox = AgentMailbox()
    first = mailbox.publish_input(
        recipient_task_id="task-1",
        recipient_thread_id="thread-1",
        content="same logical input",
        idempotency_key="request-1",
        message_id="message-1",
    )
    duplicate = mailbox.publish_input(
        recipient_task_id="task-1",
        recipient_thread_id="thread-1",
        content="same logical input",
        idempotency_key="request-1",
        message_id="message-1",
    )

    assert first is not None
    assert first.message_id == "message-1"
    assert duplicate is None
    assert [item.message_id for item in mailbox.drain("thread-1")] == ["message-1"]


def test_render_general_mailbox_input_includes_sender_identity() -> None:
    mailbox = AgentMailbox()
    message = mailbox.publish_input(
        recipient_task_id="task-child",
        recipient_thread_id="thread-child",
        sender_task_id="task-parent",
        sender_agent_name="moderator",
        content="Respond to the disputed assumption.",
    )
    assert message is not None

    rendered = render_mailbox_messages([message])

    assert "[mailbox] New input received." in rendered
    assert "sender_task_id=task-parent" in rendered
    assert "sender_agent=moderator" in rendered
    assert "Respond to the disputed assumption." in rendered


def test_middleware_claims_before_model_and_runtime_acknowledges_after_run(
    monkeypatch, tmp_path
) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="new constraint",
        )
        monkeypatch.setattr(
            mailbox_middleware_module,
            "get_config",
            lambda: {
                "configurable": {"task_id": "task-1", "thread_id": "thread-1"}
            },
        )
        middleware = MailboxMiddleware(mailbox)

        update = middleware.before_model({}, None)

        assert update is not None
        injected = update["messages"][0]
        assert "new constraint" in injected.content
        assert mailbox.has_triggering_messages("task-1") is False

        mailbox.acknowledge_task("task-1", "thread-1")

        assert mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        ) == []
    finally:
        store.close()


def test_task_acknowledgement_includes_legacy_thread_scoped_claims(tmp_path) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        mailbox.publish_settled(
            recipient_thread_id="thread-1",
            child_task_id="child-1",
            child_agent_name="worker",
            run_count=1,
            status="completed",
            content="done",
        )
        claimed = mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )
        assert len(claimed) == 1

        mailbox.acknowledge_task("task-1", "thread-1")

        mailbox.recover_claims()
        assert mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        ) == []
    finally:
        store.close()


def test_claim_is_atomic_across_two_store_connections(tmp_path) -> None:
    db_path = tmp_path / "mailbox.sqlite"
    first_store = MailboxStore(str(db_path))
    second_store = MailboxStore(str(db_path))
    try:
        first_mailbox = AgentMailbox(first_store)
        second_mailbox = AgentMailbox(second_store)
        first_mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="deliver once",
        )

        first_claim = first_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )
        second_claim = second_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )

        assert [message.content for message in first_claim] == ["deliver once"]
        assert second_claim == []
        second_mailbox.recover_claims()
        assert second_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        ) == []
    finally:
        second_store.close()
        first_store.close()
