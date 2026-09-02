from __future__ import annotations

from datetime import UTC, datetime, timedelta

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
                "configurable": {
                    "task_id": "task-1",
                    "thread_id": "thread-1",
                    "mailbox_run_id": "run-1",
                }
            },
        )
        middleware = MailboxMiddleware(mailbox)

        with mailbox.run_scope("run-1"):
            update = middleware.before_model({}, None)

            assert update is not None
            injected = update["messages"][0]
            assert "new constraint" in injected.content
            assert mailbox.has_triggering_messages("task-1") is False

            mailbox.acknowledge_run("run-1")

        assert mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        ) == []
    finally:
        store.close()


def test_run_acknowledgement_includes_legacy_thread_scoped_claims(tmp_path) -> None:
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
        with mailbox.run_scope("run-1"):
            claimed = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
            assert len(claimed) == 1

            mailbox.acknowledge_run("run-1")

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


def test_run_acknowledges_multiple_claim_batches(tmp_path) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="first batch",
        )
        with mailbox.run_scope("run-1"):
            first = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
            mailbox.publish_input(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
                content="second batch",
            )
            second = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
            assert [message.content for message in first] == ["first batch"]
            assert [message.content for message in second] == ["second batch"]
            assert first[0].claim_token != second[0].claim_token

            mailbox.acknowledge_run("run-1")

        assert mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        ) == []
    finally:
        store.close()


def test_run_acknowledgement_does_not_bulk_ack_same_owner_recipient_claim(
    tmp_path,
) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="first batch",
        )
        with mailbox.run_scope("run-1"):
            first = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
            assert [message.content for message in first] == ["first batch"]
            assert first[0].claim_token is not None

            mailbox.publish_input(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
                content="second batch",
            )
            with mailbox.run_scope("run-2"):
                second = mailbox.claim(
                    recipient_task_id="task-1",
                    recipient_thread_id="thread-1",
                )
                assert [message.content for message in second] == ["second batch"]
                assert second[0].claim_token != first[0].claim_token

                mailbox.acknowledge_run("run-1")
                rows = {
                    row["content"]: row["status"]
                    for row in store._conn.execute(
                        "SELECT content, status FROM agent_mailbox_messages"
                    ).fetchall()
                }
                assert rows == {"first batch": "delivered", "second batch": "claimed"}
                mailbox.release_run("run-2")

        reclaimed = mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )
        assert [message.content for message in reclaimed] == ["second batch"]
    finally:
        store.close()


def test_run_release_does_not_bulk_release_same_owner_recipient_claim(
    tmp_path,
) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="first batch",
        )
        with mailbox.run_scope("run-1"):
            first = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
            assert [message.content for message in first] == ["first batch"]

            mailbox.publish_input(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
                content="second batch",
            )
            with mailbox.run_scope("run-2"):
                second = mailbox.claim(
                    recipient_task_id="task-1",
                    recipient_thread_id="thread-1",
                )
                assert [message.content for message in second] == ["second batch"]

                mailbox.release_run("run-1")
                rows = {
                    row["content"]: row["status"]
                    for row in store._conn.execute(
                        "SELECT content, status FROM agent_mailbox_messages"
                    ).fetchall()
                }
                assert rows == {"first batch": "pending", "second batch": "claimed"}
                mailbox.release_run("run-2")

        reclaimed = mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )
        assert {message.content for message in reclaimed} == {
            "first batch",
            "second batch",
        }
    finally:
        store.close()


def test_stale_owner_or_token_cannot_acknowledge_or_release_reclaimed_message(
    tmp_path,
) -> None:
    db_path = tmp_path / "mailbox.sqlite"
    first_store = MailboxStore(str(db_path))
    second_store = MailboxStore(str(db_path))
    try:
        first_mailbox = AgentMailbox(first_store)
        second_mailbox = AgentMailbox(second_store)
        first_mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="fenced input",
        )
        stale = first_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )[0]
        assert stale.claim_token is not None
        with first_store._lock:
            first_store._conn.execute(
                """
                UPDATE agent_mailbox_messages
                SET claim_expires_at = '2000-01-01T00:00:00+00:00'
                WHERE message_id = ?
                """,
                (stale.message_id,),
            )
            first_store._conn.commit()

        current = second_mailbox.claim(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
        )[0]
        assert current.claim_token is not None
        assert current.claim_token != stale.claim_token
        assert first_store.acknowledge(
            [(stale.message_id, stale.claim_token)]
        ) == 0
        assert first_store.release_claim_tokens([stale.claim_token]) == 0

        row = second_store._conn.execute(
            "SELECT status, claim_token FROM agent_mailbox_messages WHERE message_id = ?",
            (stale.message_id,),
        ).fetchone()
        assert row is not None
        assert row["status"] == "claimed"
        assert row["claim_token"] == current.claim_token
    finally:
        second_store.close()
        first_store.close()


def test_recovery_renews_only_live_run_tokens_and_never_revives_expired_claims(
    tmp_path,
) -> None:
    store = MailboxStore(str(tmp_path / "mailbox.sqlite"))
    try:
        mailbox = AgentMailbox(store)
        mailbox.publish_input(
            recipient_task_id="task-1",
            recipient_thread_id="thread-1",
            content="live",
        )
        with mailbox.run_scope("run-1"):
            live = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
            )
            assert [message.content for message in live] == ["live"]
            live_token = live[0].claim_token
            assert live_token is not None
            mailbox.publish_input(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
                content="unbound",
            )
            unbound = mailbox.claim(
                recipient_task_id="task-1",
                recipient_thread_id="thread-1",
                run_id="not-an-active-run",
            )
            assert [message.content for message in unbound] == ["unbound"]
            unbound_token = unbound[0].claim_token
            assert unbound_token is not None and unbound_token != live_token
            before_renewal = (datetime.now(UTC) + timedelta(seconds=1)).isoformat()
            with store._lock:
                store._conn.execute(
                    """
                    UPDATE agent_mailbox_messages
                    SET claim_expires_at = ?
                    WHERE claim_token IN (?, ?)
                    """,
                    (before_renewal, live_token, unbound_token),
                )
                store._conn.commit()
            mailbox.recover_claims()
            renewed = {
                row["content"]: row["claim_expires_at"]
                for row in store._conn.execute(
                    """
                    SELECT content, claim_expires_at FROM agent_mailbox_messages
                    ORDER BY content
                    """
                ).fetchall()
            }
            assert str(renewed["live"]) > before_renewal
            assert renewed["unbound"] == before_renewal

            with store._lock:
                store._conn.execute(
                    """
                    UPDATE agent_mailbox_messages
                    SET claim_expires_at = '2000-01-01T00:00:00+00:00'
                    WHERE message_id = ?
                    """,
                    (live[0].message_id,),
                )
                store._conn.commit()
            mailbox.recover_claims()
            expired = store._conn.execute(
                """
                SELECT status, claim_expires_at FROM agent_mailbox_messages
                WHERE message_id = ?
                """,
                (live[0].message_id,),
            ).fetchone()
            assert expired is not None
            assert expired["status"] == "pending"
            assert expired["claim_expires_at"] is None
    finally:
        store.close()
