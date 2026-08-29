from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import json
import sqlite3

import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.settled_outbox import SettledOutboxIntent
from ruyi_agent.storage.task_store import TaskStore
from tests.support.async_subagent_runtime import build_test_remote_refs


PUBLIC_TASK_ID = "proxy-failed"
PRIVATE_TASK_ID = "upstream-private-task"
PRIVATE_URL = f"https://private.invalid/tasks/{PRIVATE_TASK_ID}"
PRIVATE_ERROR = f"private failure for {PRIVATE_TASK_ID} at {PRIVATE_URL}"
PUBLIC_ERROR = "Remote Gateway Task failed"
NOW = "2026-08-30T00:00:00+00:00"


class MaliciousRefreshClient:
    async def get_task(self, remote_ref, *, task_id: str):
        del remote_ref
        assert task_id == PRIVATE_TASK_ID
        return {
            "task_id": PRIVATE_TASK_ID,
            "agent_name": "remote_code_wiki",
            "status": "failed",
            "last_result": None,
            "error": PRIVATE_ERROR,
            "run_count": 1,
            "created_at": NOW,
            "updated_at": NOW,
            "pending_review": None,
        }


class CapturingAsyncClient:
    calls: list[dict[str, object]] = []

    def __init__(self, *, timeout: float) -> None:
        self.timeout = timeout

    async def __aenter__(self) -> CapturingAsyncClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    async def post(self, url, *, headers, json) -> None:
        self.calls.append({"url": url, "headers": headers, "json": json})


def _create_baseline_database(db_path: str) -> None:
    """Create the relevant 9ca storage shape without current migrations."""

    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            CREATE TABLE agent_tasks (
                task_id TEXT PRIMARY KEY,
                agent_name TEXT NOT NULL,
                state TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                parent_task_id TEXT,
                root_task_id TEXT NOT NULL,
                depth INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                result TEXT,
                error TEXT,
                run_count INTEGER NOT NULL,
                route_kind TEXT NOT NULL,
                upstream_task_id TEXT,
                parent_thread_id TEXT,
                mailbox_suppressed INTEGER NOT NULL DEFAULT 0,
                mailbox_delivered INTEGER NOT NULL DEFAULT 0,
                webhook_json TEXT,
                delegation_root_id TEXT,
                delegation_max_depth INTEGER,
                delegation_max_tasks_per_root INTEGER,
                delegation_visited_nodes_json TEXT NOT NULL DEFAULT '[]',
                permission_profile TEXT NOT NULL DEFAULT '',
                effective_skill_names_json TEXT NOT NULL DEFAULT '[]',
                skill_view_path TEXT,
                skill_view_hash TEXT,
                pending_review_json TEXT,
                artifacts_json TEXT NOT NULL DEFAULT '[]'
            );
            CREATE TABLE agent_task_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                run_count INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                data_json TEXT NOT NULL
            );
            CREATE TABLE agent_task_pending_reviews (
                review_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL UNIQUE,
                root_task_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(task_id) REFERENCES agent_tasks(task_id)
            );
            CREATE TABLE agent_task_settled_outbox (
                outbox_key TEXT PRIMARY KEY,
                message_id TEXT NOT NULL UNIQUE,
                task_id TEXT NOT NULL,
                run_count INTEGER NOT NULL,
                recipient_task_id TEXT,
                recipient_thread_id TEXT NOT NULL,
                child_agent_name TEXT NOT NULL,
                settled_status TEXT NOT NULL,
                content TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                claim_expires_at TEXT,
                claimed_by TEXT,
                claim_token TEXT,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                delivered_at TEXT,
                retracted_at TEXT,
                UNIQUE(task_id, run_count),
                FOREIGN KEY(task_id) REFERENCES agent_tasks(task_id)
            );
            CREATE TABLE agent_storage_migrations (
                name TEXT PRIMARY KEY,
                cursor TEXT NOT NULL DEFAULT '',
                completed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE agent_mailbox_messages (
                message_id TEXT PRIMARY KEY,
                idempotency_key TEXT UNIQUE,
                recipient_task_id TEXT,
                recipient_thread_id TEXT NOT NULL,
                sender_task_id TEXT,
                sender_agent_name TEXT,
                child_task_id TEXT,
                child_agent_name TEXT,
                child_run_count INTEGER,
                settled_status TEXT,
                content TEXT NOT NULL,
                trigger_run INTEGER NOT NULL DEFAULT 1,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                claimed_at TEXT,
                claim_expires_at TEXT,
                delivered_at TEXT,
                claimed_by TEXT,
                claim_token TEXT
            );
            """
        )
        _insert_raw_remote_tasks(connection)
        _insert_raw_notifications(connection)
        connection.execute(
            """
            INSERT INTO agent_storage_migrations (name, cursor, completed)
            VALUES ('settled_outbox_v1', ?, 1)
            """,
            (PUBLIC_TASK_ID,),
        )
        connection.commit()
    finally:
        connection.close()


def _insert_raw_remote_tasks(connection: sqlite3.Connection) -> None:
    task_sql = """
        INSERT INTO agent_tasks (
            task_id, agent_name, state, thread_id, parent_task_id, root_task_id,
            depth, created_at, updated_at, result, error, run_count, route_kind,
            upstream_task_id, parent_thread_id, mailbox_suppressed,
            mailbox_delivered, webhook_json, delegation_root_id,
            delegation_max_depth, delegation_max_tasks_per_root,
            delegation_visited_nodes_json, permission_profile,
            effective_skill_names_json, skill_view_path, skill_view_hash,
            pending_review_json, artifacts_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?,
                  '[]', '', '[]', NULL, NULL, ?, '[]')
    """
    connection.execute(
        task_sql,
        (
            PUBLIC_TASK_ID,
            "remote_code_wiki",
            "failed",
            PRIVATE_TASK_ID,
            "parent-task",
            "parent-task",
            2,
            NOW,
            NOW,
            None,
            PRIVATE_ERROR,
            1,
            "remote_ref",
            PRIVATE_TASK_ID,
            "parent-thread",
            json.dumps(
                {"url": "https://caller.example/hooks", "token": "caller-token"}
            ),
            "parent-task",
            4,
            16,
            None,
        ),
    )
    review_payload = {
        "review_id": "remote-review",
        "source_task_id": "upstream-review-private",
        "action_requests": [{"name": "execute"}],
        "review_configs": [],
    }
    connection.execute(
        task_sql,
        (
            "proxy-review",
            "remote_code_wiki",
            "waiting_for_human",
            "upstream-review-private",
            None,
            "proxy-review",
            1,
            NOW,
            NOW,
            None,
            PRIVATE_ERROR,
            1,
            "remote_ref",
            "upstream-review-private",
            None,
            None,
            "proxy-review",
            4,
            16,
            json.dumps(review_payload),
        ),
    )
    connection.execute(
        """
        INSERT INTO agent_task_pending_reviews (
            review_id, task_id, root_task_id, payload_json, created_at, updated_at
        ) VALUES ('remote-review', 'proxy-review', 'proxy-review', ?, ?, ?)
        """,
        (json.dumps(review_payload), NOW, NOW),
    )
    event_data = {
        "task_id": "upstream-review-private",
        "thread_id": "upstream-review-private",
        "status": "waiting_for_human",
        "last_result": None,
        "error": PRIVATE_ERROR,
        "error_truncated": True,
        "updated_at": NOW,
        "pending_review": review_payload,
        "artifacts": [],
    }
    connection.execute(
        """
        INSERT INTO agent_task_events (
            task_id, run_count, event_type, created_at, data_json
        ) VALUES ('proxy-review', 1, 'task.review_requested', ?, ?)
        """,
        (NOW, json.dumps(event_data)),
    )


def _insert_raw_notifications(connection: sqlite3.Connection) -> None:
    outbox_key = f"settled:parent-thread:{PUBLIC_TASK_ID}:1"
    connection.execute(
        """
        INSERT INTO agent_task_settled_outbox (
            outbox_key, message_id, task_id, run_count, recipient_task_id,
            recipient_thread_id, child_agent_name, settled_status, content,
            status, created_at, claimed_at, claim_expires_at, claimed_by,
            claim_token, attempt_count, last_error
        ) VALUES (?, 'outbox-message', ?, 1, 'parent-task', 'parent-thread',
                  'remote_code_wiki', 'failed', ?, 'claimed', ?, ?, ?,
                  'legacy-owner', 'legacy-outbox-token', 1, ?)
        """,
        (
            outbox_key,
            PUBLIC_TASK_ID,
            PRIVATE_ERROR,
            NOW,
            NOW,
            "2099-01-01T00:00:00+00:00",
            PRIVATE_ERROR,
        ),
    )
    connection.execute(
        """
        INSERT INTO agent_mailbox_messages (
            message_id, idempotency_key, recipient_task_id,
            recipient_thread_id, sender_task_id, sender_agent_name,
            child_task_id, child_agent_name, child_run_count, settled_status,
            content, trigger_run, status, created_at, claimed_at,
            claim_expires_at, claimed_by, claim_token
        ) VALUES ('outbox-message', ?, 'parent-task', 'parent-thread', ?,
                  'remote_code_wiki', ?, 'remote_code_wiki', 1, 'failed', ?, 1,
                  'claimed', ?, ?, '2099-01-01T00:00:00+00:00',
                  'legacy-mailbox-owner', 'legacy-mailbox-token')
        """,
        (outbox_key, PRIVATE_TASK_ID, PRIVATE_TASK_ID, PRIVATE_ERROR, NOW, NOW),
    )


def _database_row(db_path: str, query: str) -> sqlite3.Row:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(query).fetchone()
        assert row is not None
        return row
    finally:
        connection.close()


def _database_rows(db_path: str, query: str) -> list[sqlite3.Row]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        return connection.execute(query).fetchall()
    finally:
        connection.close()


def _create_current_collision_database(db_path: str) -> None:
    task_store = TaskStore(db_path)
    task_store.close()
    mailbox_store = MailboxStore(db_path)
    mailbox_store.close()


def _insert_collision_task(
    connection: sqlite3.Connection,
    *,
    task_id: str,
    upstream_task_id: str,
    agent_name: str,
    run_count: int = 2,
) -> None:
    connection.execute(
        """
        INSERT INTO agent_tasks (
            task_id, agent_name, state, thread_id, parent_task_id, root_task_id,
            depth, created_at, updated_at, result, error, run_count, route_kind,
            upstream_task_id, parent_thread_id
        ) VALUES (?, ?, 'failed', ?, 'parent-task', 'parent-task', 2, ?, ?,
                  NULL, ?, ?, 'remote_ref', ?, 'parent-thread')
        """,
        (
            task_id,
            agent_name,
            upstream_task_id,
            NOW,
            NOW,
            PRIVATE_ERROR,
            run_count,
            upstream_task_id,
        ),
    )


def _insert_local_collision_task(
    connection: sqlite3.Connection,
    *,
    task_id: str,
    agent_name: str,
) -> None:
    connection.execute(
        """
        INSERT INTO agent_tasks (
            task_id, agent_name, state, thread_id, parent_task_id, root_task_id,
            depth, created_at, updated_at, result, error, run_count, route_kind,
            upstream_task_id, parent_thread_id
        ) VALUES (?, ?, 'failed', ?, 'parent-task', 'parent-task', 2, ?, ?,
                  NULL, 'local failure', 1, 'local', NULL, 'parent-thread')
        """,
        (task_id, agent_name, task_id, NOW, NOW),
    )


def _insert_collision_message(
    connection: sqlite3.Connection,
    *,
    message_id: str,
    stored_task_id: str,
    agent_name: str,
    run_count: int,
    idempotency_key: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO agent_mailbox_messages (
            message_id, idempotency_key, recipient_task_id,
            recipient_thread_id, sender_task_id, sender_agent_name,
            child_task_id, child_agent_name, child_run_count, settled_status,
            content, trigger_run, status, created_at, claimed_at,
            claim_expires_at, claimed_by, claim_token
        ) VALUES (?, ?, 'parent-task', 'parent-thread', ?, ?, ?, ?, ?, 'failed',
                  ?, 1, 'claimed', ?, ?, '2099-01-01T00:00:00+00:00',
                  'legacy-owner', ?)
        """,
        (
            message_id,
            idempotency_key,
            stored_task_id,
            agent_name,
            stored_task_id,
            agent_name,
            run_count,
            PRIVATE_ERROR,
            NOW,
            NOW,
            f"legacy-token-{message_id}",
        ),
    )


def _insert_collision_outbox(
    connection: sqlite3.Connection,
    *,
    message_id: str,
    task_id: str,
    agent_name: str,
    run_count: int,
) -> str:
    outbox_key = f"settled:parent-thread:{task_id}:{run_count}"
    connection.execute(
        """
        INSERT INTO agent_task_settled_outbox (
            outbox_key, message_id, task_id, run_count, recipient_task_id,
            recipient_thread_id, child_agent_name, settled_status, content,
            status, created_at, claimed_at, claim_expires_at, claimed_by,
            claim_token, attempt_count
        ) VALUES (?, ?, ?, ?, 'parent-task', 'parent-thread', ?, 'failed', ?,
                  'claimed', ?, ?, '2099-01-01T00:00:00+00:00',
                  'legacy-outbox-owner', 'legacy-outbox-token', 1)
        """,
        (
            outbox_key,
            message_id,
            task_id,
            run_count,
            agent_name,
            PRIVATE_ERROR,
            NOW,
            NOW,
        ),
    )
    return outbox_key


def _assert_no_raw_remote(value: object) -> None:
    serialized = json.dumps(value, default=str)
    assert PRIVATE_ERROR not in serialized
    assert PRIVATE_URL not in serialized
    assert "upstream-review-private" not in serialized


def test_baseline_remote_upgrade_fences_raw_claims_before_notifier_and_replays_safe(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "baseline-9ca.sqlite")
    _create_baseline_database(db_path)
    CapturingAsyncClient.calls.clear()
    monkeypatch.setattr(
        async_subagent_runtime.httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )

    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=MaliciousRefreshClient(),  # type: ignore[arg-type]
        task_store=task_store,
        mailbox=mailbox,
    )

    migrated = task_store.get_task(PUBLIC_TASK_ID)
    review_task = task_store.get_task("proxy-review")
    pending_review = task_store.get_pending_review("remote-review")
    events = task_store.list_task_events(
        task_id="proxy-review",
        run_count=1,
        after_event_id=0,
    )
    outbox_before = task_store.list_settled_outbox()[0]
    mailbox_before = _database_row(
        db_path,
        "SELECT * FROM agent_mailbox_messages WHERE message_id = 'outbox-message'",
    )

    assert migrated is not None
    assert migrated.thread_id == PUBLIC_TASK_ID
    assert migrated.error == PUBLIC_ERROR
    assert review_task is not None
    assert review_task.thread_id == "proxy-review"
    assert review_task.error == PUBLIC_ERROR
    assert review_task.pending_review is not None
    assert review_task.pending_review["source_task_id"] == "proxy-review"
    assert pending_review is not None
    assert pending_review.payload["source_task_id"] == "proxy-review"
    assert pending_review.cursor_order_updated_at == datetime.fromisoformat(NOW)
    assert events[0].data["task_id"] == "proxy-review"
    assert events[0].data["thread_id"] == "proxy-review"
    assert events[0].data["error"] == PUBLIC_ERROR
    assert events[0].data["pending_review"]["source_task_id"] == "proxy-review"
    assert "error_truncated" not in events[0].data
    assert outbox_before["status"] == "pending"
    assert outbox_before["claim_token"] is None
    assert outbox_before["content"] == PUBLIC_ERROR
    assert outbox_before["last_error"] is None
    assert mailbox_before["status"] == "pending"
    assert mailbox_before["claim_token"] is None
    assert mailbox_before["sender_task_id"] == PUBLIC_TASK_ID
    assert mailbox_before["child_task_id"] == PUBLIC_TASK_ID
    assert mailbox_before["content"] == PUBLIC_ERROR

    stale = SettledOutboxIntent(
        outbox_key=str(outbox_before["outbox_key"]),
        message_id=str(outbox_before["message_id"]),
        task_id=PUBLIC_TASK_ID,
        run_count=1,
        recipient_task_id="parent-task",
        recipient_thread_id="parent-thread",
        child_agent_name="remote_code_wiki",
        settled_status="failed",
        content=PRIVATE_ERROR,
        created_at=datetime.now(UTC),
        claim_token="legacy-outbox-token",
    )
    assert mailbox.publish_claimed_settled_outbox(stale) is False

    async def scenario():
        refreshed = await control.refresh_task(PUBLIC_TASK_ID)
        await control._settled_notifier.reconcile()  # noqa: SLF001
        await control._send_settled_webhook(PUBLIC_TASK_ID)  # noqa: SLF001
        return refreshed, mailbox.claim(
            recipient_task_id="parent-task",
            recipient_thread_id="parent-thread",
        )

    try:
        refreshed, messages = asyncio.run(scenario())
        outbox_after = task_store.list_settled_outbox()[0]
    finally:
        asyncio.run(control.close())
        mailbox_store.close()
        task_store.close()

    assert refreshed.error == PUBLIC_ERROR
    assert refreshed.thread_id == PUBLIC_TASK_ID
    assert outbox_after["status"] == "delivered"
    assert len(messages) == 1
    assert messages[0].child_task_id == PUBLIC_TASK_ID
    assert messages[0].sender_task_id == PUBLIC_TASK_ID
    assert messages[0].content == PUBLIC_ERROR
    assert len(CapturingAsyncClient.calls) == 1
    webhook_payload = CapturingAsyncClient.calls[0]["json"]
    assert isinstance(webhook_payload, dict)
    assert webhook_payload["task_id"] == PUBLIC_TASK_ID
    assert webhook_payload["error"] == PUBLIC_ERROR
    _assert_no_raw_remote(
        {
            "migrated": {
                "task_id": migrated.task_id,
                "thread_id": migrated.thread_id,
                "error": migrated.error,
                "pending_review": migrated.pending_review,
            },
            "review_task": {
                "task_id": review_task.task_id,
                "thread_id": review_task.thread_id,
                "error": review_task.error,
                "pending_review": review_task.pending_review,
            },
            "pending_review": pending_review.payload,
            "events": [event.data for event in events],
            "outbox_before": dict(outbox_before),
            "mailbox_before": dict(mailbox_before),
            "refreshed": {
                "task_id": refreshed.task_id,
                "thread_id": refreshed.thread_id,
                "error": refreshed.error,
                "pending_review": refreshed.pending_review,
            },
            "outbox_after": outbox_after,
            "messages": messages,
            "webhook": CapturingAsyncClient.calls,
        }
    )


def test_remote_projection_upgrade_is_idempotent_and_preserves_frozen_v2_order(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "idempotent-9ca.sqlite")
    _create_baseline_database(db_path)
    first = TaskStore(db_path)
    first_review = first.get_pending_review("remote-review")
    first.close()
    second = TaskStore(db_path)
    try:
        second_review = second.get_pending_review("remote-review")
        migrations = _database_row(
            db_path,
            """
            SELECT COUNT(*) AS count, MIN(completed) AS completed
            FROM agent_storage_migrations
            WHERE name = 'remote_public_projection_v1'
            """,
        )
    finally:
        second.close()

    assert first_review is not None and second_review is not None
    assert first_review.cursor_order_updated_at == datetime.fromisoformat(NOW)
    assert second_review.cursor_order_updated_at == first_review.cursor_order_updated_at
    assert migrations["count"] == 1
    assert migrations["completed"] == 1


def test_delivered_outbox_only_anchors_pending_mailbox_identity_and_not_content(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = str(tmp_path / "delivered-outbox-pending-mailbox.sqlite")
    _create_baseline_database(db_path)
    private_outbox_content = f"delivered raw {PRIVATE_TASK_ID} via {PRIVATE_URL}"
    private_mailbox_content = f"pending raw {PRIVATE_TASK_ID} via {PRIVATE_URL}"
    outbox_key = f"settled:parent-thread:{PUBLIC_TASK_ID}:1"
    CapturingAsyncClient.calls.clear()
    monkeypatch.setattr(
        async_subagent_runtime.httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            """
            UPDATE agent_task_settled_outbox
            SET content = ?, status = 'delivered', delivered_at = ?,
                claimed_at = NULL, claim_expires_at = NULL,
                claimed_by = NULL, claim_token = NULL
            WHERE message_id = 'outbox-message'
            """,
            (private_outbox_content, NOW),
        )
        connection.execute(
            """
            UPDATE agent_mailbox_messages
            SET content = ?, status = 'pending', claimed_at = NULL,
                claim_expires_at = NULL, claimed_by = NULL, claim_token = NULL
            WHERE message_id = 'outbox-message'
            """,
            (private_mailbox_content,),
        )
        connection.commit()
    finally:
        connection.close()

    first = TaskStore(db_path)
    first.close()
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=object(),  # type: ignore[arg-type]
        task_store=task_store,
        mailbox=mailbox,
    )
    try:
        mailbox_before = _database_row(
            db_path,
            """
            SELECT * FROM agent_mailbox_messages
            WHERE message_id = 'outbox-message'
            """,
        )
        outbox = {
            str(row["outbox_key"]): row
            for row in task_store.list_settled_outbox()
        }[outbox_key]
        claimable_outbox = task_store.claim_settled_outbox()
        claimed_mailbox = mailbox.claim(
            recipient_task_id="parent-task",
            recipient_thread_id="parent-thread",
        )
        asyncio.run(control._send_settled_webhook(PUBLIC_TASK_ID))  # noqa: SLF001
    finally:
        asyncio.run(control.close())
        mailbox_store.close()
        task_store.close()

    assert outbox["status"] == "delivered"
    assert outbox["content"] == private_outbox_content
    assert claimable_outbox == []
    assert mailbox_before["status"] == "pending"
    assert mailbox_before["idempotency_key"] == outbox_key
    assert mailbox_before["sender_task_id"] == PUBLIC_TASK_ID
    assert mailbox_before["child_task_id"] == PUBLIC_TASK_ID
    assert mailbox_before["content"] == PUBLIC_ERROR
    assert len(claimed_mailbox) == 1
    assert claimed_mailbox[0].sender_task_id == PUBLIC_TASK_ID
    assert claimed_mailbox[0].child_task_id == PUBLIC_TASK_ID
    assert claimed_mailbox[0].content == PUBLIC_ERROR
    assert len(CapturingAsyncClient.calls) == 1
    webhook_payload = CapturingAsyncClient.calls[0]["json"]
    assert isinstance(webhook_payload, dict)
    assert webhook_payload["task_id"] == PUBLIC_TASK_ID
    assert webhook_payload["error"] == PUBLIC_ERROR
    downstream_projection = json.dumps(
        {
            "mailbox_before": dict(mailbox_before),
            "claimed_mailbox": claimed_mailbox,
            "caller_webhook": CapturingAsyncClient.calls,
        },
        default=str,
    )
    assert private_outbox_content not in downstream_projection
    assert private_mailbox_content not in downstream_projection
    assert PRIVATE_TASK_ID not in downstream_projection
    assert PRIVATE_URL not in downstream_projection


def test_retracted_mailbox_fences_every_linked_remote_outbox_on_each_reopen(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "retracted-linked-outbox.sqlite")
    _create_current_collision_database(db_path)
    connection = sqlite3.connect(db_path)
    try:
        for task_id, upstream_task_id, run_count in (
            ("proxy-retracted-a", "private-retracted-a", 2),
            ("proxy-retracted-b", "private-retracted-b", 3),
            ("proxy-partial", "private-partial", 4),
        ):
            _insert_collision_task(
                connection,
                task_id=task_id,
                upstream_task_id=upstream_task_id,
                agent_name="remote_retracted",
                run_count=run_count,
            )
        message_link_key = _insert_collision_outbox(
            connection,
            message_id="retracted-linked-message",
            task_id="proxy-retracted-a",
            agent_name="remote_retracted",
            run_count=2,
        )
        key_link_key = _insert_collision_outbox(
            connection,
            message_id="different-outbox-message",
            task_id="proxy-retracted-b",
            agent_name="remote_retracted",
            run_count=3,
        )
        _insert_collision_message(
            connection,
            message_id="retracted-linked-message",
            stored_task_id="private-retracted-a",
            agent_name="remote_retracted",
            run_count=2,
            idempotency_key=key_link_key,
        )
        connection.execute(
            """
            UPDATE agent_mailbox_messages
            SET status = 'retracted'
            WHERE message_id = 'retracted-linked-message'
            """
        )

        partial_key = _insert_collision_outbox(
            connection,
            message_id="partially-isolated-message",
            task_id="proxy-partial",
            agent_name="remote_retracted",
            run_count=4,
        )
        _insert_collision_message(
            connection,
            message_id="partially-isolated-message",
            stored_task_id="private-partial",
            agent_name="remote_retracted",
            run_count=4,
            idempotency_key=partial_key,
        )
        connection.execute(
            """
            UPDATE agent_mailbox_messages
            SET idempotency_key = NULL, sender_task_id = NULL,
                sender_agent_name = NULL, child_task_id = NULL,
                child_agent_name = NULL, status = 'retracted'
            WHERE message_id = 'partially-isolated-message'
            """
        )
        connection.commit()
    finally:
        connection.close()

    first = TaskStore(db_path)
    first_outboxes = {
        str(row["outbox_key"]): row for row in first.list_settled_outbox()
    }
    first.close()
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    try:
        second_outboxes = {
            str(row["outbox_key"]): row
            for row in task_store.list_settled_outbox()
        }
        mailbox_rows = {
            str(row["message_id"]): row
            for row in _database_rows(
                db_path,
                "SELECT * FROM agent_mailbox_messages ORDER BY message_id",
            )
        }
        claimable_outbox = task_store.claim_settled_outbox()
        retryable_suppression = task_store.list_suppressed_settled_outbox()
        claimed_mailbox = mailbox.claim(
            recipient_task_id="parent-task",
            recipient_thread_id="parent-thread",
        )
    finally:
        mailbox_store.close()
        task_store.close()

    for key in (message_link_key, key_link_key, partial_key):
        first_row = first_outboxes[key]
        second_row = second_outboxes[key]
        assert first_row["status"] == "suppressed"
        assert second_row["status"] == "suppressed"
        assert first_row["claim_token"] is None
        assert second_row["claim_token"] is None
        assert first_row["claimed_by"] is None
        assert second_row["claimed_by"] is None
        assert first_row["claim_expires_at"] is None
        assert second_row["claim_expires_at"] is None
        assert first_row["retracted_at"] is not None
        assert second_row["retracted_at"] == first_row["retracted_at"]
    for message_id in ("retracted-linked-message", "partially-isolated-message"):
        message = mailbox_rows[message_id]
        assert message["status"] == "retracted"
        assert message["idempotency_key"] is None
        assert message["sender_task_id"] is None
        assert message["sender_agent_name"] is None
        assert message["child_task_id"] is None
        assert message["child_agent_name"] is None
        assert message["claim_token"] is None
    assert claimable_outbox == []
    assert retryable_suppression == []
    assert claimed_mailbox == []


def test_shared_upstream_uses_outbox_binding_and_isolates_unanchored_ambiguity(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "shared-upstream.sqlite")
    _create_current_collision_database(db_path)
    connection = sqlite3.connect(db_path)
    try:
        for task_id in ("proxy-shared-a", "proxy-shared-b"):
            _insert_collision_task(
                connection,
                task_id=task_id,
                upstream_task_id="shared-upstream",
                agent_name="remote_shared",
            )
        linked_key = _insert_collision_outbox(
            connection,
            message_id="linked-message",
            task_id="proxy-shared-a",
            agent_name="remote_shared",
            run_count=2,
        )
        _insert_collision_message(
            connection,
            message_id="linked-message",
            stored_task_id="shared-upstream",
            agent_name="remote_shared",
            run_count=2,
            idempotency_key=linked_key,
        )
        _insert_collision_message(
            connection,
            message_id="ambiguous-message",
            stored_task_id="shared-upstream",
            agent_name="remote_shared",
            run_count=1,
        )
        isolated_key = _insert_collision_outbox(
            connection,
            message_id="mismatched-linked-message",
            task_id="proxy-shared-b",
            agent_name="remote_shared",
            run_count=3,
        )
        _insert_collision_message(
            connection,
            message_id="mismatched-linked-message",
            stored_task_id="shared-upstream",
            agent_name="wrong-agent",
            run_count=3,
            idempotency_key=isolated_key,
        )
        connection.commit()
    finally:
        connection.close()

    first_task_store = TaskStore(db_path)
    first_task_store.close()
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    try:
        rows = {
            row["message_id"]: row
            for row in _database_rows(
                db_path,
                "SELECT * FROM agent_mailbox_messages ORDER BY message_id",
            )
        }
        linked = rows["linked-message"]
        ambiguous = rows["ambiguous-message"]
        mismatched = rows["mismatched-linked-message"]
        stale = SettledOutboxIntent(
            outbox_key=isolated_key,
            message_id="mismatched-linked-message",
            task_id="proxy-shared-b",
            run_count=3,
            recipient_task_id="parent-task",
            recipient_thread_id="parent-thread",
            child_agent_name="remote_shared",
            settled_status="failed",
            content=PRIVATE_ERROR,
            created_at=datetime.now(UTC),
            claim_token="legacy-outbox-token",
        )
        stale_delivered = mailbox.publish_claimed_settled_outbox(stale)
        claimed = mailbox.claim(
            recipient_task_id="parent-task",
            recipient_thread_id="parent-thread",
        )
        outboxes = {
            str(row["outbox_key"]): row
            for row in task_store.list_settled_outbox()
        }
    finally:
        mailbox_store.close()
        task_store.close()

    assert linked["status"] == "pending"
    assert linked["idempotency_key"] == linked_key
    assert linked["sender_task_id"] == "proxy-shared-a"
    assert linked["child_task_id"] == "proxy-shared-a"
    assert linked["sender_agent_name"] == "remote_shared"
    assert linked["child_agent_name"] == "remote_shared"
    assert linked["content"] == PUBLIC_ERROR
    assert linked["claim_token"] is None
    assert ambiguous["status"] == "retracted"
    assert ambiguous["idempotency_key"] is None
    assert ambiguous["sender_task_id"] is None
    assert ambiguous["sender_agent_name"] is None
    assert ambiguous["child_task_id"] is None
    assert ambiguous["child_agent_name"] is None
    assert ambiguous["content"] == PUBLIC_ERROR
    assert ambiguous["claim_token"] is None
    assert mismatched["status"] == "retracted"
    assert mismatched["idempotency_key"] is None
    assert mismatched["sender_task_id"] is None
    assert mismatched["sender_agent_name"] is None
    assert mismatched["child_task_id"] is None
    assert mismatched["child_agent_name"] is None
    assert mismatched["claim_token"] is None
    assert stale_delivered is False
    assert [message.message_id for message in claimed] == ["linked-message"]
    assert outboxes[linked_key]["status"] == "pending"
    assert outboxes[linked_key]["content"] == PUBLIC_ERROR
    assert outboxes[isolated_key]["status"] == "suppressed"
    assert outboxes[isolated_key]["claim_token"] is None
    assert outboxes[isolated_key]["retracted_at"] is not None


def test_public_id_upstream_collision_uses_agent_and_isolates_same_agent_pair(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "public-upstream-collision.sqlite")
    _create_current_collision_database(db_path)
    connection = sqlite3.connect(db_path)
    try:
        _insert_collision_task(
            connection,
            task_id="collision-id",
            upstream_task_id="private-a",
            agent_name="remote_a",
        )
        _insert_collision_task(
            connection,
            task_id="proxy-b",
            upstream_task_id="collision-id",
            agent_name="remote_b",
        )
        _insert_collision_message(
            connection,
            message_id="agent-resolved-message",
            stored_task_id="collision-id",
            agent_name="remote_b",
            run_count=1,
        )
        _insert_collision_task(
            connection,
            task_id="ambiguous-public",
            upstream_task_id="private-c",
            agent_name="remote_same",
        )
        _insert_collision_task(
            connection,
            task_id="proxy-d",
            upstream_task_id="ambiguous-public",
            agent_name="remote_same",
        )
        _insert_collision_message(
            connection,
            message_id="public-ambiguous-message",
            stored_task_id="ambiguous-public",
            agent_name="remote_same",
            run_count=1,
        )
        connection.commit()
    finally:
        connection.close()

    first = TaskStore(db_path)
    first.close()
    second = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    try:
        rows = {
            row["message_id"]: row
            for row in _database_rows(
                db_path,
                "SELECT * FROM agent_mailbox_messages ORDER BY message_id",
            )
        }
        claimed = mailbox.claim(
            recipient_task_id="parent-task",
            recipient_thread_id="parent-thread",
        )
    finally:
        mailbox_store.close()
        second.close()

    resolved = rows["agent-resolved-message"]
    ambiguous = rows["public-ambiguous-message"]
    assert resolved["status"] == "pending"
    assert resolved["sender_task_id"] == "proxy-b"
    assert resolved["child_task_id"] == "proxy-b"
    assert resolved["sender_agent_name"] == "remote_b"
    assert resolved["child_agent_name"] == "remote_b"
    assert resolved["content"] == PUBLIC_ERROR
    assert resolved["idempotency_key"] == "settled:parent-thread:proxy-b:1"
    assert ambiguous["status"] == "retracted"
    assert ambiguous["sender_task_id"] is None
    assert ambiguous["sender_agent_name"] is None
    assert ambiguous["child_task_id"] is None
    assert ambiguous["child_agent_name"] is None
    assert ambiguous["idempotency_key"] is None
    assert ambiguous["claim_token"] is None
    assert [message.message_id for message in claimed] == [
        "agent-resolved-message"
    ]


def test_authoritative_local_outbox_wins_over_colliding_remote_upstream(
    tmp_path,
) -> None:
    db_path = str(tmp_path / "local-authoritative-collision.sqlite")
    _create_current_collision_database(db_path)
    connection = sqlite3.connect(db_path)
    try:
        _insert_local_collision_task(
            connection,
            task_id="local-collision",
            agent_name="local_agent",
        )
        _insert_collision_task(
            connection,
            task_id="proxy-remote",
            upstream_task_id="local-collision",
            agent_name="remote_agent",
        )
        local_key = _insert_collision_outbox(
            connection,
            message_id="local-linked-message",
            task_id="local-collision",
            agent_name="local_agent",
            run_count=1,
        )
        _insert_collision_message(
            connection,
            message_id="local-linked-message",
            stored_task_id="local-collision",
            agent_name="local_agent",
            run_count=1,
            idempotency_key=local_key,
        )
        connection.commit()
    finally:
        connection.close()

    first = TaskStore(db_path)
    first.close()
    second = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    try:
        message = _database_row(
            db_path,
            """
            SELECT * FROM agent_mailbox_messages
            WHERE message_id = 'local-linked-message'
            """,
        )
        outbox = {
            str(row["outbox_key"]): row
            for row in second.list_settled_outbox()
        }[local_key]
    finally:
        mailbox_store.close()
        second.close()

    assert message["status"] == "claimed"
    assert message["idempotency_key"] == local_key
    assert message["sender_task_id"] == "local-collision"
    assert message["sender_agent_name"] == "local_agent"
    assert message["child_task_id"] == "local-collision"
    assert message["child_agent_name"] == "local_agent"
    assert message["content"] == PRIVATE_ERROR
    assert message["claim_token"] == "legacy-token-local-linked-message"
    assert outbox["status"] == "claimed"
    assert outbox["content"] == PRIVATE_ERROR
    assert outbox["claim_token"] == "legacy-outbox-token"
