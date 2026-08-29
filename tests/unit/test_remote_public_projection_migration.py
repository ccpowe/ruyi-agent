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
