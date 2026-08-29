from __future__ import annotations

import asyncio
import json

import httpx
from fastapi.testclient import TestClient
import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from tests.support.async_subagent_runtime import build_test_remote_refs
from tests.unit.gateway_http_support import (
    auth_headers,
    build_agent_configs,
    parse_sse_records,
)


PRIVATE_TASK_ID = "upstream-private-task"
PRIVATE_URL = f"https://private.invalid/tasks/{PRIVATE_TASK_ID}"
PRIVATE_ERROR = f"private message for {PRIVATE_TASK_ID} at {PRIVATE_URL}"
PUBLIC_ERROR = "Remote Gateway Task failed"


def _failed_payload() -> dict[str, object]:
    return {
        "task_id": PRIVATE_TASK_ID,
        "agent_name": "remote_code_wiki",
        "status": "failed",
        "last_result": None,
        "error": PRIVATE_ERROR,
        "run_count": 1,
        "created_at": "2026-08-30T00:00:00Z",
        "updated_at": "2026-08-30T00:00:01Z",
        "pending_review": None,
    }


class MaliciousRefreshClient:
    async def get_task(self, remote_ref, *, task_id: str):
        del remote_ref
        assert task_id == PRIVATE_TASK_ID
        return _failed_payload()


class MaliciousCreateFailureClient:
    async def create_task(self, remote_ref, **kwargs):
        del remote_ref, kwargs
        raise A2AClientError(
            status_code=502,
            code="upstream_gateway_error",
            message=PRIVATE_ERROR,
        )


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


def _control(
    db_path: str,
    *,
    a2a_client: object,
) -> tuple[
    async_subagent_runtime.AgentControl,
    TaskStore,
    MailboxStore,
    AgentMailbox,
]:
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,  # type: ignore[arg-type]
        task_store=task_store,
        mailbox=mailbox,
    )
    return control, task_store, mailbox_store, mailbox


def _seed_remote_record(
    control: async_subagent_runtime.AgentControl,
    *,
    task_id: str,
    webhook: dict[str, str] | None = None,
) -> None:
    control._task_manager.create_task_record(  # noqa: SLF001
        task_id,
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        route_kind="remote_ref",
        upstream_task_id=PRIVATE_TASK_ID,
        parent_thread_id="parent-thread",
        webhook=webhook,
    )


def _assert_no_private_value(value: object) -> None:
    serialized = json.dumps(value, default=str)
    assert PRIVATE_ERROR not in serialized
    assert PRIVATE_URL not in serialized
    assert PRIVATE_TASK_ID not in serialized


def test_remote_refresh_sanitizes_record_and_sqlite_outbox_before_persistence(
    tmp_path,
) -> None:
    control, task_store, mailbox_store, _mailbox = _control(
        str(tmp_path / "refresh.sqlite"),
        a2a_client=MaliciousRefreshClient(),
    )
    _seed_remote_record(control, task_id="proxy-refresh")

    async def scenario():
        return await control.refresh_task("proxy-refresh")

    try:
        refreshed = asyncio.run(scenario())
        durable = task_store.get_task("proxy-refresh")
        outbox = task_store.list_settled_outbox()
    finally:
        asyncio.run(control.close())
        mailbox_store.close()
        task_store.close()

    assert refreshed.error == PUBLIC_ERROR
    assert refreshed.thread_id == refreshed.task_id
    assert durable is not None and durable.error == PUBLIC_ERROR
    assert durable.thread_id == durable.task_id
    assert len(outbox) == 1
    assert outbox[0]["content"] == PUBLIC_ERROR
    _assert_no_private_value(
        {
            "record": {
                "error": durable.error,
                "thread_id": durable.thread_id,
                "pending_review": durable.pending_review,
            },
            "outbox": outbox,
        }
    )


def test_remote_webhook_sanitizes_record_outbox_mailbox_and_caller_webhook(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    CapturingAsyncClient.calls.clear()
    monkeypatch.setattr(
        async_subagent_runtime.httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )
    control, task_store, mailbox_store, mailbox = _control(
        str(tmp_path / "webhook.sqlite"),
        a2a_client=object(),
    )
    _seed_remote_record(
        control,
        task_id="proxy-webhook",
        webhook={"url": "https://caller.example/hooks", "token": "caller-token"},
    )

    async def scenario() -> bool:
        return await control.handle_remote_task_event(_failed_payload())

    try:
        handled = asyncio.run(scenario())
        durable = task_store.get_task("proxy-webhook")
        outbox = task_store.list_settled_outbox()
        messages = mailbox.drain("parent-thread")
    finally:
        asyncio.run(control.close())
        mailbox_store.close()
        task_store.close()

    assert handled is True
    assert durable is not None and durable.error == PUBLIC_ERROR
    assert durable.thread_id == durable.task_id
    assert len(outbox) == 1 and outbox[0]["content"] == PUBLIC_ERROR
    assert len(messages) == 1 and messages[0].content == PUBLIC_ERROR
    assert len(CapturingAsyncClient.calls) == 1
    webhook_payload = CapturingAsyncClient.calls[0]["json"]
    assert isinstance(webhook_payload, dict)
    assert webhook_payload["task_id"] == "proxy-webhook"
    assert webhook_payload["error"] == PUBLIC_ERROR
    _assert_no_private_value(
        {
            "record": {
                "error": durable.error,
                "thread_id": durable.thread_id,
                "pending_review": durable.pending_review,
            },
            "outbox": outbox,
            "mailbox": messages,
            "caller_webhook": CapturingAsyncClient.calls,
        }
    )


def test_remote_allocation_failure_persists_only_static_error(tmp_path) -> None:
    control, task_store, mailbox_store, _mailbox = _control(
        str(tmp_path / "allocation.sqlite"),
        a2a_client=MaliciousCreateFailureClient(),
    )

    async def scenario() -> None:
        with pytest.raises(A2AClientError):
            await control.spawn_task(
                "remote_code_wiki",
                "create",
                task_id="proxy-allocation",
                parent_thread_id="parent-thread",
            )

    try:
        asyncio.run(scenario())
        durable = task_store.get_task("proxy-allocation")
        outbox = task_store.list_settled_outbox()
    finally:
        asyncio.run(control.close())
        mailbox_store.close()
        task_store.close()

    assert (
        durable is not None and durable.error == "Remote Gateway Task creation failed"
    )
    assert len(outbox) == 1
    assert outbox[0]["content"] == "Remote Gateway Task creation failed"
    _assert_no_private_value(
        {
            "record": {"error": durable.error, "thread_id": durable.thread_id},
            "outbox": outbox,
        }
    )


def test_remote_http_task_and_sse_rewrite_private_review_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith(
            "/agents/code_wiki/tasks"
        ):
            return httpx.Response(
                201,
                json={
                    "task_id": PRIVATE_TASK_ID,
                    "agent_name": "code_wiki",
                    "status": "waiting_for_human",
                    "last_result": None,
                    "error": None,
                    "run_count": 1,
                    "created_at": "2026-08-30T00:00:00Z",
                    "updated_at": "2026-08-30T00:00:01Z",
                    "pending_review": {
                        "review_id": "remote-review",
                        "source_task_id": PRIVATE_TASK_ID,
                        "action_requests": [{"name": "execute"}],
                        "review_configs": [],
                    },
                    "artifacts": [],
                },
            )
        if request.method == "GET" and request.url.path.endswith(
            f"/tasks/{PRIVATE_TASK_ID}/events"
        ):
            snapshot = {
                "task_id": PRIVATE_TASK_ID,
                "run_count": 1,
                "created_at": "2026-08-30T00:00:00Z",
                "status": "waiting_for_human",
                "last_result": None,
                "error": PRIVATE_ERROR,
                "updated_at": "2026-08-30T00:00:01Z",
                "pending_review": {
                    "review_id": "remote-review",
                    "source_task_id": PRIVATE_TASK_ID,
                    "action_requests": [{"name": "execute"}],
                    "review_configs": [],
                },
                "artifacts": [],
            }
            ended = {
                "task_id": PRIVATE_TASK_ID,
                "run_count": 1,
                "created_at": "2026-08-30T00:00:02Z",
                "reason": "review_required",
            }
            body = (
                "id: remote-cursor\n"
                "event: task.snapshot\n"
                f"data: {json.dumps(snapshot, separators=(',', ':'))}\n\n"
                "event: stream.end\n"
                f"data: {json.dumps(ended, separators=(',', ':'))}\n\n"
            )
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body.encode(),
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    store = TaskStore(str(tmp_path / "http-sse.sqlite"))
    control = async_subagent_runtime.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=A2AClient(
            transports={
                "https://example.com/a2a": httpx.MockTransport(handler),
            }
        ),
    )
    service = GatewayTaskModule(
        main_agent_name="remote_code_wiki",
        agent_configs=build_agent_configs(),
        control=control,
    )
    app = create_gateway_app(service=service, bearer_token="secret-token")
    try:
        with TestClient(app) as client:
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json={"input": {"content": "review"}, "metadata": {}},
            )
            assert created.status_code == 201, created.json()
            created_payload = created.json()
            public_task_id = created_payload["task_id"]
            assert public_task_id != PRIVATE_TASK_ID
            assert created_payload["pending_review"]["source_task_id"] == public_task_id

            streamed = client.get(
                f"/tasks/{public_task_id}/events?run_count=1",
                headers=auth_headers(),
            )
            assert streamed.status_code == 200, streamed.text
            records = parse_sse_records(streamed.text)
    finally:
        asyncio.run(control.close())
        store.close()

    assert [item["event"] for item in records] == [
        "task.snapshot",
        "stream.end",
    ]
    snapshot_payload = records[0]["data"]
    assert snapshot_payload["task_id"] == public_task_id
    assert snapshot_payload["error"] == PUBLIC_ERROR
    assert snapshot_payload["pending_review"]["source_task_id"] == public_task_id
    _assert_no_private_value({"task_response": created_payload, "sse": records})
