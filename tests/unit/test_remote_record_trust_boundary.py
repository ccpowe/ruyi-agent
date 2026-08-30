from __future__ import annotations

import asyncio
import json

import httpx
from fastapi.testclient import TestClient
import pytest

from ruyi_agent.task_models import TaskRecord
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.delegation.task_manager import TaskManager
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
ATTACKER_TASK_ID = "different-private-task"
ATTACKER_RESULT = "forged result from different-private-task"


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
    AgentControl,
    TaskStore,
    MailboxStore,
    AgentMailbox,
]:
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    control = AgentControl(
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
    task_store: TaskStore,
    *,
    task_id: str,
    webhook: dict[str, str] | None = None,
) -> None:
    TaskManager(task_store, settled_outbox_enabled=True).create_task_record(
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


def _bound_payload(
    *,
    task_id: str,
    status: str,
    run_count: int,
    result: str | None = None,
    review_id: str | None = None,
) -> dict[str, object]:
    pending_review: dict[str, object] | None = None
    if review_id is not None:
        pending_review = {
            "review_id": review_id,
            "source_task_id": task_id,
            "action_requests": [{"name": "execute"}],
            "review_configs": [],
        }
    return {
        "task_id": task_id,
        "agent_name": "remote_code_wiki",
        "status": status,
        "last_result": result,
        "error": None,
        "run_count": run_count,
        "created_at": "2026-08-30T00:00:00Z",
        "updated_at": "2026-08-30T00:00:01Z",
        "pending_review": pending_review,
    }


def _seed_bound_operation(
    task_store: TaskStore,
    *,
    operation: str,
    task_id: str,
) -> None:
    manager = TaskManager(task_store, settled_outbox_enabled=True)
    _seed_remote_record(task_store, task_id=task_id)
    if operation == "review":
        manager.sync_remote_task(
            task_id,
            _bound_payload(
                task_id=PRIVATE_TASK_ID,
                status="waiting_for_human",
                run_count=1,
                review_id="review-1",
            ),
        )
    elif operation == "cancel":
        manager.sync_remote_task(
            task_id,
            _bound_payload(
                task_id=PRIVATE_TASK_ID,
                status="running",
                run_count=1,
            ),
        )
    else:
        manager.sync_remote_task(
            task_id,
            _bound_payload(
                task_id=PRIVATE_TASK_ID,
                status="completed",
                run_count=1,
                result="original result",
            ),
        )
    if operation == "refresh":
        manager.begin_external_operation(
            task_id,
            operation="send",
            identity="refresh-reconciliation",
            allow_replay=True,
        )


@pytest.mark.parametrize("operation", ["refresh", "review", "send", "cancel"])
@pytest.mark.parametrize("matching_identity", [False, True])
def test_bound_remote_operation_validates_response_identity_before_sync(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    operation: str,
    matching_identity: bool,
) -> None:
    """Four real A2A paths fence a cross-Task response before persistence."""

    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    async def scenario() -> tuple[object, object, list[dict[str, object]], object]:
        dispatched = asyncio.Event()
        release_response = asyncio.Event()
        requests: list[httpx.Request] = []
        returned_task_id = PRIVATE_TASK_ID if matching_identity else ATTACKER_TASK_ID
        if matching_identity:
            status = {
                "refresh": "completed",
                "review": "running",
                "send": "completed",
                "cancel": "cancelled",
            }[operation]
            run_count = 1 if operation == "cancel" else 2
            payload = _bound_payload(
                task_id=returned_task_id,
                status=status,
                run_count=run_count,
                result=(
                    f"verified {operation}"
                    if operation in {"refresh", "send"}
                    else None
                ),
            )
        else:
            payload = _bound_payload(
                task_id=returned_task_id,
                status="failed",
                run_count=99,
                result=ATTACKER_RESULT,
                review_id="attacker-review",
            )
            payload["error"] = PRIVATE_ERROR

        async def dispatch(request: httpx.Request) -> httpx.Response:
            assert f"/tasks/{PRIVATE_TASK_ID}" in request.url.path
            requests.append(request)
            dispatched.set()
            await release_response.wait()
            return httpx.Response(200, json=payload)

        control, task_store, mailbox_store, _mailbox = _control(
            str(tmp_path / f"identity-{operation}-{matching_identity}.sqlite"),
            a2a_client=A2AClient(
                transports={
                    "https://example.com/a2a": httpx.MockTransport(dispatch),
                }
            ),
        )
        task_id = f"proxy-{operation}"
        _seed_bound_operation(task_store, operation=operation, task_id=task_id)
        if operation == "refresh":
            call = control.refresh_task(task_id)
        elif operation == "review":
            call = control.submit_review_decision(
                "review-1",
                [{"type": "approve"}],
            )
        elif operation == "send":
            call = control.send_task_input(
                task_id,
                "continue",
                idempotency_key="send-1",
            )
        else:
            call = control.cancel_task(task_id)
        mutation = asyncio.create_task(call)
        try:
            await dispatched.wait()
            before = task_store.get_task(task_id)
            before_outbox = task_store.list_settled_outbox()
            assert before is not None
            release_response.set()
            if matching_identity:
                outcome: object = await mutation
            else:
                with pytest.raises(ValueError) as raised:
                    await mutation
                assert ATTACKER_TASK_ID not in str(raised.value)
                assert PRIVATE_TASK_ID not in str(raised.value)
                outcome = raised.value
            after = task_store.get_task(task_id)
            after_outbox = task_store.list_settled_outbox()
            assert after is not None
            if not matching_identity:
                assert after == before
                assert after_outbox == before_outbox
                assert (
                    after.external_operation
                    == {
                        "refresh": "send",
                        "review": "review",
                        "send": "send",
                        "cancel": "cancel",
                    }[operation]
                )
                serialized = json.dumps(
                    {
                        "record": after,
                        "outbox": after_outbox,
                    },
                    default=str,
                )
                assert ATTACKER_TASK_ID not in serialized
                assert ATTACKER_RESULT not in serialized
                assert "attacker-review" not in serialized
                assert PRIVATE_ERROR not in serialized
            return after, outcome, after_outbox, requests[0]
        finally:
            release_response.set()
            if not mutation.done():
                mutation.cancel()
                await asyncio.gather(mutation, return_exceptions=True)
            await control.close()
            mailbox_store.close()
            task_store.close()

    record, outcome, _outbox, request = asyncio.run(scenario())
    assert isinstance(request, httpx.Request)
    if matching_identity:
        assert isinstance(outcome, TaskRecord)
        assert record.external_operation is None
        assert record.external_outcome_uncertain is False
        assert record.upstream_task_id == PRIVATE_TASK_ID
        assert (
            record.state
            == {
                "refresh": "completed",
                "review": "running",
                "send": "completed",
                "cancel": "cancelled",
            }[operation]
        )


def test_remote_refresh_sanitizes_record_and_sqlite_outbox_before_persistence(
    tmp_path,
) -> None:
    control, task_store, mailbox_store, _mailbox = _control(
        str(tmp_path / "refresh.sqlite"),
        a2a_client=MaliciousRefreshClient(),
    )
    _seed_remote_record(task_store, task_id="proxy-refresh")

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
        httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )
    control, task_store, mailbox_store, mailbox = _control(
        str(tmp_path / "webhook.sqlite"),
        a2a_client=object(),
    )
    _seed_remote_record(
        task_store,
        task_id="proxy-webhook",
        webhook={"url": "https://caller.example/hooks", "token": "caller-token"},
    )
    control.get_task_record("proxy-webhook")

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


def test_ambiguous_remote_allocation_failure_persists_only_static_error(
    tmp_path,
) -> None:
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

    assert durable is not None
    assert durable.state == "interrupted"
    assert durable.error == "Remote create outcome is uncertain; refresh is required"
    assert durable.external_operation == "create"
    assert durable.external_outcome_uncertain is True
    assert outbox == []
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
    control = AgentControl(
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
