from __future__ import annotations

import asyncio
from functools import wraps
from pathlib import Path
from typing import Any

import httpx
import pytest

import ruyi_agent.runtime.delegation.async_runtime as runtime_module
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from tests.support.async_subagent_runtime import build_test_remote_refs


def async_test(function: Any) -> Any:
    @wraps(function)
    def run(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(function(*args, **kwargs))

    return run


def _payload(
    *,
    task_id: str = "upstream-task",
    status: str,
    run_count: int,
    review_id: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "task_id": task_id,
        "agent_name": "remote_code_wiki",
        "status": status,
        "last_result": "done" if status == "completed" else None,
        "error": None,
        "run_count": run_count,
        "created_at": "2026-08-30T00:00:00Z",
        "updated_at": "2026-08-30T00:00:01Z",
        "pending_review": None,
    }
    if review_id is not None:
        payload["pending_review"] = {
            "review_id": review_id,
            "action_requests": [],
            "review_configs": [],
        }
    return payload


def _remote_manager(
    db_path: Path,
    *,
    operation: str,
) -> tuple[TaskStore, TaskManager, str]:
    store = TaskStore(str(db_path))
    manager = TaskManager(store, settled_outbox_enabled=True)
    task_id = f"proxy-{operation}"
    upstream = None if operation == "create" else "upstream-task"
    manager.create_task_record(
        task_id,
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        route_kind="remote_ref",
        upstream_task_id=upstream,
        parent_thread_id="parent-thread",
    )
    if operation == "send":
        manager.sync_remote_task(task_id, _payload(status="completed", run_count=1))
    elif operation == "review":
        manager.sync_remote_task(
            task_id,
            _payload(
                status="waiting_for_human",
                run_count=1,
                review_id="review-1",
            ),
        )
    elif operation == "cancel":
        manager.sync_remote_task(task_id, _payload(status="running", run_count=1))
    return store, manager, task_id


@pytest.mark.parametrize("operation", ["create", "send", "review", "cancel"])
def test_restart_turns_residual_remote_operation_into_durable_unknown(
    tmp_path: Path,
    operation: str,
) -> None:
    db_path = tmp_path / f"restart-{operation}.sqlite"
    store, manager, task_id = _remote_manager(db_path, operation=operation)
    identity = "review-1" if operation == "review" else f"identity-{operation}"
    manager.begin_external_operation(task_id, operation=operation, identity=identity)
    store.close()

    restarted_store = TaskStore(str(db_path))
    restarted = TaskManager(restarted_store, settled_outbox_enabled=True)
    try:
        record = restarted.load_task_by_id(task_id)
        assert record is not None
        assert record.state == "interrupted"
        assert record.result is None
        assert record.external_operation == operation
        assert record.external_operation_identity == identity
        assert record.external_outcome_uncertain is True
        assert record.error == (
            f"Remote {operation} outcome is uncertain; refresh is required"
        )
        assert restarted_store.list_settled_outbox() == []
    finally:
        restarted_store.close()


@pytest.mark.parametrize("operation", ["create", "send", "review", "cancel"])
@pytest.mark.parametrize(
    "failure_mode",
    ["credentials", "connect", "invalid-url"],
)
@async_test
async def test_real_a2a_pre_dispatch_failure_restores_and_can_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    operation: str,
    failure_mode: str,
) -> None:
    if failure_mode == "credentials":
        monkeypatch.delenv("REMOTE_CODE_WIKI_TOKEN", raising=False)
    else:
        monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "configured-token")
    requests: list[httpx.Request] = []
    connect_attempts = 0

    def dispatch(request: httpx.Request) -> httpx.Response:
        nonlocal connect_attempts
        connect_attempts += 1
        if failure_mode == "connect" and connect_attempts == 1:
            raise httpx.ConnectError(
                "connection failed before request dispatch",
                request=request,
            )
        requests.append(request)
        if request.url.path.endswith("/input"):
            payload = _payload(status="completed", run_count=2)
        elif "/reviews/" in request.url.path:
            payload = _payload(status="running", run_count=2)
        elif request.url.path.endswith("/cancel"):
            payload = _payload(status="cancelled", run_count=1)
        else:
            payload = _payload(
                task_id="upstream-create",
                status="running",
                run_count=1,
            )
        return httpx.Response(200, json=payload)

    db_path = str(
        tmp_path / f"pre-dispatch-{failure_mode}-{operation}.sqlite"
    )
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    remote_refs = build_test_remote_refs()
    if failure_mode == "invalid-url":
        remote_refs["remote_code_wiki"].url = "not-a-valid-http-url"
    control = runtime_module.AgentControl(
        {},
        remote_refs,
        checkpointer=object(),
        backend=object(),
        a2a_client=A2AClient(
            transports={
                "https://example.com/a2a": httpx.MockTransport(dispatch),
            }
        ),
        task_store=task_store,
        mailbox=AgentMailbox(mailbox_store),
    )
    task_id = f"pre-dispatch-{operation}"
    try:
        if operation == "create":
            call = control.spawn_task(
                "remote_code_wiki",
                "dispatch",
                task_id=task_id,
                parent_thread_id="parent-thread",
            )
        else:
            manager = control._task_manager
            manager.create_task_record(
                task_id,
                "remote_code_wiki",
                parent_task_id=None,
                root_task_id=task_id,
                depth=1,
                route_kind="remote_ref",
                upstream_task_id="upstream-task",
                parent_thread_id="parent-thread",
            )
            if operation == "send":
                manager.sync_remote_task(
                    task_id,
                    _payload(status="completed", run_count=1),
                )
                call = control.send_task_input(task_id, "continue")
            elif operation == "review":
                record = manager.sync_remote_task(
                    task_id,
                    _payload(
                        status="waiting_for_human",
                        run_count=1,
                        review_id="review-1",
                    ),
                )
                assert record.pending_review is not None
                call = control.submit_review_decision(
                    "review-1",
                    [{"type": "approve"}],
                )
            else:
                manager.sync_remote_task(
                    task_id,
                    _payload(status="running", run_count=1),
                )
                call = control.cancel_task(task_id)

        with pytest.raises(A2AClientError) as raised:
            await call
        assert raised.value.effect_boundary == "not_dispatched"
        assert requests == []
        restored = control.get_task_record(task_id)
        assert restored.external_operation is None
        assert restored.external_operation_identity is None
        assert restored.external_outcome_uncertain is False
        assert restored.state == {
            "create": "pending",
            "send": "completed",
            "review": "waiting_for_human",
            "cancel": "running",
        }[operation]
        assert all(
            row["settled_status"] != "interrupted"
            for row in task_store.list_settled_outbox()
        )

        monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "fixed-token")
        if failure_mode == "invalid-url":
            remote_refs["remote_code_wiki"].url = "https://example.com/a2a"
        if operation == "create":
            retried = await control.spawn_task(
                "remote_code_wiki",
                "dispatch",
                task_id=task_id,
                parent_thread_id="parent-thread",
            )
            assert retried.upstream_task_id == "upstream-create"
        elif operation == "send":
            retried = await control.send_task_input(task_id, "continue")
            assert retried.state == "completed" and retried.run_count == 2
        elif operation == "review":
            retried = await control.submit_review_decision(
                "review-1",
                [{"type": "approve"}],
            )
            assert retried.state == "running" and retried.run_count == 2
        else:
            retried = await control.cancel_task(task_id)
            assert retried.state == "cancelled"
        assert len(requests) == 1
        assert requests[0].headers["authorization"] == "Bearer fixed-token"
    finally:
        await control.close()
        mailbox_store.close()
        task_store.close()


@pytest.mark.parametrize("operation", ["send", "review", "cancel"])
def test_refresh_requires_operation_specific_effect_evidence(
    tmp_path: Path,
    operation: str,
) -> None:
    store, manager, task_id = _remote_manager(
        tmp_path / f"evidence-{operation}.sqlite",
        operation=operation,
    )
    identity = "review-1" if operation == "review" else f"identity-{operation}"
    manager.begin_external_operation(task_id, operation=operation, identity=identity)
    try:
        if operation == "send":
            stale = _payload(status="completed", run_count=1)
            proven = _payload(status="completed", run_count=2)
        elif operation == "review":
            # A newer waiting payload can be eventual-consistency noise; it is
            # not proof that this particular Review decision took effect.
            stale = _payload(
                status="waiting_for_human",
                run_count=2,
                review_id="some-other-review",
            )
            proven = _payload(status="running", run_count=2)
        else:
            stale = _payload(status="running", run_count=2)
            proven = _payload(status="cancelled", run_count=1)

        uncertain = manager.sync_remote_task(task_id, stale)
        assert uncertain.state == "interrupted"
        assert uncertain.external_operation == operation
        assert uncertain.external_outcome_uncertain is True
        assert store.list_settled_outbox() == []

        reconciled = manager.sync_remote_task(task_id, proven)
        assert reconciled.external_operation is None
        assert reconciled.external_outcome_uncertain is False
        assert reconciled.state == proven["status"]
    finally:
        store.close()


class AmbiguousOperationClient:
    def __init__(self, operation: str, failure: str) -> None:
        self.operation = operation
        self.failure = failure
        self.sent_idempotency_key: str | None = None

    def _fail_or_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.failure == "transport":
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message="response lost after dispatch",
            )
        if self.failure == "rejected":
            raise A2AClientError(
                status_code=400,
                code="invalid_request",
                message="request was rejected before effect",
            )
        return {**payload, "status": []}

    async def create_task(self, remote_ref: Any, **kwargs: Any) -> dict[str, Any]:
        del remote_ref, kwargs
        if self.operation == "create":
            return self._fail_or_payload(_payload(status="running", run_count=1))
        status = "completed"
        review_id = None
        if self.operation == "review":
            status, review_id = "waiting_for_human", "review-1"
        elif self.operation == "cancel":
            status = "running"
        return _payload(status=status, run_count=1, review_id=review_id)

    async def send_input(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args
        self.sent_idempotency_key = kwargs["idempotency_key"]
        return self._fail_or_payload(_payload(status="completed", run_count=2))

    async def submit_review_decision(
        self, *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        del args, kwargs
        return self._fail_or_payload(_payload(status="running", run_count=2))

    async def cancel_task(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        return self._fail_or_payload(_payload(status="cancelled", run_count=1))


class KillAfterDispatchClient(AmbiguousOperationClient):
    def __init__(self, operation: str) -> None:
        super().__init__(operation, "transport")
        self.dispatched = asyncio.Event()

    async def _hang(self) -> dict[str, Any]:
        self.dispatched.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled dispatch must not return")

    async def create_task(self, remote_ref: Any, **kwargs: Any) -> dict[str, Any]:
        if self.operation == "create":
            return await self._hang()
        return await super().create_task(remote_ref, **kwargs)

    async def send_input(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if self.operation == "send":
            self.sent_idempotency_key = kwargs["idempotency_key"]
            return await self._hang()
        return await super().send_input(*args, **kwargs)

    async def submit_review_decision(
        self, *args: Any, **kwargs: Any
    ) -> dict[str, Any]:
        if self.operation == "review":
            return await self._hang()
        return await super().submit_review_decision(*args, **kwargs)

    async def cancel_task(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if self.operation == "cancel":
            return await self._hang()
        return await super().cancel_task(*args, **kwargs)


@pytest.mark.parametrize("operation", ["create", "send", "review", "cancel"])
@pytest.mark.parametrize("failure", ["transport", "invalid"])
@async_test
async def test_ambiguous_remote_failure_remains_unknown_without_settlement(
    tmp_path: Path,
    operation: str,
    failure: str,
) -> None:
    db_path = str(tmp_path / f"ambiguous-{operation}-{failure}.sqlite")
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    client = AmbiguousOperationClient(operation, failure)
    control = runtime_module.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,  # type: ignore[arg-type]
        task_store=task_store,
        mailbox=AgentMailbox(mailbox_store),
    )
    task_id = f"ambiguous-{operation}"
    try:
        if operation == "create":
            call = control.spawn_task(
                "remote_code_wiki",
                "dispatch",
                task_id=task_id,
                parent_thread_id="parent-thread",
            )
        else:
            record = await control.spawn_task(
                "remote_code_wiki",
                "seed",
                task_id=task_id,
                parent_thread_id="parent-thread",
            )
            if operation == "send":
                call = control.send_task_input(record.task_id, "continue")
            elif operation == "review":
                assert record.pending_review is not None
                call = control.submit_review_decision(
                    record.pending_review["review_id"],
                    [{"type": "approve"}],
                )
            else:
                call = control.cancel_task(record.task_id)
        expected = A2AClientError if failure == "transport" else ValueError
        with pytest.raises(expected):
            await call

        uncertain = control.get_task_record(task_id)
        assert uncertain.state == "interrupted"
        assert uncertain.external_operation == operation
        assert uncertain.external_operation_identity
        assert uncertain.external_operation_run_count is not None
        assert uncertain.external_outcome_uncertain is True
        assert task_store.list_settled_outbox() == []
        if operation == "send":
            assert client.sent_idempotency_key == uncertain.external_operation_identity
            assert client.sent_idempotency_key.startswith("ruyi-send:")
    finally:
        await control.close()
        mailbox_store.close()
        task_store.close()


@pytest.mark.parametrize("operation", ["create", "send", "review", "cancel"])
@async_test
async def test_process_loss_after_dispatch_is_unknown_after_restart(
    tmp_path: Path,
    operation: str,
) -> None:
    db_path = str(tmp_path / f"killed-{operation}.sqlite")
    task_store = TaskStore(db_path)
    client = KillAfterDispatchClient(operation)
    control = runtime_module.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,  # type: ignore[arg-type]
        task_store=task_store,
    )
    task_id = f"killed-{operation}"
    if operation == "create":
        call = control.spawn_task(
            "remote_code_wiki",
            "dispatch",
            task_id=task_id,
        )
    else:
        record = await control.spawn_task(
            "remote_code_wiki",
            "seed",
            task_id=task_id,
        )
        if operation == "send":
            call = control.send_task_input(record.task_id, "continue")
        elif operation == "review":
            assert record.pending_review is not None
            call = control.submit_review_decision(
                record.pending_review["review_id"],
                [{"type": "approve"}],
            )
        else:
            call = control.cancel_task(record.task_id)
    mutation = asyncio.create_task(call)
    await client.dispatched.wait()
    mutation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await mutation
    before_restart = task_store.get_task(task_id)
    assert before_restart is not None
    assert before_restart.external_operation == operation
    assert before_restart.external_outcome_uncertain is True
    await control.close()
    task_store.close()

    restarted_store = TaskStore(db_path)
    try:
        manager = TaskManager(restarted_store)
        restarted = manager.load_task_by_id(task_id)
        assert restarted is not None
        assert restarted.state == "interrupted"
        assert restarted.external_operation == operation
        assert restarted.external_operation_identity
        assert restarted.external_outcome_uncertain is True
    finally:
        restarted_store.close()


@pytest.mark.parametrize("operation", ["create", "send", "review", "cancel"])
@async_test
async def test_only_authoritative_rejection_clears_matching_operation(
    tmp_path: Path,
    operation: str,
) -> None:
    db_path = str(tmp_path / f"rejected-{operation}.sqlite")
    task_store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    client = AmbiguousOperationClient(operation, "rejected")
    control = runtime_module.AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,  # type: ignore[arg-type]
        task_store=task_store,
        mailbox=AgentMailbox(mailbox_store),
    )
    task_id = f"rejected-{operation}"
    try:
        if operation == "create":
            call = control.spawn_task(
                "remote_code_wiki",
                "dispatch",
                task_id=task_id,
                parent_thread_id="parent-thread",
            )
        else:
            record = await control.spawn_task(
                "remote_code_wiki",
                "seed",
                task_id=task_id,
                parent_thread_id="parent-thread",
            )
            if operation == "send":
                call = control.send_task_input(record.task_id, "continue")
            elif operation == "review":
                assert record.pending_review is not None
                call = control.submit_review_decision(
                    record.pending_review["review_id"],
                    [{"type": "approve"}],
                )
            else:
                call = control.cancel_task(record.task_id)
        with pytest.raises(A2AClientError, match="rejected before effect"):
            await call

        rejected = control.get_task_record(task_id)
        assert rejected.external_operation is None
        assert rejected.external_operation_identity is None
        assert rejected.external_operation_run_count is None
        assert rejected.external_outcome_uncertain is False
        if operation == "create":
            assert rejected.state == "failed"
        elif operation == "send":
            assert rejected.state == "completed"
            rows = task_store.list_settled_outbox()
            assert len(rows) == 1 and rows[0]["settled_status"] == "completed"
        elif operation == "review":
            assert rejected.state == "waiting_for_human"
            assert rejected.pending_review is not None
        else:
            assert rejected.state == "running"
    finally:
        await control.close()
        mailbox_store.close()
        task_store.close()


@pytest.mark.parametrize(
    "boundary",
    ["pending", "claimed", "outbox-delivered", "mailbox-delivered"],
)
def test_uncertain_create_fences_legacy_settlement_until_proven_run(
    tmp_path: Path,
    boundary: str,
) -> None:
    db_path = str(tmp_path / f"legacy-{boundary}.sqlite")
    store = TaskStore(db_path)
    mailbox_store = MailboxStore(db_path)
    mailbox = AgentMailbox(mailbox_store)
    manager = TaskManager(store, settled_outbox_enabled=True)
    task_id = "safe-create"
    manager.create_task_record(
        task_id,
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        route_kind="remote_ref",
        parent_thread_id="parent-thread",
    )
    manager.mark_interrupted(task_id, "old interrupted projection")
    claimed = None
    if boundary != "pending":
        claimed = store.claim_settled_outbox()[0]
    if boundary in {"outbox-delivered", "mailbox-delivered"}:
        assert claimed is not None
        assert mailbox.publish_claimed_settled_outbox(claimed) is True
    if boundary == "mailbox-delivered":
        delivered = mailbox.drain("parent-thread")
        assert len(delivered) == 1

    # Recreate a pre-fix crash image: the invalid interrupted intent existed
    # before durable knowledge that create crossed its effect boundary.
    legacy = store.get_task(task_id)
    assert legacy is not None
    legacy.external_operation = "create"
    legacy.external_operation_identity = "stable-create"
    legacy.external_operation_run_count = 0
    legacy.external_outcome_uncertain = True
    legacy.error = "Remote create outcome is uncertain; refresh is required"
    store.update_task(legacy)

    restarted = TaskManager(store, settled_outbox_enabled=True)
    try:
        uncertain = restarted.load_task_by_id(task_id)
        assert uncertain is not None
        old_rows = store.list_settled_outbox()
        if boundary == "mailbox-delivered":
            assert len(old_rows) == 1 and old_rows[0]["status"] == "delivered"
            assert uncertain.mailbox_delivered is True
        else:
            assert old_rows == []
            assert uncertain.mailbox_delivered is False
        assert store.claim_settled_outbox() == []

        restarted.begin_external_operation(
            task_id,
            operation="create",
            identity="stable-create",
            allow_replay=True,
        )
        restarted.bind_uncertain_remote_task(task_id, "upstream-task")
        running = restarted.sync_remote_task(
            task_id,
            _payload(status="running", run_count=1),
        )
        assert running.external_operation is None
        assert running.mailbox_delivered is False
        completed = restarted.sync_remote_task(
            task_id,
            _payload(status="completed", run_count=1),
        )
        actual = store.claim_settled_outbox()
        assert len(actual) == 1
        assert actual[0].run_count == 1
        assert actual[0].settled_status == "completed"
        assert completed.state == "completed"
    finally:
        mailbox_store.close()
        store.close()


def test_reconciled_cancel_replaces_conflicting_same_run_legacy_intent(
    tmp_path: Path,
) -> None:
    store, manager, task_id = _remote_manager(
        tmp_path / "same-run-conflict.sqlite",
        operation="cancel",
    )
    manager.mark_interrupted(task_id, "old interrupted projection")
    old = store.list_settled_outbox()
    assert len(old) == 1 and old[0]["settled_status"] == "interrupted"

    legacy = store.get_task(task_id)
    assert legacy is not None
    legacy.external_operation = "cancel"
    legacy.external_operation_identity = "upstream-task"
    legacy.external_operation_run_count = 1
    legacy.external_outcome_uncertain = True
    legacy.error = "Remote cancel outcome is uncertain; refresh is required"
    store.update_task(legacy)

    restarted = TaskManager(store, settled_outbox_enabled=True)
    try:
        uncertain = restarted.load_task_by_id(task_id)
        assert uncertain is not None and uncertain.external_outcome_uncertain
        assert store.list_settled_outbox() == []
        cancelled = restarted.sync_remote_task(
            task_id,
            _payload(status="cancelled", run_count=1),
        )
        rows = store.list_settled_outbox()
        assert cancelled.state == "cancelled"
        assert len(rows) == 1
        assert rows[0]["outbox_key"] == old[0]["outbox_key"]
        assert rows[0]["settled_status"] == "cancelled"
        assert rows[0]["status"] == "pending"
    finally:
        store.close()
