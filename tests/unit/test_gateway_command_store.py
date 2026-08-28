from __future__ import annotations

import sqlite3

import pytest

from ruyi_agent.storage.gateway_command_store import (
    GatewayCommandConflictError,
    GatewayCommandStore,
)


def _claim(
    store: GatewayCommandStore,
    *,
    key: str = "request-1",
    operation: str = "create_task",
    target: str = "main",
    request_hash: str = "hash-1",
):
    return store.claim(
        principal_id="gateway-bearer",
        idempotency_key=key,
        operation=operation,
        target=target,
        request_hash=request_hash,
        proposed_task_id="task-proposed",
        proposed_mailbox_message_id="message-proposed",
    )


def test_gateway_command_store_replays_completed_response(tmp_path) -> None:
    store = GatewayCommandStore(str(tmp_path / "gateway.sqlite"))
    try:
        acquired = _claim(store)
        assert acquired.status == "acquired"
        assert acquired.task_id == "task-proposed"
        assert acquired.mailbox_message_id == "message-proposed"
        assert acquired.claim_token is not None

        store.complete(
            command_id=acquired.command_id,
            claim_token=acquired.claim_token,
            response_json='{"task_id":"task-proposed"}',
        )

        replay = _claim(store)
        assert replay.status == "replay"
        assert replay.command_id == acquired.command_id
        assert replay.task_id == acquired.task_id
        assert replay.response_json == '{"task_id":"task-proposed"}'
        assert store.count_commands() == 1
    finally:
        store.close()


def test_gateway_command_key_is_unique_across_operations_and_targets(tmp_path) -> None:
    store = GatewayCommandStore(str(tmp_path / "gateway.sqlite"))
    try:
        _claim(store)

        with pytest.raises(GatewayCommandConflictError):
            _claim(store, operation="send_input", target="task-proposed")
        with pytest.raises(GatewayCommandConflictError):
            _claim(store, target="other-agent")
        with pytest.raises(GatewayCommandConflictError):
            _claim(store, request_hash="different-hash")
    finally:
        store.close()


def test_gateway_command_reports_live_claim_and_can_be_released(tmp_path) -> None:
    store = GatewayCommandStore(str(tmp_path / "gateway.sqlite"))
    try:
        acquired = _claim(store)
        assert _claim(store).status == "busy"
        assert acquired.claim_token is not None

        store.release(
            command_id=acquired.command_id,
            claim_token=acquired.claim_token,
        )
        reacquired = _claim(store)

        assert reacquired.status == "acquired"
        assert reacquired.command_id == acquired.command_id
        assert reacquired.task_id == acquired.task_id
        assert reacquired.claim_token != acquired.claim_token
    finally:
        store.close()


def test_gateway_command_store_recovers_interrupted_claim_on_restart(tmp_path) -> None:
    path = str(tmp_path / "gateway.sqlite")
    first = GatewayCommandStore(path)
    acquired = _claim(first)
    first.close()

    second = GatewayCommandStore(path)
    try:
        recovered = _claim(second)
        assert recovered.status == "acquired"
        assert recovered.command_id == acquired.command_id
        assert recovered.task_id == acquired.task_id
    finally:
        second.close()


def test_gateway_command_store_preserves_principal_partition(tmp_path) -> None:
    store = GatewayCommandStore(str(tmp_path / "gateway.sqlite"))
    try:
        _claim(store)
        other = store.claim(
            principal_id="future-user-2",
            idempotency_key="request-1",
            operation="create_task",
            target="main",
            request_hash="hash-1",
            proposed_task_id="task-2",
        )

        assert other.status == "acquired"
        assert other.task_id == "task-2"
        assert store.count_commands() == 2
    finally:
        store.close()


def test_gateway_command_schema_enforces_principal_key_uniqueness(tmp_path) -> None:
    path = str(tmp_path / "gateway.sqlite")
    store = GatewayCommandStore(path)
    try:
        _claim(store)
        with sqlite3.connect(path) as conn, pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                """
                INSERT INTO gateway_commands (
                    command_id, principal_id, idempotency_key, operation,
                    target, request_hash, state, task_id, created_at, updated_at
                ) VALUES ('other', 'gateway-bearer', 'request-1', 'create_task',
                    'main', 'hash-1', 'pending', 'task-2', 'now', 'now')
                """
            )
    finally:
        store.close()
