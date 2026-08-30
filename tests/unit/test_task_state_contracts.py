from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

import pytest
from fastapi.testclient import TestClient

import ruyi_agent.gateway_protocol.dto as channel_dto
import ruyi_agent.channels.task_watch as task_watch
import ruyi_agent.channels.turn as channel_turn
import ruyi_agent.gateway_protocol.dto as gateway_models
import ruyi_agent.runtime.mailbox.service as mailbox_service
from ruyi_agent.channels.gateway_client import (
    GatewayClientError,
    gateway_task_from_payload,
)
from ruyi_agent.gateway_protocol.dto import TaskResponse, TaskWebhookEvent
from ruyi_agent.gateway_protocol.sse import GatewayTaskEvent, task_stream_event_from_gateway
from ruyi_agent.storage.task_codecs import row_to_task_record, task_record_values
from ruyi_agent.task_models import (
    EXECUTING_TASK_STATES,
    SETTLED_TASK_STATES,
    TASK_STATES,
    TaskRecord,
    TaskState,
    parse_task_state,
)
from tests.unit.gateway_http_support import auth_headers, build_app


class WireTaskState(StrEnum):
    COMPLETED = "completed"


def _task_response_fields() -> dict[str, object]:
    timestamp = datetime(2026, 8, 30, tzinfo=UTC)
    return {
        "task_id": "task-1",
        "agent_name": "main",
        "parent_task_id": None,
        "root_task_id": "task-1",
        "depth": 0,
        "last_result": "done",
        "error": None,
        "run_count": 1,
        "created_at": timestamp,
        "updated_at": timestamp,
        "metadata": {},
    }


def _stored_task_record() -> TaskRecord:
    timestamp = datetime(2026, 8, 30, tzinfo=UTC)
    return TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="completed",
        thread_id="thread-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=timestamp,
        updated_at=timestamp,
    )


def test_task_state_parser_normalizes_str_enum_to_plain_string() -> None:
    assert parse_task_state("completed") == "completed"
    parsed = parse_task_state(WireTaskState.COMPLETED)
    assert parsed == "completed"
    assert type(parsed) is str


def test_task_state_public_imports_and_sets_share_identity() -> None:
    assert gateway_models.TaskState is TaskState
    assert channel_dto.TaskState is TaskState
    assert mailbox_service.TaskSettledStatus is TaskState
    assert task_watch.TERMINAL_TASK_STATES is SETTLED_TASK_STATES
    assert channel_turn.SETTLED_RUN_STATES is SETTLED_TASK_STATES
    assert channel_turn.ACTIVE_RUN_STATES is EXECUTING_TASK_STATES


def test_persistence_decoder_rejects_unknown_task_state() -> None:
    row = list(task_record_values(_stored_task_record()))
    row[2] = "future_state"

    with pytest.raises(ValueError, match="Stored Task state must be one of"):
        row_to_task_record(tuple(row))


def test_gateway_and_channel_models_normalize_str_enum_status() -> None:
    response = TaskResponse(
        status=WireTaskState.COMPLETED,
        **_task_response_fields(),
    )
    webhook = TaskWebhookEvent(
        event_id="event-1",
        event_type="task.completed",
        status=WireTaskState.COMPLETED,
        **{
            key: value
            for key, value in _task_response_fields().items()
            if key not in {"parent_task_id", "root_task_id", "depth", "metadata"}
        },
    )
    channel_task = gateway_task_from_payload(
        {
            "task_id": "task-1",
            "status": WireTaskState.COMPLETED,
            "run_count": 1,
        }
    )

    assert type(response.status) is str
    assert type(webhook.status) is str
    assert type(channel_task.status) is str
    assert response.model_dump(mode="json")["status"] == "completed"
    assert channel_task.to_payload()["status"] == "completed"


def test_channel_dto_rejects_unknown_task_state() -> None:
    with pytest.raises(GatewayClientError, match="invalid Task payload"):
        gateway_task_from_payload(
            {"task_id": "task-1", "status": "future_state", "run_count": 1}
        )


def test_sse_parser_normalizes_str_enum_status() -> None:
    event = task_stream_event_from_gateway(
        GatewayTaskEvent(
            event_type="task.completed",
            event_id="cursor-1",
            data={
                "task_id": "remote-1",
                "run_count": 1,
                "created_at": "2026-08-30T00:00:00+00:00",
                "status": WireTaskState.COMPLETED,
                "last_result": "done",
                "error": None,
                "updated_at": "2026-08-30T00:00:00+00:00",
                "pending_review": None,
                "artifacts": [],
            },
        ),
        expected_task_id="remote-1",
        public_task_id="local-1",
        run_count=1,
    )

    assert event.data["status"] == "completed"
    assert type(event.data["status"]) is str


def test_task_state_json_schemas_keep_the_wire_vocabulary() -> None:
    response_schema = TaskResponse.model_json_schema()
    channel_schema = channel_dto.GatewayTask.model_json_schema()

    assert set(response_schema["properties"]["status"]["enum"]) == TASK_STATES
    assert set(channel_schema["properties"]["status"]["enum"]) == TASK_STATES


def test_gateway_listing_rejects_unknown_task_state_filter(monkeypatch) -> None:
    app, _factory = build_app(monkeypatch, delay=0)

    with TestClient(app) as client:
        response = client.get(
            "/tasks?status=future_state",
            headers=auth_headers(),
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
