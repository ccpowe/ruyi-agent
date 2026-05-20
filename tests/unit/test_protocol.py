from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pydantic import TypeAdapter

from ruyi_agent.control_plane.contracts import (
    AnyProtocolCommand,
    AnyProtocolEvent,
    ContentDeltaEvent,
    ContentDeltaPayload,
    RuntimeSnapshot,
    SendUserMessageCommand,
    SubmitReviewDecisionCommand,
    SubmitReviewDecisionPayload,
    TaskSnapshot,
    TaskStatus,
    ReviewDecision,
    ReviewDecisionKind,
    ReviewSnapshot,
    TaskUpdatedEvent,
    TaskUpdatedPayload,
)


def test_command_union_round_trips_by_kind() -> None:
    adapter = TypeAdapter(AnyProtocolCommand)
    command = adapter.validate_python(
        {
            "kind": "submit_review_decision",
            "review_id": "review-1",
            "payload": {
                "decisions": [
                    {
                        "action_id": "call-1",
                        "decision": "edit",
                        "edited_args": {"command": "git status"},
                    }
                ]
            },
        }
    )

    assert isinstance(command, SubmitReviewDecisionCommand)
    assert command.protocol_version == "v1"
    assert command.payload.decisions[0].decision is ReviewDecisionKind.EDIT


def test_command_json_is_stable() -> None:
    command = SendUserMessageCommand(
        id="command-1",
        created_at=datetime(2026, 5, 8, tzinfo=UTC),
        thread_id="thread-1",
        payload={"content": "hello", "agent_name": "main"},
    )

    assert command.model_dump(mode="json") == {
        "protocol_version": "v1",
        "id": "command-1",
        "kind": "send_user_message",
        "created_at": "2026-05-08T00:00:00Z",
        "thread_id": "thread-1",
        "task_id": None,
        "review_id": None,
        "correlation_id": None,
        "metadata": {},
        "payload": {"content": "hello", "agent_name": "main"},
    }


def test_event_union_round_trips_by_kind() -> None:
    adapter = TypeAdapter(AnyProtocolEvent)
    event = adapter.validate_python(
        {
            "kind": "task_updated",
            "seq": 7,
            "task_id": "task-1",
            "payload": {
                "task_id": "task-1",
                "agent_name": "worker",
                "status": "waiting_for_human",
                "run_count": 1,
                "updated_at": "2026-05-08T00:00:00Z",
            },
        }
    )

    assert isinstance(event, TaskUpdatedEvent)
    assert event.payload.status is TaskStatus.WAITING_FOR_HUMAN


def test_event_json_is_stable() -> None:
    event = ContentDeltaEvent(
        id="event-1",
        seq=3,
        created_at=datetime(2026, 5, 8, tzinfo=UTC),
        thread_id="thread-1",
        correlation_id="command-1",
        payload=ContentDeltaPayload(delta="partial output"),
    )

    assert event.model_dump(mode="json") == {
        "protocol_version": "v1",
        "id": "event-1",
        "kind": "content_delta",
        "created_at": "2026-05-08T00:00:00Z",
        "thread_id": "thread-1",
        "task_id": None,
        "review_id": None,
        "correlation_id": "command-1",
        "metadata": {},
        "seq": 3,
        "payload": {
            "delta": "partial output",
            "role": "assistant",
            "agent_name": None,
            "channel": None,
        },
    }


def test_runtime_snapshot_groups_tasks_reviews_and_audit() -> None:
    now = datetime(2026, 5, 8, tzinfo=UTC)
    snapshot = RuntimeSnapshot(
        snapshot_id="snapshot-1",
        created_at=now,
        active_thread_id="thread-1",
        tasks=[
            TaskSnapshot(
                task_id="task-1",
                agent_name="worker",
                status=TaskStatus.WAITING_FOR_HUMAN,
                thread_id="thread-1",
                run_count=1,
                created_at=now,
                updated_at=now,
            )
        ],
        pending_reviews=[
            ReviewSnapshot(
                review_id="review-1",
                task_id="task-1",
                thread_id="thread-1",
                allowed_decisions=[
                    ReviewDecisionKind.APPROVE,
                    ReviewDecisionKind.REJECT,
                ],
                created_at=now,
                updated_at=now,
            )
        ],
    )

    assert snapshot.protocol_version == "v1"
    assert snapshot.tasks[0].status is TaskStatus.WAITING_FOR_HUMAN
    assert snapshot.pending_reviews[0].allowed_decisions == [
        ReviewDecisionKind.APPROVE,
        ReviewDecisionKind.REJECT,
    ]


def test_payload_models_accept_instances() -> None:
    command = SubmitReviewDecisionCommand(
        review_id="review-1",
        payload=SubmitReviewDecisionPayload(
            decisions=[
                ReviewDecision(
                    action_id="call-1",
                    decision=ReviewDecisionKind.APPROVE,
                )
            ]
        ),
    )

    event = TaskUpdatedEvent(
        seq=1,
        task_id="task-1",
        payload=TaskUpdatedPayload(
            task_id="task-1",
            agent_name="worker",
            status=TaskStatus.COMPLETED,
            run_count=2,
            updated_at=datetime(2026, 5, 8, tzinfo=UTC),
        ),
    )

    assert command.payload.decisions[0].decision is ReviewDecisionKind.APPROVE
    assert event.payload.status is TaskStatus.COMPLETED
