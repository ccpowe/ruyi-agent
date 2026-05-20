from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ruyi_agent.runtime.delegation.async_runtime import TaskRecord
from ruyi_agent.control_plane.controller import ProtocolController, RootAgentTurnResult
from ruyi_agent.control_plane.contracts import (
    ReviewDecision,
    ReviewDecisionKind,
    SendUserMessageCommand,
    SubmitReviewDecisionCommand,
    SubmitReviewDecisionPayload,
    TaskStatus,
)


NOW = datetime(2026, 5, 8, tzinfo=UTC)


def make_record(
    *,
    task_id: str = "task-1",
    state: str = "running",
    result: str | None = None,
    pending_review: dict[str, Any] | None = None,
) -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        agent_name="worker",
        state=state,
        thread_id=f"thread-{task_id}",
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        created_at=NOW,
        updated_at=NOW,
        result=result,
        error=None,
        run_count=1,
        pending_review=pending_review,
    )


def make_pending_review() -> dict[str, Any]:
    return {
        "review_id": "review-1",
        "action_requests": [
            {
                "tool_call_id": "call-1",
                "name": "execute",
                "args": {"command": "rm -rf tmp"},
                "description": "Tool execution requires approval.",
                "risk": "destructive_filesystem",
                "reason": "rm requires review",
            }
        ],
        "review_configs": [
            {
                "action_name": "execute",
                "allowed_decisions": ["approve", "edit", "reject"],
            }
        ],
    }


class FakeControl:
    def __init__(self, record: TaskRecord) -> None:
        self.record = record
        self.spawn_calls: list[dict[str, Any]] = []
        self.sent_inputs: list[dict[str, Any]] = []
        self.submitted_decisions: list[list[dict[str, Any]]] = []

    async def spawn_task(
        self,
        agent_name: str,
        task: str,
        **kwargs: Any,
    ) -> TaskRecord:
        self.spawn_calls.append(
            {
                "agent_name": agent_name,
                "task": task,
                "kwargs": kwargs,
            }
        )
        return self.record

    async def refresh_task(self, task_id: str) -> TaskRecord:
        assert task_id == self.record.task_id
        return self.record

    async def send_task_input(
        self,
        task_id: str,
        message: str,
    ) -> TaskRecord:
        self.sent_inputs.append(
            {
                "task_id": task_id,
                "message": message,
            }
        )
        self.record = make_record(
            task_id=task_id,
            state="completed",
            result=f"continued: {message}",
        )
        return self.record

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
        *,
        wait: bool = False,
    ) -> TaskRecord:
        assert review_id == "review-1"
        self.submitted_decisions.append(decisions)
        self.record = make_record(
            task_id=self.record.task_id,
            state="completed",
            result="resumed done",
        )
        return self.record

    def get_task_by_review_id(self, review_id: str) -> TaskRecord:
        assert review_id == "review-1"
        return self.record

    def list_task_records(self) -> list[TaskRecord]:
        return [self.record]

    def list_pending_review_records(self) -> list[TaskRecord]:
        if self.record.state == "waiting_for_human":
            return [self.record]
        return []


class FakeRootRunner:
    def __init__(
        self,
        results: list[RootAgentTurnResult],
    ) -> None:
        self.results = list(results)
        self.run_calls: list[dict[str, Any]] = []
        self.resume_calls: list[dict[str, Any]] = []

    async def run_user_message(
        self,
        *,
        agent_name: str,
        thread_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> RootAgentTurnResult:
        self.run_calls.append(
            {
                "agent_name": agent_name,
                "thread_id": thread_id,
                "content": content,
                "metadata": metadata,
            }
        )
        return self.results.pop(0)

    async def resume_review(
        self,
        *,
        agent_name: str,
        thread_id: str,
        decisions: list[dict[str, Any]],
    ) -> RootAgentTurnResult:
        self.resume_calls.append(
            {
                "agent_name": agent_name,
                "thread_id": thread_id,
                "decisions": decisions,
            }
        )
        return self.results.pop(0)


def test_send_user_message_runs_root_agent_with_original_thread_id() -> None:
    control = FakeControl(make_record(state="completed", result="worker done"))
    runner = FakeRootRunner(
        [
            RootAgentTurnResult(
                agent_name="main",
                thread_id="thread-root",
                content="assistant reply",
            )
        ]
    )
    controller = ProtocolController(
        control=control,  # type: ignore[arg-type]
        root_runner=runner,
        default_agent_name="main",
    )
    command = SendUserMessageCommand(
        id="command-1",
        thread_id="thread-root",
        metadata={"source": "test"},
        payload={"content": "delete tmp", "agent_name": "worker"},
    )

    events = asyncio.run(controller.handle_command(command))

    assert [event.kind for event in events] == [
        "command_accepted",
        "content_delta",
    ]
    assert events[0].correlation_id == "command-1"
    assert events[1].payload.delta == "assistant reply"
    assert runner.run_calls == [
        {
            "agent_name": "worker",
            "thread_id": "thread-root",
            "content": "delete tmp",
            "metadata": {"source": "test"},
        }
    ]
    assert control.spawn_calls == []


def test_send_user_message_projects_root_review_request() -> None:
    control = FakeControl(make_record(state="completed", result="worker done"))
    runner = FakeRootRunner(
        [
            RootAgentTurnResult(
                agent_name="main",
                thread_id="thread-root",
                interrupt_requests=[make_pending_review()],
            )
        ]
    )
    controller = ProtocolController(
        control=control,  # type: ignore[arg-type]
        root_runner=runner,
        default_agent_name="main",
    )
    command = SendUserMessageCommand(
        id="command-1",
        thread_id="thread-root",
        payload={"content": "delete tmp", "agent_name": "main"},
    )

    events = asyncio.run(controller.handle_command(command))

    assert [event.kind for event in events] == [
        "command_accepted",
        "review_requested",
    ]
    assert events[1].review_id == "review-1"
    assert events[1].payload.actions[0].tool_name == "execute"
    assert events[1].payload.actions[0].allowed_decisions == [
        ReviewDecisionKind.APPROVE,
        ReviewDecisionKind.EDIT,
        ReviewDecisionKind.REJECT,
    ]


def test_submit_review_decision_converts_protocol_decision_to_runtime_payload() -> None:
    control = FakeControl(
        make_record(
            state="waiting_for_human",
            pending_review=make_pending_review(),
        )
    )
    controller = ProtocolController(
        control=control,  # type: ignore[arg-type]
        root_runner=FakeRootRunner([]),
        default_agent_name="main",
    )
    command = SubmitReviewDecisionCommand(
        id="command-2",
        review_id="review-1",
        payload=SubmitReviewDecisionPayload(
            decisions=[
                ReviewDecision(
                    action_id="call-1",
                    decision=ReviewDecisionKind.EDIT,
                    edited_args={"command": "ls tmp"},
                )
            ]
        ),
    )

    events = asyncio.run(controller.handle_command(command))

    assert control.submitted_decisions == [
        [
            {
                "type": "edit",
                "edited_action": {
                    "name": "execute",
                    "args": {"command": "ls tmp"},
                },
            }
        ]
    ]
    assert [event.kind for event in events] == [
        "command_accepted",
        "review_resolved",
        "task_updated",
        "content_delta",
    ]
    assert events[1].payload.decisions[0].decision is ReviewDecisionKind.EDIT
    assert events[2].payload.status is TaskStatus.COMPLETED
    assert events[3].payload.delta == "resumed done"


def test_submit_root_review_decision_resumes_same_thread() -> None:
    control = FakeControl(make_record(state="completed", result="worker done"))
    runner = FakeRootRunner(
        [
            RootAgentTurnResult(
                agent_name="main",
                thread_id="thread-root",
                interrupt_requests=[make_pending_review()],
            ),
            RootAgentTurnResult(
                agent_name="main",
                thread_id="thread-root",
                content="resumed root done",
            ),
        ]
    )
    controller = ProtocolController(
        control=control,  # type: ignore[arg-type]
        root_runner=runner,
        default_agent_name="main",
    )
    start = SendUserMessageCommand(
        id="command-1",
        thread_id="thread-root",
        payload={"content": "needs review", "agent_name": "main"},
    )
    asyncio.run(controller.handle_command(start))

    resume = SubmitReviewDecisionCommand(
        id="command-2",
        review_id="review-1",
        payload=SubmitReviewDecisionPayload(
            decisions=[
                ReviewDecision(
                    action_id="call-1",
                    decision=ReviewDecisionKind.EDIT,
                    edited_args={"command": "ls tmp"},
                )
            ]
        ),
    )
    events = asyncio.run(controller.handle_command(resume))

    assert runner.resume_calls == [
        {
            "agent_name": "main",
            "thread_id": "thread-root",
            "decisions": [
                {
                    "type": "edit",
                    "edited_action": {
                        "name": "execute",
                        "args": {"command": "ls tmp"},
                    },
                }
            ],
        }
    ]
    assert [event.kind for event in events] == [
        "command_accepted",
        "review_resolved",
        "content_delta",
    ]
    assert events[2].payload.delta == "resumed root done"


def test_snapshot_projects_tasks_and_pending_reviews() -> None:
    control = FakeControl(
        make_record(
            state="waiting_for_human",
            pending_review=make_pending_review(),
        )
    )
    controller = ProtocolController(
        control=control,  # type: ignore[arg-type]
        root_runner=FakeRootRunner([]),
        default_agent_name="main",
    )

    snapshot = controller.snapshot()

    assert snapshot.tasks[0].task_id == "task-1"
    assert snapshot.tasks[0].status is TaskStatus.WAITING_FOR_HUMAN
    assert snapshot.pending_reviews[0].review_id == "review-1"
    assert snapshot.pending_reviews[0].actions[0].risk == "destructive_filesystem"
