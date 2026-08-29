from __future__ import annotations

import asyncio
from typing import Any

import pytest

from ruyi_agent.channels.gateway_client import GatewayClientError
from ruyi_agent.channels.turn import (
    AgentCommandTurn,
    ChannelTurnHandler,
    ChannelTurnIdempotencyConflictError,
    InboundTurn,
    ResumeCommandTurn,
    ReviewTurn,
    parse_review_command,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


class FakeGatewayClient:
    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.listed_tasks: list[dict[str, Any]] = []
        self.created: list[dict[str, Any]] = []
        self.sent: list[dict[str, Any]] = []
        self.submitted_reviews: list[dict[str, Any]] = []
        self.agents: list[dict[str, Any]] = [
            {
                "name": "main",
                "public": True,
                "description": "main agent",
            },
            {
                "name": "background_research",
                "public": True,
                "description": "research agent",
            },
            {
                "name": "private_worker",
                "public": False,
                "description": "private",
            },
        ]

    async def list_agents(self) -> list[dict[str, Any]]:
        return list(self.agents)

    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        try:
            return self.tasks[task_id]
        except KeyError as exc:
            raise GatewayClientError(
                status_code=404,
                code="task_not_found",
                message="missing",
            ) from exc

    async def list_tasks(self, **kwargs) -> list[dict[str, Any]]:
        return list(self.listed_tasks)

    async def create_task(self, **kwargs) -> dict[str, Any]:
        self.created.append(kwargs)
        task = {
            "task_id": f"task-{len(self.created)}",
            "status": "running",
            "run_count": 1,
        }
        self.tasks[str(task["task_id"])] = task
        return task

    async def send_input(self, **kwargs) -> dict[str, Any]:
        self.sent.append(kwargs)
        task = {
            "task_id": kwargs["task_id"],
            "status": "running",
            "run_count": 2,
        }
        self.tasks[str(task["task_id"])] = task
        return task

    async def submit_review_decision(self, **kwargs) -> dict[str, Any]:
        self.submitted_reviews.append(kwargs)
        task = dict(self.tasks[kwargs["task_id"]])
        task["status"] = "running"
        task["pending_review"] = None
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[str(task["task_id"])] = task
        return task


class LegacyGatewayClient(FakeGatewayClient):
    """Gateway test double with the pre-idempotency mutation signatures."""

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        return await super().create_task(
            agent_name=agent_name,
            content=content,
            metadata=metadata,
            attachments=attachments,
        )

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        return await super().send_input(
            task_id=task_id,
            content=content,
            attachments=attachments,
        )


def make_turn(
    *,
    force_new: bool = False,
    idempotency_key: str | None = None,
    content: str = "hello",
) -> InboundTurn:
    return InboundTurn(
        platform="test",
        session_key="session-1",
        agent_name="main",
        content=content,
        metadata={"channel_session_key": "session-1"},
        fallback_metadata={"channel": "test", "user_id": "user-1"},
        chat_id="chat-1",
        user_id="user-1",
        attachments=[{"name": "note.txt"}],
        force_new=force_new,
        idempotency_key=idempotency_key,
    )


def make_review_turn(command: dict[str, Any]) -> ReviewTurn:
    return ReviewTurn(
        platform="test",
        session_key="session-1",
        default_agent_name="main",
        fallback_metadata={"channel_session_key": "session-1"},
        fallback_agent_name=None,
        chat_id="chat-1",
        user_id="user-1",
        thread_id=None,
        command=command,
    )


def test_channel_turn_creates_and_binds_new_task() -> None:
    gateway = FakeGatewayClient()
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(handler.handle(make_turn()))

    assert result.kind == "started"
    assert result.created is True
    assert gateway.created[0]["attachments"] == [{"name": "note.txt"}]
    assert sessions.get_session("session-1").current_task_id == "task-1"
    sessions.close()


def test_channel_turn_continues_settled_session_task() -> None:
    gateway = FakeGatewayClient()
    gateway.tasks["task-existing"] = {
        "task_id": "task-existing",
        "status": "completed",
        "run_count": 1,
    }
    sessions = ChannelSessionStore(":memory:")
    sessions.bind_session(
        session_key="session-1",
        platform="test",
        agent_name="main",
        current_task_id="task-existing",
    )
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)
    observed: list[str] = []

    async def before_continue(task: dict[str, Any]) -> None:
        observed.append(str(task["task_id"]))

    result = asyncio.run(handler.handle(make_turn(), before_continue=before_continue))

    assert result.kind == "started"
    assert result.created is False
    assert observed == ["task-existing"]
    assert gateway.sent[0]["task_id"] == "task-existing"
    sessions.close()


def test_channel_turn_returns_active_or_pending_review_without_starting_run() -> None:
    async def scenario(status: str, pending_review: dict | None = None) -> str:
        gateway = FakeGatewayClient()
        gateway.tasks["task-existing"] = {
            "task_id": "task-existing",
            "status": status,
            "run_count": 1,
            "pending_review": pending_review,
        }
        sessions = ChannelSessionStore(":memory:")
        sessions.bind_session(
            session_key="session-1",
            platform="test",
            agent_name="main",
            current_task_id="task-existing",
        )
        handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)
        result = await handler.handle(make_turn())
        assert gateway.created == []
        assert gateway.sent == []
        sessions.close()
        return result.kind

    assert asyncio.run(scenario("running")) == "active"
    assert asyncio.run(scenario("waiting_for_human", {"review_id": "review-1"})) == (
        "pending_review"
    )


def test_channel_turn_recovers_stale_session_from_latest_task() -> None:
    gateway = FakeGatewayClient()
    gateway.listed_tasks = [
        {"task_id": "task-latest", "status": "completed", "run_count": 1}
    ]
    sessions = ChannelSessionStore(":memory:")
    sessions.bind_session(
        session_key="session-1",
        platform="test",
        agent_name="main",
        current_task_id="task-missing",
    )
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(handler.handle(make_turn()))

    assert result.kind == "started"
    assert gateway.sent[0]["task_id"] == "task-latest"
    assert sessions.get_session("session-1").current_task_id == "task-latest"
    sessions.close()


def test_channel_turn_force_new_ignores_existing_session() -> None:
    gateway = FakeGatewayClient()
    gateway.tasks["task-existing"] = {
        "task_id": "task-existing",
        "status": "completed",
        "run_count": 1,
    }
    sessions = ChannelSessionStore(":memory:")
    sessions.bind_session(
        session_key="session-1",
        platform="test",
        agent_name="main",
        current_task_id="task-existing",
    )
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(handler.handle(make_turn(force_new=True)))

    assert result.created is True
    assert gateway.sent == []
    assert sessions.get_session("session-1").current_task_id == "task-1"
    sessions.close()


def test_channel_turn_replays_persisted_receipt_before_operation_selection(
    tmp_path,
) -> None:
    gateway = FakeGatewayClient()
    db_path = tmp_path / "channel_sessions.sqlite3"
    first_store = ChannelSessionStore(str(db_path))
    first_handler = ChannelTurnHandler(
        gateway_client=gateway,
        session_store=first_store,
    )
    turn = make_turn(idempotency_key="feishu:event:event-1")

    first = asyncio.run(first_handler.handle(turn))
    original_response = dict(first.task)
    receipt = first_store.get_turn_receipt("feishu:event:event-1")
    assert receipt is not None
    assert receipt.operation == "create"
    assert len(receipt.request_hash) == 64
    assert receipt.task_id == "task-1"
    assert receipt.response == original_response
    first_store.close()

    # Model a retry after the Gateway result was committed and the Task settled,
    # but before the platform event store was marked processed.
    gateway.tasks["task-1"] = {
        "task_id": "task-1",
        "status": "completed",
        "run_count": 1,
    }
    second_store = ChannelSessionStore(str(db_path))
    second_handler = ChannelTurnHandler(
        gateway_client=gateway,
        session_store=second_store,
    )

    replay = asyncio.run(second_handler.handle(turn))

    assert replay.kind == "started"
    assert replay.created is True
    assert replay.task.to_payload() == original_response
    assert len(gateway.created) == 1
    assert gateway.sent == []
    second_store.close()


def test_channel_turn_rejects_key_reuse_for_different_request() -> None:
    gateway = FakeGatewayClient()
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)
    key = "feishu:event:event-1"

    asyncio.run(handler.handle(make_turn(idempotency_key=key)))

    with pytest.raises(
        ChannelTurnIdempotencyConflictError,
        match="different request",
    ):
        asyncio.run(
            handler.handle(make_turn(idempotency_key=key, content="different content"))
        )

    assert len(gateway.created) == 1
    assert gateway.sent == []
    assert sessions.get_session("session-1").current_task_id == "task-1"
    sessions.close()


def test_channel_turn_receipt_conflict_rolls_back_session_binding() -> None:
    sessions = ChannelSessionStore(":memory:")
    sessions.bind_session(
        session_key="session-1",
        platform="test",
        agent_name="main",
        current_task_id="task-1",
        turn_idempotency_key="event-1",
        turn_operation="create",
        turn_request_hash="hash-1",
        turn_response={"task_id": "task-1", "status": "running"},
    )

    with pytest.raises(ValueError, match="receipt conflict"):
        sessions.bind_session(
            session_key="session-1",
            platform="test",
            agent_name="main",
            current_task_id="task-2",
            turn_idempotency_key="event-1",
            turn_operation="send",
            turn_request_hash="hash-2",
            turn_response={"task_id": "task-2", "status": "running"},
        )

    session = sessions.get_session("session-1")
    receipt = sessions.get_turn_receipt("event-1")
    assert session is not None and session.current_task_id == "task-1"
    assert receipt is not None and receipt.task_id == "task-1"
    sessions.close()


def test_channel_turn_omits_absent_idempotency_key_for_legacy_clients() -> None:
    gateway = LegacyGatewayClient()
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    created = asyncio.run(handler.handle(make_turn()))
    gateway.tasks[str(created.task["task_id"])] = {
        **created.task,
        "status": "completed",
    }
    continued = asyncio.run(handler.handle(make_turn()))
    agent_started = asyncio.run(
        handler.handle_agent_command(
            make_agent_turn("/agent background_research investigate")
        )
    )

    assert created.created is True
    assert continued.created is False
    assert agent_started.kind == "started"
    assert len(gateway.created) == 2
    assert len(gateway.sent) == 1
    sessions.close()


def test_parse_review_command_supports_bot_suffix_and_reject_message() -> None:
    assert parse_review_command("/approve@ruyi_bot review-1") == {
        "type": "approve",
        "review_id": "review-1",
    }
    assert parse_review_command("/reject@ruyi_bot review-1 unsafe") == {
        "type": "reject",
        "review_id": "review-1",
        "message": "unsafe",
    }


def test_channel_turn_submits_review_and_rebinds_session() -> None:
    gateway = FakeGatewayClient()
    gateway.tasks["task-review"] = {
        "task_id": "task-review",
        "agent_name": "research",
        "status": "waiting_for_human",
        "run_count": 1,
        "pending_review": {"review_id": "review-1"},
    }
    gateway.listed_tasks = [gateway.tasks["task-review"]]
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(
        handler.handle_review(make_review_turn({"type": "reject", "message": "unsafe"}))
    )

    assert result.kind == "submitted"
    assert result.task is not None
    assert gateway.submitted_reviews == [
        {
            "task_id": "task-review",
            "review_id": "review-1",
            "decisions": [{"type": "reject", "message": "unsafe"}],
        }
    ]
    session = sessions.get_session("session-1")
    assert session is not None
    assert session.current_task_id == "task-review"
    assert session.agent_name == "research"
    sessions.close()


def test_channel_turn_review_reports_validation_failures() -> None:
    async def scenario(task: dict[str, Any] | None, command: dict[str, Any]) -> str:
        gateway = FakeGatewayClient()
        gateway.listed_tasks = [task] if task is not None else []
        sessions = ChannelSessionStore(":memory:")
        handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)
        result = await handler.handle_review(make_review_turn(command))
        sessions.close()
        return result.kind

    assert asyncio.run(scenario(None, {"type": "approve"})) == "no_task"
    assert (
        asyncio.run(
            scenario(
                {"task_id": "task-1", "status": "completed", "run_count": 1},
                {"type": "approve"},
            )
        )
        == "no_pending_review"
    )
    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(
            scenario(
                {
                    "task_id": "task-1",
                    "status": "waiting_for_human",
                    "run_count": 1,
                    "pending_review": {},
                },
                {"type": "approve"},
            )
        )
    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "gateway_error"
    assert (
        asyncio.run(
            scenario(
                {
                    "task_id": "task-1",
                    "status": "waiting_for_human",
                    "run_count": 1,
                    "pending_review": {"review_id": "review-1"},
                },
                {"type": "approve", "review_id": "review-other"},
            )
        )
        == "review_not_found"
    )


def make_agent_turn(text: str) -> AgentCommandTurn:
    return AgentCommandTurn(
        platform="test",
        identity_key="identity-1",
        active_agent_name="main",
        text=text,
        chat_id="chat-1",
        user_id="user-1",
        thread_id=None,
        session_key_for_agent=lambda name: f"agent:{name}:session-1",
        metadata_for_session=lambda key: {"channel_session_key": key},
    )


def test_channel_turn_lists_only_public_agents() -> None:
    gateway = FakeGatewayClient()
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(handler.handle_agent_command(make_agent_turn("/agent")))

    assert result.kind == "listed"
    assert "`main` *" in result.message
    assert "`background_research`" in result.message
    assert "private_worker" not in result.message
    sessions.close()


def test_channel_turn_switches_agent_and_optionally_starts_task() -> None:
    gateway = FakeGatewayClient()
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    switched = asyncio.run(
        handler.handle_agent_command(make_agent_turn("/agent backgroundresearch"))
    )
    started = asyncio.run(
        handler.handle_agent_command(
            make_agent_turn("/agent background_research investigate")
        )
    )

    assert switched.kind == "switched"
    assert switched.agent_name == "background_research"
    assert started.kind == "started"
    assert started.task is not None
    assert gateway.created[-1]["content"] == "investigate"
    identity = sessions.get_session("identity-1")
    agent_session = sessions.get_session("agent:background_research:session-1")
    assert identity is not None and identity.agent_name == "background_research"
    assert agent_session is not None
    assert agent_session.current_task_id == started.task["task_id"]
    sessions.close()


def test_channel_turn_rejects_private_or_unknown_agent() -> None:
    gateway = FakeGatewayClient()
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(
        handler.handle_agent_command(make_agent_turn("/agent private_worker"))
    )

    assert result.kind == "unknown_agent"
    assert sessions.get_session("identity-1") is None
    sessions.close()


def make_resume_turn(
    text: str,
    *,
    belongs: bool = True,
) -> ResumeCommandTurn:
    return ResumeCommandTurn(
        platform="test",
        platform_label="Test",
        identity_key="identity-1",
        default_agent_name="main",
        text=text,
        fallback_metadata={"channel": "test", "user_id": "user-1"},
        chat_id="chat-1",
        user_id="user-1",
        thread_id=None,
        task_belongs_to_turn=lambda task: belongs,
        session_key_for_agent=lambda name: f"agent:{name}:session-1",
    )


def test_channel_turn_lists_resumable_tasks() -> None:
    gateway = FakeGatewayClient()
    gateway.listed_tasks = [
        {
            "task_id": "task-1",
            "agent_name": "main",
            "status": "completed",
            "last_result": "a useful result",
            "run_count": 1,
        }
    ]
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(handler.handle_resume_command(make_resume_turn("/resume")))

    assert result.kind == "listed"
    assert "task_id=task-1 agent=main status=completed" in result.message
    assert "a useful result" in result.message
    sessions.close()


def test_channel_turn_resumes_owned_task_and_active_agent() -> None:
    gateway = FakeGatewayClient()
    gateway.tasks["task-1"] = {
        "task_id": "task-1",
        "agent_name": "background_research",
        "status": "completed",
        "run_count": 1,
    }
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    result = asyncio.run(
        handler.handle_resume_command(make_resume_turn("/resume task-1"))
    )

    assert result.kind == "resumed"
    identity = sessions.get_session("identity-1")
    agent_session = sessions.get_session("agent:background_research:session-1")
    assert identity is not None and identity.agent_name == "background_research"
    assert agent_session is not None and agent_session.current_task_id == "task-1"
    sessions.close()


def test_channel_turn_resume_rejects_missing_or_foreign_task() -> None:
    gateway = FakeGatewayClient()
    gateway.tasks["task-1"] = {
        "task_id": "task-1",
        "agent_name": "main",
        "status": "completed",
        "run_count": 1,
    }
    sessions = ChannelSessionStore(":memory:")
    handler = ChannelTurnHandler(gateway_client=gateway, session_store=sessions)

    missing = asyncio.run(
        handler.handle_resume_command(make_resume_turn("/resume missing"))
    )
    foreign = asyncio.run(
        handler.handle_resume_command(make_resume_turn("/resume task-1", belongs=False))
    )

    assert missing.kind == "not_found"
    assert foreign.kind == "forbidden"
    assert sessions.get_session("identity-1") is None
    sessions.close()
