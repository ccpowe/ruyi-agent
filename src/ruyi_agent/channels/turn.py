"""Shared Channel Turn policy for routing user input to Gateway Tasks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from ruyi_agent.channels.gateway_client import (
    GatewayClientError,
    GatewayTaskClient,
    gateway_agent_from_payload,
    gateway_task_from_payload,
)
from ruyi_agent.gateway_protocol.dto import GatewayTask
from ruyi_agent.storage.channel_session_store import ChannelSessionStore
from ruyi_agent.task_models import EXECUTING_TASK_STATES, SETTLED_TASK_STATES


ACTIVE_RUN_STATES = EXECUTING_TASK_STATES
SETTLED_RUN_STATES = SETTLED_TASK_STATES


@dataclass(frozen=True, slots=True)
class InboundTurn:
    """Platform-neutral input required to route one Channel Turn."""

    platform: str
    session_key: str
    agent_name: str
    content: str
    metadata: dict[str, str]
    fallback_metadata: dict[str, str]
    chat_id: str
    user_id: str
    thread_id: str | None = None
    attachments: list[dict[str, str]] | None = None
    force_new: bool = False
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class ChannelTurnResult:
    """Outcome that a Channel Adapter renders using platform capabilities."""

    kind: Literal["pending_review", "active", "started"]
    task: GatewayTask
    created: bool = False


@dataclass(frozen=True, slots=True)
class ReviewTurn:
    """Platform-neutral context required to resolve one Review Command."""

    platform: str
    session_key: str
    default_agent_name: str
    fallback_metadata: dict[str, str]
    fallback_agent_name: str | None
    chat_id: str
    user_id: str
    thread_id: str | None
    command: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReviewTurnResult:
    """Outcome of applying a Review Command to the current Gateway Task."""

    kind: Literal[
        "no_task",
        "no_pending_review",
        "missing_review_id",
        "review_not_found",
        "submitted",
    ]
    message: str
    task: GatewayTask | None = None


@dataclass(frozen=True, slots=True)
class AgentCommandTurn:
    """Platform context and factories required by the `/agent` command."""

    platform: str
    identity_key: str
    active_agent_name: str
    text: str
    chat_id: str
    user_id: str
    thread_id: str | None
    session_key_for_agent: Callable[[str], str]
    metadata_for_session: Callable[[str], dict[str, str]]
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class AgentCommandResult:
    """Outcome of listing or switching the Active Agent."""

    kind: Literal["listed", "unknown_agent", "switched", "started"]
    message: str
    agent_name: str | None = None
    task: GatewayTask | None = None


@dataclass(frozen=True, slots=True)
class ResumeCommandTurn:
    """Platform context required to list or resume a Gateway Task."""

    platform: str
    platform_label: str
    identity_key: str
    default_agent_name: str
    text: str
    fallback_metadata: dict[str, str]
    chat_id: str
    user_id: str
    thread_id: str | None
    task_belongs_to_turn: Callable[[GatewayTask], bool]
    session_key_for_agent: Callable[[str], str]


@dataclass(frozen=True, slots=True)
class ResumeCommandResult:
    """Outcome of listing or resuming a Channel Session."""

    kind: Literal["listed", "no_tasks", "not_found", "forbidden", "resumed"]
    message: str
    task: GatewayTask | None = None


BeforeContinue = Callable[[GatewayTask], Awaitable[None]]


class ChannelTurnIdempotencyConflictError(ValueError):
    """Raised when one Channel Turn key is reused for another request."""


class ChannelTurnHandler:
    """Route ordinary Channel Turns while preserving Task and Session invariants."""

    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        session_store: ChannelSessionStore,
    ) -> None:
        self._gateway_client = gateway_client
        self._session_store = session_store

    async def handle(
        self,
        turn: InboundTurn,
        *,
        before_continue: BeforeContinue | None = None,
    ) -> ChannelTurnResult:
        """Create or continue the Gateway Task selected by one Channel Turn."""
        turn_request_hash = None
        if turn.idempotency_key is not None:
            turn_request_hash = _inbound_turn_request_hash(turn)
            receipt = await self._session_store.aget_turn_receipt(turn.idempotency_key)
            if receipt is not None:
                if (
                    receipt.platform != turn.platform
                    or receipt.session_key != turn.session_key
                    or receipt.request_hash != turn_request_hash
                ):
                    raise ChannelTurnIdempotencyConflictError(
                        "Channel Turn idempotency key was reused for a different "
                        "request"
                    )
                return ChannelTurnResult(
                    kind="started",
                    task=gateway_task_from_payload(receipt.response),
                    created=receipt.operation == "create",
                )

        latest_task = None
        if not turn.force_new:
            latest_task = await self._find_session_task(turn.session_key)
            if latest_task is None:
                latest_task = await self._find_latest_task(turn)
                if latest_task is not None:
                    await self._bind_session(turn, latest_task.task_id)

        if latest_task is not None and latest_task.has_pending_review:
            return ChannelTurnResult(kind="pending_review", task=latest_task)

        if latest_task is not None and latest_task.status in ACTIVE_RUN_STATES:
            return ChannelTurnResult(kind="active", task=latest_task)

        if latest_task is None:
            create_kwargs: dict[str, Any] = {
                "agent_name": turn.agent_name,
                "content": turn.content,
                "metadata": turn.metadata,
                "attachments": turn.attachments,
            }
            if turn.idempotency_key is not None:
                create_kwargs["idempotency_key"] = turn.idempotency_key
            task = gateway_task_from_payload(
                await self._gateway_client.create_task(**create_kwargs)
            )
            created = True
        else:
            if before_continue is not None:
                await before_continue(latest_task)
            send_kwargs: dict[str, Any] = {
                "task_id": latest_task.task_id,
                "content": turn.content,
                "attachments": turn.attachments,
            }
            if turn.idempotency_key is not None:
                send_kwargs["idempotency_key"] = turn.idempotency_key
            task = gateway_task_from_payload(
                await self._gateway_client.send_input(**send_kwargs)
            )
            created = False

        await self._bind_session(
            turn,
            task.task_id,
            operation="create" if created else "send",
            request_hash=turn_request_hash,
            response=task,
        )
        return ChannelTurnResult(kind="started", task=task, created=created)

    async def handle_review(self, turn: ReviewTurn) -> ReviewTurnResult:
        """Resolve one Review Command against the Channel Session's Task."""
        latest_task = await self._find_session_task(turn.session_key)
        if latest_task is None:
            items = [
                gateway_task_from_payload(item)
                for item in await self._gateway_client.list_tasks(
                    agent_name=turn.fallback_agent_name,
                    metadata=turn.fallback_metadata,
                    limit=1,
                )
            ]
            latest_task = items[0] if items else None
            if latest_task is not None:
                await self._bind_review_session(turn, latest_task)

        if latest_task is None:
            return ReviewTurnResult(
                kind="no_task",
                message="没有可审批的任务。",
            )
        pending_review = latest_task.pending_review
        if pending_review is None:
            return ReviewTurnResult(
                kind="no_pending_review",
                message="当前任务没有待审批项。",
                task=latest_task,
            )

        requested_review_id = turn.command.get("review_id")
        review_id = str(requested_review_id or pending_review.review_id)
        if requested_review_id is not None and pending_review.review_id != review_id:
            return ReviewTurnResult(
                kind="review_not_found",
                message=f"没有找到待审批 review：{review_id}",
                task=latest_task,
            )

        decision: dict[str, Any] = {"type": turn.command["type"]}
        if turn.command.get("type") == "reject" and turn.command.get("message"):
            decision["message"] = turn.command["message"]
        task = gateway_task_from_payload(
            await self._gateway_client.submit_review_decision(
                task_id=latest_task.task_id,
                review_id=review_id,
                decisions=[decision],
            )
        )
        await self._bind_review_session(turn, task)
        task_id = task.task_id
        return ReviewTurnResult(
            kind="submitted",
            message=f"审批已提交，task_id={task_id}",
            task=task,
        )

    async def handle_agent_command(
        self,
        turn: AgentCommandTurn,
    ) -> AgentCommandResult:
        """List public Agents or switch the Channel Session's Active Agent."""
        parts = turn.text.split(maxsplit=2)
        agents = [
            gateway_agent_from_payload(agent)
            for agent in await self._gateway_client.list_agents()
        ]
        public_agents = [agent for agent in agents if agent.public is True]
        public_agent_names = {agent.name for agent in public_agents}
        if len(parts) == 1:
            lines = [
                f"当前 agent：`{turn.active_agent_name}`",
                "",
                "可用 agents：",
            ]
            for agent in sorted(
                public_agents,
                key=lambda item: item.name,
            ):
                name = agent.name
                marker = " *" if name == turn.active_agent_name else ""
                description = agent.description
                suffix = f" - {description}" if description else ""
                lines.append(f"- `{name}`{marker}{suffix}")
            lines.extend(["", "切换：`/agent <agent_name>`"])
            return AgentCommandResult(kind="listed", message="\n".join(lines))

        requested_name = _resolve_agent_name(parts[1], public_agent_names)
        if requested_name is None:
            return AgentCommandResult(
                kind="unknown_agent",
                message=f"未知或不可用 agent：{parts[1]}。使用 /agent 查看列表。",
            )

        await self._session_store.abind_session(
            session_key=turn.identity_key,
            platform=turn.platform,
            agent_name=requested_name,
            current_task_id="",
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )
        agent_session_key = turn.session_key_for_agent(requested_name)
        await self._session_store.aunbind_session(agent_session_key)
        initial_message = parts[2].strip() if len(parts) > 2 else ""
        if not initial_message:
            return AgentCommandResult(
                kind="switched",
                agent_name=requested_name,
                message=(f"已切换到 agent={requested_name}。发送消息即可开始新会话。"),
            )

        create_kwargs = {
            "agent_name": requested_name,
            "content": initial_message,
            "metadata": turn.metadata_for_session(agent_session_key),
        }
        if turn.idempotency_key is not None:
            create_kwargs["idempotency_key"] = turn.idempotency_key
        task = gateway_task_from_payload(
            await self._gateway_client.create_task(**create_kwargs)
        )
        await self._session_store.abind_session(
            session_key=agent_session_key,
            platform=turn.platform,
            agent_name=requested_name,
            current_task_id=task.task_id,
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )
        task_id = task.task_id
        return AgentCommandResult(
            kind="started",
            agent_name=requested_name,
            task=task,
            message=(
                f"已切换到 agent={requested_name}，并创建新会话 task_id={task_id}"
            ),
        )

    async def handle_resume_command(
        self,
        turn: ResumeCommandTurn,
    ) -> ResumeCommandResult:
        """List resumable Tasks or bind one Task back to the Channel Session."""
        parts = turn.text.split(maxsplit=1)
        if len(parts) == 1:
            items = [
                gateway_task_from_payload(item)
                for item in await self._gateway_client.list_tasks(
                    agent_name=None,
                    metadata=turn.fallback_metadata,
                    limit=10,
                )
            ]
            if not items:
                return ResumeCommandResult(
                    kind="no_tasks",
                    message="暂无可恢复会话。",
                )
            lines = ["最近会话："]
            for item in items:
                task_id = item.task_id
                agent_name = item.agent_name
                status = item.status
                result = item.last_result or item.error or ""
                preview = _single_line_preview(result) if result else ""
                suffix = f"\n   {preview}" if preview else ""
                lines.append(
                    f"- task_id={task_id} agent={agent_name} status={status}{suffix}"
                )
            lines.extend(["", "恢复：/resume <task_id>"])
            return ResumeCommandResult(kind="listed", message="\n".join(lines))

        task_id = parts[1].strip()
        if not task_id:
            return ResumeCommandResult(kind="not_found", message="没有找到会话。")
        try:
            task = gateway_task_from_payload(
                await self._gateway_client.get_task(task_id=task_id)
            )
        except GatewayClientError as exc:
            if exc.status_code not in {404, 410}:
                raise
            return ResumeCommandResult(
                kind="not_found",
                message=f"没有找到会话：{task_id}",
            )
        if not turn.task_belongs_to_turn(task):
            return ResumeCommandResult(
                kind="forbidden",
                message=(
                    f"不能恢复不属于当前 {turn.platform_label} 会话的 task：{task_id}"
                ),
                task=task,
            )

        agent_name = task.agent_name or turn.default_agent_name
        await self._session_store.abind_session(
            session_key=turn.identity_key,
            platform=turn.platform,
            agent_name=agent_name,
            current_task_id="",
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )
        session_key = turn.session_key_for_agent(agent_name)
        await self._session_store.abind_session(
            session_key=session_key,
            platform=turn.platform,
            agent_name=agent_name,
            current_task_id=task_id,
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )
        return ResumeCommandResult(
            kind="resumed",
            message=(
                f"已恢复 agent={agent_name} task_id={task_id}。继续发送消息即可续聊。"
            ),
            task=task,
        )

    async def _find_session_task(self, session_key: str) -> GatewayTask | None:
        session = await self._session_store.aget_session(session_key)
        if session is None or not session.current_task_id:
            return None
        try:
            return gateway_task_from_payload(
                await self._gateway_client.get_task(
                    task_id=session.current_task_id,
                )
            )
        except GatewayClientError as exc:
            if exc.status_code not in {401, 403, 404, 410}:
                raise
            await self._session_store.aunbind_session(session_key)
            return None

    async def _find_latest_task(self, turn: InboundTurn) -> GatewayTask | None:
        items = [
            gateway_task_from_payload(item)
            for item in await self._gateway_client.list_tasks(
                agent_name=turn.agent_name,
                metadata=turn.fallback_metadata,
                limit=1,
            )
        ]
        return items[0] if items else None

    async def _bind_session(
        self,
        turn: InboundTurn,
        task_id: str,
        *,
        operation: str | None = None,
        request_hash: str | None = None,
        response: GatewayTask | None = None,
    ) -> None:
        record_turn = turn.idempotency_key is not None and operation is not None
        await self._session_store.abind_session(
            session_key=turn.session_key,
            platform=turn.platform,
            agent_name=turn.agent_name,
            current_task_id=task_id,
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
            turn_idempotency_key=turn.idempotency_key if record_turn else None,
            turn_operation=operation if record_turn else None,
            turn_request_hash=request_hash if record_turn else None,
            turn_response=response.to_payload() if record_turn and response else None,
        )

    async def _bind_review_session(
        self,
        turn: ReviewTurn,
        task: GatewayTask,
    ) -> None:
        await self._session_store.abind_session(
            session_key=turn.session_key,
            platform=turn.platform,
            agent_name=task.agent_name or turn.default_agent_name,
            current_task_id=task.task_id,
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )


def _inbound_turn_request_hash(turn: InboundTurn) -> str:
    canonical = json.dumps(
        {
            "platform": turn.platform,
            "session_key": turn.session_key,
            "agent_name": turn.agent_name,
            "content": turn.content,
            "metadata": turn.metadata,
            "fallback_metadata": turn.fallback_metadata,
            "chat_id": turn.chat_id,
            "user_id": turn.user_id,
            "thread_id": turn.thread_id,
            "attachments": list(turn.attachments or []),
            "force_new": turn.force_new,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def parse_review_command(text: str) -> dict[str, Any] | None:
    """Parse the cross-platform approve/reject Review Command vocabulary."""
    parts = text.split(maxsplit=2)
    if not parts:
        return None
    command = _normalize_command_token(parts[0])
    if command in {"y", "yes", "/yes", "/approve"}:
        payload: dict[str, Any] = {"type": "approve"}
        if len(parts) >= 2:
            payload["review_id"] = parts[1]
        return payload
    if command in {"n", "no", "/no", "/reject"}:
        payload = {"type": "reject"}
        if len(parts) >= 2 and command == "/reject":
            payload["review_id"] = parts[1]
        if len(parts) > 2 and command == "/reject":
            payload["message"] = parts[2]
        return payload
    return None


def _normalize_command_token(token: str) -> str:
    command = token.lower()
    if not command.startswith("/"):
        return command
    return command.split("@", 1)[0]


def _resolve_agent_name(
    requested_agent_name: str,
    public_agent_names: set[str],
) -> str | None:
    if requested_agent_name in public_agent_names:
        return requested_agent_name
    normalized = requested_agent_name.replace("_", "")
    matches = [
        agent_name
        for agent_name in public_agent_names
        if agent_name.replace("_", "") == normalized
    ]
    return matches[0] if len(matches) == 1 else None


def _single_line_preview(text: str, *, limit: int = 80) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: max(0, limit - 3)]}..."
