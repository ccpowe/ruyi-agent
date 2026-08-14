"""Shared Channel Turn policy for routing user input to Gateway Tasks."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from ruyi_agent.channels.gateway_client import GatewayClientError, GatewayTaskClient
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


ACTIVE_RUN_STATES = frozenset({"pending", "running"})
SETTLED_RUN_STATES = frozenset(
    {"completed", "failed", "cancelled", "interrupted"}
)


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


@dataclass(frozen=True, slots=True)
class ChannelTurnResult:
    """Outcome that a Channel Adapter renders using platform capabilities."""

    kind: Literal["pending_review", "active", "started"]
    task: dict[str, Any]
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
    task: dict[str, Any] | None = None


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


@dataclass(frozen=True, slots=True)
class AgentCommandResult:
    """Outcome of listing or switching the Active Agent."""

    kind: Literal["listed", "unknown_agent", "switched", "started"]
    message: str
    agent_name: str | None = None
    task: dict[str, Any] | None = None


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
    task_belongs_to_turn: Callable[[dict[str, Any]], bool]
    session_key_for_agent: Callable[[str], str]


@dataclass(frozen=True, slots=True)
class ResumeCommandResult:
    """Outcome of listing or resuming a Channel Session."""

    kind: Literal["listed", "no_tasks", "not_found", "forbidden", "resumed"]
    message: str
    task: dict[str, Any] | None = None


BeforeContinue = Callable[[dict[str, Any]], Awaitable[None]]


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
        latest_task = None
        if not turn.force_new:
            latest_task = await self._find_session_task(turn.session_key)
            if latest_task is None:
                latest_task = await self._find_latest_task(turn)
                if latest_task is not None:
                    await self._bind_session(turn, str(latest_task["task_id"]))

        if latest_task is not None and _task_has_pending_review(latest_task):
            return ChannelTurnResult(kind="pending_review", task=latest_task)

        if latest_task is not None and latest_task.get("status") in ACTIVE_RUN_STATES:
            return ChannelTurnResult(kind="active", task=latest_task)

        if latest_task is None:
            task = await self._gateway_client.create_task(
                agent_name=turn.agent_name,
                content=turn.content,
                metadata=turn.metadata,
                attachments=turn.attachments,
            )
            created = True
        else:
            if before_continue is not None:
                await before_continue(latest_task)
            task = await self._gateway_client.send_input(
                task_id=str(latest_task["task_id"]),
                content=turn.content,
                attachments=turn.attachments,
            )
            created = False

        await self._bind_session(turn, str(task["task_id"]))
        return ChannelTurnResult(kind="started", task=task, created=created)

    async def handle_review(self, turn: ReviewTurn) -> ReviewTurnResult:
        """Resolve one Review Command against the Channel Session's Task."""
        latest_task = await self._find_session_task(turn.session_key)
        if latest_task is None:
            items = await self._gateway_client.list_tasks(
                agent_name=turn.fallback_agent_name,
                metadata=turn.fallback_metadata,
                limit=1,
            )
            latest_task = items[0] if items else None
            if latest_task is not None:
                await self._bind_review_session(turn, latest_task)

        if latest_task is None:
            return ReviewTurnResult(
                kind="no_task",
                message="没有可审批的任务。",
            )
        pending_review = latest_task.get("pending_review")
        if not isinstance(pending_review, dict):
            return ReviewTurnResult(
                kind="no_pending_review",
                message="当前任务没有待审批项。",
                task=latest_task,
            )

        requested_review_id = turn.command.get("review_id")
        review_id = str(requested_review_id or pending_review.get("review_id") or "")
        if not review_id:
            return ReviewTurnResult(
                kind="missing_review_id",
                message="待审批任务缺少 review_id。",
                task=latest_task,
            )
        if (
            requested_review_id is not None
            and pending_review.get("review_id") != review_id
        ):
            return ReviewTurnResult(
                kind="review_not_found",
                message=f"没有找到待审批 review：{review_id}",
                task=latest_task,
            )

        decision: dict[str, Any] = {"type": turn.command["type"]}
        if turn.command.get("type") == "reject" and turn.command.get("message"):
            decision["message"] = turn.command["message"]
        task = await self._gateway_client.submit_review_decision(
            task_id=str(latest_task["task_id"]),
            review_id=review_id,
            decisions=[decision],
        )
        await self._bind_review_session(turn, task)
        task_id = str(task["task_id"])
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
        agents = await self._gateway_client.list_agents()
        public_agents = [
            agent
            for agent in agents
            if agent.get("public") is True and agent.get("name")
        ]
        public_agent_names = {str(agent["name"]) for agent in public_agents}
        if len(parts) == 1:
            lines = [
                f"当前 agent：`{turn.active_agent_name}`",
                "",
                "可用 agents：",
            ]
            for agent in sorted(
                public_agents,
                key=lambda item: str(item.get("name", "")),
            ):
                name = str(agent["name"])
                marker = " *" if name == turn.active_agent_name else ""
                description = str(agent.get("description") or "")
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
                message=(
                    f"已切换到 agent={requested_name}。发送消息即可开始新会话。"
                ),
            )

        task = await self._gateway_client.create_task(
            agent_name=requested_name,
            content=initial_message,
            metadata=turn.metadata_for_session(agent_session_key),
        )
        await self._session_store.abind_session(
            session_key=agent_session_key,
            platform=turn.platform,
            agent_name=requested_name,
            current_task_id=str(task["task_id"]),
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )
        task_id = str(task["task_id"])
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
            items = await self._gateway_client.list_tasks(
                agent_name=None,
                metadata=turn.fallback_metadata,
                limit=10,
            )
            if not items:
                return ResumeCommandResult(
                    kind="no_tasks",
                    message="暂无可恢复会话。",
                )
            lines = ["最近会话："]
            for item in items:
                task_id = str(item.get("task_id", ""))
                agent_name = str(item.get("agent_name", ""))
                status = str(item.get("status", ""))
                result = str(item.get("last_result") or item.get("error") or "")
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
            task = await self._gateway_client.get_task(task_id=task_id)
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

        agent_name = str(task.get("agent_name") or turn.default_agent_name)
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

    async def _find_session_task(self, session_key: str) -> dict[str, Any] | None:
        session = await self._session_store.aget_session(session_key)
        if session is None or not session.current_task_id:
            return None
        try:
            return await self._gateway_client.get_task(
                task_id=session.current_task_id,
            )
        except GatewayClientError as exc:
            if exc.status_code not in {401, 403, 404, 410}:
                raise
            await self._session_store.aunbind_session(session_key)
            return None

    async def _find_latest_task(self, turn: InboundTurn) -> dict[str, Any] | None:
        items = await self._gateway_client.list_tasks(
            agent_name=turn.agent_name,
            metadata=turn.fallback_metadata,
            limit=1,
        )
        return items[0] if items else None

    async def _bind_session(self, turn: InboundTurn, task_id: str) -> None:
        await self._session_store.abind_session(
            session_key=turn.session_key,
            platform=turn.platform,
            agent_name=turn.agent_name,
            current_task_id=task_id,
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )

    async def _bind_review_session(
        self,
        turn: ReviewTurn,
        task: dict[str, Any],
    ) -> None:
        await self._session_store.abind_session(
            session_key=turn.session_key,
            platform=turn.platform,
            agent_name=str(task.get("agent_name") or turn.default_agent_name),
            current_task_id=str(task["task_id"]),
            chat_id=turn.chat_id,
            user_id=turn.user_id,
            thread_id=turn.thread_id,
        )


def _task_has_pending_review(task: dict[str, Any]) -> bool:
    return isinstance(task.get("pending_review"), dict)


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
