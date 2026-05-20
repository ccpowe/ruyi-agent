"""
协议控制器 - 将控制面命令翻译为 runtime 操作和事件

这个模块提供面向 CLI/TUI/Gateway 的统一控制面入口。入口层只需要提交
`ProtocolCommand`，控制器负责调用 root agent、异步任务控制和审批控制，并
返回稳定的 `ProtocolEvent` 列表。

核心功能：
1. 分发用户消息、任务输入、审批决策、取消任务和切换线程命令
2. 将 runtime 的任务记录、root agent 输出和中断转换成协议事件
3. 维护入口层活跃线程和线程摘要
4. 输出完整运行时快照，供 UI 初始化和刷新

使用场景：
- CLI/TUI 将用户操作统一提交为 command
- Gateway 需要向前端返回稳定事件结构
- root agent 与 worker task 的审批结果需要走同一套事件语义

数据流：
  AnyProtocolCommand → ProtocolController → AgentControl/RootAgentRunner
  → AnyProtocolEvent / RuntimeSnapshot

关键概念：
- RootAgentRunner: root agent 的最小运行接口
- AgentControl: worker/subagent 任务生命周期控制对象
- ReviewControl: 统一 root review 与 task review 的审批控制面
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol as TypingProtocol, cast

from ruyi_agent.runtime.delegation.async_runtime import (
    AgentControl,
    TaskRecord,
)
from langgraph.types import Command as LangGraphCommand
from ruyi_agent.control_plane.reviews import ReviewControl, review_snapshot_from_record
from ruyi_agent.runtime.agent_turn import normalize_agent_turn
from ruyi_agent.control_plane.contracts import (
    AnyProtocolCommand,
    AnyProtocolEvent,
    AuditEventSnapshot,
    CancelTaskCommand,
    CommandAcceptedEvent,
    CommandAcceptedPayload,
    CommandRejectedEvent,
    CommandRejectedPayload,
    ContentDeltaEvent,
    ContentDeltaPayload,
    ErrorOccurredEvent,
    ErrorOccurredPayload,
    ProtocolCommand,
    ProtocolEvent,
    ReviewRequestedEvent,
    ReviewRequestedPayload,
    ReviewResolvedEvent,
    ReviewResolvedPayload,
    ReviewSnapshot,
    RuntimeSnapshot,
    SendTaskInputCommand,
    SendUserMessageCommand,
    SubmitReviewDecisionCommand,
    SwitchThreadCommand,
    TaskSnapshot,
    TaskStatus,
    TaskUpdatedEvent,
    TaskUpdatedPayload,
    ThreadSummary,
)


def _utc_now() -> datetime:
    """生成带 UTC 时区的当前时间。"""

    return datetime.now(UTC)


def _coerce_status(value: str) -> TaskStatus:
    """将 runtime 字符串状态转换为协议枚举，未知状态按 failed 暴露。"""

    try:
        return TaskStatus(value)
    except ValueError:
        return TaskStatus.FAILED


def _task_snapshot_from_record(
    record: TaskRecord,
    *,
    thread_id: str | None = None,
) -> TaskSnapshot:
    """把 runtime TaskRecord 转换成查询用任务快照。"""

    return TaskSnapshot(
        task_id=record.task_id,
        agent_name=record.agent_name,
        status=_coerce_status(record.state),
        thread_id=thread_id or record.thread_id,
        parent_task_id=record.parent_task_id,
        root_task_id=record.root_task_id,
        run_count=record.run_count,
        route_kind=record.route_kind,
        upstream_task_id=record.upstream_task_id,
        last_result=record.result,
        error=record.error,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _task_payload_from_record(record: TaskRecord) -> TaskUpdatedPayload:
    """把 runtime TaskRecord 转换成任务更新事件载荷。"""

    return TaskUpdatedPayload(
        task_id=record.task_id,
        agent_name=record.agent_name,
        status=_coerce_status(record.state),
        run_count=record.run_count,
        last_result=record.result,
        error=record.error,
        updated_at=record.updated_at,
    )


@dataclass(slots=True)
class RootAgentTurnResult:
    """
    root agent 单轮执行结果

    Attributes:
        agent_name: 实际执行的 root agent 名称
        thread_id: 本轮所在的线程 ID
        content: 可展示的最终文本
        interrupt_requests: 本轮产生的审批中断请求
    """

    agent_name: str
    thread_id: str
    content: str = ""
    interrupt_requests: list[dict[str, Any]] = field(default_factory=list)


class RootAgentRunner(TypingProtocol):
    """ProtocolController 调用 root agent 所需的最小接口。"""

    async def run_user_message(
        self,
        *,
        agent_name: str,
        thread_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> RootAgentTurnResult:
        """
        执行一条用户消息。

        Args:
            agent_name: 目标 root agent 名称
            thread_id: 线程 ID
            content: 用户输入文本
            metadata: 控制面透传元数据

        Returns:
            root agent 单轮执行结果
        """

        pass

    async def resume_review(
        self,
        *,
        agent_name: str,
        thread_id: str,
        decisions: list[dict[str, Any]],
    ) -> RootAgentTurnResult:
        """
        用审批决策恢复 root agent。

        Args:
            agent_name: 目标 root agent 名称
            thread_id: 线程 ID
            decisions: runtime 期望的审批决策载荷

        Returns:
            root agent 恢复执行后的单轮结果
        """

        pass


@dataclass(slots=True)
class LocalRootAgentRunner:
    """
    本地 root agent 运行器

    负责把控制面调用转换成 LangGraph agent.ainvoke 调用，并从结果或状态中提取
    文本输出与审批中断。

    Attributes:
        get_agent: 按 agent_name 获取已编译 root agent 的函数
        resolve_permission_profile: 按 agent_name 解析权限 profile 的函数
    """

    get_agent: Callable[[str], Any]
    resolve_permission_profile: Callable[[str], str]

    async def run_user_message(
        self,
        *,
        agent_name: str,
        thread_id: str,
        content: str,
        metadata: dict[str, Any],
    ) -> RootAgentTurnResult:
        """
        运行一条用户消息。

        Args:
            agent_name: root agent 名称
            thread_id: 线程 ID
            content: 用户输入内容
            metadata: 入口层透传元数据

        Returns:
            root agent 单轮执行结果
        """

        return await self._run_payload(
            agent_name=agent_name,
            thread_id=thread_id,
            payload={"messages": [{"role": "user", "content": content}]},
        )

    async def resume_review(
        self,
        *,
        agent_name: str,
        thread_id: str,
        decisions: list[dict[str, Any]],
    ) -> RootAgentTurnResult:
        """
        用审批结果恢复 root agent 线程。

        Args:
            agent_name: root agent 名称
            thread_id: 线程 ID
            decisions: runtime 期望的审批决策载荷

        Returns:
            root agent 恢复后的执行结果
        """

        return await self._run_payload(
            agent_name=agent_name,
            thread_id=thread_id,
            payload=LangGraphCommand(resume={"decisions": decisions}),
        )

    async def _run_payload(
        self,
        *,
        agent_name: str,
        thread_id: str,
        payload: Any,
    ) -> RootAgentTurnResult:
        """
        执行底层 LangGraph payload 并提取控制面需要的结果。

        Args:
            agent_name: root agent 名称
            thread_id: 线程 ID
            payload: 普通消息 payload 或 LangGraph Command

        Returns:
            root agent 单轮执行结果
        """

        agent = self.get_agent(agent_name)
        config = {
            "configurable": {
                "thread_id": thread_id,
                "agent_name": agent_name,
                "permission_profile": self.resolve_permission_profile(agent_name),
            }
        }
        # thread_id 同时承担会话恢复和审批 resume 定位作用，必须保持稳定。
        result = await agent.ainvoke(
            payload,
            config=config,
            version="v2",
        )
        outcome = await normalize_agent_turn(agent, config, result)
        return RootAgentTurnResult(
            agent_name=agent_name,
            thread_id=thread_id,
            content=outcome.content,
            interrupt_requests=outcome.review_payloads,
        )


@dataclass(slots=True)
class ProtocolController:
    """
    控制面协议控制器

    这是入口层和 runtime 之间的薄翻译层。入口层提交稳定的协议命令，控制器
    调用现有 runtime 操作，并把结果整理成有序协议事件。

    主要功能：
    - 处理 root agent 对话命令
    - 处理 worker/subagent task 输入和取消命令
    - 处理 root/task 两类审批决策
    - 维护线程摘要和运行时快照

    Attributes:
        control: 异步任务控制对象
        root_runner: root agent 运行接口
        default_agent_name: 未显式指定 agent 时使用的默认 root agent
        review_control: 审批控制面服务
    """

    control: AgentControl
    root_runner: RootAgentRunner
    default_agent_name: str
    _seq: int = 0
    _active_thread_id: str | None = None
    review_control: ReviewControl | None = None
    _threads: dict[str, ThreadSummary] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """补齐默认审批控制面。"""

        if self.review_control is None:
            self.review_control = ReviewControl(
                control=self.control,
                root_runner=self.root_runner,
            )

    async def handle_command(
        self,
        command: AnyProtocolCommand,
    ) -> list[AnyProtocolEvent]:
        """
        处理一条控制面命令

        所有未捕获异常都会被转换成 command_rejected 和 error_occurred 事件，
        让 UI 不需要处理 Python 异常边界。

        Args:
            command: 控制面协议命令

        Returns:
            对应该命令的一组协议事件
        """

        try:
            return await self._handle_command(command)
        except Exception as exc:  # noqa: BLE001
            return [
                self._command_rejected(
                    command,
                    code=exc.__class__.__name__,
                    message=str(exc),
                ),
                self._error_occurred(
                    command,
                    code=exc.__class__.__name__,
                    message=str(exc),
                ),
            ]

    async def _handle_command(
        self,
        command: AnyProtocolCommand,
    ) -> list[AnyProtocolEvent]:
        """按命令类型分发到具体处理函数。"""

        if isinstance(command, SendUserMessageCommand):
            return await self._send_user_message(command)
        if isinstance(command, SendTaskInputCommand):
            return await self._send_task_input(command)
        if isinstance(command, SubmitReviewDecisionCommand):
            return await self._submit_review_decision(command)
        if isinstance(command, CancelTaskCommand):
            return await self._cancel_task(command)
        if isinstance(command, SwitchThreadCommand):
            return self._switch_thread(command)
        return [
            self._command_rejected(
                command,
                code="unsupported_command",
                message=f"Unsupported command kind: {command.kind}",
            )
        ]

    async def _send_user_message(
        self,
        command: SendUserMessageCommand,
    ) -> list[AnyProtocolEvent]:
        """处理 root agent 用户消息命令。"""

        thread_id = self._resolve_protocol_thread_id(command)
        agent_name = command.payload.agent_name or self.default_agent_name
        result = await self.root_runner.run_user_message(
            agent_name=agent_name,
            thread_id=thread_id,
            content=command.payload.content,
            metadata=command.metadata,
        )
        self._remember_thread(
            thread_id=thread_id,
            agent_name=result.agent_name,
            active_task_id=None,
        )
        return self._events_for_root_turn(command, result)

    async def _send_task_input(
        self,
        command: SendTaskInputCommand,
    ) -> list[AnyProtocolEvent]:
        """处理发送给已有任务的继续输入命令。"""

        record = await self.control.send_task_input(
            command.task_id,
            command.payload.content,
        )
        return self._events_for_task_command(command, record)

    async def _submit_review_decision(
        self,
        command: SubmitReviewDecisionCommand,
    ) -> list[AnyProtocolEvent]:
        """处理审批决策提交命令。"""

        assert self.review_control is not None
        result = await self.review_control.submit_decision(
            command.review_id,
            command.payload.decisions,
        )
        if result.source == "root":
            # root review 没有 task_id，事件只绑定 thread/review 维度。
            root_result = cast(RootAgentTurnResult, result.root_result)
            events: list[AnyProtocolEvent] = [
                self._command_accepted(command, thread_id=result.thread_id),
                self._event(
                    ReviewResolvedEvent,
                    command,
                    thread_id=result.thread_id,
                    review_id=command.review_id,
                    payload=ReviewResolvedPayload(
                        review_id=command.review_id,
                        task_id="",
                        decisions=command.payload.decisions,
                    ),
                ),
            ]
            events.extend(
                self._events_for_root_turn(command, root_result, accepted=False)
            )
            return events

        record = result.task_record
        if record is None:
            raise ValueError(f"Review '{command.review_id}' did not return a task")
        events = [
            self._command_accepted(command),
            self._event(
                ReviewResolvedEvent,
                command,
                task_id=record.task_id,
                review_id=command.review_id,
                payload=ReviewResolvedPayload(
                    review_id=command.review_id,
                    task_id=record.task_id,
                    decisions=command.payload.decisions,
                ),
            ),
            self._task_updated(command, record),
        ]
        review_snapshot = review_snapshot_from_record(record)
        if review_snapshot is not None:
            # 审批后仍可能产生下一轮审批，控制面需要连续暴露 pending review。
            events.append(self._review_requested(command, record, review_snapshot))
        elif record.result:
            events.append(
                self._event(
                    ContentDeltaEvent,
                    command,
                    task_id=record.task_id,
                    payload=ContentDeltaPayload(
                        delta=record.result,
                        agent_name=record.agent_name,
                    ),
                )
            )
        return events

    async def _cancel_task(
        self,
        command: CancelTaskCommand,
    ) -> list[AnyProtocolEvent]:
        """处理任务取消命令。"""

        record = await self.control.cancel_task(command.task_id)
        return [
            self._command_accepted(command),
            self._task_updated(command, record),
        ]

    def _switch_thread(
        self,
        command: SwitchThreadCommand,
    ) -> list[AnyProtocolEvent]:
        """切换控制器当前活跃线程。"""

        self._active_thread_id = command.thread_id
        self._remember_thread(
            thread_id=command.thread_id,
            agent_name=command.payload.agent_name,
            active_task_id=None,
        )
        return [self._command_accepted(command)]

    def snapshot(
        self,
        *,
        recent_audit_events: list[AuditEventSnapshot] | None = None,
    ) -> RuntimeSnapshot:
        """
        构造当前运行时快照

        Args:
            recent_audit_events: 调用方额外传入的最近审计事件

        Returns:
            聚合线程、任务、审批和审计信息的控制面快照
        """

        tasks = [
            _task_snapshot_from_record(record)
            for record in self.control.list_task_records()
        ]
        assert self.review_control is not None
        return RuntimeSnapshot(
            active_thread_id=self._active_thread_id,
            threads=sorted(
                self._threads.values(),
                key=lambda item: item.updated_at,
                reverse=True,
            ),
            tasks=tasks,
            pending_reviews=self.review_control.list_pending_reviews(),
            recent_audit_events=recent_audit_events or [],
        )

    def _resolve_protocol_thread_id(self, command: ProtocolCommand) -> str:
        """解析命令所属线程，缺失时回退到当前活跃线程。"""

        if command.thread_id:
            return command.thread_id
        if self._active_thread_id:
            return self._active_thread_id
        raise ValueError("thread_id is required for root conversation commands")

    def _events_for_root_turn(
        self,
        command: ProtocolCommand,
        result: RootAgentTurnResult,
        *,
        accepted: bool = True,
    ) -> list[AnyProtocolEvent]:
        """
        将 root agent 执行结果转换为协议事件。

        Args:
            command: 触发本轮执行的命令
            result: root agent 单轮结果
            accepted: 是否需要追加 command_accepted 事件

        Returns:
            内容增量和审批请求等事件列表
        """

        events: list[AnyProtocolEvent] = []
        if accepted:
            events.append(self._command_accepted(command, thread_id=result.thread_id))
        if result.content:
            events.append(
                self._event(
                    ContentDeltaEvent,
                    command,
                    thread_id=result.thread_id,
                    payload=ContentDeltaPayload(
                        delta=result.content,
                        agent_name=result.agent_name,
                    ),
                )
            )
        assert self.review_control is not None
        for review in self.review_control.register_root_interrupts(
            agent_name=result.agent_name,
            thread_id=result.thread_id,
            interrupt_requests=result.interrupt_requests,
        ):
            events.append(
                self._event(
                    ReviewRequestedEvent,
                    command,
                    thread_id=result.thread_id,
                    review_id=review.review_id,
                    payload=ReviewRequestedPayload(
                        review_id=review.review_id,
                        task_id="",
                        agent_name=result.agent_name,
                        actions=review.actions,
                        allowed_decisions=review.allowed_decisions,
                        risk=review.risk,
                        reason=review.reason,
                    ),
                )
            )
        return events

    def _events_for_task_command(
        self,
        command: ProtocolCommand,
        record: TaskRecord,
        *,
        thread_id: str | None = None,
    ) -> list[AnyProtocolEvent]:
        """
        将任务命令结果转换为协议事件。

        Args:
            command: 触发任务操作的命令
            record: 操作后的任务记录
            thread_id: 需要覆盖的线程 ID

        Returns:
            命令接受、任务更新、审批请求或结果内容事件
        """

        events: list[AnyProtocolEvent] = [
            self._command_accepted(command, thread_id=thread_id),
            self._task_updated(command, record, thread_id=thread_id),
        ]
        review_snapshot = review_snapshot_from_record(
            record,
            thread_id=thread_id,
        )
        if review_snapshot is not None:
            events.append(
                self._review_requested(
                    command,
                    record,
                    review_snapshot,
                    thread_id=thread_id,
                )
            )
        elif record.result:
            events.append(
                self._event(
                    ContentDeltaEvent,
                    command,
                    thread_id=thread_id,
                    task_id=record.task_id,
                    payload=ContentDeltaPayload(
                        delta=record.result,
                        agent_name=record.agent_name,
                    ),
                )
            )
        return events

    def _remember_thread(
        self,
        *,
        thread_id: str,
        agent_name: str | None,
        active_task_id: str | None,
    ) -> None:
        """更新线程摘要并标记为当前活跃线程。"""

        now = _utc_now()
        current = self._threads.get(thread_id)
        self._threads[thread_id] = ThreadSummary(
            thread_id=thread_id,
            agent_name=agent_name or (current.agent_name if current else None),
            title=current.title if current else None,
            active_task_id=active_task_id,
            updated_at=now,
        )
        self._active_thread_id = thread_id

    def _command_accepted(
        self,
        command: ProtocolCommand,
        *,
        thread_id: str | None = None,
    ) -> CommandAcceptedEvent:
        """创建 command_accepted 事件。"""

        return self._event(
            CommandAcceptedEvent,
            command,
            thread_id=thread_id,
            payload=CommandAcceptedPayload(command_id=command.id),
        )

    def _command_rejected(
        self,
        command: ProtocolCommand,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        thread_id: str | None = None,
    ) -> CommandRejectedEvent:
        """创建 command_rejected 事件。"""

        return self._event(
            CommandRejectedEvent,
            command,
            thread_id=thread_id,
            payload=CommandRejectedPayload(
                command_id=command.id,
                code=code,
                message=message,
                details=details,
            ),
        )

    def _error_occurred(
        self,
        command: ProtocolCommand,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        thread_id: str | None = None,
    ) -> ErrorOccurredEvent:
        """创建 error_occurred 事件。"""

        return self._event(
            ErrorOccurredEvent,
            command,
            thread_id=thread_id,
            payload=ErrorOccurredPayload(
                code=code,
                message=message,
                details=details,
            ),
        )

    def _task_updated(
        self,
        command: ProtocolCommand,
        record: TaskRecord,
        *,
        thread_id: str | None = None,
    ) -> TaskUpdatedEvent:
        """创建 task_updated 事件。"""

        return self._event(
            TaskUpdatedEvent,
            command,
            thread_id=thread_id,
            task_id=record.task_id,
            payload=_task_payload_from_record(record),
        )

    def _review_requested(
        self,
        command: ProtocolCommand,
        record: TaskRecord,
        review: ReviewSnapshot,
        *,
        thread_id: str | None = None,
    ) -> ReviewRequestedEvent:
        """创建 review_requested 事件。"""

        return self._event(
            ReviewRequestedEvent,
            command,
            thread_id=thread_id,
            task_id=record.task_id,
            review_id=review.review_id,
            payload=ReviewRequestedPayload(
                review_id=review.review_id,
                task_id=record.task_id,
                agent_name=record.agent_name,
                actions=review.actions,
                allowed_decisions=review.allowed_decisions,
                risk=review.risk,
                reason=review.reason,
            ),
        )

    def _event(
        self,
        event_type: type[ProtocolEvent],
        command: ProtocolCommand,
        *,
        payload: Any,
        thread_id: str | None = None,
        task_id: str | None = None,
        review_id: str | None = None,
    ) -> AnyProtocolEvent:
        """
        创建带统一信封字段的协议事件

        Args:
            event_type: 具体事件模型类型
            command: 触发事件的命令
            payload: 事件载荷
            thread_id: 覆盖事件线程 ID
            task_id: 覆盖事件任务 ID
            review_id: 覆盖事件审批 ID

        Returns:
            可判别联合类型中的协议事件
        """

        self._seq += 1
        return cast(
            AnyProtocolEvent,
            event_type(
                seq=self._seq,
                thread_id=thread_id or command.thread_id,
                task_id=task_id or command.task_id,
                review_id=review_id or command.review_id,
                correlation_id=command.id,
                metadata=dict(command.metadata),
                payload=payload,
            ),
        )
