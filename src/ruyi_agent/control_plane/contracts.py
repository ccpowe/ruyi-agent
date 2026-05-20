"""
控制面协议契约 - CLI、TUI、Gateway 共用的数据模型

这个模块定义了控制面上的命令、事件和快照结构，用于让不同入口以统一
协议驱动任务、审批和线程状态。

核心功能：
1. 定义任务、审批、命令和事件的稳定枚举值
2. 用 Pydantic 模型约束协议输入输出
3. 提供可判别联合类型，方便入口层按 kind 做安全解析

使用场景：
- UI 客户端提交用户消息、任务输入和审批决策
- Runtime 将任务状态、内容增量和审批请求转成事件流
- 控制面输出当前线程、任务、审批和审计的完整快照

数据流：
  ProtocolCommand → ProtocolController → Runtime → ProtocolEvent/RuntimeSnapshot

关键概念：
- command: 外部入口发给控制面的操作请求
- event: 控制面对一次 command 的有序响应
- snapshot: 当前运行时状态的查询视图
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Any, Literal, TypeAlias
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


def _utc_now() -> datetime:
    """生成带 UTC 时区的当前时间，避免协议时间戳出现本地时区歧义。"""

    return datetime.now(UTC)


def _new_id() -> str:
    """生成协议对象默认 ID。"""

    return str(uuid4())


class TaskStatus(str, Enum):
    """任务生命周期状态枚举。"""

    PENDING = "pending"
    RUNNING = "running"
    WAITING_FOR_HUMAN = "waiting_for_human"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INTERRUPTED = "interrupted"


class ReviewDecisionKind(str, Enum):
    """人工审批决策类型。"""

    APPROVE = "approve"
    REJECT = "reject"
    EDIT = "edit"


class CommandKind(str, Enum):
    """控制面可接受的命令类型。"""

    SEND_USER_MESSAGE = "send_user_message"
    SEND_TASK_INPUT = "send_task_input"
    SUBMIT_REVIEW_DECISION = "submit_review_decision"
    CANCEL_TASK = "cancel_task"
    SWITCH_THREAD = "switch_thread"


class EventKind(str, Enum):
    """控制面向入口层返回的事件类型。"""

    COMMAND_ACCEPTED = "command_accepted"
    COMMAND_REJECTED = "command_rejected"
    TURN_STARTED = "turn_started"
    CONTENT_DELTA = "content_delta"
    TASK_UPDATED = "task_updated"
    REVIEW_REQUESTED = "review_requested"
    REVIEW_RESOLVED = "review_resolved"
    AUDIT_APPENDED = "audit_appended"
    TURN_COMPLETED = "turn_completed"
    ERROR_OCCURRED = "error_occurred"


class ProtocolModel(BaseModel):
    """
    控制面协议模型基类

    统一禁止额外字段，避免不同入口悄悄传入协议未声明的数据，导致后续
    兼容性和审计语义不清晰。
    """

    model_config = ConfigDict(extra="forbid")


class ProtocolEnvelope(ProtocolModel):
    """
    控制面协议信封

    所有命令和事件都携带同一组关联字段，便于入口层按 command、thread、
    task 或 review 建立链路追踪。

    Attributes:
        protocol_version: 协议版本
        id: 当前协议对象 ID
        kind: 命令或事件类型
        created_at: 协议对象创建时间
        thread_id: 关联线程 ID
        task_id: 关联任务 ID
        review_id: 关联审批 ID
        correlation_id: 关联的上游命令 ID
        metadata: 入口层透传的元数据
    """

    protocol_version: Literal["v1"] = "v1"
    id: str = Field(default_factory=_new_id)
    kind: str
    created_at: datetime = Field(default_factory=_utc_now)
    thread_id: str | None = None
    task_id: str | None = None
    review_id: str | None = None
    correlation_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProtocolCommand(ProtocolEnvelope):
    """控制面命令基类。"""

    pass


class UserMessagePayload(ProtocolModel):
    """发起 root agent 对话的用户消息载荷。"""

    content: str = Field(min_length=1)
    agent_name: str | None = None


class TaskInputPayload(ProtocolModel):
    """发送给已有任务的继续输入载荷。"""

    content: str = Field(min_length=1)


class ReviewDecision(ProtocolModel):
    """
    单个审批动作的决策

    edit 决策必须携带 edited_args，转换到运行时协议时会被组装成
    LangGraph/DeepAgents 期望的 edited_action。
    """

    action_id: str | None = None
    decision: ReviewDecisionKind
    message: str | None = None
    edited_args: dict[str, Any] | None = None


class SubmitReviewDecisionPayload(ProtocolModel):
    """提交审批决策的命令载荷。"""

    decisions: list[ReviewDecision] = Field(min_length=1)


class CancelTaskPayload(ProtocolModel):
    """取消任务命令载荷。"""

    reason: str | None = None


class SwitchThreadPayload(ProtocolModel):
    """切换当前交互线程的命令载荷。"""

    agent_name: str | None = None


class SendUserMessageCommand(ProtocolCommand):
    """向 root agent 发送新用户消息。"""

    kind: Literal["send_user_message"] = CommandKind.SEND_USER_MESSAGE.value
    payload: UserMessagePayload


class SendTaskInputCommand(ProtocolCommand):
    """向正在等待输入的任务发送继续内容。"""

    kind: Literal["send_task_input"] = CommandKind.SEND_TASK_INPUT.value
    task_id: str
    payload: TaskInputPayload


class SubmitReviewDecisionCommand(ProtocolCommand):
    """提交某个 pending review 的人工审批结果。"""

    kind: Literal["submit_review_decision"] = CommandKind.SUBMIT_REVIEW_DECISION.value
    review_id: str
    payload: SubmitReviewDecisionPayload


class CancelTaskCommand(ProtocolCommand):
    """取消一个已有任务。"""

    kind: Literal["cancel_task"] = CommandKind.CANCEL_TASK.value
    task_id: str
    payload: CancelTaskPayload = Field(default_factory=CancelTaskPayload)


class SwitchThreadCommand(ProtocolCommand):
    """切换入口层当前活跃线程。"""

    kind: Literal["switch_thread"] = CommandKind.SWITCH_THREAD.value
    thread_id: str
    payload: SwitchThreadPayload = Field(default_factory=SwitchThreadPayload)


AnyProtocolCommand: TypeAlias = Annotated[
    SendUserMessageCommand
    | SendTaskInputCommand
    | SubmitReviewDecisionCommand
    | CancelTaskCommand
    | SwitchThreadCommand,
    Field(discriminator="kind"),
]


class ProtocolEvent(ProtocolEnvelope):
    """
    控制面事件基类

    seq 是 controller 内的单调递增序号，用于让 UI 在同一次会话中按产生顺序
    稳定渲染事件。
    """

    seq: int = Field(ge=0)


class CommandAcceptedPayload(ProtocolModel):
    """命令已被控制面接受的事件载荷。"""

    command_id: str


class CommandRejectedPayload(ProtocolModel):
    """命令被拒绝或执行失败时的事件载荷。"""

    command_id: str
    code: str
    message: str
    details: dict[str, Any] | None = None


class TurnPayload(ProtocolModel):
    """root agent 一轮对话开始或结束的事件载荷。"""

    turn_id: str
    agent_name: str | None = None


class ContentDeltaPayload(ProtocolModel):
    """模型或任务输出的文本增量载荷。"""

    delta: str
    role: str = "assistant"
    agent_name: str | None = None
    channel: str | None = None


class TaskUpdatedPayload(ProtocolModel):
    """任务状态变化事件载荷。"""

    task_id: str
    agent_name: str
    status: TaskStatus
    run_count: int = Field(ge=0)
    last_result: str | None = None
    error: str | None = None
    updated_at: datetime


class ReviewActionSnapshot(ProtocolModel):
    """
    审批动作快照

    保存一次待审批工具调用的用户可见信息，用于 CLI/TUI/Gateway 展示同一份
    审批上下文。
    """

    action_id: str | None = None
    tool_name: str
    args: dict[str, Any] = Field(default_factory=dict)
    description: str | None = None
    risk: str | None = None
    reason: str | None = None
    allowed_decisions: list[ReviewDecisionKind] = Field(default_factory=list)


class ReviewRequestedPayload(ProtocolModel):
    """新审批请求事件载荷。"""

    review_id: str
    task_id: str
    agent_name: str | None = None
    actions: list[ReviewActionSnapshot] = Field(default_factory=list)
    allowed_decisions: list[ReviewDecisionKind] = Field(default_factory=list)
    risk: str | None = None
    reason: str | None = None


class ReviewResolvedPayload(ProtocolModel):
    """审批已处理事件载荷。"""

    review_id: str
    task_id: str
    decisions: list[ReviewDecision] = Field(default_factory=list)


class AuditAppendedPayload(ProtocolModel):
    """审计事件已写入后的通知载荷。"""

    audit_id: str
    event_type: str
    source: str | None = None
    payload: dict[str, Any] | None = None


class ErrorOccurredPayload(ProtocolModel):
    """通用错误事件载荷。"""

    code: str
    message: str
    details: dict[str, Any] | None = None


class CommandAcceptedEvent(ProtocolEvent):
    """命令接受事件。"""

    kind: Literal["command_accepted"] = EventKind.COMMAND_ACCEPTED.value
    payload: CommandAcceptedPayload


class CommandRejectedEvent(ProtocolEvent):
    """命令拒绝事件。"""

    kind: Literal["command_rejected"] = EventKind.COMMAND_REJECTED.value
    payload: CommandRejectedPayload


class TurnStartedEvent(ProtocolEvent):
    """root agent 对话轮次开始事件。"""

    kind: Literal["turn_started"] = EventKind.TURN_STARTED.value
    payload: TurnPayload


class ContentDeltaEvent(ProtocolEvent):
    """内容增量事件。"""

    kind: Literal["content_delta"] = EventKind.CONTENT_DELTA.value
    payload: ContentDeltaPayload


class TaskUpdatedEvent(ProtocolEvent):
    """任务状态更新事件。"""

    kind: Literal["task_updated"] = EventKind.TASK_UPDATED.value
    payload: TaskUpdatedPayload


class ReviewRequestedEvent(ProtocolEvent):
    """审批请求创建事件。"""

    kind: Literal["review_requested"] = EventKind.REVIEW_REQUESTED.value
    payload: ReviewRequestedPayload


class ReviewResolvedEvent(ProtocolEvent):
    """审批请求完成事件。"""

    kind: Literal["review_resolved"] = EventKind.REVIEW_RESOLVED.value
    payload: ReviewResolvedPayload


class AuditAppendedEvent(ProtocolEvent):
    """审计记录追加事件。"""

    kind: Literal["audit_appended"] = EventKind.AUDIT_APPENDED.value
    payload: AuditAppendedPayload


class TurnCompletedEvent(ProtocolEvent):
    """root agent 对话轮次完成事件。"""

    kind: Literal["turn_completed"] = EventKind.TURN_COMPLETED.value
    payload: TurnPayload


class ErrorOccurredEvent(ProtocolEvent):
    """通用错误事件。"""

    kind: Literal["error_occurred"] = EventKind.ERROR_OCCURRED.value
    payload: ErrorOccurredPayload


AnyProtocolEvent: TypeAlias = Annotated[
    CommandAcceptedEvent
    | CommandRejectedEvent
    | TurnStartedEvent
    | ContentDeltaEvent
    | TaskUpdatedEvent
    | ReviewRequestedEvent
    | ReviewResolvedEvent
    | AuditAppendedEvent
    | TurnCompletedEvent
    | ErrorOccurredEvent,
    Field(discriminator="kind"),
]


class ThreadSummary(ProtocolModel):
    """线程列表中的轻量摘要。"""

    thread_id: str
    agent_name: str | None = None
    title: str | None = None
    active_task_id: str | None = None
    updated_at: datetime


class TaskSnapshot(ProtocolModel):
    """任务查询视图。"""

    task_id: str
    agent_name: str
    status: TaskStatus
    thread_id: str
    parent_task_id: str | None = None
    root_task_id: str | None = None
    run_count: int = Field(ge=0)
    route_kind: str = "local"
    upstream_task_id: str | None = None
    last_result: str | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime


class ReviewSnapshot(ProtocolModel):
    """审批查询视图。"""

    review_id: str
    task_id: str
    agent_name: str | None = None
    thread_id: str | None = None
    status: Literal["pending", "resolved"] = "pending"
    actions: list[ReviewActionSnapshot] = Field(default_factory=list)
    allowed_decisions: list[ReviewDecisionKind] = Field(default_factory=list)
    risk: str | None = None
    reason: str | None = None
    created_at: datetime
    updated_at: datetime


class AuditEventSnapshot(ProtocolModel):
    """审批审计记录查询视图。"""

    audit_id: str
    event_type: str
    created_at: datetime
    source: str | None = None
    review_id: str | None = None
    task_id: str | None = None
    thread_id: str | None = None
    agent_name: str | None = None
    tool_name: str | None = None
    policy_decision: str | None = None
    risk: str | None = None
    reason: str | None = None
    payload: dict[str, Any] | None = None


class RuntimeSnapshot(ProtocolModel):
    """
    控制面运行时快照

    聚合当前线程、任务、待审批项和最近审计事件，供 UI 初始化或刷新时一次性
    恢复可见状态。
    """

    protocol_version: Literal["v1"] = "v1"
    snapshot_id: str = Field(default_factory=_new_id)
    created_at: datetime = Field(default_factory=_utc_now)
    active_thread_id: str | None = None
    threads: list[ThreadSummary] = Field(default_factory=list)
    tasks: list[TaskSnapshot] = Field(default_factory=list)
    pending_reviews: list[ReviewSnapshot] = Field(default_factory=list)
    recent_audit_events: list[AuditEventSnapshot] = Field(default_factory=list)
