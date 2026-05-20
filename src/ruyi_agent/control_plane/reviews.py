"""
审批控制 - 统一 root agent 与任务审批的控制面视图

这个模块把不同来源的 HITL 审批请求统一整理为 `ReviewSnapshot`，并负责把
用户提交的协议层决策转换回 runtime 需要的 resume payload。

核心功能：
1. 从 DeepAgents pending_review payload 中提取审批动作快照
2. 维护 root agent interrupt 产生的内存审批注册表
3. 将 root review 与 task review 聚合成同一个控制面接口
4. 将 approve/reject/edit 决策转换为 runtime 可恢复执行的结构

使用场景：
- CLI/TUI/Gateway 查询所有待处理审批
- 用户提交审批决策后恢复 root agent 或 worker task
- 审批 UI 需要展示工具名、参数、风险和可选决策

数据流：
  pending_review/TaskRecord → ReviewSnapshot → ReviewDecision → runtime decisions

关键概念：
- root review: root agent 当前线程中断产生的审批，未绑定 task_id
- task review: worker/subagent 任务等待人工决策产生的审批
- action snapshot: 单个工具调用在审批界面中的展示数据
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import uuid4

from ruyi_agent.runtime.delegation.async_runtime import TaskRecord
from ruyi_agent.control_plane.contracts import (
    ReviewActionSnapshot,
    ReviewDecision,
    ReviewDecisionKind,
    ReviewSnapshot,
)


def _utc_now() -> datetime:
    """生成带 UTC 时区的当前时间，供 root review 注册表使用。"""

    return datetime.now(UTC)


def _review_id_from_record(record: TaskRecord) -> str | None:
    """从任务记录的 pending_review 字段中提取有效 review_id。"""

    pending_review = record.pending_review or {}
    review_id = pending_review.get("review_id")
    return review_id if isinstance(review_id, str) and review_id else None


def _allowed_decisions_from_values(values: Any) -> list[ReviewDecisionKind]:
    """把运行时 payload 中的字符串决策转换成协议枚举。"""

    if not isinstance(values, list):
        return []
    allowed: list[ReviewDecisionKind] = []
    for value in values:
        try:
            allowed.append(ReviewDecisionKind(str(value)))
        except ValueError:
            continue
    return allowed


def review_actions_from_payload(
    pending_review: dict[str, Any],
) -> list[ReviewActionSnapshot]:
    """
    从运行时 pending_review payload 构造审批动作快照

    DeepAgents 的中断 payload 会把 action_requests 和 review_configs 分开存放。
    控制面需要把同一索引位置的 action/config 合并，才能给 UI 提供完整的工具名、
    参数、风险说明和 allowed_decisions。

    Args:
        pending_review: 运行时保存的 pending review 原始 payload

    Returns:
        可直接暴露给控制面协议的审批动作快照列表
    """

    raw_actions = pending_review.get("action_requests")
    raw_configs = pending_review.get("review_configs")
    actions = raw_actions if isinstance(raw_actions, list) else []
    configs = raw_configs if isinstance(raw_configs, list) else []
    snapshots: list[ReviewActionSnapshot] = []
    for index, raw_action in enumerate(actions):
        if not isinstance(raw_action, dict):
            continue
        raw_config = configs[index] if index < len(configs) else {}
        config = raw_config if isinstance(raw_config, dict) else {}
        # 兼容不同 runtime 版本：有的 payload 使用 id，有的使用 tool_call_id。
        action_id = raw_action.get("id") or raw_action.get("tool_call_id")
        tool_name = raw_action.get("name") or config.get("action_name") or "tool"
        args = raw_action.get("args")
        snapshots.append(
            ReviewActionSnapshot(
                action_id=str(action_id) if action_id is not None else None,
                tool_name=str(tool_name),
                args=args if isinstance(args, dict) else {},
                description=(
                    str(raw_action["description"])
                    if isinstance(raw_action.get("description"), str)
                    else None
                ),
                risk=(
                    str(raw_action["risk"])
                    if isinstance(raw_action.get("risk"), str)
                    else None
                ),
                reason=(
                    str(raw_action["reason"])
                    if isinstance(raw_action.get("reason"), str)
                    else None
                ),
                allowed_decisions=_allowed_decisions_from_values(
                    config.get("allowed_decisions")
                ),
            )
        )
    return snapshots


def allowed_decisions_from_actions(
    actions: Iterable[ReviewActionSnapshot],
) -> list[ReviewDecisionKind]:
    """
    汇总审批动作允许的决策类型

    Args:
        actions: 待审批动作快照集合

    Returns:
        去重且保持首次出现顺序的决策类型；若动作未声明，则默认允许 approve/reject
    """

    seen: set[ReviewDecisionKind] = set()
    allowed: list[ReviewDecisionKind] = []
    for action in actions:
        for decision in action.allowed_decisions:
            if decision not in seen:
                seen.add(decision)
                allowed.append(decision)
    if allowed:
        return allowed
    return [ReviewDecisionKind.APPROVE, ReviewDecisionKind.REJECT]


def review_snapshot_from_record(
    record: TaskRecord,
    *,
    thread_id: str | None = None,
) -> ReviewSnapshot | None:
    """
    从任务记录构造审批快照

    Args:
        record: 可能处于 pending_review 状态的任务记录
        thread_id: 调用方需要覆盖展示线程时传入的线程 ID

    Returns:
        如果任务正在等待审批，返回审批快照；否则返回 `None`
    """

    review_id = _review_id_from_record(record)
    if review_id is None or record.pending_review is None:
        return None
    actions = review_actions_from_payload(record.pending_review)
    return ReviewSnapshot(
        review_id=review_id,
        task_id=record.task_id,
        agent_name=record.agent_name,
        thread_id=thread_id or record.thread_id,
        status="pending",
        actions=actions,
        allowed_decisions=allowed_decisions_from_actions(actions),
        risk=next((action.risk for action in actions if action.risk), None),
        reason=next((action.reason for action in actions if action.reason), None),
        created_at=record.updated_at,
        updated_at=record.updated_at,
    )


def runtime_decisions_from_review_payload(
    decisions: list[ReviewDecision],
    *,
    pending_review: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    将协议层审批决策转换为 runtime resume 载荷

    Args:
        decisions: 用户通过控制面提交的审批决策
        pending_review: 原始 pending review payload，用于给 edit 决策补回工具名

    Returns:
        DeepAgents/LangGraph resume 所需的 decisions 列表

    Raises:
        ValueError: edit 决策缺少 edited_args 时抛出
    """

    actions = review_actions_from_payload(pending_review)
    converted: list[dict[str, Any]] = []
    for index, decision in enumerate(decisions):
        payload: dict[str, Any] = {"type": decision.decision.value}
        if decision.message is not None:
            payload["message"] = decision.message
        if decision.decision == ReviewDecisionKind.EDIT:
            if decision.edited_args is None:
                raise ValueError("Edit review decision requires edited_args")
            # runtime 的 edited_action 需要工具名；协议层只允许用户改 args。
            tool_name = actions[index].tool_name if index < len(actions) else "tool"
            payload["edited_action"] = {
                "name": tool_name,
                "args": decision.edited_args,
            }
        converted.append(payload)
    return converted


def runtime_decisions_from_record(
    decisions: list[ReviewDecision],
    *,
    record: TaskRecord,
) -> list[dict[str, Any]]:
    """
    基于任务记录转换审批决策

    Args:
        decisions: 协议层审批决策
        record: 持有 pending_review payload 的任务记录

    Returns:
        runtime resume 所需的 decisions 列表
    """

    return runtime_decisions_from_review_payload(
        decisions,
        pending_review=record.pending_review or {},
    )


@dataclass(slots=True)
class RootReviewState:
    """
    root agent 审批的内存状态

    root agent 的中断不一定对应一个 TaskRecord，因此需要单独保存 thread 和
    pending_review，用于后续提交审批时恢复同一条 LangGraph 线程。

    Attributes:
        agent_name: 产生审批请求的 root agent 名称
        thread_id: root agent 所在线程 ID
        pending_review: 原始审批 payload
        created_at: 审批注册时间
        updated_at: 审批最后更新时间
    """

    agent_name: str
    thread_id: str
    pending_review: dict[str, Any]
    created_at: datetime = field(default_factory=_utc_now)
    updated_at: datetime = field(default_factory=_utc_now)


class RootReviewRegistry:
    """
    root agent 审批注册表

    只负责管理当前进程内 root interrupt 的待审批状态。worker/subagent 的审批
    仍由 TaskRecord 持有，避免把两类生命周期混在一个存储结构里。
    """

    def __init__(self) -> None:
        """初始化空的 root review 注册表。"""

        self._reviews: dict[str, RootReviewState] = {}

    def register(
        self,
        *,
        agent_name: str,
        thread_id: str,
        pending_review: dict[str, Any],
    ) -> ReviewSnapshot | None:
        """
        注册一个 root agent 中断产生的审批请求

        Args:
            agent_name: 产生中断的 agent 名称
            thread_id: 当前 LangGraph 线程 ID
            pending_review: 运行时返回的审批 payload

        Returns:
            注册后的审批快照；payload 无法转换时返回 `None`
        """

        review_payload = dict(pending_review)
        review_id = review_payload.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            # root interrupt 可能没有稳定 ID；控制面补 ID 后才能让 UI 定位决策。
            review_id = str(uuid4())
            review_payload["review_id"] = review_id
        now = _utc_now()
        self._reviews[review_id] = RootReviewState(
            agent_name=agent_name,
            thread_id=thread_id,
            pending_review=review_payload,
            created_at=now,
            updated_at=now,
        )
        return self.snapshot(review_id)

    def snapshot(self, review_id: str) -> ReviewSnapshot | None:
        """
        查询 root review 的协议快照

        Args:
            review_id: 审批 ID

        Returns:
            审批快照；不存在时返回 `None`
        """

        state = self._reviews.get(review_id)
        if state is None:
            return None
        actions = review_actions_from_payload(state.pending_review)
        return ReviewSnapshot(
            review_id=review_id,
            task_id="",
            agent_name=state.agent_name,
            thread_id=state.thread_id,
            status="pending",
            actions=actions,
            allowed_decisions=allowed_decisions_from_actions(actions),
            risk=next((action.risk for action in actions if action.risk), None),
            reason=next((action.reason for action in actions if action.reason), None),
            created_at=state.created_at,
            updated_at=state.updated_at,
        )

    def get(self, review_id: str) -> RootReviewState | None:
        """按 ID 获取 root review 状态，不存在时返回 `None`。"""

        return self._reviews.get(review_id)

    def pop(self, review_id: str) -> RootReviewState:
        """
        移除并返回 root review 状态

        Args:
            review_id: 审批 ID

        Returns:
            被移除的 root review 状态

        Raises:
            KeyError: review_id 不存在时抛出
        """

        try:
            return self._reviews.pop(review_id)
        except KeyError as exc:
            raise KeyError(f"Unknown root review: {review_id}") from exc

    def list_pending(self) -> list[ReviewSnapshot]:
        """按更新时间升序列出所有待处理 root review。"""

        reviews = [
            review
            for review_id in self._reviews
            if (review := self.snapshot(review_id)) is not None
        ]
        return sorted(reviews, key=lambda item: item.updated_at)


class RootReviewRunner(Protocol):
    """root agent 审批恢复执行所需的最小接口。"""

    async def resume_review(
        self,
        *,
        agent_name: str,
        thread_id: str,
        decisions: list[dict[str, Any]],
    ) -> Any:
        """
        用审批决策恢复 root agent 线程。

        Args:
            agent_name: root agent 名称
            thread_id: 需要恢复的线程 ID
            decisions: runtime 期望的决策载荷

        Returns:
            root agent 恢复执行后的结果对象
        """

        pass


@dataclass(slots=True)
class ReviewSubmitResult:
    """
    审批提交结果

    Attributes:
        source: 审批来源，root 表示 root interrupt，task 表示 TaskRecord
        review_id: 被处理的审批 ID
        decisions: 用户提交的协议层决策
        root_result: root review 恢复后的运行结果
        task_record: task review 恢复后的任务记录
        thread_id: 审批所属线程 ID
    """

    source: Literal["root", "task"]
    review_id: str
    decisions: list[ReviewDecision]
    root_result: Any | None = None
    task_record: TaskRecord | None = None
    thread_id: str | None = None


@dataclass(slots=True)
class ReviewControl:
    """
    审批控制面服务

    统一封装 root review 与 task review，让入口层无需关心审批来自 root agent
    当前线程，还是来自异步 worker/subagent 任务。

    主要功能：
    - 注册 root agent interrupt
    - 查询待处理审批
    - 获取单个审批快照
    - 提交审批决策并恢复对应运行时

    Attributes:
        control: 任务控制对象，提供 task review 的查询和恢复能力
        root_runner: root agent 恢复执行接口
        root_reviews: root review 内存注册表
    """

    control: Any
    root_runner: RootReviewRunner
    root_reviews: RootReviewRegistry = field(default_factory=RootReviewRegistry)

    def register_root_interrupts(
        self,
        *,
        agent_name: str,
        thread_id: str,
        interrupt_requests: list[dict[str, Any]],
    ) -> list[ReviewSnapshot]:
        """
        批量注册 root agent 返回的 interrupt 请求

        Args:
            agent_name: 产生中断的 agent 名称
            thread_id: 当前线程 ID
            interrupt_requests: root agent 返回的原始中断 payload 列表

        Returns:
            成功注册的审批快照列表
        """

        reviews: list[ReviewSnapshot] = []
        for pending_review in interrupt_requests:
            review = self.root_reviews.register(
                agent_name=agent_name,
                thread_id=thread_id,
                pending_review=pending_review,
            )
            if review is not None:
                reviews.append(review)
        return reviews

    def list_pending_reviews(self) -> list[ReviewSnapshot]:
        """
        列出所有待处理审批

        Returns:
            root review 与 task review 合并后的审批快照列表
        """

        task_reviews = [
            review
            for record in self.control.list_pending_review_records()
            if (review := review_snapshot_from_record(record)) is not None
        ]
        return [*self.root_reviews.list_pending(), *task_reviews]

    def get_review(self, review_id: str) -> ReviewSnapshot:
        """
        获取单个待审批项

        Args:
            review_id: 审批 ID

        Returns:
            审批快照

        Raises:
            ValueError: 审批不存在或已不处于 pending 状态
        """

        root_review = self.root_reviews.snapshot(review_id)
        if root_review is not None:
            return root_review
        record = self.control.get_task_by_review_id(review_id)
        review = review_snapshot_from_record(record)
        if review is None:
            raise ValueError(f"Review '{review_id}' is not pending")
        return review

    async def submit_decision(
        self,
        review_id: str,
        decisions: list[ReviewDecision],
        *,
        wait: bool = False,
    ) -> ReviewSubmitResult:
        """
        提交审批决策并恢复对应运行时

        Args:
            review_id: 审批 ID
            decisions: 用户提交的协议层审批决策
            wait: task review 是否等待任务继续执行后的结果

        Returns:
            审批提交结果，包含来源和恢复后的结果对象
        """

        root_review = self.root_reviews.get(review_id)
        if root_review is not None:
            runtime_decisions = runtime_decisions_from_review_payload(
                decisions,
                pending_review=root_review.pending_review,
            )
            root_result = await self.root_runner.resume_review(
                agent_name=root_review.agent_name,
                thread_id=root_review.thread_id,
                decisions=runtime_decisions,
            )
            # root review 恢复成功后从内存注册表移除，避免重复提交同一审批。
            self.root_reviews.pop(review_id)
            return ReviewSubmitResult(
                source="root",
                review_id=review_id,
                decisions=decisions,
                root_result=root_result,
                thread_id=root_review.thread_id,
            )

        record = self.control.get_task_by_review_id(review_id)
        runtime_decisions = runtime_decisions_from_record(
            decisions,
            record=record,
        )
        task_record = await self.control.submit_review_decision(
            review_id,
            runtime_decisions,
            wait=wait,
        )
        return ReviewSubmitResult(
            source="task",
            review_id=review_id,
            decisions=decisions,
            task_record=task_record,
            thread_id=task_record.thread_id,
        )
