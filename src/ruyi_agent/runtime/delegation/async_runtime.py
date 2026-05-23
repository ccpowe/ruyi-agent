"""
Async Subagent Runtime - 异步委托任务运行时

这个模块实现了主 agent 委托本地 worker 或远端 agent 的调度控制层。
它把 agent 注册、任务状态机、本地异步执行、远端 A2A 调用、委托预算、
mailbox 通知和工具暴露统一在一个 runtime 中。

核心功能：
1. 注册本地 worker 与远端 remote_ref 目标
2. 创建、继续、等待、查询、取消委托任务
3. 管理任务树的 root、depth 和每棵树的任务预算
4. 同步远端网关任务状态并处理 webhook 回调
5. 将调度能力包装成 LangChain StructuredTool 供 agent 调用

使用场景：
- 主 agent 把可并行或可分工的工作委托给本地 worker
- 本地网关把任务转发给远端网关上的 agent
- Gateway HTTP 层需要结构化创建、查询、取消任务
- 子任务终态后通过 mailbox 或 webhook 通知父调用方

数据流：
  spawn_agent/spawn_task → AgentRegistry 校验目标 → TaskManager 创建记录
    → 本地 worker 异步运行 或 A2AClient 调用远端网关
    → TaskManager 更新状态 → mailbox/webhook 投递终态

关键概念：
- task_id: 本地 runtime 追踪的任务 ID
- upstream_task_id: 远端网关返回的原始任务 ID
- root_task_id: 委托树根任务，用于深度和总任务数限制
- parent_thread_id: 父 agent thread，用于 mailbox 回投
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from collections.abc import Mapping, Sequence
from typing import Any

import httpx
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool
from langgraph.types import Command
from pydantic import BaseModel, Field

from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.runtime.delegation.context import (
    DelegationContext,
    MetadataScalar,
    build_child_context,
    build_root_context,
    inject_context_metadata,
    parse_inbound_metadata,
    validate_node_id,
)
from ruyi_agent.runtime.agent_turn import normalize_agent_turn
from ruyi_agent.runtime.agent_factory import create_runtime_agent
from ruyi_agent.storage.task_store import TaskStore, task_record_for_restart
from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.storage.review_audit import ReviewAuditStore
from ruyi_agent.runtime.skills.resolver import resolve_skill_names
from ruyi_agent.runtime.skills.sync import SkillSyncer
from ruyi_agent.runtime.skills.types import SkillEntry

logger = logging.getLogger(__name__)

# 显式把已登记的目标写进 tool description，
# 这样模型在调用 spawn_agent 时能拿到合法名称，也能区分本地 worker 和远端引用。
SPAWN_AGENT_TOOL_DESCRIPTION = """Start a delegated task and return a task ID immediately when the target is spawnable.

Available local workers:
{available_local_workers}

Available remote refs:
{available_remote_refs}

## Usage notes:
1. `agent_name` must be exactly one of the registered target names above.
2. Use this tool for tasks that can be delegated to a focused local worker or remote ref.
3. If the current turn depends on the result, call `wait_agent` after spawning.
4. If the task can continue in the background, report the task ID and stop.
5. Remote refs run through the remote gateway and may have network or auth failures.
6. Do not invent agent target names.
"""


class SpawnAgentSchema(BaseModel):
    """
    spawn_agent 工具参数 schema

    使用显式 schema 是为了把 agent_name 和 task 的业务语义写进工具参数
    描述，减少模型调用时编造目标名或传入模糊任务的概率。
    """

    # 使用显式 schema 而不是函数签名推断，
    # 是为了把 agent_name/task 的语义描述写得更准确。
    agent_name: str = Field(
        description=(
            "The local worker type to use. Must be one of the available "
            "types listed in the tool description."
        )
    )
    task: str = Field(
        description=("A detailed task description for the worker to execute.")
    )


class TaskIdSchema(BaseModel):
    """只接收 task_id 的工具参数 schema"""

    task_id: str = Field(
        description="The exact task_id string returned by spawn_agent."
    )


class SendInputSchema(BaseModel):
    """send_input 工具参数 schema"""

    task_id: str = Field(
        description="The exact task_id string returned by spawn_agent."
    )
    message: str = Field(
        description="Follow-up instructions or new context for the same task."
    )


class ListAgentsSchema(BaseModel):
    """list_agents 工具的空参数 schema"""

    pass


class UnknownAgentTargetError(ValueError):
    """请求的 agent 目标未在当前 runtime 中注册"""

    pass


class RemoteExecutorNotImplementedError(ValueError):
    """远端执行器能力缺失或暂不可用"""

    pass


class UnknownWorkerTaskError(ValueError):
    """请求的 worker task 不存在或对调用方不可见"""

    pass


class TaskAlreadyRunningError(ValueError):
    """同一个 task 上已有未结束的活跃 run"""

    pass


class MaxDelegationDepthError(ValueError):
    """
    委托深度超过限制

    Attributes:
        current_depth: 当前准备创建的任务深度
        max_depth: 允许的最大委托深度
    """

    def __init__(self, *, current_depth: int, max_depth: int) -> None:
        """保存深度限制错误的上下文字段"""
        self.current_depth = current_depth
        self.max_depth = max_depth
        super().__init__(f"current_depth={current_depth} max_depth={max_depth}")


class MaxTasksPerRootError(ValueError):
    """
    单棵委托树的任务数量超过限制

    Attributes:
        root_task_id: 超出预算的委托树根任务 ID
        current_count: 当前已登记的任务数量
        max_tasks_per_root: 单棵委托树允许的最大任务数量
    """

    def __init__(
        self,
        *,
        root_task_id: str,
        current_count: int,
        max_tasks_per_root: int,
    ) -> None:
        """保存任务预算错误的上下文字段"""
        self.root_task_id = root_task_id
        self.current_count = current_count
        self.max_tasks_per_root = max_tasks_per_root
        super().__init__(
            "root_task_id="
            f"{root_task_id} current_count={current_count} "
            f"max_tasks_per_root={max_tasks_per_root}"
        )


ACTIVE_TASK_STATES = {"pending", "running", "waiting_for_human"}
TERMINAL_TASK_STATES = {"completed", "failed", "cancelled", "interrupted"}
RESUMABLE_TASK_STATES = TERMINAL_TASK_STATES


def _now() -> datetime:
    """返回当前 UTC 时间"""
    # 为什么抽成单独时间入口：任务状态和测试都依赖统一时间语义，避免时间来源散落各处。
    return datetime.now(UTC)


def _flatten_exception_messages(exc: BaseException) -> list[str]:
    """
    展开异常或异常组中的可读错误信息

    Args:
        exc: 捕获到的异常对象

    Returns:
        展平后的异常摘要列表
    """
    # 为什么单独展开异常组：TaskGroup / ExceptionGroup 的 str() 信息太弱，
    # 必须把真正的子异常拿出来才能定位并发任务失败原因。
    if isinstance(exc, BaseExceptionGroup):
        messages: list[str] = []
        for sub_exc in exc.exceptions:
            messages.extend(_flatten_exception_messages(sub_exc))
        return messages

    message = str(exc).strip()
    if not message:
        message = exc.__class__.__name__
    return [f"{exc.__class__.__name__}: {message}"]


def _format_exception_summary(exc: BaseException) -> str:
    """
    格式化任务失败摘要

    Args:
        exc: 捕获到的异常对象

    Returns:
        去重后的单行错误摘要
    """
    # 为什么统一格式化异常：任务状态里需要稳定、可读、可截断的错误摘要。
    messages = _flatten_exception_messages(exc)
    unique_messages: list[str] = []
    for message in messages:
        if message not in unique_messages:
            unique_messages.append(message)
    return " | ".join(unique_messages)


def _format_interrupted_error(exc: BaseException) -> str:
    """Format a runtime interruption as a stable, user-readable task error."""
    return "Task interrupted: " + _format_exception_summary(exc)


def _parse_task_timestamp(value: Any, *, fallback: datetime) -> datetime:
    """
    解析远端任务时间戳

    远端网关返回的时间可能是 datetime、ISO 字符串或缺失值。这里统一转换为
    timezone-aware datetime，解析失败时保留本地已有时间。

    Args:
        value: 远端返回的时间戳字段
        fallback: 解析失败时使用的本地时间

    Returns:
        解析后的 datetime
    """
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return fallback
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


@dataclass(slots=True)
class AgentRecord:
    """
    agent 与 task 的轻量关系记录

    Attributes:
        name: agent 名称
        description: agent 描述
        kind: agent 类型（worker/remote_ref）
        runtime: 运行时位置或类型
        task_id: 关联任务 ID
        parent_task_id: 父任务 ID（如果存在）
    """

    name: str
    description: str
    kind: str
    runtime: str
    task_id: str
    parent_task_id: str | None = None


@dataclass(slots=True)
class RegisteredAgent:
    """
    已注册 agent 目标

    Attributes:
        name: agent 目标名称
        description: 面向模型和用户的能力描述
        kind: 目标类型（worker/remote_ref）
    """

    name: str
    description: str
    kind: str


@dataclass(slots=True)
class LocalWorkerEntry(RegisteredAgent):
    """
    本地 worker 注册项

    Attributes:
        spec: 本地 worker 的完整运行配置
    """

    spec: LocalWorkerSpec


@dataclass(slots=True)
class RemoteRefEntry(RegisteredAgent):
    """
    远端 agent 引用注册项

    Attributes:
        ref: 远端网关上的 agent 引用配置
    """

    ref: RemoteRef


@dataclass(slots=True)
class TaskRecord:
    """
    委托任务状态记录

    保存一个本地或远端委托任务的完整生命周期状态。TaskManager 负责创建和
    修改这些记录，HTTP 层和 agent 工具层都基于它返回任务视图。

    Attributes:
        task_id: 当前 runtime 内部的任务 ID
        agent_name: 执行该任务的 agent 目标名称
        state: 任务状态（pending/running/completed/failed/cancelled 等）
        thread_id: agent 会话 thread ID，本地任务默认等于 task_id
        parent_task_id: 父任务 ID
        root_task_id: 委托树根任务 ID
        depth: 当前任务在委托树中的深度
        created_at: 任务创建时间
        updated_at: 任务最后更新时间
        result: 成功终态的结果摘要
        error: 失败终态的错误摘要
        active_run: 本地任务当前活跃的 asyncio task
        run_count: 当前任务已执行的轮次数
        route_kind: 执行路由（local/remote_ref）
        upstream_task_id: 远端网关上的原始任务 ID
        parent_thread_id: 接收 mailbox 通知的父 thread ID
        mailbox_suppressed: 是否禁止终态 mailbox 投递
        mailbox_delivered: 是否已经投递过终态 mailbox 消息
        webhook: 终态 webhook 配置
        cancel_requested: 是否由显式 cancel_task 请求触发取消
        delegation_root_id: 跨网关传递的委托根 ID
        delegation_max_depth: 跨网关传递的最大委托深度
        delegation_max_tasks_per_root: 跨网关传递的单树任务预算
        delegation_visited_nodes: 跨网关委托已访问节点
        permission_profile: 当前任务运行使用的权限 profile
        effective_skill_names: 当前任务实际可见的 skill 名称
        skill_view_path: 同步到 backend 后的 skill view 目录
        skill_view_hash: skill view 内容指纹
        pending_review: worker 等待人工审批时的 review payload
    """

    task_id: str
    agent_name: str
    state: str
    thread_id: str
    parent_task_id: str | None
    root_task_id: str
    depth: int
    created_at: datetime
    updated_at: datetime
    result: str | None = None
    error: str | None = None
    active_run: asyncio.Task[None] | None = None
    run_count: int = 0
    route_kind: str = "local"
    upstream_task_id: str | None = None
    parent_thread_id: str | None = None
    mailbox_suppressed: bool = False
    mailbox_delivered: bool = False
    webhook: dict[str, Any] | None = None
    cancel_requested: bool = False
    delegation_root_id: str | None = None
    delegation_max_depth: int | None = None
    delegation_max_tasks_per_root: int | None = None
    delegation_visited_nodes: tuple[str, ...] = ()
    permission_profile: str = ""
    effective_skill_names: tuple[str, ...] = ()
    skill_view_path: str | None = None
    skill_view_hash: str | None = None
    pending_review: dict[str, Any] | None = None


class AgentRegistry:
    """
    agent 目标注册表

    统一管理本地 worker 和远端 remote_ref 的登记信息，供 spawn、工具描述、
    Gateway 列表接口和本地 worker 编译使用。

    主要功能：
    - 校验 agent 目标是否存在
    - 按本地 worker / remote_ref 渲染工具描述
    - 返回结构化注册项快照
    - 为本地 worker 提供 LocalWorkerSpec

    设计要点：
    - 本地 worker 和 remote_ref 共用同一命名空间，避免模型调用时产生歧义
    - get_spec 只允许本地 worker，远端目标必须通过 A2AClient 调用

    Attributes:
        _entries: 以 agent 名称索引的注册项
    """

    def __init__(
        self,
        specs: dict[str, LocalWorkerSpec],
        remote_refs: dict[str, RemoteRef] | None = None,
    ) -> None:
        """
        初始化 agent 注册表

        Args:
            specs: 本地 worker 配置表
            remote_refs: 远端 agent 引用配置表
        """
        # 为什么有 registry：运行时需要同时知道有哪些本地 worker 可用，以及哪些 remote_ref 已登记。
        #  ["name_1":LocalWorkerEntry(),"name_2":RemoteRefEntry()...]
        self._entries: dict[str, RegisteredAgent] = {
            name: LocalWorkerEntry(
                name=spec.name,
                description=spec.description,
                kind="worker",
                spec=spec,
            )
            for name, spec in specs.items()
        }
        for name, remote_ref in (remote_refs or {}).items():
            self._entries[name] = RemoteRefEntry(
                name=remote_ref.name,
                description=remote_ref.description,
                kind="remote_ref",
                ref=remote_ref,
            )

    def has_agent(self, agent_name: str) -> bool:
        """
        判断 agent 目标是否已注册

        Args:
            agent_name: agent 目标名称

        Returns:
            已注册返回 True，否则返回 False
        """
        # 为什么显式判断是否存在：spawn 前要尽早给出清晰错误，而不是在更深层才失败。
        return agent_name in self._entries

    def list_target_names(self, allowed_targets: set[str] | None = None) -> list[str]:
        """
        列出可用 agent 目标名称

        Args:
            allowed_targets: 调用方允许访问的目标名称集合；None 表示全部目标

        Returns:
            排序后的目标名称列表
        """
        # 为什么列出登记名：当配置错误时，需要把当前可用 target 明确反馈给主 agent。
        if allowed_targets is None:
            return sorted(self._entries.keys())
        return sorted(name for name in allowed_targets if name in self._entries)

    def render_local_worker_descriptions(
        self,
        allowed_targets: set[str] | None = None,
    ) -> str:
        """
        渲染本地 worker 的工具提示文本

        Args:
            allowed_targets: 调用方允许访问的目标名称集合；None 表示全部目标

        Returns:
            可写入 spawn_agent tool description 的 worker 列表
        """
        # 为什么分开渲染本地 worker：spawnable 列表和 remote_ref 提示需要分别展示。
        local_entries = [
            entry
            for name, entry in self._entries.items()
            if isinstance(entry, LocalWorkerEntry)
            and (allowed_targets is None or name in allowed_targets)
        ]
        if not local_entries:
            return "- none"
        return "\n".join(
            f"- {entry.name}: {entry.description}"
            for entry in sorted(local_entries, key=lambda item: item.name)
        )

    def render_remote_ref_descriptions(
        self,
        allowed_targets: set[str] | None = None,
    ) -> str:
        """
        渲染远端引用的工具提示文本

        Args:
            allowed_targets: 调用方允许访问的目标名称集合；None 表示全部目标

        Returns:
            可写入 spawn_agent tool description 的 remote_ref 列表
        """
        remote_entries = [
            entry
            for name, entry in self._entries.items()
            if isinstance(entry, RemoteRefEntry)
            and (allowed_targets is None or name in allowed_targets)
        ]
        if not remote_entries:
            return "- none"
        return "\n".join(
            (
                f"- {entry.name}: {entry.description} "
                "(remote_ref, spawnable via remote gateway)"
            )
            for entry in sorted(remote_entries, key=lambda item: item.name)
        )

    def list_registered_agents(
        self,
        allowed_targets: set[str] | None = None,
    ) -> list[RegisteredAgent]:
        """
        返回已注册 agent 的结构化列表

        Args:
            allowed_targets: 调用方允许访问的目标名称集合；None 表示全部目标

        Returns:
            已注册 agent 条目列表
        """
        if allowed_targets is None:
            return list(self._entries.values())
        return [
            entry for name, entry in self._entries.items() if name in allowed_targets
        ]

    def get_entry(self, agent_name: str) -> RegisteredAgent:
        """
        获取 agent 注册项

        Args:
            agent_name: agent 目标名称

        Returns:
            对应的注册项

        Raises:
            UnknownAgentTargetError: agent_name 未注册
        """
        try:
            return self._entries[agent_name]
        except KeyError as exc:
            raise UnknownAgentTargetError(
                f"Unknown agent target: {agent_name}"
            ) from exc

    def get_spec(self, agent_name: str) -> LocalWorkerSpec:
        """
        获取本地 worker 配置

        Args:
            agent_name: 本地 worker 名称

        Returns:
            本地 worker 的 LocalWorkerSpec

        Raises:
            UnknownAgentTargetError: agent_name 未注册
            ValueError: agent_name 指向 remote_ref，不能本地执行
        """
        # 为什么通过 registry 取 spec：只有本地 worker 才有本地运行时定义。
        entry = self.get_entry(agent_name)
        if not isinstance(entry, LocalWorkerEntry):
            raise ValueError(
                f"Agent target '{agent_name}' is a remote_ref and cannot be executed locally."
            )
        return entry.spec

    def register_task(
        self,
        task_id: str,
        *,
        agent_name: str,
        parent_task_id: str | None = None,
    ) -> None:
        """
        校验任务关联的 agent 目标

        当前实现不额外保存索引，只通过调用 get_entry 保证创建 task 前目标
        已注册。

        Args:
            task_id: 即将登记的任务 ID
            agent_name: 执行该任务的 agent 名称
            parent_task_id: 父任务 ID（如果存在）

        Raises:
            UnknownAgentTargetError: agent_name 未注册
        """
        # 为什么单独登记 task 与 agent 关系：后续 list、可视化和 parent-child 跟踪都依赖这层索引。
        self.get_entry(agent_name)


class TaskManager:
    """
    委托任务状态管理器

    负责维护所有 TaskRecord，并提供任务创建、查询、状态变更和远端状态同步
    的唯一写入口。

    主要功能：
    - 创建 pending 任务记录
    - 标记 running/completed/failed/cancelled 状态
    - 根据 thread_id 或 upstream_task_id 反查任务
    - 将远端网关 payload 同步到本地 TaskRecord

    设计要点：
    - 状态写入集中在 TaskManager，避免调用方直接分散修改字段
    - 本地和远端任务共用同一种 TaskRecord 视图

    Attributes:
        _tasks: 以 task_id 索引的任务记录表
    """

    def __init__(self, store: TaskStore | None = None) -> None:
        """初始化空任务表"""
        # 为什么有 task manager：异步子任务的状态、结果、取消和等待必须由一个中心层统一管理。
        self._tasks: dict[str, TaskRecord] = {}
        self._store = store

    def load_by_parent_thread_id(self, parent_thread_id: str) -> None:
        """
        按当前激活 agent thread 加载它创建的任务

        这个方法用于 agent middleware。每个 agent run 激活时，只恢复
        parent_thread_id 等于当前 thread_id 的 task，避免启动时全量加载历史。
        """
        if self._store is None:
            return
        for stored in self._store.list_tasks_by_parent_thread_id(parent_thread_id):
            current = self._tasks.get(stored.task_id)
            if current is not None and self._has_live_active_run(current):
                continue
            record = task_record_for_restart(stored)
            self._tasks[record.task_id] = record
            if record.state != stored.state or record.error != stored.error:
                self._save(record)

    def load_task_for_parent_thread(
        self,
        *,
        task_id: str,
        parent_thread_id: str,
    ) -> TaskRecord | None:
        """
        按 task_id 和父 thread 懒加载单个 task

        agent tool 使用这个方法恢复当前调用方可见的 task；Gateway 路径不走
        parent_thread_id 过滤，会用 load_task_by_id。
        """
        if self._store is None:
            return None
        stored = self._store.get_task_by_parent_thread_id(
            task_id=task_id,
            parent_thread_id=parent_thread_id,
        )
        if stored is None:
            return None
        current = self._tasks.get(stored.task_id)
        if current is not None and self._has_live_active_run(current):
            return current
        record = task_record_for_restart(stored)
        self._tasks[record.task_id] = record
        if record.state != stored.state or record.error != stored.error:
            self._save(record)
        return record

    def load_task_by_id(self, task_id: str) -> TaskRecord | None:
        """
        按 task_id 懒加载单个 task

        这个入口用于 Gateway HTTP、webhook 和其它控制面路径，不做 agent
        parent_thread_id 可见性过滤。
        """
        if self._store is None:
            return None
        stored = self._store.get_task(task_id)
        if stored is None:
            return None
        current = self._tasks.get(stored.task_id)
        if current is not None and self._has_live_active_run(current):
            return current
        record = task_record_for_restart(stored)
        self._tasks[record.task_id] = record
        if record.state != stored.state or record.error != stored.error:
            self._save(record)
        return record

    def _has_live_active_run(self, record: TaskRecord) -> bool:
        return record.active_run is not None and not record.active_run.done()

    def _save(self, record: TaskRecord) -> None:
        """把当前任务记录写入持久化存储"""
        if self._store is not None:
            self._store.save_task(record)

    def _mirror_pending_review_to_root(self, record: TaskRecord) -> None:
        """Mirror a child review onto the root task for channel adapters."""
        if record.root_task_id == record.task_id:
            return
        root = self._tasks.get(record.root_task_id)
        if root is None:
            root = self.load_task_by_id(record.root_task_id)
        if root is None:
            return
        payload = dict(record.pending_review or {})
        if not payload:
            return
        payload["source_task_id"] = record.task_id
        root.pending_review = payload
        root.updated_at = _now()
        self._save(root)

    def _clear_mirrored_pending_review_from_root(self, record: TaskRecord) -> None:
        """Clear the mirrored root review once the child review is resolved."""
        if record.root_task_id == record.task_id:
            return
        root = self._tasks.get(record.root_task_id)
        if root is None:
            root = self.load_task_by_id(record.root_task_id)
        if root is None:
            return
        pending_review = root.pending_review or {}
        if pending_review.get("source_task_id") != record.task_id:
            return
        root.pending_review = None
        root.updated_at = _now()
        self._save(root)

    def create_task_record(
        self,
        task_id: str,
        agent_name: str,
        *,
        parent_task_id: str | None,
        root_task_id: str,
        depth: int,
        route_kind: str = "local",
        upstream_task_id: str | None = None,
        parent_thread_id: str | None = None,
        webhook: dict[str, Any] | None = None,
        delegation_context: DelegationContext | None = None,
        permission_profile: str = "",
        effective_skill_names: Sequence[str] = (),
        skill_view_path: str | None = None,
        skill_view_hash: str | None = None,
    ) -> TaskRecord:
        """
        创建任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID
            agent_name: 执行任务的 agent 名称
            parent_task_id: 父任务 ID
            root_task_id: 委托树根任务 ID
            depth: 当前任务深度
            route_kind: 执行路由（local/remote_ref）
            upstream_task_id: 远端网关原始任务 ID
            parent_thread_id: 父 agent thread ID
            webhook: 终态 webhook 配置
            delegation_context: 跨网关委托上下文

        Returns:
            新创建的任务记录
        """
        # 为什么先创建 task record：异步任务一旦被调度，就需要立即进入可追踪状态。
        record = TaskRecord(
            task_id=task_id,
            agent_name=agent_name,
            state="pending",
            thread_id=upstream_task_id or task_id,
            parent_task_id=parent_task_id,
            root_task_id=root_task_id,
            depth=depth,
            created_at=_now(),
            updated_at=_now(),
            route_kind=route_kind,
            upstream_task_id=upstream_task_id,
            parent_thread_id=parent_thread_id,
            webhook=webhook,
            delegation_root_id=(
                delegation_context.root_id if delegation_context is not None else None
            ),
            delegation_max_depth=(
                delegation_context.max_depth if delegation_context is not None else None
            ),
            delegation_max_tasks_per_root=(
                delegation_context.max_tasks_per_root
                if delegation_context is not None
                else None
            ),
            delegation_visited_nodes=(
                delegation_context.visited_nodes
                if delegation_context is not None
                else ()
            ),
            permission_profile=permission_profile,
            effective_skill_names=tuple(effective_skill_names),
            skill_view_path=skill_view_path,
            skill_view_hash=skill_view_hash,
        )
        self._tasks[task_id] = record
        self._save(record)
        return record

    def get_task(self, task_id: str) -> TaskRecord:
        """
        获取任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            对应的任务记录

        Raises:
            UnknownWorkerTaskError: task_id 不存在
        """
        # 为什么集中读取 task：所有状态读写都应经过一个统一入口，避免分散状态判断。
        try:
            return self._tasks[task_id]
        except KeyError:
            loaded = self.load_task_by_id(task_id)
            if loaded is not None:
                return loaded
            raise UnknownWorkerTaskError(f"Unknown worker task: {task_id}")

    def list_tasks(self) -> list[TaskRecord]:
        """
        列出所有任务记录

        Returns:
            当前 runtime 已追踪的任务记录列表
        """
        # 为什么暴露任务列表：主 agent 和 UI 都需要知道当前 runtime 中有哪些活动任务。
        return list(self._tasks.values())

    def find_by_review_id(self, review_id: str) -> TaskRecord | None:
        mirrored_match: TaskRecord | None = None
        for record in self._tasks.values():
            pending_review = record.pending_review or {}
            if pending_review.get("review_id") == review_id:
                if record.state == "waiting_for_human":
                    return record
                if mirrored_match is None:
                    mirrored_match = record
        if self._store is None:
            return mirrored_match
        for stored in self._store.list_tasks():
            pending_review = stored.pending_review or {}
            if pending_review.get("review_id") == review_id:
                record = task_record_for_restart(stored)
                self._tasks[record.task_id] = record
                if record.state == "waiting_for_human":
                    return record
                if mirrored_match is None:
                    mirrored_match = record
        return mirrored_match

    def find_by_thread_id(self, thread_id: str) -> TaskRecord | None:
        """
        通过 thread_id 查找任务

        Args:
            thread_id: agent 会话 thread ID

        Returns:
            匹配的任务记录；找不到则返回 None
        """
        # 为什么提供 thread_id 反查：部分工具调用路径可能只保留 thread_id，
        # 这里作为 task_id 上下文缺失时的防御性兜底。
        for record in self._tasks.values():
            if record.thread_id == thread_id:
                return record
        return None

    def count_tasks_under_root(self, root_task_id: str) -> int:
        """
        统计某棵委托树下的任务数量

        Args:
            root_task_id: 委托树根任务 ID

        Returns:
            root_task_id 相同的任务数量
        """
        return sum(
            1 for record in self._tasks.values() if record.root_task_id == root_task_id
        )

    def mark_running(self, task_id: str, run_task: asyncio.Task[None]) -> None:
        """
        标记任务进入 running 状态

        Args:
            task_id: 当前 runtime 内部任务 ID
            run_task: 本地执行的 asyncio task
        """
        # 为什么单独标记 running：异步任务真正开始执行的时点需要被明确记录。
        record = self.get_task(task_id)
        record.state = "running"
        record.updated_at = _now()
        record.active_run = run_task
        record.run_count += 1
        record.mailbox_suppressed = False
        record.mailbox_delivered = False
        record.cancel_requested = False
        self._clear_mirrored_pending_review_from_root(record)
        record.pending_review = None
        record.error = None
        self._save(record)

    def mark_waiting_for_human(
        self,
        task_id: str,
        pending_review: dict[str, Any],
    ) -> None:
        """标记本地 worker 暂停在人工审批点。"""
        record = self.get_task(task_id)
        pending_review = dict(pending_review)
        review_id = pending_review.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            pending_review["review_id"] = str(uuid.uuid4())
        record.state = "waiting_for_human"
        record.updated_at = _now()
        record.active_run = None
        record.pending_review = pending_review
        record.error = None
        self._save(record)
        self._mirror_pending_review_to_root(record)

    def mark_completed(self, task_id: str, result: str) -> None:
        """
        标记任务成功完成

        Args:
            task_id: 当前 runtime 内部任务 ID
            result: 最终结果摘要
        """
        # 为什么单独标记 completed：wait/check 依赖一个稳定的终态和最终文本结果。
        record = self.get_task(task_id)
        record.state = "completed"
        record.result = result
        record.error = None
        record.updated_at = _now()
        record.active_run = None
        self._clear_mirrored_pending_review_from_root(record)
        record.pending_review = None
        self._save(record)

    def mark_failed(self, task_id: str, error: str) -> None:
        """
        标记任务失败

        Args:
            task_id: 当前 runtime 内部任务 ID
            error: 失败错误摘要
        """
        # 为什么单独标记 failed：失败应保留为结构化状态，而不是只在日志中消失。
        record = self.get_task(task_id)
        record.state = "failed"
        record.error = error
        record.updated_at = _now()
        record.active_run = None
        self._clear_mirrored_pending_review_from_root(record)
        record.pending_review = None
        self._save(record)

    def mark_cancelled(self, task_id: str) -> None:
        """
        标记任务取消

        Args:
            task_id: 当前 runtime 内部任务 ID
        """
        # 为什么单独标记 cancelled：取消是有业务语义的终态，不应和失败混在一起。
        record = self.get_task(task_id)
        record.state = "cancelled"
        record.updated_at = _now()
        record.active_run = None
        record.cancel_requested = False
        self._clear_mirrored_pending_review_from_root(record)
        record.pending_review = None
        record.error = None
        self._save(record)

    def mark_interrupted(self, task_id: str, error: str) -> None:
        """
        标记任务被运行时中断

        这个状态用于进程退出、事件循环关闭或其它非业务取消场景。它区别于
        用户显式 cancel_agent/cancel_task 产生的 cancelled。
        """
        record = self.get_task(task_id)
        record.state = "interrupted"
        record.error = error
        record.updated_at = _now()
        record.active_run = None
        record.cancel_requested = False
        self._clear_mirrored_pending_review_from_root(record)
        record.pending_review = None
        self._save(record)

    def sync_remote_task(self, task_id: str, payload: dict[str, Any]) -> TaskRecord:
        """
        将远端任务 payload 同步到本地任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID
            payload: 远端网关返回的任务状态 payload

        Returns:
            更新后的任务记录

        Raises:
            ValueError: 远端 payload 缺少必要字段或类型不合法
        """
        # 为什么集中同步远端状态：CLI 和 wait/check/send_input/cancel 都需要一致地映射远端任务视图。
        record = self.get_task(task_id)
        status = payload.get("status")
        if not isinstance(status, str) or not status:
            raise ValueError(f"Remote task '{task_id}' returned invalid status")

        run_count = payload.get("run_count")
        if not isinstance(run_count, int):
            raise ValueError(f"Remote task '{task_id}' returned invalid run_count")

        last_result = payload.get("last_result")
        error = payload.get("error")
        pending_review = payload.get("pending_review")
        record.state = status
        record.result = last_result if isinstance(last_result, str) else None
        record.error = (
            error
            if status in {"failed", "interrupted"} and isinstance(error, str)
            else None
        )
        record.pending_review = (
            pending_review if isinstance(pending_review, dict) else None
        )
        record.run_count = run_count
        record.created_at = _parse_task_timestamp(
            payload.get("created_at"),
            fallback=record.created_at,
        )
        record.updated_at = _parse_task_timestamp(
            payload.get("updated_at"),
            fallback=record.updated_at,
        )
        record.active_run = None
        if record.state == "waiting_for_human" and record.pending_review is not None:
            self._mirror_pending_review_to_root(record)
        else:
            self._clear_mirrored_pending_review_from_root(record)
        self._save(record)
        return record

    def find_by_upstream_task_id(self, upstream_task_id: str) -> TaskRecord | None:
        """
        通过远端 upstream_task_id 查找本地任务

        Args:
            upstream_task_id: 远端网关返回的任务 ID

        Returns:
            匹配的本地任务记录；找不到则返回 None
        """
        for record in self._tasks.values():
            if record.upstream_task_id == upstream_task_id:
                return record
        return None


class AgentControl:
    """
    异步 agent 委托控制器

    这是 runtime 的核心协调层。它连接 agent 注册表、任务状态管理器、本地
    worker 执行器、远端 A2A 客户端、mailbox、webhook 和 LangChain 工具。

    主要功能：
    - 创建并调度本地 worker 任务
    - 创建并同步远端 remote_ref 任务
    - 控制任务树深度和单树任务预算
    - 为不同 agent 构造带作用域的委托工具
    - 向 Gateway 和工具层提供结构化任务操作接口

    设计要点：
    - 本地和远端任务都用 TaskRecord 表示，降低上层分支复杂度
    - 对模型暴露文本工具返回，对 Gateway 暴露结构化对象返回
    - 每棵委托树有独立 asyncio.Lock，防止并发 spawn 绕过预算限制

    Attributes:
        _registry: agent 目标注册表
        _task_manager: 任务状态管理器
        _compiled_agents: 已编译本地 worker agent 缓存
        _a2a_client: 远端网关 A2A 客户端
        _mailbox: 进程内终态消息通道
        _root_budget_locks: 按 root_task_id 分组的预算锁
    """

    def __init__(
        self,
        specs: dict[str, LocalWorkerSpec],
        remote_refs: dict[str, RemoteRef] | None = None,
        *,
        checkpointer: Any,
        backend: Any,
        a2a_client: A2AClient | None = None,
        mailbox: AgentMailbox | None = None,
        webhook_url: str | None = None,
        webhook_token: str | None = None,
        remote_poll_interval: float = 0.5,
        remote_status_retry_attempts: int = 3,
        max_delegation_depth: int = 3,
        max_tasks_per_root: int = 20,
        node_id: str | None = None,
        task_store: TaskStore | None = None,
        permission_default_profile: str = "",
        permission_policy: PermissionPolicy | None = None,
        backend_kind: str = "unknown",
        workspace_root: str = "",
        review_audit_store: ReviewAuditStore | None = None,
        skill_catalog: Mapping[str, SkillEntry] | None = None,
        skill_syncer: SkillSyncer | None = None,
    ) -> None:
        """
        初始化委托控制器

        Args:
            specs: 本地 worker 配置表
            remote_refs: 远端 agent 引用配置表
            checkpointer: LangGraph checkpointer
            backend: runtime 后端依赖
            a2a_client: 自定义 A2A 客户端（测试或特殊传输层使用）
            mailbox: 进程内 mailbox，用于向父 thread 投递终态消息
            webhook_url: 当前节点接收远端 webhook 的 URL
            webhook_token: webhook Bearer Token
            remote_poll_interval: 轮询远端任务状态的间隔（秒）
            remote_status_retry_attempts: 查询远端状态的重试次数
            max_delegation_depth: 默认最大委托深度
            max_tasks_per_root: 默认单棵委托树最大任务数
            node_id: 当前网关节点 ID
            task_store: 任务控制面持久化存储
            permission_default_profile: 未显式配置时使用的权限 profile
            permission_policy: 工具审批策略
            backend_kind: 当前 runtime 后端类型
            workspace_root: 当前 runtime 工作目录
            review_audit_store: 审批与权限审计日志

        Raises:
            ValueError: 委托深度或任务预算配置不合法
        """
        # 为什么单独有 control：main agent 只需要统一调度接口，不需要知道本地 worker 的内部细节。
        if max_delegation_depth < 1:
            raise ValueError("max_delegation_depth must be at least 1")
        if max_tasks_per_root < 1:
            raise ValueError("max_tasks_per_root must be at least 1")
        self._registry = AgentRegistry(specs, remote_refs)
        self._task_manager = TaskManager(task_store)
        self._checkpointer = checkpointer
        self._backend = backend
        self._compiled_agents: dict[str, Any] = {}
        self._a2a_client = a2a_client or A2AClient()
        self._mailbox = mailbox
        self._webhook_url = webhook_url
        self._webhook_token = webhook_token
        self._remote_poll_interval = remote_poll_interval
        self._remote_status_retry_attempts = max(remote_status_retry_attempts, 1)
        self._max_delegation_depth = max_delegation_depth
        self._max_tasks_per_root = max_tasks_per_root
        self._node_id = validate_node_id(node_id or f"node-{uuid.uuid4()}")
        self._root_budget_locks: dict[str, asyncio.Lock] = {}
        self._permission_default_profile = permission_default_profile
        self._permission_policy = permission_policy
        self._backend_kind = backend_kind
        self._workspace_root = workspace_root
        self._review_audit_store = review_audit_store
        self._skill_catalog = skill_catalog or {}
        self._skill_syncer = skill_syncer

    @property
    def workspace_root(self) -> str:
        return self._workspace_root

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[Any]:
        return self._backend.upload_files(files)

    def download_files(self, paths: list[str]) -> list[Any]:
        return self._backend.download_files(paths)

    def _resolve_task_skill_view(
        self,
        entry: RegisteredAgent,
        *,
        parent_task_id: str | None,
    ) -> tuple[tuple[str, ...], str | None, str | None]:
        if not isinstance(entry, LocalWorkerEntry) or self._skill_syncer is None:
            return (), None, None
        parent_skill_names: tuple[str, ...] | None = None
        if parent_task_id is not None:
            parent_record = self._task_manager.get_task(parent_task_id)
            parent_skill_names = parent_record.effective_skill_names
        skill_names = resolve_skill_names(
            entry.spec.skills,
            self._skill_catalog,
            parent_skill_names=parent_skill_names,
        )
        view = self._skill_syncer.ensure_view(self._skill_catalog, skill_names)
        if view is None:
            return skill_names, None, None
        return skill_names, view.path, view.view_hash

    def _get_or_create_agent(self, agent_name: str) -> Any:
        """
        获取或编译本地 worker agent

        Args:
            agent_name: 本地 worker 名称

        Returns:
            可执行的 runtime agent 实例

        Raises:
            UnknownAgentTargetError: agent_name 未注册
            ValueError: agent_name 指向远端引用，不能本地编译
        """
        # 为什么缓存已编译 agent：同一个 async worker 多轮继续执行时应复用会话定义，而不是反复重建。
        agent = self._compiled_agents.get(agent_name)
        if agent is not None:
            return agent

        spec = self._registry.get_spec(agent_name)
        # worker 自己内部也走新的 runtime 包装器，
        # 这样整个系统里不会混入 deepagents 默认 task/general-purpose。
        agent = create_runtime_agent(
            model=spec.model,
            system_prompt=spec.system_prompt,
            tools=spec.tools,
            local_worker_specs=spec.delegation_local_worker_specs,
            remote_refs=spec.delegation_remote_refs,
            build_worker_tools=spec.build_delegation_tools,
            memory=spec.memory,
            skills=spec.skills,
            backend=self._backend,
            checkpointer=self._checkpointer,
            mailbox=self._mailbox,
            load_tasks_for_thread=self.load_tasks_for_thread,
            permission_policy=self._permission_policy,
            backend_kind=self._backend_kind,
            workspace_root=self._workspace_root,
            permission_profile=(
                spec.permission_profile or self._permission_default_profile
            ),
            review_audit_store=self._review_audit_store,
            tool_search_registry=spec.tool_search_registry if spec.tool_search else None,
            tool_search_server_names=spec.tool_search_server_names,
            tool_search_tool_names=spec.tool_search_tool_names,
            name=agent_name,
        )
        self._compiled_agents[agent_name] = agent  # agent 对象被复用（缓存）
        return agent

    def _build_agent_run_config(self, record: TaskRecord) -> dict[str, Any]:
        return {
            "configurable": {
                "thread_id": record.thread_id,
                "task_id": record.task_id,
                "parent_task_id": record.parent_task_id,
                "root_task_id": record.root_task_id,
                "delegation_depth": record.depth,
                "agent_name": record.agent_name,
                "permission_profile": record.permission_profile,
                "effective_skill_names": list(record.effective_skill_names),
                "skill_view_path": record.skill_view_path,
                "skill_view_hash": record.skill_view_hash,
            }
        }

    def _audit_task_review(
        self,
        event_type: str,
        record: TaskRecord,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._review_audit_store is None:
            return
        review_id = None
        if payload is not None:
            raw_review_id = payload.get("review_id")
            if isinstance(raw_review_id, str):
                review_id = raw_review_id
        self._review_audit_store.append(
            event_type,
            source=record.route_kind,
            review_id=review_id,
            task_id=record.task_id,
            thread_id=record.thread_id,
            agent_name=record.agent_name,
            profile_name=record.permission_profile,
            backend_kind=self._backend_kind,
            workspace_root=self._workspace_root,
            payload=payload,
        )

    async def _run_agent_payload(self, task_id: str, payload: Any) -> None:
        """
        执行本地 worker 的一轮输入

        Args:
            task_id: 当前 runtime 内部任务 ID
            payload: 本轮传给 worker 的 graph 输入或 resume command
        """
        # 为什么把单轮执行封装出来：本地 async worker 的实际运行逻辑需要和调度控制解耦。
        record = self._task_manager.get_task(task_id)
        agent = self._get_or_create_agent(record.agent_name)
        run_config = self._build_agent_run_config(record)
        try:
            result = await agent.ainvoke(
                payload,
                config=run_config,
                version="v2",
            )
        except asyncio.CancelledError as exc:
            latest = self._task_manager.get_task(task_id)
            if latest.cancel_requested:
                self._task_manager.mark_cancelled(task_id)
            else:
                self._task_manager.mark_interrupted(
                    task_id,
                    _format_interrupted_error(exc),
                )
            self._maybe_publish_terminal_message(task_id)
            await self._send_terminal_webhook(task_id)  # 如果配置了webhook 就会发送
            raise
        except Exception as exc:
            self._task_manager.mark_failed(task_id, _format_exception_summary(exc))
            self._maybe_publish_terminal_message(task_id)
            await self._send_terminal_webhook(task_id)
            return

        outcome = await normalize_agent_turn(agent, run_config, result)
        if outcome.review_payloads:
            if len(outcome.review_payloads) > 1:
                self._task_manager.mark_failed(
                    task_id,
                    "Worker produced multiple simultaneous human review requests.",
                )
                self._maybe_publish_terminal_message(task_id)
                await self._send_terminal_webhook(task_id)
                return
            self._task_manager.mark_waiting_for_human(
                task_id,
                outcome.review_payloads[0],
            )
            self._audit_task_review(
                "task_waiting_for_human",
                self._task_manager.get_task(task_id),
                payload=outcome.review_payloads[0],
            )
            return

        if outcome.has_unresolved_tool_calls:
            self._task_manager.mark_failed(
                task_id,
                "Worker stopped before resolving pending tool calls.",
            )
            self._maybe_publish_terminal_message(task_id)
            await self._send_terminal_webhook(task_id)
            return

        result_text = (
            outcome.content
            or "Task completed, but the final assistant reply was empty."
        )
        self._task_manager.mark_completed(task_id, result_text)
        self._maybe_publish_terminal_message(task_id)
        await self._send_terminal_webhook(task_id)

    async def _run_agent_turn(self, task_id: str, user_input: str) -> None:
        await self._run_agent_payload(
            task_id,
            {"messages": [{"role": "user", "content": user_input}]},
        )

    def _start_run(self, task_id: str, user_input: str) -> None:
        """
        启动本地任务的一轮异步执行

        Args:
            task_id: 当前 runtime 内部任务 ID
            user_input: 本轮传给 worker 的输入

        Raises:
            TaskAlreadyRunningError: 该任务已有未结束的活跃 run
        """
        # 为什么单独启动 run：send_input 和首次 spawn 都需要走同一套任务启动约束。
        record = self._task_manager.get_task(task_id)
        if record.active_run is not None and not record.active_run.done():
            raise TaskAlreadyRunningError(f"Worker task is already running: {task_id}")
        run_task = asyncio.create_task(self._run_agent_turn(task_id, user_input))
        self._task_manager.mark_running(task_id, run_task)

    def _resume_run(self, task_id: str, decisions: list[dict[str, Any]]) -> None:
        record = self._task_manager.get_task(task_id)
        if record.active_run is not None and not record.active_run.done():
            raise TaskAlreadyRunningError(f"Worker task is already running: {task_id}")
        run_task = asyncio.create_task(
            self._run_agent_payload(
                task_id,
                Command(resume={"decisions": decisions}),
            )
        )
        self._audit_task_review(
            "task_review_resumed",
            record,
            payload={
                "review_id": (
                    record.pending_review or {}
                ).get("review_id"),
                "decisions": decisions,
            },
        )
        self._task_manager.mark_running(task_id, run_task)

    def _format_task_record(self, record: TaskRecord) -> str:
        """
        格式化任务记录为工具返回文本

        Args:
            record: 任务记录

        Returns:
            单行可读任务状态
        """
        # waiting_for_human is a control-plane pause, not useful model context.
        # Keep it internal and expose it as running so callers do not retry or
        # re-delegate while a user is reviewing the blocked tool call.
        model_state = (
            "running" if record.state == "waiting_for_human" else record.state
        )
        parts = [
            f"task_id={record.task_id}",
            f"agent={record.agent_name}",
            f"route={record.route_kind}",
            f"state={model_state}",
            f"depth={record.depth}",
            f"runs={record.run_count}",
        ]
        if record.parent_task_id:
            parts.append(f"parent={record.parent_task_id}")
        if record.root_task_id != record.task_id:
            parts.append(f"root={record.root_task_id}")
        if record.state == "completed" and record.result:
            parts.append(f"result={record.result}")
        if record.state in {"failed", "interrupted"} and record.error:
            parts.append(f"error={record.error}")
        return " | ".join(parts)

    async def _resolve_pending_reviews_from_config(
        self,
        config: RunnableConfig | None,
    ) -> bool:
        """Run an optional UI/control-plane review resolver while wait_agent blocks."""
        if not config:
            return False
        configurable = config.get("configurable") or {}
        resolver = configurable.get("resolve_pending_reviews")
        if not callable(resolver):
            return False
        try:
            result = resolver()
            if inspect.isawaitable(result):
                result = await result
        except Exception:
            logger.exception("Pending review resolver failed during wait_agent.")
            return False
        return bool(result)

    def _get_remote_entry_for_task(self, task_id: str) -> RemoteRefEntry:
        """
        获取远端任务对应的 remote_ref 注册项

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            远端引用注册项

        Raises:
            ValueError: task_id 对应的任务不是 remote_ref
        """
        record = self._task_manager.get_task(task_id)
        entry = self._registry.get_entry(record.agent_name)
        if not isinstance(entry, RemoteRefEntry):
            raise ValueError(f"Task '{task_id}' is not a remote_ref task")
        return entry

    async def _refresh_remote_task(self, task_id: str) -> TaskRecord:
        """
        查询一次远端任务状态并同步本地记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            同步后的任务记录

        Raises:
            A2AClientError: 远端请求失败
            ValueError: 远端 payload 不合法
        """
        entry = self._get_remote_entry_for_task(task_id)
        record = self._task_manager.get_task(task_id)
        upstream_task_id = record.upstream_task_id or task_id
        payload = await self._a2a_client.get_task(entry.ref, task_id=upstream_task_id)
        return self._task_manager.sync_remote_task(task_id, payload)

    async def _refresh_remote_task_with_retries(self, task_id: str) -> TaskRecord:
        """
        带重试地刷新远端任务状态

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            同步后的任务记录

        Raises:
            A2AClientError: 多次重试后仍无法获取远端状态
        """
        last_exc: A2AClientError | None = None
        for attempt in range(self._remote_status_retry_attempts):
            try:
                return await self._refresh_remote_task(task_id)
            except A2AClientError as exc:
                last_exc = exc
                if attempt + 1 >= self._remote_status_retry_attempts:
                    raise
                await asyncio.sleep(self._remote_poll_interval)
        if last_exc is not None:
            raise last_exc
        raise AssertionError("unreachable")

    def _format_remote_status_unavailable(
        self,
        record: TaskRecord,
        exc: BaseException,
    ) -> str:
        """
        格式化远端状态暂不可用的工具返回文本

        Args:
            record: 本地最后已知任务记录
            exc: 最后一次远端查询异常

        Returns:
            包含最后已知状态和告警的文本
        """
        # 为什么保留最后已知状态：远端瞬时抖动不应让主 agent 误判为任务已经失败。
        return (
            f"{self._format_task_record(record)} | warning=remote_status_temporarily_unavailable "
            f"after {self._remote_status_retry_attempts} attempts: {exc}"
        )

    def _extract_parent_thread_id(
        self,
        config: RunnableConfig | None,
    ) -> str | None:
        """
        从工具调用 config 中提取父 thread ID

        Args:
            config: LangChain RunnableConfig

        Returns:
            父 thread ID；不存在或类型不合法时返回 None
        """
        if not config:
            return None
        thread_id = (config.get("configurable") or {}).get("thread_id")
        return thread_id if isinstance(thread_id, str) and thread_id else None

    def _extract_parent_task_record(
        self,
        config: RunnableConfig | None,
    ) -> TaskRecord | None:
        """
        从工具调用 config 中解析父任务记录

        Args:
            config: LangChain RunnableConfig

        Returns:
            父任务记录；无法解析时返回 None

        Raises:
            UnknownWorkerTaskError: config 中 task_id 存在但无法映射到任务
        """
        if not config:
            return None
        configurable = config.get("configurable") or {}
        task_id = configurable.get("task_id")
        thread_id = configurable.get("thread_id")
        if isinstance(task_id, str) and task_id:
            try:
                return self._task_manager.get_task(task_id)
            except UnknownWorkerTaskError:
                if isinstance(thread_id, str) and thread_id:
                    fallback = self._task_manager.find_by_thread_id(thread_id)
                    if fallback is not None:
                        logger.warning(
                            "spawn_agent could not resolve task_id=%s and "
                            "fell back to thread_id=%s",
                            task_id,
                            thread_id,
                        )
                        return fallback
                raise
        if isinstance(thread_id, str) and thread_id:
            fallback = self._task_manager.find_by_thread_id(thread_id)
            if fallback is not None:
                logger.warning(
                    "spawn_agent called without task_id in configurable; "
                    "falling back to thread_id=%s",
                    thread_id,
                )
            return fallback
        return None

    def _resolve_task_tree_context(
        self,
        *,
        task_id: str,
        parent_task_id: str | None,
        delegation_context: DelegationContext | None,
    ) -> tuple[str, int, DelegationContext]:
        """
        解析新任务所在的委托树上下文

        根任务会创建新的 DelegationContext；子任务会基于父任务上下文增加
        depth；跨网关入站任务会沿用 inbound metadata 中的上下文。

        Args:
            task_id: 即将创建的任务 ID
            parent_task_id: 父任务 ID
            delegation_context: 入站或显式传入的委托上下文

        Returns:
            root_task_id、depth 和最终 DelegationContext

        Raises:
            ValueError: 同时传入 parent_task_id 和 delegation_context
        """
        if parent_task_id is None:
            if delegation_context is not None:
                return (
                    delegation_context.root_id,
                    delegation_context.depth,
                    delegation_context,
                )
            root_context = build_root_context(
                node_id=self._node_id,
                task_id=task_id,
                max_depth=self._max_delegation_depth,
                max_tasks_per_root=self._max_tasks_per_root,
            )
            return task_id, 1, root_context
        if delegation_context is not None:
            raise ValueError(
                "delegation_context cannot be combined with parent_task_id"
            )
        parent = self._task_manager.get_task(parent_task_id)
        depth = parent.depth + 1
        return (
            parent.root_task_id,
            depth,
            build_child_context(
                self._delegation_context_from_record(parent), depth=depth
            ),
        )

    def _delegation_context_from_record(self, record: TaskRecord) -> DelegationContext:
        """
        从任务记录恢复委托上下文

        Args:
            record: 任务记录

        Returns:
            可继续向下游传递的 DelegationContext
        """
        if (
            record.delegation_root_id is not None
            and record.delegation_max_depth is not None
            and record.delegation_max_tasks_per_root is not None
            and record.delegation_visited_nodes
        ):
            return DelegationContext(
                root_id=record.delegation_root_id,
                depth=record.depth,
                max_depth=record.delegation_max_depth,
                max_tasks_per_root=record.delegation_max_tasks_per_root,
                visited_nodes=record.delegation_visited_nodes,
            )
        return build_root_context(
            node_id=self._node_id,
            task_id=record.root_task_id,
            max_depth=self._max_delegation_depth,
            max_tasks_per_root=self._max_tasks_per_root,
        )

    def _resolve_permission_profile(
        self,
        *,
        entry: RegisteredAgent,
        parent_task_id: str | None,
    ) -> str:
        if isinstance(entry, LocalWorkerEntry) and entry.spec.permission_profile:
            return entry.spec.permission_profile
        if parent_task_id is not None:
            parent = self._task_manager.get_task(parent_task_id)
            if parent.permission_profile:
                return parent.permission_profile
        return self._permission_default_profile

    def _enforce_delegation_limits(
        self,
        *,
        root_task_id: str,
        depth: int,
        max_depth: int,
        max_tasks_per_root: int,
    ) -> None:
        """
        校验委托深度和单树任务预算

        Args:
            root_task_id: 委托树根任务 ID
            depth: 即将创建任务的深度
            max_depth: 允许的最大深度
            max_tasks_per_root: 单棵委托树最大任务数

        Raises:
            MaxDelegationDepthError: depth 超过 max_depth
            MaxTasksPerRootError: 当前委托树任务数达到上限
        """
        if depth > max_depth:
            raise MaxDelegationDepthError(
                current_depth=depth,
                max_depth=max_depth,
            )
        current_count = self._task_manager.count_tasks_under_root(root_task_id)
        if current_count >= max_tasks_per_root:
            raise MaxTasksPerRootError(
                root_task_id=root_task_id,
                current_count=current_count,
                max_tasks_per_root=max_tasks_per_root,
            )

    def _get_root_budget_lock(self, root_task_id: str) -> asyncio.Lock:
        """
        获取某棵委托树的预算锁

        Args:
            root_task_id: 委托树根任务 ID

        Returns:
            与 root_task_id 绑定的 asyncio.Lock
        """
        lock = self._root_budget_locks.get(root_task_id)
        if lock is None:
            lock = asyncio.Lock()
            self._root_budget_locks[root_task_id] = lock
        return lock

    def _format_depth_limit_error(self, exc: MaxDelegationDepthError) -> str:
        """
        格式化深度限制错误为模型可读文本

        Args:
            exc: 深度限制异常

        Returns:
            面向 agent 的错误说明
        """
        return (
            "Delegation depth limit exceeded: "
            f"current_depth={exc.current_depth} max_depth={exc.max_depth}. "
            "You have reached the maximum delegation depth. Complete the "
            "remaining work yourself, or return partial results to your caller. "
            "Do not attempt to spawn more agents."
        )

    def _format_task_budget_error(self, exc: MaxTasksPerRootError) -> str:
        """
        格式化任务预算错误为模型可读文本

        Args:
            exc: 任务预算异常

        Returns:
            面向 agent 的错误说明
        """
        return (
            "Task budget exhausted: "
            f"root_task_id={exc.root_task_id} current_count={exc.current_count} "
            f"max_tasks_per_root={exc.max_tasks_per_root}. "
            "This delegation tree has reached its task limit. Complete the "
            "remaining work yourself, or return partial results to your caller."
        )

    def _build_webhook_config(self) -> dict[str, Any] | None:
        """
        构造传给远端网关的 webhook 配置

        Returns:
            webhook 配置；未配置 webhook_url 时返回 None
        """
        if not self._webhook_url:
            return None
        webhook: dict[str, Any] = {"url": self._webhook_url}
        if self._webhook_token:
            webhook["token"] = self._webhook_token
        return webhook

    def _is_terminal_record(self, record: TaskRecord) -> bool:
        """
        判断任务是否处于终态

        Args:
            record: 任务记录

        Returns:
            completed、failed 或 cancelled 返回 True
        """
        return record.state in TERMINAL_TASK_STATES

    def _maybe_publish_terminal_message(self, task_id: str) -> None:
        """
        尝试向父 thread 发布终态 mailbox 消息

        只有配置了 mailbox、任务有 parent_thread_id、未被 suppress、未投递过且
        已进入终态时才会发布。

        Args:
            task_id: 当前 runtime 内部任务 ID
        """
        # 如果任务终止，发布消息到 mailbox
        record = self._task_manager.get_task(task_id)
        if (
            self._mailbox is None
            or record.parent_thread_id is None
            or record.mailbox_suppressed
            or record.mailbox_delivered
            or not self._is_terminal_record(record)
        ):
            return
        status = record.state
        if status not in TERMINAL_TASK_STATES:
            return
        content = record.result or record.error or f"Task ended with state={status}"
        # content 支持 执行结果 已知错误 或者未知状态
        published = self._mailbox.publish_terminal(
            recipient_thread_id=record.parent_thread_id,
            child_task_id=record.task_id,
            child_agent_name=record.agent_name,
            status=status,  # type: ignore[arg-type]
            content=content,
        )
        if published is not None:
            record.mailbox_delivered = True

    def _suppress_mailbox_delivery(self, record: TaskRecord) -> None:
        """
        禁止或撤回任务的 mailbox 终态投递

        当调用方已经通过 wait/check 主动获取结果时，后续不应再把同一个结果
        作为 mailbox 消息注入父 agent。

        Args:
            record: 需要抑制 mailbox 投递的任务记录
        """
        record.mailbox_suppressed = True
        if self._mailbox is not None and record.parent_thread_id is not None:
            self._mailbox.retract(
                recipient_thread_id=record.parent_thread_id,
                child_task_id=record.task_id,
            )

    async def _send_terminal_webhook(self, task_id: str) -> None:
        """
        发送任务终态 webhook

        Args:
            task_id: 当前 runtime 内部任务 ID
        """
        record = self._task_manager.get_task(task_id)
        if not record.webhook or not self._is_terminal_record(record):
            return
        url = record.webhook.get("url")
        if not isinstance(url, str) or not url:
            return
        headers = {"Content-Type": "application/json"}
        token = record.webhook.get("token")
        if isinstance(token, str) and token:
            headers["Authorization"] = f"Bearer {token}"
        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": f"task.{record.state}",
            "task_id": record.task_id,
            "agent_name": record.agent_name,
            "status": record.state,
            "last_result": record.result,
            "error": record.error,
            "run_count": record.run_count,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                await client.post(url, headers=headers, json=payload)
        except httpx.HTTPError:
            return

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool:
        """
        处理远端任务 webhook 事件

        Args:
            payload: 远端网关推送的任务状态 payload

        Returns:
            找到并同步到本地任务返回 True；无法识别事件返回 False
        """
        upstream_task_id = payload.get("task_id")
        if not isinstance(upstream_task_id, str) or not upstream_task_id:
            return False
        record = self._task_manager.find_by_upstream_task_id(upstream_task_id)
        if record is None:
            return False
        synced = self._task_manager.sync_remote_task(record.task_id, payload)
        if self._is_terminal_record(synced):
            self._maybe_publish_terminal_message(synced.task_id)
            await self._send_terminal_webhook(synced.task_id)
        return True

    def _allowed_targets_for_agent(self, agent_name: str) -> set[str]:
        """
        计算某个本地 worker 可委托的目标集合

        Args:
            agent_name: 本地 worker 名称

        Returns:
            该 worker 配置中允许继续委托的本地 worker 和 remote_ref 名称集合
        """
        spec = self._registry.get_spec(agent_name)
        allowed_targets: set[str] = set()
        if spec.delegation_local_worker_specs:
            allowed_targets.update(spec.delegation_local_worker_specs)
        if spec.delegation_remote_refs:
            allowed_targets.update(spec.delegation_remote_refs)
        return allowed_targets

    def build_tools_for(self, agent_name: str) -> list[StructuredTool]:
        """
        为指定 worker 构造带作用域的委托工具

        Args:
            agent_name: 本地 worker 名称

        Returns:
            该 worker 可使用的 StructuredTool 列表
        """
        return self._build_tools(
            allowed_targets=self._allowed_targets_for_agent(agent_name),
            caller_agent_name=agent_name,
        )

    def build_tools(self) -> list[StructuredTool]:
        """
        构造主 agent 使用的全量委托工具

        Returns:
            不限制目标范围的 StructuredTool 列表
        """
        return self._build_tools()

    def _build_tools(
        self,
        *,
        allowed_targets: set[str] | None = None,
        caller_agent_name: str | None = None,
    ) -> list[StructuredTool]:
        """
        构造委托工具集合

        Args:
            allowed_targets: 当前调用方允许访问的 agent 目标集合
            caller_agent_name: 当前调用方 agent 名称，用于错误提示

        Returns:
            spawn/wait/check/send_input/cancel/list 工具列表
        """
        # 为什么显式构造工具：需要把可用 worker 列表直接写进工具描述，而不是只依赖函数签名。
        spawn_description = SPAWN_AGENT_TOOL_DESCRIPTION.format(
            available_local_workers=self._registry.render_local_worker_descriptions(
                allowed_targets
            ),
            available_remote_refs=self._registry.render_remote_ref_descriptions(
                allowed_targets
            ),
        )

        def task_is_visible_to_caller(
            record: TaskRecord,
            config: RunnableConfig | None,
        ) -> bool:
            """
            判断任务是否对当前工具调用方可见

            worker 的委托工具只能操作自己创建或同一父 thread 下的任务，避免
            子 agent 越权查看其他分支的 task。
            """
            if allowed_targets is None:
                return True
            if record.agent_name not in allowed_targets:
                return False
            if not config:
                return False
            try:
                parent_record = self._extract_parent_task_record(config)
            except UnknownWorkerTaskError:
                parent_record = None
            if parent_record is not None:
                return record.parent_task_id == parent_record.task_id
            parent_thread_id = self._extract_parent_thread_id(config)
            if parent_thread_id is not None:
                return record.parent_thread_id == parent_thread_id
            return False

        def get_scoped_task(
            task_id: str,
            config: RunnableConfig | None,
        ) -> TaskRecord:
            """
            获取当前调用方可见的任务

            Raises:
                UnknownWorkerTaskError: 任务不存在或不在当前工具作用域内
            """
            parent_thread_id = self._extract_parent_thread_id(config)
            try:
                record = self._task_manager.get_task(task_id)
            except UnknownWorkerTaskError:
                if parent_thread_id is None:
                    raise
                loaded = self._task_manager.load_task_for_parent_thread(
                    task_id=task_id,
                    parent_thread_id=parent_thread_id,
                )
                if loaded is None:
                    raise
                record = loaded
            if not task_is_visible_to_caller(record, config):
                caller = caller_agent_name or "current agent"
                raise UnknownWorkerTaskError(
                    f"Worker task '{task_id}' is not visible to '{caller}'."
                )
            return record

        async def scoped_spawn_agent(
            agent_name: str,
            task: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带目标白名单校验的 spawn_agent 包装器"""
            if allowed_targets is not None and agent_name not in allowed_targets:
                available = ", ".join(self._registry.list_target_names(allowed_targets))
                caller = caller_agent_name or "current agent"
                return (
                    f"Agent target '{agent_name}' is not allowed for '{caller}'. "
                    f"Available: {available}"
                )
            return await self.spawn_agent(agent_name, task, config)

        async def scoped_wait_agent(
            task_id: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 wait_agent 包装器"""
            try:
                get_scoped_task(task_id, config)
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self.wait_agent(task_id, config)

        async def scoped_check_agent(
            task_id: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 check_agent 包装器"""
            try:
                get_scoped_task(task_id, config)
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self.check_agent(task_id, config)

        async def scoped_send_input(
            task_id: str,
            message: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 send_input 包装器"""
            try:
                get_scoped_task(task_id, config)
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self.send_input(task_id, message)

        async def scoped_cancel_agent(
            task_id: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 cancel_agent 包装器"""
            try:
                get_scoped_task(task_id, config)
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self.cancel_agent(task_id)

        async def scoped_list_agents(
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """列出当前调用方可见的 agent 目标和任务"""
            parent_thread_id = self._extract_parent_thread_id(config)
            if parent_thread_id is not None:
                self._task_manager.load_by_parent_thread_id(parent_thread_id)
            visible_task_ids = {
                record.task_id
                for record in self._task_manager.list_tasks()
                if task_is_visible_to_caller(record, config)
            }
            return self._format_agents_and_tasks(
                allowed_targets,
                visible_task_ids=visible_task_ids,
            )

        # 这里统一生成 StructuredTool，主 agent 看到的是稳定的 schema + description，
        # 而不是一组信息不足的裸方法。
        return [
            StructuredTool.from_function(
                coroutine=scoped_spawn_agent,
                name="spawn_agent",
                description=spawn_description,
                infer_schema=False,
                args_schema=SpawnAgentSchema,
            ),
            StructuredTool.from_function(
                coroutine=scoped_wait_agent,
                name="wait_agent",
                description=(
                    "Wait for a delegated task to reach a final state. If the "
                    "task is blocked on external review and no review handler can "
                    "resolve it, return the latest running state instead."
                ),
                infer_schema=False,
                args_schema=TaskIdSchema,
            ),
            StructuredTool.from_function(
                coroutine=scoped_check_agent,
                name="check_agent",
                description=(
                    "Check the current state of a delegated task without blocking."
                ),
                infer_schema=False,
                args_schema=TaskIdSchema,
            ),
            StructuredTool.from_function(
                coroutine=scoped_send_input,
                name="send_input",
                description=(
                    "Send follow-up instructions to an existing delegated task "
                    "after its current run has finished."
                ),
                infer_schema=False,
                args_schema=SendInputSchema,
            ),
            StructuredTool.from_function(
                coroutine=scoped_cancel_agent,
                name="cancel_agent",
                description="Cancel a delegated task that is no longer needed.",
                infer_schema=False,
                args_schema=TaskIdSchema,
            ),
            StructuredTool.from_function(
                coroutine=scoped_list_agents,
                name="list_agents",
                description="List registered agent targets and tracked worker tasks.",
                infer_schema=False,
                args_schema=ListAgentsSchema,
            ),
        ]

    def list_registered_agents_snapshot(self) -> list[RegisteredAgent]:
        """
        获取已注册 agent 的结构化快照

        Returns:
            当前 runtime 中全部已注册 agent 目标
        """
        # 为什么暴露结构化已登记目标：Gateway 需要机器可读的 agent 列表，而不是 prompt 文本。
        return self._registry.list_registered_agents()

    def load_tasks_for_thread(self, thread_id: str) -> None:
        """
        加载指定 agent thread 创建的任务

        runtime agent middleware 在每次 agent run 开始时调用该方法。它只恢复
        parent_thread_id 等于当前 thread_id 的任务，不做全库加载。
        """
        self._task_manager.load_by_parent_thread_id(thread_id)

    def get_registered_agent(self, agent_name: str) -> RegisteredAgent:
        """
        获取指定 agent 注册项

        Args:
            agent_name: agent 目标名称

        Returns:
            对应的注册项
        """
        return self._registry.get_entry(agent_name)

    def get_task_record(self, task_id: str) -> TaskRecord:
        """
        获取指定任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            对应的任务记录
        """
        return self._task_manager.get_task(task_id)

    def list_task_records(self) -> list[TaskRecord]:
        """
        列出当前 runtime 追踪的全部任务

        Returns:
            任务记录列表
        """
        return self._task_manager.list_tasks()

    def list_pending_review_records(self) -> list[TaskRecord]:
        return [
            record
            for record in self._task_manager.list_tasks()
            if record.state == "waiting_for_human" and record.pending_review is not None
        ]

    def get_task_by_review_id(self, review_id: str) -> TaskRecord:
        record = self._task_manager.find_by_review_id(review_id)
        if record is None:
            raise UnknownWorkerTaskError(f"Unknown pending review: {review_id}")
        return record

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
        *,
        wait: bool = False,
    ) -> TaskRecord:
        record = self.get_task_by_review_id(review_id)
        if record.route_kind == "remote_ref":
            entry = self._get_remote_entry_for_task(record.task_id)
            payload = await self._a2a_client.submit_review_decision(
                entry.ref,
                task_id=record.upstream_task_id or record.task_id,
                review_id=review_id,
                decisions=decisions,
            )
            return self._task_manager.sync_remote_task(record.task_id, payload)
        if record.route_kind != "local":
            raise ValueError(f"Unsupported review route={record.route_kind}")
        if record.state != "waiting_for_human":
            raise ValueError(
                f"Review '{review_id}' is not pending; task state={record.state}"
            )
        if record.pending_review is None:
            raise ValueError(f"Review '{review_id}' has no pending payload")
        self._resume_run(record.task_id, decisions)
        resumed = self._task_manager.get_task(record.task_id)
        if wait and resumed.active_run is not None:
            try:
                await resumed.active_run
            except asyncio.CancelledError:
                pass
        return self._task_manager.get_task(record.task_id)

    def prepare_delegation_metadata(
        self,
        metadata: dict[str, MetadataScalar],
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
        """
        解析入站委托 metadata

        Gateway 接收远端请求时调用该方法，把跨网关委托上下文从 metadata 中
        解析出来，并合并本地默认限制。

        Args:
            metadata: 入站请求 metadata

        Returns:
            清理后的 metadata 和可选 DelegationContext
        """
        return parse_inbound_metadata(
            metadata,
            node_id=self._node_id,
            local_max_depth=self._max_delegation_depth,
            local_max_tasks_per_root=self._max_tasks_per_root,
        )

    def ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None = None,
    ) -> TaskRecord:
        """
        确保远端代理任务在本地已有记录

        用于 webhook 或远端同步路径：如果本地还没有对应 task，则创建一个
        remote_ref TaskRecord；如果已有记录，则校验 agent 和 upstream 绑定。

        Args:
            agent_name: 远端引用名称
            task_id: 当前 runtime 内部任务 ID
            upstream_task_id: 远端网关上的任务 ID
            webhook: 终态 webhook 配置

        Returns:
            已存在或新创建的任务记录

        Raises:
            ValueError: agent 类型、任务路由或 upstream_task_id 不匹配
        """
        try:
            record = self._task_manager.get_task(task_id)
        except UnknownWorkerTaskError:
            entry = self._registry.get_entry(agent_name)
            if not isinstance(entry, RemoteRefEntry):
                raise ValueError(f"Agent target '{agent_name}' is not a remote_ref")
            self._registry.register_task(task_id, agent_name=agent_name)
            return self._task_manager.create_task_record(
                task_id,
                agent_name,
                parent_task_id=None,
                root_task_id=task_id,
                depth=1,
                route_kind="remote_ref",
                upstream_task_id=upstream_task_id,
                webhook=webhook,
                delegation_context=build_root_context(
                    node_id=self._node_id,
                    task_id=task_id,
                    max_depth=self._max_delegation_depth,
                    max_tasks_per_root=self._max_tasks_per_root,
                ),
                permission_profile=self._permission_default_profile,
            )
        if record.agent_name != agent_name:
            raise ValueError(
                f"Task '{task_id}' is registered for '{record.agent_name}', "
                f"not '{agent_name}'"
            )
        if record.route_kind != "remote_ref":
            raise ValueError(f"Task '{task_id}' is not a remote_ref task")
        if record.upstream_task_id != upstream_task_id:
            raise ValueError(
                f"Task '{task_id}' is linked to upstream task "
                f"'{record.upstream_task_id}', not '{upstream_task_id}'"
            )
        if record.webhook is None and webhook is not None:
            record.webhook = webhook
        return record

    async def refresh_task(self, task_id: str) -> TaskRecord:
        """
        刷新任务状态

        本地任务直接返回当前记录；远端任务会调用远端网关同步最新状态。

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            最新任务记录
        """
        record = self._task_manager.get_task(task_id)
        if record.route_kind == "remote_ref":
            return await self._refresh_remote_task_with_retries(task_id)
        return record

    async def spawn_task(
        self,
        agent_name: str,
        task: str,
        *,
        parent_task_id: str | None = None,
        parent_thread_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        webhook: dict[str, Any] | None = None,
        delegation_context: DelegationContext | None = None,
    ) -> TaskRecord:
        """
        创建结构化委托任务

        这是 Gateway 和工具层共享的核心 spawn 入口。本地 worker 会立即创建
        TaskRecord 并异步启动；远端 remote_ref 会先调用远端网关创建任务，再
        把远端 payload 同步成本地 TaskRecord。

        Args:
            agent_name: 要委托的 agent 目标名称
            task: 任务输入内容
            parent_task_id: 父任务 ID
            parent_thread_id: 父 agent thread ID，用于 mailbox 回投
            metadata: 传给远端任务的 metadata
            webhook: 当前调用方希望接收的终态 webhook
            delegation_context: 入站或显式委托上下文

        Returns:
            新创建或同步后的任务记录

        Raises:
            UnknownAgentTargetError: agent_name 未注册
            MaxDelegationDepthError: 超过最大委托深度
            MaxTasksPerRootError: 单棵委托树任务数达到上限
            A2AClientError: 远端创建任务失败
            ValueError: 远端返回 payload 不合法
        """
        # 为什么提供结构化 spawn：Gateway 需要稳定的对象结果，而不是面向模型的描述字符串。
        entry = self._registry.get_entry(agent_name)
        task_id = str(uuid.uuid4())
        permission_profile = self._resolve_permission_profile(
            entry=entry,
            parent_task_id=parent_task_id,
        )
        root_task_id, depth, task_context = self._resolve_task_tree_context(
            task_id=task_id,
            parent_task_id=parent_task_id,
            delegation_context=delegation_context,
        )
        effective_skill_names, skill_view_path, skill_view_hash = (
            self._resolve_task_skill_view(entry, parent_task_id=parent_task_id)
        )
        async with self._get_root_budget_lock(root_task_id):
            self._enforce_delegation_limits(
                root_task_id=root_task_id,
                depth=depth,
                max_depth=task_context.max_depth,
                max_tasks_per_root=task_context.max_tasks_per_root,
            )
            if isinstance(entry, RemoteRefEntry):
                create_kwargs: dict[str, Any] = {
                    "input_content": task,
                    "metadata": inject_context_metadata(
                        dict(metadata or {}),
                        task_context,
                    ),
                }
                if attachments:
                    create_kwargs["attachments"] = attachments
                webhook_config = self._build_webhook_config()
                if webhook_config is not None:
                    create_kwargs["webhook"] = webhook_config
                payload = await self._a2a_client.create_task(
                    entry.ref,
                    **create_kwargs,
                )
                upstream_task_id = payload.get("task_id")
                if not isinstance(upstream_task_id, str) or not upstream_task_id:
                    raise ValueError(
                        f"Remote ref '{agent_name}' returned no task_id from remote gateway."
                    )
                self._registry.register_task(
                    task_id,
                    agent_name=agent_name,
                    parent_task_id=parent_task_id,
                )
                self._task_manager.create_task_record(
                    task_id,
                    agent_name,
                    parent_task_id=parent_task_id,
                    root_task_id=root_task_id,
                    depth=depth,
                    route_kind="remote_ref",
                    upstream_task_id=upstream_task_id,
                    parent_thread_id=parent_thread_id,
                    webhook=webhook,
                    delegation_context=task_context,
                    permission_profile=permission_profile,
                    effective_skill_names=effective_skill_names,
                    skill_view_path=skill_view_path,
                    skill_view_hash=skill_view_hash,
                )
                return self._task_manager.sync_remote_task(task_id, payload)

            self._registry.register_task(
                task_id,
                agent_name=agent_name,
                parent_task_id=parent_task_id,
            )
            self._task_manager.create_task_record(
                task_id,
                agent_name,
                parent_task_id=parent_task_id,
                root_task_id=root_task_id,
                depth=depth,
                parent_thread_id=parent_thread_id,
                webhook=webhook,
                delegation_context=task_context,
                permission_profile=permission_profile,
                effective_skill_names=effective_skill_names,
                skill_view_path=skill_view_path,
                skill_view_hash=skill_view_hash,
            )
            self._start_run(task_id, task)
            return self._task_manager.get_task(task_id)

    async def send_task_input(
        self,
        task_id: str,
        message: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
    ) -> TaskRecord:
        """
        向已有任务发送后续输入

        本地任务会复用同一个 thread 启动新一轮 run；远端任务会通过 A2AClient
        转发到远端网关。

        Args:
            task_id: 当前 runtime 内部任务 ID
            message: 后续输入内容

        Returns:
            最新任务记录

        Raises:
            UnknownWorkerTaskError: task_id 不存在
            TaskAlreadyRunningError: 本地任务当前仍在运行
            A2AClientError: 远端 send_input 失败
        """
        # 为什么提供结构化继续接口：Gateway 需要直接把状态机错误映射到 HTTP 错误码。
        record = self._task_manager.get_task(task_id)
        if record.route_kind == "remote_ref":
            if record.state in ACTIVE_TASK_STATES:
                record = await self._refresh_remote_task_with_retries(task_id)
            entry = self._get_remote_entry_for_task(task_id)
            payload = await self._a2a_client.send_input(
                entry.ref,
                task_id=record.upstream_task_id or task_id,
                input_content=message,
                attachments=attachments,
            )
            return self._task_manager.sync_remote_task(task_id, payload)
        if record.active_run is not None and not record.active_run.done():
            # 或许之后可以投递到 subagent的mailbox里 这样可以对运行的agent提供命令而不是只有一切结束才行
            raise TaskAlreadyRunningError(f"Worker task is already running: {task_id}")
        if record.state in ACTIVE_TASK_STATES:
            raise TaskAlreadyRunningError(f"Worker task is already running: {task_id}")
        if record.state not in RESUMABLE_TASK_STATES:
            raise ValueError(
                f"Task '{task_id}' cannot receive input in state={record.state}"
            )
        # send_input 延续同一个 task/thread，不是新建一次委派，因此不增加 depth。
        self._start_run(task_id, message)
        return self._task_manager.get_task(task_id)

    async def cancel_task(self, task_id: str) -> TaskRecord:
        """
        取消已有任务

        本地任务会取消活跃 asyncio task；远端任务会通过 A2AClient 转发取消
        请求到远端网关。

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            最新任务记录

        Raises:
            UnknownWorkerTaskError: task_id 不存在
            A2AClientError: 远端取消失败
        """
        # 为什么提供结构化取消接口：HTTP 层要返回最新 task 视图，而不是人类可读文本。
        record = self._task_manager.get_task(task_id)
        if record.route_kind == "remote_ref":
            entry = self._get_remote_entry_for_task(task_id)
            payload = await self._a2a_client.cancel_task(
                entry.ref,
                task_id=record.upstream_task_id or task_id,
            )
            return self._task_manager.sync_remote_task(task_id, payload)
        if record.active_run is None or record.active_run.done():
            if record.state != "cancelled":
                self._task_manager.mark_cancelled(task_id)
            return self._task_manager.get_task(task_id)
        record.cancel_requested = True
        record.active_run.cancel()
        try:
            await record.active_run
        except asyncio.CancelledError:
            pass
        return self._task_manager.get_task(task_id)

    async def spawn_agent(
        self,
        agent_name: str,
        task: str,
        config: RunnableConfig = None,  # type: ignore[assignment]
    ) -> str:
        # 为什么有这个工具：让主 agent 能启动本地 worker 或远端引用，而不用阻塞当前流程。
        """
        委派本地 worker 或远端引用执行任务

        Args:
            agent_name: 要委托的 agent 目标名称
            task: 任务输入内容
            config: LangChain 工具调用上下文

        Returns:
            面向模型的启动结果文本，包含 task_id 和 route
        """
        try:
            parent_record = self._extract_parent_task_record(config)
            parent_thread_id = self._extract_parent_thread_id(config)
            if parent_thread_id is None and parent_record is not None:
                parent_thread_id = parent_record.thread_id
            record = await self.spawn_task(
                agent_name,
                task,
                parent_task_id=(
                    parent_record.task_id if parent_record is not None else None
                ),
                parent_thread_id=parent_thread_id,
            )
        except UnknownAgentTargetError:
            available = ", ".join(self._registry.list_target_names())
            # 工具参数错误返回普通文本，而不是抛异常打断整轮 agent 执行。
            return f"Unknown agent target: {agent_name}. Available: {available}"
        except MaxDelegationDepthError as exc:
            return self._format_depth_limit_error(exc)
        except MaxTasksPerRootError as exc:
            return self._format_task_budget_error(exc)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except (RemoteExecutorNotImplementedError, A2AClientError, ValueError) as exc:
            return str(exc)
        return (
            f"Started worker task: task_id={record.task_id} agent={agent_name} "
            f"route={record.route_kind}"
        )

    async def wait_agent(
        self,
        task_id: str,
        config: RunnableConfig = None,  # type: ignore[assignment]
    ) -> str:
        # 为什么有这个工具：让主 agent 在需要同步结果时，再显式等待本地或远端任务完成。
        """
        等待委托任务进入终态。

        如果任务暂停在人工审批点，且当前 config 没有可用的审批处理回调，
        本方法会返回最新的 agent-facing running 状态，而不是无限阻塞。

        Args:
            task_id: 当前 runtime 内部任务 ID
            config: LangChain 工具调用上下文

        Returns:
            面向模型的任务状态文本
        """
        try:
            record = self._task_manager.get_task(task_id)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        self._suppress_mailbox_delivery(record)
        if record.route_kind == "remote_ref":
            try:
                while True:
                    record = await self._refresh_remote_task_with_retries(task_id)
                    if record.state in TERMINAL_TASK_STATES:
                        return self._format_task_record(record)
                    if record.state == "waiting_for_human":
                        resolved = await self._resolve_pending_reviews_from_config(
                            config,
                        )
                        if not resolved:
                            return self._format_task_record(record)
                    await asyncio.sleep(self._remote_poll_interval)
            except A2AClientError as exc:
                return self._format_remote_status_unavailable(
                    self._task_manager.get_task(task_id),
                    exc,
                )
            except ValueError as exc:
                return str(exc)
        while True:
            if record.active_run is not None:
                try:
                    await record.active_run
                except asyncio.CancelledError:
                    pass
            record = self._task_manager.get_task(task_id)
            if record.state in TERMINAL_TASK_STATES:
                return self._format_task_record(record)
            if record.state == "waiting_for_human":
                resolved = await self._resolve_pending_reviews_from_config(config)
                record = self._task_manager.get_task(task_id)
                if record.state == "waiting_for_human" and not resolved:
                    return self._format_task_record(record)
                await asyncio.sleep(0.1)
                continue
            return self._format_task_record(record)

    async def check_agent(
        self,
        task_id: str,
        config: RunnableConfig = None,  # type: ignore[assignment]
    ) -> str:
        # 为什么有这个工具：让主 agent 可以非阻塞地轮询 worker 当前状态。
        """
        非阻塞查询委托任务状态

        Args:
            task_id: 当前 runtime 内部任务 ID
            config: LangChain 工具调用上下文

        Returns:
            面向模型的任务状态文本
        """
        try:
            record = self._task_manager.get_task(task_id)
            if record.route_kind == "remote_ref":
                record = await self._refresh_remote_task_with_retries(task_id)
            if record.state in TERMINAL_TASK_STATES:
                self._suppress_mailbox_delivery(record)
            return self._format_task_record(record)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except A2AClientError as exc:
            return self._format_remote_status_unavailable(
                self._task_manager.get_task(task_id),
                exc,
            )
        except ValueError as exc:
            return str(exc)

    async def send_input(self, task_id: str, message: str) -> str:
        # 为什么有这个工具：让主 agent 可以继续推进同一个 worker 任务，而不用重新创建会话。
        """
        向已有委托任务发送后续输入

        Args:
            task_id: 当前 runtime 内部任务 ID
            message: 后续输入内容

        Returns:
            面向模型的发送结果文本
        """
        try:
            record = await self.send_task_input(task_id, message)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except TaskAlreadyRunningError:
            # 同一个 task 同一时间只允许一个活跃 run，避免线程语义混乱。
            return f"Worker task is still running: {task_id}. Wait for it before sending more input."
        except (A2AClientError, ValueError) as exc:
            return str(exc)
        return f"Sent input to worker task: task_id={record.task_id}"

    async def cancel_agent(self, task_id: str) -> str:
        # 为什么有这个工具：让主 agent 可以主动中断不再需要的 worker 任务。
        """
        取消不再需要的委托任务

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            面向模型的取消结果文本
        """
        try:
            record = await self.cancel_task(task_id)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except (A2AClientError, ValueError) as exc:
            return str(exc)
        if record.state == "cancelled":
            return f"Cancelled worker task: task_id={record.task_id}"
        return f"Cancelled worker task: task_id={record.task_id}"

    async def list_agents(self) -> str:
        # 为什么有这个工具：让主 agent 能看到当前 runtime 里有哪些 worker 任务正在被管理。
        """
        列出当前 runtime 的 agent 目标和任务

        Returns:
            面向模型的 agent 与 task 列表文本
        """
        return self._format_agents_and_tasks()

    def _format_agents_and_tasks(
        self,
        allowed_targets: set[str] | None = None,
        visible_task_ids: set[str] | None = None,
    ) -> str:
        """
        格式化 agent 目标和任务列表

        Args:
            allowed_targets: 当前调用方允许访问的目标名称集合
            visible_task_ids: 当前调用方可见的任务 ID 集合

        Returns:
            面向模型的多行状态文本
        """
        registered_lines = [
            "Registered agent targets:",
            *[
                self._format_registered_agent(entry)
                for entry in sorted(
                    self._registry.list_registered_agents(allowed_targets),
                    key=lambda item: item.name,
                )
            ],
        ]
        records = self._task_manager.list_tasks()
        if not records:
            return "\n".join([*registered_lines, "Tracked tasks:", "- none"])
        task_lines = ["Tracked tasks:"]
        task_lines.extend(
            self._format_task_record(record)
            for record in records
            if (allowed_targets is None or record.agent_name in allowed_targets)
            and (visible_task_ids is None or record.task_id in visible_task_ids)
        )
        if len(task_lines) == 1:
            task_lines.append("- none")
        return "\n".join([*registered_lines, *task_lines])

    def _format_registered_agent(self, entry: RegisteredAgent) -> str:
        """
        格式化单个注册项

        Args:
            entry: agent 注册项

        Returns:
            单行 agent 目标描述
        """
        parts = [
            f"name={entry.name}",
            f"kind={entry.kind}",
            f"description={entry.description}",
        ]
        if isinstance(entry, RemoteRefEntry):
            parts.append("availability=spawnable")
            parts.append("runtime=remote_gateway")
        else:
            parts.append("availability=spawnable")
        return " | ".join(parts)
