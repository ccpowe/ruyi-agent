"""LangChain delegation tools and their agent-facing presentation.

DelegationTools adapts the structured runtime API to scoped model tools. It
owns visibility rules and human-readable results, but never mutates TaskRecord
state directly.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Protocol

from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool

from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.contracts import (
    ListAgentsSchema,
    MaxDelegationDepthError,
    RemoteExecutorNotImplementedError,
    SPAWN_AGENT_TOOL_DESCRIPTION,
    SendInputSchema,
    SpawnAgentSchema,
    TaskAlreadyRunningError,
    TaskIdSchema,
    UnknownAgentTargetError,
    UnknownWorkerTaskError,
)
from ruyi_agent.runtime.delegation.registry import (
    RegisteredAgent,
    RemoteRefEntry,
)
from ruyi_agent.storage.task_store import TaskRootBudgetExceededError as MaxTasksPerRootError
from ruyi_agent.task_models import SETTLED_TASK_STATES, TaskRecord

logger = logging.getLogger(__name__)


class DelegationToolsHost(Protocol):
    _registry: Any
    _remote_poll_interval: float
    _remote_status_retry_attempts: int
    _task_manager: Any

    def _allowed_targets_for_agent(self, agent_name: str) -> set[str]: ...
    def _extract_parent_task_record(
        self, config: RunnableConfig | None
    ) -> TaskRecord | None: ...
    def _extract_parent_thread_id(self, config: RunnableConfig | None) -> str | None: ...
    def _format_agents_and_tasks(
        self,
        allowed_targets: set[str] | None = None,
        visible_task_ids: set[str] | None = None,
    ) -> str: ...
    def _format_depth_limit_error(self, exc: MaxDelegationDepthError) -> str: ...
    def _format_remote_status_unavailable(
        self, record: TaskRecord, exc: BaseException
    ) -> str: ...
    def _format_task_budget_error(self, exc: MaxTasksPerRootError) -> str: ...
    def _format_task_record(self, record: TaskRecord) -> str: ...
    async def _refresh_remote_task_with_retries(self, task_id: str) -> TaskRecord: ...
    async def _resolve_pending_reviews_from_config(
        self, config: RunnableConfig | None
    ) -> bool: ...
    def _suppress_mailbox_delivery(self, record: TaskRecord) -> None: ...
    async def cancel_task(self, task_id: str) -> TaskRecord: ...
    async def send_task_input(self, task_id: str, message: str, **kwargs: Any) -> TaskRecord: ...
    async def spawn_task(self, agent_name: str, task: str, **kwargs: Any) -> TaskRecord: ...


class DelegationTools:
    """Build scoped delegation tools over the structured runtime port."""

    def __init__(self, control: DelegationToolsHost) -> None:
        self._control = control

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
        model_state = "running" if record.state == "waiting_for_human" else record.state
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


    def _allowed_targets_for_agent(self, agent_name: str) -> set[str]:
        """
        计算某个本地 worker 可委托的目标集合

        Args:
            agent_name: 本地 worker 名称

        Returns:
            该 worker 配置中允许继续委托的本地 worker 和 remote_ref 名称集合
        """
        return set(self._control._registry.get_spec(agent_name).delegation_targets)

    def build_tools_for(self, agent_name: str) -> list[StructuredTool]:
        """
        为指定 worker 构造带作用域的委托工具

        Args:
            agent_name: 本地 worker 名称

        Returns:
            该 worker 可使用的 StructuredTool 列表
        """
        spec = self._control._registry.get_spec(agent_name)
        return self._control._build_tools(
            allowed_targets=self._control._allowed_targets_for_agent(agent_name),
            caller_agent_name=agent_name,
            enabled_tools=spec.system_tools,
        )

    def build_tools(self) -> list[StructuredTool]:
        """
        构造主 agent 使用的全量委托工具

        Returns:
            不限制目标范围的 StructuredTool 列表
        """
        return self._control._build_tools()

    def _caller_and_visible_tasks(
        self,
        config: RunnableConfig | None,
        *,
        allowed_targets: set[str] | None,
    ) -> tuple[TaskRecord | None, list[TaskRecord]]:
        """Resolve the caller and exactly the tasks visible to its tool scope."""
        try:
            caller = self._control._extract_parent_task_record(config)
        except UnknownWorkerTaskError:
            caller = None
        if caller is not None:
            records: list[TaskRecord] = [caller]
            if caller.parent_task_id is not None:
                try:
                    records.append(
                        self._control._task_manager.get_task(caller.parent_task_id)
                    )
                except UnknownWorkerTaskError:
                    pass
            records.extend(
                record
                for record in self._control._task_manager.list_persisted_tasks()
                if record.parent_task_id == caller.task_id
            )
            return caller, records
        parent_thread_id = self._control._extract_parent_thread_id(config)
        if parent_thread_id is None:
            return None, []
        self._control._task_manager.load_by_parent_thread_id(parent_thread_id)
        return None, [
            record
            for record in self._control._task_manager.list_tasks()
            if record.parent_thread_id == parent_thread_id
            and (allowed_targets is None or record.agent_name in allowed_targets)
        ]

    @staticmethod
    def _format_scope_error(
        *,
        operation: str,
        requested_task_id: str,
        caller: TaskRecord | None,
        visible: list[TaskRecord],
    ) -> str:
        """Explain a rejected task operation without exposing unrelated tasks."""
        allowed = [
            record
            for record in visible
            if record.task_id != (caller.task_id if caller else None)
            and (
                operation == "send_input"
                or caller is None
                or record.parent_task_id == caller.task_id
            )
        ]
        lines = [
            f"Cannot use {operation} with task '{requested_task_id}'.",
            (
                "send_input may target only your direct parent or direct children."
                if operation == "send_input"
                else f"{operation} may target only your direct children."
            ),
            "Allowed task IDs:",
        ]
        if not allowed:
            lines.append("- none")
        else:
            for record in allowed:
                relation = (
                    "parent"
                    if caller is not None and caller.parent_task_id == record.task_id
                    else "child"
                )
                lines.append(f"- {relation}: {record.task_id}")
        lines.append(
            "Use an exact ID above, or call list_agents to refresh the task list."
        )
        return "\n".join(lines)

    def _get_scoped_task(
        self,
        task_id: str,
        config: RunnableConfig | None,
        *,
        operation: str,
        allowed_targets: set[str] | None,
    ) -> TaskRecord:
        """Return one task only when it is visible to the current tool caller."""
        try:
            record = self._control._task_manager.get_task(task_id)
        except UnknownWorkerTaskError:
            raise UnknownWorkerTaskError(
                f"Unknown task_id '{task_id}'. Call list_agents and use an exact "
                "task_id from its result."
            ) from None
        caller, visible = self._caller_and_visible_tasks(
            config,
            allowed_targets=allowed_targets,
        )
        visible_ids = {item.task_id for item in visible}
        allowed = record.task_id in visible_ids
        if caller is not None:
            is_parent = caller.parent_task_id == record.task_id
            is_child = record.parent_task_id == caller.task_id
            allowed = is_child or (operation == "send_input" and is_parent)
        if not allowed:
            raise UnknownWorkerTaskError(
                self._format_scope_error(
                    operation=operation,
                    requested_task_id=task_id,
                    caller=caller,
                    visible=visible,
                )
            )
        return record

    def _build_tools(
        self,
        *,
        allowed_targets: set[str] | None = None,
        caller_agent_name: str | None = None,
        enabled_tools: frozenset[str] | None = None,
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
            available_local_workers=self._control._registry.render_local_worker_descriptions(
                allowed_targets
            ),
            available_remote_refs=self._control._registry.render_remote_ref_descriptions(
                allowed_targets
            ),
        )

        async def scoped_spawn_agent(
            agent_name: str,
            task: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带目标白名单校验的 spawn_agent 包装器"""
            if allowed_targets is not None and agent_name not in allowed_targets:
                available = ", ".join(self._control._registry.list_target_names(allowed_targets))
                caller = caller_agent_name or "current agent"
                return (
                    f"Agent target '{agent_name}' is not allowed for '{caller}'. "
                    f"Available: {available}"
                )
            return await self._control.spawn_agent(agent_name, task, config)

        async def scoped_wait_agent(
            task_id: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 wait_agent 包装器"""
            try:
                self._get_scoped_task(
                    task_id,
                    config,
                    operation="wait_agent",
                    allowed_targets=allowed_targets,
                )
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self._control.wait_agent(task_id, config)

        async def scoped_check_agent(
            task_id: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 check_agent 包装器"""
            try:
                self._get_scoped_task(
                    task_id,
                    config,
                    operation="check_agent",
                    allowed_targets=allowed_targets,
                )
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self._control.check_agent(task_id, config)

        async def scoped_send_input(
            task_id: str,
            message: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 send_input 包装器"""
            try:
                self._get_scoped_task(
                    task_id,
                    config,
                    operation="send_input",
                    allowed_targets=allowed_targets,
                )
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self._control.send_input(task_id, message)

        async def scoped_cancel_agent(
            task_id: str,
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """带任务可见性校验的 cancel_agent 包装器"""
            try:
                self._get_scoped_task(
                    task_id,
                    config,
                    operation="cancel_agent",
                    allowed_targets=allowed_targets,
                )
            except UnknownWorkerTaskError as exc:
                return str(exc)
            return await self._control.cancel_agent(task_id)

        async def scoped_list_agents(
            config: RunnableConfig = None,  # type: ignore[assignment]
        ) -> str:
            """列出当前调用方可见的 agent 目标和任务"""
            parent_thread_id = self._control._extract_parent_thread_id(config)
            if parent_thread_id is not None:
                self._control._task_manager.load_by_parent_thread_id(parent_thread_id)
            _, visible = self._caller_and_visible_tasks(
                config,
                allowed_targets=allowed_targets,
            )
            visible_task_ids = {record.task_id for record in visible}
            return self._control._format_agents_and_tasks(
                allowed_targets,
                visible_task_ids=visible_task_ids,
            )

        # 这里统一生成 StructuredTool，主 agent 看到的是稳定的 schema + description，
        # 而不是一组信息不足的裸方法。
        tools = [
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
                    "Wait for a delegated task's current run to settle. If the "
                    "run is blocked on external review and no review handler can "
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
                    "Send additional input to your direct parent task or one of "
                    "your direct child tasks. Running tasks receive it at the "
                    "next safe model boundary; idle tasks are awakened."
                ),
                infer_schema=False,
                args_schema=SendInputSchema,
            ),
            StructuredTool.from_function(
                coroutine=scoped_cancel_agent,
                name="cancel_agent",
                description=(
                    "Cancel a delegated task's current run. The agent session "
                    "remains available for follow-up input."
                ),
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
        if enabled_tools is None:
            return tools
        return [tool for tool in tools if tool.name in enabled_tools]


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
            parent_record = self._control._extract_parent_task_record(config)
            parent_thread_id = self._control._extract_parent_thread_id(config)
            if parent_thread_id is None and parent_record is not None:
                parent_thread_id = parent_record.thread_id
            record = await self._control.spawn_task(
                agent_name,
                task,
                parent_task_id=(
                    parent_record.task_id if parent_record is not None else None
                ),
                parent_thread_id=parent_thread_id,
            )
        except UnknownAgentTargetError as exc:
            available = ", ".join(self._control._registry.list_target_names())
            # 工具参数错误返回普通文本，而不是抛异常打断整轮 agent 执行。
            return f"{exc}. Available: {available}"
        except MaxDelegationDepthError as exc:
            return self._control._format_depth_limit_error(exc)
        except MaxTasksPerRootError as exc:
            return self._control._format_task_budget_error(exc)
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
        等待委托任务当前 run settled。

        如果任务暂停在人工审批点，且当前 config 没有可用的审批处理回调，
        本方法会返回最新的 agent-facing running 状态，而不是无限阻塞。

        Args:
            task_id: 当前 runtime 内部任务 ID
            config: LangChain 工具调用上下文

        Returns:
            面向模型的任务状态文本
        """
        try:
            record = self._control._task_manager.get_task(task_id)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        self._control._suppress_mailbox_delivery(record)
        if record.route_kind == "remote_ref":
            try:
                while True:
                    record = await self._control._refresh_remote_task_with_retries(task_id)
                    if record.state in SETTLED_TASK_STATES:
                        return self._control._format_task_record(record)
                    if record.state == "waiting_for_human":
                        resolved = await self._control._resolve_pending_reviews_from_config(
                            config,
                        )
                        if not resolved:
                            return self._control._format_task_record(record)
                    await asyncio.sleep(self._control._remote_poll_interval)
            except A2AClientError as exc:
                return self._control._format_remote_status_unavailable(
                    self._control._task_manager.get_task(task_id),
                    exc,
                )
            except ValueError as exc:
                return str(exc)
        while True:
            run_task = self._control._task_manager.get_live_run(task_id)
            if run_task is not None:
                try:
                    await asyncio.shield(run_task)
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    if current is not None and current.cancelling():
                        raise
            record = self._control._task_manager.get_task(task_id)
            if record.state in SETTLED_TASK_STATES:
                return self._control._format_task_record(record)
            if record.state == "waiting_for_human":
                resolved = await self._control._resolve_pending_reviews_from_config(config)
                record = self._control._task_manager.get_task(task_id)
                if record.state == "waiting_for_human" and not resolved:
                    return self._control._format_task_record(record)
                await asyncio.sleep(0.1)
                continue
            return self._control._format_task_record(record)

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
            record = self._control._task_manager.get_task(task_id)
            if record.route_kind == "remote_ref":
                record = await self._control._refresh_remote_task_with_retries(task_id)
            if record.state in SETTLED_TASK_STATES:
                self._control._suppress_mailbox_delivery(record)
            return self._control._format_task_record(record)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except A2AClientError as exc:
            return self._control._format_remote_status_unavailable(
                self._control._task_manager.get_task(task_id),
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
            record = await self._control.send_task_input(task_id, message)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except TaskAlreadyRunningError:
            # 同一个 task 同一时间只允许一个活跃 run，避免线程语义混乱。
            return f"Worker task is still running: {task_id}. Wait for it before sending more input."
        except (A2AClientError, ValueError) as exc:
            return str(exc)
        return f"Sent input to worker task: task_id={record.task_id}"

    async def cancel_agent(self, task_id: str) -> str:
        # 为什么有这个工具：让主 agent 可以中断 worker 当前 run，同时保留会话。
        """
        取消委托 Task 当前 run，不关闭长期 agent 会话

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            面向模型的取消结果文本
        """
        try:
            record = await self._control.cancel_task(task_id)
        except UnknownWorkerTaskError as exc:
            return str(exc)
        except (A2AClientError, ValueError) as exc:
            return str(exc)
        if record.state == "cancelled":
            return f"Cancelled current worker run: task_id={record.task_id}"
        return (
            "No active worker run to cancel: "
            f"task_id={record.task_id} state={record.state}"
        )

    async def list_agents(self) -> str:
        # 为什么有这个工具：让主 agent 能看到当前 runtime 里有哪些 worker 任务正在被管理。
        """
        列出当前 runtime 的 agent 目标和任务

        Returns:
            面向模型的 agent 与 task 列表文本
        """
        return self._control._format_agents_and_tasks()

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
                self._control._format_registered_agent(entry)
                for entry in sorted(
                    self._control._registry.list_registered_agents(allowed_targets),
                    key=lambda item: item.name,
                )
            ],
        ]
        records = self._control._task_manager.list_tasks()
        if not records:
            return "\n".join([*registered_lines, "Tracked tasks:", "- none"])
        task_lines = ["Tracked tasks:"]
        task_lines.extend(
            self._control._format_task_record(record)
            for record in records
            if (
                record.task_id in visible_task_ids
                if visible_task_ids is not None
                else allowed_targets is None or record.agent_name in allowed_targets
            )
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
