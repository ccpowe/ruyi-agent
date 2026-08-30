"""Delegation tree context, permissions, and budget policy.

This component computes immutable task-tree context and guards structural
limits. It never schedules a run or performs transport I/O.
"""

from __future__ import annotations

import logging
import asyncio

from langchain_core.runnables import RunnableConfig

from ruyi_agent.runtime.delegation.context import (
    DelegationContext,
    build_child_context,
    build_root_context,
    parse_inbound_metadata,
)
from ruyi_agent.runtime.delegation.contracts import (
    MaxDelegationDepthError,
    UnknownWorkerTaskError,
)
from ruyi_agent.runtime.delegation.registry import LocalWorkerEntry, RegisteredAgent
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.storage.task_store import (
    TaskRootBudgetExceededError as MaxTasksPerRootError,
)
from ruyi_agent.task_models import MetadataScalar, TaskRecord

logger = logging.getLogger(__name__)


class DelegationPolicy:
    """Own delegation-tree invariants without owning task execution."""

    def __init__(self, task_manager: TaskManager, *, node_id: str, max_delegation_depth: int, max_tasks_per_root: int, permission_default_profile: str) -> None:  # fmt: skip
        if max_delegation_depth < 1:
            raise ValueError("max_delegation_depth must be at least 1")
        if max_tasks_per_root < 1:
            raise ValueError("max_tasks_per_root must be at least 1")
        self._task_manager = task_manager
        self._node_id = node_id
        self._max_delegation_depth = max_delegation_depth
        self._max_tasks_per_root = max_tasks_per_root
        self._permission_default_profile = permission_default_profile
        self._root_budget_locks: dict[str, asyncio.Lock] = {}

    def extract_parent_thread_id(
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

    def extract_parent_task_record(
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

    def resolve_task_tree_context(
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
                self.delegation_context_from_record(parent), depth=depth
            ),
        )

    def delegation_context_from_record(self, record: TaskRecord) -> DelegationContext:
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

    def resolve_permission_profile(
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

    def enforce_delegation_depth(
        self,
        *,
        depth: int,
        max_depth: int,
    ) -> None:
        """
        校验委托深度

        Args:
            depth: 即将创建任务的深度
            max_depth: 允许的最大深度

        Raises:
            MaxDelegationDepthError: depth 超过 max_depth
        """
        if depth > max_depth:
            raise MaxDelegationDepthError(
                current_depth=depth,
                max_depth=max_depth,
            )

    def get_root_budget_lock(self, root_task_id: str) -> asyncio.Lock:
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

    def format_depth_limit_error(self, exc: MaxDelegationDepthError) -> str:
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

    def format_task_budget_error(self, exc: MaxTasksPerRootError) -> str:
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

    def prepare_delegation_metadata(
        self,
        metadata: dict[str, MetadataScalar],
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
        return parse_inbound_metadata(
            metadata,
            node_id=self._node_id,
            local_max_depth=self._max_delegation_depth,
            local_max_tasks_per_root=self._max_tasks_per_root,
        )
