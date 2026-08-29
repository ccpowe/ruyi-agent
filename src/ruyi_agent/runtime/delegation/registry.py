"""Agent target registry for local workers and remote references.

The registry owns the shared target namespace and target lookup only. It does
not create runtime agents or execute tasks.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.runtime.delegation.contracts import (
    UnavailableAgentTargetError,
    UnknownAgentTargetError,
)

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
        unavailable_agents: dict[str, str] | None = None,
    ) -> None:
        """
        初始化 agent 注册表

        Args:
            specs: 本地 worker 配置表
            remote_refs: 远端 agent 引用配置表
            unavailable_agents: 已配置但初始化失败的本地 agent 及原因
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
        self._unavailable_agents = dict(unavailable_agents or {})

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
        entry = self._entries.get(agent_name)
        if entry is not None:
            return entry
        unavailable_reason = self._unavailable_agents.get(agent_name)
        if unavailable_reason is not None:
            raise UnavailableAgentTargetError(
                f"Agent target '{agent_name}' is unavailable: {unavailable_reason}"
            )
        raise UnknownAgentTargetError(f"Unknown agent target: {agent_name}")

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

    def select_local_specs(
        self,
        target_names: Sequence[str],
    ) -> dict[str, LocalWorkerSpec]:
        """Resolve local targets in declaration order, skipping unavailable entries."""
        return {
            name: entry.spec
            for name in target_names
            if isinstance((entry := self._entries.get(name)), LocalWorkerEntry)
        }

    def select_remote_refs(
        self,
        target_names: Sequence[str],
    ) -> dict[str, RemoteRef]:
        """Resolve remote targets in declaration order."""
        return {
            name: entry.ref
            for name in target_names
            if isinstance((entry := self._entries.get(name)), RemoteRefEntry)
        }

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
