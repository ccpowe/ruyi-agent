"""Structured task runtime orchestration.

TaskRuntime coordinates registry, policy, local execution, and the remote port
for Gateway-facing task use cases. It owns use-case ordering, not persistence
internals, transport details, or model-facing tool presentation.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Protocol

from ruyi_agent.runtime.delegation.context import (
    DelegationContext,
    parse_inbound_metadata,
)
from ruyi_agent.runtime.delegation.contracts import (
    DurableTaskMailboxRequiredError,
    TaskAlreadyRunningError,
    UnknownWorkerTaskError,
)
from ruyi_agent.runtime.delegation.registry import (
    LocalWorkerEntry,
    RegisteredAgent,
    RemoteRefEntry,
)
from ruyi_agent.runtime.delegation.run_supervisor import MutationPermit
from ruyi_agent.runtime.message_history import TaskMessageSnapshot
from ruyi_agent.runtime.task_events import (
    TaskEventSubscription,
    TaskEventsUnavailableError,
)
from ruyi_agent.storage.task_store import StoredTaskAlreadyExistsError
from ruyi_agent.task_models import (
    ACTIVE_TASK_STATES,
    RESUMABLE_TASK_STATES,
    MetadataScalar,
    PendingReviewRecord,
    TaskRecord,
)


class TaskRuntimeHost(Protocol):
    _mailbox: Any
    _max_delegation_depth: int
    _max_tasks_per_root: int
    _message_state_reader: Any
    _node_id: str
    _permission_default_profile: str
    _registry: Any
    _run_supervisor: Any
    _task_manager: Any

    async def _allocate_remote_task(self, **kwargs: Any) -> TaskRecord: ...
    async def _cancel_remote_task(self, record: TaskRecord) -> TaskRecord: ...
    def _delegation_context_from_record(
        self, record: TaskRecord
    ) -> DelegationContext: ...
    def _enforce_delegation_depth(self, *, depth: int, max_depth: int) -> None: ...
    def _existing_idempotent_task(self, **kwargs: Any) -> TaskRecord | None: ...
    async def _ensure_task_awake(self, task_id: str) -> TaskRecord: ...
    def _get_root_budget_lock(self, root_task_id: str) -> asyncio.Lock: ...
    async def _refresh_remote_task_with_retries(self, task_id: str) -> TaskRecord: ...
    async def _send_remote_task_input(
        self, record: TaskRecord, **kwargs: Any
    ) -> TaskRecord: ...
    async def _submit_remote_review_decision(
        self, record: TaskRecord, **kwargs: Any
    ) -> TaskRecord: ...
    def _resolve_permission_profile(self, **kwargs: Any) -> str: ...
    def _resolve_task_skill_view(
        self, entry: RegisteredAgent, *, parent_task_id: str | None
    ) -> tuple[tuple[str, ...], str | None, str | None]: ...
    def _resolve_task_tree_context(
        self, **kwargs: Any
    ) -> tuple[str, int, DelegationContext]: ...
    async def _resume_run(
        self,
        task_id: str,
        decisions: list[dict[str, Any]],
        *,
        permit: MutationPermit | None = None,
    ) -> asyncio.Task[None]: ...
    async def _start_run(
        self,
        task_id: str,
        user_input: str,
        *,
        permit: MutationPermit | None = None,
    ) -> asyncio.Task[None]: ...


class TaskRuntime:
    """Coordinate structured task operations across runtime boundaries."""

    def __init__(self, control: TaskRuntimeHost) -> None:
        self._control = control

    def list_registered_agents_snapshot(self) -> list[RegisteredAgent]:
        """
        获取已注册 agent 的结构化快照

        Returns:
            当前 runtime 中全部已注册 agent 目标
        """
        # 为什么暴露结构化已登记目标：Gateway 需要机器可读的 agent 列表，而不是 prompt 文本。
        return self._control._registry.list_registered_agents()

    def load_tasks_for_thread(self, thread_id: str) -> None:
        """
        加载指定 agent thread 创建的任务

        runtime agent middleware 在每次 agent run 开始时调用该方法。它只恢复
        parent_thread_id 等于当前 thread_id 的任务，不做全库加载。
        """
        self._control._task_manager.load_by_parent_thread_id(thread_id)

    def get_registered_agent(self, agent_name: str) -> RegisteredAgent:
        """
        获取指定 agent 注册项

        Args:
            agent_name: agent 目标名称

        Returns:
            对应的注册项
        """
        return self._control._registry.get_entry(agent_name)

    def get_task_record(self, task_id: str) -> TaskRecord:
        """
        获取指定任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            对应的任务记录
        """
        return self._control._task_manager.get_task(task_id)

    def get_live_run(self, task_id: str) -> asyncio.Task[None] | None:
        """Return a local run handle from the runtime-only registry.

        Task status consumers should normally use ``get_task_record``. This
        method exists for runtime coordination and diagnostics that must await
        the currently scheduled run without coupling persistence to asyncio.
        """

        self._control._task_manager.get_task(task_id)
        return self._control._run_supervisor.get_run(task_id)

    def open_local_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> TaskEventSubscription:
        """Open one fixed-run stream without taking ownership of the Agent run."""

        record = self._control._task_manager.get_task(task_id)
        if record.route_kind != "local":
            raise ValueError(f"Task '{task_id}' is not a local task")
        ledger = self._control._task_manager.event_ledger
        if ledger is None:
            raise TaskEventsUnavailableError(
                f"Task events for '{task_id}' require durable Task storage"
            )
        return ledger.open_stream(
            task_id=task_id,
            run_count=run_count,
            last_event_id=last_event_id,
        )

    async def get_local_task_message_snapshot(
        self,
        task_id: str,
        *,
        checkpoint_id: str | None = None,
    ) -> TaskMessageSnapshot:
        """Reconstruct one exact local Task conversation checkpoint."""

        record = self._control._task_manager.get_task(task_id)
        if record.route_kind != "local":
            raise ValueError(f"Task '{task_id}' is not a local task")
        return await self._control._message_state_reader.read(
            thread_id=record.thread_id,
            checkpoint_id=checkpoint_id,
        )

    def list_task_records(self) -> list[TaskRecord]:
        """
        列出当前 runtime 追踪的全部任务

        Returns:
            任务记录列表
        """
        return self._control._task_manager.list_tasks()

    def list_persisted_task_records(self) -> list[TaskRecord]:
        """List runtime and persisted tasks for Gateway discovery."""
        return self._control._task_manager.list_persisted_tasks()

    def list_pending_review_records(self) -> list[TaskRecord]:
        return [
            self._control._task_manager.get_task(review.task_id)
            for review in self._control._task_manager.list_pending_reviews()
        ]

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        """Enumerate authoritative pending Review resources."""

        return self._control._task_manager.list_pending_reviews(
            root_task_id=root_task_id,
            task_id=task_id,
        )

    def get_pending_review(self, review_id: str) -> PendingReviewRecord:
        """Resolve one authoritative pending Review resource."""

        review = self._control._task_manager.get_pending_review(review_id)
        if review is None:
            raise UnknownWorkerTaskError(f"Unknown pending review: {review_id}")
        return review

    def get_task_by_review_id(self, review_id: str) -> TaskRecord:
        record = self._control._task_manager.find_by_review_id(review_id)
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
        permit = await self._control._run_supervisor.acquire_mutation()
        try:
            record = self._control.get_task_by_review_id(review_id)
            if record.route_kind == "remote_ref":
                operation = await self._control._run_supervisor.promote_to_operation(
                    permit
                )
                try:
                    return await self._control._submit_remote_review_decision(
                        record,
                        review_id=review_id,
                        decisions=decisions,
                    )
                finally:
                    await self._control._run_supervisor.release_operation(operation)
            if record.route_kind != "local":
                raise ValueError(f"Unsupported review route={record.route_kind}")
            if record.state != "waiting_for_human":
                raise ValueError(
                    f"Review '{review_id}' is not pending; task state={record.state}"
                )
            if record.pending_review is None:
                raise ValueError(f"Review '{review_id}' has no pending payload")
            task_id = record.task_id
            run_task = await self._control._resume_run(
                task_id,
                decisions,
                permit=permit,
            )
        finally:
            await self._control._run_supervisor.release_mutation(permit)
        if wait and run_task is not None:
            try:
                await run_task
            except asyncio.CancelledError:
                pass
        return self._control._task_manager.get_task(task_id)

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
            node_id=self._control._node_id,
            local_max_depth=self._control._max_delegation_depth,
            local_max_tasks_per_root=self._control._max_tasks_per_root,
        )

    def _existing_idempotent_task(
        self,
        *,
        task_id: str,
        agent_name: str,
        entry: RegisteredAgent,
        parent_task_id: str | None,
        root_task_id: str,
        depth: int,
        delegation_context: DelegationContext,
    ) -> TaskRecord | None:
        """Return a compatible pre-existing task without overwriting its state."""

        try:
            record = self._control._task_manager.get_task(task_id)
        except UnknownWorkerTaskError:
            return None
        expected_route_kind = (
            "remote_ref" if isinstance(entry, RemoteRefEntry) else "local"
        )
        if (
            record.agent_name != agent_name
            or record.parent_task_id != parent_task_id
            or record.root_task_id != root_task_id
            or record.depth != depth
            or record.route_kind != expected_route_kind
            or record.delegation_root_id != delegation_context.root_id
            or record.delegation_max_depth != delegation_context.max_depth
            or record.delegation_max_tasks_per_root
            != delegation_context.max_tasks_per_root
            or record.delegation_visited_nodes != delegation_context.visited_nodes
        ):
            raise ValueError(
                f"Task '{task_id}' already exists with a different binding"
            )
        return record

    async def spawn_task(
        self,
        agent_name: str,
        task: str,
        *,
        task_id: str | None = None,
        idempotency_key: str | None = None,
        parent_task_id: str | None = None,
        parent_thread_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        webhook: dict[str, Any] | None = None,
        delegation_context: DelegationContext | None = None,
    ) -> TaskRecord:
        permit = await self._control._run_supervisor.acquire_mutation()
        try:
            return await self._spawn_task_admitted(
                agent_name,
                task,
                task_id=task_id,
                idempotency_key=idempotency_key,
                parent_task_id=parent_task_id,
                parent_thread_id=parent_thread_id,
                metadata=metadata,
                attachments=attachments,
                webhook=webhook,
                delegation_context=delegation_context,
                permit=permit,
            )
        finally:
            await self._control._run_supervisor.release_mutation(permit)

    async def _spawn_task_admitted(
        self,
        agent_name: str,
        task: str,
        *,
        task_id: str | None,
        idempotency_key: str | None,
        parent_task_id: str | None,
        parent_thread_id: str | None,
        metadata: dict[str, Any] | None,
        attachments: list[dict[str, Any]] | None,
        webhook: dict[str, Any] | None,
        delegation_context: DelegationContext | None,
        permit: MutationPermit,
    ) -> TaskRecord:
        """
        创建结构化委托任务

        这是 Gateway 和工具层共享的核心 spawn 入口。本地 worker 会立即创建
        TaskRecord 并异步启动；远端 remote_ref 会先调用远端网关创建任务，再
        把远端 payload 同步成本地 TaskRecord。

        Args:
            agent_name: 要委托的 agent 目标名称
            task: 任务输入内容
            task_id: 调用方预留的稳定任务 ID；用于幂等 Gateway create
            idempotency_key: 透传给 remote_ref Gateway 的幂等键
            parent_task_id: 父任务 ID
            parent_thread_id: 父 agent thread ID，用于 mailbox 回投
            metadata: 传给远端任务的 metadata
            webhook: 当前调用方希望接收的 run settled webhook
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
        entry = self._control._registry.get_entry(agent_name)
        task_id = task_id or str(uuid.uuid4())
        permission_profile = self._control._resolve_permission_profile(
            entry=entry,
            parent_task_id=parent_task_id,
        )
        root_task_id, depth, task_context = self._control._resolve_task_tree_context(
            task_id=task_id,
            parent_task_id=parent_task_id,
            delegation_context=delegation_context,
        )
        effective_skill_names, skill_view_path, skill_view_hash = (
            self._control._resolve_task_skill_view(entry, parent_task_id=parent_task_id)
        )
        budget_lock = self._control._get_root_budget_lock(root_task_id)
        permit = await self._control._run_supervisor.wait_for_lock(
            permit,
            budget_lock,
        )
        try:
            existing = self._control._existing_idempotent_task(
                task_id=task_id,
                agent_name=agent_name,
                entry=entry,
                parent_task_id=parent_task_id,
                root_task_id=root_task_id,
                depth=depth,
                delegation_context=task_context,
            )
            if existing is not None:
                if (
                    isinstance(entry, LocalWorkerEntry)
                    and existing.state == "pending"
                    and existing.run_count == 0
                    and not self._control._task_manager.has_active_run(task_id)
                ):
                    await self._control._start_run(
                        task_id,
                        task,
                        permit=permit,
                    )
                    return self._control._task_manager.get_task(task_id)
                if not (
                    isinstance(entry, RemoteRefEntry)
                    and existing.upstream_task_id is None
                ):
                    return existing
            self._control._enforce_delegation_depth(
                depth=depth,
                max_depth=task_context.max_depth,
            )
            self._control._registry.register_task(
                task_id,
                agent_name=agent_name,
                parent_task_id=parent_task_id,
            )
            if existing is None:
                try:
                    record = self._control._task_manager.create_task_record(
                        task_id,
                        agent_name,
                        parent_task_id=parent_task_id,
                        root_task_id=root_task_id,
                        depth=depth,
                        route_kind=(
                            "remote_ref"
                            if isinstance(entry, RemoteRefEntry)
                            else "local"
                        ),
                        parent_thread_id=parent_thread_id,
                        webhook=webhook,
                        delegation_context=task_context,
                        permission_profile=permission_profile,
                        effective_skill_names=effective_skill_names,
                        skill_view_path=skill_view_path,
                        skill_view_hash=skill_view_hash,
                    )
                except StoredTaskAlreadyExistsError:
                    record = self._control._existing_idempotent_task(
                        task_id=task_id,
                        agent_name=agent_name,
                        entry=entry,
                        parent_task_id=parent_task_id,
                        root_task_id=root_task_id,
                        depth=depth,
                        delegation_context=task_context,
                    )
                    if record is None:
                        raise RuntimeError(
                            f"Persisted Task disappeared during create: {task_id}"
                        ) from None
                    if not (
                        isinstance(entry, RemoteRefEntry)
                        and record.upstream_task_id is None
                    ):
                        return record
            else:
                record = existing
            if isinstance(entry, RemoteRefEntry):
                operation = await self._control._run_supervisor.promote_to_operation(
                    permit
                )
                try:
                    return await self._control._allocate_remote_task(
                        record=record,
                        entry=entry,
                        input_content=task,
                        delegation_context=task_context,
                        metadata=metadata,
                        attachments=attachments,
                        idempotency_key=idempotency_key or task_id,
                    )
                finally:
                    await self._control._run_supervisor.release_operation(operation)

            await self._control._start_run(task_id, task, permit=permit)
            return self._control._task_manager.get_task(task_id)
        finally:
            await self._control._run_supervisor.release_mutation(permit)
            budget_lock.release()

    async def send_task_input(
        self,
        task_id: str,
        message: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        mailbox_message_id: str | None = None,
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
        permit = await self._control._run_supervisor.acquire_mutation()
        wake_mailbox = False
        try:
            record = self._control._task_manager.get_task(task_id)
            if record.route_kind == "remote_ref":
                operation = await self._control._run_supervisor.promote_to_operation(
                    permit
                )
                try:
                    if record.state in ACTIVE_TASK_STATES:
                        record = await self._control._refresh_remote_task_with_retries(
                            task_id
                        )
                    return await self._control._send_remote_task_input(
                        record,
                        message=message,
                        attachments=attachments,
                        idempotency_key=idempotency_key,
                    )
                finally:
                    await self._control._run_supervisor.release_operation(operation)
            if record.state == "waiting_for_human":
                raise TaskAlreadyRunningError(
                    f"Worker task is waiting for review: {task_id}"
                )
            if record.state not in ACTIVE_TASK_STATES | RESUMABLE_TASK_STATES:
                raise ValueError(
                    f"Task '{task_id}' cannot receive input in state={record.state}"
                )
            if idempotency_key is not None and (
                self._control._mailbox is None or not self._control._mailbox.is_durable
            ):
                raise DurableTaskMailboxRequiredError(
                    "Idempotent local input requires a durable Task Mailbox"
                )
            if self._control._mailbox is None:
                if record.state in ACTIVE_TASK_STATES:
                    raise TaskAlreadyRunningError(
                        f"Worker task is already running: {task_id}"
                    )
                await self._control._start_run(task_id, message, permit=permit)
                return self._control._task_manager.get_task(task_id)

            self._control._mailbox.publish_input(
                recipient_task_id=record.task_id,
                recipient_thread_id=record.thread_id,
                content=message,
                trigger_run=True,
                idempotency_key=idempotency_key,
                message_id=mailbox_message_id,
            )
            wake_mailbox = True
        finally:
            await self._control._run_supervisor.release_mutation(permit)
        # Waking can take a scheduling lock, so it begins a fresh admission
        # after the durable publish critical section has been released.
        if wake_mailbox:
            return await self._control._ensure_task_awake(task_id)
        raise AssertionError("Local mailbox input did not select a wakeup path")

    async def cancel_task(self, task_id: str) -> TaskRecord:
        """
        取消已有 Task 当前正在执行或等待审批的 run

        本地 Task 会取消活跃 asyncio task；远端 Task 会通过 A2AClient 转发
        取消请求。Task 已 settled 时保持原状态，长期会话仍可继续输入。

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            最新任务记录

        Raises:
            UnknownWorkerTaskError: task_id 不存在
            A2AClientError: 远端取消失败
        """
        # 为什么提供结构化取消接口：HTTP 层要返回最新 task 视图，而不是人类可读文本。
        permit = await self._control._run_supervisor.acquire_mutation()
        operation = None
        try:
            record = self._control._task_manager.get_task(task_id)
            if record.route_kind == "remote_ref":
                if record.state not in ACTIVE_TASK_STATES:
                    return record
                operation = await self._control._run_supervisor.promote_to_operation(
                    permit
                )
                try:
                    return await self._control._cancel_remote_task(record)
                finally:
                    await self._control._run_supervisor.release_operation(operation)
            run_task = self._control._run_supervisor.get_run(task_id)
            if run_task is None or run_task.done():
                if record.state in ACTIVE_TASK_STATES:
                    self._control._task_manager.mark_cancelled(task_id)
                return self._control._task_manager.get_task(task_id)
            requested = self._control._task_manager.request_cancel(task_id)
            assert requested is run_task
            operation = await self._control._run_supervisor.promote_to_operation(permit)
        finally:
            await self._control._run_supervisor.release_mutation(permit)
        try:
            await run_task
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
        finally:
            assert operation is not None
            await self._control._run_supervisor.release_operation(operation)
        return self._control._task_manager.get_task(task_id)
