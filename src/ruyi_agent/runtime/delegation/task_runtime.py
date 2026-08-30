"""Application coordinator for local and remote Gateway Tasks."""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from langgraph.types import Command

from ruyi_agent.config.system_tools import DELEGATION_SYSTEM_TOOLS
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.agent_turn import normalize_agent_turn
from ruyi_agent.runtime.delegation.context import DelegationContext
from ruyi_agent.runtime.delegation.contracts import (
    DurableTaskMailboxRequiredError,
    TaskAlreadyRunningError,
    UnknownWorkerTaskError,
    _format_exception_summary,
    _format_interrupted_error,
)
from ruyi_agent.runtime.delegation.local_executor import LocalTaskExecutor
from ruyi_agent.runtime.delegation.notifications import SettledRunNotifier
from ruyi_agent.runtime.delegation.policy import DelegationPolicy
from ruyi_agent.runtime.delegation.registry import (
    AgentRegistry,
    LocalWorkerEntry,
    RegisteredAgent,
    RemoteRefEntry,
)
from ruyi_agent.runtime.delegation.remote_port import RemoteTaskPort
from ruyi_agent.runtime.delegation.run_supervisor import (
    RunCompletionPort,
    RunSupervisor,
    RuntimeClosingError,
    _MutationPermit,
    _OperationPermit,
)
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.delegation.tools import DelegationTools, TaskCommandPort
from ruyi_agent.runtime.message_history import (
    TaskMessageSnapshot,
    TaskMessageStateReader,
)
from ruyi_agent.runtime.task_events import (
    TaskEventSubscription,
    TaskEventsUnavailableError,
)
from ruyi_agent.storage.task_store import (
    StoredTaskAlreadyExistsError,
)
from ruyi_agent.task_models import (
    ACTIVE_TASK_STATES,
    RESUMABLE_TASK_STATES,
    SETTLED_TASK_STATES,
    MetadataScalar,
    PendingReviewRecord,
    TaskRecord,
)

logger = logging.getLogger(__name__)


# fmt: off
class TaskRuntime(TaskCommandPort, RunCompletionPort):
    def __init__(self, registry: AgentRegistry, task_manager: TaskManager, policy: DelegationPolicy, supervisor: RunSupervisor, local_executor: LocalTaskExecutor, remote_port: RemoteTaskPort, notifier: SettledRunNotifier, tools: DelegationTools, *, mailbox: Any, message_state_reader: TaskMessageStateReader, remote_poll_interval: float = 0.5, remote_status_retry_attempts: int = 3) -> None:  # fmt: skip
        self._registry, self._task_manager, self._policy, self._supervisor = registry, task_manager, policy, supervisor
        self._local_executor, self._remote_port, self._notifier, self._tools = local_executor, remote_port, notifier, tools
        self._mailbox, self._message_state_reader = mailbox, message_state_reader
        self._remote_poll_interval, self._remote_status_retry_attempts = remote_poll_interval, max(remote_status_retry_attempts, 1)
        self._compiled_agents, self._task_input_locks, self._recovery_started = {}, {}, False

    def get_task_record(self, task_id: str) -> TaskRecord:
        """
        获取指定任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            对应的任务记录
        """
        return self._task_manager.get_task(task_id)

    def open_local_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> TaskEventSubscription:
        """Open one fixed-run stream without taking ownership of the Agent run."""

        record = self._task_manager.get_task(task_id)
        if record.route_kind != "local":
            raise ValueError(f"Task '{task_id}' is not a local task")
        ledger = self._task_manager.event_ledger
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

        record = self._task_manager.get_task(task_id)
        if record.route_kind != "local":
            raise ValueError(f"Task '{task_id}' is not a local task")
        return await self._message_state_reader.read(
            thread_id=record.thread_id,
            checkpoint_id=checkpoint_id,
        )

    def list_persisted_task_records(self) -> list[TaskRecord]:
        """List runtime and persisted tasks for Gateway discovery."""

        return self._task_manager.list_persisted_tasks()

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        """Enumerate authoritative pending Review resources."""

        return self._task_manager.list_pending_reviews(
            root_task_id=root_task_id,
            task_id=task_id,
        )

    def get_pending_review(self, review_id: str) -> PendingReviewRecord:
        """Resolve one authoritative pending Review resource."""

        review = self._task_manager.get_pending_review(review_id)
        if review is None:
            raise UnknownWorkerTaskError(f"Unknown pending review: {review_id}")
        return review

    def _get_or_create_agent(self, agent_name: str) -> Any:
        if (cached := self._compiled_agents.get(agent_name)) is not None:
            return cached
        spec = self._registry.get_spec(agent_name)
        targets = spec.delegation_targets
        has_tools = bool(targets) if spec.system_tools is None else bool(spec.system_tools & DELEGATION_SYSTEM_TOOLS)
        worker_tools = self._tools.build_tools_for(agent_name, command_port=self) if has_tools else None  # fmt: skip
        agent = self._local_executor.compile_agent(agent_name, worker_tools=worker_tools)
        self._compiled_agents[agent_name] = agent
        return agent

    async def _run_agent_payload(self, task_id: str, payload: Any) -> None:
        try:
            record = self._task_manager.get_task(task_id)
            agent = self._get_or_create_agent(record.agent_name)
            run_config = self._local_executor.build_run_config(record)
            result = await self._local_executor.execute(task_id, agent, payload, run_config, record)  # fmt: skip
            outcome = await normalize_agent_turn(agent, run_config, result)
            if outcome.review_payloads and len(outcome.review_payloads) == 1:
                self._task_manager.mark_waiting_for_human(task_id, outcome.review_payloads[0])  # fmt: skip
                try:
                    self._local_executor.audit_task_review("task_waiting_for_human", self._task_manager.get_task(task_id), payload=outcome.review_payloads[0])  # fmt: skip
                except Exception:
                    logger.exception("Non-authoritative review audit failed after review commit: %s", task_id)
                return
            if len(outcome.review_payloads) > 1:
                self._task_manager.mark_failed(task_id, "Worker produced multiple simultaneous human review requests.")  # fmt: skip
            elif outcome.has_unresolved_tool_calls:
                self._task_manager.mark_failed(task_id, "Worker stopped before resolving pending tool calls.")  # fmt: skip
            else:
                self._task_manager.mark_completed(task_id, outcome.content or "Task completed, but the final assistant reply was empty.")  # fmt: skip
            await self._after_settled(task_id)
        except asyncio.CancelledError as exc:
            explicit_cancel = self._finalize_cancelled_run(task_id, exc)
            if explicit_cancel:
                await self._remote_port.send_settled_webhook(task_id)
            raise
        except Exception as exc:
            current = self._task_manager.get_task(task_id)
            if current.state == "running":
                self._task_manager.mark_failed(task_id, _format_exception_summary(exc))
                await self._after_settled(task_id)
            else:
                logger.exception("Non-authoritative local run tail failed: %s", task_id)

    async def _after_settled(self, task_id: str) -> None:
        wake_ids = self._notifier.publish_settled_message(task_id)
        await self._remote_port.send_settled_webhook(task_id)
        await self._wake_tasks(wake_ids)

    def _finalize_cancelled_run(self, task_id: str, error: asyncio.CancelledError) -> bool:  # fmt: skip
        try:
            if self._task_manager.get_task(task_id).state != "running":
                return False
            explicit_cancel = self._task_manager.was_cancel_requested(task_id)
            if explicit_cancel:
                self._task_manager.mark_cancelled(task_id)
            else:
                self._task_manager.mark_interrupted(task_id, _format_interrupted_error(error))  # fmt: skip
            wake_ids = self._notifier.publish_settled_message(task_id)
            if wake_ids and self._supervisor.is_accepting:
                self._supervisor.start_maintenance(
                    lambda ids=tuple(wake_ids): self._wake_tasks(ids),
                    name=f"mailbox-wakeup:{task_id}",
                )
            return explicit_cancel
        except Exception:
            logger.exception("Failed to finalize cancelled local run: %s", task_id)
            return False

    async def _start_run(
        self,
        task_id: str,
        user_input: str,
        *,
        permit: _MutationPermit | None = None,
    ) -> asyncio.Task[None]:
        return await self._supervisor.schedule(task_id, lambda: self._run_agent_payload(task_id, {"messages": [{"role": "user", "content": user_input}]}), completion_port=self, permit=permit)  # fmt: skip

    async def _ensure_task_awake(self, task_id: str) -> TaskRecord:  # fmt: skip
        lock = self._task_input_locks.setdefault(task_id, asyncio.Lock())
        async with lock:
            record = self._task_manager.get_task(task_id)
            if not self._supervisor.is_accepting:
                return record
            if self._mailbox is None or record.route_kind != "local":
                return record
            if self._task_manager.has_active_run(task_id):
                return record
            if record.state not in RESUMABLE_TASK_STATES or not self._mailbox.has_triggering_messages(task_id):
                return record
            try:
                await self._supervisor.schedule(task_id, lambda: self._run_agent_payload(task_id, {"messages": []}), completion_port=self)  # fmt: skip
            except RuntimeClosingError:
                pass
            return self._task_manager.get_task(task_id)

    async def _wake_tasks(self, task_ids: list[str] | set[str]) -> None:  # fmt: skip
        await asyncio.gather(*(self._ensure_task_awake(task_id) for task_id in sorted(set(task_ids))))  # fmt: skip

    def on_run_finished(self, task_id: str, _run_task: asyncio.Task[None]) -> None:  # fmt: skip
        if self._supervisor.is_accepting:
            self._supervisor.start_maintenance(lambda: self._ensure_task_awake(task_id), name=f"mailbox-wakeup:{task_id}")  # fmt: skip

    async def wake_pending_mailbox_tasks(self, *, reconcile_outbox: bool = True) -> None:  # fmt: skip
        wake_ids = await self._notifier.reconcile() if reconcile_outbox else []
        if self._mailbox is not None:
            self._mailbox.recover_claims()
            wake_ids.extend(self._mailbox.pending_trigger_recipient_task_ids())
        wake_ids.extend(r.task_id for r in self._task_manager.list_persisted_tasks())  # fmt: skip
        await self._wake_tasks(wake_ids)

    def start_mailbox_recovery(self) -> None:
        if self._recovery_started or not self._supervisor.is_accepting:
            return
        self._recovery_started = True

        async def reconcile_outbox() -> None:
            while True:
                await asyncio.sleep(1)
                await self._wake_tasks(await self._notifier.reconcile())

        async def recover_mailbox() -> None:
            while True:
                await asyncio.sleep(5)
                await self.wake_pending_mailbox_tasks(reconcile_outbox=False)

        if self._task_manager.settled_outbox_enabled:
            self._supervisor.start_maintenance(
                reconcile_outbox,
                name="settled-outbox-recovery",
            )
        if self._mailbox is not None:
            self._supervisor.start_recovery(recover_mailbox)

    async def close(self) -> None:
        cancelled = False
        try:
            await self._supervisor.close()
        except asyncio.CancelledError:
            cancelled = True
        finally:
            self._compiled_agents.clear()
            self._task_input_locks.clear()
            self._recovery_started = False
            if ledger := self._task_manager.event_ledger:
                ledger.close()
        if cancelled:
            raise asyncio.CancelledError

    async def _resume_run(self, task_id: str, decisions: list[dict[str, Any]], *, permit: _MutationPermit | None = None) -> asyncio.Task[None]:  # fmt: skip
        record = self._task_manager.get_task(task_id)
        review_id = (record.pending_review or {}).get("review_id")
        run_task = await self._supervisor.schedule(task_id, lambda: self._run_agent_payload(task_id, Command(resume={"decisions": decisions})), completion_port=self, permit=permit)  # fmt: skip
        try:
            self._local_executor.audit_task_review("task_review_resumed", record, payload={"review_id": review_id, "decisions": decisions})  # fmt: skip
        except Exception:
            logger.exception(
                "Non-authoritative review audit failed after resume commit: %s",
                review_id,
            )
        return run_task

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
        *,
        wait: bool = False,
    ) -> TaskRecord:
        permit = await self._supervisor.acquire_mutation()
        task_id = ""
        run_task: asyncio.Task[None] | None = None
        try:
            review = self._task_manager.get_pending_review(review_id)
            if review is None:
                raise UnknownWorkerTaskError(f"Unknown pending review: {review_id}")
            record = self._task_manager.get_task(review.task_id)
            task_id = record.task_id
            if record.route_kind == "remote_ref":
                operation = await self._supervisor.promote_to_operation(permit)
                try:
                    updated = await self._remote_port.submit_review_decision(
                        record,
                        review_id=review_id,
                        decisions=decisions,
                    )
                    if updated.state in SETTLED_TASK_STATES:
                        await self._after_settled(task_id)
                    return updated
                finally:
                    await self._supervisor.cleanup_operation(operation)
            if record.state != "waiting_for_human" or record.pending_review is None:
                raise ValueError(f"Review '{review_id}' is not pending")
            run_task = await self._resume_run(task_id, decisions, permit=permit)
        finally:
            await self._supervisor.cleanup_mutation(permit)
        if wait and run_task is not None:
            try:
                await asyncio.shield(run_task)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if current is not None and current.cancelling():
                    raise
        return self._task_manager.get_task(task_id)

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
        return self._policy.prepare_delegation_metadata(metadata)

    def remote_create_idempotency_guaranteed(self, agent_name: str) -> bool:
        entry = self._registry.get_entry(agent_name)
        return (
            isinstance(entry, RemoteRefEntry)
            and entry.ref.create_idempotency_guaranteed
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
            record = self._task_manager.get_task(task_id)
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
        permit = await self._supervisor.acquire_mutation()
        try:
            return await self._spawn_admitted(
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
            await self._supervisor.cleanup_mutation(permit)

    async def _spawn_admitted(
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
        permit: _MutationPermit,
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
        entry = self._registry.get_entry(agent_name)
        task_id = task_id or str(uuid.uuid4())
        permission_profile = self._policy.resolve_permission_profile(
            entry=entry,
            parent_task_id=parent_task_id,
        )
        root_task_id, depth, task_context = self._policy.resolve_task_tree_context(
            task_id=task_id,
            parent_task_id=parent_task_id,
            delegation_context=delegation_context,
        )
        effective_skill_names, skill_view_path, skill_view_hash = (
            self._local_executor.resolve_task_skill_view(
                entry,
                parent_task_id=parent_task_id,
            )
        )
        budget_lock = self._policy.get_root_budget_lock(root_task_id)
        permit = await self._supervisor.wait_for_lock(permit, budget_lock)
        try:
            existing = self._existing_idempotent_task(
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
                    and not self._task_manager.has_active_run(task_id)
                ):
                    await self._start_run(task_id, task, permit=permit)
                    return self._task_manager.get_task(task_id)
                if not (
                    isinstance(entry, RemoteRefEntry)
                    and existing.upstream_task_id is None
                ):
                    return existing
            self._policy.enforce_delegation_depth(
                depth=depth,
                max_depth=task_context.max_depth,
            )
            self._registry.register_task(
                task_id,
                agent_name=agent_name,
                parent_task_id=parent_task_id,
            )
            if existing is None:
                try:
                    record = self._task_manager.create_task_record(
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
                    record = self._existing_idempotent_task(
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
                        )
                    if not (
                        isinstance(entry, RemoteRefEntry)
                        and record.upstream_task_id is None
                    ):
                        return record
            else:
                record = existing
            if isinstance(entry, RemoteRefEntry):
                effective_key = idempotency_key or task_id
                if (
                    record.external_outcome_uncertain
                    and record.external_operation == "create"
                    and record.upstream_task_id is None
                ):
                    stored_identity = record.external_operation_identity
                    if not stored_identity:
                        raise RuntimeError(
                            f"Task '{task_id}' has no reconciliation identity"
                        )
                    if (
                        idempotency_key is not None
                        and idempotency_key != stored_identity
                    ):
                        raise ValueError(
                            f"Task '{task_id}' must reuse its persisted idempotency key"
                        )
                    if entry.ref.create_idempotency != "ruyi_gateway_v1":
                        raise RuntimeError(
                            f"Remote create outcome for task '{task_id}' is uncertain; the route does not guarantee idempotent create reconciliation"
                        )
                    effective_key = stored_identity
                operation = await self._supervisor.promote_to_operation(permit)
                try:
                    updated = await self._remote_port.allocate_task(
                        record=record,
                        entry=entry,
                        input_content=task,
                        delegation_context=task_context,
                        metadata=metadata,
                        attachments=attachments,
                        idempotency_key=effective_key,
                    )
                finally:
                    await self._supervisor.cleanup_operation(operation)
                if updated.state in {"completed", "failed", "cancelled", "interrupted"}:
                    await self._after_settled(updated.task_id)
                return updated
            await self._start_run(task_id, task, permit=permit)
            return self._task_manager.get_task(task_id)
        finally:
            await self._supervisor.cleanup_mutation(permit)
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
        permit = await self._supervisor.acquire_mutation()
        wake_mailbox = False
        try:
            record = self._task_manager.get_task(task_id)
            if record.route_kind == "remote_ref":
                operation = await self._supervisor.promote_to_operation(permit)
                try:
                    if record.state in ACTIVE_TASK_STATES:
                        record = await self._refresh_remote_task_with_retries(
                            task_id,
                            permit=operation,
                        )
                    return await self._remote_port.send_input(
                        record,
                        message=message,
                        attachments=attachments,
                        idempotency_key=idempotency_key,
                    )
                finally:
                    await self._supervisor.cleanup_operation(operation)
            if record.state == "waiting_for_human":
                raise TaskAlreadyRunningError(
                    f"Worker task is waiting for review: {task_id}"
                )
            if record.state not in ACTIVE_TASK_STATES | RESUMABLE_TASK_STATES:
                raise ValueError(
                    f"Task '{task_id}' cannot receive input in state={record.state}"
                )
            if idempotency_key is not None and (
                self._mailbox is None or not self._mailbox.is_durable
            ):
                raise DurableTaskMailboxRequiredError(
                    "Idempotent local input requires a durable Task Mailbox"
                )
            if self._mailbox is None:
                if record.state in ACTIVE_TASK_STATES:
                    raise TaskAlreadyRunningError(
                        f"Worker task is already running: {task_id}"
                    )
                await self._start_run(task_id, message, permit=permit)
                return self._task_manager.get_task(task_id)
            self._mailbox.publish_input(
                recipient_task_id=record.task_id,
                recipient_thread_id=record.thread_id,
                content=message,
                trigger_run=True,
                idempotency_key=idempotency_key,
                message_id=mailbox_message_id,
            )
            wake_mailbox = True
        finally:
            await self._supervisor.cleanup_mutation(permit)
        if wake_mailbox:
            return await self._ensure_task_awake(task_id)
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
        permit = await self._supervisor.acquire_mutation()
        operation = None
        run_task: asyncio.Task[None] | None = None
        try:
            record = self._task_manager.get_task(task_id)
            if record.state not in ACTIVE_TASK_STATES:
                return record
            if record.route_kind == "remote_ref":
                operation = await self._supervisor.promote_to_operation(permit)
                try:
                    updated = await self._remote_port.cancel(record)
                    if updated.state in SETTLED_TASK_STATES:
                        await self._after_settled(updated.task_id)
                    return updated
                finally:
                    await self._supervisor.cleanup_operation(operation)
            run_task = self._supervisor.get_run(task_id)
            if run_task is None or run_task.done():
                self._task_manager.mark_cancelled(task_id)
                return self._task_manager.get_task(task_id)
            requested = self._task_manager.request_cancel(task_id)
            assert requested is run_task
            operation = await self._supervisor.promote_to_operation(permit)
        finally:
            await self._supervisor.cleanup_mutation(permit)
        try:
            await asyncio.shield(run_task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
        finally:
            assert operation is not None
            await self._supervisor.cleanup_operation(operation)
        return self._task_manager.get_task(task_id)

    async def _refresh_remote_task_with_retries(self, task_id: str, *, permit: _OperationPermit | None = None) -> TaskRecord:  # fmt: skip
        owns_permit = permit is None
        mutation: _MutationPermit | None = None
        if owns_permit:
            mutation = await self._supervisor.acquire_mutation()
            try:
                permit = await self._supervisor.promote_to_operation(mutation)
            except BaseException:
                await self._supervisor.cleanup_mutation(mutation)
                raise
        try:
            for attempt in range(self._remote_status_retry_attempts):
                try:
                    return await self._remote_port.refresh_once(task_id)
                except A2AClientError:
                    if attempt + 1 >= self._remote_status_retry_attempts:
                        raise
                    await asyncio.sleep(self._remote_poll_interval)
            raise AssertionError("unreachable")
        finally:
            if owns_permit:
                assert permit is not None
                await self._supervisor.cleanup_operation(permit)

    async def refresh_task(self, task_id: str) -> TaskRecord:
        record = self._task_manager.get_task(task_id)
        if record.route_kind == "remote_ref":
            return await self._refresh_remote_task_with_retries(task_id)
        return record

    def ensure_remote_task_record(self, *, agent_name: str, task_id: str, upstream_task_id: str, webhook: dict[str, Any] | None = None) -> TaskRecord:  # fmt: skip
        return self._supervisor.mutate_now(
            lambda: self._remote_port.ensure_remote_task_record(
                agent_name=agent_name,
                task_id=task_id,
                upstream_task_id=upstream_task_id,
                webhook=webhook,
            )
        )

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool:  # fmt: skip
        permit = await self._supervisor.acquire_mutation()
        wake_ids: list[str] = []
        try:
            synced = await self._remote_port.handle_remote_task_event(payload)
            if synced is None:
                return False
            if synced.state in SETTLED_TASK_STATES:
                wake_ids = self._notifier.publish_settled_message(synced.task_id)
                operation = await self._supervisor.promote_to_operation(permit)
                try:
                    await self._remote_port.send_settled_webhook(synced.task_id)
                finally:
                    await self._supervisor.cleanup_operation(operation)
            return True
        finally:
            await self._supervisor.cleanup_mutation(permit)
            await self._wake_tasks(wake_ids)

    async def list_remote_task_messages(self, task_id: str, *, cursor: str | None, limit: int) -> dict[str, Any]:  # fmt: skip
        permit = await self._supervisor.acquire_mutation()
        operation = await self._supervisor.promote_to_operation(permit)
        try:
            return await self._remote_port.list_remote_task_messages(
                task_id,
                cursor=cursor,
                limit=limit,
            )
        finally:
            await self._supervisor.cleanup_operation(operation)

    def open_remote_task_event_stream(self, task_id: str, *, run_count: int, last_event_id: str | None) -> AbstractAsyncContextManager[Any]:  # fmt: skip
        @asynccontextmanager
        async def admitted_stream() -> Any:
            permit = await self._supervisor.acquire_mutation()
            operation = await self._supervisor.promote_to_operation(permit)
            try:
                async with self._remote_port.open_remote_task_event_stream(
                    task_id,
                    run_count=run_count,
                    last_event_id=last_event_id,
                ) as stream:
                    yield stream
            finally:
                await self._supervisor.cleanup_operation(operation)

        return admitted_stream()
# fmt: on
