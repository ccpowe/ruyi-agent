"""Compatibility facade for the delegation runtime.

AgentControl intentionally owns only dependency assembly and stable API
forwarding. Concrete behavior lives in focused components:

- AgentRegistry: target namespace and lookup
- TaskManager: authoritative TaskRecord lifecycle state
- LocalTaskExecutor: local agent compilation and run lifecycle
- RemoteTaskPort: A2A and webhook transport
- DelegationPolicy: task-tree context and budgets
- SettledRunNotifier: mailbox delivery policy
- TaskRuntime: structured Gateway-facing use cases
- DelegationTools: scoped model-facing tool adapter
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any

import httpx
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import StructuredTool

from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError
from ruyi_agent.runtime.agent_factory import create_runtime_agent
from ruyi_agent.runtime.delegation.context import DelegationContext, validate_node_id
from ruyi_agent.runtime.delegation.contracts import (
    DurableTaskMailboxRequiredError,
    ListAgentsSchema,
    MaxDelegationDepthError,
    RemoteExecutorNotImplementedError,
    SPAWN_AGENT_TOOL_DESCRIPTION,
    SendInputSchema,
    SpawnAgentSchema,
    TaskAlreadyRunningError,
    TaskIdSchema,
    UnavailableAgentTargetError,
    UnknownAgentTargetError,
    UnknownWorkerTaskError,
    _format_exception_summary,
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
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.delegation.task_runtime import TaskRuntime
from ruyi_agent.runtime.delegation.tools import DelegationTools
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.message_history import (
    TaskMessageSnapshot,
    TaskMessageStateReader,
)
from ruyi_agent.runtime.skills.sync import SkillSyncer
from ruyi_agent.runtime.skills.types import SkillEntry
from ruyi_agent.runtime.task_events import TaskEventSubscription
from ruyi_agent.storage.review_audit import ReviewAuditStore
from ruyi_agent.storage.task_store import (
    TaskRootBudgetExceededError as MaxTasksPerRootError,
    TaskStore,
)
from ruyi_agent.task_models import (
    MetadataScalar,
    PendingReviewRecord,
    PublishedArtifact,
    TaskRecord,
)


class AgentControl:
    """Thin compatibility facade over focused delegation runtime components.

    The facade preserves the pre-refactor import path, constructor, public
    signatures, and selected private seams used by runtime integration tests.
    It owns no task policy: each method forwards to exactly one component.
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
        unavailable_agents: dict[str, str] | None = None,
    ) -> None:
        if max_delegation_depth < 1:
            raise ValueError("max_delegation_depth must be at least 1")
        if max_tasks_per_root < 1:
            raise ValueError("max_tasks_per_root must be at least 1")

        self._registry = AgentRegistry(specs, remote_refs, unavailable_agents)
        if (
            task_store is not None
            and mailbox is not None
            and not mailbox.shares_database(task_store.db_path)
        ):
            raise RuntimeError(
                "Durable Task settlement requires TaskStore and MailboxStore "
                "to share one SQLite database; separate databases, an in-memory "
                "AgentMailbox, and independent ':memory:' connections are unsafe"
            )
        settled_outbox_enabled = task_store is not None and mailbox is not None
        self._task_manager = TaskManager(
            task_store,
            settled_outbox_enabled=settled_outbox_enabled,
        )
        self._checkpointer = checkpointer
        self._message_state_reader = TaskMessageStateReader(checkpointer)
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
        self._task_input_locks: dict[str, asyncio.Lock] = {}
        self._mailbox_recovery_task: asyncio.Task[None] | None = None
        self._permission_default_profile = permission_default_profile
        self._permission_policy = permission_policy
        self._backend_kind = backend_kind
        self._workspace_root = workspace_root
        self._review_audit_store = review_audit_store
        self._skill_catalog = skill_catalog or {}
        self._skill_syncer = skill_syncer

        # Preserve monkeypatch seams from this compatibility module while
        # keeping implementation dependencies explicit in their components.
        self._agent_factory = create_runtime_agent
        self._httpx = httpx

        self._local_executor = LocalTaskExecutor(self)
        self._remote_port = RemoteTaskPort(self)
        self._delegation_policy = DelegationPolicy(self)
        self._settled_notifier = SettledRunNotifier(self)
        self._task_runtime = TaskRuntime(self)
        self._delegation_tools = DelegationTools(self)

    @property
    def workspace_root(self) -> str:
        return self._workspace_root

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[Any]:
        return self._backend.upload_files(files)

    def download_files(self, paths: list[str]) -> list[Any]:
        return self._backend.download_files(paths)

    # Local execution boundary.
    def register_artifact(
        self, *, task_id: str, artifact: dict[str, Any]
    ) -> dict[str, Any]:
        return self._local_executor.register_artifact(
            task_id=task_id, artifact=artifact
        )

    def _resolve_task_skill_view(
        self, entry: RegisteredAgent, *, parent_task_id: str | None
    ) -> tuple[tuple[str, ...], str | None, str | None]:
        return self._local_executor._resolve_task_skill_view(
            entry, parent_task_id=parent_task_id
        )

    def _get_or_create_agent(self, agent_name: str) -> Any:
        return self._local_executor._get_or_create_agent(agent_name)

    def _build_agent_run_config(self, record: TaskRecord) -> dict[str, Any]:
        return self._local_executor._build_agent_run_config(record)

    def _audit_task_review(
        self,
        event_type: str,
        record: TaskRecord,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self._local_executor._audit_task_review(event_type, record, payload=payload)

    async def _run_agent_payload(self, task_id: str, payload: Any) -> None:
        await self._local_executor._run_agent_payload(task_id, payload)

    async def _run_agent_turn(self, task_id: str, user_input: str) -> None:
        await self._local_executor._run_agent_turn(task_id, user_input)

    def _start_run(self, task_id: str, user_input: str) -> None:
        self._local_executor._start_run(task_id, user_input)

    def _start_mailbox_run(self, task_id: str) -> None:
        self._local_executor._start_mailbox_run(task_id)

    def _attach_mailbox_wakeup(
        self, task_id: str, run_task: asyncio.Task[None]
    ) -> None:
        self._local_executor._attach_mailbox_wakeup(task_id, run_task)

    async def _ensure_task_awake(self, task_id: str) -> TaskRecord:
        return await self._local_executor._ensure_task_awake(task_id)

    async def wake_pending_mailbox_tasks(self) -> None:
        await self._settled_notifier.reconcile()
        await self._local_executor.wake_pending_mailbox_tasks()

    def start_mailbox_recovery(self) -> None:
        self._settled_notifier.start()
        self._local_executor.start_mailbox_recovery()

    async def close(self) -> None:
        await self._local_executor.close()
        await self._settled_notifier.close()

    def _resume_run(self, task_id: str, decisions: list[dict[str, Any]]) -> None:
        self._local_executor._resume_run(task_id, decisions)

    # Delegation policy boundary.
    def _extract_parent_thread_id(self, config: RunnableConfig | None) -> str | None:
        return self._delegation_policy._extract_parent_thread_id(config)

    def _extract_parent_task_record(
        self, config: RunnableConfig | None
    ) -> TaskRecord | None:
        return self._delegation_policy._extract_parent_task_record(config)

    def _resolve_task_tree_context(
        self,
        *,
        task_id: str,
        parent_task_id: str | None,
        delegation_context: DelegationContext | None,
    ) -> tuple[str, int, DelegationContext]:
        return self._delegation_policy._resolve_task_tree_context(
            task_id=task_id,
            parent_task_id=parent_task_id,
            delegation_context=delegation_context,
        )

    def _delegation_context_from_record(self, record: TaskRecord) -> DelegationContext:
        return self._delegation_policy._delegation_context_from_record(record)

    def _resolve_permission_profile(
        self, *, entry: RegisteredAgent, parent_task_id: str | None
    ) -> str:
        return self._delegation_policy._resolve_permission_profile(
            entry=entry, parent_task_id=parent_task_id
        )

    def _enforce_delegation_depth(self, *, depth: int, max_depth: int) -> None:
        self._delegation_policy._enforce_delegation_depth(
            depth=depth, max_depth=max_depth
        )

    def _get_root_budget_lock(self, root_task_id: str) -> asyncio.Lock:
        return self._delegation_policy._get_root_budget_lock(root_task_id)

    def _format_depth_limit_error(self, exc: MaxDelegationDepthError) -> str:
        return self._delegation_policy._format_depth_limit_error(exc)

    def _format_task_budget_error(self, exc: MaxTasksPerRootError) -> str:
        return self._delegation_policy._format_task_budget_error(exc)

    # Settled notification boundary.
    def _is_settled_record(self, record: TaskRecord) -> bool:
        return self._settled_notifier._is_settled_record(record)

    def _maybe_publish_settled_message(self, task_id: str) -> None:
        self._settled_notifier._maybe_publish_settled_message(task_id)

    def _suppress_mailbox_delivery(self, record: TaskRecord) -> None:
        self._settled_notifier._suppress_mailbox_delivery(record)

    # Remote transport boundary.
    def _get_remote_entry_for_task(self, task_id: str) -> RemoteRefEntry:
        return self._remote_port._get_remote_entry_for_task(task_id)

    async def _refresh_remote_task(self, task_id: str) -> TaskRecord:
        return await self._remote_port._refresh_remote_task(task_id)

    async def _refresh_remote_task_with_retries(self, task_id: str) -> TaskRecord:
        return await self._remote_port._refresh_remote_task_with_retries(task_id)

    def _format_remote_status_unavailable(
        self, record: TaskRecord, exc: BaseException
    ) -> str:
        return self._remote_port._format_remote_status_unavailable(record, exc)

    def _build_webhook_config(self) -> dict[str, Any] | None:
        return self._remote_port._build_webhook_config()

    async def _send_settled_webhook(self, task_id: str) -> None:
        await self._remote_port._send_settled_webhook(task_id)

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool:
        return await self._remote_port.handle_remote_task_event(payload)

    async def list_remote_task_messages(
        self, task_id: str, *, cursor: str | None, limit: int
    ) -> dict[str, Any]:
        return await self._remote_port.list_remote_task_messages(
            task_id, cursor=cursor, limit=limit
        )

    def open_remote_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AbstractAsyncContextManager[Any]:
        return self._remote_port.open_remote_task_event_stream(
            task_id, run_count=run_count, last_event_id=last_event_id
        )

    def ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None = None,
    ) -> TaskRecord:
        return self._remote_port.ensure_remote_task_record(
            agent_name=agent_name,
            task_id=task_id,
            upstream_task_id=upstream_task_id,
            webhook=webhook,
        )

    async def refresh_task(self, task_id: str) -> TaskRecord:
        return await self._remote_port.refresh_task(task_id)

    async def _allocate_remote_task(
        self,
        *,
        record: TaskRecord,
        entry: RemoteRefEntry,
        input_content: str,
        delegation_context: DelegationContext,
        metadata: dict[str, Any] | None,
        attachments: list[dict[str, Any]] | None,
        idempotency_key: str,
    ) -> TaskRecord:
        return await self._remote_port.allocate_task(
            record=record,
            entry=entry,
            input_content=input_content,
            delegation_context=delegation_context,
            metadata=metadata,
            attachments=attachments,
            idempotency_key=idempotency_key,
        )

    async def _submit_remote_review_decision(
        self,
        record: TaskRecord,
        *,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskRecord:
        return await self._remote_port.submit_review_decision(
            record, review_id=review_id, decisions=decisions
        )

    async def _send_remote_task_input(
        self,
        record: TaskRecord,
        *,
        message: str,
        attachments: list[dict[str, Any]] | None,
        idempotency_key: str | None,
    ) -> TaskRecord:
        return await self._remote_port.send_input(
            record,
            message=message,
            attachments=attachments,
            idempotency_key=idempotency_key,
        )

    async def _cancel_remote_task(self, record: TaskRecord) -> TaskRecord:
        return await self._remote_port.cancel(record)

    # Structured task runtime boundary.
    def list_registered_agents_snapshot(self) -> list[RegisteredAgent]:
        return self._task_runtime.list_registered_agents_snapshot()

    def load_tasks_for_thread(self, thread_id: str) -> None:
        self._task_runtime.load_tasks_for_thread(thread_id)

    def get_registered_agent(self, agent_name: str) -> RegisteredAgent:
        return self._task_runtime.get_registered_agent(agent_name)

    def get_task_record(self, task_id: str) -> TaskRecord:
        return self._task_runtime.get_task_record(task_id)

    def get_live_run(self, task_id: str) -> asyncio.Task[None] | None:
        return self._task_runtime.get_live_run(task_id)

    def open_local_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> TaskEventSubscription:
        return self._task_runtime.open_local_task_event_stream(
            task_id, run_count=run_count, last_event_id=last_event_id
        )

    async def get_local_task_message_snapshot(
        self, task_id: str, *, checkpoint_id: str | None = None
    ) -> TaskMessageSnapshot:
        return await self._task_runtime.get_local_task_message_snapshot(
            task_id, checkpoint_id=checkpoint_id
        )

    def list_task_records(self) -> list[TaskRecord]:
        return self._task_runtime.list_task_records()

    def list_persisted_task_records(self) -> list[TaskRecord]:
        return self._task_runtime.list_persisted_task_records()

    def list_pending_review_records(self) -> list[TaskRecord]:
        return self._task_runtime.list_pending_review_records()

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        return self._task_runtime.list_pending_reviews(
            root_task_id=root_task_id, task_id=task_id
        )

    def get_pending_review(self, review_id: str) -> PendingReviewRecord:
        return self._task_runtime.get_pending_review(review_id)

    def get_task_by_review_id(self, review_id: str) -> TaskRecord:
        return self._task_runtime.get_task_by_review_id(review_id)

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
        *,
        wait: bool = False,
    ) -> TaskRecord:
        return await self._task_runtime.submit_review_decision(
            review_id, decisions, wait=wait
        )

    def prepare_delegation_metadata(
        self, metadata: dict[str, MetadataScalar]
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
        return self._task_runtime.prepare_delegation_metadata(metadata)

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
        return self._task_runtime._existing_idempotent_task(
            task_id=task_id,
            agent_name=agent_name,
            entry=entry,
            parent_task_id=parent_task_id,
            root_task_id=root_task_id,
            depth=depth,
            delegation_context=delegation_context,
        )

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
        return await self._task_runtime.spawn_task(
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
        )

    def remote_create_idempotency_guaranteed(self, agent_name: str) -> bool:
        """Return the declared create replay contract for a remote target."""

        entry = self._registry.get_entry(agent_name)
        return (
            isinstance(entry, RemoteRefEntry)
            and entry.ref.create_idempotency_guaranteed
        )

    async def send_task_input(
        self,
        task_id: str,
        message: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        mailbox_message_id: str | None = None,
    ) -> TaskRecord:
        return await self._task_runtime.send_task_input(
            task_id,
            message,
            attachments=attachments,
            idempotency_key=idempotency_key,
            mailbox_message_id=mailbox_message_id,
        )

    async def cancel_task(self, task_id: str) -> TaskRecord:
        return await self._task_runtime.cancel_task(task_id)

    # Model-facing delegation tools boundary.
    def _format_task_record(self, record: TaskRecord) -> str:
        return self._delegation_tools._format_task_record(record)

    async def _resolve_pending_reviews_from_config(
        self, config: RunnableConfig | None
    ) -> bool:
        return await self._delegation_tools._resolve_pending_reviews_from_config(config)

    def _allowed_targets_for_agent(self, agent_name: str) -> set[str]:
        return self._delegation_tools._allowed_targets_for_agent(agent_name)

    def build_tools_for(self, agent_name: str) -> list[StructuredTool]:
        return self._delegation_tools.build_tools_for(agent_name)

    def build_tools(self) -> list[StructuredTool]:
        return self._delegation_tools.build_tools()

    def _build_tools(
        self,
        *,
        allowed_targets: set[str] | None = None,
        caller_agent_name: str | None = None,
        enabled_tools: frozenset[str] | None = None,
    ) -> list[StructuredTool]:
        return self._delegation_tools._build_tools(
            allowed_targets=allowed_targets,
            caller_agent_name=caller_agent_name,
            enabled_tools=enabled_tools,
        )

    async def spawn_agent(
        self,
        agent_name: str,
        task: str,
        config: RunnableConfig = None,  # type: ignore[assignment]
    ) -> str:
        return await self._delegation_tools.spawn_agent(agent_name, task, config)

    async def wait_agent(
        self,
        task_id: str,
        config: RunnableConfig = None,  # type: ignore[assignment]
    ) -> str:
        return await self._delegation_tools.wait_agent(task_id, config)

    async def check_agent(
        self,
        task_id: str,
        config: RunnableConfig = None,  # type: ignore[assignment]
    ) -> str:
        return await self._delegation_tools.check_agent(task_id, config)

    async def send_input(self, task_id: str, message: str) -> str:
        return await self._delegation_tools.send_input(task_id, message)

    async def cancel_agent(self, task_id: str) -> str:
        return await self._delegation_tools.cancel_agent(task_id)

    async def list_agents(self) -> str:
        return await self._delegation_tools.list_agents()

    def _format_agents_and_tasks(
        self,
        allowed_targets: set[str] | None = None,
        visible_task_ids: set[str] | None = None,
    ) -> str:
        return self._delegation_tools._format_agents_and_tasks(
            allowed_targets, visible_task_ids
        )

    def _format_registered_agent(self, entry: RegisteredAgent) -> str:
        return self._delegation_tools._format_registered_agent(entry)


__all__ = [
    "A2AClientError",
    "AgentControl",
    "AgentRegistry",
    "DurableTaskMailboxRequiredError",
    "ListAgentsSchema",
    "LocalWorkerEntry",
    "MaxDelegationDepthError",
    "MaxTasksPerRootError",
    "PublishedArtifact",
    "RegisteredAgent",
    "RemoteExecutorNotImplementedError",
    "RemoteRefEntry",
    "TaskAlreadyRunningError",
    "TaskIdSchema",
    "TaskManager",
    "TaskRecord",
    "UnavailableAgentTargetError",
    "UnknownAgentTargetError",
    "UnknownWorkerTaskError",
    "SPAWN_AGENT_TOOL_DESCRIPTION",
    "SendInputSchema",
    "SpawnAgentSchema",
    "_format_exception_summary",
]
