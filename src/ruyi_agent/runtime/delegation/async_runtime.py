"""Composition root for the delegation runtime."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Any
import uuid

import httpx as httpx_module

import ruyi_agent.config.loader as config_loader
import ruyi_agent.control_plane.permissions as permissions_module
import ruyi_agent.integrations.a2a.client as a2a_module
import ruyi_agent.runtime.agent_factory as agent_factory_module
import ruyi_agent.runtime.delegation.context as context_module
import ruyi_agent.runtime.delegation.local_executor as local_module
import ruyi_agent.runtime.delegation.notifications as notifications_module
import ruyi_agent.runtime.delegation.policy as policy_module
import ruyi_agent.runtime.delegation.registry as registry_module
import ruyi_agent.runtime.delegation.remote_port as remote_module
import ruyi_agent.runtime.delegation.run_supervisor as supervisor_module
import ruyi_agent.runtime.delegation.task_manager as manager_module
import ruyi_agent.runtime.delegation.task_runtime as runtime_module
import ruyi_agent.runtime.delegation.tools as tools_module
import ruyi_agent.runtime.mailbox.service as mailbox_module
import ruyi_agent.runtime.message_history as message_module
import ruyi_agent.runtime.skills.sync as skill_sync_module
import ruyi_agent.runtime.skills.types as skill_types_module
import ruyi_agent.runtime.task_events as events_module
import ruyi_agent.storage.review_audit as review_module
import ruyi_agent.storage.task_store as store_module
import ruyi_agent.task_models as models_module


# fmt: off
def _compose_task_runtime(specs: dict[str, config_loader.LocalWorkerSpec], remote_refs: dict[str, config_loader.RemoteRef] | None, checkpointer: Any, backend: Any, a2a_client: a2a_module.A2AClient | None, mailbox: mailbox_module.AgentMailbox | None, webhook_url: str | None, webhook_token: str | None, remote_poll_interval: float, remote_status_retry_attempts: int, max_delegation_depth: int, max_tasks_per_root: int, node_id: str | None, task_store: store_module.TaskStore | None, permission_default_profile: str, permission_policy: permissions_module.PermissionPolicy | None, backend_kind: str, workspace_root: str, review_audit_store: review_module.ReviewAuditStore | None, skill_catalog: Mapping[str, skill_types_module.SkillEntry] | None, skill_syncer: skill_sync_module.SkillSyncer | None, unavailable_agents: dict[str, str] | None, shutdown_grace_period: float) -> runtime_module.TaskRuntime:  # fmt: skip
    registry = registry_module.AgentRegistry(specs, remote_refs, unavailable_agents)
    if task_store is not None and mailbox is not None and not mailbox.shares_database(task_store.db_path):  # fmt: skip
        raise RuntimeError("Durable Task settlement requires TaskStore and MailboxStore to share one SQLite database; separate databases, an in-memory AgentMailbox, and independent ':memory:' connections are unsafe")  # fmt: skip
    manager = manager_module.TaskManager(task_store, settled_outbox_enabled=task_store is not None and mailbox is not None)
    resolved_node_id = context_module.validate_node_id(node_id or f"node-{uuid.uuid4()}")
    policy = policy_module.DelegationPolicy(manager, node_id=resolved_node_id, max_delegation_depth=max_delegation_depth, max_tasks_per_root=max_tasks_per_root, permission_default_profile=permission_default_profile)
    supervisor = supervisor_module.RunSupervisor(manager, shutdown_grace_period=shutdown_grace_period)
    notifier = notifications_module.SettledRunNotifier(manager, mailbox)
    local_executor = local_module.LocalTaskExecutor(manager, registry=registry, agent_factory=agent_factory_module.create_runtime_agent, backend=backend, checkpointer=checkpointer, mailbox=mailbox, permission_policy=permission_policy, permission_default_profile=permission_default_profile, skill_catalog=skill_catalog or {}, skill_syncer=skill_syncer, review_audit_store=review_audit_store, backend_kind=backend_kind, workspace_root=workspace_root)  # fmt: skip
    remote_port = remote_module.RemoteTaskPort(manager, registry, a2a_client or a2a_module.A2AClient(), httpx_module=httpx_module, node_id=resolved_node_id, max_delegation_depth=max_delegation_depth, max_tasks_per_root=max_tasks_per_root, permission_default_profile=permission_default_profile, webhook_url=webhook_url, webhook_token=webhook_token)  # fmt: skip
    delegation_tools = tools_module.DelegationTools(registry, manager, policy, notifier, remote_poll_interval=remote_poll_interval)
    return runtime_module.TaskRuntime(registry, manager, policy, supervisor, local_executor, remote_port, notifier, delegation_tools, mailbox=mailbox, message_state_reader=message_module.TaskMessageStateReader(checkpointer), remote_poll_interval=remote_poll_interval, remote_status_retry_attempts=remote_status_retry_attempts)  # fmt: skip
# fmt: on


class AgentControl:
    """Compose runtime components and expose the stable Gateway API."""

    def __init__(self, specs: dict[str, config_loader.LocalWorkerSpec], remote_refs: dict[str, config_loader.RemoteRef] | None = None, *, checkpointer: Any, backend: Any, a2a_client: a2a_module.A2AClient | None = None, mailbox: mailbox_module.AgentMailbox | None = None, webhook_url: str | None = None, webhook_token: str | None = None, remote_poll_interval: float = 0.5, remote_status_retry_attempts: int = 3, max_delegation_depth: int = 3, max_tasks_per_root: int = 20, node_id: str | None = None, task_store: store_module.TaskStore | None = None, permission_default_profile: str = "", permission_policy: permissions_module.PermissionPolicy | None = None, backend_kind: str = "unknown", workspace_root: str = "", review_audit_store: review_module.ReviewAuditStore | None = None, skill_catalog: Mapping[str, skill_types_module.SkillEntry] | None = None, skill_syncer: skill_sync_module.SkillSyncer | None = None, unavailable_agents: dict[str, str] | None = None, shutdown_grace_period: float = 5.0) -> None:  # fmt: skip
        self._backend = backend
        self._workspace_root = workspace_root
        self._task_runtime = _compose_task_runtime(specs, remote_refs, checkpointer, backend, a2a_client, mailbox, webhook_url, webhook_token, remote_poll_interval, remote_status_retry_attempts, max_delegation_depth, max_tasks_per_root, node_id, task_store, permission_default_profile, permission_policy, backend_kind, workspace_root, review_audit_store, skill_catalog, skill_syncer, unavailable_agents, shutdown_grace_period)  # fmt: skip

    @property
    def workspace_root(self) -> str:
        return self._workspace_root

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[Any]:
        return self._backend.upload_files(files)

    def download_files(self, paths: list[str]) -> list[Any]:
        return self._backend.download_files(paths)

    async def wake_pending_mailbox_tasks(self) -> None:
        await self._task_runtime.wake_pending_mailbox_tasks()

    def start_mailbox_recovery(self) -> None:
        self._task_runtime.start_mailbox_recovery()

    async def close(self) -> None:
        await self._task_runtime.close()

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool:
        return await self._task_runtime.handle_remote_task_event(payload)

    async def list_remote_task_messages(
        self, task_id: str, *, cursor: str | None, limit: int
    ) -> dict[str, Any]:
        return await self._task_runtime.list_remote_task_messages(
            task_id, cursor=cursor, limit=limit
        )

    def open_remote_task_event_stream(
        self, task_id: str, *, run_count: int, last_event_id: str | None
    ) -> AbstractAsyncContextManager[Any]:
        return self._task_runtime.open_remote_task_event_stream(
            task_id, run_count=run_count, last_event_id=last_event_id
        )

    def ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None = None,
    ) -> models_module.TaskRecord:
        return self._task_runtime.ensure_remote_task_record(
            agent_name=agent_name,
            task_id=task_id,
            upstream_task_id=upstream_task_id,
            webhook=webhook,
        )

    async def refresh_task(self, task_id: str) -> models_module.TaskRecord:
        return await self._task_runtime.refresh_task(task_id)

    def get_task_record(self, task_id: str) -> models_module.TaskRecord:
        return self._task_runtime.get_task_record(task_id)

    def open_local_task_event_stream(
        self, task_id: str, *, run_count: int, last_event_id: str | None
    ) -> events_module.TaskEventSubscription:
        return self._task_runtime.open_local_task_event_stream(
            task_id, run_count=run_count, last_event_id=last_event_id
        )

    async def get_local_task_message_snapshot(
        self, task_id: str, *, checkpoint_id: str | None = None
    ) -> message_module.TaskMessageSnapshot:
        return await self._task_runtime.get_local_task_message_snapshot(
            task_id, checkpoint_id=checkpoint_id
        )

    def list_persisted_task_records(self) -> list[models_module.TaskRecord]:
        return self._task_runtime.list_persisted_task_records()

    def list_pending_reviews(
        self, *, root_task_id: str | None = None, task_id: str | None = None
    ) -> list[models_module.PendingReviewRecord]:
        return self._task_runtime.list_pending_reviews(
            root_task_id=root_task_id, task_id=task_id
        )

    def get_pending_review(self, review_id: str) -> models_module.PendingReviewRecord:
        return self._task_runtime.get_pending_review(review_id)

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
        *,
        wait: bool = False,
    ) -> models_module.TaskRecord:
        return await self._task_runtime.submit_review_decision(
            review_id, decisions, wait=wait
        )

    def prepare_delegation_metadata(
        self, metadata: dict[str, models_module.MetadataScalar]
    ) -> tuple[
        dict[str, models_module.MetadataScalar], context_module.DelegationContext | None
    ]:
        return self._task_runtime.prepare_delegation_metadata(metadata)

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
        delegation_context: context_module.DelegationContext | None = None,
    ) -> models_module.TaskRecord:
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
        return self._task_runtime.remote_create_idempotency_guaranteed(agent_name)

    async def send_task_input(
        self,
        task_id: str,
        message: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        mailbox_message_id: str | None = None,
    ) -> models_module.TaskRecord:
        return await self._task_runtime.send_task_input(
            task_id,
            message,
            attachments=attachments,
            idempotency_key=idempotency_key,
            mailbox_message_id=mailbox_message_id,
        )

    async def cancel_task(self, task_id: str) -> models_module.TaskRecord:
        return await self._task_runtime.cancel_task(task_id)


__all__ = ["AgentControl"]
