"""Local worker compilation and run execution boundary.

LocalTaskExecutor owns the backend-facing artifact boundary and one local model
invocation. Agent compilation, caching, run admission, and post-run ordering
belong to TaskRuntime; this component receives an explicit compiled agent and
payload for every invocation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
import uuid
from typing import Any

from langgraph.types import GraphOutput

from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.runtime.agent_factory import create_runtime_agent
from ruyi_agent.runtime.delegation.contracts import (
    _artifact_int,
    _artifact_optional_string,
    _artifact_string,
    _published_artifact_to_dict,
)
from ruyi_agent.runtime.delegation.registry import (
    AgentRegistry,
    LocalWorkerEntry,
    RegisteredAgent,
)
from ruyi_agent.runtime.skills.types import SkillEntry
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.skills.resolver import resolve_skill_names
from ruyi_agent.runtime.task_events import (
    assistant_delta_from_stream_part,
    normalize_task_event_text,
)
from ruyi_agent.storage.review_audit import ReviewAuditStore
from ruyi_agent.task_models import PublishedArtifact, TaskRecord

_MISSING_STREAM_VALUE = object()


class LocalTaskExecutor:
    """Execute an explicitly selected compiled local agent."""

    # fmt: off
    def __init__(self, task_manager: TaskManager, *, registry: AgentRegistry, agent_factory: Callable[..., Any] = create_runtime_agent, backend: Any, checkpointer: Any, mailbox: Any, permission_policy: PermissionPolicy | None, permission_default_profile: str, skill_catalog: Mapping[str, SkillEntry], skill_syncer: Any, review_audit_store: ReviewAuditStore | None, backend_kind: str, workspace_root: str) -> None:  # fmt: skip
        self._task_manager, self._registry, self._agent_factory = task_manager, registry, agent_factory
        self._backend, self._checkpointer, self._mailbox = backend, checkpointer, mailbox
        self._permission_policy, self._permission_default_profile = permission_policy, permission_default_profile
        self._skill_catalog, self._skill_syncer, self._review_audit_store = skill_catalog, skill_syncer, review_audit_store
        self._backend_kind, self._workspace_root = backend_kind, workspace_root
    # fmt: on

    def compile_agent(
        self,
        agent_name: str,
        *,
        worker_tools: list[Any] | None = None,
    ) -> Any:
        """Compile one local worker from immutable runtime dependencies."""
        spec = self._registry.get_spec(agent_name)
        declared_targets = spec.delegation_targets
        return self._agent_factory(model=spec.model, system_prompt=spec.system_prompt, tools=spec.tools, local_worker_specs=self._registry.select_local_specs(declared_targets), remote_refs=self._registry.select_remote_refs(declared_targets), worker_tools=worker_tools, memory=spec.memory, skills=spec.skills, backend=self._backend, checkpointer=self._checkpointer, mailbox=self._mailbox, load_tasks_for_thread=self._task_manager.load_by_parent_thread_id, permission_policy=self._permission_policy, backend_kind=self._backend_kind, workspace_root=self._workspace_root, register_artifact=self.register_artifact, permission_profile=spec.permission_profile or self._permission_default_profile, review_audit_store=self._review_audit_store, tool_search_registry=spec.tool_search_registry if spec.tool_search else None, tool_search_server_names=spec.tool_search_server_names, tool_search_tool_names=spec.tool_search_tool_names, system_tools=spec.system_tools, name=agent_name)  # fmt: skip

    # fmt: off
    def build_run_config(self, record: TaskRecord) -> dict[str, Any]:
        return {"configurable": {"thread_id": record.thread_id, "task_id": record.task_id, "parent_task_id": record.parent_task_id, "root_task_id": record.root_task_id, "delegation_depth": record.depth, "agent_name": record.agent_name, "permission_profile": record.permission_profile, "effective_skill_names": list(record.effective_skill_names), "skill_view_path": record.skill_view_path, "skill_view_hash": record.skill_view_hash, "mailbox_run_id": uuid.uuid4().hex}}  # fmt: skip
    # fmt: on

    def register_artifact(
        self,
        *,
        task_id: str,
        artifact: dict[str, Any],
    ) -> dict[str, Any]:
        record = self._task_manager.get_task(task_id)
        caption = _artifact_optional_string(artifact, "caption")
        published = PublishedArtifact(
            artifact_id=f"art_{uuid.uuid4().hex}",
            path=_artifact_string(artifact, "path"),
            name=normalize_task_event_text(_artifact_string(artifact, "name")),
            caption=(
                normalize_task_event_text(caption) if caption is not None else None
            ),
            content_type=normalize_task_event_text(
                _artifact_string(artifact, "content_type")
            ),
            size=_artifact_int(artifact, "size"),
            run_count=record.run_count,
        )
        self._task_manager.add_artifact(task_id, published)
        return _published_artifact_to_dict(published)

    def resolve_task_skill_view(
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

    def audit_task_review(
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

    # fmt: off
    async def execute(
        self,
        task_id: str,
        agent: Any,
        payload: Any,
        run_config: dict[str, Any],
        record: TaskRecord,
    ) -> Any:
        """Invoke the explicit compiled agent with the explicit graph payload."""
        del task_id
        if self._mailbox is None:
            return await self._invoke_agent_payload(agent, payload, run_config, record)
        configurable = run_config.setdefault("configurable", {})
        run_id = configurable.get("mailbox_run_id")
        if not isinstance(run_id, str) or not run_id:
            run_id = uuid.uuid4().hex
            configurable["mailbox_run_id"] = run_id
        with self._mailbox.run_scope(run_id):
            try:
                result = await self._invoke_agent_payload(
                    agent,
                    payload,
                    run_config,
                    record,
                )
            except BaseException:
                self._mailbox.release_run(run_id)
                raise
            try:
                self._mailbox.acknowledge_run(run_id)
            except BaseException:
                self._mailbox.release_run(run_id)
                raise
            return result
    # fmt: on

    async def _invoke_agent_payload(
        self,
        agent: Any,
        payload: Any,
        run_config: dict[str, Any],
        record: TaskRecord,
    ) -> Any:
        astream = getattr(agent, "astream", None)
        if not callable(astream):
            return await agent.ainvoke(payload, config=run_config, version="v2")
        latest: Any = _MISSING_STREAM_VALUE
        interrupts: list[Any] = []
        async for part in astream(
            payload,
            config=run_config,
            stream_mode=["messages", "values"],
            version="v2",
        ):
            delta = assistant_delta_from_stream_part(part)
            if delta is not None:
                ledger = self._task_manager.event_ledger
                if ledger is not None:
                    ledger.publish_assistant_delta(
                        task_id=record.task_id,
                        run_count=record.run_count,
                        content=delta,
                    )
            if isinstance(part, dict) and part.get("type") == "values":
                if "data" in part:
                    latest = part["data"]
                raw_interrupts = part.get("interrupts")
                if isinstance(raw_interrupts, (tuple, list)):
                    interrupts.extend(raw_interrupts)
        if latest is _MISSING_STREAM_VALUE:
            aget_state = getattr(agent, "aget_state", None)
            if not callable(aget_state):
                raise RuntimeError(
                    "Agent stream completed without values or readable state"
                )
            snapshot = await aget_state(run_config)
            latest = getattr(snapshot, "values", _MISSING_STREAM_VALUE)
            if latest is _MISSING_STREAM_VALUE:
                raise RuntimeError(
                    "Agent stream completed without values or readable state"
                )
            raw_interrupts = getattr(snapshot, "interrupts", ())
            if isinstance(raw_interrupts, (tuple, list)):
                interrupts.extend(raw_interrupts)
        return GraphOutput(value=latest, interrupts=tuple(interrupts))
