"""Local worker compilation and run execution boundary.

LocalTaskExecutor owns compiled-agent caching and the process-local execution
lifecycle. Cross-cutting notification and tool capabilities are reached only
through the narrow host port supplied by AgentControl, so execution remains
independent from Gateway and remote transport policy.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Protocol

from langgraph.types import Command, GraphOutput

from ruyi_agent.config.system_tools import DELEGATION_SYSTEM_TOOLS
from ruyi_agent.runtime.agent_turn import normalize_agent_turn
from ruyi_agent.runtime.delegation.contracts import (
    _artifact_int,
    _artifact_optional_string,
    _artifact_string,
    _format_exception_summary,
    _format_interrupted_error,
    _published_artifact_to_dict,
)
from ruyi_agent.runtime.delegation.registry import LocalWorkerEntry, RegisteredAgent
from ruyi_agent.runtime.delegation.run_supervisor import RuntimeClosingError
from ruyi_agent.runtime.skills.resolver import resolve_skill_names
from ruyi_agent.runtime.task_events import (
    assistant_delta_from_stream_part,
    normalize_task_event_text,
)
from ruyi_agent.task_models import RESUMABLE_TASK_STATES, PublishedArtifact, TaskRecord

_MISSING_STREAM_VALUE = object()


class LocalExecutionHost(Protocol):
    """Capabilities supplied by the compatibility facade to local execution."""

    _agent_factory: Any
    _backend: Any
    _backend_kind: str
    _checkpointer: Any
    _compiled_agents: dict[str, Any]
    _mailbox: Any
    _permission_default_profile: str
    _permission_policy: Any
    _registry: Any
    _review_audit_store: Any
    _run_supervisor: Any
    _skill_catalog: Any
    _skill_syncer: Any
    _task_input_locks: dict[str, asyncio.Lock]
    _task_manager: Any
    _workspace_root: str

    def build_tools_for(self, agent_name: str) -> list[Any]: ...
    def load_tasks_for_thread(self, thread_id: str) -> None: ...
    def register_artifact(
        self, *, task_id: str, artifact: dict[str, Any]
    ) -> dict[str, Any]: ...
    def _audit_task_review(
        self,
        event_type: str,
        record: TaskRecord,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None: ...
    async def _ensure_task_awake(self, task_id: str) -> TaskRecord: ...
    def _get_or_create_agent(self, agent_name: str) -> Any: ...
    def _maybe_publish_settled_message(self, task_id: str) -> None: ...
    async def _run_agent_payload(self, task_id: str, payload: Any) -> None: ...
    async def _run_agent_turn(self, task_id: str, user_input: str) -> None: ...
    async def _send_settled_webhook(self, task_id: str) -> None: ...
    async def _start_mailbox_run(self, task_id: str) -> asyncio.Task[None]: ...


class LocalTaskExecutor:
    """Compile and execute local agents while preserving run lifecycle semantics."""

    def __init__(self, control: LocalExecutionHost) -> None:
        self._control = control

    def register_artifact(
        self,
        *,
        task_id: str,
        artifact: dict[str, Any],
    ) -> dict[str, Any]:
        record = self._control._task_manager.get_task(task_id)
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
        self._control._task_manager.add_artifact(task_id, published)
        return _published_artifact_to_dict(published)

    def _resolve_task_skill_view(
        self,
        entry: RegisteredAgent,
        *,
        parent_task_id: str | None,
    ) -> tuple[tuple[str, ...], str | None, str | None]:
        if not isinstance(entry, LocalWorkerEntry) or self._control._skill_syncer is None:
            return (), None, None
        parent_skill_names: tuple[str, ...] | None = None
        if parent_task_id is not None:
            parent_record = self._control._task_manager.get_task(parent_task_id)
            parent_skill_names = parent_record.effective_skill_names
        skill_names = resolve_skill_names(
            entry.spec.skills,
            self._control._skill_catalog,
            parent_skill_names=parent_skill_names,
        )
        view = self._control._skill_syncer.ensure_view(self._control._skill_catalog, skill_names)
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
        agent = self._control._compiled_agents.get(agent_name)
        if agent is not None:
            return agent

        spec = self._control._registry.get_spec(agent_name)
        declared_targets = spec.delegation_targets
        allowed_targets = set(declared_targets)
        has_delegation_tools = (
            bool(allowed_targets)
            if spec.system_tools is None
            else bool(spec.system_tools & DELEGATION_SYSTEM_TOOLS)
        )
        worker_tools = (
            self._control.build_tools_for(agent_name) if has_delegation_tools else None
        )
        # worker 自己内部也走新的 runtime 包装器，
        # 这样整个系统里不会混入 deepagents 默认 task/general-purpose。
        agent = self._control._agent_factory(
            model=spec.model,
            system_prompt=spec.system_prompt,
            tools=spec.tools,
            local_worker_specs=self._control._registry.select_local_specs(declared_targets),
            remote_refs=self._control._registry.select_remote_refs(declared_targets),
            worker_tools=worker_tools,
            memory=spec.memory,
            skills=spec.skills,
            backend=self._control._backend,
            checkpointer=self._control._checkpointer,
            mailbox=self._control._mailbox,
            load_tasks_for_thread=self._control.load_tasks_for_thread,
            permission_policy=self._control._permission_policy,
            backend_kind=self._control._backend_kind,
            workspace_root=self._control._workspace_root,
            register_artifact=self._control.register_artifact,
            permission_profile=(
                spec.permission_profile or self._control._permission_default_profile
            ),
            review_audit_store=self._control._review_audit_store,
            tool_search_registry=spec.tool_search_registry
            if spec.tool_search
            else None,
            tool_search_server_names=spec.tool_search_server_names,
            tool_search_tool_names=spec.tool_search_tool_names,
            system_tools=spec.system_tools,
            name=agent_name,
        )
        self._control._compiled_agents[agent_name] = agent  # agent 对象被复用（缓存）
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
        if self._control._review_audit_store is None:
            return
        review_id = None
        if payload is not None:
            raw_review_id = payload.get("review_id")
            if isinstance(raw_review_id, str):
                review_id = raw_review_id
        self._control._review_audit_store.append(
            event_type,
            source=record.route_kind,
            review_id=review_id,
            task_id=record.task_id,
            thread_id=record.thread_id,
            agent_name=record.agent_name,
            profile_name=record.permission_profile,
            backend_kind=self._control._backend_kind,
            workspace_root=self._control._workspace_root,
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
        record = self._control._task_manager.get_task(task_id)
        agent = self._control._get_or_create_agent(record.agent_name)
        run_config = self._control._build_agent_run_config(record)
        try:
            astream = getattr(agent, "astream", None)
            if callable(astream):
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
                        ledger = self._control._task_manager.event_ledger
                        if ledger is not None:
                            ledger.publish_assistant_delta(
                                task_id=task_id,
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
                result = GraphOutput(value=latest, interrupts=tuple(interrupts))
            else:
                result = await agent.ainvoke(
                    payload,
                    config=run_config,
                    version="v2",
                )
            # Complete stream consumption (or the compatibility fallback) only
            # returns after LangGraph has checkpointed the graph step.
            if self._control._mailbox is not None:
                self._control._mailbox.acknowledge_task(task_id, record.thread_id)
        except asyncio.CancelledError as exc:
            if self._control._task_manager.was_cancel_requested(task_id):
                self._control._task_manager.mark_cancelled(task_id)
            else:
                self._control._task_manager.mark_interrupted(
                    task_id,
                    _format_interrupted_error(exc),
                )
            self._control._maybe_publish_settled_message(task_id)
            await self._control._send_settled_webhook(task_id)  # 如果配置了webhook 就会发送
            raise
        except Exception as exc:
            self._control._task_manager.mark_failed(task_id, _format_exception_summary(exc))
            self._control._maybe_publish_settled_message(task_id)
            await self._control._send_settled_webhook(task_id)
            return

        outcome = await normalize_agent_turn(agent, run_config, result)
        if outcome.review_payloads:
            if len(outcome.review_payloads) > 1:
                self._control._task_manager.mark_failed(
                    task_id,
                    "Worker produced multiple simultaneous human review requests.",
                )
                self._control._maybe_publish_settled_message(task_id)
                await self._control._send_settled_webhook(task_id)
                return
            self._control._task_manager.mark_waiting_for_human(
                task_id,
                outcome.review_payloads[0],
            )
            self._control._audit_task_review(
                "task_waiting_for_human",
                self._control._task_manager.get_task(task_id),
                payload=outcome.review_payloads[0],
            )
            return

        if outcome.has_unresolved_tool_calls:
            self._control._task_manager.mark_failed(
                task_id,
                "Worker stopped before resolving pending tool calls.",
            )
            self._control._maybe_publish_settled_message(task_id)
            await self._control._send_settled_webhook(task_id)
            return

        result_text = (
            outcome.content
            or "Task completed, but the final assistant reply was empty."
        )
        self._control._task_manager.mark_completed(task_id, result_text)
        self._control._maybe_publish_settled_message(task_id)
        await self._control._send_settled_webhook(task_id)

    async def _run_agent_turn(self, task_id: str, user_input: str) -> None:
        await self._control._run_agent_payload(
            task_id,
            {"messages": [{"role": "user", "content": user_input}]},
        )

    async def _start_run(
        self,
        task_id: str,
        user_input: str,
    ) -> asyncio.Task[None]:
        """
        启动本地任务的一轮异步执行

        Args:
            task_id: 当前 runtime 内部任务 ID
            user_input: 本轮传给 worker 的输入

        Raises:
            TaskAlreadyRunningError: 该任务已有未结束的活跃 run
        """
        # 为什么单独启动 run：send_input 和首次 spawn 都需要走同一套任务启动约束。
        return await self._control._run_supervisor.schedule(
            task_id,
            lambda: self._control._run_agent_turn(task_id, user_input),
        )

    async def _start_mailbox_run(self, task_id: str) -> asyncio.Task[None]:
        """Start a run whose user input will be supplied by MailboxMiddleware."""
        return await self._control._run_supervisor.schedule(
            task_id,
            lambda: self._control._run_agent_payload(task_id, {"messages": []}),
        )

    async def _ensure_task_awake(self, task_id: str) -> TaskRecord:
        """Start at most one mailbox-driven run when a resumable task has input."""
        lock = self._control._task_input_locks.setdefault(task_id, asyncio.Lock())
        async with lock:
            record = self._control._task_manager.get_task(task_id)
            if not self._control._run_supervisor.is_accepting:
                return record
            if self._control._mailbox is None or record.route_kind != "local":
                return record
            if self._control._task_manager.has_active_run(task_id):
                return record
            if record.state not in RESUMABLE_TASK_STATES:
                return record
            if not self._control._mailbox.has_triggering_messages(task_id):
                return record
            try:
                await self._control._start_mailbox_run(task_id)
            except RuntimeClosingError:
                return self._control._task_manager.get_task(task_id)
            return self._control._task_manager.get_task(task_id)

    async def wake_pending_mailbox_tasks(self) -> None:
        """Resume durable triggering inputs left behind by a process restart."""
        if self._control._mailbox is not None:
            self._control._mailbox.recover_claims()
        for record in self._control._task_manager.list_persisted_tasks():
            await self._control._ensure_task_awake(record.task_id)

    def start_mailbox_recovery(self) -> None:
        """Periodically recover expired claims from interrupted runtimes."""
        if self._control._mailbox is None:
            return

        async def recover() -> None:
            while True:
                await asyncio.sleep(5)
                await self._control.wake_pending_mailbox_tasks()

        self._control._run_supervisor.start_recovery(recover)

    async def close(self) -> None:
        """Drain runtime-owned work before closing the lifecycle event ledger."""
        await self._control._run_supervisor.close()
        ledger = self._control._task_manager.event_ledger
        if ledger is not None:
            ledger.close()

    async def _resume_run(
        self,
        task_id: str,
        decisions: list[dict[str, Any]],
    ) -> asyncio.Task[None]:
        record = self._control._task_manager.get_task(task_id)
        review_id = (record.pending_review or {}).get("review_id")
        run_task = await self._control._run_supervisor.schedule(
            task_id,
            lambda: self._control._run_agent_payload(
                task_id,
                Command(resume={"decisions": decisions}),
            ),
        )
        self._control._audit_task_review(
            "task_review_resumed",
            record,
            payload={
                "review_id": review_id,
                "decisions": decisions,
            },
        )
        return run_task
