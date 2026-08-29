from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from deepagents.backends.protocol import BackendFactory, BackendProtocol

from ruyi_agent.runtime.middleware.deepagents_adapters import (
    AnthropicPromptCachingMiddleware,
    FilesystemMiddleware,
    MemoryMiddleware,
    PatchToolCallsMiddleware,
    TodoListMiddleware,
    create_summarization_middleware,
)
from ruyi_agent.runtime.middleware.artifact_publishing import (
    ArtifactPublishingMiddleware,
)
from ruyi_agent.runtime.middleware.tool_error import ToolErrorMiddleware
from ruyi_agent.runtime.middleware.tool_search import ToolSearchMiddleware
from ruyi_agent.runtime.middleware.ruyi_skills import RuyiSkillsMiddleware
from ruyi_agent.runtime.middleware.mailbox import MailboxMiddleware
from ruyi_agent.runtime.middleware.tool_call_protocol import (
    ToolCallProtocolMiddleware,
)
from ruyi_agent.runtime.middleware.task_hydration import TaskHydrationMiddleware
from ruyi_agent.runtime.middleware.worker_delegation import WorkerDelegationMiddleware
from ruyi_agent.runtime.middleware.human_approval import HumanApprovalMiddleware
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.storage.review_audit import ReviewAuditStore
from ruyi_agent.integrations.mcp.registry import MCPRegistry


def build_runtime_middleware(
    *,
    resolved_model: Any,
    backend: BackendProtocol | BackendFactory,
    skills: Any,
    memory: list[str] | None,
    local_worker_specs: dict[str, LocalWorkerSpec] | None,
    remote_refs: dict[str, RemoteRef] | None,
    worker_tools: list[Any] | None = None,
    mailbox: AgentMailbox | None = None,
    load_tasks_for_thread: Callable[[str], None] | None = None,
    permission_policy: PermissionPolicy | None = None,
    backend_kind: str = "unknown",
    workspace_root: str = "",
    register_artifact: Callable[..., dict[str, Any]] | None = None,
    agent_name: str | None = None,
    permission_profile: str | None = None,
    review_audit_store: ReviewAuditStore | None = None,
    tool_search_registry: MCPRegistry | None = None,
    tool_search_server_names: list[str] | None = None,
    tool_search_tool_names: list[str] | None = None,
    system_tools: frozenset[str] | None = None,
) -> list[Any]:
    """Build the middleware stack used by project runtime agents."""
    middleware: list[Any] = [TodoListMiddleware(), ToolErrorMiddleware()]

    if load_tasks_for_thread is not None:
        middleware.append(TaskHydrationMiddleware(load_tasks_for_thread))

    if mailbox is not None:
        middleware.append(MailboxMiddleware(mailbox))

    # Mailbox input can arrive while a tool call is in flight.  Normalize the
    # resulting history immediately after mailbox delivery so strict OpenAI-
    # compatible providers always see contiguous tool responses.
    middleware.append(ToolCallProtocolMiddleware())

    enabled_system_tools = system_tools

    if tool_search_registry is not None and (
        enabled_system_tools is None
        or bool(enabled_system_tools & {"tool_search", "call_tool"})
    ):
        tool_search_middleware = ToolSearchMiddleware(
            registry=tool_search_registry,
            server_names=tool_search_server_names,
            tool_names=tool_search_tool_names,
        )
        if enabled_system_tools is not None:
            tool_search_middleware.tools = [
                tool
                for tool in tool_search_middleware.tools
                if tool.name in enabled_system_tools
            ]
        if tool_search_middleware.tools:
            middleware.append(tool_search_middleware)

    if register_artifact is not None and (
        enabled_system_tools is None or "publish_artifact" in enabled_system_tools
    ):
        artifact_middleware = ArtifactPublishingMiddleware(
            backend=backend,
            workspace_root=workspace_root,
            register_artifact=register_artifact,
        )
        middleware.append(artifact_middleware)

    middleware.append(RuyiSkillsMiddleware(backend=backend))

    if enabled_system_tools is None or bool(
        enabled_system_tools
        & {"ls", "read_file", "write_file", "edit_file", "glob", "grep", "execute"}
    ):
        filesystem_middleware = FilesystemMiddleware(backend=backend)
        if enabled_system_tools is not None:
            filesystem_middleware.tools = [
                tool
                for tool in filesystem_middleware.tools
                if tool.name in enabled_system_tools
            ]
        if filesystem_middleware.tools:
            middleware.append(filesystem_middleware)
    middleware.extend(
        [
            create_summarization_middleware(resolved_model, backend),
            PatchToolCallsMiddleware(),
        ]
    )

    if worker_tools is not None:
        middleware.append(
            WorkerDelegationMiddleware(
                specs=local_worker_specs or {},
                remote_refs=remote_refs,
                tools=worker_tools,
            )
        )

    if permission_policy is not None:
        middleware.append(
            HumanApprovalMiddleware(
                policy=permission_policy,
                backend_kind=backend_kind,
                workspace_root=workspace_root,
                agent_name=agent_name,
                permission_profile=permission_profile,
                audit_store=review_audit_store,
            )
        )

    middleware.append(
        AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore")
    )
    if memory:
        middleware.append(MemoryMiddleware(backend=backend, sources=memory))

    return middleware
