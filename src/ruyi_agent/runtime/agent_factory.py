from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from deepagents._models import resolve_model
from deepagents._version import __version__
from deepagents.backends import StateBackend
from deepagents.backends.protocol import BackendFactory, BackendProtocol
from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import SystemMessage
from langchain_core.tools import BaseTool

from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.runtime.middleware.stack import build_runtime_middleware
from ruyi_agent.runtime.prompts import PROJECT_BASE_AGENT_PROMPT
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.control_plane.permissions import PermissionPolicy
from ruyi_agent.integrations.mcp.registry import MCPRegistry
from ruyi_agent.storage.review_audit import ReviewAuditStore


def create_runtime_agent(
    model: str | BaseChatModel,
    tools: Sequence[BaseTool | Callable | dict[str, Any]] | None = None,
    *,
    system_prompt: str | SystemMessage | None = None,
    local_worker_specs: dict[str, LocalWorkerSpec] | None = None,
    remote_refs: dict[str, RemoteRef] | None = None,
    build_worker_tools: Callable[[], list[Any]] | None = None,
    skills: list[str] | None = None,
    memory: list[str] | None = None,
    checkpointer: Any | None = None,
    backend: BackendProtocol | BackendFactory | None = None,
    mailbox: AgentMailbox | None = None,
    load_tasks_for_thread: Callable[[str], None] | None = None,
    permission_policy: PermissionPolicy | None = None,
    backend_kind: str = "unknown",
    workspace_root: str = "",
    permission_profile: str | None = None,
    review_audit_store: ReviewAuditStore | None = None,
    tool_search_registry: MCPRegistry | None = None,
    tool_search_server_names: list[str] | None = None,
    tool_search_tool_names: list[str] | None = None,
    debug: bool = False,
    name: str | None = None,
):
    """Create a runtime agent without deepagents' built-in subagent middleware."""

    # 这里只复用 deepagents 的模型解析，不再读取 deepagents profile 隐式改写 prompt/tools/middleware。
    resolved_model = resolve_model(model)
    runtime_backend = backend if backend is not None else StateBackend()

    middleware = build_runtime_middleware(
        resolved_model=resolved_model,
        backend=runtime_backend,
        skills=skills,
        memory=memory,
        local_worker_specs=local_worker_specs,
        remote_refs=remote_refs,
        build_worker_tools=build_worker_tools,  # ???
        mailbox=mailbox,
        load_tasks_for_thread=load_tasks_for_thread,
        permission_policy=permission_policy,
        backend_kind=backend_kind,
        workspace_root=workspace_root,
        agent_name=name,
        permission_profile=permission_profile,
        review_audit_store=review_audit_store,
        tool_search_registry=tool_search_registry,
        tool_search_server_names=tool_search_server_names,
        tool_search_tool_names=tool_search_tool_names,
    )

    # base prompt 固定在项目代码里，避免 deepagents profile 动态拼接系统提示词。
    base_prompt = PROJECT_BASE_AGENT_PROMPT
    if system_prompt is None:
        final_system_prompt: str | SystemMessage = base_prompt
    elif isinstance(system_prompt, SystemMessage):
        final_system_prompt = SystemMessage(
            content_blocks=[
                *system_prompt.content_blocks,
                {"type": "text", "text": f"\n\n{base_prompt}"},
            ]
        )
    else:
        final_system_prompt = system_prompt + "\n\n" + base_prompt

    # 这里直接回到底层 create_agent，避免 deepagents 自动插入 general-purpose/task。
    return create_agent(
        resolved_model,
        system_prompt=final_system_prompt,
        tools=tools,
        middleware=middleware,
        checkpointer=checkpointer,
        debug=debug,
        name=name,
    ).with_config(
        {
            "recursion_limit": 9_999,
            "metadata": {
                "ls_integration": "ruyi_agent",
                "versions": {"deepagents": __version__},
                "lc_agent_name": name,
            },
        }
    )
