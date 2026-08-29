from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ruyi_agent.config.agent_models import AgentConfig, LocalAgentConfig


DELEGATION_SYSTEM_TOOLS = frozenset(
    {
        "spawn_agent",
        "wait_agent",
        "check_agent",
        "send_input",
        "cancel_agent",
        "list_agents",
    }
)
FILESYSTEM_SYSTEM_TOOLS = frozenset(
    {"ls", "read_file", "write_file", "edit_file", "glob", "grep", "execute"}
)
ARTIFACT_SYSTEM_TOOLS = frozenset({"publish_artifact"})
TOOL_SEARCH_SYSTEM_TOOLS = frozenset({"tool_search", "call_tool"})
KNOWN_SYSTEM_TOOLS = frozenset(
    DELEGATION_SYSTEM_TOOLS
    | FILESYSTEM_SYSTEM_TOOLS
    | ARTIFACT_SYSTEM_TOOLS
    | TOOL_SEARCH_SYSTEM_TOOLS
)


@dataclass(frozen=True, slots=True)
class ResolvedSystemTools:
    enabled: frozenset[str]
    automatic: frozenset[str]
    explicit: frozenset[str]
    disabled: frozenset[str]


def local_agent_parent_names(
    agent_name: str,
    agent_configs: Mapping[str, AgentConfig],
) -> frozenset[str]:
    return frozenset(
        parent_name
        for parent_name, config in agent_configs.items()
        if isinstance(config, LocalAgentConfig) and agent_name in config.workers
    )


def resolve_system_tools(
    agent_name: str,
    agent_configs: Mapping[str, AgentConfig],
    *,
    backend_available: bool = True,
    artifact_available: bool = True,
) -> ResolvedSystemTools:
    config = agent_configs[agent_name]
    if not isinstance(config, LocalAgentConfig):
        raise ValueError(f"Agent '{agent_name}' is not a local agent.")
    automatic: set[str] = set()
    if config.workers:
        automatic.update(DELEGATION_SYSTEM_TOOLS)
    elif local_agent_parent_names(agent_name, agent_configs):
        automatic.update({"send_input", "list_agents"})
    if backend_available:
        automatic.update(FILESYSTEM_SYSTEM_TOOLS)
    if artifact_available:
        automatic.update(ARTIFACT_SYSTEM_TOOLS)
    if config.tool_search:
        automatic.update(TOOL_SEARCH_SYSTEM_TOOLS)

    explicit = set(config.system_tools)
    disabled = set(config.disabled_system_tools)
    enabled = (automatic | explicit) - disabled
    return ResolvedSystemTools(
        enabled=frozenset(enabled),
        automatic=frozenset(automatic),
        explicit=frozenset(explicit),
        disabled=frozenset(disabled),
    )


def validate_system_tool_names(agent_name: str, field_name: str, value: object) -> None:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValueError(
            f"Agent '{agent_name}' field '{field_name}' must be a list of strings."
        )
    unknown = sorted(set(value) - KNOWN_SYSTEM_TOOLS)
    if unknown:
        raise ValueError(
            f"Agent '{agent_name}' field '{field_name}' contains unknown system "
            f"tools: {', '.join(unknown)}. Available: "
            f"{', '.join(sorted(KNOWN_SYSTEM_TOOLS))}."
        )
