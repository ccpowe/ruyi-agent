"""Parse and validate raw TOML Agent declarations into typed models."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from ruyi_agent.config.agent_models import (
    AgentConfig,
    AgentConfigs,
    BearerAuthConfig,
    LocalAgentConfig,
    RemoteAgentConfig,
    SkillSelection,
)
from ruyi_agent.config.system_tools import validate_system_tool_names
from ruyi_agent.config.url_validation import validate_http_url


LOCAL_AGENT_REQUIRED_FIELDS = {
    "kind",
    "public",
    "name",
    "description",
    "system_prompt",
    "provider",
    "model",
    "memory",
    "skills",
    "server_names",
    "tool_names",
    "workers",
}
LOCAL_AGENT_ALLOWED_FIELDS = set(LOCAL_AGENT_REQUIRED_FIELDS) | {
    "permission_profile",
    "tool_search",
    "system_tools",
    "disabled_system_tools",
}
REMOTE_AGENT_REQUIRED_FIELDS = {
    "kind",
    "public",
    "name",
    "description",
    "url",
    "remote_agent_name",
}
REMOTE_AGENT_ALLOWED_FIELDS = REMOTE_AGENT_REQUIRED_FIELDS | {"auth"}


def parse_skill_selection(raw: object) -> SkillSelection:
    """Parse an Agent's skill visibility declaration."""

    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in {"inherit", "none"}:
            return cast("SkillSelection", value)
        raise ValueError(
            "Agent config field 'skills' must be 'inherit', 'none', "
            "or a list of skill names."
        )
    if isinstance(raw, list):
        return tuple(
            _non_empty_string(
                item, path="Agent config field 'skills' list item"
            ).strip()
            for item in raw
        )
    raise ValueError(
        "Agent config field 'skills' must be 'inherit', 'none', "
        "or a list of skill names."
    )


def parse_agent_configs(
    main_agent_name: object,
    raw_agent_configs: object,
) -> tuple[str, AgentConfigs]:
    """Return a fully validated discriminated Agent configuration graph."""

    main_name = _non_empty_string(main_agent_name, path="main_agent")
    if not isinstance(raw_agent_configs, Mapping):
        raise ValueError("agents must be a table")

    configs: AgentConfigs = {}
    for raw_name, raw_config in raw_agent_configs.items():
        agent_name = _non_empty_string(raw_name, path="Agent key")
        configs[agent_name] = parse_agent_config(agent_name, raw_config)
    _validate_agent_graph(main_name, configs)
    return main_name, configs


def parse_agent_config(agent_name: str, raw: object) -> AgentConfig:
    """Parse one raw Agent table; typed inputs pass through unchanged."""

    if isinstance(raw, (LocalAgentConfig, RemoteAgentConfig)):
        if raw.name != agent_name:
            _raise_name_mismatch(agent_name, raw.name)
        return raw
    if not isinstance(raw, Mapping):
        raise ValueError(f"Agent '{agent_name}' must be a table.")

    config = dict(raw)
    kind = config.get("kind")
    if kind == "local":
        return _parse_local_agent(agent_name, config)
    if kind == "remote_ref":
        return _parse_remote_agent(agent_name, config)
    raise ValueError(
        f"Agent '{agent_name}' has unsupported kind: {kind!r}. "
        "Expected one of: 'local', 'remote_ref'."
    )


def coerce_agent_configs(raw_agent_configs: Mapping[str, object]) -> AgentConfigs:
    """Normalize legacy programmatic dictionaries at the public builder boundary."""

    return {
        agent_name: parse_agent_config(agent_name, raw_config)
        for agent_name, raw_config in raw_agent_configs.items()
    }


def _parse_local_agent(
    agent_name: str,
    config: dict[str, Any],
) -> LocalAgentConfig:
    _validate_fields(
        agent_name,
        config,
        required=LOCAL_AGENT_REQUIRED_FIELDS,
        allowed=LOCAL_AGENT_ALLOWED_FIELDS,
    )
    public = _boolean(config["public"], path=_field(agent_name, "public"))
    configured_name = _non_empty_string(
        config["name"], path=_field(agent_name, "name")
    )
    if configured_name != agent_name:
        _raise_name_mismatch(agent_name, configured_name)
    permission_profile = config.get("permission_profile")
    if permission_profile is not None:
        permission_profile = _non_empty_string(
            permission_profile,
            path=_field(agent_name, "permission_profile"),
        )
    tool_search = config.get("tool_search", False)
    tool_search = _boolean(tool_search, path=_field(agent_name, "tool_search"))
    system_tools = config.get("system_tools", [])
    disabled_system_tools = config.get("disabled_system_tools", [])
    validate_system_tool_names(agent_name, "system_tools", system_tools)
    validate_system_tool_names(
        agent_name,
        "disabled_system_tools",
        disabled_system_tools,
    )
    return LocalAgentConfig(
        name=configured_name,
        public=public,
        description=_string(config["description"], path=_field(agent_name, "description")),
        system_prompt=_string(
            config["system_prompt"], path=_field(agent_name, "system_prompt")
        ),
        provider=_non_empty_string(
            config["provider"], path=_field(agent_name, "provider")
        ),
        model=_non_empty_string(config["model"], path=_field(agent_name, "model")),
        memory=_string_tuple(config["memory"], path=_field(agent_name, "memory")),
        skills=parse_skill_selection(config["skills"]),
        server_names=_string_tuple(
            config["server_names"], path=_field(agent_name, "server_names")
        ),
        tool_names=_string_tuple(
            config["tool_names"], path=_field(agent_name, "tool_names")
        ),
        workers=_string_tuple(config["workers"], path=_field(agent_name, "workers")),
        permission_profile=permission_profile,
        tool_search=tool_search,
        system_tools=frozenset(system_tools),
        disabled_system_tools=frozenset(disabled_system_tools),
    )


def _parse_remote_agent(
    agent_name: str,
    config: dict[str, Any],
) -> RemoteAgentConfig:
    _validate_fields(
        agent_name,
        config,
        required=REMOTE_AGENT_REQUIRED_FIELDS,
        allowed=REMOTE_AGENT_ALLOWED_FIELDS,
    )
    configured_name = _non_empty_string(
        config["name"], path=_field(agent_name, "name")
    )
    if configured_name != agent_name:
        _raise_name_mismatch(agent_name, configured_name)
    return RemoteAgentConfig(
        name=configured_name,
        public=_boolean(config["public"], path=_field(agent_name, "public")),
        description=_string(config["description"], path=_field(agent_name, "description")),
        url=_remote_url(config["url"], agent_name=agent_name),
        remote_agent_name=_non_empty_string(
            config["remote_agent_name"],
            path=_field(agent_name, "remote_agent_name"),
        ),
        auth=_parse_remote_auth(config.get("auth"), agent_name=agent_name),
    )


def _parse_remote_auth(raw: object, *, agent_name: str) -> BearerAuthConfig | None:
    path = _field(agent_name, "auth")
    if raw is None or raw == {}:
        return None
    if isinstance(raw, BearerAuthConfig):
        return raw
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must be a table.")
    unexpected = sorted(set(raw) - {"type", "token_env"})
    if unexpected:
        raise ValueError(f"{path} has unexpected fields: {', '.join(unexpected)}")
    missing = sorted({"type", "token_env"} - set(raw))
    if missing:
        raise ValueError(f"{path} is missing required fields: {', '.join(missing)}")
    auth_type = raw["type"]
    if auth_type != "bearer":
        raise ValueError(f"{path}.type must be 'bearer'.")
    return BearerAuthConfig(
        type="bearer",
        token_env=_non_empty_string(raw["token_env"], path=f"{path}.token_env"),
    )


def _validate_agent_graph(main_agent_name: str, configs: AgentConfigs) -> None:
    main = configs.get(main_agent_name)
    if main is None:
        raise ValueError(f"main_agent '{main_agent_name}' is not defined in [agents].")
    if not isinstance(main, LocalAgentConfig):
        raise ValueError(
            f"main_agent '{main_agent_name}' must reference an agent with kind='local'."
        )

    local_edges: dict[str, tuple[str, ...]] = {}
    for agent_name, config in configs.items():
        if not isinstance(config, LocalAgentConfig):
            continue
        if agent_name in config.workers:
            raise ValueError(f"Agent '{agent_name}' cannot list itself in workers.")
        for target_name in config.workers:
            if target_name not in configs:
                raise ValueError(
                    f"Agent '{agent_name}' references unknown worker target "
                    f"'{target_name}'."
                )
        local_edges[agent_name] = tuple(
            target_name
            for target_name in config.workers
            if isinstance(configs[target_name], LocalAgentConfig)
        )
    _validate_local_graph_acyclic(local_edges)


def _validate_local_graph_acyclic(local_edges: Mapping[str, tuple[str, ...]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()
    path: list[str] = []

    def visit(agent_name: str) -> None:
        if agent_name in visited:
            return
        if agent_name in visiting:
            cycle_start = path.index(agent_name)
            cycle = [*path[cycle_start:], agent_name]
            raise ValueError(
                "Local worker graph contains a cycle: " + " -> ".join(cycle)
            )
        visiting.add(agent_name)
        path.append(agent_name)
        for target_name in local_edges.get(agent_name, ()):
            visit(target_name)
        path.pop()
        visiting.remove(agent_name)
        visited.add(agent_name)

    for agent_name in local_edges:
        visit(agent_name)


def _validate_fields(
    agent_name: str,
    config: Mapping[str, object],
    *,
    required: set[str],
    allowed: set[str],
) -> None:
    missing = sorted(required - set(config))
    if missing:
        raise ValueError(
            f"Agent '{agent_name}' is missing required fields: {', '.join(missing)}"
        )
    unexpected = sorted(set(config) - allowed)
    if unexpected:
        raise ValueError(
            f"Agent '{agent_name}' has unexpected fields: {', '.join(unexpected)}"
        )


def _field(agent_name: str, name: str) -> str:
    return f"Agent '{agent_name}' field '{name}'"


def _string(value: object, *, path: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{path} must be a string.")
    return value


def _non_empty_string(value: object, *, path: str) -> str:
    parsed = _string(value, path=path)
    if not parsed.strip():
        raise ValueError(f"{path} must be a non-empty string.")
    return parsed


def _boolean(value: object, *, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be a boolean.")
    return value


def _string_tuple(value: object, *, path: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list of strings.")
    return tuple(
        _non_empty_string(item, path=f"{path} item")
        for item in value
    )


def _remote_url(value: object, *, agent_name: str) -> str:
    path = _field(agent_name, "url")
    url = _non_empty_string(value, path=path)
    return validate_http_url(url, path=path)


def _raise_name_mismatch(agent_name: str, configured_name: str) -> None:
    raise ValueError(
        f"Agent key '{agent_name}' must match its configured name "
        f"'{configured_name}'."
    )
