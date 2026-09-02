"""Compile typed Agent configuration into runtime execution specifications."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any

from ruyi_agent.config.agent_models import (
    BearerAuthConfig,
    LocalAgentConfig,
    RemoteAgentConfig,
    RemoteCreateIdempotency,
)
from ruyi_agent.config.agent_parser import coerce_agent_configs
from ruyi_agent.config.provider_models import LLMProviderSpec
from ruyi_agent.config.system_tools import resolve_system_tools
from ruyi_agent.integrations.mcp.registry import MCPRegistry
from ruyi_agent.integrations.model_providers import build_chat_model
from ruyi_agent.safe_errors import safe_exception_summary


@dataclass(slots=True)
class LocalWorkerSpec:
    """Executable runtime projection of one validated local Agent config."""

    name: str
    description: str
    system_prompt: str
    model: Any
    tools: list[Any]
    memory: list[str]
    skills: str | list[str]
    permission_profile: str | None = None
    delegation_targets: tuple[str, ...] = ()
    tool_search: bool = False
    tool_search_registry: MCPRegistry | None = None
    tool_search_server_names: list[str] = field(default_factory=list)
    tool_search_tool_names: list[str] = field(default_factory=list)
    system_tools: frozenset[str] | None = None


@dataclass(slots=True)
class RemoteRef:
    """Runtime connection details for one validated remote Agent."""

    name: str
    description: str
    url: str
    remote_agent_name: str
    auth: BearerAuthConfig | None = None
    create_idempotency: RemoteCreateIdempotency = "none"

    def __post_init__(self) -> None:
        # Programmatic callers historically passed the former raw auth dictionary.
        if isinstance(self.auth, dict):
            auth_type = self.auth.get("type")
            token_env = self.auth.get("token_env")
            if auth_type != "bearer" or not isinstance(token_env, str) or not token_env:
                raise ValueError("RemoteRef auth must define bearer type and token_env")
            self.auth = BearerAuthConfig(type="bearer", token_env=token_env)
        if not isinstance(self.create_idempotency, str) or (
            self.create_idempotency not in {"none", "ruyi_gateway_v1"}
        ):
            raise ValueError(
                "RemoteRef create_idempotency must be 'none' or 'ruyi_gateway_v1'"
            )

    @property
    def create_idempotency_guaranteed(self) -> bool:
        return self.create_idempotency == "ruyi_gateway_v1"


def to_backend_paths(paths: Sequence[str], base_dir: str) -> list[str]:
    """Resolve config-relative POSIX paths under the backend home."""

    base = PurePosixPath(base_dir or "/")
    return [
        str(PurePosixPath(path) if PurePosixPath(path).is_absolute() else base / path)
        for path in paths
    ]


def build_remote_ref(
    agent_name: str,
    agent_configs: Mapping[str, object],
) -> RemoteRef:
    configs = coerce_agent_configs(agent_configs)
    config = configs[agent_name]
    if not isinstance(config, RemoteAgentConfig):
        raise ValueError(f"Agent '{agent_name}' is not a remote_ref.")
    return RemoteRef(
        name=config.name,
        description=config.description,
        url=config.url,
        remote_agent_name=config.remote_agent_name,
        auth=config.auth,
        create_idempotency=config.create_idempotency,
    )


async def build_local_worker_spec(
    agent_name: str,
    agent_configs: Mapping[str, object],
    registry: MCPRegistry,
    *,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
    home_dir: str,
    skills_root: str | None = None,
) -> LocalWorkerSpec:
    """Compile one validated local config into its executable runtime spec."""

    del skills_root  # Retained only for compatibility with pre-typed callers.
    configs = coerce_agent_configs(agent_configs)
    config = configs[agent_name]
    if not isinstance(config, LocalAgentConfig):
        raise ValueError(f"Agent '{agent_name}' is not a local agent.")
    resolved_system_tools = resolve_system_tools(agent_name, configs)
    tool_search = bool(resolved_system_tools.enabled & {"tool_search", "call_tool"})
    server_names = list(config.server_names)
    tool_names = list(config.tool_names)
    tools = (
        []
        if tool_search
        else await registry.resolve_tools(
            server_names=server_names,
            tool_names=tool_names,
        )
    )
    skills = config.skills if isinstance(config.skills, str) else list(config.skills)
    return LocalWorkerSpec(
        name=config.name,
        description=config.description,
        system_prompt=config.system_prompt,
        model=build_chat_model(
            model_name=config.model,
            provider_name=config.provider,
            providers=providers,
            getenv=getenv,
        ),
        tools=tools,
        memory=to_backend_paths(config.memory, home_dir),
        skills=skills,
        permission_profile=config.permission_profile,
        delegation_targets=config.workers,
        tool_search=tool_search,
        tool_search_registry=registry if tool_search else None,
        tool_search_server_names=server_names,
        tool_search_tool_names=tool_names,
        system_tools=resolved_system_tools.enabled,
    )


async def build_all_local_worker_specs(
    agent_configs: Mapping[str, object],
    registry: MCPRegistry,
    *,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
    home_dir: str,
    skills_root: str | None = None,
    unavailable_errors: dict[str, str] | None = None,
) -> dict[str, LocalWorkerSpec]:
    del skills_root  # Compatibility-only parameter; runtime no longer propagates it.
    configs = coerce_agent_configs(agent_configs)
    specs: dict[str, LocalWorkerSpec] = {}
    for agent_name, config in configs.items():
        if not isinstance(config, LocalAgentConfig):
            continue
        try:
            specs[agent_name] = await build_local_worker_spec(
                agent_name,
                configs,
                registry,
                providers=providers,
                getenv=getenv,
                home_dir=home_dir,
            )
        except Exception as exc:
            if unavailable_errors is None:
                raise
            unavailable_errors[agent_name] = safe_exception_summary(
                exc,
                known_secrets=_configured_provider_secrets(config, providers, getenv),
            )
    return specs


def _configured_provider_secrets(
    config: LocalAgentConfig,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
) -> tuple[str, ...]:
    """Read only the configured provider key when an unavailable reason is emitted."""

    provider = providers.get(config.provider)
    if provider is None or provider.api_key_env is None:
        return ()
    try:
        value = getenv(provider.api_key_env)
    except Exception:
        return ()
    return (value,) if isinstance(value, str) and value else ()


async def build_all_remote_refs(
    agent_configs: Mapping[str, object],
) -> dict[str, RemoteRef]:
    configs = coerce_agent_configs(agent_configs)
    return {
        name: build_remote_ref(name, configs)
        for name, config in configs.items()
        if isinstance(config, RemoteAgentConfig)
    }


def select_public_local_worker_specs(
    agent_configs: Mapping[str, object],
    all_local_specs: dict[str, LocalWorkerSpec],
) -> dict[str, LocalWorkerSpec]:
    configs = coerce_agent_configs(agent_configs)
    return {
        name: spec for name, spec in all_local_specs.items() if configs[name].public
    }


async def build_public_remote_refs(
    agent_configs: Mapping[str, object],
) -> dict[str, RemoteRef]:
    configs = coerce_agent_configs(agent_configs)
    return {
        name: build_remote_ref(name, configs)
        for name, config in configs.items()
        if isinstance(config, RemoteAgentConfig) and config.public
    }
