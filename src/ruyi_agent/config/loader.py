"""Configuration loading and stable public compatibility exports.

Raw TOML mappings stop here. Agent, provider, and permission validation live in
dedicated parsers; runtime and provider construction consume their typed output.
"""

from __future__ import annotations

import tomllib
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from ruyi_agent.config.agent_models import (
    AgentConfig,
    AgentConfigs,
    BearerAuthConfig,
    LocalAgentConfig,
    RemoteAgentConfig,
    SkillSelection,
)
from ruyi_agent.config.agent_parser import (
    LOCAL_AGENT_ALLOWED_FIELDS,
    LOCAL_AGENT_REQUIRED_FIELDS,
    REMOTE_AGENT_ALLOWED_FIELDS as REMOTE_REF_ALLOWED_FIELDS,
    REMOTE_AGENT_REQUIRED_FIELDS as REMOTE_REF_REQUIRED_FIELDS,
    coerce_agent_configs,
    parse_agent_configs,
    parse_skill_selection,
)
from ruyi_agent.config.errors import ConfigError
from ruyi_agent.config.paths import resolve_ruyi_paths
from ruyi_agent.config.permission_parser import parse_permission_config
from ruyi_agent.config.provider_models import (
    LLMProviderSpec,
    RESERVED_PROVIDER_INIT_KWARGS,
    SUPPORTED_MODEL_PROVIDERS,
    validate_provider_init_kwargs,
)
from ruyi_agent.config.provider_parser import parse_llm_provider_configs
from ruyi_agent.control_plane.permissions import PermissionConfig
from ruyi_agent.integrations.model_providers import build_chat_model
from ruyi_agent.config.agent_runtime import (
    LocalWorkerSpec,
    RemoteRef,
    build_all_local_worker_specs,
    build_all_remote_refs,
    build_local_worker_spec,
    build_public_remote_refs,
    build_remote_ref,
    select_public_local_worker_specs,
    to_backend_paths,
)

CONFIG_DIR = Path(".ruyi_agent") / "config"
MCP_CONFIG_PATH = CONFIG_DIR / "mcp_servers.toml"
AGENTS_CONFIG_PATH = CONFIG_DIR / "agents.toml"
LLM_PROVIDERS_CONFIG_PATH = CONFIG_DIR / "llm_providers.toml"
PERMISSIONS_CONFIG_PATH = CONFIG_DIR / "permissions.toml"


def load_toml_config(path: Path) -> dict[str, Any]:
    """Read one TOML document and add a path-specific syntax diagnostic."""

    try:
        with path.open("rb") as file:
            return tomllib.load(file)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(
            f"Invalid TOML in {path}: {exc}. "
            "TOML requires quote string values, including model names, URLs, "
            'and Windows paths. Example: model = "qwen/qwen3.6-plus".'
        ) from exc


def load_mcp_server_configs(path: Path | None = None) -> dict[str, dict[str, Any]]:
    data = load_toml_config(path or _config_path("mcp_servers.toml"))
    configs = data.get("mcp_servers")
    if not isinstance(configs, dict):
        raise ValueError("mcp_servers must be a table")
    return configs


def load_agent_configs(path: Path | None = None) -> tuple[str, AgentConfigs]:
    """Load Agent TOML and return its typed, validated graph."""

    data = load_toml_config(path or _config_path("agents.toml"))
    return parse_agent_configs(data.get("main_agent"), data.get("agents"))


def validate_agent_configs(
    main_agent_name: str,
    agent_configs: Mapping[str, object],
) -> None:
    """Compatibility validation entry point; typed results come from the loader."""

    parse_agent_configs(main_agent_name, agent_configs)


def load_llm_provider_configs(
    path: Path | None = None,
) -> dict[str, LLMProviderSpec]:
    data = load_toml_config(path or _config_path("llm_providers.toml"))
    return parse_llm_provider_configs(data.get("providers", {}))


def load_permission_config(path: Path | None = None) -> PermissionConfig:
    data = load_toml_config(path or _config_path("permissions.toml"))
    return parse_permission_config(data)


def build_chat_model_from_config(
    agent_config: LocalAgentConfig | Mapping[str, object],
    *,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
) -> Any:
    """Compatibility adapter for callers that only provide provider/model fields."""

    if isinstance(agent_config, LocalAgentConfig):
        provider_name = agent_config.provider
        model_name = agent_config.model
    else:
        provider_name = agent_config.get("provider")
        model_name = agent_config.get("model")
        if provider_name is not None and not isinstance(provider_name, str):
            raise ValueError("Agent config field 'provider' must be a string.")
        if model_name is not None and not isinstance(model_name, str):
            raise ValueError("Agent config field 'model' must be a string.")
    return build_chat_model(
        model_name=model_name or "",
        provider_name=provider_name or "",
        providers=providers,
        getenv=getenv,
    )


def _validate_provider_init_kwargs(
    provider_name: str,
    init_kwargs: dict[str, Any],
) -> None:
    """Former private name retained for downstream imports."""

    validate_provider_init_kwargs(provider_name, init_kwargs)


def _config_path(filename: str) -> Path:
    return resolve_ruyi_paths().config_dir / filename


__all__ = [
    "AGENTS_CONFIG_PATH",
    "AgentConfig",
    "AgentConfigs",
    "BearerAuthConfig",
    "CONFIG_DIR",
    "ConfigError",
    "LLMProviderSpec",
    "LLM_PROVIDERS_CONFIG_PATH",
    "LOCAL_AGENT_ALLOWED_FIELDS",
    "LOCAL_AGENT_REQUIRED_FIELDS",
    "LocalAgentConfig",
    "LocalWorkerSpec",
    "MCP_CONFIG_PATH",
    "PERMISSIONS_CONFIG_PATH",
    "REMOTE_REF_ALLOWED_FIELDS",
    "REMOTE_REF_REQUIRED_FIELDS",
    "RESERVED_PROVIDER_INIT_KWARGS",
    "RemoteAgentConfig",
    "RemoteRef",
    "SUPPORTED_MODEL_PROVIDERS",
    "SkillSelection",
    "build_all_local_worker_specs",
    "build_all_remote_refs",
    "build_chat_model_from_config",
    "build_local_worker_spec",
    "build_public_remote_refs",
    "build_remote_ref",
    "coerce_agent_configs",
    "load_agent_configs",
    "load_llm_provider_configs",
    "load_mcp_server_configs",
    "load_permission_config",
    "load_toml_config",
    "parse_agent_configs",
    "parse_skill_selection",
    "select_public_local_worker_specs",
    "to_backend_paths",
    "validate_agent_configs",
]
