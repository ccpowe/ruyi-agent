from __future__ import annotations

import tomllib
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from langchain.chat_models import init_chat_model

from ruyi_agent.config.paths import resolve_ruyi_paths
from ruyi_agent.integrations.mcp.registry import MCPRegistry
from ruyi_agent.control_plane.permissions import (
    ExecuteRuleConfig,
    KNOWN_EXECUTE_RISKS,
    PermissionConfig,
    PermissionDecision,
    PermissionProfile,
    ToolPermissionConfig,
)

SkillSelection = str | list[str]

CONFIG_DIR = Path(".ruyi_agent") / "config"
MCP_CONFIG_PATH = CONFIG_DIR / "mcp_servers.toml"
AGENTS_CONFIG_PATH = CONFIG_DIR / "agents.toml"
LLM_PROVIDERS_CONFIG_PATH = CONFIG_DIR / "llm_providers.toml"
PERMISSIONS_CONFIG_PATH = CONFIG_DIR / "permissions.toml"


@dataclass(slots=True)
class LocalWorkerSpec:
    # 为什么引入这个规格对象：本地 worker 需要一份独立于远端引用结构的可执行定义。
    name: str
    description: str
    system_prompt: str
    model: Any
    tools: list[Any]
    memory: list[str]
    skills: SkillSelection
    permission_profile: str | None = None
    delegation_local_worker_specs: dict[str, LocalWorkerSpec] | None = None
    delegation_remote_refs: dict[str, RemoteRef] | None = None
    build_delegation_tools: Callable[[], list[Any]] | None = None
    tool_search: bool = False
    tool_search_registry: MCPRegistry | None = None
    tool_search_server_names: list[str] = field(default_factory=list)
    tool_search_tool_names: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RemoteRef:
    # 为什么引入远端引用：远端目标只有连接信息，不应被误建模成可本地执行的 worker。
    name: str
    description: str
    url: str
    remote_agent_name: str
    auth: dict[str, Any] | None = None


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
}
REMOTE_REF_REQUIRED_FIELDS = {
    "kind",
    "public",
    "name",
    "description",
    "url",
    "remote_agent_name",
}
REMOTE_REF_ALLOWED_FIELDS = REMOTE_REF_REQUIRED_FIELDS | {"auth"}
SUPPORTED_MODEL_PROVIDERS = {
    "anthropic",
    "deepseek",
    "litellm",
    "moonshot",
    "openai",
    "openai_codex",
    "openrouter",
}
RESERVED_PROVIDER_INIT_KWARGS = {"api_key", "base_url", "model", "model_provider"}


@dataclass(slots=True)
class LLMProviderSpec:
    name: str
    kind: str
    base_url: str | None = None
    api_key_env: str | None = None
    init_kwargs: dict[str, Any] = field(default_factory=dict)


def _validate_required_fields(
    agent_name: str,
    agent_config: dict[str, Any],
    *,
    required_fields: set[str],
) -> None:
    """校验 agent 配置是否包含所有必填字段，缺失时抛出 ValueError。"""
    missing = sorted(field for field in required_fields if field not in agent_config)
    if missing:
        missing_fields = ", ".join(missing)
        raise ValueError(
            f"Agent '{agent_name}' is missing required fields: {missing_fields}"
        )


def _validate_allowed_fields(
    agent_name: str,
    agent_config: dict[str, Any],
    *,
    allowed_fields: set[str],
) -> None:
    """校验 agent 配置不含未知字段，存在多余字段时抛出 ValueError。"""
    unexpected = sorted(field for field in agent_config if field not in allowed_fields)
    if unexpected:
        unexpected_fields = ", ".join(unexpected)
        raise ValueError(
            f"Agent '{agent_name}' has unexpected fields: {unexpected_fields}"
        )


def _validate_agent_kind(agent_name: str, agent_config: dict[str, Any]) -> None:
    """校验 agent kind 合法，并按 kind 分派字段校验。"""
    kind = agent_config.get("kind")
    if kind not in {"local", "remote_ref"}:
        raise ValueError(
            f"Agent '{agent_name}' has unsupported kind: {kind!r}. "
            "Expected one of: 'local', 'remote_ref'."
        )
    if not isinstance(agent_config.get("public"), bool):
        raise ValueError(f"Agent '{agent_name}' field 'public' must be a boolean.")

    if kind == "local":
        _validate_required_fields(
            agent_name,
            agent_config,
            required_fields=LOCAL_AGENT_REQUIRED_FIELDS,
        )
        _validate_allowed_fields(
            agent_name,
            agent_config,
            allowed_fields=LOCAL_AGENT_ALLOWED_FIELDS,
        )
        permission_profile = agent_config.get("permission_profile")
        if permission_profile is not None and not isinstance(
            permission_profile, str
        ):
            raise ValueError(
                f"Agent '{agent_name}' field 'permission_profile' must be a string."
            )
        for field in ("provider", "model"):
            value = agent_config.get(field)
            if value is not None and not isinstance(value, str):
                raise ValueError(
                    f"Agent '{agent_name}' field '{field}' must be a string."
                )
        tool_search = agent_config.get("tool_search")
        if tool_search is not None and not isinstance(tool_search, bool):
            raise ValueError(
                f"Agent '{agent_name}' field 'tool_search' must be a boolean."
            )
        return

    _validate_required_fields(
        agent_name,
        agent_config,
        required_fields=REMOTE_REF_REQUIRED_FIELDS,
    )
    _validate_allowed_fields(
        agent_name,
        agent_config,
        allowed_fields=REMOTE_REF_ALLOWED_FIELDS,
    )


def _validate_main_agent_reference(
    main_agent_name: str,
    agent_configs: dict[str, dict[str, Any]],
) -> None:
    """校验 main_agent 存在且 kind 为 local。"""
    if main_agent_name not in agent_configs:
        raise ValueError(f"main_agent '{main_agent_name}' is not defined in [agents].")

    main_agent_config = agent_configs[main_agent_name]
    if main_agent_config["kind"] != "local":
        raise ValueError(
            f"main_agent '{main_agent_name}' must reference an agent with kind='local'."
        )


def _validate_worker_targets(agent_configs: dict[str, dict[str, Any]]) -> None:
    """校验每个 local agent 的 workers 列表中的目标都存在且 kind 合法。"""
    for agent_name, agent_config in agent_configs.items():
        if agent_config["kind"] != "local":
            continue
        for target_name in agent_config.get("workers", []):
            if target_name not in agent_configs:
                raise ValueError(
                    f"Agent '{agent_name}' references unknown worker target "
                    f"'{target_name}'."
                )
            target_kind = agent_configs[target_name]["kind"]
            if target_kind not in {"local", "remote_ref"}:
                raise ValueError(
                    f"Agent '{agent_name}' references non-spawnable target "
                    f"'{target_name}' of kind '{target_kind}'."
                )


def _validate_no_self_workers(agent_configs: dict[str, dict[str, Any]]) -> None:
    """禁止 agent 把自身列入 workers，防止直接自调用。"""
    for agent_name, agent_config in agent_configs.items():
        if agent_config["kind"] != "local":
            continue
        if agent_name in agent_config.get("workers", []):
            raise ValueError(f"Agent '{agent_name}' cannot list itself in workers.")


def _validate_local_worker_graph_acyclic(
    agent_configs: dict[str, dict[str, Any]],
) -> None:
    """用 DFS 检测本地 worker 图中是否存在环，存在时抛出 ValueError 并列出环路。"""
    local_edges: dict[str, list[str]] = {}
    for agent_name, agent_config in agent_configs.items():
        if agent_config["kind"] != "local":
            continue
        local_edges[agent_name] = [
            target_name
            for target_name in agent_config.get("workers", [])
            if agent_configs[target_name]["kind"] == "local"
        ]

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
        for target_name in local_edges.get(agent_name, []):
            visit(target_name)
        path.pop()
        visiting.remove(agent_name)
        visited.add(agent_name)

    for agent_name in local_edges:
        visit(agent_name)


def validate_agent_configs(
    main_agent_name: str,
    agent_configs: dict[str, dict[str, Any]],
) -> None:
    """对所有 agent 配置做完整校验：字段合法性、main agent 引用、worker 目标、自引用和环路。"""
    # 为什么集中做字段校验：配置模型要在 loader 层收口，不能把歧义留到 runtime。
    for agent_name, agent_config in agent_configs.items():
        _validate_agent_kind(agent_name, agent_config)
    _validate_main_agent_reference(main_agent_name, agent_configs)
    _validate_worker_targets(agent_configs)
    _validate_no_self_workers(agent_configs)
    _validate_local_worker_graph_acyclic(agent_configs)


def load_toml_config(path: Path) -> dict[str, Any]:
    """读取并解析指定路径的 TOML 文件，返回顶层字典。"""
    # 为什么集中读取 TOML：让配置入口固定，后续做校验、默认值和错误包装时不需要改业务代码。
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(
            f"Invalid TOML in {path}: {exc}. "
            "TOML requires quote string values, including model names, URLs, "
            'and Windows paths. Example: model = "qwen/qwen3.6-plus".'
        ) from exc


def _config_path(filename: str) -> Path:
    return resolve_ruyi_paths().config_dir / filename


def load_mcp_server_configs(path: Path | None = None) -> dict[str, dict[str, Any]]:
    """从 mcp_servers.toml 读取 MCP server 配置列表。"""
    # 为什么单独暴露 MCP 配置读取：让 registry 初始化和配置来源解耦，新增 MCP 只改配置文件。
    return load_toml_config(path or _config_path("mcp_servers.toml"))["mcp_servers"]


def load_agent_configs(
    path: Path | None = None,
) -> tuple[str, dict[str, dict[str, Any]]]:
    """从 agents.toml 读取并校验 agent 配置，返回 (main_agent_name, agent_configs)。"""
    # 为什么统一读取 agent 配置：让 main agent 和 worker 都共享同一套声明式来源。
    data = load_toml_config(path or _config_path("agents.toml"))
    main_agent_name = data["main_agent"]
    agent_configs = data["agents"]
    validate_agent_configs(main_agent_name, agent_configs)
    return main_agent_name, agent_configs


def load_llm_provider_configs(
    path: Path | None = None,
) -> dict[str, LLMProviderSpec]:
    """从 llm_providers.toml 读取 provider 定义，集中管理 base_url/api_key_env。"""
    data = load_toml_config(path or _config_path("llm_providers.toml"))
    raw_providers = data.get("providers", {})
    if not isinstance(raw_providers, dict):
        raise ValueError("providers must be a table")

    providers: dict[str, LLMProviderSpec] = {}
    for provider_name, raw_provider in raw_providers.items():
        if not isinstance(provider_name, str):
            raise ValueError("providers keys must be strings")
        if not isinstance(raw_provider, dict):
            raise ValueError(f"providers.{provider_name} must be a table")

        kind = raw_provider.get("kind")
        if not isinstance(kind, str) or not kind.strip():
            raise ValueError(f"providers.{provider_name}.kind must be a non-empty string")
        if kind not in SUPPORTED_MODEL_PROVIDERS:
            allowed = ", ".join(sorted(SUPPORTED_MODEL_PROVIDERS))
            raise ValueError(
                f"providers.{provider_name}.kind must be one of: {allowed}"
            )

        base_url = raw_provider.get("base_url")
        if base_url is not None and not isinstance(base_url, str):
            raise ValueError(f"providers.{provider_name}.base_url must be a string")

        api_key_env = raw_provider.get("api_key_env")
        if api_key_env is not None and not isinstance(api_key_env, str):
            raise ValueError(f"providers.{provider_name}.api_key_env must be a string")
        if kind != "openai_codex" and (api_key_env is None or not api_key_env.strip()):
            raise ValueError(
                f"providers.{provider_name}.api_key_env must be a non-empty string"
            )

        init_kwargs = raw_provider.get("init_kwargs", {})
        if not isinstance(init_kwargs, dict):
            raise ValueError(f"providers.{provider_name}.init_kwargs must be a table")
        _validate_provider_init_kwargs(provider_name, init_kwargs)

        allowed_keys = {"kind", "base_url", "api_key_env", "init_kwargs"}
        unexpected = sorted(key for key in raw_provider if key not in allowed_keys)
        if unexpected:
            unexpected_fields = ", ".join(unexpected)
            raise ValueError(
                f"providers.{provider_name} has unexpected fields: {unexpected_fields}"
            )

        providers[provider_name] = LLMProviderSpec(
            name=provider_name,
            kind=kind,
            base_url=base_url,
            api_key_env=api_key_env,
            init_kwargs=dict(init_kwargs),
        )

    return providers


def _parse_permission_decision(value: Any, *, path: str) -> PermissionDecision:
    if not isinstance(value, str):
        raise ValueError(f"{path}.policy must be a string")
    try:
        return PermissionDecision(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in PermissionDecision)
        raise ValueError(f"{path}.policy must be one of: {allowed}") from exc


def _parse_allowed_decisions(value: Any, *, path: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(item, str) for item in value
    ):
        raise ValueError(f"{path}.allowed_decisions must be a string list")
    unexpected = sorted(
        item for item in value if item not in {"approve", "edit", "reject"}
    )
    if unexpected:
        raise ValueError(
            f"{path}.allowed_decisions has unsupported values: "
            + ", ".join(unexpected)
        )
    return list(value)


def _parse_tool_permission(raw: Any, *, path: str) -> ToolPermissionConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a table")
    if "policy" not in raw:
        raise ValueError(f"{path}.policy is required")
    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"{path}.description must be a string")
    return ToolPermissionConfig(
        policy=_parse_permission_decision(raw["policy"], path=path),
        allowed_decisions=_parse_allowed_decisions(
            raw.get("allowed_decisions"), path=path
        ),
        description=description,
    )


def _parse_execute_rule(raw: Any, *, path: str) -> ExecuteRuleConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a table")
    match = raw.get("match")
    if not isinstance(match, list) or not all(
        isinstance(item, str) and item for item in match
    ):
        raise ValueError(f"{path}.match must be a non-empty string list")
    if "policy" not in raw:
        raise ValueError(f"{path}.policy is required")
    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"{path}.description must be a string")
    return ExecuteRuleConfig(
        match=list(match),
        policy=_parse_permission_decision(raw["policy"], path=path),
        allowed_decisions=_parse_allowed_decisions(
            raw.get("allowed_decisions"), path=path
        ),
        description=description,
    )


def _parse_execute_review_risks(value: Any, *, path: str) -> set[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ValueError(f"{path} must be a string list")
    unexpected = sorted(set(value) - KNOWN_EXECUTE_RISKS)
    if unexpected:
        raise ValueError(
            f"{path} has unsupported risk values: " + ", ".join(unexpected)
        )
    return set(value)


def load_permission_config(
    path: Path | None = None,
) -> PermissionConfig:
    data = load_toml_config(path or _config_path("permissions.toml"))
    default_profile = data.get("default_profile")
    if not isinstance(default_profile, str) or not default_profile:
        raise ValueError("permissions.default_profile must be a non-empty string")
    raw_profiles = data.get("profiles")
    if not isinstance(raw_profiles, dict) or not raw_profiles:
        raise ValueError("permissions profiles must be a non-empty table")

    profiles: dict[str, PermissionProfile] = {}
    for profile_name, raw_profile in raw_profiles.items():
        if not isinstance(profile_name, str) or not profile_name:
            raise ValueError("permission profile names must be non-empty strings")
        if not isinstance(raw_profile, dict):
            raise ValueError(f"profiles.{profile_name} must be a table")
        description = raw_profile.get("description")
        if description is not None and not isinstance(description, str):
            raise ValueError(f"profiles.{profile_name}.description must be a string")

        raw_tools = raw_profile.get("tools", {})
        if not isinstance(raw_tools, dict):
            raise ValueError(f"profiles.{profile_name}.tools must be a table")
        tools = {
            tool_name: _parse_tool_permission(
                raw_tool,
                path=f"profiles.{profile_name}.tools.{tool_name}",
            )
            for tool_name, raw_tool in raw_tools.items()
        }

        raw_execute = raw_profile.get("execute", {})
        if raw_execute is None:
            raw_execute = {}
        if not isinstance(raw_execute, dict):
            raise ValueError(f"profiles.{profile_name}.execute must be a table")
        raw_rules = raw_execute.get("rules", [])
        if not isinstance(raw_rules, list):
            raise ValueError(f"profiles.{profile_name}.execute.rules must be a list")
        execute_rules = [
            _parse_execute_rule(
                raw_rule,
                path=f"profiles.{profile_name}.execute.rules[{idx}]",
            )
            for idx, raw_rule in enumerate(raw_rules)
        ]
        profiles[profile_name] = PermissionProfile(
            name=profile_name,
            description=description,
            tools=tools,
            execute_rules=execute_rules,
            execute_review_risks=_parse_execute_review_risks(
                raw_execute.get("review_risks"),
                path=f"profiles.{profile_name}.execute.review_risks",
            ),
        )

    if default_profile not in profiles:
        raise ValueError(
            f"permissions.default_profile '{default_profile}' is not defined"
        )
    return PermissionConfig(default_profile=default_profile, profiles=profiles)


def to_backend_paths(paths: list[str], base_dir: str) -> list[str]:
    """把配置中的相对路径列表拼接到 backend 根目录，返回绝对路径列表。"""
    # 为什么做路径转换：配置文件应该表达项目语义路径，运行时再映射到 backend/sandbox 中的真实位置。
    base = PurePosixPath(base_dir or "/")
    return [
        str(PurePosixPath(path) if PurePosixPath(path).is_absolute() else base / path)
        for path in paths
    ]


def parse_skill_selection(raw: Any) -> SkillSelection:
    """Parse agent skills visibility config.

    `skills` no longer names backend directories. It now controls which discovered
    skill names an agent can see.
    """

    if isinstance(raw, str):
        value = raw.strip().lower()
        if value in {"inherit", "none"}:
            return value
        raise ValueError(
            "Agent config field 'skills' must be 'inherit', 'none', "
            "or a list of skill names."
        )
    if isinstance(raw, list):
        names: list[str] = []
        for item in raw:
            if not isinstance(item, str) or not item.strip():
                raise ValueError(
                    "Agent config field 'skills' list must contain non-empty strings."
                )
            names.append(item.strip())
        return names
    raise ValueError(
        "Agent config field 'skills' must be 'inherit', 'none', "
        "or a list of skill names."
    )


def _resolve_api_key_from_provider(
    provider: LLMProviderSpec,
    *,
    getenv: Callable[[str], str | None],
) -> str | None:
    if not provider.api_key_env:
        return None
    api_key = getenv(provider.api_key_env)
    if api_key:
        return api_key
    raise ValueError(
        f"Environment variable {provider.api_key_env!r} configured by providers.{provider.name}.api_key_env is not set."
    )


def _validate_provider_init_kwargs(
    provider_name: str,
    init_kwargs: dict[str, Any],
) -> None:
    reserved = sorted(set(init_kwargs) & RESERVED_PROVIDER_INIT_KWARGS)
    if reserved:
        raise ValueError(
            f"providers.{provider_name}.init_kwargs cannot include reserved keys: "
            + ", ".join(reserved)
        )


def _get_chat_moonshot_class() -> Any:
    """延迟导入 Moonshot 集成，避免未使用该 provider 时强制安装。"""
    try:
        from langchain_moonshot import ChatMoonshot
    except ImportError as exc:
        raise ValueError(
            "Provider kind='moonshot' requires the 'langchain-moonshot' package. "
            "Install it with `uv add langchain-moonshot`."
        ) from exc
    return ChatMoonshot


def _get_chat_deepseek_class() -> Any:
    """延迟导入 DeepSeek 集成，避免未使用该 provider 时强制安装。"""
    try:
        from langchain_deepseek import ChatDeepSeek
    except ImportError as exc:
        raise ValueError(
            "Provider kind='deepseek' requires the 'langchain-deepseek' package. "
            "Install it with `uv add langchain-deepseek`."
        ) from exc
    return ChatDeepSeek


def _message_has_tool_calls(message: Any, encoded: dict[str, Any]) -> bool:
    if isinstance(message, dict):
        return bool(
            message.get("tool_calls")
            or message.get("invalid_tool_calls")
            or message.get("function_call")
            or encoded.get("tool_calls")
            or encoded.get("function_call")
        )
    return bool(
        getattr(message, "tool_calls", None)
        or getattr(message, "invalid_tool_calls", None)
        or getattr(message, "additional_kwargs", {}).get("tool_calls")
        or encoded.get("tool_calls")
        or encoded.get("function_call")
    )


def _reasoning_content_for_request(message: Any, encoded: dict[str, Any]) -> str | None:
    if isinstance(message, dict):
        additional_kwargs = message.get("additional_kwargs", {})
        reasoning_content = message.get("reasoning_content")
        if isinstance(reasoning_content, str) and reasoning_content:
            return reasoning_content
        reasoning = message.get("reasoning")
        if isinstance(reasoning, str) and reasoning:
            return reasoning
    else:
        additional_kwargs = getattr(message, "additional_kwargs", {})
    reasoning_content = additional_kwargs.get("reasoning_content")
    if isinstance(reasoning_content, str) and reasoning_content:
        return reasoning_content

    reasoning = additional_kwargs.get("reasoning")
    if isinstance(reasoning, str) and reasoning:
        return reasoning

    if _message_has_tool_calls(message, encoded):
        return " "
    return None


def _messages_from_model_input(model: Any, input_: Any) -> list[Any]:
    if isinstance(input_, list):
        return input_
    convert_input = getattr(model, "_convert_input", None)
    if callable(convert_input):
        converted = convert_input(input_)
        to_messages = getattr(converted, "to_messages", None)
        if callable(to_messages):
            return list(to_messages())
    return []


def _patch_tool_call_reasoning_history(
    original_messages: list[Any],
    payload: dict[str, Any],
) -> None:
    encoded_messages = payload.get("messages")
    if not isinstance(encoded_messages, list):
        return

    for original, encoded in zip(original_messages, encoded_messages, strict=False):
        if not isinstance(encoded, dict):
            continue
        if not _message_has_tool_calls(original, encoded):
            continue
        reasoning_content = _reasoning_content_for_request(original, encoded)
        if reasoning_content is not None:
            encoded["reasoning_content"] = reasoning_content


def _build_deepseek_model(model: str, **kwargs: Any) -> Any:
    chat_deepseek = _get_chat_deepseek_class()

    class ReasoningChatDeepSeek(chat_deepseek):  # type: ignore[misc, valid-type]
        def _get_request_payload(
            self,
            input_: Any,
            *args: Any,
            **kwargs: Any,
        ) -> dict[str, Any]:
            payload = super()._get_request_payload(input_, *args, **kwargs)
            _patch_tool_call_reasoning_history(
                _messages_from_model_input(self, input_),
                payload,
            )
            return payload

    return ReasoningChatDeepSeek(model=model, **kwargs)


def _get_chat_litellm_class() -> Any:
    """延迟导入 LiteLLM 集成，避免未使用该 provider 时强制安装。"""
    try:
        from langchain_litellm import ChatLiteLLM
    except ImportError as exc:
        raise ValueError(
            "Provider kind='litellm' requires the 'langchain-litellm' package. "
            "Install it with `uv add langchain-litellm`."
        ) from exc
    return ChatLiteLLM


def _build_litellm_model(model: str, **kwargs: Any) -> Any:
    chat_litellm = _get_chat_litellm_class()
    if "base_url" in kwargs:
        kwargs["api_base"] = kwargs.pop("base_url")
    return chat_litellm(model=model, **kwargs)


def _build_openai_codex_model(model: str, **kwargs: Any) -> Any:
    """延迟导入 Codex 模型适配器，避免普通 provider 强制加载 OpenAI SDK 细节。"""
    from ruyi_agent.integrations.openai_codex import CodexChatModel

    return CodexChatModel(model=model, **kwargs)


def _build_provider_kwargs(
    provider: LLMProviderSpec,
    *,
    getenv: Callable[[str], str | None],
) -> dict[str, Any]:
    """构造 provider 初始化参数，保留配置里的 provider-specific init_kwargs。"""
    _validate_provider_init_kwargs(provider.name, provider.init_kwargs)
    kwargs: dict[str, Any] = dict(provider.init_kwargs)
    api_key = _resolve_api_key_from_provider(provider, getenv=getenv)
    if api_key:
        kwargs["api_key"] = api_key
    if provider.base_url:
        kwargs["base_url"] = provider.base_url
    return kwargs


def build_chat_model_from_config(
    agent_config: dict[str, Any],
    *,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
) -> Any:
    """按配置构造 LangChain chat model，支持官方和 OpenAI-compatible provider。"""
    model_name = agent_config.get("model")
    provider_name = agent_config.get("provider")
    if not model_name:
        raise ValueError("Agent config field 'model' must be set.")
    if not provider_name:
        raise ValueError("Agent config field 'provider' must be set.")
    if provider_name not in providers:
        raise ValueError(f"Unknown provider: {provider_name!r}")
    provider = providers[provider_name]

    kwargs = _build_provider_kwargs(provider, getenv=getenv)
    if provider.kind == "moonshot":
        chat_moonshot = _get_chat_moonshot_class()
        return chat_moonshot(model=model_name, **kwargs)
    if provider.kind == "deepseek":
        return _build_deepseek_model(model_name, **kwargs)
    if provider.kind == "litellm":
        return _build_litellm_model(model_name, **kwargs)
    if provider.kind == "openai_codex":
        return _build_openai_codex_model(model_name, **kwargs)

    return init_chat_model(model_name, model_provider=provider.kind, **kwargs)


def build_remote_ref(
    agent_name: str,
    agent_configs: dict[str, dict[str, Any]],
) -> RemoteRef:
    """从配置构造 RemoteRef，仅保留连接信息，不含本地执行定义。"""
    # 为什么单独构造远端引用：远端目标没有本地运行时定义，只保留连接信息。
    agent_config = agent_configs[agent_name]
    if agent_config["kind"] != "remote_ref":
        raise ValueError(f"Agent '{agent_name}' is not a remote_ref.")

    return RemoteRef(
        name=agent_config["name"],
        description=agent_config["description"],
        url=agent_config["url"],
        remote_agent_name=agent_config["remote_agent_name"],
        auth=agent_config.get("auth"),
    )


async def build_local_worker_spec(
    agent_name: str,
    agent_configs: dict[str, dict[str, Any]],
    registry: MCPRegistry,
    *,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
    home_dir: str,
    skills_root: str,
) -> LocalWorkerSpec:
    """从配置和 MCP registry 构造单个 local agent 的 LocalWorkerSpec。"""
    # 为什么单独构造本地 agent 规格：本地可执行目标需要统一配置来源，但不能复用远端引用结构。
    agent_config = agent_configs[agent_name]
    if agent_config["kind"] != "local":
        raise ValueError(f"Agent '{agent_name}' is not a local agent.")

    tool_search = bool(agent_config.get("tool_search", False))
    server_names = list(agent_config.get("server_names", []))
    tool_names = list(agent_config.get("tool_names", []))
    tools = (
        []
        if tool_search
        else await registry.resolve_tools(
            server_names=server_names,
            tool_names=tool_names,
        )
    )
    return LocalWorkerSpec(
        name=agent_config["name"],
        description=agent_config["description"],
        system_prompt=agent_config["system_prompt"],
        model=build_chat_model_from_config(
            agent_config,
            providers=providers,
            getenv=getenv,
        ),
        tools=tools,
        memory=to_backend_paths(agent_config.get("memory", []), home_dir),
        skills=parse_skill_selection(agent_config.get("skills", "inherit")),
        permission_profile=agent_config.get("permission_profile"),
        tool_search=tool_search,
        tool_search_registry=registry if tool_search else None,
        tool_search_server_names=server_names,
        tool_search_tool_names=tool_names,
    )


async def build_all_local_worker_specs(
    agent_configs: dict[str, dict[str, Any]],
    registry: MCPRegistry,
    *,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
    home_dir: str,
    skills_root: str,
) -> dict[str, LocalWorkerSpec]:
    """构造所有 local agent 的基础 LocalWorkerSpec，不含 delegation scope。"""
    # 为什么先构造全量基础 spec：main/gateway/worker 都应从同一批本地 agent 定义中选择，
    # delegation scope 则在装配层统一注入，避免 main agent 特权化。
    specs: dict[str, LocalWorkerSpec] = {}
    for agent_name, agent_config in agent_configs.items():
        if agent_config["kind"] != "local":
            continue
        specs[agent_name] = await build_local_worker_spec(
            agent_name,
            agent_configs,
            registry,
            providers=providers,
            getenv=getenv,
            home_dir=home_dir,
            skills_root=skills_root,
        )
    return specs


async def build_all_remote_refs(
    agent_configs: dict[str, dict[str, Any]],
) -> dict[str, RemoteRef]:
    """构造所有 remote_ref 的 RemoteRef 字典，供后续按 scope 过滤使用。"""
    # 为什么构造全量 remote_ref：Gateway public scope 和 agent delegation scope 是两套选择规则，
    # 必须先有完整远端引用集合，再按各自 scope 过滤。
    refs: dict[str, RemoteRef] = {}
    for agent_name, agent_config in agent_configs.items():
        if agent_config["kind"] != "remote_ref":
            continue
        refs[agent_name] = build_remote_ref(agent_name, agent_configs)
    return refs


def select_local_worker_specs_for_agent(
    agent_name: str,
    agent_configs: dict[str, dict[str, Any]],
    all_local_specs: dict[str, LocalWorkerSpec],
) -> dict[str, LocalWorkerSpec]:
    """从全量 local spec 中筛选出指定 agent 的 workers 配置里的本地目标。"""
    agent_config = agent_configs[agent_name]
    if agent_config["kind"] != "local":
        raise ValueError(f"Agent '{agent_name}' is not a local agent.")
    return {
        target_name: all_local_specs[target_name]
        for target_name in agent_config.get("workers", [])
        if target_name in all_local_specs
    }


def select_remote_refs_for_agent(
    agent_name: str,
    agent_configs: dict[str, dict[str, Any]],
    all_remote_refs: dict[str, RemoteRef],
) -> dict[str, RemoteRef]:
    """从全量 remote ref 中筛选出指定 agent 的 workers 配置里的远端目标。"""
    agent_config = agent_configs[agent_name]
    if agent_config["kind"] != "local":
        raise ValueError(f"Agent '{agent_name}' is not a local agent.")
    return {
        target_name: all_remote_refs[target_name]
        for target_name in agent_config.get("workers", [])
        if target_name in all_remote_refs
    }


def select_public_local_worker_specs(
    agent_configs: dict[str, dict[str, Any]],
    all_local_specs: dict[str, LocalWorkerSpec],
) -> dict[str, LocalWorkerSpec]:
    """从全量 local spec 中筛选出标记为 public=true 的 agent，供 Gateway 入口使用。"""
    return {
        agent_name: spec
        for agent_name, spec in all_local_specs.items()
        if agent_configs[agent_name]["public"]
    }


async def build_public_remote_refs(
    agent_configs: dict[str, dict[str, Any]],
) -> dict[str, RemoteRef]:
    """构造标记为 public=true 的 remote_ref 字典，供 Gateway 入口使用。"""
    refs: dict[str, RemoteRef] = {}
    for agent_name, agent_config in agent_configs.items():
        if agent_config["kind"] != "remote_ref" or not agent_config["public"]:
            continue
        refs[agent_name] = build_remote_ref(agent_name, agent_configs)
    return refs
