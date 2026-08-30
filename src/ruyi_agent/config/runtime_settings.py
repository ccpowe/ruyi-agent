"""Typed runtime configuration and the single environment compatibility edge.

The runtime TOML document is deliberately parsed in one place. Consumers get
immutable dataclasses; the environment projection exists only for third-party
SDKs, dynamic provider/remote-reference secrets, and legacy scripts.
"""

from __future__ import annotations

import copy
import math
import os
import re
import tomllib
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field, replace
from importlib.resources import files
from ipaddress import IPv4Address, IPv6Address
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

from ruyi_agent.config.errors import ConfigError
from ruyi_agent.config.paths import RuyiPaths, resolve_ruyi_paths
from ruyi_agent.config.url_validation import validate_http_url
from ruyi_agent.runtime.delegation.context import validate_node_id


BackendKind = Literal["local", "daytona"]
FeishuDomain = Literal["feishu", "lark"]
FeishuConnectionMode = Literal["websocket"]
FeishuGroupPolicy = Literal["disabled", "open", "allowlist"]
FeishuAckMode = Literal["reaction", "message", "off"]


@dataclass(frozen=True, slots=True)
class CredentialsSettings:
    """Optional model credentials kept out of ordinary runtime consumers."""

    openrouter_api_key: str | None = None
    kimi_api_key: str | None = None
    deepseek_api_key: str | None = None
    zai_api_key: str | None = None
    openai_api_key: str | None = None
    anthropic_api_key: str | None = None


ModelCredentials = CredentialsSettings


@dataclass(frozen=True, slots=True)
class LocalBackendSettings:
    timeout: int = 120
    max_output_bytes: int = 100_000
    inherit_env: bool = True


@dataclass(frozen=True, slots=True)
class DaytonaBackendSettings:
    api_key: str | None = None
    api_url: str | None = None
    target: str | None = None
    sandbox_name: str = "ruyi-agent"


@dataclass(frozen=True, slots=True)
class BackendSettings:
    kind: BackendKind = "local"
    workspace: Path = field(default_factory=Path.cwd)
    local: LocalBackendSettings = field(default_factory=LocalBackendSettings)
    daytona: DaytonaBackendSettings = field(default_factory=DaytonaBackendSettings)


@dataclass(frozen=True, slots=True)
class GatewaySettings:
    host: str = "127.0.0.1"
    port: int = 8000
    base_url: str = "http://127.0.0.1:8000"
    bearer_token: str = "dev-token"


@dataclass(frozen=True, slots=True)
class StorageSettings:
    checkpoint_db: Path
    gateway_route_db: Path
    task_db: Path
    review_audit_db: Path
    channel_session_db: Path


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    agent_node_id: str = "local-dev"
    max_delegation_depth: int = 3
    max_tasks_per_root: int = 20
    a2a_webhook_url: str | None = None
    a2a_webhook_token: str | None = None
    remote_research_token: str | None = None


@dataclass(frozen=True, slots=True)
class TelegramSettings:
    bot_token: str | None = None
    default_agent: str = "main"
    session_db: Path = field(
        default_factory=lambda: Path("data/channel_sessions.sqlite3")
    )
    update_db: Path = field(
        default_factory=lambda: Path("data/telegram_updates.sqlite3")
    )
    poll_timeout: int = 30
    api_timeout: float = 40.0
    task_poll_interval: float = 2.0
    terminal_review_grace_checks: int = 3
    message_parse_mode: str = "MarkdownV2"
    media_max_bytes: int = 50 * 1024 * 1024
    fallback_ips: tuple[str, ...] = ()
    kroki_base_url: str = "https://kroki.io"


@dataclass(frozen=True, slots=True)
class FeishuSettings:
    app_id: str | None = None
    app_secret: str | None = None
    domain: FeishuDomain = "feishu"
    connection_mode: FeishuConnectionMode = "websocket"
    default_agent: str = "main"
    session_db: Path = field(
        default_factory=lambda: Path("data/channel_sessions.sqlite3")
    )
    event_db: Path = field(default_factory=lambda: Path("data/feishu_events.sqlite3"))
    group_policy: FeishuGroupPolicy = "disabled"
    require_mention: bool = True
    allowed_users: tuple[str, ...] = ()
    allowed_groups: tuple[str, ...] = ()
    bot_name: str | None = None
    bot_open_id: str | None = None
    bot_user_id: str | None = None
    bot_union_id: str | None = None
    api_timeout: float = 10.0
    task_poll_interval: float = 2.0
    terminal_review_grace_checks: int = 3
    media_max_bytes: int = 30 * 1024 * 1024
    ack_mode: FeishuAckMode = "reaction"
    reactions: bool = True
    processing_reaction: str = "Typing"
    approval_reaction: str = "CheckMark"
    failure_reaction: str = "CrossMark"


@dataclass(frozen=True, slots=True)
class ChannelsSettings:
    telegram: TelegramSettings = field(default_factory=TelegramSettings)
    feishu: FeishuSettings = field(default_factory=FeishuSettings)


ChannelSettings = ChannelsSettings


@dataclass(frozen=True, slots=True)
class LangSmithSettings:
    tracing: bool = False
    endpoint: str = "https://api.smith.langchain.com"
    api_key: str | None = None
    project: str | None = None


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    """Complete immutable runtime settings produced by the TOML boundary."""

    paths: RuyiPaths
    credentials: CredentialsSettings
    backend: BackendSettings
    gateway: GatewaySettings
    storage: StorageSettings
    runtime: RuntimeLimits
    channels: ChannelsSettings
    langsmith: LangSmithSettings

    @property
    def workspace(self) -> Path:
        """Compatibility accessor for the canonical backend workspace."""

        return self.backend.workspace

    @property
    def limits(self) -> RuntimeLimits:
        return self.runtime

    @property
    def runtime_limits(self) -> RuntimeLimits:
        return self.runtime

    @property
    def model_credentials(self) -> CredentialsSettings:
        return self.credentials

    @property
    def telegram(self) -> TelegramSettings:
        return self.channels.telegram

    @property
    def feishu(self) -> FeishuSettings:
        return self.channels.feishu

    @classmethod
    def defaults(cls, paths: RuyiPaths | None = None) -> RuntimeSettings:
        """Build typed defaults for legacy embedding shims.

        Normal application startup always calls :func:`configure_runtime_environment`.
        This constructor is only a narrow compatibility fallback for embedders that
        still invoke an older configure hook which returns no settings object.
        """

        active_paths = paths or resolve_ruyi_paths(env={})
        data_dir = active_paths.data_dir
        storage = StorageSettings(
            checkpoint_db=data_dir / DEFAULT_STORAGE_FILES["CHECKPOINT_DB"],
            gateway_route_db=data_dir / DEFAULT_STORAGE_FILES["GATEWAY_ROUTE_DB"],
            task_db=data_dir / DEFAULT_STORAGE_FILES["TASK_DB"],
            review_audit_db=data_dir / DEFAULT_STORAGE_FILES["REVIEW_AUDIT_DB"],
            channel_session_db=data_dir / DEFAULT_STORAGE_FILES["CHANNEL_SESSION_DB"],
        )
        return cls(
            paths=active_paths,
            credentials=CredentialsSettings(),
            backend=BackendSettings(workspace=active_paths.workspace),
            gateway=GatewaySettings(),
            storage=storage,
            runtime=RuntimeLimits(),
            channels=ChannelsSettings(
                telegram=TelegramSettings(
                    session_db=storage.channel_session_db,
                    update_db=data_dir / "telegram_updates.sqlite3",
                ),
                feishu=FeishuSettings(
                    session_db=storage.channel_session_db,
                    event_db=data_dir / "feishu_events.sqlite3",
                ),
            ),
            langsmith=LangSmithSettings(),
        )


DEFAULT_STORAGE_FILES = {
    "CHECKPOINT_DB": "checkpoints.sqlite",
    "GATEWAY_ROUTE_DB": "gateway_routes.sqlite",
    "TASK_DB": "tasks.sqlite",
    "REVIEW_AUDIT_DB": "review_audit.sqlite",
    "CHANNEL_SESSION_DB": "channel_sessions.sqlite3",
}

STORAGE_FIELD_ENV = {
    "checkpoint_db": "CHECKPOINT_DB",
    "gateway_route_db": "GATEWAY_ROUTE_DB",
    "task_db": "TASK_DB",
    "review_audit_db": "REVIEW_AUDIT_DB",
    "channel_session_db": "CHANNEL_SESSION_DB",
}

MODEL_CREDENTIAL_ENV = {
    "openrouter_api_key": "OPENROUTER_API_KEY",
    "kimi_api_key": "KIMI_API_KEY",
    "deepseek_api_key": "DEEPSEEK_API_KEY",
    "zai_api_key": "ZAI_API_KEY",
    "openai_api_key": "OPENAI_API_KEY",
    "anthropic_api_key": "ANTHROPIC_API_KEY",
}

BACKEND_LOCAL_ENV = {
    "timeout": "LOCAL_BACKEND_TIMEOUT",
    "max_output_bytes": "LOCAL_BACKEND_MAX_OUTPUT_BYTES",
    "inherit_env": "LOCAL_BACKEND_INHERIT_ENV",
}

BACKEND_DAYTONA_ENV = {
    "api_key": "DAYTONA_API_KEY",
    "api_url": "DAYTONA_API_URL",
    "target": "DAYTONA_TARGET",
    "sandbox_name": "DAYTONA_SANDBOX_NAME",
}

RUNTIME_ENV = {
    "agent_node_id": "AGENT_NODE_ID",
    "max_delegation_depth": "AGENT_MAX_DELEGATION_DEPTH",
    "max_tasks_per_root": "AGENT_MAX_TASKS_PER_ROOT",
    "a2a_webhook_url": "A2A_WEBHOOK_URL",
    "a2a_webhook_token": "A2A_WEBHOOK_TOKEN",
    "remote_research_token": "REMOTE_RESEARCH_TOKEN",
}

TELEGRAM_ENV = {
    "bot_token": "TELEGRAM_BOT_TOKEN",
    "default_agent": "TELEGRAM_DEFAULT_AGENT",
    "poll_timeout": "TELEGRAM_POLL_TIMEOUT",
    "api_timeout": "TELEGRAM_API_TIMEOUT",
    "task_poll_interval": "TELEGRAM_TASK_POLL_INTERVAL",
    "terminal_review_grace_checks": "TELEGRAM_TERMINAL_REVIEW_GRACE_CHECKS",
    "message_parse_mode": "TELEGRAM_MESSAGE_PARSE_MODE",
    "media_max_bytes": "TELEGRAM_MEDIA_MAX_BYTES",
    "fallback_ips": "TELEGRAM_FALLBACK_IPS",
    "kroki_base_url": "KROKI_BASE_URL",
}

TELEGRAM_PATH_ENV = {
    "session_db": "TELEGRAM_SESSION_DB",
    "update_db": "TELEGRAM_UPDATE_DB",
}

FEISHU_ENV = {
    "app_id": "FEISHU_APP_ID",
    "app_secret": "FEISHU_APP_SECRET",
    "domain": "FEISHU_DOMAIN",
    "connection_mode": "FEISHU_CONNECTION_MODE",
    "default_agent": "FEISHU_DEFAULT_AGENT",
    "group_policy": "FEISHU_GROUP_POLICY",
    "require_mention": "FEISHU_REQUIRE_MENTION",
    "allowed_users": "FEISHU_ALLOWED_USERS",
    "allowed_groups": "FEISHU_ALLOWED_GROUPS",
    "bot_name": "FEISHU_BOT_NAME",
    "bot_open_id": "FEISHU_BOT_OPEN_ID",
    "bot_user_id": "FEISHU_BOT_USER_ID",
    "bot_union_id": "FEISHU_BOT_UNION_ID",
    "api_timeout": "FEISHU_API_TIMEOUT",
    "task_poll_interval": "FEISHU_TASK_POLL_INTERVAL",
    "terminal_review_grace_checks": "FEISHU_TERMINAL_REVIEW_GRACE_CHECKS",
    "media_max_bytes": "FEISHU_MEDIA_MAX_BYTES",
    "ack_mode": "FEISHU_ACK_MODE",
    "reactions": "FEISHU_REACTIONS",
    "processing_reaction": "FEISHU_PROCESSING_REACTION",
    "approval_reaction": "FEISHU_APPROVAL_REACTION",
    "failure_reaction": "FEISHU_FAILURE_REACTION",
}

FEISHU_PATH_ENV = {
    "session_db": "FEISHU_SESSION_DB",
    "event_db": "FEISHU_EVENT_DB",
}

LANGSMITH_ENV = {
    "tracing": "LANGSMITH_TRACING",
    "endpoint": "LANGSMITH_ENDPOINT",
    "api_key": "LANGSMITH_API_KEY",
    "project": "LANGSMITH_PROJECT",
}

# Finite and auditable compatibility surface. BACKEND_KIND,
# LOCAL_BACKEND_ROOT, and GATEWAY_* are projection keys only, never TOML
# aliases.
TABLE_SCOPED_TOML_ALIASES = {
    "model_credentials": frozenset(MODEL_CREDENTIAL_ENV.values()),
    "backend.local": frozenset(BACKEND_LOCAL_ENV.values()),
    "backend.daytona": frozenset(BACKEND_DAYTONA_ENV.values()),
    "runtime": frozenset(RUNTIME_ENV.values()),
    "channels.telegram": frozenset(
        (*TELEGRAM_ENV.values(), *TELEGRAM_PATH_ENV.values())
    ),
    "channels.feishu": frozenset((*FEISHU_ENV.values(), *FEISHU_PATH_ENV.values())),
    "langsmith": frozenset(LANGSMITH_ENV.values()),
    "storage": frozenset(STORAGE_FIELD_ENV.values()),
}
TOML_ALIAS_NAMES = frozenset(
    alias for aliases in TABLE_SCOPED_TOML_ALIASES.values() for alias in aliases
)

_ALLOWED_TOP_LEVEL = frozenset(
    {
        "model_credentials",
        "backend",
        "gateway",
        "storage",
        "runtime",
        "channels",
        "langsmith",
    }
)
_MISSING = object()
_TRUE_ENV_TOKENS = frozenset({"1", "true", "yes", "y", "on"})
_FALSE_ENV_TOKENS = frozenset({"0", "false", "no", "n", "off"})


@dataclass(frozen=True, slots=True)
class _EnvironmentValue:
    value: Any
    name: str


@dataclass(frozen=True, slots=True)
class _OverrideValue:
    value: Any


def configure_runtime_environment(
    *,
    workspace: str | Path | None = None,
    env: MutableMapping[str, str] | None = None,
    init_force: bool = False,
    init_templates: bool = False,
    overrides: Mapping[str, Mapping[str, Any]] | None = None,
    gateway_overrides: Mapping[str, Any] | None = None,
) -> RuntimeSettings:
    """Load, validate, and project runtime settings exactly once."""

    target = os.environ if env is None else env
    workspace_override = workspace
    if _is_unset(workspace_override) and target.get("RUYI_RUNTIME_CONFIGURED") == "1":
        # A re-entry keeps the workspace selected by the first configure call.
        workspace_override = target.get("RUYI_WORKSPACE")
    path_env = target
    if _is_unset(workspace_override):
        # RUYI_WORKSPACE is a loader fallback, not a path-discovery input. This
        # also lets a canonical backend.workspace override a stale or
        # platform-incompatible old variable.
        path_env = dict(target)
        path_env.pop("RUYI_WORKSPACE", None)
    paths = resolve_ruyi_paths(
        workspace=workspace_override if not _is_unset(workspace_override) else None,
        env=path_env,
    )
    if init_templates:
        ensure_ruyi_home(paths, force=init_force)
    else:
        _require_initialized_runtime_settings(paths)
        ensure_runtime_dirs(paths)

    effective_overrides = _merge_overrides(overrides, gateway_overrides)
    settings = load_runtime_settings(
        paths,
        workspace_override=(
            workspace_override if not _is_unset(workspace_override) else None
        ),
        env=target,
        overrides=effective_overrides,
    )
    apply_runtime_settings_to_env(settings, env=env)
    return settings


def ensure_runtime_dirs(paths: RuyiPaths) -> None:
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.skills_dir.mkdir(parents=True, exist_ok=True)


def ensure_ruyi_home(paths: RuyiPaths, *, force: bool = False) -> None:
    paths.ruyi_home.mkdir(parents=True, exist_ok=True)
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    paths.skills_dir.mkdir(parents=True, exist_ok=True)
    template_root = files("ruyi_agent.templates.ruyi_home")
    _copy_resource_if_missing(
        template_root.joinpath("ruyi.toml"),
        paths.ruyi_home / "ruyi.toml",
        force=force,
    )
    config_template_root = template_root.joinpath("config")
    for template in config_template_root.iterdir():
        if not template.name.endswith((".toml", ".toml.example")):
            continue
        _copy_resource_if_missing(
            template,
            paths.config_dir / template.name,
            force=force,
        )


def _require_initialized_runtime_settings(paths: RuyiPaths) -> None:
    settings_path = paths.ruyi_home / "ruyi.toml"
    if settings_path.is_file():
        return
    raise ConfigError(
        f"Ruyi config is not initialized at {paths.ruyi_home}. "
        "Run `ruyi --init` first, or set RUYI_HOME to an existing config directory."
    )


def load_runtime_settings(
    paths: RuyiPaths,
    *,
    workspace_override: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    overrides: Mapping[str, Mapping[str, Any]] | None = None,
    gateway_overrides: Mapping[str, Any] | None = None,
) -> RuntimeSettings:
    """Parse one runtime TOML document into immutable typed submodels."""

    settings_path = (paths.ruyi_home / "ruyi.toml").resolve()
    data = _load_settings_toml(settings_path)
    effective_overrides = _merge_overrides(overrides, gateway_overrides)
    if effective_overrides:
        data = _apply_overrides(data, effective_overrides, settings_path)
    _validate_shape(data, settings_path)
    source_env = os.environ if env is None else env

    backend = _table(data, "backend", settings_path)
    backend_local = _table(backend, "local", settings_path, parent="backend")
    backend_daytona = _table(backend, "daytona", settings_path, parent="backend")
    gateway = _table(data, "gateway", settings_path)
    storage = _table(data, "storage", settings_path)
    credentials = _table(data, "model_credentials", settings_path)
    runtime = _table(data, "runtime", settings_path)
    channels = _table(data, "channels", settings_path)
    telegram = _table(channels, "telegram", settings_path, parent="channels")
    feishu = _table(channels, "feishu", settings_path, parent="channels")
    langsmith = _table(data, "langsmith", settings_path)

    selected_workspace, workspace_source = _select_workspace(
        backend,
        paths=paths,
        workspace_override=workspace_override,
        env=source_env,
    )
    workspace = _path_value(
        selected_workspace,
        default=paths.workspace,
        base=None,
        source=workspace_source,
        path=settings_path,
    )
    resolved_paths = replace(paths, workspace=workspace)

    backend_settings = BackendSettings(
        kind=_enum_value(
            _selected(
                backend,
                "kind",
                None,
                default="local",
                env=source_env,
                allow_alias=False,
                allow_env=False,
                table_path="backend",
            ),
            allowed={"local", "daytona"},
            path=settings_path,
            field="backend.kind",
        ),
        workspace=workspace,
        local=LocalBackendSettings(
            timeout=_int_value(
                _selected(
                    backend_local,
                    "timeout",
                    BACKEND_LOCAL_ENV["timeout"],
                    default=120,
                    env=source_env,
                    table_path="backend.local",
                ),
                minimum=1,
                path=settings_path,
                field="backend.local.timeout",
            ),
            max_output_bytes=_int_value(
                _selected(
                    backend_local,
                    "max_output_bytes",
                    BACKEND_LOCAL_ENV["max_output_bytes"],
                    default=100_000,
                    env=source_env,
                    table_path="backend.local",
                ),
                minimum=1,
                path=settings_path,
                field="backend.local.max_output_bytes",
            ),
            inherit_env=_bool_value(
                _selected(
                    backend_local,
                    "inherit_env",
                    BACKEND_LOCAL_ENV["inherit_env"],
                    default=True,
                    env=source_env,
                    table_path="backend.local",
                ),
                path=settings_path,
                field="backend.local.inherit_env",
            ),
        ),
        daytona=DaytonaBackendSettings(
            api_key=_optional_string(
                _selected(
                    backend_daytona,
                    "api_key",
                    BACKEND_DAYTONA_ENV["api_key"],
                    default=None,
                    env=source_env,
                    table_path="backend.daytona",
                ),
                path=settings_path,
                field="backend.daytona.api_key",
            ),
            api_url=_optional_http_url(
                _selected(
                    backend_daytona,
                    "api_url",
                    BACKEND_DAYTONA_ENV["api_url"],
                    default=None,
                    env=source_env,
                    table_path="backend.daytona",
                ),
                path=settings_path,
                field="backend.daytona.api_url",
            ),
            target=_optional_string(
                _selected(
                    backend_daytona,
                    "target",
                    BACKEND_DAYTONA_ENV["target"],
                    default=None,
                    env=source_env,
                    table_path="backend.daytona",
                ),
                path=settings_path,
                field="backend.daytona.target",
            ),
            sandbox_name=_non_empty_string(
                _selected(
                    backend_daytona,
                    "sandbox_name",
                    BACKEND_DAYTONA_ENV["sandbox_name"],
                    default="ruyi-agent",
                    env=source_env,
                    table_path="backend.daytona",
                ),
                path=settings_path,
                field="backend.daytona.sandbox_name",
            ),
        ),
    )

    gateway_settings = GatewaySettings(
        host=_gateway_host(
            _selected(
                gateway,
                "host",
                None,
                default="127.0.0.1",
                env=source_env,
                allow_alias=False,
                allow_env=False,
                table_path="gateway",
            ),
            path=settings_path,
            field="gateway.host",
        ),
        port=_int_value(
            _selected(
                gateway,
                "port",
                None,
                default=8000,
                env=source_env,
                allow_alias=False,
                allow_env=False,
                table_path="gateway",
            ),
            minimum=1,
            maximum=65_535,
            path=settings_path,
            field="gateway.port",
        ),
        base_url=_http_url(
            _selected(
                gateway,
                "base_url",
                None,
                default="http://127.0.0.1:8000",
                env=source_env,
                allow_alias=False,
                allow_env=False,
                table_path="gateway",
            ),
            path=settings_path,
            field="gateway.base_url",
        ),
        bearer_token=_non_empty_string(
            _selected(
                gateway,
                "bearer_token",
                None,
                default="dev-token",
                env=source_env,
                allow_alias=False,
                allow_env=False,
                table_path="gateway",
            ),
            path=settings_path,
            field="gateway.bearer_token",
        ),
    )

    storage_values: dict[str, Path] = {}
    for field_name, env_name in STORAGE_FIELD_ENV.items():
        selected = _selected(
            storage,
            field_name,
            env_name,
            default=paths.data_dir / DEFAULT_STORAGE_FILES[env_name],
            env=source_env,
            allow_env=False,
            table_path="storage",
        )
        storage_values[field_name] = _path_value(
            selected,
            default=paths.data_dir / DEFAULT_STORAGE_FILES[env_name],
            base=paths.ruyi_home,
            source="storage." + field_name,
            path=settings_path,
        )
    storage_settings = StorageSettings(**storage_values)

    credentials_settings = CredentialsSettings(
        **{
            field_name: _optional_string(
                _selected(
                    credentials,
                    field_name,
                    env_name,
                    default=None,
                    env=source_env,
                    table_path="model_credentials",
                ),
                path=settings_path,
                field="model_credentials." + field_name,
            )
            for field_name, env_name in MODEL_CREDENTIAL_ENV.items()
        }
    )

    runtime_settings = RuntimeLimits(
        agent_node_id=_node_id(
            _selected(
                runtime,
                "agent_node_id",
                RUNTIME_ENV["agent_node_id"],
                default="local-dev",
                env=source_env,
                table_path="runtime",
            ),
            path=settings_path,
            field="runtime.agent_node_id",
        ),
        max_delegation_depth=_int_value(
            _selected(
                runtime,
                "max_delegation_depth",
                RUNTIME_ENV["max_delegation_depth"],
                default=3,
                env=source_env,
                table_path="runtime",
            ),
            minimum=1,
            path=settings_path,
            field="runtime.max_delegation_depth",
        ),
        max_tasks_per_root=_int_value(
            _selected(
                runtime,
                "max_tasks_per_root",
                RUNTIME_ENV["max_tasks_per_root"],
                default=20,
                env=source_env,
                table_path="runtime",
            ),
            minimum=1,
            path=settings_path,
            field="runtime.max_tasks_per_root",
        ),
        a2a_webhook_url=_optional_http_url(
            _selected(
                runtime,
                "a2a_webhook_url",
                RUNTIME_ENV["a2a_webhook_url"],
                default=None,
                env=source_env,
                table_path="runtime",
            ),
            path=settings_path,
            field="runtime.a2a_webhook_url",
        ),
        a2a_webhook_token=_optional_string(
            _selected(
                runtime,
                "a2a_webhook_token",
                RUNTIME_ENV["a2a_webhook_token"],
                default=None,
                env=source_env,
                table_path="runtime",
            ),
            path=settings_path,
            field="runtime.a2a_webhook_token",
        ),
        remote_research_token=_optional_string(
            _selected(
                runtime,
                "remote_research_token",
                RUNTIME_ENV["remote_research_token"],
                default=None,
                env=source_env,
                table_path="runtime",
            ),
            path=settings_path,
            field="runtime.remote_research_token",
        ),
    )

    telegram_values: dict[str, Any] = {
        "bot_token": _optional_string(
            _selected(
                telegram,
                "bot_token",
                TELEGRAM_ENV["bot_token"],
                default=None,
                env=source_env,
                table_path="channels.telegram",
            ),
            path=settings_path,
            field="channels.telegram.bot_token",
        ),
        "default_agent": _non_empty_string(
            _selected(
                telegram,
                "default_agent",
                TELEGRAM_ENV["default_agent"],
                default="main",
                env=source_env,
                table_path="channels.telegram",
            ),
            path=settings_path,
            field="channels.telegram.default_agent",
        ),
        "poll_timeout": _int_value(
            _selected(
                telegram,
                "poll_timeout",
                TELEGRAM_ENV["poll_timeout"],
                default=30,
                env=source_env,
                table_path="channels.telegram",
            ),
            minimum=0,
            path=settings_path,
            field="channels.telegram.poll_timeout",
        ),
        "task_poll_interval": _finite_float(
            _selected(
                telegram,
                "task_poll_interval",
                TELEGRAM_ENV["task_poll_interval"],
                default=2.0,
                env=source_env,
                table_path="channels.telegram",
            ),
            minimum=0.0,
            path=settings_path,
            field="channels.telegram.task_poll_interval",
        ),
        "terminal_review_grace_checks": _int_value(
            _selected(
                telegram,
                "terminal_review_grace_checks",
                TELEGRAM_ENV["terminal_review_grace_checks"],
                default=3,
                env=source_env,
                table_path="channels.telegram",
            ),
            minimum=0,
            path=settings_path,
            field="channels.telegram.terminal_review_grace_checks",
        ),
        "message_parse_mode": _parse_mode(
            _selected(
                telegram,
                "message_parse_mode",
                TELEGRAM_ENV["message_parse_mode"],
                default="MarkdownV2",
                env=source_env,
                table_path="channels.telegram",
            ),
            path=settings_path,
            field="channels.telegram.message_parse_mode",
        ),
        "media_max_bytes": _int_value(
            _selected(
                telegram,
                "media_max_bytes",
                TELEGRAM_ENV["media_max_bytes"],
                default=50 * 1024 * 1024,
                env=source_env,
                table_path="channels.telegram",
            ),
            minimum=1,
            path=settings_path,
            field="channels.telegram.media_max_bytes",
        ),
        "fallback_ips": _string_tuple(
            _selected(
                telegram,
                "fallback_ips",
                TELEGRAM_ENV["fallback_ips"],
                default=(),
                env=source_env,
                table_path="channels.telegram",
            ),
            path=settings_path,
            field="channels.telegram.fallback_ips",
        ),
        "kroki_base_url": _http_url(
            _selected(
                telegram,
                "kroki_base_url",
                TELEGRAM_ENV["kroki_base_url"],
                default="https://kroki.io",
                env=source_env,
                table_path="channels.telegram",
            ),
            path=settings_path,
            field="channels.telegram.kroki_base_url",
        ),
    }
    telegram_api_timeout = _selected(
        telegram,
        "api_timeout",
        TELEGRAM_ENV["api_timeout"],
        default=float(telegram_values["poll_timeout"] + 10),
        env=source_env,
        table_path="channels.telegram",
    )
    telegram_values["api_timeout"] = _finite_float(
        telegram_api_timeout,
        minimum=0.0,
        exclusive_minimum=True,
        path=settings_path,
        field="channels.telegram.api_timeout",
    )
    telegram_values["session_db"] = _path_from_table(
        telegram,
        "session_db",
        TELEGRAM_PATH_ENV["session_db"],
        default=storage_settings.channel_session_db,
        env=source_env,
        base=paths.ruyi_home,
        path=settings_path,
        table_path="channels.telegram",
    )
    telegram_values["update_db"] = _path_from_table(
        telegram,
        "update_db",
        TELEGRAM_PATH_ENV["update_db"],
        default=paths.ruyi_home / "data/telegram_updates.sqlite3",
        env=source_env,
        base=paths.ruyi_home,
        path=settings_path,
        table_path="channels.telegram",
    )
    telegram_settings = TelegramSettings(**telegram_values)

    feishu_settings = FeishuSettings(
        app_id=_optional_string(
            _selected(
                feishu,
                "app_id",
                FEISHU_ENV["app_id"],
                default=None,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.app_id",
        ),
        app_secret=_optional_string(
            _selected(
                feishu,
                "app_secret",
                FEISHU_ENV["app_secret"],
                default=None,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.app_secret",
        ),
        domain=_enum_value(
            _selected(
                feishu,
                "domain",
                FEISHU_ENV["domain"],
                default="feishu",
                env=source_env,
                table_path="channels.feishu",
            ),
            allowed={"feishu", "lark"},
            path=settings_path,
            field="channels.feishu.domain",
        ),
        connection_mode=_enum_value(
            _selected(
                feishu,
                "connection_mode",
                FEISHU_ENV["connection_mode"],
                default="websocket",
                env=source_env,
                table_path="channels.feishu",
            ),
            allowed={"websocket"},
            path=settings_path,
            field="channels.feishu.connection_mode",
        ),
        default_agent=_non_empty_string(
            _selected(
                feishu,
                "default_agent",
                FEISHU_ENV["default_agent"],
                default="main",
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.default_agent",
        ),
        session_db=_path_from_table(
            feishu,
            "session_db",
            FEISHU_PATH_ENV["session_db"],
            default=storage_settings.channel_session_db,
            env=source_env,
            base=paths.ruyi_home,
            path=settings_path,
            table_path="channels.feishu",
        ),
        event_db=_path_from_table(
            feishu,
            "event_db",
            FEISHU_PATH_ENV["event_db"],
            default=paths.ruyi_home / "data/feishu_events.sqlite3",
            env=source_env,
            base=paths.ruyi_home,
            path=settings_path,
            table_path="channels.feishu",
        ),
        group_policy=_enum_value(
            _selected(
                feishu,
                "group_policy",
                FEISHU_ENV["group_policy"],
                default="disabled",
                env=source_env,
                table_path="channels.feishu",
            ),
            allowed={"disabled", "open", "allowlist"},
            path=settings_path,
            field="channels.feishu.group_policy",
        ),
        require_mention=_bool_value(
            _selected(
                feishu,
                "require_mention",
                FEISHU_ENV["require_mention"],
                default=True,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.require_mention",
        ),
        allowed_users=_string_tuple(
            _selected(
                feishu,
                "allowed_users",
                FEISHU_ENV["allowed_users"],
                default=(),
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.allowed_users",
        ),
        allowed_groups=_string_tuple(
            _selected(
                feishu,
                "allowed_groups",
                FEISHU_ENV["allowed_groups"],
                default=(),
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.allowed_groups",
        ),
        bot_name=_optional_string(
            _selected(
                feishu,
                "bot_name",
                FEISHU_ENV["bot_name"],
                default=None,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.bot_name",
        ),
        bot_open_id=_optional_string(
            _selected(
                feishu,
                "bot_open_id",
                FEISHU_ENV["bot_open_id"],
                default=None,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.bot_open_id",
        ),
        bot_user_id=_optional_string(
            _selected(
                feishu,
                "bot_user_id",
                FEISHU_ENV["bot_user_id"],
                default=None,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.bot_user_id",
        ),
        bot_union_id=_optional_string(
            _selected(
                feishu,
                "bot_union_id",
                FEISHU_ENV["bot_union_id"],
                default=None,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.bot_union_id",
        ),
        api_timeout=_finite_float(
            _selected(
                feishu,
                "api_timeout",
                FEISHU_ENV["api_timeout"],
                default=10.0,
                env=source_env,
                table_path="channels.feishu",
            ),
            minimum=0.0,
            exclusive_minimum=True,
            path=settings_path,
            field="channels.feishu.api_timeout",
        ),
        task_poll_interval=_finite_float(
            _selected(
                feishu,
                "task_poll_interval",
                FEISHU_ENV["task_poll_interval"],
                default=2.0,
                env=source_env,
                table_path="channels.feishu",
            ),
            minimum=0.0,
            path=settings_path,
            field="channels.feishu.task_poll_interval",
        ),
        terminal_review_grace_checks=_int_value(
            _selected(
                feishu,
                "terminal_review_grace_checks",
                FEISHU_ENV["terminal_review_grace_checks"],
                default=3,
                env=source_env,
                table_path="channels.feishu",
            ),
            minimum=0,
            path=settings_path,
            field="channels.feishu.terminal_review_grace_checks",
        ),
        media_max_bytes=_int_value(
            _selected(
                feishu,
                "media_max_bytes",
                FEISHU_ENV["media_max_bytes"],
                default=30 * 1024 * 1024,
                env=source_env,
                table_path="channels.feishu",
            ),
            minimum=1,
            path=settings_path,
            field="channels.feishu.media_max_bytes",
        ),
        ack_mode=_enum_value(
            _selected(
                feishu,
                "ack_mode",
                FEISHU_ENV["ack_mode"],
                default="reaction",
                env=source_env,
                table_path="channels.feishu",
            ),
            allowed={"reaction", "message", "off"},
            path=settings_path,
            field="channels.feishu.ack_mode",
        ),
        reactions=_bool_value(
            _selected(
                feishu,
                "reactions",
                FEISHU_ENV["reactions"],
                default=True,
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.reactions",
        ),
        processing_reaction=_non_empty_string(
            _selected(
                feishu,
                "processing_reaction",
                FEISHU_ENV["processing_reaction"],
                default="Typing",
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.processing_reaction",
        ),
        approval_reaction=_non_empty_string(
            _selected(
                feishu,
                "approval_reaction",
                FEISHU_ENV["approval_reaction"],
                default="CheckMark",
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.approval_reaction",
        ),
        failure_reaction=_non_empty_string(
            _selected(
                feishu,
                "failure_reaction",
                FEISHU_ENV["failure_reaction"],
                default="CrossMark",
                env=source_env,
                table_path="channels.feishu",
            ),
            path=settings_path,
            field="channels.feishu.failure_reaction",
        ),
    )
    if (
        feishu_settings.group_policy != "disabled"
        and feishu_settings.require_mention
        and not any(
            (
                feishu_settings.bot_name,
                feishu_settings.bot_open_id,
                feishu_settings.bot_user_id,
                feishu_settings.bot_union_id,
            )
        )
    ):
        raise ConfigError(
            f"{settings_path}: channels.feishu requires a bot identity when "
            "group_policy is not disabled and require_mention is true"
        )

    langsmith_settings = LangSmithSettings(
        tracing=_bool_value(
            _selected(
                langsmith,
                "tracing",
                LANGSMITH_ENV["tracing"],
                default=False,
                env=source_env,
                table_path="langsmith",
            ),
            path=settings_path,
            field="langsmith.tracing",
        ),
        endpoint=_http_url(
            _selected(
                langsmith,
                "endpoint",
                LANGSMITH_ENV["endpoint"],
                default="https://api.smith.langchain.com",
                env=source_env,
                table_path="langsmith",
            ),
            path=settings_path,
            field="langsmith.endpoint",
        ),
        api_key=_optional_string(
            _selected(
                langsmith,
                "api_key",
                LANGSMITH_ENV["api_key"],
                default=None,
                env=source_env,
                table_path="langsmith",
            ),
            path=settings_path,
            field="langsmith.api_key",
        ),
        project=_optional_string(
            _selected(
                langsmith,
                "project",
                LANGSMITH_ENV["project"],
                default=None,
                env=source_env,
                table_path="langsmith",
            ),
            path=settings_path,
            field="langsmith.project",
        ),
    )

    return RuntimeSettings(
        paths=resolved_paths,
        credentials=credentials_settings,
        backend=backend_settings,
        gateway=gateway_settings,
        storage=storage_settings,
        runtime=runtime_settings,
        channels=ChannelsSettings(
            telegram=telegram_settings,
            feishu=feishu_settings,
        ),
        langsmith=langsmith_settings,
    )


def apply_runtime_settings_to_env(
    settings: RuntimeSettings,
    env: MutableMapping[str, str] | None = None,
) -> None:
    """Project typed values for SDKs, dynamic secrets, and legacy scripts."""

    target = os.environ if env is None else env
    target.update(_project_runtime_settings(settings))
    target["RUYI_HOME"] = str(settings.paths.ruyi_home)
    target["RUYI_CONFIG_DIR"] = str(settings.paths.config_dir)
    target["RUYI_DATA_DIR"] = str(settings.paths.data_dir)
    target["RUYI_SKILLS_DIR"] = str(settings.paths.skills_dir)
    target["RUYI_WORKSPACE"] = str(settings.workspace)
    target["RUYI_RUNTIME_CONFIGURED"] = "1"


project_runtime_settings_to_env = apply_runtime_settings_to_env


def _project_runtime_settings(settings: RuntimeSettings) -> dict[str, str]:
    env: dict[str, str] = {
        "BACKEND_KIND": settings.backend.kind,
        "LOCAL_BACKEND_ROOT": str(settings.backend.workspace),
        "GATEWAY_HOST": settings.gateway.host,
        "GATEWAY_PORT": str(settings.gateway.port),
        "GATEWAY_BASE_URL": settings.gateway.base_url,
        "GATEWAY_BEARER_TOKEN": settings.gateway.bearer_token,
    }
    for field_name, env_name in STORAGE_FIELD_ENV.items():
        env[env_name] = str(getattr(settings.storage, field_name))
    for field_name, env_name in MODEL_CREDENTIAL_ENV.items():
        value = getattr(settings.credentials, field_name)
        if value is not None:
            env[env_name] = value
    for field_name, env_name in BACKEND_LOCAL_ENV.items():
        env[env_name] = _string_value(getattr(settings.backend.local, field_name))
    for field_name, env_name in BACKEND_DAYTONA_ENV.items():
        value = getattr(settings.backend.daytona, field_name)
        if value is not None:
            env[env_name] = _string_value(value)
    for field_name, env_name in RUNTIME_ENV.items():
        value = getattr(settings.runtime, field_name)
        if value is not None:
            env[env_name] = _string_value(value)
    for field_name, env_name in TELEGRAM_ENV.items():
        value = getattr(settings.channels.telegram, field_name)
        if value is not None:
            env[env_name] = _string_value(value)
    for field_name, env_name in TELEGRAM_PATH_ENV.items():
        env[env_name] = str(getattr(settings.channels.telegram, field_name))
    for field_name, env_name in FEISHU_ENV.items():
        value = getattr(settings.channels.feishu, field_name)
        if value is not None:
            env[env_name] = _string_value(value)
    for field_name, env_name in FEISHU_PATH_ENV.items():
        env[env_name] = str(getattr(settings.channels.feishu, field_name))
    for field_name, env_name in LANGSMITH_ENV.items():
        value = getattr(settings.langsmith, field_name)
        if value is not None:
            env[env_name] = _string_value(value)
    return env


def _load_settings_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as file:
            return tomllib.load(file)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(
            f"Invalid TOML in {path}: {exc}. TOML requires quote string values, "
            "including URLs and Windows paths."
        ) from exc


def _copy_resource_if_missing(
    resource: Any,
    destination: Path,
    *,
    force: bool = False,
) -> None:
    if destination.exists() and not force and destination.stat().st_size > 0:
        return
    content = resource.read_bytes()
    if not content.strip():
        raise ConfigError(
            f"Packaged bootstrap template {resource} is empty. "
            "Reinstall ruyi-agent from a wheel that includes non-empty templates."
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)


def _validate_shape(data: Mapping[str, Any], path: Path) -> None:
    for key in data:
        if key not in _ALLOWED_TOP_LEVEL:
            _shape_error(path, str(key), "unknown runtime settings key")
    _validate_table_keys(
        data.get("model_credentials"),
        path,
        "model_credentials",
        set(MODEL_CREDENTIAL_ENV) | set(MODEL_CREDENTIAL_ENV.values()),
    )
    backend = data.get("backend")
    _validate_table_keys(
        backend,
        path,
        "backend",
        {"kind", "workspace", "local", "daytona"},
    )
    _validate_table_keys(
        backend.get("local") if isinstance(backend, dict) else None,
        path,
        "backend.local",
        set(BACKEND_LOCAL_ENV) | set(BACKEND_LOCAL_ENV.values()),
    )
    _validate_table_keys(
        backend.get("daytona") if isinstance(backend, dict) else None,
        path,
        "backend.daytona",
        set(BACKEND_DAYTONA_ENV) | set(BACKEND_DAYTONA_ENV.values()),
    )
    _validate_table_keys(
        data.get("gateway"),
        path,
        "gateway",
        {"host", "port", "base_url", "bearer_token"},
    )
    _validate_table_keys(
        data.get("storage"),
        path,
        "storage",
        set(STORAGE_FIELD_ENV) | set(STORAGE_FIELD_ENV.values()),
    )
    _validate_table_keys(
        data.get("runtime"),
        path,
        "runtime",
        set(RUNTIME_ENV) | set(RUNTIME_ENV.values()),
    )
    channels = data.get("channels")
    _validate_table_keys(channels, path, "channels", {"telegram", "feishu"})
    _validate_table_keys(
        channels.get("telegram") if isinstance(channels, dict) else None,
        path,
        "channels.telegram",
        set(TELEGRAM_ENV)
        | set(TELEGRAM_PATH_ENV)
        | set(TELEGRAM_ENV.values())
        | set(TELEGRAM_PATH_ENV.values()),
    )
    _validate_table_keys(
        channels.get("feishu") if isinstance(channels, dict) else None,
        path,
        "channels.feishu",
        set(FEISHU_ENV)
        | set(FEISHU_PATH_ENV)
        | set(FEISHU_ENV.values())
        | set(FEISHU_PATH_ENV.values()),
    )
    _validate_table_keys(
        data.get("langsmith"),
        path,
        "langsmith",
        set(LANGSMITH_ENV) | set(LANGSMITH_ENV.values()),
    )


def _validate_table_keys(
    value: Any,
    path: Path,
    table_path: str,
    allowed: set[str],
) -> None:
    if value is None:
        return
    if not isinstance(value, dict):
        _shape_error(path, table_path, "must be a table")
    for key in value:
        if key not in allowed:
            _shape_error(path, f"{table_path}.{key}", "unknown runtime settings key")


def _table(
    data: Mapping[str, Any],
    key: str,
    path: Path,
    *,
    parent: str | None = None,
) -> dict[str, Any]:
    value = data.get(key, {})
    dotted = f"{parent}.{key}" if parent else key
    if not isinstance(value, dict):
        _shape_error(path, dotted, "must be a table")
    return value


def _shape_error(path: Path, field: str, reason: str) -> None:
    raise ConfigError(f"{path.resolve()}: {field} {reason}")


def _merge_overrides(
    overrides: Mapping[str, Mapping[str, Any]] | None,
    gateway_overrides: Mapping[str, Any] | None,
) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for table, values in (overrides or {}).items():
        # Keep malformed values intact so _apply_overrides can report the
        # originating dotted table path as ConfigError instead of leaking a
        # generic dict-construction exception.
        merged[table] = dict(values) if isinstance(values, Mapping) else values
    if gateway_overrides:
        gateway = merged.setdefault("gateway", {})
        if not isinstance(gateway, Mapping):
            merged["gateway"] = gateway
        else:
            merged["gateway"] = {**gateway, **gateway_overrides}
    return merged


def _apply_overrides(
    data: Mapping[str, Any],
    overrides: Mapping[str, Mapping[str, Any]],
    path: Path,
) -> dict[str, Any]:
    merged = copy.deepcopy(dict(data))
    for table, values in overrides.items():
        if not isinstance(values, Mapping):
            _shape_error(path, table, "override must be a table")
        current = merged.get(table, {})
        if not isinstance(current, dict):
            _shape_error(path, table, "must be a table")
        current.update({key: _OverrideValue(value) for key, value in values.items()})
        merged[table] = current
    return merged


def _select_workspace(
    backend: Mapping[str, Any],
    *,
    paths: RuyiPaths,
    workspace_override: str | Path | None,
    env: Mapping[str, str],
) -> tuple[str | Path, str]:
    if not _is_unset(workspace_override):
        return workspace_override, "workspace override"
    value = backend.get("workspace")
    if not _is_unset(value):
        return value, "backend.workspace"
    value = env.get("RUYI_WORKSPACE")
    if not _is_unset(value):
        return value, "RUYI_WORKSPACE"
    return paths.workspace, "workspace default"


def _selected(
    table: Mapping[str, Any],
    field: str,
    env_name: str | None,
    *,
    default: Any,
    env: Mapping[str, str],
    table_path: str,
    allow_alias: bool = True,
    allow_env: bool = True,
) -> Any:
    value = table.get(field, _MISSING)
    if value is not _MISSING and not _is_unset(value):
        return value
    if allow_alias and env_name is not None:
        value = table.get(env_name, _MISSING)
        if value is not _MISSING and not _is_unset(value):
            return value
    if allow_env and env_name is not None:
        value = env.get(env_name, _MISSING)
        if value is not _MISSING and not _is_unset(value):
            return _EnvironmentValue(value, env_name)
    return default


def _is_unset(value: Any) -> bool:
    if isinstance(value, (_EnvironmentValue, _OverrideValue)):
        value = value.value
    return (
        value is None
        or (isinstance(value, str) and value.strip() == "")
        or (isinstance(value, list) and not value)
    )


def _path_value(
    value: Any,
    *,
    default: Path,
    base: Path | None,
    source: str,
    path: Path,
) -> Path:
    value = _unwrap(value)
    if _is_unset(value):
        return default.resolve()
    if not isinstance(value, (str, Path)):
        _value_error(path, source, "must be a path string")
    if source == "RUYI_WORKSPACE" and os.name != "nt":
        raw_path = str(value)
        if "\\" in raw_path or re.match(r"^[A-Za-z]:/", raw_path):
            _value_error(
                path,
                source,
                "uses a Windows-style path on this POSIX system",
            )
    try:
        candidate = Path(value).expanduser()
        if not candidate.is_absolute() and base is not None:
            candidate = base / candidate
        return candidate.resolve()
    except (OSError, RuntimeError, ValueError) as exc:
        raise ConfigError(f"{path.resolve()}: {source} is not a valid path") from exc


def _path_from_table(
    table: Mapping[str, Any],
    field: str,
    env_name: str,
    *,
    default: Path,
    env: Mapping[str, str],
    base: Path,
    path: Path,
    table_path: str,
) -> Path:
    value = _selected(
        table,
        field,
        env_name,
        default=default,
        env=env,
        table_path=table_path,
    )
    return _path_value(
        value,
        default=default,
        base=base,
        source=f"{table_path}.{field}",
        path=path,
    )


def _optional_string(value: Any, *, path: Path, field: str) -> str | None:
    value = _unwrap(value)
    if _is_unset(value):
        return None
    if not isinstance(value, str):
        _value_error(path, field, "must be a string")
    return value.strip()


def _non_empty_string(value: Any, *, path: Path, field: str) -> str:
    value = _unwrap(value)
    if not isinstance(value, str):
        _value_error(path, field, "must be a string")
    result = value.strip()
    if not result:
        _value_error(path, field, "must be non-empty")
    return result


def _node_id(value: Any, *, path: Path, field: str) -> str:
    value = _unwrap(value)
    if not isinstance(value, str):
        _value_error(path, field, "must be a string")
    try:
        return validate_node_id(value.strip())
    except ValueError as exc:
        raise ConfigError(
            f"{path.resolve()}: {field} must be non-empty and at most 128 characters"
        ) from exc


def _enum_value(
    value: Any,
    *,
    allowed: set[str],
    path: Path,
    field: str,
) -> str:
    value = _unwrap(value)
    result = _non_empty_string(value, path=path, field=field).lower()
    if result not in allowed:
        _value_error(
            path,
            field,
            f"has unsupported value (expected one of {sorted(allowed)})",
        )
    return result


def _int_value(
    value: Any,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
    path: Path,
    field: str,
) -> int:
    if isinstance(value, (_EnvironmentValue, _OverrideValue)):
        value = value.value
        if not isinstance(value, str):
            if type(value) is not int:
                _value_error(path, field, "must be an integer override token")
        else:
            try:
                value = int(value.strip(), 10)
            except ValueError as exc:
                raise ConfigError(
                    f"{path.resolve()}: {field} must be an integer environment token"
                ) from exc
    if type(value) is not int:
        _value_error(path, field, "must be an integer")
    if minimum is not None and value < minimum:
        _value_error(path, field, f"must be >= {minimum}")
    if maximum is not None and value > maximum:
        _value_error(path, field, f"must be <= {maximum}")
    return value


def _finite_float(
    value: Any,
    *,
    minimum: float | None = None,
    exclusive_minimum: bool = False,
    path: Path,
    field: str,
) -> float:
    external = isinstance(value, (_EnvironmentValue, _OverrideValue))
    value = _unwrap(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        if not external or not isinstance(value, str):
            _value_error(path, field, "must be a number")
        try:
            result = float(value.strip())
        except (ValueError, AttributeError) as exc:
            raise ConfigError(f"{path.resolve()}: {field} must be a number") from exc
    else:
        result = float(value)
    if not math.isfinite(result):
        _value_error(path, field, "must be finite")
    if minimum is not None:
        if exclusive_minimum and result <= minimum:
            _value_error(path, field, f"must be > {minimum}")
        if not exclusive_minimum and result < minimum:
            _value_error(path, field, f"must be >= {minimum}")
    return result


def _bool_value(value: Any, *, path: Path, field: str) -> bool:
    if type(value) is bool:
        return value
    external = isinstance(value, (_EnvironmentValue, _OverrideValue))
    value = _unwrap(value)
    if external and isinstance(value, str):
        token = value.strip().lower()
        if token in _TRUE_ENV_TOKENS:
            return True
        if token in _FALSE_ENV_TOKENS:
            return False
        _value_error(path, field, "must use an explicit boolean environment token")
    _value_error(path, field, "must be a TOML boolean or explicit environment token")


def _string_tuple(value: Any, *, path: Path, field: str) -> tuple[str, ...]:
    external = isinstance(value, (_EnvironmentValue, _OverrideValue))
    value = _unwrap(value)
    if external and isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple)):
        values = list(value)
    elif value is None:
        values = []
    else:
        _value_error(path, field, "must be a list of strings")
    result: list[str] = []
    for item in values:
        if not isinstance(item, str) or not item.strip():
            _value_error(path, field, "must contain only non-empty strings")
        result.append(item.strip())
    return tuple(result)


def _parse_mode(value: Any, *, path: Path, field: str) -> str:
    value = _unwrap(value)
    result = _non_empty_string(value, path=path, field=field)
    if result not in {"Markdown", "MarkdownV2", "HTML"}:
        _value_error(path, field, "has unsupported value")
    return result


def _optional_http_url(value: Any, *, path: Path, field: str) -> str | None:
    value = _unwrap(value)
    if _is_unset(value):
        return None
    return _http_url(value, path=path, field=field)


def _http_url(value: Any, *, path: Path, field: str) -> str:
    value = _unwrap(value)
    if not isinstance(value, str):
        _value_error(path, field, "must be a URL string")
    if not value or not value.strip():
        _value_error(path, field, "must be a non-empty URL")
    if value != value.strip():
        _value_error(path, field, "must not contain surrounding whitespace")
    result = validate_http_url(value, path=f"{path.resolve()}: {field}")
    try:
        port = urlsplit(result).port
    except ValueError as exc:
        raise ConfigError(f"{path.resolve()}: {field} has an invalid port") from exc
    if port == 0:
        _value_error(path, field, "must not use port 0")
    return result


def _gateway_host(value: Any, *, path: Path, field: str) -> str:
    value = _unwrap(value)
    if not isinstance(value, str):
        _value_error(path, field, "must be a host, not a URL or path")
    host = value
    if not host or any(character.isspace() for character in host):
        _value_error(path, field, "must be a non-empty host")
    if "://" in host or "/" in host or "?" in host or "#" in host:
        _value_error(path, field, "must be a host, not a URL or path")
    if host.startswith("[") or host.endswith("]"):
        if not (host.startswith("[") and host.endswith("]")):
            _value_error(path, field, "has an invalid bracketed host")
        inner = host[1:-1]
        try:
            IPv6Address(inner.split("%", maxsplit=1)[0])
        except ValueError as exc:
            raise ConfigError(
                f"{path.resolve()}: {field} must be a valid host"
            ) from exc
        return host
    if host.count(":") == 1:
        _value_error(path, field, "must not include a port")
    if ":" in host:
        try:
            IPv6Address(host.split("%", maxsplit=1)[0])
        except ValueError as exc:
            raise ConfigError(
                f"{path.resolve()}: {field} must be a valid host"
            ) from exc
        return host
    try:
        IPv4Address(host)
        return host
    except ValueError:
        pass
    try:
        ascii_host = host.encode("idna").decode("ascii")
    except UnicodeError as exc:
        raise ConfigError(f"{path.resolve()}: {field} must be a valid host") from exc
    ascii_host = ascii_host.rstrip(".")
    labels = ascii_host.split(".")
    if len(labels) == 4 and all(label.isdigit() for label in labels):
        _value_error(path, field, "must be a valid host")
    if (
        not ascii_host
        or len(ascii_host) > 253
        or any(
            not label
            or len(label) > 63
            or not label[0].isalnum()
            or not label[-1].isalnum()
            or any(not (char.isalnum() or char == "-") for char in label)
            for label in labels
        )
    ):
        _value_error(path, field, "must be a valid host")
    return host


def _value_error(path: Path, field: str, reason: str) -> None:
    raise ConfigError(f"{path.resolve()}: {field} {reason}")


def _unwrap(value: Any) -> Any:
    return (
        value.value if isinstance(value, (_EnvironmentValue, _OverrideValue)) else value
    )


def _string_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple)):
        return ",".join(_string_value(item) for item in value)
    return str(value)


__all__ = [
    "BACKEND_DAYTONA_ENV",
    "BACKEND_LOCAL_ENV",
    "BackendSettings",
    "ChannelSettings",
    "ChannelsSettings",
    "CredentialsSettings",
    "DEFAULT_STORAGE_FILES",
    "DaytonaBackendSettings",
    "FEISHU_ENV",
    "FEISHU_PATH_ENV",
    "FeishuSettings",
    "GatewaySettings",
    "LANGSMITH_ENV",
    "LangSmithSettings",
    "LocalBackendSettings",
    "MODEL_CREDENTIAL_ENV",
    "ModelCredentials",
    "RUNTIME_ENV",
    "RuntimeLimits",
    "RuntimeSettings",
    "STORAGE_FIELD_ENV",
    "StorageSettings",
    "TABLE_SCOPED_TOML_ALIASES",
    "TELEGRAM_ENV",
    "TELEGRAM_PATH_ENV",
    "TOML_ALIAS_NAMES",
    "apply_runtime_settings_to_env",
    "configure_runtime_environment",
    "ensure_ruyi_home",
    "ensure_runtime_dirs",
    "load_runtime_settings",
    "project_runtime_settings_to_env",
]
