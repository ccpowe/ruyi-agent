from __future__ import annotations

import os
import tomllib
from collections.abc import MutableMapping
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from ruyi_agent.config.paths import RuyiPaths, resolve_ruyi_paths


@dataclass(frozen=True, slots=True)
class RuntimeSettings:
    paths: RuyiPaths
    workspace: Path
    env: dict[str, str]


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
    "media_root": "FEISHU_MEDIA_ROOT",
}

LANGSMITH_ENV = {
    "tracing": "LANGSMITH_TRACING",
    "endpoint": "LANGSMITH_ENDPOINT",
    "api_key": "LANGSMITH_API_KEY",
    "project": "LANGSMITH_PROJECT",
}


def configure_runtime_environment(
    *,
    workspace: str | Path | None = None,
    env: MutableMapping[str, str] | None = None,
    init_force: bool = False,
    init_templates: bool = False,
) -> RuntimeSettings:
    target = os.environ if env is None else env
    workspace_override = workspace
    if workspace_override is None and target.get("RUYI_RUNTIME_CONFIGURED") == "1":
        workspace_override = target.get("RUYI_WORKSPACE")
    paths = resolve_ruyi_paths(workspace=workspace_override, env=target)
    if init_templates:
        ensure_ruyi_home(paths, force=init_force)
    else:
        _require_initialized_runtime_settings(paths)
        ensure_runtime_dirs(paths)
    settings = load_runtime_settings(paths, workspace_override=workspace_override)
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
    raise ValueError(
        f"Ruyi config is not initialized at {paths.ruyi_home}. "
        "Run `ruyi --init` first, or set RUYI_HOME to an existing config directory."
    )


def load_runtime_settings(
    paths: RuyiPaths,
    *,
    workspace_override: str | Path | None = None,
) -> RuntimeSettings:
    data = _load_settings_toml(paths.ruyi_home / "ruyi.toml")
    backend = _table(data, "backend")
    backend_local = _table(backend, "local")
    backend_daytona = _table(backend, "daytona")
    gateway = _table(data, "gateway")
    storage = _table(data, "storage")
    model_credentials = _table(data, "model_credentials")
    runtime = _table(data, "runtime")
    channels = _table(data, "channels")
    telegram = _table(channels, "telegram")
    feishu = _table(channels, "feishu")
    langsmith = _table(data, "langsmith")

    workspace = _path_value(
        (
            workspace_override
            if workspace_override is not None
            else backend.get("workspace")
        ),
        default=paths.workspace,
    )
    env = _default_env(paths, workspace)
    _set_if_present(env, "BACKEND_KIND", backend.get("kind"))
    _set_if_present(env, "GATEWAY_HOST", gateway.get("host"))
    _set_if_present(env, "GATEWAY_PORT", gateway.get("port"))
    _set_if_present(env, "GATEWAY_BASE_URL", gateway.get("base_url"))
    _set_if_present(env, "GATEWAY_BEARER_TOKEN", gateway.get("bearer_token"))

    for field_name, env_name in STORAGE_FIELD_ENV.items():
        value = _config_value(storage, field_name, env_name)
        if not _is_unset(value):
            env[env_name] = str(
                _path_value(
                    value,
                    default=paths.data_dir,
                    base=paths.ruyi_home,
                )
            )

    for field_name, env_name in MODEL_CREDENTIAL_ENV.items():
        _set_if_present(
            env,
            env_name,
            _config_value(model_credentials, field_name, env_name),
        )
    _apply_scalar_mapping(env, backend_local, BACKEND_LOCAL_ENV)
    _apply_scalar_mapping(env, backend_daytona, BACKEND_DAYTONA_ENV)
    _apply_scalar_mapping(env, runtime, RUNTIME_ENV)
    _apply_scalar_mapping(env, telegram, TELEGRAM_ENV)
    _apply_path_mapping(env, telegram, TELEGRAM_PATH_ENV, base=paths.ruyi_home)
    _apply_scalar_mapping(env, feishu, FEISHU_ENV)
    _apply_path_mapping(env, feishu, FEISHU_PATH_ENV, base=paths.ruyi_home)
    _apply_scalar_mapping(env, langsmith, LANGSMITH_ENV)

    return RuntimeSettings(paths=paths, workspace=workspace, env=env)


def apply_runtime_settings_to_env(
    settings: RuntimeSettings,
    env: MutableMapping[str, str] | None = None,
) -> None:
    target = os.environ if env is None else env
    target.update(settings.env)
    target["RUYI_HOME"] = str(settings.paths.ruyi_home)
    target["RUYI_CONFIG_DIR"] = str(settings.paths.config_dir)
    target["RUYI_DATA_DIR"] = str(settings.paths.data_dir)
    target["RUYI_SKILLS_DIR"] = str(settings.paths.skills_dir)
    target["RUYI_WORKSPACE"] = str(settings.workspace)
    target["RUYI_RUNTIME_CONFIGURED"] = "1"


def _load_settings_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with path.open("rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(
            f"Invalid TOML in {path}: {exc}. "
            "TOML requires quote string values, including URLs and Windows "
            'paths. Example: api_url = "https://example.com" or '
            'workspace = "C:/Users/name/project".'
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
        raise ValueError(
            f"Packaged bootstrap template {resource} is empty. "
            "Reinstall ruyi-agent from a wheel that includes non-empty templates."
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)


def _default_env(paths: RuyiPaths, workspace: Path) -> dict[str, str]:
    env = {
        "BACKEND_KIND": "local",
        "LOCAL_BACKEND_ROOT": str(workspace),
        "GATEWAY_HOST": "127.0.0.1",
        "GATEWAY_PORT": "8000",
        "GATEWAY_BASE_URL": "http://127.0.0.1:8000",
        "GATEWAY_BEARER_TOKEN": "dev-token",
    }
    for env_name, filename in DEFAULT_STORAGE_FILES.items():
        env[env_name] = str(paths.data_dir / filename)
    return env


def _table(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be a table")
    return value


def _set_if_present(env: dict[str, str], key: str, value: Any) -> None:
    if _is_unset(value):
        return
    env[key] = _string_value(value)


def _apply_scalar_mapping(
    env: dict[str, str],
    table: dict[str, Any],
    mapping: dict[str, str],
) -> None:
    for field_name, env_name in mapping.items():
        _set_if_present(env, env_name, _config_value(table, field_name, env_name))


def _apply_path_mapping(
    env: dict[str, str],
    table: dict[str, Any],
    mapping: dict[str, str],
    *,
    base: Path,
) -> None:
    for field_name, env_name in mapping.items():
        value = _config_value(table, field_name, env_name)
        if _is_unset(value):
            continue
        env[env_name] = str(
            _path_value(value, default=base, base=base)
        )


def _config_value(table: dict[str, Any], field_name: str, env_name: str) -> Any:
    value = table.get(field_name)
    if _is_unset(value):
        return table.get(env_name)
    return value


def _is_unset(value: Any) -> bool:
    return (
        value is None
        or (isinstance(value, str) and value.strip() == "")
        or (isinstance(value, list) and not value)
    )


def _string_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ",".join(_string_value(item) for item in value)
    return str(value)


def _path_value(
    value: Any,
    *,
    default: Path,
    base: Path | None = None,
) -> Path:
    if value is None or value == "":
        return default.resolve()
    path = Path(str(value)).expanduser()
    if not path.is_absolute() and base is not None:
        path = base / path
    return path.resolve()
