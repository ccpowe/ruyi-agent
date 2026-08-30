from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from ruyi_agent.config.errors import ConfigError
from ruyi_agent.config.paths import resolve_ruyi_paths
from ruyi_agent.config.paths import RuyiPaths
from ruyi_agent.config.runtime_settings import apply_runtime_settings_to_env
from ruyi_agent.config.runtime_settings import configure_runtime_environment
from ruyi_agent.config.runtime_settings import GatewayLaunchOverrides
from ruyi_agent.config.runtime_settings import load_runtime_settings
from ruyi_agent.config.runtime_settings import TABLE_SCOPED_TOML_ALIASES
from ruyi_agent.config.runtime_settings import TOML_ALIAS_NAMES


def _toml_path(path: Path) -> str:
    return path.as_posix()


def _load_text(
    tmp_path: Path,
    body: str,
    *,
    env: dict[str, str] | None = None,
):
    ruyi_home = tmp_path / ".ruyi_agent"
    ruyi_home.mkdir(parents=True)
    settings_path = ruyi_home / "ruyi.toml"
    settings_path.write_text(body, encoding="utf-8")
    paths = RuyiPaths(
        ruyi_home=ruyi_home,
        config_dir=ruyi_home / "config",
        data_dir=ruyi_home / "data",
        skills_dir=ruyi_home / "skills",
        workspace=tmp_path / "cwd",
    )
    return load_runtime_settings(paths, env=env or {}), settings_path


def test_load_runtime_settings_maps_ruyi_toml_to_runtime_values(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    workspace = tmp_path / "workspace"
    ruyi_home = project / ".ruyi_agent"
    workspace.mkdir()
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        "\n".join(
            [
                "[backend]",
                'kind = "local"',
                f'workspace = "{_toml_path(workspace)}"',
                "",
                "[gateway]",
                'host = "0.0.0.0"',
                "port = 8765",
                'bearer_token = "strong-token"',
                "",
                "[storage]",
                'checkpoint_db = "state/checkpoints.sqlite"',
                "",
                "[model_credentials]",
                'openrouter_api_key = "openrouter-key"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)

    settings = load_runtime_settings(resolve_ruyi_paths())

    assert settings.workspace == workspace
    assert settings.backend.kind == "local"
    assert settings.backend.workspace == workspace
    assert settings.gateway.host == "0.0.0.0"
    assert settings.gateway.port == 8765
    assert settings.gateway.bearer_token == "strong-token"
    assert (
        settings.storage.checkpoint_db
        == (ruyi_home / "state/checkpoints.sqlite").resolve()
    )
    assert settings.credentials.openrouter_api_key == "openrouter-key"

    projected: dict[str, str] = {}
    apply_runtime_settings_to_env(settings, env=projected)
    assert projected["GATEWAY_PORT"] == "8765"


def test_load_runtime_settings_maps_channel_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    ruyi_home = project / ".ruyi_agent"
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        "\n".join(
            [
                "[channels.telegram]",
                'bot_token = "telegram-token"',
                'default_agent = "main"',
                'fallback_ips = ["149.154.167.220"]',
                "",
                "[channels.feishu]",
                'app_id = "feishu-id"',
                'app_secret = "feishu-secret"',
                "require_mention = false",
                'allowed_users = ["ou_1", "ou_2"]',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)

    settings = load_runtime_settings(resolve_ruyi_paths())

    assert settings.channels.telegram.bot_token == "telegram-token"
    assert settings.channels.telegram.default_agent == "main"
    assert settings.channels.telegram.fallback_ips == ("149.154.167.220",)
    assert settings.channels.feishu.app_id == "feishu-id"
    assert settings.channels.feishu.app_secret == "feishu-secret"
    assert settings.channels.feishu.require_mention is False
    assert settings.channels.feishu.allowed_users == ("ou_1", "ou_2")


def test_load_runtime_settings_accepts_env_style_toml_keys(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    ruyi_home = project / ".ruyi_agent"
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        "\n".join(
            [
                "[backend.daytona]",
                'DAYTONA_API_KEY = "daytona-key"',
                'DAYTONA_API_URL = "https://daytona.example/api"',
                'DAYTONA_TARGET = "us"',
                'DAYTONA_SANDBOX_NAME = "sandbox"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)

    settings = load_runtime_settings(resolve_ruyi_paths())

    assert settings.backend.daytona.api_key == "daytona-key"
    assert settings.backend.daytona.api_url == "https://daytona.example/api"
    assert settings.backend.daytona.target == "us"
    assert settings.backend.daytona.sandbox_name == "sandbox"


def test_configure_runtime_environment_applies_workspace_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    config_workspace = tmp_path / "from-config"
    cli_workspace = tmp_path / "from-cli"
    ruyi_home = project / ".ruyi_agent"
    config_workspace.mkdir()
    cli_workspace.mkdir()
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        "\n".join(
            [
                "[backend]",
                f'workspace = "{_toml_path(config_workspace)}"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)
    env: dict[str, str] = {}

    settings = configure_runtime_environment(workspace=cli_workspace, env=env)

    assert settings.workspace == cli_workspace
    assert env["LOCAL_BACKEND_ROOT"] == str(cli_workspace)


def test_configure_runtime_environment_keeps_existing_workspace_override(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    config_workspace = tmp_path / "from-config"
    cli_workspace = tmp_path / "from-cli"
    ruyi_home = project / ".ruyi_agent"
    config_workspace.mkdir()
    cli_workspace.mkdir()
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        "\n".join(
            [
                "[backend]",
                f'workspace = "{_toml_path(config_workspace)}"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)
    env: dict[str, str] = {}

    configure_runtime_environment(workspace=cli_workspace, env=env)
    settings = configure_runtime_environment(env=env)

    assert settings.workspace == cli_workspace
    assert env["LOCAL_BACKEND_ROOT"] == str(cli_workspace)
    assert env["RUYI_RUNTIME_CONFIGURED"] == "1"


def test_configure_runtime_environment_bootstraps_missing_user_home(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    user_home = tmp_path / "home"
    project.mkdir()
    user_home.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)
    env: dict[str, str] = {}

    settings = configure_runtime_environment(env=env, init_templates=True)

    assert settings.paths.ruyi_home == user_home / ".ruyi_agent"
    assert (settings.paths.ruyi_home / "ruyi.toml").is_file()
    assert (settings.paths.config_dir / "agents.toml").is_file()
    assert settings.paths.data_dir.is_dir()
    assert settings.paths.skills_dir.is_dir()


def test_configure_runtime_environment_requires_init_without_creating_templates(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    user_home = tmp_path / "home"
    project.mkdir()
    user_home.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)
    env: dict[str, str] = {}

    try:
        configure_runtime_environment(env=env)
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected missing runtime config to fail")

    assert "ruyi --init" in message
    assert not (user_home / ".ruyi_agent").exists()


def test_configure_runtime_environment_repairs_empty_bootstrap_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    user_home = tmp_path / "home"
    ruyi_home = user_home / ".ruyi_agent"
    project.mkdir()
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text("", encoding="utf-8")
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)
    env: dict[str, str] = {}

    settings = configure_runtime_environment(env=env, init_templates=True)

    assert settings.paths.ruyi_home == ruyi_home
    assert (
        (ruyi_home / "ruyi.toml")
        .read_text(encoding="utf-8")
        .startswith("# Ruyi runtime settings")
    )
    assert env["RUYI_RUNTIME_CONFIGURED"] == "1"


def test_load_runtime_settings_reports_invalid_toml_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    ruyi_home = project / ".ruyi_agent"
    ruyi_home.mkdir(parents=True)
    settings_path = ruyi_home / "ruyi.toml"
    settings_path.write_text(
        "\n".join(
            [
                "[backend.daytona]",
                "api_url = https://example.test",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)

    try:
        load_runtime_settings(resolve_ruyi_paths())
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected invalid TOML to raise ValueError")

    assert str(settings_path) in message
    assert "Invalid TOML" in message
    assert "quote string values" in message


def test_empty_toml_values_do_not_override_existing_environment(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    ruyi_home = project / ".ruyi_agent"
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        "\n".join(
            [
                "[model_credentials]",
                'openrouter_api_key = ""',
                "",
                "[channels.feishu]",
                'session_db = ""',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)
    env = {"OPENROUTER_API_KEY": "external-key"}

    configure_runtime_environment(env=env)

    assert env["OPENROUTER_API_KEY"] == "external-key"
    assert env["FEISHU_SESSION_DB"] == str(
        (ruyi_home / "data/channel_sessions.sqlite3").resolve()
    )


def test_runtime_settings_are_frozen_and_alias_surface_is_exact(
    tmp_path: Path,
) -> None:
    settings, _ = _load_text(tmp_path, "")

    assert len(TOML_ALIAS_NAMES) == 64
    assert all(name.isupper() for name in TOML_ALIAS_NAMES)
    assert {
        "BACKEND_KIND",
        "LOCAL_BACKEND_ROOT",
        "GATEWAY_HOST",
        "GATEWAY_PORT",
        "GATEWAY_BASE_URL",
        "GATEWAY_BEARER_TOKEN",
    }.isdisjoint(TOML_ALIAS_NAMES)
    assert {
        table: len(aliases) for table, aliases in TABLE_SCOPED_TOML_ALIASES.items()
    } == {
        "model_credentials": 6,
        "backend.local": 3,
        "backend.daytona": 4,
        "runtime": 6,
        "channels.telegram": 12,
        "channels.feishu": 24,
        "langsmith": 4,
        "storage": 5,
    }
    assert not hasattr(settings, "env")
    assert isinstance(settings.channels.telegram.fallback_ips, tuple)
    assert isinstance(settings.channels.feishu.allowed_users, tuple)
    with pytest.raises(FrozenInstanceError):
        settings.gateway.port = 9000


@pytest.mark.parametrize(
    ("canonical", "alias", "environment", "expected"),
    [
        ("timeout = 11", "LOCAL_BACKEND_TIMEOUT = 12", "13", 11),
        ('timeout = ""', "LOCAL_BACKEND_TIMEOUT = 12", "13", 12),
        ('timeout = ""', 'LOCAL_BACKEND_TIMEOUT = ""', "13", 13),
        ('timeout = ""', 'LOCAL_BACKEND_TIMEOUT = ""', "", 120),
    ],
)
def test_same_table_precedence_keeps_canonical_alias_env_and_default_order(
    tmp_path: Path,
    canonical: str,
    alias: str,
    environment: str,
    expected: int,
) -> None:
    settings, _ = _load_text(
        tmp_path,
        f"[backend.local]\n{canonical}\n{alias}\n",
        env={"LOCAL_BACKEND_TIMEOUT": environment},
    )
    assert settings.backend.local.timeout == expected


def test_workspace_and_projection_precedence_excludes_projection_inputs(
    tmp_path: Path,
) -> None:
    canonical_workspace = tmp_path / "canonical"
    env_workspace = tmp_path / "environment"
    cli_workspace = tmp_path / "cli"
    body = f'[backend]\nworkspace = "{canonical_workspace.as_posix()}"\n'
    env = {
        "RUYI_WORKSPACE": str(env_workspace),
        "LOCAL_BACKEND_ROOT": str(tmp_path / "legacy"),
        "GATEWAY_HOST": "legacy-host",
        "GATEWAY_PORT": "1",
        "CHECKPOINT_DB": "legacy.sqlite",
    }
    settings, _ = _load_text(tmp_path, body, env=env)
    assert settings.workspace == canonical_workspace.resolve()
    assert settings.gateway.host == "127.0.0.1"
    assert settings.gateway.port == 8000
    assert settings.storage.checkpoint_db != (tmp_path / "legacy.sqlite").resolve()

    settings, _ = _load_text(
        tmp_path / "env-case",
        '[backend]\nworkspace = ""\n',
        env={"RUYI_WORKSPACE": str(env_workspace)},
    )
    assert settings.workspace == env_workspace.resolve()

    settings, _ = _load_text(
        tmp_path / "cli-case",
        body,
        env={"RUYI_WORKSPACE": str(env_workspace)},
    )
    settings = load_runtime_settings(
        settings.paths,
        workspace_override=cli_workspace,
        env={"RUYI_WORKSPACE": str(env_workspace)},
    )
    assert settings.workspace == cli_workspace.resolve()


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ("[gateway]\nport = true\n", "gateway.port"),
        ("[gateway]\nport = 0\n", "gateway.port"),
        ("[gateway]\nport = 65536\n", "gateway.port"),
        ("[channels.telegram]\napi_timeout = nan\n", "channels.telegram.api_timeout"),
        (
            "[channels.telegram]\ntask_poll_interval = 0\n",
            "channels.telegram.task_poll_interval",
        ),
        (
            "[channels.feishu]\ntask_poll_interval = -1\n",
            "channels.feishu.task_poll_interval",
        ),
        ("[channels.feishu]\napi_timeout = 0\n", "channels.feishu.api_timeout"),
        (
            '[channels.telegram]\nfallback_ips = "127.0.0.1"\n',
            "channels.telegram.fallback_ips",
        ),
        (
            '[channels.feishu]\nrequire_mention = "true"\n',
            "channels.feishu.require_mention",
        ),
        ('[gateway]\nbase_url = "http://example.test:0"\n', "gateway.base_url"),
        (
            '[gateway]\nbase_url = "http://user:secret@example.test"\n',
            "gateway.base_url",
        ),
        ('[gateway]\nhost = "http://example.test"\n', "gateway.host"),
        ('[gateway]\nhost = "example.test:8000"\n', "gateway.host"),
    ],
)
def test_strict_runtime_types_ranges_and_authorities(
    tmp_path: Path,
    body: str,
    field: str,
) -> None:
    with pytest.raises(ConfigError) as raised:
        _load_text(tmp_path, body)
    message = str(raised.value)
    assert str((tmp_path / ".ruyi_agent/ruyi.toml").resolve()) in message
    assert field in message


@pytest.mark.parametrize(
    ("body", "field"),
    [
        ("[unknown]\nvalue = 1\n", "unknown"),
        ('[gateway]\nGATEWAY_HOST = "127.0.0.1"\n', "gateway.GATEWAY_HOST"),
        ('[backend]\nBACKEND_KIND = "local"\n', "backend.BACKEND_KIND"),
        ("[backend]\n[backend.local.extra]\nvalue = 1\n", "backend.local.extra"),
        ('[channels.feishu]\nmedia_root = "legacy"\n', "channels.feishu.media_root"),
    ],
)
def test_unknown_runtime_shape_and_aliases_fail_at_source(
    tmp_path: Path,
    body: str,
    field: str,
) -> None:
    with pytest.raises(ConfigError) as raised:
        _load_text(tmp_path, body)
    message = str(raised.value)
    assert str((tmp_path / ".ruyi_agent/ruyi.toml").resolve()) in message
    assert field in message


def test_feishu_group_policy_requires_identity_at_configuration_edge(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError, match="channels.feishu"):
        _load_text(
            tmp_path,
            '[channels.feishu]\ngroup_policy = "open"\nrequire_mention = true\n',
        )


def test_projection_serializes_typed_values_for_compatibility_consumers(
    tmp_path: Path,
) -> None:
    settings, _ = _load_text(
        tmp_path,
        """
[backend.local]
inherit_env = false
[channels.telegram]
fallback_ips = ["1.2.3.4", "5.6.7.8"]
[channels.feishu]
allowed_users = ["ou_1", "ou_2"]
""",
    )
    projected: dict[str, str] = {}
    apply_runtime_settings_to_env(settings, env=projected)
    assert projected["LOCAL_BACKEND_INHERIT_ENV"] == "false"
    assert projected["TELEGRAM_FALLBACK_IPS"] == "1.2.3.4,5.6.7.8"
    assert projected["FEISHU_ALLOWED_USERS"] == "ou_1,ou_2"


@pytest.mark.parametrize("kind", ["local", "localshell", "local_shell"])
def test_backend_kind_accepts_only_exact_lowercase_spellings(
    tmp_path: Path,
    kind: str,
) -> None:
    settings, _ = _load_text(tmp_path, f'[backend]\nkind = "{kind}"\n')
    assert settings.backend.kind == "local"


@pytest.mark.parametrize("value", ["LOCAL", " local", "local ", "", "unknown"])
def test_backend_kind_rejects_case_or_whitespace_variants(
    tmp_path: Path,
    value: str,
) -> None:
    body = f'[backend]\nkind = "{value}"\n'
    with pytest.raises(ConfigError, match="backend.kind"):
        _load_text(tmp_path, body)


@pytest.mark.parametrize("value", ["0", "-1", "", "nan", "inf", "junk"])
def test_task_poll_interval_environment_values_are_strict(
    tmp_path: Path,
    value: str,
) -> None:
    with pytest.raises(ConfigError) as raised:
        _load_text(
            tmp_path,
            "",
            env={"TELEGRAM_TASK_POLL_INTERVAL": value},
        )
    message = str(raised.value)
    assert "channels.telegram.task_poll_interval" in message
    assert "TELEGRAM_TASK_POLL_INTERVAL" in message


@pytest.mark.parametrize("value", ["::1", "example.test", "256.1.1.1", ""])
def test_fallback_ips_environment_accepts_only_ipv4(
    tmp_path: Path,
    value: str,
) -> None:
    with pytest.raises(ConfigError) as raised:
        _load_text(
            tmp_path,
            "",
            env={"TELEGRAM_FALLBACK_IPS": value},
        )
    message = str(raised.value)
    assert "channels.telegram.fallback_ips" in message
    assert "TELEGRAM_FALLBACK_IPS" in message


def test_launch_overrides_are_typed_applied_after_strict_load(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    ruyi_home = project / ".ruyi_agent"
    ruyi_home.mkdir(parents=True)
    (ruyi_home / "ruyi.toml").write_text(
        '[gateway]\nbearer_token = "keep-me"\nunknown = true\n',
        encoding="utf-8",
    )
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    env: dict[str, str] = {}
    with pytest.raises(ConfigError, match="gateway.unknown"):
        configure_runtime_environment(
            env=env,
            launch_overrides=GatewayLaunchOverrides(
                host="127.0.0.1",
                port="9000",
                base_url="http://127.0.0.1:9000",
            ),
        )

    (ruyi_home / "ruyi.toml").write_text(
        '[gateway]\nbearer_token = "keep-me"\n',
        encoding="utf-8",
    )
    settings = configure_runtime_environment(
        env=env,
        launch_overrides=GatewayLaunchOverrides(
            host="127.0.0.1",
            port="9000",
            base_url="http://127.0.0.1:9000",
        ),
    )
    assert settings.gateway.port == 9000
    assert settings.gateway.bearer_token == "keep-me"
    assert env["GATEWAY_PORT"] == "9000"
