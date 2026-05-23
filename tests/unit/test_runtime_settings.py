from __future__ import annotations

from pathlib import Path

from ruyi_agent.config.paths import resolve_ruyi_paths
from ruyi_agent.config.runtime_settings import configure_runtime_environment
from ruyi_agent.config.runtime_settings import load_runtime_settings


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
                f'workspace = "{workspace}"',
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
    assert settings.env["BACKEND_KIND"] == "local"
    assert settings.env["LOCAL_BACKEND_ROOT"] == str(workspace)
    assert settings.env["GATEWAY_HOST"] == "0.0.0.0"
    assert settings.env["GATEWAY_PORT"] == "8765"
    assert settings.env["GATEWAY_BEARER_TOKEN"] == "strong-token"
    assert settings.env["CHECKPOINT_DB"] == str(ruyi_home / "state/checkpoints.sqlite")
    assert settings.env["OPENROUTER_API_KEY"] == "openrouter-key"


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

    assert settings.env["TELEGRAM_BOT_TOKEN"] == "telegram-token"
    assert settings.env["TELEGRAM_DEFAULT_AGENT"] == "main"
    assert settings.env["TELEGRAM_FALLBACK_IPS"] == "149.154.167.220"
    assert settings.env["FEISHU_APP_ID"] == "feishu-id"
    assert settings.env["FEISHU_APP_SECRET"] == "feishu-secret"
    assert settings.env["FEISHU_REQUIRE_MENTION"] == "false"
    assert settings.env["FEISHU_ALLOWED_USERS"] == "ou_1,ou_2"


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

    assert settings.env["DAYTONA_API_KEY"] == "daytona-key"
    assert settings.env["DAYTONA_API_URL"] == "https://daytona.example/api"
    assert settings.env["DAYTONA_TARGET"] == "us"
    assert settings.env["DAYTONA_SANDBOX_NAME"] == "sandbox"


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
                f'workspace = "{config_workspace}"',
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
                f'workspace = "{config_workspace}"',
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
    assert (ruyi_home / "ruyi.toml").read_text(encoding="utf-8").startswith(
        "# Ruyi runtime settings"
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
                'media_root = ""',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.chdir(project)
    env = {"OPENROUTER_API_KEY": "external-key"}

    configure_runtime_environment(env=env)

    assert env["OPENROUTER_API_KEY"] == "external-key"
    assert "FEISHU_MEDIA_ROOT" not in env
