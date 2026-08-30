from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import ruyi_agent.entrypoints.main as entrypoint
from ruyi_agent.config.paths import RuyiPaths
from ruyi_agent.config.runtime_settings import RuntimeSettings
from ruyi_agent.config.runtime_settings import load_runtime_settings


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def run_channels(
        self,
        channels: tuple[str, ...],
        settings: RuntimeSettings,
    ) -> None:
        self.calls.append(("channels", channels, settings))


def _runtime_settings(
    tmp_path: Path,
    *,
    telegram: bool = False,
    feishu: bool = False,
) -> RuntimeSettings:
    ruyi_home = tmp_path / ".ruyi_agent"
    ruyi_home.mkdir()
    (ruyi_home / "ruyi.toml").write_text("", encoding="utf-8")
    settings = load_runtime_settings(
        RuyiPaths(
            ruyi_home=ruyi_home,
            config_dir=ruyi_home / "config",
            data_dir=ruyi_home / "data",
            skills_dir=ruyi_home / "skills",
            workspace=tmp_path,
        ),
        env={},
    )
    telegram_settings = replace(
        settings.channels.telegram,
        bot_token="telegram-token" if telegram else None,
    )
    feishu_settings = replace(
        settings.channels.feishu,
        app_id="feishu-id" if feishu else None,
        app_secret="feishu-secret" if feishu else None,
    )
    return replace(
        settings,
        channels=replace(
            settings.channels,
            telegram=telegram_settings,
            feishu=feishu_settings,
        ),
    )


def _clear_channel_env(monkeypatch) -> None:
    for env_name in [
        "TELEGRAM_BOT_TOKEN",
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
    ]:
        monkeypatch.delenv(env_name, raising=False)


def test_cli_requires_an_explicit_entrypoint(capsys) -> None:
    try:
        entrypoint.parse_cli_options([])
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected missing entrypoint to fail")

    assert "select an entrypoint" in capsys.readouterr().err


def test_cli_all_starts_gateway_only_when_no_adapters_are_configured(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _clear_channel_env(monkeypatch)
    settings = _runtime_settings(tmp_path)
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda **kwargs: settings,
    )
    runner = FakeRunner()

    entrypoint.main(["--all"], runner=runner)

    assert runner.calls[0][:2] == ("channels", ("gateway",))
    assert runner.calls[0][2] is settings


def test_cli_all_starts_only_configured_adapters(tmp_path: Path, monkeypatch) -> None:
    _clear_channel_env(monkeypatch)
    monkeypatch.setenv("FEISHU_APP_ID", "feishu-id")
    monkeypatch.setenv("FEISHU_APP_SECRET", "feishu-secret")
    settings = _runtime_settings(tmp_path, feishu=True)
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda **kwargs: settings,
    )
    runner = FakeRunner()

    entrypoint.main(["--all"], runner=runner)

    assert runner.calls[0][:2] == ("channels", ("gateway", "feishu"))
    assert runner.calls[0][2] is settings


def test_cli_all_starts_all_configured_adapters(tmp_path: Path, monkeypatch) -> None:
    _clear_channel_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")
    monkeypatch.setenv("FEISHU_APP_ID", "feishu-id")
    monkeypatch.setenv("FEISHU_APP_SECRET", "feishu-secret")
    settings = _runtime_settings(tmp_path, telegram=True, feishu=True)
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda **kwargs: settings,
    )
    runner = FakeRunner()

    entrypoint.main(["--all"], runner=runner)

    assert runner.calls[0][:2] == (
        "channels",
        ("gateway", "telegram", "feishu"),
    )
    assert runner.calls[0][2] is settings


def test_cli_all_requires_existing_config_without_creating_templates(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    project = tmp_path / "workspace"
    user_home = tmp_path / "home"
    project.mkdir()
    user_home.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)

    try:
        entrypoint.main(["--all"], runner=FakeRunner())
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected missing config to fail")

    assert "ruyi --init" in capsys.readouterr().err
    assert not (project / ".ruyi_agent").exists()
    assert not (user_home / ".ruyi_agent").exists()


def test_cli_single_channel_starts_gateway_with_selected_adapter(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = _runtime_settings(tmp_path, telegram=True)
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda **kwargs: settings,
    )
    runner = FakeRunner()

    entrypoint.main(["--telegram"], runner=runner)

    assert runner.calls[0][:2] == ("channels", ("gateway", "telegram"))
    assert runner.calls[0][2] is settings


def test_cli_gateway_flag_starts_gateway_only(tmp_path: Path, monkeypatch) -> None:
    settings = _runtime_settings(tmp_path)
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda **kwargs: settings,
    )
    runner = FakeRunner()

    entrypoint.main(["--gateway"], runner=runner)

    assert runner.calls[0][:2] == ("channels", ("gateway",))
    assert runner.calls[0][2] is settings


def test_cli_init_configures_runtime_and_exits(monkeypatch) -> None:
    configured_calls: list[tuple[Path | str | None, bool, bool]] = []
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: (
            configured_calls.append((workspace, init_force, init_templates))
        ),
    )
    runner = FakeRunner()

    entrypoint.main(["--init"], runner=runner)

    assert configured_calls == [(None, False, True)]
    assert runner.calls == []


def test_cli_init_force_overwrites_bootstrap_files(monkeypatch) -> None:
    configured_calls: list[tuple[Path | str | None, bool, bool]] = []
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: (
            configured_calls.append((workspace, init_force, init_templates))
        ),
    )
    runner = FakeRunner()

    entrypoint.main(["--init", "--force"], runner=runner)

    assert configured_calls == [(None, True, True)]
    assert runner.calls == []


def test_cli_force_requires_init() -> None:
    try:
        entrypoint.parse_cli_options(["--force"])
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected --force without --init to fail")


def test_cli_reports_runtime_configuration_errors(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: (
            _ for _ in ()
        ).throw(ValueError("Invalid TOML in C:/Users/test/.ruyi_agent/ruyi.toml")),
    )

    try:
        entrypoint.main(["--gateway"], runner=FakeRunner())
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected runtime configuration error to exit")

    stderr = capsys.readouterr().err
    assert "Invalid TOML" in stderr
