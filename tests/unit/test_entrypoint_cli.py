from __future__ import annotations

from pathlib import Path

import ruyi_agent.entrypoints.main as entrypoint


class FakeRunner:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def run_tui(self) -> None:
        self.calls.append("tui")

    def run_channels(self, channels: tuple[str, ...]) -> None:
        self.calls.append(("channels", channels))


def _clear_channel_env(monkeypatch) -> None:
    for env_name in [
        "TELEGRAM_BOT_TOKEN",
        "FEISHU_APP_ID",
        "FEISHU_APP_SECRET",
    ]:
        monkeypatch.delenv(env_name, raising=False)


def test_cli_defaults_to_tui_and_applies_workspace(
    tmp_path: Path,
    monkeypatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    configured_calls: list[tuple[Path | str | None, bool]] = []
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: configured_calls.append(
            (workspace, init_templates)
        ),
    )
    runner = FakeRunner()

    entrypoint.main(["--workspace", str(workspace)], runner=runner)

    assert configured_calls == [(str(workspace), False)]
    assert runner.calls == ["tui"]


def test_cli_all_starts_gateway_only_when_no_adapters_are_configured(monkeypatch) -> None:
    _clear_channel_env(monkeypatch)
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: None,
    )
    runner = FakeRunner()

    entrypoint.main(["--all"], runner=runner)

    assert runner.calls == [("channels", ("gateway",))]


def test_cli_all_starts_only_configured_adapters(monkeypatch) -> None:
    _clear_channel_env(monkeypatch)
    monkeypatch.setenv("FEISHU_APP_ID", "feishu-id")
    monkeypatch.setenv("FEISHU_APP_SECRET", "feishu-secret")
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: None,
    )
    runner = FakeRunner()

    entrypoint.main(["--all"], runner=runner)

    assert runner.calls == [("channels", ("gateway", "feishu"))]


def test_cli_all_starts_all_configured_adapters(monkeypatch) -> None:
    _clear_channel_env(monkeypatch)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "telegram-token")
    monkeypatch.setenv("FEISHU_APP_ID", "feishu-id")
    monkeypatch.setenv("FEISHU_APP_SECRET", "feishu-secret")
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: None,
    )
    runner = FakeRunner()

    entrypoint.main(["--all"], runner=runner)

    assert runner.calls == [("channels", ("gateway", "telegram", "feishu"))]


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


def test_cli_single_channel_starts_gateway_with_selected_adapter(monkeypatch) -> None:
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: None,
    )
    runner = FakeRunner()

    entrypoint.main(["--telegram"], runner=runner)

    assert runner.calls == [("channels", ("gateway", "telegram"))]


def test_cli_gateway_flag_starts_gateway_only(monkeypatch) -> None:
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: None,
    )
    runner = FakeRunner()

    entrypoint.main(["--gateway"], runner=runner)

    assert runner.calls == [("channels", ("gateway",))]


def test_cli_tui_flag_starts_tui(monkeypatch) -> None:
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: None,
    )
    runner = FakeRunner()

    entrypoint.main(["--tui"], runner=runner)

    assert runner.calls == ["tui"]


def test_cli_init_configures_runtime_and_exits(monkeypatch) -> None:
    configured_calls: list[tuple[Path | str | None, bool, bool]] = []
    monkeypatch.setattr(
        entrypoint,
        "configure_runtime_environment",
        lambda *, workspace=None, init_force=False, init_templates=False: configured_calls.append(
            (workspace, init_force, init_templates)
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
        lambda *, workspace=None, init_force=False, init_templates=False: configured_calls.append(
            (workspace, init_force, init_templates)
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
        lambda *, workspace=None, init_force=False, init_templates=False: (_ for _ in ()).throw(
            ValueError("Invalid TOML in C:/Users/test/.ruyi_agent/ruyi.toml")
        ),
    )

    try:
        entrypoint.main(["--tui"], runner=FakeRunner())
    except SystemExit as exc:
        assert exc.code == 2
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected runtime configuration error to exit")

    stderr = capsys.readouterr().err
    assert "Invalid TOML" in stderr
