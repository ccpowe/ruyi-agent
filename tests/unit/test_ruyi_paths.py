from __future__ import annotations

from pathlib import Path

from ruyi_agent.config.paths import resolve_ruyi_paths


def test_resolve_ruyi_paths_prefers_workspace_local_home(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    project_home = project / ".ruyi_agent"
    user_home = tmp_path / "user"
    explicit_workspace = tmp_path / "workspace"
    project_home.mkdir(parents=True)
    (project_home / "ruyi.toml").write_text("", encoding="utf-8")
    user_home.mkdir()
    explicit_workspace.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)

    paths = resolve_ruyi_paths(workspace=explicit_workspace)

    assert paths.ruyi_home == project_home
    assert paths.config_dir == project_home / "config"
    assert paths.data_dir == project_home / "data"
    assert paths.skills_dir == project_home / "skills"
    assert paths.workspace == explicit_workspace


def test_resolve_ruyi_paths_ignores_runtime_only_workspace_home(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    runtime_dir = project / ".ruyi_agent" / "runtime" / "skill-views"
    user_home = tmp_path / "user"
    runtime_dir.mkdir(parents=True)
    user_home.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)

    paths = resolve_ruyi_paths()

    assert paths.ruyi_home == user_home / ".ruyi_agent"
    assert paths.workspace == project


def test_resolve_ruyi_paths_uses_user_home_when_project_has_no_ruyi_home(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project = tmp_path / "project"
    user_home = tmp_path / "user"
    project.mkdir()
    user_home.mkdir()
    monkeypatch.chdir(project)
    monkeypatch.setattr(Path, "home", lambda: user_home)

    paths = resolve_ruyi_paths()

    assert paths.ruyi_home == user_home / ".ruyi_agent"
    assert paths.workspace == project


def test_resolve_ruyi_paths_rejects_windows_style_ruyi_home_on_posix() -> None:
    try:
        resolve_ruyi_paths(env={"RUYI_HOME": r"C:\Users\cc\.ruyi_agent"})
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected Windows-style RUYI_HOME to fail on POSIX")

    assert "RUYI_HOME" in message
    assert "Windows-style path" in message


def test_resolve_ruyi_paths_rejects_windows_style_workspace_env_on_posix() -> None:
    try:
        resolve_ruyi_paths(env={"RUYI_WORKSPACE": r"C:\Users\cc\project"})
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected Windows-style RUYI_WORKSPACE to fail on POSIX")

    assert "RUYI_WORKSPACE" in message
    assert "Windows-style path" in message


def test_resolve_ruyi_paths_rejects_windows_style_workspace_arg_on_posix() -> None:
    try:
        resolve_ruyi_paths(workspace="C:/Users/cc/project")
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - assertion helper
        raise AssertionError("expected Windows-style workspace to fail on POSIX")

    assert "workspace" in message
    assert "Windows-style path" in message
