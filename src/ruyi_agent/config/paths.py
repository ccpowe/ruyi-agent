from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RuyiPaths:
    ruyi_home: Path
    config_dir: Path
    data_dir: Path
    skills_dir: Path
    workspace: Path


def resolve_ruyi_paths(
    *,
    workspace: str | Path | None = None,
    env: Mapping[str, str] | None = None,
) -> RuyiPaths:
    current_env = os.environ if env is None else env
    cwd = Path.cwd().resolve()
    project_ruyi_home = cwd / ".ruyi_agent"
    ruyi_home_value = current_env.get("RUYI_HOME")
    if ruyi_home_value:
        _reject_windows_style_path_on_posix("RUYI_HOME", ruyi_home_value)
        ruyi_home = Path(ruyi_home_value).expanduser()
    else:
        ruyi_home = (
            project_ruyi_home
            if _has_project_config(project_ruyi_home)
            else Path.home() / ".ruyi_agent"
        )
    ruyi_home = ruyi_home.resolve()

    if workspace is None:
        workspace_value = current_env.get("RUYI_WORKSPACE")
        if workspace_value:
            _reject_windows_style_path_on_posix("RUYI_WORKSPACE", workspace_value)
        workspace_path = Path(workspace_value).expanduser() if workspace_value else cwd
    else:
        _reject_windows_style_path_on_posix("workspace", str(workspace))
        workspace_path = Path(workspace).expanduser()

    workspace_path = workspace_path.resolve()
    return RuyiPaths(
        ruyi_home=ruyi_home,
        config_dir=ruyi_home / "config",
        data_dir=ruyi_home / "data",
        skills_dir=ruyi_home / "skills",
        workspace=workspace_path,
    )


def _has_project_config(project_ruyi_home: Path) -> bool:
    return (
        (project_ruyi_home / "ruyi.toml").is_file()
        or (project_ruyi_home / "config").is_dir()
    )


def _reject_windows_style_path_on_posix(name: str, value: str) -> None:
    if os.name == "nt":
        return
    if "\\" not in value and re.match(r"^[A-Za-z]:/", value) is None:
        return
    raise ValueError(
        f"{name} uses a Windows-style path on this POSIX system: {value!r}. "
        "Use a POSIX path such as ~/.ruyi_agent, or unset the variable to use "
        "the default home config directory."
    )
