from __future__ import annotations

from pathlib import Path

import pytest

from ruyi_agent.runtime.skills.resolver import resolve_skill_names
from ruyi_agent.runtime.skills.types import SkillEntry


def entry(name: str) -> SkillEntry:
    path = Path("/skills") / name
    return SkillEntry(
        name=name,
        description=f"{name} desc",
        path=path,
        source_root=Path("/skills"),
    )


def test_resolve_skill_names_applies_selection_modes() -> None:
    catalog = {"frontend": entry("frontend"), "repo-workflow": entry("repo-workflow")}

    assert resolve_skill_names("inherit", catalog, parent_skill_names=("frontend",)) == (
        "frontend",
    )
    assert resolve_skill_names("none", catalog, parent_skill_names=("frontend",)) == ()
    assert resolve_skill_names(["repo-workflow"], catalog) == ("repo-workflow",)


def test_resolve_skill_names_rejects_unknown_names() -> None:
    with pytest.raises(ValueError, match="Unknown skills"):
        resolve_skill_names(["missing"], {"frontend": entry("frontend")})
