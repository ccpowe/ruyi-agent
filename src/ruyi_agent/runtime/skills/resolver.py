from __future__ import annotations

from collections.abc import Mapping, Sequence

from ruyi_agent.config.loader import SkillSelection
from ruyi_agent.runtime.skills.types import SkillEntry


def resolve_skill_names(
    selection: SkillSelection,
    catalog: Mapping[str, SkillEntry],
    *,
    parent_skill_names: Sequence[str] | None = None,
) -> tuple[str, ...]:
    if selection == "none":
        return ()
    if selection == "inherit":
        if parent_skill_names is not None:
            return tuple(parent_skill_names)
        return tuple(sorted(catalog))

    unknown = [name for name in selection if name not in catalog]
    if unknown:
        raise ValueError("Unknown skills: " + ", ".join(sorted(unknown)))
    return tuple(selection)
