from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SkillEntry:
    name: str
    description: str
    path: Path
    source_root: Path


@dataclass(frozen=True, slots=True)
class SkillView:
    path: str
    view_hash: str
    skill_names: tuple[str, ...]
