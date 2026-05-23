from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from yaml import YAMLError

from ruyi_agent.runtime.skills.types import SkillEntry


@dataclass(frozen=True, slots=True)
class SkillCatalogSnapshot:
    skills: dict[str, SkillEntry]


class SkillCatalog:
    """Discover skills from Ruyi's fixed host-side skill roots."""

    def __init__(self, *, workspace_root: Path, home_dir: Path | None = None) -> None:
        self._workspace_root = workspace_root
        self._home_dir = home_dir or Path.home()

    @property
    def roots_by_precedence(self) -> list[Path]:
        return [
            self._home_dir / ".ruyi_agent" / "skills",
            self._home_dir / ".agents" / "skills",
            self._workspace_root / ".agents" / "skills",
        ]

    def scan(self) -> SkillCatalogSnapshot:
        skills: dict[str, SkillEntry] = {}
        for source_root in self.roots_by_precedence:
            if not source_root.is_dir():
                continue
            for child in sorted(source_root.iterdir()):
                entry = _read_skill_entry(child, source_root)
                if entry is None:
                    continue
                skills[entry.name] = entry
        return SkillCatalogSnapshot(skills=skills)


def _read_skill_entry(skill_dir: Path, source_root: Path) -> SkillEntry | None:
    if not skill_dir.is_dir():
        return None
    skill_file = skill_dir / "SKILL.md"
    if not skill_file.is_file():
        return None
    content = skill_file.read_text(encoding="utf-8")
    metadata = _parse_frontmatter(content)
    name = _metadata_string(metadata, "name")
    description = _metadata_string(metadata, "description")
    if not name or not description:
        return None
    return SkillEntry(
        name=name,
        description=description,
        path=skill_dir,
        source_root=source_root,
    )


def _parse_frontmatter(content: str) -> dict[str, Any]:
    if not content.startswith("---\n"):
        return {}
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}
    _prefix, frontmatter, _body = parts
    try:
        raw = yaml.safe_load(frontmatter)
    except YAMLError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _metadata_string(metadata: dict[str, Any], key: str) -> str:
    value = metadata.get(key)
    return value.strip() if isinstance(value, str) else ""
