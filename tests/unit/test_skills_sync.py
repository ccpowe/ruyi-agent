from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ruyi_agent.runtime.skills.sync import SkillSyncer
from ruyi_agent.runtime.skills.types import SkillEntry


@dataclass(slots=True)
class UploadResponse:
    path: str
    error: str | None = None


class MemoryUploadBackend:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[UploadResponse]:
        for path, content in files:
            self.files[path] = content
        return [UploadResponse(path=path) for path, _content in files]


def write_skill(root: Path, name: str) -> SkillEntry:
    skill_dir = root / name
    skill_dir.mkdir(parents=True)
    skill_dir.joinpath("SKILL.md").write_text(
        "\n".join(
            [
                "---",
                f"name: {name}",
                f"description: {name} desc",
                "---",
                "",
                f"# {name}",
            ]
        ),
        encoding="utf-8",
    )
    skill_dir.joinpath("helper.txt").write_text("helper", encoding="utf-8")
    return SkillEntry(
        name=name,
        description=f"{name} desc",
        path=skill_dir,
        source_root=root,
    )


def test_skill_syncer_materializes_selected_skills_to_backend_view(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "skills"
    frontend = write_skill(source_root, "frontend")
    hidden = write_skill(source_root, "hidden")
    backend = MemoryUploadBackend()

    view = SkillSyncer(backend=backend, views_root="/.ruyi_agent/runtime/skill-views").ensure_view(
        {"frontend": frontend, "hidden": hidden},
        ("frontend",),
    )

    assert view.skill_names == ("frontend",)
    assert view.path.startswith("/.ruyi_agent/runtime/skill-views/")
    assert f"{view.path}/frontend/SKILL.md" in backend.files
    assert f"{view.path}/frontend/helper.txt" in backend.files
    assert all("/hidden/" not in path for path in backend.files)
    assert f"{view.path}/.manifest.json" in backend.files
