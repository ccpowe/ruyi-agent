from __future__ import annotations

from pathlib import Path

from ruyi_agent.runtime.skills.catalog import SkillCatalog


def write_skill(root: Path, name: str, description: str) -> None:
    skill_dir = root / name
    skill_dir.mkdir(parents=True)
    skill_dir.joinpath("SKILL.md").write_text(
        "\n".join(
            [
                "---",
                f"name: {name}",
                f"description: {description}",
                "---",
                "",
                f"# {name}",
            ]
        ),
        encoding="utf-8",
    )


def test_catalog_scans_fixed_roots_with_workspace_precedence(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    home = tmp_path / "home"
    workspace_skills = workspace / ".agents" / "skills"
    user_skills = home / ".agents" / "skills"
    managed_skills = home / ".ruyi_agent" / "skills"

    write_skill(managed_skills, "shared", "managed copy")
    write_skill(user_skills, "shared", "user copy")
    write_skill(workspace_skills, "shared", "workspace copy")
    write_skill(user_skills, "personal", "user only")

    catalog = SkillCatalog(workspace_root=workspace, home_dir=home).scan()

    assert sorted(catalog.skills) == ["personal", "shared"]
    assert catalog.skills["shared"].description == "workspace copy"
    assert catalog.skills["shared"].source_root == workspace_skills
    assert catalog.skills["personal"].source_root == user_skills
