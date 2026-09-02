from __future__ import annotations

from pathlib import Path

import pytest

from ruyi_agent.runtime.skills.catalog import SkillCatalog


def write_skill(
    root: Path,
    directory_name: str,
    description: str,
    *,
    metadata_name: str | None = None,
) -> Path:
    skill_dir = root / directory_name
    skill_dir.mkdir(parents=True)
    skill_dir.joinpath("SKILL.md").write_text(
        "\n".join(
            [
                "---",
                f"name: {metadata_name if metadata_name is not None else directory_name}",
                f"description: {description}",
                "---",
                "",
                f"# {directory_name}",
            ]
        ),
        encoding="utf-8",
    )
    return skill_dir


def create_symlink(link: Path, target: Path, *, is_directory: bool) -> None:
    try:
        link.symlink_to(target, target_is_directory=is_directory)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this test environment: {exc}")


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


def test_catalog_skips_symlinked_skill_directory_and_skill_file(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    home = tmp_path / "home"
    source_root = home / ".agents" / "skills"
    outside_root = tmp_path / "outside"

    write_skill(source_root, "ordinary", "ordinary skill")
    outside_skill = write_skill(outside_root, "linked-directory", "outside skill")
    create_symlink(
        source_root / "linked-directory",
        outside_skill,
        is_directory=True,
    )

    linked_file_skill = source_root / "linked-file"
    linked_file_skill.mkdir()
    create_symlink(
        linked_file_skill / "SKILL.md",
        outside_skill / "SKILL.md",
        is_directory=False,
    )

    catalog = SkillCatalog(workspace_root=workspace, home_dir=home).scan()

    assert sorted(catalog.skills) == ["ordinary"]


@pytest.mark.parametrize(
    "name",
    ["", "   ", ".", "..", "/absolute", "nested/name", r"nested\name", "bad\0name"],
)
def test_catalog_rejects_unsafe_frontmatter_names(tmp_path: Path, name: str) -> None:
    workspace = tmp_path / "workspace"
    home = tmp_path / "home"
    source_root = home / ".agents" / "skills"
    write_skill(source_root, "skill-directory", "description", metadata_name=name)

    catalog = SkillCatalog(workspace_root=workspace, home_dir=home).scan()

    assert catalog.skills == {}


def test_catalog_allows_safe_unicode_uppercase_and_dot_name(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    home = tmp_path / "home"
    source_root = home / ".agents" / "skills"
    name = "Skill.技能"
    write_skill(source_root, "skill-directory", "description", metadata_name=name)

    catalog = SkillCatalog(workspace_root=workspace, home_dir=home).scan()

    assert tuple(catalog.skills) == (name,)
