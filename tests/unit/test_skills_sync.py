from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from ruyi_agent.runtime.skills.sync import SkillSyncer, _sort_snapshot_paths
from ruyi_agent.runtime.skills.types import SkillEntry


@dataclass(slots=True)
class UploadResponse:
    path: str
    error: str | None = None


class MemoryUploadBackend:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.upload_calls: list[list[tuple[str, bytes]]] = []

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[UploadResponse]:
        self.upload_calls.append(files)
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


def write_binary_skill(root: Path, files: dict[str, bytes]) -> SkillEntry:
    skill_dir = root / "frontend"
    skill_dir.mkdir(parents=True)
    skill_dir.joinpath("SKILL.md").write_bytes(
        b"---\nname: frontend\ndescription: frontend desc\n---\n"
    )
    for relative, content in files.items():
        file_path = skill_dir / relative
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(content)
    return SkillEntry(
        name="frontend",
        description="frontend desc",
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
    manifest = json.loads(backend.files[f"{view.path}/.manifest.json"])
    assert set(manifest["skills"]["frontend"]) == {"hash"}
    assert "source" not in manifest["skills"]["frontend"]
    assert str(source_root) not in backend.files[f"{view.path}/.manifest.json"].decode(
        "utf-8"
    )


@pytest.mark.parametrize(
    "name",
    [
        "",
        "   ",
        ".",
        "..",
        ".manifest.json",
        "/absolute",
        "nested/name",
        r"nested\name",
        "bad\0name",
    ],
)
def test_skill_syncer_rejects_unsafe_or_reserved_forged_entry_name_before_upload(
    tmp_path: Path,
    name: str,
) -> None:
    source_root = tmp_path / "skills"
    ordinary = write_skill(source_root, "ordinary")
    forged = SkillEntry(
        name=name,
        description=ordinary.description,
        path=ordinary.path,
        source_root=ordinary.source_root,
    )
    backend = MemoryUploadBackend()

    with pytest.raises(ValueError, match="Unsafe skill name"):
        SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
            {name: forged},
            (name,),
        )

    assert backend.upload_calls == []


def test_skill_syncer_rejects_forged_entry_outside_source_root_before_upload(
    tmp_path: Path,
) -> None:
    source_root = tmp_path / "skills"
    ordinary = write_skill(source_root, "ordinary")
    outside = write_skill(tmp_path / "outside", "frontend")
    forged = SkillEntry(
        name="frontend",
        description="frontend desc",
        path=outside.path,
        source_root=source_root,
    )
    backend = MemoryUploadBackend()

    with pytest.raises(ValueError, match="escapes its source root"):
        SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
            {"ordinary": ordinary, "frontend": forged},
            ("ordinary", "frontend"),
        )

    assert backend.upload_calls == []


@pytest.mark.parametrize("symlink_kind", ["directory", "skill-file"])
def test_skill_syncer_rejects_static_symlinked_entry_before_upload(
    tmp_path: Path,
    symlink_kind: str,
) -> None:
    source_root = tmp_path / "skills"
    source_root.mkdir()
    outside = write_skill(tmp_path / "outside", "frontend")
    if symlink_kind == "directory":
        skill_path = source_root / "frontend"
        try:
            skill_path.symlink_to(outside.path, target_is_directory=True)
        except OSError as exc:
            pytest.skip(f"symlinks are unavailable in this test environment: {exc}")
    else:
        skill_path = source_root / "frontend"
        skill_path.mkdir()
        try:
            skill_path.joinpath("SKILL.md").symlink_to(
                outside.path / "SKILL.md",
                target_is_directory=False,
            )
        except OSError as exc:
            pytest.skip(f"symlinks are unavailable in this test environment: {exc}")
    forged = SkillEntry(
        name="frontend",
        description="frontend desc",
        path=skill_path,
        source_root=source_root,
    )
    backend = MemoryUploadBackend()

    with pytest.raises(ValueError, match="Unsafe"):
        SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
            {"frontend": forged},
            ("frontend",),
        )

    assert backend.upload_calls == []


@pytest.mark.parametrize("is_directory", [False, True])
def test_skill_syncer_rejects_static_nested_symlinks_before_upload(
    tmp_path: Path,
    is_directory: bool,
) -> None:
    source_root = tmp_path / "skills"
    frontend = write_skill(source_root, "frontend")
    link = frontend.path / ("linked-directory" if is_directory else "linked-file")
    target = tmp_path / ("outside-directory" if is_directory else "outside-file.txt")
    if is_directory:
        target.mkdir()
        target.joinpath("outside.txt").write_text("outside", encoding="utf-8")
    else:
        target.write_text("outside", encoding="utf-8")
    try:
        link.symlink_to(target, target_is_directory=is_directory)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this test environment: {exc}")
    backend = MemoryUploadBackend()

    with pytest.raises(ValueError, match="contains a symlink"):
        SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
            {"frontend": frontend},
            ("frontend",),
        )

    assert backend.upload_calls == []


def test_skill_syncer_snapshots_stable_files_once_and_reuses_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_root = tmp_path / "skills"
    frontend = write_skill(source_root, "frontend")
    nested_file = frontend.path / "nested" / "payload.bin"
    nested_file.parent.mkdir()
    nested_file.write_bytes(b"\xff\x00payload")
    expected_files = {
        frontend.path / "SKILL.md": (frontend.path / "SKILL.md").read_bytes(),
        frontend.path / "helper.txt": (frontend.path / "helper.txt").read_bytes(),
        nested_file: nested_file.read_bytes(),
    }
    read_counts = {path: 0 for path in expected_files}
    rglob_calls = 0
    original_read_bytes = Path.read_bytes
    original_rglob = Path.rglob

    def read_bytes_once(path: Path) -> bytes:
        if path in read_counts:
            read_counts[path] += 1
            if read_counts[path] > 1:
                raise AssertionError(f"read more than once: {path}")
        return original_read_bytes(path)

    def snapshot_once(path: Path, pattern: str):
        nonlocal rglob_calls
        if path == frontend.path:
            rglob_calls += 1
        return original_rglob(path, pattern)

    monkeypatch.setattr(Path, "read_bytes", read_bytes_once)
    monkeypatch.setattr(Path, "rglob", snapshot_once)
    backend = MemoryUploadBackend()

    view = SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
        {"frontend": frontend},
        ("frontend",),
    )

    # This stable fixture verifies one read shared by hashing and upload. It
    # does not model a replacement racing validation with the open operation.
    ordered_files = [
        (path.relative_to(frontend.path).as_posix(), expected_files[path])
        for path in sorted(expected_files)
    ]
    skill_digest = hashlib.sha256(b"ruyi-agent.skill.v2\0")
    for relative, content in ordered_files:
        relative_bytes = relative.encode("utf-8")
        skill_digest.update(len(relative_bytes).to_bytes(8, byteorder="big"))
        skill_digest.update(relative_bytes)
        skill_digest.update(len(content).to_bytes(8, byteorder="big"))
        skill_digest.update(content)
    view_digest = hashlib.sha256(b"ruyi-agent.skill-view.v2\0")
    name_bytes = b"frontend"
    skill_hash_bytes = skill_digest.hexdigest().encode("ascii")
    view_digest.update(len(name_bytes).to_bytes(8, byteorder="big"))
    view_digest.update(name_bytes)
    view_digest.update(len(skill_hash_bytes).to_bytes(8, byteorder="big"))
    view_digest.update(skill_hash_bytes)

    assert rglob_calls == 1
    assert read_counts == {path: 1 for path in expected_files}
    assert view.view_hash == view_digest.hexdigest()[:16]
    for relative, content in ordered_files:
        assert backend.files[f"{view.path}/frontend/{relative}"] == content


def test_skill_syncer_v2_hash_separates_nul_framing_collisions(
    tmp_path: Path,
) -> None:
    first = write_binary_skill(
        tmp_path / "first",
        {"a": b"X\0b\0Y"},
    )
    second = write_binary_skill(
        tmp_path / "second",
        {"a": b"X", "b": b"Y"},
    )
    backend = MemoryUploadBackend()
    syncer = SkillSyncer(backend=backend, views_root="/skill-views")

    first_view = syncer.ensure_view({"frontend": first}, ("frontend",))
    second_view = syncer.ensure_view({"frontend": second}, ("frontend",))

    assert first_view.view_hash != second_view.view_hash
    assert first_view.path != second_view.path
    assert backend.files[f"{first_view.path}/frontend/a"] == b"X\0b\0Y"
    assert f"{first_view.path}/frontend/b" not in backend.files
    assert backend.files[f"{second_view.path}/frontend/a"] == b"X"
    assert backend.files[f"{second_view.path}/frontend/b"] == b"Y"


def test_skill_syncer_v2_hash_is_deterministic_for_same_stable_contents(
    tmp_path: Path,
) -> None:
    first = write_binary_skill(tmp_path / "first", {"payload.bin": b"ordinary"})
    second = write_binary_skill(tmp_path / "second", {"payload.bin": b"ordinary"})
    backend = MemoryUploadBackend()
    syncer = SkillSyncer(backend=backend, views_root="/skill-views")

    first_view = syncer.ensure_view({"frontend": first}, ("frontend",))
    second_view = syncer.ensure_view({"frontend": second}, ("frontend",))

    assert first_view.view_hash == second_view.view_hash
    assert first_view.path == second_view.path


def test_skill_syncer_allows_safe_unicode_uppercase_and_dot_name(tmp_path: Path) -> None:
    source_root = tmp_path / "skills"
    name = ".Skill.技能"
    skill = write_skill(source_root, name)
    backend = MemoryUploadBackend()

    view = SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
        {name: skill},
        (name,),
    )

    assert f"{view.path}/{name}/SKILL.md" in backend.files


class SnapshotOrderPath:
    def __init__(self, *, platform_rank: int, posix_text: str) -> None:
        self.platform_rank = platform_rank
        self.posix_text = posix_text

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, SnapshotOrderPath):
            return NotImplemented
        return self.platform_rank < other.platform_rank

    def as_posix(self) -> str:
        return self.posix_text


def test_snapshot_sort_keeps_path_order_not_posix_text_order() -> None:
    platform_first = SnapshotOrderPath(platform_rank=0, posix_text="z")
    platform_second = SnapshotOrderPath(platform_rank=1, posix_text="a")
    paths = [platform_second, platform_first]

    _sort_snapshot_paths(paths)  # type: ignore[arg-type]

    assert paths == [platform_first, platform_second]


def test_skill_syncer_rejects_static_symlinked_source_root_before_upload(
    tmp_path: Path,
) -> None:
    target_root = tmp_path / "target-skills"
    ordinary = write_skill(target_root, "frontend")
    linked_root = tmp_path / "linked-skills"
    try:
        linked_root.symlink_to(target_root, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"symlinks are unavailable in this test environment: {exc}")
    forged = SkillEntry(
        name="frontend",
        description=ordinary.description,
        path=linked_root / "frontend",
        source_root=linked_root,
    )
    backend = MemoryUploadBackend()

    with pytest.raises(ValueError, match="source root"):
        SkillSyncer(backend=backend, views_root="/skill-views").ensure_view(
            {"frontend": forged},
            ("frontend",),
        )

    assert backend.upload_calls == []
