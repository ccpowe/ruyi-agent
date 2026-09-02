from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from ruyi_agent.runtime.skills.types import SkillEntry, SkillView


class SkillSyncer:
    """Materialize selected host-side skills into a backend-readable view."""

    def __init__(self, *, backend: Any, views_root: str) -> None:
        self._backend = backend
        self._views_root = views_root.rstrip("/") or "/"

    def ensure_view(
        self,
        catalog: Mapping[str, SkillEntry],
        skill_names: Sequence[str],
    ) -> SkillView | None:
        names = tuple(skill_names)
        if not names:
            return None

        entries: list[SkillEntry] = []
        snapshots: dict[str, tuple[tuple[str, bytes], ...]] = {}
        skill_hashes: dict[str, str] = {}
        for name in names:
            if not _is_safe_skill_name(name):
                raise ValueError(f"Unsafe skill name: {name!r}")
            entry = catalog[name]
            if entry.name != name:
                raise ValueError(f"Skill entry name does not match catalog key: {name!r}")
            if name not in snapshots:
                snapshots[name] = _snapshot_skill_files(entry)
                skill_hashes[name] = _hash_skill(snapshots[name])
            entries.append(entry)

        view_hash = _hash_view(names, skill_hashes)
        view_path = str(PurePosixPath(self._views_root) / view_hash)

        uploads: list[tuple[str, bytes]] = []
        for entry in entries:
            for relative, content in snapshots[entry.name]:
                backend_path = str(PurePosixPath(view_path) / entry.name / relative)
                uploads.append((backend_path, content))

        manifest = {
            "view_hash": view_hash,
            "skills": {
                entry.name: {
                    "hash": skill_hashes[entry.name],
                }
                for entry in entries
            },
        }
        uploads.append(
            (
                str(PurePosixPath(view_path) / ".manifest.json"),
                json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2).encode(
                    "utf-8"
                ),
            )
        )
        responses = self._backend.upload_files(uploads)
        errors = [
            getattr(response, "error", None)
            for response in responses
            if getattr(response, "error", None)
        ]
        if errors:
            raise ValueError("Failed to sync skills: " + "; ".join(errors))
        return SkillView(path=view_path, view_hash=view_hash, skill_names=names)


def _snapshot_skill_files(entry: SkillEntry) -> tuple[tuple[str, bytes], ...]:
    _validate_skill_entry(entry)
    skill_dir = entry.path
    paths = list(skill_dir.rglob("*"))
    for path in paths:
        _validate_tree_path(path, skill_dir, entry.source_root, entry.name)
    paths.sort(key=lambda path: path.relative_to(skill_dir).as_posix())

    files: list[tuple[str, bytes]] = []
    for path in paths:
        if path.is_file():
            files.append((path.relative_to(skill_dir).as_posix(), path.read_bytes()))
    return tuple(files)


def _validate_skill_entry(entry: SkillEntry) -> None:
    if not isinstance(entry, SkillEntry):
        raise ValueError("Invalid skill entry")
    if not _is_safe_skill_name(entry.name):
        raise ValueError(f"Unsafe skill name: {entry.name!r}")
    if not isinstance(entry.path, Path) or not isinstance(entry.source_root, Path):
        raise ValueError(f"Invalid skill paths for {entry.name!r}")
    if entry.source_root.is_symlink() or not entry.source_root.is_dir():
        raise ValueError(f"Unsafe skill source root for {entry.name!r}")
    if entry.path.is_symlink() or not entry.path.is_dir():
        raise ValueError(f"Unsafe skill directory for {entry.name!r}")
    _require_contained(entry.path, entry.source_root, entry.name)

    skill_file = entry.path / "SKILL.md"
    if skill_file.is_symlink() or not skill_file.is_file():
        raise ValueError(f"Unsafe SKILL.md for {entry.name!r}")
    _require_contained(skill_file, entry.path, entry.name)
    _require_contained(skill_file, entry.source_root, entry.name)


def _validate_tree_path(
    path: Path,
    skill_dir: Path,
    source_root: Path,
    skill_name: str,
) -> None:
    if path.is_symlink():
        raise ValueError(f"Skill tree contains a symlink for {skill_name!r}")
    _require_contained(path, skill_dir, skill_name)
    _require_contained(path, source_root, skill_name)


def _require_contained(path: Path, root: Path, skill_name: str) -> None:
    try:
        path.relative_to(root)
        path.resolve(strict=True).relative_to(root.resolve(strict=True))
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError(f"Skill path escapes its source root: {skill_name!r}") from exc


def _is_safe_skill_name(name: object) -> bool:
    return (
        isinstance(name, str)
        and bool(name)
        and bool(name.strip())
        and name not in {".", ".."}
        and not name.startswith("/")
        and "/" not in name
        and "\\" not in name
        and "\0" not in name
    )


def _hash_skill(files: Sequence[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for relative, content in files:
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(content)
        digest.update(b"\0")
    return digest.hexdigest()


def _hash_view(names: tuple[str, ...], skill_hashes: Mapping[str, str]) -> str:
    digest = hashlib.sha256()
    for name in names:
        digest.update(name.encode("utf-8"))
        digest.update(b":")
        digest.update(skill_hashes[name].encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()[:16]
