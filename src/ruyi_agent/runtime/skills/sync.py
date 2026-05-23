from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
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

        entries = [catalog[name] for name in names]
        skill_hashes = {entry.name: _hash_skill(entry) for entry in entries}
        view_hash = _hash_view(names, skill_hashes)
        view_path = str(PurePosixPath(self._views_root) / view_hash)

        uploads: list[tuple[str, bytes]] = []
        for entry in entries:
            for file_path in sorted(
                path for path in entry.path.rglob("*") if path.is_file()
            ):
                relative = file_path.relative_to(entry.path).as_posix()
                backend_path = str(PurePosixPath(view_path) / entry.name / relative)
                uploads.append((backend_path, file_path.read_bytes()))

        manifest = {
            "view_hash": view_hash,
            "skills": {
                entry.name: {
                    "hash": skill_hashes[entry.name],
                    "source": str(entry.path),
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


def _hash_skill(entry: SkillEntry) -> str:
    digest = hashlib.sha256()
    for file_path in sorted(path for path in entry.path.rglob("*") if path.is_file()):
        relative = file_path.relative_to(entry.path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_path.read_bytes())
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
