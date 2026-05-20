from __future__ import annotations

from pathlib import Path

import pytest

from ruyi_agent.integrations.backend.runtime import create_backend_runtime


def test_create_local_backend_runtime_exposes_shell_and_file_transfer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    outside_file = tmp_path.parent / "outside-workspace.txt"
    outside_file.write_text("secret", encoding="utf-8")
    monkeypatch.setenv("BACKEND_KIND", "local")
    monkeypatch.setenv("LOCAL_BACKEND_ROOT", str(tmp_path))
    monkeypatch.setenv("LOCAL_BACKEND_INHERIT_ENV", "true")

    runtime = create_backend_runtime()

    assert runtime.kind == "local"
    assert runtime.home_dir == "/"
    assert runtime.skills_root == "/"

    file_path = "/nested/example.txt"

    upload_result = runtime.backend.upload_files([(file_path, b"hello")])
    assert upload_result[0].error is None

    download_result = runtime.backend.download_files([file_path])
    assert download_result[0].error is None
    assert download_result[0].content == b"hello"

    execute_result = runtime.backend.execute("pwd")
    assert execute_result.exit_code == 0
    assert str(tmp_path) in execute_result.output

    outside_read = runtime.backend.read(str(outside_file), limit=1)
    assert outside_read.error is not None

    runtime.close()


def test_create_local_backend_runtime_allows_custom_skills_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    skills_root = tmp_path / "data" / "skills"
    monkeypatch.setenv("BACKEND_KIND", "localshell")
    monkeypatch.setenv("LOCAL_BACKEND_ROOT", str(tmp_path))
    monkeypatch.setenv("LOCAL_BACKEND_SKILLS_ROOT", str(skills_root))

    runtime = create_backend_runtime()

    assert runtime.kind == "local"
    assert runtime.home_dir == "/"
    assert runtime.skills_root == "/data/skills"


def test_create_local_backend_runtime_rejects_skills_root_outside_workspace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BACKEND_KIND", "localshell")
    monkeypatch.setenv("LOCAL_BACKEND_ROOT", str(tmp_path / "workspace"))
    monkeypatch.setenv("LOCAL_BACKEND_SKILLS_ROOT", str(tmp_path / "skills"))

    with pytest.raises(ValueError, match="LOCAL_BACKEND_SKILLS_ROOT"):
        create_backend_runtime()


def test_create_backend_runtime_rejects_unknown_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BACKEND_KIND", "invalid")

    with pytest.raises(ValueError, match="Unsupported BACKEND_KIND"):
        create_backend_runtime()
