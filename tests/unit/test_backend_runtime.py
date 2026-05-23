from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import ruyi_agent.integrations.backend.runtime as backend_runtime
from ruyi_agent.integrations.backend.runtime import create_backend_runtime
from ruyi_agent.integrations.backend.runtime import RuyiLocalShellBackend


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
    assert runtime.skills_root == "/.ruyi_agent/runtime/skill-views"

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


def test_create_local_backend_runtime_uses_fixed_skill_views_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BACKEND_KIND", "localshell")
    monkeypatch.setenv("LOCAL_BACKEND_ROOT", str(tmp_path))
    monkeypatch.setenv("LOCAL_BACKEND_SKILLS_ROOT", str(tmp_path / "legacy-skills"))

    runtime = create_backend_runtime()

    assert runtime.kind == "local"
    assert runtime.home_dir == "/"
    assert runtime.skills_root == "/.ruyi_agent/runtime/skill-views"


def test_local_shell_backend_decodes_utf8_output_on_windows_codepage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_run(*args: object, **kwargs: object) -> SimpleNamespace:
        assert kwargs["text"] is False
        return SimpleNamespace(
            stdout="box: ╦\n".encode("utf-8"),
            stderr=b"",
            returncode=0,
        )

    monkeypatch.setattr(backend_runtime.subprocess, "run", fake_run)
    backend = RuyiLocalShellBackend(
        root_dir=tmp_path,
        virtual_mode=True,
        inherit_env=False,
    )

    result = backend.execute("fake-command")

    assert result.exit_code == 0
    assert "box: ╦" in result.output


def test_create_backend_runtime_rejects_unknown_kind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BACKEND_KIND", "invalid")

    with pytest.raises(ValueError, match="Unsupported BACKEND_KIND"):
        create_backend_runtime()
