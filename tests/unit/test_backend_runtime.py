from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from daytona import DaytonaNotFoundError, SandboxState

from ruyi_agent.config.errors import ConfigError
from ruyi_agent.config.runtime_settings import configure_runtime_environment
import ruyi_agent.integrations.backend.runtime as backend_runtime
from ruyi_agent.integrations.backend.runtime import create_backend_runtime
from ruyi_agent.integrations.backend.runtime import RuyiLocalShellBackend


class FakeSandbox:
    def __init__(
        self,
        state: object,
        *,
        state_error: Exception | None = None,
        start_error: Exception | None = None,
    ) -> None:
        self._state = state
        self._state_error = state_error
        self._start_error = start_error
        self.start_calls = 0

    @property
    def state(self) -> object:
        if self._state_error is not None:
            raise self._state_error
        return self._state

    def start(self) -> None:
        self.start_calls += 1
        if self._start_error is not None:
            raise self._start_error


class FakeDaytonaClient:
    def __init__(
        self,
        *,
        sandbox: FakeSandbox | None = None,
        get_error: Exception | None = None,
        created_sandbox: FakeSandbox | None = None,
    ) -> None:
        self._sandbox = sandbox
        self._get_error = get_error
        self._created_sandbox = created_sandbox
        self.get_requests: list[str] = []
        self.create_requests: list[object] = []

    def get(self, sandbox_name: str) -> FakeSandbox:
        self.get_requests.append(sandbox_name)
        if self._get_error is not None:
            raise self._get_error
        assert self._sandbox is not None
        return self._sandbox

    def create(self, params: object) -> FakeSandbox:
        self.create_requests.append(params)
        assert self._created_sandbox is not None
        return self._created_sandbox


def _daytona_settings() -> SimpleNamespace:
    return SimpleNamespace(
        backend=SimpleNamespace(
            daytona=SimpleNamespace(
                api_key="test-key",
                api_url="https://daytona.test",
                target="test",
                sandbox_name="test-sandbox",
            )
        )
    )


def _patch_daytona_client(
    monkeypatch: pytest.MonkeyPatch,
    client: FakeDaytonaClient,
) -> None:
    def fake_config(**kwargs: object) -> SimpleNamespace:
        return SimpleNamespace(**kwargs)

    def fake_daytona(_config: object) -> FakeDaytonaClient:
        return client

    monkeypatch.setattr(backend_runtime, "DaytonaConfig", fake_config)
    monkeypatch.setattr(backend_runtime, "Daytona", fake_daytona)


@pytest.mark.parametrize("state", [SandboxState.STARTED, "started", "STARTED"])
def test_create_sandbox_skips_start_for_started_state(
    state: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = FakeSandbox(state)
    client = FakeDaytonaClient(sandbox=sandbox)
    _patch_daytona_client(monkeypatch, client)

    result = backend_runtime._create_sandbox(_daytona_settings())

    assert result is sandbox
    assert client.get_requests == ["test-sandbox"]
    assert client.create_requests == []
    assert sandbox.start_calls == 0


@pytest.mark.parametrize(
    "state",
    [SandboxState.STOPPED, "stopped", "STOPPED", SandboxState.UNKNOWN, None],
)
def test_create_sandbox_starts_non_started_or_unknown_state_once(
    state: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = FakeSandbox(state)
    client = FakeDaytonaClient(sandbox=sandbox)
    _patch_daytona_client(monkeypatch, client)

    result = backend_runtime._create_sandbox(_daytona_settings())

    assert result is sandbox
    assert client.create_requests == []
    assert sandbox.start_calls == 1


def test_create_sandbox_creates_not_found_without_starting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created_sandbox = FakeSandbox(SandboxState.CREATING)
    client = FakeDaytonaClient(
        get_error=DaytonaNotFoundError("sandbox missing"),
        created_sandbox=created_sandbox,
    )
    _patch_daytona_client(monkeypatch, client)

    result = backend_runtime._create_sandbox(_daytona_settings())

    assert result is created_sandbox
    assert client.get_requests == ["test-sandbox"]
    assert len(client.create_requests) == 1
    assert created_sandbox.start_calls == 0


@pytest.mark.parametrize(
    ("failure", "exception_type"),
    [
        ("state", RuntimeError),
        ("state", DaytonaNotFoundError),
        ("start", RuntimeError),
        ("start", DaytonaNotFoundError),
    ],
)
def test_create_sandbox_propagates_state_and_start_errors(
    failure: str,
    exception_type: type[Exception],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sandbox = FakeSandbox(SandboxState.STOPPED)
    error = exception_type("sandbox unavailable")
    setattr(sandbox, f"_{failure}_error", error)
    client = FakeDaytonaClient(sandbox=sandbox)
    _patch_daytona_client(monkeypatch, client)

    with pytest.raises(exception_type, match="sandbox unavailable"):
        backend_runtime._create_sandbox(_daytona_settings())

    assert client.create_requests == []
    assert sandbox.start_calls == (1 if failure == "start" else 0)


def test_create_local_backend_runtime_exposes_shell_and_file_transfer(
    tmp_path: Path,
) -> None:
    outside_file = tmp_path.parent / "outside-workspace.txt"
    outside_file.write_text("secret", encoding="utf-8")
    env = {"RUYI_HOME": str(tmp_path / ".ruyi_agent")}
    settings = configure_runtime_environment(
        workspace=tmp_path,
        env=env,
        init_templates=True,
    )

    runtime = create_backend_runtime(settings)

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
) -> None:
    env = {"RUYI_HOME": str(tmp_path / ".ruyi_agent")}
    settings = configure_runtime_environment(
        workspace=tmp_path,
        env=env,
        init_templates=True,
    )

    runtime = create_backend_runtime(settings)

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
    tmp_path: Path,
) -> None:
    ruyi_home = tmp_path / ".ruyi_agent"
    env = {"RUYI_HOME": str(ruyi_home)}
    configure_runtime_environment(workspace=tmp_path, env=env, init_templates=True)
    (ruyi_home / "ruyi.toml").write_text(
        '[backend]\nkind = "invalid"\n',
        encoding="utf-8",
    )

    with pytest.raises(ConfigError, match="backend.kind"):
        configure_runtime_environment(workspace=tmp_path, env=env)
