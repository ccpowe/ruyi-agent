from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from ruyi_agent.runtime.bootstrap import _is_loopback_gateway_host
from ruyi_agent.runtime.bootstrap import bootstrap_application
import ruyi_agent.runtime.bootstrap as bootstrap_module
import ruyi_agent.entrypoints.main as entrypoint_module
from ruyi_agent.config.paths import RuyiPaths
from ruyi_agent.config.runtime_settings import load_runtime_settings


def test_bootstrap_has_no_mutable_control_reference_or_scope_rewrite() -> None:
    source = inspect.getsource(bootstrap_application.__wrapped__)

    assert "control_ref" not in source
    assert "attach_delegation" not in source


@pytest.mark.parametrize(
    "host",
    ["127.0.0.1", "localhost", "::1", "[::1]"],
)
def test_is_loopback_gateway_host_accepts_local_hosts(host: str) -> None:
    assert _is_loopback_gateway_host(host)


@pytest.mark.parametrize(
    "host",
    ["0.0.0.0", "::", "example.com"],
)
def test_is_loopback_gateway_host_rejects_public_hosts(host: str) -> None:
    assert not _is_loopback_gateway_host(host)


def test_entrypoint_create_app_configures_runtime_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls = 0

    ruyi_home = tmp_path / ".ruyi_agent"
    ruyi_home.mkdir()
    (ruyi_home / "ruyi.toml").write_text("", encoding="utf-8")
    settings = load_runtime_settings(
        RuyiPaths(
            ruyi_home=ruyi_home,
            config_dir=ruyi_home / "config",
            data_dir=ruyi_home / "data",
            skills_dir=ruyi_home / "skills",
            workspace=tmp_path,
        ),
        env={},
    )

    def fake_configure_runtime_environment() -> object:
        nonlocal calls
        calls += 1
        return settings

    monkeypatch.setattr(
        entrypoint_module,
        "configure_runtime_environment",
        fake_configure_runtime_environment,
    )
    observed: list[object] = []

    def fake_create_app(active_settings: object) -> object:
        observed.append(active_settings)
        return object()

    monkeypatch.setattr(
        bootstrap_module, "create_bootstrapped_gateway_app", fake_create_app
    )

    result = entrypoint_module.create_app()

    assert calls == 1
    assert observed == [settings]
    assert result is not None
