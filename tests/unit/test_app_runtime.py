from __future__ import annotations

import inspect

import pytest

from ruyi_agent.runtime.bootstrap import DEFAULT_AGENT_NODE_ID
from ruyi_agent.runtime.bootstrap import _is_loopback_gateway_host
from ruyi_agent.runtime.bootstrap import _read_node_id_env
from ruyi_agent.runtime.bootstrap import bootstrap_application
from ruyi_agent.runtime.bootstrap import create_bootstrapped_gateway_app
import ruyi_agent.runtime.bootstrap as bootstrap_module


def test_bootstrap_has_no_mutable_control_reference_or_scope_rewrite() -> None:
    source = inspect.getsource(bootstrap_application.__wrapped__)

    assert "control_ref" not in source
    assert "attach_delegation" not in source


def test_read_node_id_env_uses_default_for_missing_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AGENT_NODE_ID", raising=False)

    assert _read_node_id_env() == DEFAULT_AGENT_NODE_ID


def test_read_node_id_env_returns_configured_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AGENT_NODE_ID", "node-a")

    assert _read_node_id_env() == "node-a"


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


def test_create_bootstrapped_gateway_app_configures_runtime_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def fake_configure_runtime_environment() -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(
        bootstrap_module,
        "configure_runtime_environment",
        fake_configure_runtime_environment,
    )

    create_bootstrapped_gateway_app()

    assert calls == 1
