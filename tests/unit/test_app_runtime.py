from __future__ import annotations

import pytest

from ruyi_agent.runtime.bootstrap import AppRuntime
from ruyi_agent.runtime.bootstrap import DEFAULT_AGENT_NODE_ID
from ruyi_agent.runtime.bootstrap import _attach_delegation_scopes_to_local_specs
from ruyi_agent.runtime.bootstrap import _is_loopback_gateway_host
from ruyi_agent.runtime.bootstrap import _read_node_id_env
from ruyi_agent.config.loader import LocalWorkerSpec
from ruyi_agent.config.loader import RemoteRef


class FakeWorkerControl:
    def __init__(self) -> None:
        self.build_tools_calls = 0

    def build_tools_for(self, agent_name: str) -> list[object]:
        self.build_tools_calls += 1
        return [f"worker-tool:{agent_name}"]


def _local_spec(name: str) -> LocalWorkerSpec:
    return LocalWorkerSpec(
        name=name,
        description=f"{name} desc",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
    )


def test_attach_delegation_scopes_uses_each_agent_workers() -> None:
    agent_configs = {
        "main": {
            "kind": "local",
            "workers": ["research", "remote_wiki"],
        },
        "research": {
            "kind": "local",
            "workers": ["checker"],
        },
        "checker": {
            "kind": "local",
            "workers": [],
        },
        "remote_wiki": {
            "kind": "remote_ref",
        },
    }
    local_specs = {
        name: LocalWorkerSpec(
            name=name,
            description=f"{name} desc",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=[],
        )
        for name in ["main", "research", "checker"]
    }
    remote_refs = {
        "remote_wiki": RemoteRef(
            name="remote_wiki",
            description="remote wiki",
            url="https://example.com/a2a",
            remote_agent_name="wiki",
        )
    }
    worker_control = FakeWorkerControl()
    worker_control_ref = {"control": worker_control}

    attached = _attach_delegation_scopes_to_local_specs(
        agent_configs=agent_configs,
        all_local_specs=local_specs,
        all_remote_refs=remote_refs,
        worker_control_ref=worker_control_ref,
    )

    assert sorted(attached["main"].delegation_local_worker_specs or {}) == ["research"]
    assert sorted(attached["main"].delegation_remote_refs or {}) == ["remote_wiki"]
    assert sorted(attached["research"].delegation_local_worker_specs or {}) == [
        "checker"
    ]
    nested_research = (attached["main"].delegation_local_worker_specs or {})[
        "research"
    ]
    assert nested_research is attached["research"]
    assert sorted(nested_research.delegation_local_worker_specs or {}) == ["checker"]
    assert attached["checker"].delegation_local_worker_specs is None
    assert attached["checker"].build_delegation_tools is None
    assert attached["main"].build_delegation_tools is not None
    assert attached["main"].build_delegation_tools() == ["worker-tool:main"]
    assert attached["research"].build_delegation_tools is not None
    assert attached["research"].build_delegation_tools() == ["worker-tool:research"]


def test_app_runtime_get_local_agent_builds_and_caches_by_name() -> None:
    built_names: list[str] = []

    def build_local_agent(agent_name: str) -> object:
        built_names.append(agent_name)
        return object()

    runtime = AppRuntime(
        main_agent_name="main",
        agent_configs={
            "main": {"kind": "local", "description": "main agent"},
            "research": {"kind": "local", "description": "research agent"},
            "remote_wiki": {"kind": "remote_ref", "description": "remote wiki"},
        },
        local_agent_specs={
            "research": _local_spec("research"),
            "main": _local_spec("main"),
        },
        gateway_service=object(),  # type: ignore[arg-type]
        worker_control=object(),  # type: ignore[arg-type]
        gateway_control=object(),  # type: ignore[arg-type]
        checkpoint_db="checkpoints.sqlite",
        route_db="routes.sqlite",
        task_db="tasks.sqlite",
        _build_local_agent=build_local_agent,
        _local_agent_cache={},
    )

    assert runtime.list_local_agent_names() == ["main", "research"]

    first = runtime.get_local_agent("research")
    second = runtime.get_local_agent("research")
    default_agent = runtime.get_default_local_agent()

    assert first is second
    assert default_agent is runtime.get_local_agent("main")
    assert built_names == ["research", "main"]


def test_app_runtime_rejects_non_local_agent_for_streaming() -> None:
    runtime = AppRuntime(
        main_agent_name="main",
        agent_configs={
            "main": {"kind": "local"},
            "remote_wiki": {"kind": "remote_ref"},
        },
        local_agent_specs={"main": _local_spec("main")},
        gateway_service=object(),  # type: ignore[arg-type]
        worker_control=object(),  # type: ignore[arg-type]
        gateway_control=object(),  # type: ignore[arg-type]
        checkpoint_db="checkpoints.sqlite",
        route_db="routes.sqlite",
        task_db="tasks.sqlite",
        _build_local_agent=lambda agent_name: object(),
        _local_agent_cache={},
    )

    with pytest.raises(ValueError, match="kind='remote_ref'"):
        runtime.get_local_agent("remote_wiki")


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
