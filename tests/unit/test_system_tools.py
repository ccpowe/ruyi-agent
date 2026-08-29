from __future__ import annotations

import pytest

from ruyi_agent.config.agent_models import LocalAgentConfig
from ruyi_agent.config.system_tools import resolve_system_tools
from ruyi_agent.config.system_tools import validate_system_tool_names


def _local_config(
    name: str,
    *,
    workers: tuple[str, ...] = (),
    system_tools: frozenset[str] = frozenset(),
    disabled_system_tools: frozenset[str] = frozenset(),
) -> LocalAgentConfig:
    return LocalAgentConfig(
        name=name,
        public=False,
        description="",
        system_prompt="",
        provider="provider",
        model="model",
        memory=(),
        skills=(),
        server_names=(),
        tool_names=(),
        workers=workers,
        system_tools=system_tools,
        disabled_system_tools=disabled_system_tools,
    )


def test_system_tools_merge_automatic_explicit_and_disabled() -> None:
    configs = {
        "main": _local_config(
            "main",
            workers=("child",),
            system_tools=frozenset({"tool_search"}),
            disabled_system_tools=frozenset({"cancel_agent"}),
        ),
        "child": _local_config("child"),
    }

    resolved = resolve_system_tools("main", configs)

    assert "spawn_agent" in resolved.enabled
    assert "tool_search" in resolved.enabled
    assert "cancel_agent" not in resolved.enabled
    assert "cancel_agent" in resolved.disabled


def test_leaf_worker_automatically_gets_parent_communication_only() -> None:
    configs = {
        "main": _local_config("main", workers=("child",)),
        "child": _local_config("child"),
    }

    resolved = resolve_system_tools("child", configs)

    assert {"send_input", "list_agents"} <= resolved.enabled
    assert "spawn_agent" not in resolved.enabled
    assert "wait_agent" not in resolved.enabled
    assert "cancel_agent" not in resolved.enabled


def test_unknown_system_tool_reports_available_names() -> None:
    with pytest.raises(ValueError, match="unknown system tools: magic") as exc_info:
        validate_system_tool_names("main", "system_tools", ["magic"])

    assert "send_input" in str(exc_info.value)
