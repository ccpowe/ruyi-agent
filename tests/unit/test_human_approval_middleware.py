from __future__ import annotations

from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, ToolMessage

from ruyi_agent.runtime.middleware.human_approval import HumanApprovalMiddleware
from ruyi_agent.control_plane.permissions import (
    ExecuteRuleConfig,
    PermissionConfig,
    PermissionDecision,
    PermissionPolicy,
    PermissionProfile,
    ToolPermissionConfig,
)


def build_middleware() -> HumanApprovalMiddleware:
    policy = PermissionPolicy(
        PermissionConfig(
            default_profile="standard",
            profiles={
                "standard": PermissionProfile(
                    name="standard",
                    tools={
                        "execute": ToolPermissionConfig(
                            policy=PermissionDecision.REQUIRE_APPROVAL,
                            allowed_decisions=["approve", "edit", "reject"],
                        ),
                        "read_file": ToolPermissionConfig(
                            policy=PermissionDecision.ALLOW
                        ),
                    },
                    execute_rules=[
                        ExecuteRuleConfig(
                            match=["git", "status"],
                            policy=PermissionDecision.ALLOW,
                        ),
                        ExecuteRuleConfig(
                            match=["rm"],
                            policy=PermissionDecision.DENY,
                        ),
                    ],
                )
            },
        )
    )
    return HumanApprovalMiddleware(
        policy=policy,
        backend_kind="local",
        workspace_root="/tmp/project",
        agent_name="coder",
        permission_profile="standard",
    )


def test_allow_tool_does_not_update_messages() -> None:
    middleware = build_middleware()
    message = AIMessage(
        content="",
        tool_calls=[
            {"name": "read_file", "args": {"file_path": "README.md"}, "id": "call-1"}
        ],
    )

    assert middleware.after_model({"messages": [message]}, runtime=None) is None


def test_deny_tool_adds_artificial_error_message() -> None:
    middleware = build_middleware()
    message = AIMessage(
        content="",
        tool_calls=[
            {"name": "execute", "args": {"command": "rm -rf build"}, "id": "call-1"}
        ],
    )

    result = middleware.after_model({"messages": [message]}, runtime=None)

    assert result is not None
    messages = result["messages"]
    assert messages[0] is message
    assert isinstance(messages[1], ToolMessage)
    assert messages[1].status == "error"
    assert messages[1].tool_call_id == "call-1"
    assert "denied by permission policy" in str(messages[1].content)


def test_approval_allows_tool_call(monkeypatch: pytest.MonkeyPatch) -> None:
    middleware = build_middleware()
    message = AIMessage(
        content="",
        tool_calls=[
            {"name": "execute", "args": {"command": "python -V"}, "id": "call-1"}
        ],
    )
    interrupts: list[dict] = []

    def fake_interrupt(payload):
        interrupts.append(payload)
        return {"decisions": [{"type": "approve"}]}

    monkeypatch.setattr("ruyi_agent.runtime.middleware.human_approval.interrupt", fake_interrupt)

    result = middleware.after_model({"messages": [message]}, runtime=None)

    assert result is not None
    assert len(interrupts) == 1
    assert result["messages"] == [message]
    assert message.tool_calls[0]["args"] == {"command": "python -V"}


def test_edit_rechecks_policy_and_blocks_denied_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    middleware = build_middleware()
    message = AIMessage(
        content="",
        tool_calls=[
            {"name": "execute", "args": {"command": "python -V"}, "id": "call-1"}
        ],
    )

    def fake_interrupt(_payload):
        return {
            "decisions": [
                {
                    "type": "edit",
                    "edited_action": {
                        "name": "execute",
                        "args": {"command": "rm -rf build"},
                    },
                }
            ]
        }

    monkeypatch.setattr("ruyi_agent.runtime.middleware.human_approval.interrupt", fake_interrupt)

    result = middleware.after_model({"messages": [message]}, runtime=None)

    assert result is not None
    messages = result["messages"]
    assert message.tool_calls[0]["args"] == {"command": "rm -rf build"}
    assert isinstance(messages[1], ToolMessage)
    assert messages[1].status == "error"
    assert "denied by permission policy" in str(messages[1].content)
