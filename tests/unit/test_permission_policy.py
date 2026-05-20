from __future__ import annotations

from ruyi_agent.control_plane.permissions import (
    ExecuteRuleConfig,
    PermissionConfig,
    PermissionContext,
    PermissionDecision,
    PermissionPolicy,
    PermissionProfile,
    ToolPermissionConfig,
)


def build_policy() -> PermissionPolicy:
    return PermissionPolicy(
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


def build_yolo_policy() -> PermissionPolicy:
    return PermissionPolicy(
        PermissionConfig(
            default_profile="yolo",
            profiles={
                "yolo": PermissionProfile(
                    name="yolo",
                    tools={
                        "execute": ToolPermissionConfig(
                            policy=PermissionDecision.ALLOW,
                            allowed_decisions=["approve", "edit", "reject"],
                        ),
                    },
                    execute_rules=[
                        ExecuteRuleConfig(
                            match=["rm"],
                            policy=PermissionDecision.DENY,
                        ),
                    ],
                    execute_review_risks={
                        "command_parse_error",
                        "empty_command",
                        "privilege_escalation",
                        "destructive_filesystem",
                    },
                )
            },
        )
    )


def build_context(profile_name: str = "standard") -> PermissionContext:
    return PermissionContext(
        agent_name="coder",
        profile_name=profile_name,
        backend_kind="local",
        workspace_root="/tmp/project",
    )


def test_read_tool_uses_tool_policy_allow() -> None:
    result = build_policy().evaluate(
        tool_name="read_file",
        tool_args={"file_path": "README.md"},
        context=build_context(),
    )

    assert result.decision == PermissionDecision.ALLOW


def test_execute_allow_prefix_matches_simple_command() -> None:
    result = build_policy().evaluate(
        tool_name="execute",
        tool_args={"command": "git status --short"},
        context=build_context(),
    )

    assert result.decision == PermissionDecision.ALLOW
    assert result.risk == "execute_prefix_rule"


def test_execute_deny_prefix_wins() -> None:
    result = build_policy().evaluate(
        tool_name="execute",
        tool_args={"command": "rm -rf build"},
        context=build_context(),
    )

    assert result.decision == PermissionDecision.DENY
    assert result.risk == "execute_deny_rule"


def test_shell_control_operator_blocks_allow_prefix() -> None:
    result = build_policy().evaluate(
        tool_name="execute",
        tool_args={"command": "git status && echo ok"},
        context=build_context(),
    )

    assert result.decision == PermissionDecision.REQUIRE_APPROVAL
    assert result.risk == "shell_control_operator"


def test_yolo_allows_unreviewed_shell_control_risk() -> None:
    result = build_yolo_policy().evaluate(
        tool_name="execute",
        tool_args={"command": "git status && echo ok"},
        context=build_context("yolo"),
    )

    assert result.decision == PermissionDecision.ALLOW
    assert result.risk == "execute_default"


def test_deny_prefix_matches_shell_control_segments() -> None:
    result = build_yolo_policy().evaluate(
        tool_name="execute",
        tool_args={"command": "echo ok && rm -rf build"},
        context=build_context("yolo"),
    )

    assert result.decision == PermissionDecision.DENY
    assert result.risk == "execute_deny_rule"
