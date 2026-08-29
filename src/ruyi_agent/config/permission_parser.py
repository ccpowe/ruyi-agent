"""Parse permission-profile configuration independently from TOML loading."""

from __future__ import annotations

from typing import Any

from ruyi_agent.control_plane.permissions import (
    ExecuteRuleConfig,
    KNOWN_EXECUTE_RISKS,
    PermissionConfig,
    PermissionDecision,
    PermissionProfile,
    ToolPermissionConfig,
)


def parse_permission_config(data: dict[str, Any]) -> PermissionConfig:
    default_profile = data.get("default_profile")
    if not isinstance(default_profile, str) or not default_profile:
        raise ValueError("permissions.default_profile must be a non-empty string")
    raw_profiles = data.get("profiles")
    if not isinstance(raw_profiles, dict) or not raw_profiles:
        raise ValueError("permissions profiles must be a non-empty table")

    profiles = {
        profile_name: _parse_permission_profile(profile_name, raw_profile)
        for profile_name, raw_profile in raw_profiles.items()
    }
    if default_profile not in profiles:
        raise ValueError(
            f"permissions.default_profile '{default_profile}' is not defined"
        )
    return PermissionConfig(default_profile=default_profile, profiles=profiles)


def _parse_permission_profile(
    profile_name: object,
    raw_profile: object,
) -> PermissionProfile:
    if not isinstance(profile_name, str) or not profile_name:
        raise ValueError("permission profile names must be non-empty strings")
    if not isinstance(raw_profile, dict):
        raise ValueError(f"profiles.{profile_name} must be a table")
    description = raw_profile.get("description")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"profiles.{profile_name}.description must be a string")
    raw_tools = raw_profile.get("tools", {})
    if not isinstance(raw_tools, dict):
        raise ValueError(f"profiles.{profile_name}.tools must be a table")
    tools = {
        tool_name: _parse_tool_permission(
            raw_tool,
            path=f"profiles.{profile_name}.tools.{tool_name}",
        )
        for tool_name, raw_tool in raw_tools.items()
    }
    raw_execute = raw_profile.get("execute", {})
    if raw_execute is None:
        raw_execute = {}
    if not isinstance(raw_execute, dict):
        raise ValueError(f"profiles.{profile_name}.execute must be a table")
    raw_rules = raw_execute.get("rules", [])
    if not isinstance(raw_rules, list):
        raise ValueError(f"profiles.{profile_name}.execute.rules must be a list")
    execute_rules = [
        _parse_execute_rule(
            raw_rule,
            path=f"profiles.{profile_name}.execute.rules[{idx}]",
        )
        for idx, raw_rule in enumerate(raw_rules)
    ]
    return PermissionProfile(
        name=profile_name,
        description=description,
        tools=tools,
        execute_rules=execute_rules,
        execute_review_risks=_parse_execute_review_risks(
            raw_execute.get("review_risks"),
            path=f"profiles.{profile_name}.execute.review_risks",
        ),
    )


def _parse_permission_decision(value: Any, *, path: str) -> PermissionDecision:
    if not isinstance(value, str):
        raise ValueError(f"{path}.policy must be a string")
    try:
        return PermissionDecision(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in PermissionDecision)
        raise ValueError(f"{path}.policy must be one of: {allowed}") from exc


def _parse_allowed_decisions(value: Any, *, path: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{path}.allowed_decisions must be a string list")
    unexpected = sorted(
        item for item in value if item not in {"approve", "edit", "reject"}
    )
    if unexpected:
        raise ValueError(
            f"{path}.allowed_decisions has unsupported values: " + ", ".join(unexpected)
        )
    return list(value)


def _parse_tool_permission(raw: Any, *, path: str) -> ToolPermissionConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a table")
    if "policy" not in raw:
        raise ValueError(f"{path}.policy is required")
    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"{path}.description must be a string")
    return ToolPermissionConfig(
        policy=_parse_permission_decision(raw["policy"], path=path),
        allowed_decisions=_parse_allowed_decisions(
            raw.get("allowed_decisions"), path=path
        ),
        description=description,
    )


def _parse_execute_rule(raw: Any, *, path: str) -> ExecuteRuleConfig:
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must be a table")
    match = raw.get("match")
    if not isinstance(match, list) or not all(
        isinstance(item, str) and item for item in match
    ):
        raise ValueError(f"{path}.match must be a non-empty string list")
    if "policy" not in raw:
        raise ValueError(f"{path}.policy is required")
    description = raw.get("description")
    if description is not None and not isinstance(description, str):
        raise ValueError(f"{path}.description must be a string")
    return ExecuteRuleConfig(
        match=list(match),
        policy=_parse_permission_decision(raw["policy"], path=path),
        allowed_decisions=_parse_allowed_decisions(
            raw.get("allowed_decisions"), path=path
        ),
        description=description,
    )


def _parse_execute_review_risks(value: Any, *, path: str) -> set[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item for item in value
    ):
        raise ValueError(f"{path} must be a string list")
    unexpected = sorted(set(value) - KNOWN_EXECUTE_RISKS)
    if unexpected:
        raise ValueError(
            f"{path} has unsupported risk values: " + ", ".join(unexpected)
        )
    return set(value)
