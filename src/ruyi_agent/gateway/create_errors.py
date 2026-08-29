"""Error classification for Gateway Task creation and delegation limits."""

from __future__ import annotations

from typing import Literal

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.public_errors import (
    public_upstream_error,
    public_upstream_payload_error,
)
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import (
    MaxDelegationDepthError,
    MaxTasksPerRootError,
    RemoteExecutorNotImplementedError,
    UnknownAgentTargetError,
)


def create_effect_error(
    exc: Exception,
    *,
    agent_name: str,
    route_kind: Literal["local", "remote_ref"],
) -> tuple[GatewayTaskError, bool, str]:
    """Translate create failures and identify result-uncertain effects."""

    if isinstance(exc, UnknownAgentTargetError):
        locality = "Local runtime" if route_kind == "local" else "Runtime"
        return (
            GatewayTaskError(
                code="runtime_unavailable",
                message=f"{locality} is not configured for agent '{agent_name}'",
            ),
            False,
            "not_started",
        )
    if isinstance(exc, RemoteExecutorNotImplementedError):
        return (
            GatewayTaskError(
                code="remote_executor_not_implemented",
                message=str(exc),
            ),
            False,
            "not_started",
        )
    if isinstance(exc, MaxDelegationDepthError):
        return (
            delegation_depth_error(exc.current_depth, exc.max_depth),
            False,
            "not_started",
        )
    if isinstance(exc, MaxTasksPerRootError):
        return delegation_budget_error(exc), False, "not_started"
    if isinstance(exc, A2AClientError):
        uncertain = exc.status_code >= 500
        return (
            public_upstream_error(exc, operation="create"),
            uncertain,
            "uncertain" if uncertain else "not_started",
        )
    if isinstance(exc, ValueError):
        uncertain = route_kind == "remote_ref"
        return (
            public_upstream_payload_error(operation="create"),
            uncertain,
            "uncertain" if uncertain else "not_started",
        )
    return (
        GatewayTaskError(
            code="task_creation_failed",
            message="Gateway Task creation failed",
        ),
        True,
        "uncertain",
    )


def delegation_depth_error(current_depth: int, max_depth: int) -> GatewayTaskError:
    return GatewayTaskError(
        code="delegation_depth_exceeded",
        message=(
            "Delegation depth limit exceeded: "
            f"current_depth={current_depth} max_depth={max_depth}"
        ),
    )


def delegation_budget_error(exc: MaxTasksPerRootError) -> GatewayTaskError:
    return GatewayTaskError(
        code="delegation_budget_exhausted",
        message=(
            "Task budget exhausted: "
            f"root_task_id={exc.root_task_id} "
            f"current_count={exc.current_count} "
            f"max_tasks_per_root={exc.max_tasks_per_root}"
        ),
    )
