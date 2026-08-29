"""Error classification for Gateway Task creation and delegation limits."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Literal

from ruyi_agent.gateway.errors import GatewayEffectDisposition, GatewayTaskError
from ruyi_agent.gateway.public_errors import (
    public_upstream_error,
    public_upstream_payload_error,
)
from ruyi_agent.gateway.route_reservations import (
    route_persistence_error,
    with_route_identity,
)
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import (
    MaxDelegationDepthError,
    MaxTasksPerRootError,
    RemoteExecutorNotImplementedError,
    UnknownAgentTargetError,
)
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import TaskRouteRecord


CreateFailureRecorder = Callable[
    [TaskRouteRecord, GatewayTaskError, bool],
    Awaitable[TaskRouteRecord | None],
]


async def recover_create_effect_error(
    exc: Exception,
    *,
    agent_name: str,
    route_kind: Literal["local", "remote_ref"],
    task_id: str,
    idempotency_key_present: bool,
    remote_replay_safe: bool,
    reservation: TaskRouteRecord,
    route_store: GatewayRouteStore,
    fail_reservation: CreateFailureRecorder,
) -> GatewayTaskError:
    """Classify a create failure and durably project its public route state."""

    error = create_effect_error(
        exc,
        agent_name=agent_name,
        route_kind=route_kind,
    )
    disposition = error.effect_disposition
    if disposition is None:  # pragma: no cover - classifier invariant
        raise RuntimeError("Create failure has no effect disposition") from exc
    durable_route: TaskRouteRecord | None = reservation
    if disposition == GatewayEffectDisposition.NOT_DISPATCHED:
        try:
            durable_route = await route_store.arestore_create_not_dispatched(task_id)
        except Exception as persistence_exc:
            raise route_persistence_error(
                task_id,
                route_state="unknown",
                queryable=True,
                retryable=False,
                effect_outcome="not_started",
            ) from persistence_exc
        retryable = True
    else:
        retryable = bool(
            disposition == GatewayEffectDisposition.OUTCOME_UNKNOWN
            and idempotency_key_present
            and route_kind == "remote_ref"
            and remote_replay_safe
        )
    if disposition != GatewayEffectDisposition.NOT_DISPATCHED and not retryable:
        durable_route = await fail_reservation(
            reservation,
            error,
            disposition == GatewayEffectDisposition.OUTCOME_UNKNOWN,
        )
    return with_route_identity(
        error,
        durable_route,
        task_id=task_id,
        retryable=retryable,
        route_state=reservation.route_state,
        effect_outcome=(
            "uncertain"
            if disposition == GatewayEffectDisposition.OUTCOME_UNKNOWN
            else "not_started"
        ),
    )


def create_effect_error(
    exc: Exception,
    *,
    agent_name: str,
    route_kind: Literal["local", "remote_ref"],
) -> GatewayTaskError:
    """Translate a create failure while preserving its effect disposition."""

    if isinstance(exc, UnknownAgentTargetError):
        locality = "Local runtime" if route_kind == "local" else "Runtime"
        return GatewayTaskError(
            code="runtime_unavailable",
            message=f"{locality} is not configured for agent '{agent_name}'",
            effect_disposition=GatewayEffectDisposition.AUTHORITATIVE_REJECTION,
        )
    if isinstance(exc, RemoteExecutorNotImplementedError):
        return GatewayTaskError(
            code="remote_executor_not_implemented",
            message=str(exc),
            effect_disposition=GatewayEffectDisposition.AUTHORITATIVE_REJECTION,
        )
    if isinstance(exc, MaxDelegationDepthError):
        return _with_disposition(
            delegation_depth_error(exc.current_depth, exc.max_depth),
            GatewayEffectDisposition.AUTHORITATIVE_REJECTION,
        )
    if isinstance(exc, MaxTasksPerRootError):
        return _with_disposition(
            delegation_budget_error(exc),
            GatewayEffectDisposition.AUTHORITATIVE_REJECTION,
        )
    if isinstance(exc, A2AClientError):
        disposition = (
            GatewayEffectDisposition.NOT_DISPATCHED
            if exc.effect_boundary == "not_dispatched"
            else (
                GatewayEffectDisposition.AUTHORITATIVE_REJECTION
                if 400 <= exc.status_code < 500
                else GatewayEffectDisposition.OUTCOME_UNKNOWN
            )
        )
        return _with_disposition(
            public_upstream_error(exc, operation="create"),
            disposition,
        )
    if isinstance(exc, ValueError):
        return _with_disposition(
            public_upstream_payload_error(operation="create"),
            (
                GatewayEffectDisposition.OUTCOME_UNKNOWN
                if route_kind == "remote_ref"
                else GatewayEffectDisposition.AUTHORITATIVE_REJECTION
            ),
        )
    return GatewayTaskError(
        code="task_creation_failed",
        message="Gateway Task creation failed",
        effect_disposition=GatewayEffectDisposition.OUTCOME_UNKNOWN,
    )


def _with_disposition(
    error: GatewayTaskError,
    disposition: GatewayEffectDisposition,
) -> GatewayTaskError:
    error.effect_disposition = disposition
    return error


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
