"""Public projection and errors for durable Gateway route reservations."""

from __future__ import annotations

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.task_models import TaskRecord, TaskRouteRecord


def reservation_record(route: TaskRouteRecord) -> TaskRecord:
    """Expose an effect-less reservation through the existing Task contract."""

    state = "failed" if route.route_state == "failed" else "interrupted"
    return TaskRecord(
        task_id=route.task_id,
        agent_name=route.agent_name,
        state=state,
        thread_id=route.task_id,
        parent_task_id=None,
        root_task_id=route.task_id,
        depth=1,
        created_at=route.created_at,
        updated_at=route.updated_at,
        error=route.route_error
        or f"Gateway route reservation is {route.route_state}",
        route_kind=route.route_kind,
        upstream_task_id=None,
        webhook=dict(route.webhook) if route.webhook is not None else None,
    )


def with_route_identity(
    error: GatewayTaskError,
    route: TaskRouteRecord | None,
    *,
    task_id: str,
    retryable: bool,
    route_state: str | None = None,
    effect_outcome: str,
    downstream_idempotency_guaranteed: bool | None = None,
) -> GatewayTaskError:
    details = dict(error.details or {})
    queryable = route is not None
    durable_state = route.route_state if route is not None else route_state or "unknown"
    details.update(
        {
            "task_id": task_id,
            "route_state": durable_state,
            "task_queryable": queryable,
            "create_retryable": retryable,
            "effect_outcome": effect_outcome,
            **({"task_url": f"/tasks/{task_id}"} if queryable else {}),
        }
    )
    if downstream_idempotency_guaranteed is not None:
        details["downstream_idempotency_guaranteed"] = (
            downstream_idempotency_guaranteed
        )
        details["upstream_task_id"] = (
            route.upstream_task_id if route is not None else None
        )
    return GatewayTaskError(
        code=error.code,
        message=error.message,
        details=details,
        kind=error.kind,
    )


def route_persistence_error(
    task_id: str,
    *,
    route_state: str,
    queryable: bool,
    retryable: bool,
    effect_outcome: str,
) -> GatewayTaskError:
    return GatewayTaskError(
        code="route_persistence_failed",
        message="Gateway could not durably transition the Task route",
        details={
            "task_id": task_id,
            "route_state": route_state,
            "task_queryable": queryable,
            "create_retryable": retryable,
            "effect_outcome": effect_outcome,
            **({"task_url": f"/tasks/{task_id}"} if queryable else {}),
        },
    )
