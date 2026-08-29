"""Public projection and errors for durable Gateway route reservations."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.task_models import TaskRecord, TaskRouteRecord, TaskRouteKind


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
        # Older databases may contain an untrusted downstream error string.
        # A reservation has no routable downstream identity, so expose only
        # its public state instead of replaying that persisted content.
        error=f"Gateway Task route is {route.route_state}",
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
) -> GatewayTaskError:
    # Downstream error payloads are not part of the public Gateway identity.
    # Rebuild details from the durable route so an upstream Task id cannot leak.
    details: dict[str, object] = {}
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


async def shield_durable_cleanup(cleanup: Awaitable[None]) -> None:
    """Finish a durability update before propagating request cancellation."""

    cleanup_task = asyncio.ensure_future(cleanup)
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError:
            continue
    await cleanup_task


def has_durable_create_effect(
    record: TaskRecord,
    *,
    route_kind: TaskRouteKind,
) -> bool:
    if route_kind == "remote_ref":
        return bool(record.upstream_task_id)
    return record.run_count > 0 and record.state != "pending"


def has_active_route_binding(route: TaskRouteRecord) -> bool:
    return route.route_state == "active" and (
        route.route_kind != "remote_ref" or bool(route.upstream_task_id)
    )
