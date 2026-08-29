"""Sanitize errors and records crossing a remote Gateway trust boundary."""

from __future__ import annotations

from dataclasses import replace

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.task_models import TaskRecord, TaskRouteKind, TaskRouteRecord

_SAFE_UPSTREAM_CODES = {
    "agent_unavailable",
    "runtime_unavailable",
    "task_events_unavailable",
    "task_history_unavailable",
    "task_not_found",
    "upstream_gateway_error",
}

_OPERATION_MESSAGES = {
    "create": "Remote Gateway Task creation failed",
    "get": "Remote Gateway Task refresh failed",
    "send": "Remote Gateway Task input failed",
    "cancel": "Remote Gateway Task cancellation failed",
    "review": "Remote Gateway review submission failed",
    "messages": "Remote Gateway Task message request failed",
    "events": "Remote Gateway Task event request failed",
}


def public_upstream_error(
    exc: A2AClientError,
    *,
    operation: str,
    route: TaskRouteRecord | None = None,
    task_id: str | None = None,
) -> GatewayTaskError:
    """Map an untrusted downstream error to public-only Gateway fields."""

    public_id = route.task_id if route is not None else task_id
    details = public_route_details(route=route, task_id=public_id)
    if operation == "messages" and (
        exc.status_code == 400 and exc.code == "invalid_request"
    ):
        return GatewayTaskError(
            code="invalid_request",
            message="Remote Gateway rejected the Task message request",
            details=details or None,
        )
    if operation == "events" and (
        exc.status_code == 400 and exc.code == "invalid_request"
    ):
        return GatewayTaskError(
            code="invalid_request",
            message="Remote Gateway rejected the Task event request",
            details=details or None,
        )
    if operation == "events" and (
        exc.status_code == 409 and exc.code == "task_run_mismatch"
    ):
        for key in ("requested_run_count", "current_run_count"):
            value = (exc.details or {}).get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                details[key] = value
        return GatewayTaskError(
            code="task_run_mismatch",
            message="Remote Gateway Task run does not match the requested run",
            details=details or None,
        )
    return GatewayTaskError(
        kind="upstream_failure",
        code=(
            exc.code if exc.code in _SAFE_UPSTREAM_CODES else "upstream_gateway_error"
        ),
        message=_OPERATION_MESSAGES.get(
            operation,
            "Remote Gateway Task operation failed",
        ),
        details=details or None,
    )


def public_upstream_payload_error(
    *,
    operation: str,
    route: TaskRouteRecord | None = None,
    task_id: str | None = None,
) -> GatewayTaskError:
    """Return a static payload-validation error without echoing peer content."""

    public_id = route.task_id if route is not None else task_id
    details = public_route_details(route=route, task_id=public_id)
    return GatewayTaskError(
        kind="upstream_failure",
        code="upstream_gateway_error",
        message="Remote Gateway returned an invalid Task payload",
        details=details or None,
    )


def public_route_details(
    *,
    route: TaskRouteRecord | None,
    task_id: str | None,
) -> dict[str, object]:
    """Build the only route identity allowed in public proxy errors."""

    if task_id is None:
        return {}
    details: dict[str, object] = {
        "task_id": task_id,
        "task_url": f"/tasks/{task_id}",
    }
    if route is not None:
        details["route_state"] = route.route_state
    return details


def public_remote_record(record: TaskRecord) -> TaskRecord:
    """Defensively project a remote record using only local public identity."""

    pending_review = (
        dict(record.pending_review) if record.pending_review is not None else None
    )
    if pending_review is not None and "source_task_id" in pending_review:
        pending_review["source_task_id"] = record.task_id
    return replace(
        record,
        thread_id=record.task_id,
        error=("Remote Gateway Task failed" if record.error is not None else None),
        pending_review=pending_review,
    )


def public_create_route_error(
    error: BaseException | str,
    *,
    route_kind: TaskRouteKind,
) -> str:
    """Return a safe durable route error; never persist a downstream payload."""

    if route_kind == "remote_ref":
        return "Remote Gateway Task creation failed"
    if isinstance(error, GatewayTaskError):
        return error.message
    if isinstance(error, str):
        return error
    return "Gateway Task creation failed"
