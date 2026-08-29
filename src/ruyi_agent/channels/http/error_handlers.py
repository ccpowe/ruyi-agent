"""Stable Gateway HTTP exception-to-response mapping."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ruyi_agent.gateway.errors import GatewayTaskError

from .context import GatewayHttpContext

HTTP_STATUS_BY_ERROR = {
    "unauthorized": 401,
    "invalid_request": 400,
    "invalid_attachment": 400,
    "invalid_delegation_context": 400,
    "agent_not_public": 403,
    "delegation_depth_exceeded": 403,
    "delegation_loop_detected": 403,
    "workspace_path_forbidden": 403,
    "agent_not_found": 404,
    "artifact_not_found": 404,
    "review_not_found": 404,
    "task_not_found": 404,
    "review_task_mismatch": 409,
    "task_already_running": 409,
    "task_run_mismatch": 409,
    "task_route_unavailable": 409,
    "task_creation_not_retryable": 409,
    "idempotency_outcome_uncertain": 409,
    "idempotency_in_progress": 409,
    "idempotency_key_reused": 409,
    "attachment_too_large": 413,
    "artifact_too_large": 413,
    "remote_executor_not_implemented": 422,
    "delegation_budget_exhausted": 429,
    "upstream_gateway_error": 502,
    "agent_unavailable": 503,
    "attachment_upload_failed": 503,
    "runtime_unavailable": 503,
    "route_persistence_failed": 503,
    "task_effect_not_durable": 503,
    "task_history_unavailable": 503,
    "task_events_unavailable": 503,
    "idempotency_unavailable": 503,
}
HTTP_STATUS_BY_ERROR_KIND = {"upstream_failure": 502}


def attach_error_handlers(app: FastAPI, context: GatewayHttpContext) -> None:
    @app.exception_handler(GatewayTaskError)
    async def handle_gateway_error(
        request: Request,
        exc: GatewayTaskError,
    ) -> JSONResponse:
        del request
        payload: dict[str, Any] = {
            "error": {"code": exc.code, "message": exc.message}
        }
        if exc.details:
            payload["error"]["details"] = exc.details
        status_code = HTTP_STATUS_BY_ERROR_KIND.get(
            exc.kind,
            HTTP_STATUS_BY_ERROR.get(exc.code, 500),
        )
        headers: dict[str, str] | None = None
        if exc.code == "idempotency_in_progress":
            headers = {"Retry-After": "1"}
        elif exc.code == "unauthorized":
            headers = {"WWW-Authenticate": 'Bearer realm="ruyi-agent-gateway"'}
        return JSONResponse(status_code=status_code, content=payload, headers=headers)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        del exc
        headers = (
            {"Cache-Control": "no-store"}
            if context.console_auth.console_api_was_authenticated(request.scope)
            else None
        )
        return JSONResponse(
            status_code=500,
            headers=headers,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "Internal gateway error",
                }
            },
        )
