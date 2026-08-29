"""Compatibility composition facade for Gateway HTTP subroutes."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import FastAPI, Request

from ruyi_agent.gateway.tasks import GatewayTaskModule

from .artifact_routes import attach_artifact_routes
from .console_routes import attach_console_routes
from .context import GatewayHttpContext
from .error_handlers import attach_error_handlers
from .event_routes import attach_event_routes
from .probe_routes import attach_probe_routes
from .review_routes import attach_review_routes
from .task_routes import attach_task_routes
from .team_console_auth import TeamConsoleNoStoreMiddleware


def attach_gateway_routes(
    app: FastAPI,
    *,
    service_getter: Callable[[Request], GatewayTaskModule],
    bearer_token: str,
    readiness_getter: Callable[[Request], bool] | None = None,
) -> None:
    """Attach the stable Gateway HTTP API from focused transport subroutes."""

    context = GatewayHttpContext(
        service_getter=service_getter,
        bearer_token=bearer_token,
    )
    app.add_middleware(TeamConsoleNoStoreMiddleware)
    attach_probe_routes(app, context, readiness_getter=readiness_getter)
    attach_console_routes(app, context)
    attach_error_handlers(app, context)
    attach_task_routes(app, context)
    attach_event_routes(app, context)
    attach_review_routes(app, context)
    attach_artifact_routes(app, context)


def create_gateway_app(*, service: GatewayTaskModule, bearer_token: str) -> FastAPI:
    app = FastAPI(title="ruyi-agent Gateway")
    attach_gateway_routes(
        app,
        service_getter=lambda request: service,
        bearer_token=bearer_token,
    )
    return app
