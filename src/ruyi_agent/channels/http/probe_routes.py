"""Unauthenticated liveness and readiness Gateway routes."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .context import GatewayHttpContext
from .schemas import HealthProbeResponse, NotReadyProbeResponse, ReadyProbeResponse

_PROBE_HEADERS = {"Cache-Control": "no-store"}


def attach_probe_routes(
    app: FastAPI,
    context: GatewayHttpContext,
    *,
    readiness_getter: Callable[[Request], bool] | None,
) -> None:
    @app.get(
        "/health",
        response_model=HealthProbeResponse,
        tags=["Operations"],
        summary="Check Gateway liveness",
    )
    async def health_probe() -> JSONResponse:
        return JSONResponse(
            status_code=200,
            content={"status": "ok"},
            headers=dict(_PROBE_HEADERS),
        )

    @app.get(
        "/ready",
        response_model=ReadyProbeResponse,
        responses={
            503: {
                "model": NotReadyProbeResponse,
                "description": "Gateway runtime is not ready to accept traffic",
            }
        },
        tags=["Operations"],
        summary="Check Gateway readiness",
    )
    async def readiness_probe(request: Request) -> JSONResponse:
        try:
            ready = readiness_getter is None or readiness_getter(request)
            if ready:
                context.service(request)
        except Exception:
            ready = False
        if ready:
            return JSONResponse(
                status_code=200,
                content={"status": "ready"},
                headers=dict(_PROBE_HEADERS),
            )
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready"},
            headers={**_PROBE_HEADERS, "Retry-After": "1"},
        )
