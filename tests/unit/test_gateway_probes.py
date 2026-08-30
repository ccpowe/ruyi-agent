from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import cast

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import ruyi_agent.runtime.bootstrap as bootstrap_module
from ruyi_agent.channels.http.routes import (
    attach_gateway_routes,
    create_gateway_app,
)
from ruyi_agent.gateway.tasks import GatewayTaskModule


class ProbeCallbacks:
    def __init__(self, *, ready: bool = True) -> None:
        self.ready = ready
        self.readiness_calls = 0
        self.service_calls = 0
        self.readiness_error: Exception | None = None
        self.service_error: Exception | None = None
        self.service = cast(GatewayTaskModule, object())

    def get_readiness(self, request: Request) -> bool:
        del request
        self.readiness_calls += 1
        if self.readiness_error is not None:
            raise self.readiness_error
        return self.ready

    def get_service(self, request: Request) -> GatewayTaskModule:
        del request
        self.service_calls += 1
        if self.service_error is not None:
            raise self.service_error
        return self.service


def build_probe_app(callbacks: ProbeCallbacks) -> FastAPI:
    app = FastAPI()
    attach_gateway_routes(
        app,
        service_getter=callbacks.get_service,
        bearer_token="secret-token",
        readiness_getter=callbacks.get_readiness,
    )
    return app


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer wrong-token"}],
)
def test_health_probe_is_public_constant_and_no_store(
    headers: dict[str, str],
) -> None:
    callbacks = ProbeCallbacks()

    with TestClient(build_probe_app(callbacks)) as client:
        response = client.get("/health", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert response.headers["cache-control"] == "no-store"
    assert "www-authenticate" not in response.headers
    assert callbacks.readiness_calls == 0
    assert callbacks.service_calls == 0


@pytest.mark.parametrize(
    "headers",
    [{}, {"Authorization": "Bearer wrong-token"}],
)
def test_ready_probe_is_public_and_resolves_the_gateway_service(
    headers: dict[str, str],
) -> None:
    callbacks = ProbeCallbacks()

    with TestClient(build_probe_app(callbacks)) as client:
        response = client.get("/ready", headers=headers)

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert response.headers["cache-control"] == "no-store"
    assert "retry-after" not in response.headers
    assert "www-authenticate" not in response.headers
    assert callbacks.readiness_calls == 1
    assert callbacks.service_calls == 1


def test_ready_probe_short_circuits_service_resolution_while_not_ready() -> None:
    callbacks = ProbeCallbacks(ready=False)

    with TestClient(build_probe_app(callbacks)) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["retry-after"] == "1"
    assert callbacks.readiness_calls == 1
    assert callbacks.service_calls == 0


@pytest.mark.parametrize("failure_source", ["readiness", "service"])
def test_ready_probe_hides_internal_probe_failures(failure_source: str) -> None:
    callbacks = ProbeCallbacks()
    secret = "sensitive internal failure"
    if failure_source == "readiness":
        callbacks.readiness_error = RuntimeError(secret)
    else:
        callbacks.service_error = RuntimeError(secret)

    with TestClient(build_probe_app(callbacks)) as client:
        response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready"}
    assert secret not in response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["retry-after"] == "1"


def test_probe_endpoints_allow_only_get_and_leave_task_api_authenticated() -> None:
    callbacks = ProbeCallbacks()

    with TestClient(build_probe_app(callbacks)) as client:
        for path in ("/health", "/ready"):
            assert client.head(path).status_code == 405
            assert client.post(path).status_code == 405
        agents_response = client.get("/agents")

    assert agents_response.status_code == 401
    assert agents_response.headers["www-authenticate"] == (
        'Bearer realm="ruyi-agent-gateway"'
    )


def test_injected_gateway_app_is_ready_immediately() -> None:
    service = cast(GatewayTaskModule, object())
    app = create_gateway_app(service=service, bearer_token="secret-token")

    with TestClient(app) as client:
        response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_bootstrapped_app_readiness_tracks_runtime_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    teardown_gate_values: list[bool] = []
    app: FastAPI
    settings = SimpleNamespace(
        gateway=SimpleNamespace(
            bearer_token="dev-token",
            host="127.0.0.1",
        )
    )

    @asynccontextmanager
    async def fake_bootstrap_application(active_settings):
        assert active_settings is settings
        try:
            yield SimpleNamespace(gateway_service=object())
        finally:
            teardown_gate_values.append(bool(app.state.gateway_ready))

    monkeypatch.setattr(
        bootstrap_module,
        "configure_runtime_environment",
        lambda: settings,
    )
    monkeypatch.setattr(
        bootstrap_module,
        "bootstrap_application",
        fake_bootstrap_application,
    )
    app = bootstrap_module.create_bootstrapped_gateway_app(settings)

    async def request_without_lifespan() -> tuple[httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gateway.test",
        ) as client:
            return await client.get("/health"), await client.get("/ready")

    assert app.state.gateway_ready is False
    health_before, ready_before = asyncio.run(request_without_lifespan())
    assert health_before.status_code == 200
    assert ready_before.status_code == 503

    with TestClient(app) as client:
        assert app.state.gateway_ready is True
        ready_during = client.get("/ready")
        assert ready_during.status_code == 200

    assert app.state.gateway_ready is False
    assert teardown_gate_values == [False]
    _, ready_after = asyncio.run(request_without_lifespan())
    assert ready_after.status_code == 503


def test_bootstrapped_app_clears_readiness_before_failing_teardown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    teardown_gate_values: list[bool] = []
    app: FastAPI
    settings = SimpleNamespace(
        gateway=SimpleNamespace(
            bearer_token="dev-token",
            host="127.0.0.1",
        )
    )

    @asynccontextmanager
    async def failing_bootstrap_application(active_settings):
        assert active_settings is settings
        try:
            yield SimpleNamespace(gateway_service=object())
        finally:
            teardown_gate_values.append(bool(app.state.gateway_ready))
            raise RuntimeError("simulated runtime teardown failure")

    monkeypatch.setattr(
        bootstrap_module,
        "configure_runtime_environment",
        lambda: settings,
    )
    monkeypatch.setattr(
        bootstrap_module,
        "bootstrap_application",
        failing_bootstrap_application,
    )
    app = bootstrap_module.create_bootstrapped_gateway_app(settings)

    with pytest.raises(RuntimeError, match="simulated runtime teardown failure"):
        with TestClient(app) as client:
            assert client.get("/ready").status_code == 200

    assert app.state.gateway_ready is False
    assert teardown_gate_values == [False]
