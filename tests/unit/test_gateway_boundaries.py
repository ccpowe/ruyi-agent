"""Contract tests for the Gateway application and HTTP composition boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from fastapi.routing import APIRoute

import ruyi_agent.channels.http.error_handlers as http_error_handlers
import ruyi_agent.channels.http.event_routes as http_event_routes
import ruyi_agent.channels.http.routes as http_routes
import ruyi_agent.channels.http.schemas as http_schemas
import ruyi_agent.gateway.attachments as gateway_attachments
import ruyi_agent.gateway.commands as gateway_commands
import ruyi_agent.gateway.tasks as gateway_tasks
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.gateway.application import GatewayAgentService
from ruyi_agent.gateway.artifacts import GatewayArtifactService
from ruyi_agent.gateway.commands import GatewayCommandOutcome, GatewayCommandService
from ruyi_agent.gateway.listing import GatewayListingService
from ruyi_agent.gateway.reviews import GatewayReviewService
from ruyi_agent.gateway.task_service import GatewayTaskService
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.runtime.delegation.async_runtime import AgentControl


@dataclass(frozen=True)
class TypedLikeAgentConfig:
    name: str
    kind: str
    public: bool
    description: str


def build_boundary_service() -> GatewayTaskModule:
    return GatewayTaskModule(
        main_agent_name="main",
        agent_configs={
            "main": TypedLikeAgentConfig(
                name="main",
                kind="local",
                public=True,
                description="Main Agent",
            ),
            "private": TypedLikeAgentConfig(
                name="private",
                kind="local",
                public=False,
                description="Private Agent",
            ),
        },
        control=cast(AgentControl, object()),
    )


def test_gateway_facade_composes_focused_application_services() -> None:
    service = build_boundary_service()

    assert isinstance(service._agents, GatewayAgentService)  # noqa: SLF001
    assert isinstance(service._tasks, GatewayTaskService)  # noqa: SLF001
    assert isinstance(service._commands, GatewayCommandService)  # noqa: SLF001
    assert isinstance(service._listings, GatewayListingService)  # noqa: SLF001
    assert isinstance(service._reviews, GatewayReviewService)  # noqa: SLF001
    assert isinstance(service._artifacts, GatewayArtifactService)  # noqa: SLF001

    agents = service.list_agents()
    assert [item.name for item in agents] == ["main"]
    assert service.get_agent("main").description == "Main Agent"


def test_gateway_command_outcome_remains_reexported_from_facade_module() -> None:
    from ruyi_agent.gateway.tasks import GatewayCommandOutcome as FacadeOutcome

    assert FacadeOutcome is GatewayCommandOutcome


def test_http_routes_facade_preserves_baseline_import_snapshot() -> None:
    expected_exports = {
        "TASK_EVENT_HEARTBEAT_SECONDS",
        "TaskInput",
        "HealthProbeResponse",
        "ReadyProbeResponse",
        "NotReadyProbeResponse",
        "CreateTaskRequest",
        "SendInputRequest",
        "ReviewDecisionInput",
        "ArtifactDownloadRequest",
        "HTTP_STATUS_BY_ERROR",
        "HTTP_STATUS_BY_ERROR_KIND",
    }

    assert expected_exports <= vars(http_routes).keys()
    assert (
        http_routes.TASK_EVENT_HEARTBEAT_SECONDS
        is http_event_routes.TASK_EVENT_HEARTBEAT_SECONDS
    )
    assert http_routes.TaskInput is http_schemas.TaskInput
    assert http_routes.HealthProbeResponse is http_schemas.HealthProbeResponse
    assert http_routes.ReadyProbeResponse is http_schemas.ReadyProbeResponse
    assert http_routes.NotReadyProbeResponse is http_schemas.NotReadyProbeResponse
    assert http_routes.CreateTaskRequest is http_schemas.CreateTaskRequest
    assert http_routes.SendInputRequest is http_schemas.SendInputRequest
    assert http_routes.ReviewDecisionInput is http_schemas.ReviewDecisionInput
    assert http_routes.ArtifactDownloadRequest is http_schemas.ArtifactDownloadRequest
    assert http_routes.HTTP_STATUS_BY_ERROR is http_error_handlers.HTTP_STATUS_BY_ERROR
    assert (
        http_routes.HTTP_STATUS_BY_ERROR_KIND
        is http_error_handlers.HTTP_STATUS_BY_ERROR_KIND
    )


def test_gateway_tasks_facade_preserves_baseline_import_snapshot() -> None:
    expected_exports = {
        "ATTACHMENT_METADATA_KEY",
        "ATTACHMENT_INBOX_SUBDIR",
        "SAFE_ATTACHMENT_CHARS",
        "COMMAND_WAIT_TIMEOUT_SECONDS",
    }

    assert expected_exports <= vars(gateway_tasks).keys()
    assert (
        gateway_tasks.ATTACHMENT_METADATA_KEY
        is gateway_attachments.ATTACHMENT_METADATA_KEY
    )
    assert (
        gateway_tasks.ATTACHMENT_INBOX_SUBDIR
        is gateway_attachments.ATTACHMENT_INBOX_SUBDIR
    )
    assert gateway_tasks.SAFE_ATTACHMENT_CHARS is gateway_attachments.SAFE_ATTACHMENT_CHARS
    assert (
        gateway_tasks.COMMAND_WAIT_TIMEOUT_SECONDS
        is gateway_commands.COMMAND_WAIT_TIMEOUT_SECONDS
    )


def test_gateway_http_composition_preserves_public_route_contract() -> None:
    app = create_gateway_app(service=build_boundary_service(), bearer_token="secret")
    actual = {
        (method, route.path, route.status_code or 200)
        for route in app.routes
        if isinstance(route, APIRoute)
        for method in route.methods
    }
    expected: set[tuple[str, str, int]] = {
        ("GET", "/health", 200),
        ("GET", "/ready", 200),
        ("GET", "/debug/team/login", 200),
        ("POST", "/debug/team/login", 200),
        ("POST", "/debug/team/logout", 200),
        ("GET", "/debug/team", 200),
        ("GET", "/debug/team/app.css", 200),
        ("GET", "/debug/team/app.js", 200),
        ("GET", "/agents", 200),
        ("GET", "/agents/{agent_name}", 200),
        ("POST", "/agents/{agent_name}/tasks", 201),
        ("GET", "/tasks/{task_id}", 200),
        ("GET", "/tasks/{task_id}/messages", 200),
        ("GET", "/tasks/{task_id}/events", 200),
        ("GET", "/reviews", 200),
        ("GET", "/reviews/{review_id}", 200),
        ("GET", "/tasks/{task_id}/reviews", 200),
        ("POST", "/tasks/{task_id}/input", 202),
        ("POST", "/artifacts/download", 200),
        ("GET", "/tasks/{task_id}/artifacts/{artifact_id}/download", 200),
        ("POST", "/tasks/{task_id}/cancel", 202),
        ("POST", "/tasks/{task_id}/reviews/{review_id}/decision", 202),
        ("POST", "/webhooks/tasks", 202),
        ("GET", "/tasks", 200),
    }

    assert actual == expected


def test_transport_models_stay_out_of_gateway_application_service_signatures() -> None:
    application_services: tuple[type[Any], ...] = (
        GatewayAgentService,
        GatewayTaskService,
        GatewayCommandService,
        GatewayListingService,
        GatewayReviewService,
        GatewayArtifactService,
    )

    assert all("fastapi" not in cls.__module__ for cls in application_services)
