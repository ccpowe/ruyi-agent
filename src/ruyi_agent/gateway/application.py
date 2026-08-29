"""Shared application dependencies and transport-neutral Gateway projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import (
    AgentRefResponse,
    PublishedArtifactResponse,
    ReviewResponse,
    TaskResponse,
)
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.task_models import (
    MetadataScalar,
    PendingReviewRecord,
    PublishedArtifact,
    TaskRecord,
)


def agent_config_value(config: Any, field: str) -> Any:
    """Read one field from either legacy mappings or typed Agent configs."""

    if isinstance(config, dict):
        return config[field]
    return getattr(config, field)


@dataclass(slots=True)
class GatewayApplicationContext:
    """Mutable dependency holder shared by the Gateway application services.

    The holder intentionally contains no business behavior.  Keeping the router
    reference in one place also preserves the historical test/embedder ability
    to replace ``GatewayTaskModule._router`` after construction.
    """

    main_agent_name: str
    agent_configs: dict[str, Any]
    control: AgentControl
    router: TaskRouter
    command_store: GatewayCommandStore
    attachment_max_bytes: int
    artifact_max_bytes: int
    unavailable_agents: dict[str, str]
    remote_listing_concurrency: int


class GatewayProjection:
    """Map runtime records to the stable public Gateway response contract."""

    def build_agent(
        self,
        *,
        agent_name: str,
        config: Any,
        main_agent_name: str,
        unavailable_agents: dict[str, str],
    ) -> AgentRefResponse:
        return AgentRefResponse(
            name=agent_config_value(config, "name"),
            kind=agent_config_value(config, "kind"),
            public=agent_config_value(config, "public"),
            description=agent_config_value(config, "description"),
            is_default=agent_name == main_agent_name,
            available=agent_name not in unavailable_agents,
            unavailable_reason=unavailable_agents.get(agent_name),
        )

    def build_task(
        self,
        record: TaskRecord,
        metadata: dict[str, MetadataScalar],
    ) -> TaskResponse:
        return TaskResponse(
            task_id=record.task_id,
            agent_name=record.agent_name,
            parent_task_id=record.parent_task_id,
            root_task_id=record.root_task_id,
            depth=record.depth,
            status=record.state,
            last_result=record.result,
            error=record.error,
            run_count=record.run_count,
            created_at=record.created_at,
            updated_at=record.updated_at,
            metadata=dict(metadata),
            pending_review=record.pending_review,
            artifacts=[self.build_artifact(item) for item in record.artifacts],
        )

    def build_review(
        self,
        pending: PendingReviewRecord,
        record: TaskRecord,
        metadata: dict[str, MetadataScalar],
    ) -> ReviewResponse:
        raw_actions = pending.payload.get("action_requests")
        raw_configs = pending.payload.get("review_configs")
        return ReviewResponse(
            review_id=pending.review_id,
            task_id=pending.task_id,
            thread_id=record.thread_id,
            agent_name=record.agent_name,
            route_kind=record.route_kind,
            status="pending",
            action_requests=raw_actions if isinstance(raw_actions, list) else [],
            review_configs=raw_configs if isinstance(raw_configs, list) else [],
            created_at=pending.created_at,
            updated_at=pending.updated_at,
            metadata=dict(metadata),
        )

    def build_artifact(self, artifact: PublishedArtifact) -> PublishedArtifactResponse:
        return PublishedArtifactResponse(
            artifact_id=artifact.artifact_id,
            path=artifact.path,
            name=artifact.name,
            caption=artifact.caption,
            content_type=artifact.content_type,
            size=artifact.size,
            run_count=artifact.run_count,
        )


class GatewayAgentService:
    """Own public Agent discovery and availability policy."""

    def __init__(
        self,
        context: GatewayApplicationContext,
        projection: GatewayProjection,
    ) -> None:
        self._context = context
        self._projection = projection

    def list_agents(self) -> list[AgentRefResponse]:
        return [
            self._projection.build_agent(
                agent_name=name,
                config=config,
                main_agent_name=self._context.main_agent_name,
                unavailable_agents=self._context.unavailable_agents,
            )
            for name, config in sorted(self._context.agent_configs.items())
            if agent_config_value(config, "public")
        ]

    def get_agent(self, agent_name: str) -> AgentRefResponse:
        config = self.get_config(agent_name)
        self.ensure_public(agent_name, config)
        return self._projection.build_agent(
            agent_name=agent_name,
            config=config,
            main_agent_name=self._context.main_agent_name,
            unavailable_agents=self._context.unavailable_agents,
        )

    def get_config(self, agent_name: str) -> Any:
        try:
            return self._context.agent_configs[agent_name]
        except KeyError as exc:
            raise GatewayTaskError(
                code="agent_not_found",
                message=f"Agent '{agent_name}' does not exist",
            ) from exc

    def ensure_public(self, agent_name: str, config: Any) -> None:
        if agent_config_value(config, "public"):
            return
        raise GatewayTaskError(
            code="agent_not_public",
            message=f"Agent '{agent_name}' is not publicly callable",
        )

    def ensure_available(self, agent_name: str) -> None:
        reason = self._context.unavailable_agents.get(agent_name)
        if reason is None:
            return
        raise GatewayTaskError(
            code="agent_unavailable",
            message=f"Agent '{agent_name}' is unavailable: {reason}",
        )
