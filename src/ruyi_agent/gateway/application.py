"""Shared application dependencies and transport-neutral Gateway projections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ruyi_agent.config.agent_models import AgentConfig, AgentConfigs
from ruyi_agent.config.agent_parser import coerce_agent_configs
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


_LEGACY_GATEWAY_CATALOG_FIELDS = {
    "kind",
    "public",
    "name",
    "description",
}
_LEGACY_LOCAL_RUNTIME_DEFAULTS: dict[str, object] = {
    "system_prompt": "",
    "provider": "gateway-catalog",
    "model": "gateway-catalog",
    "memory": [],
    "skills": [],
    "server_names": [],
    "tool_names": [],
    "workers": [],
}


def parse_gateway_agent_configs(
    raw_agent_configs: Mapping[str, object],
) -> AgentConfigs:
    """Validate the legacy constructor input once at the Gateway boundary."""

    if not raw_agent_configs:
        # Listing-only embedders historically constructed an empty Gateway and
        # supplied their own remote listing router after initialization.
        return {}
    normalized = {
        name: _normalize_legacy_gateway_config(config)
        for name, config in raw_agent_configs.items()
    }
    return coerce_agent_configs(normalized)


def _normalize_legacy_gateway_config(config: object) -> object:
    if not isinstance(config, Mapping):
        return config
    if not set(config) <= _LEGACY_GATEWAY_CATALOG_FIELDS:
        return config
    if config.get("kind") == "local":
        return {**config, **_LEGACY_LOCAL_RUNTIME_DEFAULTS}
    if config.get("kind") == "remote_ref":
        configured_name = config.get("name")
        remote_agent_name = (
            configured_name
            if isinstance(configured_name, str) and configured_name
            else "gateway-catalog"
        )
        return {
            **config,
            "url": "https://gateway-catalog.invalid",
            "remote_agent_name": remote_agent_name,
        }
    return config


@dataclass(slots=True)
class GatewayApplicationContext:
    """Mutable dependency holder shared by the Gateway application services.

    The holder intentionally contains no business behavior.  Keeping the router
    reference in one place also preserves the historical test/embedder ability
    to replace ``GatewayTaskModule._router`` after construction.
    """

    main_agent_name: str
    agent_configs: AgentConfigs
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
        config: AgentConfig,
        main_agent_name: str,
        unavailable_agents: dict[str, str],
    ) -> AgentRefResponse:
        return AgentRefResponse(
            name=config.name,
            kind=config.kind,
            public=config.public,
            description=config.description,
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
            if config.public
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

    def get_config(self, agent_name: str) -> AgentConfig:
        try:
            return self._context.agent_configs[agent_name]
        except KeyError as exc:
            raise GatewayTaskError(
                code="agent_not_found",
                message=f"Agent '{agent_name}' does not exist",
            ) from exc

    def ensure_public(self, agent_name: str, config: AgentConfig) -> None:
        if config.public:
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
