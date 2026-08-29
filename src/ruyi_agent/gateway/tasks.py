"""Transport-neutral facade for Gateway Task application services."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from ruyi_agent.gateway.application import (
    GatewayAgentService,
    GatewayApplicationContext,
    GatewayProjection,
)
from ruyi_agent.gateway.artifacts import GatewayArtifactService
from ruyi_agent.gateway.attachments import (
    ATTACHMENT_INBOX_SUBDIR as ATTACHMENT_INBOX_SUBDIR,
    ATTACHMENT_METADATA_KEY as ATTACHMENT_METADATA_KEY,
    SAFE_ATTACHMENT_CHARS as SAFE_ATTACHMENT_CHARS,
    GatewayAttachmentService,
)
from ruyi_agent.gateway.commands import (
    COMMAND_WAIT_TIMEOUT_SECONDS as COMMAND_WAIT_TIMEOUT_SECONDS,
    DEFAULT_GATEWAY_PRINCIPAL,
    GatewayCommandOutcome,
    GatewayCommandService,
)
from ruyi_agent.gateway.listing import GatewayListingService
from ruyi_agent.gateway.models import (
    AgentRefResponse,
    AttachmentInput,
    GatewayArtifact,
    ReviewListResponse,
    ReviewResponse,
    TaskListResponse,
    TaskMessageListResponse,
    TaskResponse,
    TaskWebhookEvent,
)
from ruyi_agent.gateway.reviews import GatewayReviewService
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.gateway.task_service import GatewayTaskService
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.task_events import TaskStreamEvent
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import MetadataScalar

DEFAULT_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_ARTIFACT_MAX_BYTES = 50 * 1024 * 1024
DEFAULT_REMOTE_LISTING_CONCURRENCY = 8


class GatewayTaskModule:
    """Stable facade composed from focused Gateway application services."""

    def __init__(
        self,
        *,
        main_agent_name: str,
        agent_configs: dict[str, Any],
        control: AgentControl,
        route_store: GatewayRouteStore | None = None,
        command_store: GatewayCommandStore | None = None,
        remote_event_handlers: list[AgentControl] | None = None,
        attachment_max_bytes: int = DEFAULT_ATTACHMENT_MAX_BYTES,
        artifact_max_bytes: int = DEFAULT_ARTIFACT_MAX_BYTES,
        unavailable_agents: dict[str, str] | None = None,
        remote_listing_concurrency: int = DEFAULT_REMOTE_LISTING_CONCURRENCY,
    ) -> None:
        if remote_listing_concurrency <= 0:
            raise ValueError("remote_listing_concurrency must be positive")
        router = TaskRouter(
            control=control,
            route_store=route_store or GatewayRouteStore(":memory:"),
            remote_event_handlers=remote_event_handlers,
        )
        context = GatewayApplicationContext(
            main_agent_name=main_agent_name,
            agent_configs=agent_configs,
            control=control,
            router=router,
            command_store=command_store or GatewayCommandStore(":memory:"),
            attachment_max_bytes=attachment_max_bytes,
            artifact_max_bytes=artifact_max_bytes,
            unavailable_agents=dict(unavailable_agents or {}),
            remote_listing_concurrency=remote_listing_concurrency,
        )
        projection = GatewayProjection()
        agents = GatewayAgentService(context, projection)
        attachments = GatewayAttachmentService(context)
        tasks = GatewayTaskService(context, agents, attachments, projection)
        listings = GatewayListingService(context, projection)
        self._context = context
        self._agents = agents
        self._tasks = tasks
        self._commands = GatewayCommandService(context, tasks)
        self._listings = listings
        self._reviews = GatewayReviewService(context, projection, listings)
        self._artifacts = GatewayArtifactService(context, attachments)

        # Preserve historical embedder/test aliases while production services
        # obtain their dependencies from the shared context.
        self._main_agent_name = main_agent_name
        self._agent_configs = agent_configs
        self._control = control
        self._command_store = context.command_store
        self._unavailable_agents = context.unavailable_agents

    @property
    def _router(self) -> TaskRouter:
        return self._context.router

    @_router.setter
    def _router(self, router: TaskRouter) -> None:
        self._context.router = router

    def list_agents(self) -> list[AgentRefResponse]:
        return self._agents.list_agents()

    def get_agent(self, agent_name: str) -> AgentRefResponse:
        return self._agents.get_agent(agent_name)

    async def create_task(
        self,
        *,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None = None,
    ) -> TaskResponse:
        return (
            await self.create_task_command(
                agent_name=agent_name,
                input_content=input_content,
                attachments=attachments,
                metadata=metadata,
                webhook=webhook,
                idempotency_key=None,
            )
        ).task

    async def create_task_command(
        self,
        *,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None = None,
        idempotency_key: str | None,
        principal_id: str = DEFAULT_GATEWAY_PRINCIPAL,
    ) -> GatewayCommandOutcome:
        return await self._commands.create_task(
            agent_name=agent_name,
            input_content=input_content,
            attachments=attachments,
            metadata=metadata,
            webhook=webhook,
            idempotency_key=idempotency_key,
            principal_id=principal_id,
        )

    async def get_task(self, task_id: str) -> TaskResponse:
        return await self._tasks.get_task(task_id)

    async def list_task_messages(
        self,
        task_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> TaskMessageListResponse:
        return await self._tasks.list_task_messages(task_id, cursor=cursor, limit=limit)

    @asynccontextmanager
    async def open_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AsyncIterator[AsyncIterator[TaskStreamEvent]]:
        async with self._tasks.open_task_event_stream(
            task_id,
            run_count=run_count,
            last_event_id=last_event_id,
        ) as events:
            yield events

    async def send_input(
        self,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
    ) -> TaskResponse:
        return (
            await self.send_input_command(
                task_id=task_id,
                input_content=input_content,
                attachments=attachments,
                idempotency_key=None,
            )
        ).task

    async def send_input_command(
        self,
        *,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
        idempotency_key: str | None,
        principal_id: str = DEFAULT_GATEWAY_PRINCIPAL,
    ) -> GatewayCommandOutcome:
        return await self._commands.send_input(
            task_id=task_id,
            input_content=input_content,
            attachments=attachments,
            idempotency_key=idempotency_key,
            principal_id=principal_id,
        )

    async def cancel_task(self, task_id: str) -> TaskResponse:
        return await self._tasks.cancel_task(task_id)

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskResponse:
        return await self._reviews.submit_decision(
            task_id=task_id,
            review_id=review_id,
            decisions=decisions,
        )

    async def download_artifact(self, path: str) -> GatewayArtifact:
        return await self._artifacts.download(path)

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact:
        return await self._artifacts.download_task_artifact(
            task_id=task_id,
            artifact_id=artifact_id,
        )

    async def list_tasks(
        self,
        *,
        agent_name: str | None,
        status: str | None,
        metadata_filters: dict[str, str],
        cursor: str | None,
        limit: int,
        root_task_id: str | None = None,
    ) -> TaskListResponse:
        return await self._listings.list_tasks(
            agent_name=agent_name,
            status=status,
            metadata_filters=metadata_filters,
            cursor=cursor,
            limit=limit,
            root_task_id=root_task_id,
        )

    async def list_reviews(
        self,
        *,
        cursor: str | None,
        limit: int,
    ) -> ReviewListResponse:
        return await self._reviews.list_reviews(cursor=cursor, limit=limit)

    async def get_review(self, review_id: str) -> ReviewResponse:
        return await self._reviews.get_review(review_id)

    async def list_task_reviews(self, task_id: str) -> ReviewListResponse:
        return await self._reviews.list_task_reviews(task_id)

    async def handle_task_webhook(self, event: TaskWebhookEvent) -> dict[str, Any]:
        return await self._tasks.handle_task_webhook(event)
