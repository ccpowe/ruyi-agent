"""Core Gateway Task operations independent of transport and command replay."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from ruyi_agent.gateway.application import (
    GatewayAgentService,
    GatewayApplicationContext,
    GatewayProjection,
)
from ruyi_agent.gateway.attachments import GatewayAttachmentService
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import (
    AttachmentInput,
    TaskMessageListResponse,
    TaskMessageResponse,
    TaskMessageToolCallResponse,
    TaskResponse,
    TaskWebhookEvent,
)
from ruyi_agent.runtime.delegation.context import DelegationContext
from ruyi_agent.runtime.task_events import TaskStreamEvent
from ruyi_agent.task_models import MetadataScalar, TaskRouteRecord


class GatewayTaskService:
    """Own Task routing orchestration and stable response projection."""

    def __init__(
        self,
        context: GatewayApplicationContext,
        agents: GatewayAgentService,
        attachments: GatewayAttachmentService,
        projection: GatewayProjection,
    ) -> None:
        self._context = context
        self._agents = agents
        self._attachments = attachments
        self._projection = projection

    async def create_effect(
        self,
        *,
        task_id: str,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput],
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None,
        idempotency_key: str | None,
    ) -> TaskResponse:
        config = self._agents.get_config(agent_name)
        self._agents.ensure_public(agent_name, config)
        self._agents.ensure_available(agent_name)
        clean_metadata, delegation = (
            self._context.router.prepare_delegation_metadata(metadata)
        )
        return await self._create_routed_task(
            task_id=task_id,
            agent_name=agent_name,
            route_kind=config.kind,
            input_content=input_content,
            attachments=attachments,
            metadata=clean_metadata,
            webhook=webhook,
            delegation=delegation,
            idempotency_key=idempotency_key,
        )

    async def _create_routed_task(
        self,
        *,
        task_id: str,
        agent_name: str,
        route_kind: str,
        input_content: str,
        attachments: list[AttachmentInput],
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None,
        delegation: DelegationContext | None,
        idempotency_key: str | None,
    ) -> TaskResponse:
        if route_kind == "remote_ref":
            routed = await self._context.router.create_task(
                agent_name=agent_name,
                route_kind="remote_ref",
                input_content=input_content,
                attachments=[item.model_dump(mode="json") for item in attachments],
                metadata=metadata,
                webhook=webhook,
                delegation_context=delegation,
                task_id=task_id,
                idempotency_key=idempotency_key,
            )
            return self._projection.build_task(routed.record, routed.route.metadata)

        prepared = await self._attachments.prepare(
            input_content,
            attachments,
            batch_id=task_id,
        )
        route_metadata = self._attachments.metadata_with_attachments(
            metadata,
            prepared.attachment_metadata,
        )
        routed = await self._context.router.create_task(
            agent_name=agent_name,
            route_kind="local",
            input_content=prepared.content,
            metadata=route_metadata,
            webhook=webhook,
            delegation_context=delegation,
            task_id=task_id,
            idempotency_key=idempotency_key,
        )
        return self._projection.build_task(routed.record, routed.route.metadata)

    async def get_task(self, task_id: str) -> TaskResponse:
        route = await self._context.router.get_route(task_id)
        record = await self._context.router.get_record(route)
        return self._projection.build_task(record, route.metadata)

    async def list_task_messages(
        self,
        task_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> TaskMessageListResponse:
        if limit <= 0 or limit > 100:
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )
        route = await self._context.router.get_route(task_id)
        page = await self._context.router.list_task_messages(
            route,
            cursor=cursor,
            limit=limit,
        )
        return TaskMessageListResponse(
            task_id=page.task_id,
            items=[
                TaskMessageResponse(
                    sequence=item.sequence,
                    message_id=item.message_id,
                    role=item.role,
                    content=item.content,
                    name=item.name,
                    tool_call_id=item.tool_call_id,
                    tool_calls=[
                        TaskMessageToolCallResponse(
                            tool_call_id=call.tool_call_id,
                            name=call.name,
                            arguments=dict(call.arguments),
                        )
                        for call in item.tool_calls
                    ],
                    status=item.status,
                )
                for item in page.items
            ],
            next_cursor=page.next_cursor,
        )

    @asynccontextmanager
    async def open_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AsyncIterator[AsyncIterator[TaskStreamEvent]]:
        if (
            not isinstance(run_count, int)
            or isinstance(run_count, bool)
            or run_count < 0
        ):
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'run_count' must be a non-negative integer",
            )
        route = await self._context.router.get_route(task_id)
        async with self._context.router.open_task_event_stream(
            route,
            run_count=run_count,
            last_event_id=last_event_id,
        ) as events:
            yield events

    async def send_effect(
        self,
        *,
        route: TaskRouteRecord,
        input_content: str,
        attachments: list[AttachmentInput],
        batch_id: str,
        downstream_idempotency_key: str | None,
        mailbox_message_id: str | None,
    ) -> TaskResponse:
        if route.route_kind == "remote_ref":
            record = await self._context.router.send_input(
                route,
                input_content,
                attachments=[item.model_dump(mode="json") for item in attachments],
                idempotency_key=downstream_idempotency_key,
            )
            return self._projection.build_task(record, route.metadata)

        prepared = await self._attachments.prepare(
            input_content,
            attachments,
            batch_id=batch_id,
        )
        route_metadata = self._attachments.metadata_with_attachments(
            route.metadata,
            prepared.attachment_metadata,
        )
        record = await self._context.router.send_input(
            route,
            prepared.content,
            idempotency_key=downstream_idempotency_key,
            mailbox_message_id=mailbox_message_id,
        )
        if prepared.attachment_metadata:
            route.metadata = route_metadata
            await self._context.router.save_route(route)
        return self._projection.build_task(record, route_metadata)

    async def cancel_task(self, task_id: str) -> TaskResponse:
        route = await self._context.router.get_route(task_id)
        record = await self._context.router.cancel(route)
        return self._projection.build_task(record, route.metadata)

    async def handle_task_webhook(self, event: TaskWebhookEvent) -> dict[str, Any]:
        delivered = await self._context.router.handle_remote_event(
            event.model_dump(mode="json")
        )
        return {"delivered": delivered}
