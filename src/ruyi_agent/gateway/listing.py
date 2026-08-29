"""Gateway Task listing and bounded remote refresh service."""

from __future__ import annotations

import asyncio
import base64
import binascii
from collections.abc import Awaitable

from ruyi_agent.gateway.application import (
    GatewayApplicationContext,
    GatewayProjection,
)
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import TaskListResponse, TaskResponse
from ruyi_agent.task_models import MetadataScalar, TaskRouteRecord


class GatewayListingService:
    """Refresh, filter, sort, and page Gateway Tasks."""

    def __init__(
        self,
        context: GatewayApplicationContext,
        projection: GatewayProjection,
    ) -> None:
        self._context = context
        self._projection = projection

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
        if limit <= 0 or limit > 100:
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )
        offset = self.decode_cursor(cursor)
        routes = [
            route
            for route in await self._context.router.list_routes()
            if (agent_name is None or route.agent_name == agent_name)
            and self._metadata_matches(route.metadata, metadata_filters)
        ]
        items = []
        for _route, item in await self.collect_tasks(routes):
            if item is None:
                continue
            if status is not None and item.status != status:
                continue
            if root_task_id is not None and item.root_task_id != root_task_id:
                continue
            items.append(item)
        items.sort(key=lambda item: (item.updated_at, item.task_id), reverse=True)
        page = items[offset : offset + limit]
        next_cursor = (
            self.encode_cursor(offset + limit)
            if offset + limit < len(items)
            else None
        )
        return TaskListResponse(items=page, next_cursor=next_cursor)

    async def get_task_for_listing(
        self,
        route: TaskRouteRecord,
    ) -> TaskResponse | None:
        try:
            record = await self._context.router.get_record(route)
        except GatewayTaskError:
            return None
        return self._projection.build_task(record, route.metadata)

    async def collect_tasks(
        self,
        routes: list[TaskRouteRecord],
    ) -> list[tuple[TaskRouteRecord, TaskResponse | None]]:
        results: list[tuple[TaskRouteRecord, TaskResponse | None] | None] = [
            None
        ] * len(routes)
        semaphore = asyncio.Semaphore(self._context.remote_listing_concurrency)

        async def collect_remote(index: int, route: TaskRouteRecord) -> None:
            async with semaphore:
                results[index] = (route, await self.get_task_for_listing(route))

        remote_calls: list[Awaitable[None]] = []
        for index, route in enumerate(routes):
            if route.route_kind == "remote_ref":
                remote_calls.append(collect_remote(index, route))
            else:
                results[index] = (route, await self.get_task_for_listing(route))
        if remote_calls:
            await asyncio.gather(*remote_calls)
        return [item for item in results if item is not None]

    def encode_cursor(self, offset: int) -> str:
        return base64.urlsafe_b64encode(str(offset).encode()).decode("ascii")

    def decode_cursor(self, cursor: str | None) -> int:
        if cursor is None:
            return 0
        try:
            offset = int(base64.urlsafe_b64decode(cursor.encode()).decode())
        except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
            raise self._invalid_cursor() from exc
        if offset < 0:
            raise self._invalid_cursor()
        return offset

    def _metadata_matches(
        self,
        metadata: dict[str, MetadataScalar],
        filters: dict[str, str],
    ) -> bool:
        return all(
            self._stringify(metadata.get(key)) == expected
            for key, expected in filters.items()
        )

    def _stringify(self, value: MetadataScalar) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _invalid_cursor(self) -> GatewayTaskError:
        return GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'cursor' is invalid",
        )
