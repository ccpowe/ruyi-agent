"""Gateway Pending Review application service."""

from __future__ import annotations

from typing import Any

from ruyi_agent.gateway.application import (
    GatewayApplicationContext,
    GatewayProjection,
)
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.listing import GatewayListingService
from ruyi_agent.gateway.models import ReviewListResponse, ReviewResponse, TaskResponse


class GatewayReviewService:
    """Own review lookup, projection, and decision orchestration."""

    def __init__(
        self,
        context: GatewayApplicationContext,
        projection: GatewayProjection,
        listings: GatewayListingService,
    ) -> None:
        self._context = context
        self._projection = projection
        self._listings = listings

    async def submit_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskResponse:
        route = await self._context.router.get_route(task_id)
        review = self._context.router.get_pending_review(review_id)
        review_route = route
        if review is not None and review.task_id != task_id:
            review_route = await self._context.router.get_route(review.task_id)
        if review_route.route_kind == "remote_ref":
            await self._context.router.get_record(review_route)
            review = self._context.router.get_pending_review(review_id)
        if review is None or task_id not in {review.task_id, review.root_task_id}:
            raise GatewayTaskError(
                code="review_not_found",
                message=f"Review '{review_id}' does not belong to task '{task_id}'",
            )
        source_task_id = review.task_id
        record = await self._context.router.submit_review(
            task_id=task_id,
            review_id=review_id,
            decisions=decisions,
        )
        if record.task_id == task_id:
            return self._projection.build_task(record, route.metadata)
        if source_task_id == record.task_id:
            root = self._context.router.ensure_record(route)
            return self._projection.build_task(root, route.metadata)
        raise GatewayTaskError(
            code="review_task_mismatch",
            message=f"Review '{review_id}' does not belong to task '{task_id}'",
        )

    async def list_reviews(
        self,
        *,
        cursor: str | None,
        limit: int,
    ) -> ReviewListResponse:
        if limit <= 0 or limit > 100:
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )
        offset = self._listings.decode_cursor(cursor)
        routes = await self._context.router.list_routes()
        routes_by_id = {
            route.task_id: route
            for route, task in await self._listings.collect_tasks(routes)
            if task is not None
        }
        items = []
        for pending in self._context.router.list_pending_reviews():
            route = routes_by_id.get(pending.task_id)
            if route is None:
                continue
            record = self._context.router.ensure_record(route)
            items.append(
                self._projection.build_review(pending, record, route.metadata)
            )
        items.sort(key=lambda item: (item.updated_at, item.review_id), reverse=True)
        page = items[offset : offset + limit]
        next_cursor = (
            self._listings.encode_cursor(offset + limit)
            if offset + limit < len(items)
            else None
        )
        return ReviewListResponse(items=page, next_cursor=next_cursor)

    async def get_review(self, review_id: str) -> ReviewResponse:
        routes = await self._context.router.list_routes()
        routes_by_id = {
            route.task_id: route
            for route, task in await self._listings.collect_tasks(routes)
            if task is not None
        }
        pending = self._context.router.get_pending_review(review_id)
        if pending is not None and (route := routes_by_id.get(pending.task_id)):
            record = self._context.router.ensure_record(route)
            return self._projection.build_review(pending, record, route.metadata)
        raise GatewayTaskError(
            code="review_not_found",
            message=f"Review '{review_id}' does not exist",
        )

    async def list_task_reviews(self, task_id: str) -> ReviewListResponse:
        route = await self._context.router.get_route(task_id)
        task = await self._listings.get_task_for_listing(route)
        if task is None:
            raise GatewayTaskError(
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            )
        record = self._context.router.ensure_record(route)
        routes = await self._context.router.list_routes()
        routes_by_id = {
            candidate.task_id: candidate
            for candidate, item in await self._listings.collect_tasks(routes)
            if item is not None
        }
        pending_reviews = self._context.router.list_pending_reviews(
            root_task_id=task_id if record.root_task_id == task_id else None,
            task_id=None if record.root_task_id == task_id else task_id,
        )
        items = []
        for pending in pending_reviews:
            owner_route = routes_by_id.get(pending.task_id)
            if owner_route is None:
                continue
            owner = self._context.router.ensure_record(owner_route)
            items.append(
                self._projection.build_review(
                    pending,
                    owner,
                    owner_route.metadata,
                )
            )
        return ReviewListResponse(items=items, next_cursor=None)
