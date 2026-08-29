"""Gateway Pending Review application service."""

from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ruyi_agent.gateway.application import (
    GatewayApplicationContext,
    GatewayProjection,
)
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.listing import GatewayListingService
from ruyi_agent.gateway.models import ReviewListResponse, ReviewResponse, TaskResponse
from ruyi_agent.task_models import PendingReviewRecord, TaskRouteRecord


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
        await self._context.router.require_active_route(route)
        review = self._context.router.get_pending_review(review_id)
        review_route = route
        if review is not None and review.task_id != task_id:
            review_route = await self._context.router.get_route(review.task_id)
            await self._context.router.require_active_route(review_route)
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
        pending_reviews = sorted(
            self._context.router.list_pending_reviews(),
            key=_review_sort_key,
            reverse=True,
        )
        page = self._review_cursor_page(cursor, pending_reviews)
        pending_reviews = page.records
        scan_index = page.scan_index
        items = []
        blocked = False
        while len(items) < limit and scan_index < len(pending_reviews):
            batch = pending_reviews[scan_index : scan_index + (limit - len(items))]
            refreshed = await self._refresh_review_owner_routes(batch)
            for original in batch:
                if original.task_id in refreshed.unavailable_owner_ids:
                    blocked = True
                    break
                scan_index += 1
                pending = self._context.router.get_pending_review(original.review_id)
                if pending is None or pending.task_id != original.task_id:
                    continue
                route = refreshed.routes_by_id.get(pending.task_id)
                if route is None:
                    continue
                record = self._context.router.ensure_record(route)
                items.append(
                    self._projection.build_review(
                        pending,
                        record,
                        route.metadata,
                    )
                )
            if blocked:
                break
        next_cursor = (
            self._encode_review_cursor(
                pending_reviews[scan_index],
                snapshot_frontier=page.snapshot_frontier,
            )
            if scan_index < len(pending_reviews)
            else None
        )
        return ReviewListResponse(items=items, next_cursor=next_cursor)

    async def get_review(self, review_id: str) -> ReviewResponse:
        pending = self._context.router.get_pending_review(review_id)
        if pending is not None:
            refreshed_routes = await self._refresh_review_owner_routes([pending])
            refreshed = self._context.router.get_pending_review(review_id)
            route = refreshed_routes.routes_by_id.get(pending.task_id)
            if (
                refreshed is not None
                and refreshed.task_id == pending.task_id
                and route is not None
            ):
                record = self._context.router.ensure_record(route)
                return self._projection.build_review(
                    refreshed,
                    record,
                    route.metadata,
                )
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
        pending_reviews = self._context.router.list_pending_reviews(
            root_task_id=task_id if record.root_task_id == task_id else None,
            task_id=None if record.root_task_id == task_id else task_id,
        )
        refreshed_routes = await self._refresh_review_owner_routes(
            pending_reviews,
            known_routes={task_id: route},
        )
        items = []
        for pending in pending_reviews:
            current = self._context.router.get_pending_review(pending.review_id)
            if current is None or current.task_id != pending.task_id:
                continue
            owner_route = refreshed_routes.routes_by_id.get(current.task_id)
            if owner_route is None:
                continue
            owner = self._context.router.ensure_record(owner_route)
            items.append(
                self._projection.build_review(
                    current,
                    owner,
                    owner_route.metadata,
                )
            )
        return ReviewListResponse(items=items, next_cursor=None)

    async def _refresh_review_owner_routes(
        self,
        pending_reviews: list[PendingReviewRecord],
        *,
        known_routes: dict[str, TaskRouteRecord] | None = None,
        refresh_known: bool = False,
    ) -> _OwnerRefreshResult:
        """Refresh exactly the deduplicated owners named by Pending Reviews."""

        routes_by_id: dict[str, TaskRouteRecord] = {}
        unavailable_owner_ids: set[str] = set()
        semaphore = asyncio.Semaphore(self._context.remote_listing_concurrency)

        async def refresh(task_id: str) -> None:
            try:
                known = task_id in (known_routes or {})
                route = (known_routes or {}).get(task_id)
                if route is None:
                    route = await self._context.router.get_route(task_id)
                if route.route_state != "active":
                    return
                if known and not refresh_known:
                    pass
                elif route.route_kind == "remote_ref" or refresh_known:
                    async with semaphore:
                        await self._context.router.get_record(route)
                else:
                    await self._context.router.get_record(route)
                routes_by_id[task_id] = route
            except GatewayTaskError as exc:
                if exc.code != "task_not_found":
                    unavailable_owner_ids.add(task_id)

        owner_ids = list(dict.fromkeys(item.task_id for item in pending_reviews))
        if owner_ids:
            await asyncio.gather(*(refresh(task_id) for task_id in owner_ids))
        return _OwnerRefreshResult(routes_by_id, unavailable_owner_ids)

    def _review_cursor_page(
        self,
        cursor: str | None,
        records: list[PendingReviewRecord],
    ) -> _ReviewCursorPage:
        if cursor is None:
            return _ReviewCursorPage(
                records=records,
                scan_index=0,
                snapshot_frontier=(
                    _review_sort_key(records[0]) if records else None
                ),
            )
        try:
            payload = _decode_review_cursor_payload(cursor)
            if type(payload) is int:
                if payload < 0 or payload > 2**63 - 1:
                    raise ValueError
                return _ReviewCursorPage(
                    records=records,
                    scan_index=payload,
                    snapshot_frontier=(
                        _review_sort_key(records[0]) if records else None
                    ),
                )
            if not isinstance(payload, dict):
                raise ValueError
            snapshot_frontier: tuple[datetime, str] | None = None
            if set(payload) == {"review_id", "updated_at", "version"}:
                if type(payload["version"]) is not int or payload["version"] != 1:
                    raise ValueError
                review_id = payload["review_id"]
                raw_fallback_at = payload["updated_at"]
            elif set(payload) == {
                "fallback_updated_at",
                "resume_review_id",
                "version",
            }:
                if type(payload["version"]) is not int or payload["version"] != 2:
                    raise ValueError
                review_id = payload["resume_review_id"]
                raw_fallback_at = payload["fallback_updated_at"]
            elif set(payload) == {
                "fallback_created_at",
                "resume_review_id",
                "snapshot_created_at",
                "snapshot_review_id",
                "version",
            }:
                if type(payload["version"]) is not int or payload["version"] != 3:
                    raise ValueError
                review_id = payload["resume_review_id"]
                raw_fallback_at = payload["fallback_created_at"]
                snapshot_review_id = payload["snapshot_review_id"]
                if (
                    not isinstance(snapshot_review_id, str)
                    or not 1 <= len(snapshot_review_id) <= 512
                ):
                    raise ValueError
                snapshot_frontier = (
                    _parse_cursor_timestamp(payload["snapshot_created_at"]),
                    snapshot_review_id,
                )
                records = [
                    record
                    for record in records
                    if _review_sort_key(record) <= snapshot_frontier
                ]
            else:
                raise ValueError
            if (
                not isinstance(review_id, str)
                or not 1 <= len(review_id) <= 512
            ):
                raise ValueError
            fallback_at = _parse_cursor_timestamp(raw_fallback_at)
            for index, record in enumerate(records):
                if record.review_id == review_id:
                    return _ReviewCursorPage(
                        records=records,
                        scan_index=index,
                        snapshot_frontier=(
                            snapshot_frontier
                            or (_review_sort_key(records[0]) if records else None)
                        ),
                    )
            fallback_key = (fallback_at, review_id)
            scan_index = next(
                (
                    index
                    for index, record in enumerate(records)
                    if _review_sort_key(record) < fallback_key
                ),
                len(records),
            )
            return _ReviewCursorPage(
                records=records,
                scan_index=scan_index,
                snapshot_frontier=(
                    snapshot_frontier
                    or (_review_sort_key(records[0]) if records else None)
                ),
            )
        except Exception as exc:
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'cursor' is invalid",
            ) from exc

    def _encode_review_cursor(
        self,
        record: PendingReviewRecord,
        *,
        snapshot_frontier: tuple[datetime, str] | None,
    ) -> str:
        if snapshot_frontier is None:
            raise ValueError("Review cursor snapshot frontier is missing")
        payload = json.dumps(
            {
                "fallback_created_at": record.created_at.isoformat(),
                "resume_review_id": record.review_id,
                "snapshot_created_at": snapshot_frontier[0].isoformat(),
                "snapshot_review_id": snapshot_frontier[1],
                "version": 3,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return base64.urlsafe_b64encode(payload).decode("ascii")


def _decode_review_cursor_payload(cursor: str) -> object:
    if not 1 <= len(cursor) <= 4096:
        raise ValueError
    encoded = cursor.encode("ascii")
    padding = b"=" * (-len(encoded) % 4)
    decoded = base64.b64decode(
        encoded + padding,
        altchars=b"-_",
        validate=True,
    )
    if len(decoded) > 2048:
        raise ValueError
    return json.loads(decoded.decode("utf-8"))


@dataclass(frozen=True, slots=True)
class _OwnerRefreshResult:
    routes_by_id: dict[str, TaskRouteRecord]
    unavailable_owner_ids: set[str]


@dataclass(frozen=True, slots=True)
class _ReviewCursorPage:
    records: list[PendingReviewRecord]
    scan_index: int
    snapshot_frontier: tuple[datetime, str] | None


def _review_sort_key(record: PendingReviewRecord) -> tuple[datetime, str]:
    return record.created_at, record.review_id


def _parse_cursor_timestamp(raw: object) -> datetime:
    if not isinstance(raw, str) or not 1 <= len(raw) <= 128:
        raise ValueError
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError
    return parsed
