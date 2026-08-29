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
                legacy_v2_boundary=page.legacy_v2_boundary,
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
                snapshot_frontier=_review_snapshot_frontier(records),
            )
        try:
            payload = _decode_review_cursor_payload(cursor)
            if type(payload) is int:
                if payload < 0 or payload > 2**63 - 1:
                    raise ValueError
                return _ReviewCursorPage(
                    records=records,
                    scan_index=payload,
                    snapshot_frontier=_review_snapshot_frontier(records),
                )
            if not isinstance(payload, dict):
                raise ValueError
            if set(payload) == {
                "fallback_ingest_sequence",
                "resume_review_id",
                "snapshot_ingest_sequence",
                "version",
            }:
                if type(payload["version"]) is not int or payload["version"] != 4:
                    raise ValueError
                review_id = _parse_cursor_review_id(payload["resume_review_id"])
                fallback_sequence = _parse_cursor_sequence(
                    payload["fallback_ingest_sequence"]
                )
                snapshot_frontier = _parse_cursor_sequence(
                    payload["snapshot_ingest_sequence"]
                )
                records = [
                    record
                    for record in records
                    if record.ingest_sequence <= snapshot_frontier
                ]
                return _resume_sequence_page(
                    records,
                    review_id=review_id,
                    fallback_sequence=fallback_sequence,
                    snapshot_frontier=snapshot_frontier,
                )
            if set(payload) == {
                "fallback_cursor_order_updated_at",
                "legacy_fallback_updated_at",
                "legacy_resume_included",
                "legacy_resume_review_id",
                "resume_review_id",
                "snapshot_ingest_sequence",
                "version",
            }:
                if type(payload["version"]) is not int or payload["version"] != 5:
                    raise ValueError
                if type(payload["legacy_resume_included"]) is not bool:
                    raise ValueError
                boundary = _LegacyV2Boundary(
                    fallback_updated_at=_parse_cursor_timestamp(
                        payload["legacy_fallback_updated_at"]
                    ),
                    resume_review_id=_parse_cursor_review_id(
                        payload["legacy_resume_review_id"]
                    ),
                    include_resume=payload["legacy_resume_included"],
                )
                review_id = _parse_cursor_review_id(payload["resume_review_id"])
                fallback_cursor_order_updated_at = _parse_cursor_timestamp(
                    payload["fallback_cursor_order_updated_at"]
                )
                snapshot_frontier = _parse_cursor_sequence(
                    payload["snapshot_ingest_sequence"]
                )
                records = _legacy_v2_snapshot_records(
                    records,
                    boundary=boundary,
                    snapshot_frontier=snapshot_frontier,
                )
                page = _resume_legacy_v2_page(
                    records,
                    review_id=review_id,
                    fallback_cursor_order_updated_at=fallback_cursor_order_updated_at,
                    snapshot_frontier=snapshot_frontier,
                )
                return _ReviewCursorPage(
                    records=page.records,
                    scan_index=page.scan_index,
                    snapshot_frontier=page.snapshot_frontier,
                    legacy_v2_boundary=boundary,
                )
            if set(payload) == {"review_id", "updated_at", "version"}:
                if type(payload["version"]) is not int or payload["version"] != 1:
                    raise ValueError
                review_id = _parse_cursor_review_id(payload["review_id"])
                raw_fallback_at = payload["updated_at"]
                legacy_timestamp_field = "updated_at"
            elif set(payload) == {
                "fallback_updated_at",
                "resume_review_id",
                "version",
            }:
                if type(payload["version"]) is not int or payload["version"] != 2:
                    raise ValueError
                review_id = _parse_cursor_review_id(payload["resume_review_id"])
                raw_fallback_at = payload["fallback_updated_at"]
                fallback_at = _parse_cursor_timestamp(raw_fallback_at)
                snapshot_frontier = _review_snapshot_frontier(records)
                boundary = _LegacyV2Boundary(
                    fallback_updated_at=fallback_at,
                    resume_review_id=review_id,
                    include_resume=any(
                        record.review_id == review_id for record in records
                    ),
                )
                if snapshot_frontier is None:
                    return _ReviewCursorPage(records=[], scan_index=0)
                records = _legacy_v2_snapshot_records(
                    records,
                    boundary=boundary,
                    snapshot_frontier=snapshot_frontier,
                )
                return _ReviewCursorPage(
                    records=records,
                    scan_index=0,
                    snapshot_frontier=snapshot_frontier,
                    legacy_v2_boundary=boundary,
                )
            elif set(payload) == {
                "fallback_created_at",
                "resume_review_id",
                "snapshot_created_at",
                "snapshot_review_id",
                "version",
            }:
                if type(payload["version"]) is not int or payload["version"] != 3:
                    raise ValueError
                review_id = _parse_cursor_review_id(payload["resume_review_id"])
                raw_fallback_at = payload["fallback_created_at"]
                snapshot_review_id = _parse_cursor_review_id(
                    payload["snapshot_review_id"]
                )
                legacy_snapshot_frontier = (
                    _parse_cursor_timestamp(payload["snapshot_created_at"]),
                    snapshot_review_id,
                )
                records = [
                    record
                    for record in records
                    if (record.created_at, record.review_id) <= legacy_snapshot_frontier
                ]
                legacy_timestamp_field = "created_at"
            else:
                raise ValueError
            fallback_at = _parse_cursor_timestamp(raw_fallback_at)
            for index, record in enumerate(records):
                if record.review_id == review_id:
                    return _ReviewCursorPage(
                        records=records,
                        scan_index=index,
                        snapshot_frontier=_review_snapshot_frontier(records),
                    )
            fallback_key = (fallback_at, review_id)
            records = [
                record
                for record in records
                if (
                    getattr(record, legacy_timestamp_field),
                    record.review_id,
                )
                < fallback_key
            ]
            return _ReviewCursorPage(
                records=records,
                scan_index=0,
                snapshot_frontier=_review_snapshot_frontier(records),
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
        snapshot_frontier: int | None,
        legacy_v2_boundary: _LegacyV2Boundary | None = None,
    ) -> str:
        if snapshot_frontier is None:
            raise ValueError("Review cursor snapshot frontier is missing")
        _parse_cursor_sequence(record.ingest_sequence)
        _parse_cursor_sequence(snapshot_frontier)
        cursor_payload: dict[str, object] = {
            "fallback_ingest_sequence": record.ingest_sequence,
            "resume_review_id": record.review_id,
            "snapshot_ingest_sequence": snapshot_frontier,
            "version": 4,
        }
        if legacy_v2_boundary is not None:
            cursor_payload = {
                "fallback_cursor_order_updated_at": (
                    _legacy_cursor_order_updated_at(record).isoformat()
                ),
                "legacy_fallback_updated_at": (
                    legacy_v2_boundary.fallback_updated_at.isoformat()
                ),
                "legacy_resume_included": legacy_v2_boundary.include_resume,
                "legacy_resume_review_id": legacy_v2_boundary.resume_review_id,
                "resume_review_id": record.review_id,
                "snapshot_ingest_sequence": snapshot_frontier,
                "version": 5,
            }
        payload = json.dumps(
            cursor_payload,
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
    snapshot_frontier: int | None = None
    legacy_v2_boundary: _LegacyV2Boundary | None = None


@dataclass(frozen=True, slots=True)
class _LegacyV2Boundary:
    fallback_updated_at: datetime
    resume_review_id: str
    include_resume: bool


def _review_sort_key(record: PendingReviewRecord) -> tuple[int, str]:
    return record.ingest_sequence, record.review_id


def _review_snapshot_frontier(records: list[PendingReviewRecord]) -> int | None:
    return records[0].ingest_sequence if records else None


def _legacy_v2_snapshot_records(
    records: list[PendingReviewRecord],
    *,
    boundary: _LegacyV2Boundary,
    snapshot_frontier: int,
) -> list[PendingReviewRecord]:
    fallback_key = (boundary.fallback_updated_at, boundary.resume_review_id)
    return sorted(
        (
            record
            for record in records
            if record.ingest_sequence <= snapshot_frontier
            and (
                (
                    boundary.include_resume
                    and record.review_id == boundary.resume_review_id
                )
                or (_legacy_cursor_order_updated_at(record), record.review_id)
                < fallback_key
            )
        ),
        key=_legacy_v2_sort_key,
        reverse=True,
    )


def _legacy_cursor_order_updated_at(record: PendingReviewRecord) -> datetime:
    immutable_order = getattr(record, "cursor_order_updated_at", None)
    return immutable_order if immutable_order is not None else record.updated_at


def _legacy_v2_sort_key(record: PendingReviewRecord) -> tuple[datetime, str]:
    return _legacy_cursor_order_updated_at(record), record.review_id


def _resume_legacy_v2_page(
    records: list[PendingReviewRecord],
    *,
    review_id: str,
    fallback_cursor_order_updated_at: datetime,
    snapshot_frontier: int,
) -> _ReviewCursorPage:
    for index, record in enumerate(records):
        if record.review_id == review_id:
            return _ReviewCursorPage(records, index, snapshot_frontier)
    fallback_key = (fallback_cursor_order_updated_at, review_id)
    scan_index = next(
        (
            index
            for index, record in enumerate(records)
            if _legacy_v2_sort_key(record) < fallback_key
        ),
        len(records),
    )
    return _ReviewCursorPage(records, scan_index, snapshot_frontier)


def _resume_sequence_page(
    records: list[PendingReviewRecord],
    *,
    review_id: str,
    fallback_sequence: int,
    snapshot_frontier: int,
) -> _ReviewCursorPage:
    for index, record in enumerate(records):
        if record.review_id == review_id:
            return _ReviewCursorPage(records, index, snapshot_frontier)
    scan_index = next(
        (
            index
            for index, record in enumerate(records)
            if record.ingest_sequence < fallback_sequence
        ),
        len(records),
    )
    return _ReviewCursorPage(records, scan_index, snapshot_frontier)


def _parse_cursor_review_id(raw: object) -> str:
    if not isinstance(raw, str) or not 1 <= len(raw) <= 512:
        raise ValueError
    return raw


def _parse_cursor_sequence(raw: object) -> int:
    if type(raw) is not int or not 1 <= raw <= 2**63 - 1:
        raise ValueError
    return raw


def _parse_cursor_timestamp(raw: object) -> datetime:
    if not isinstance(raw, str) or not 1 <= len(raw) <= 128:
        raise ValueError
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError
    return parsed
