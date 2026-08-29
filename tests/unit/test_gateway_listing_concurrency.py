from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.task_models import PendingReviewRecord, TaskRecord, TaskRouteRecord


class ListingRouter:
    def __init__(
        self,
        routes: list[TaskRouteRecord],
        records: dict[str, TaskRecord],
        *,
        barrier_size: int = 0,
        failed_task_ids: set[str] | None = None,
        pending_reviews: list[PendingReviewRecord] | None = None,
        missing_task_ids: set[str] | None = None,
    ) -> None:
        self.routes = routes
        self.records = records
        self.barrier_size = barrier_size
        self.failed_task_ids = failed_task_ids or set()
        self.pending_reviews = pending_reviews
        self.missing_task_ids = missing_task_ids or set()
        self.release = asyncio.Event()
        self.called: list[str] = []
        self.active = 0
        self.peak_active = 0
        self.entered = 0
        self.list_routes_calls = 0
        self.get_route_calls: list[str] = []

    async def list_routes(self) -> list[TaskRouteRecord]:
        self.list_routes_calls += 1
        return list(self.routes)

    async def get_route(self, task_id: str) -> TaskRouteRecord:
        self.get_route_calls.append(task_id)
        if task_id in self.missing_task_ids:
            raise GatewayTaskError(
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            )
        return next(route for route in self.routes if route.task_id == task_id)

    async def get_record(self, route: TaskRouteRecord) -> TaskRecord:
        self.called.append(route.task_id)
        if route.task_id in self.failed_task_ids:
            raise GatewayTaskError(
                code="remote_gateway_error",
                message="remote unavailable",
            )
        if route.route_kind == "remote_ref" and self.barrier_size:
            self.active += 1
            self.entered += 1
            self.peak_active = max(self.peak_active, self.active)
            try:
                await self.release.wait()
            finally:
                self.active -= 1
        return self.records[route.task_id]

    def ensure_record(self, route: TaskRouteRecord) -> TaskRecord:
        return self.records[route.task_id]

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        if self.pending_reviews is not None:
            return [
                review
                for review in self.pending_reviews
                if (root_task_id is None or review.root_task_id == root_task_id)
                and (task_id is None or review.task_id == task_id)
            ]
        reviews: list[PendingReviewRecord] = []
        for record in self.records.values():
            payload = record.pending_review
            if payload is None or "source_task_id" in payload:
                continue
            review_id = payload.get("review_id")
            if not isinstance(review_id, str):
                continue
            if root_task_id is not None and record.root_task_id != root_task_id:
                continue
            if task_id is not None and record.task_id != task_id:
                continue
            reviews.append(
                PendingReviewRecord(
                    review_id=review_id,
                    task_id=record.task_id,
                    root_task_id=record.root_task_id,
                    payload=dict(payload),
                    created_at=record.updated_at,
                    updated_at=record.updated_at,
                )
            )
        return reviews

    def get_pending_review(self, review_id: str) -> PendingReviewRecord | None:
        return next(
            (
                review
                for review in self.list_pending_reviews()
                if review.review_id == review_id
            ),
            None,
        )


def route(
    index: int,
    *,
    agent_name: str = "remote",
    route_kind: str = "remote_ref",
    metadata: dict[str, str] | None = None,
) -> TaskRouteRecord:
    return TaskRouteRecord(
        task_id=f"task-{index}",
        agent_name=agent_name,
        metadata=dict(metadata or {}),
        route_kind=route_kind,  # type: ignore[arg-type]
        upstream_task_id=f"upstream-{index}",
    )


def record(
    index: int,
    *,
    agent_name: str = "remote",
    state: str = "completed",
    root_task_id: str | None = None,
    pending_review: dict[str, Any] | None = None,
) -> TaskRecord:
    now = datetime(2026, 8, 29, tzinfo=UTC) + timedelta(seconds=index)
    return TaskRecord(
        task_id=f"task-{index}",
        agent_name=agent_name,
        state=state,  # type: ignore[arg-type]
        thread_id=f"thread-{index}",
        parent_task_id=None,
        root_task_id=root_task_id or f"task-{index}",
        depth=1,
        created_at=now,
        updated_at=now,
        run_count=1,
        route_kind="remote_ref",
        upstream_task_id=f"upstream-{index}",
        pending_review=pending_review,
    )


def service_with_router(
    router: ListingRouter,
    *,
    concurrency: int = 2,
) -> GatewayTaskModule:
    service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs={},
        control=object(),  # type: ignore[arg-type]
        remote_listing_concurrency=concurrency,
    )
    service._router = router  # type: ignore[assignment]
    return service


@pytest.mark.parametrize("operation", ["tasks", "reviews", "review"])
def test_remote_listing_operations_overlap_with_a_bounded_concurrency(
    operation: str,
) -> None:
    async def scenario() -> None:
        routes = [route(index) for index in range(5)]
        records = {
            item.task_id: record(
                index,
                state="waiting_for_human",
                pending_review={
                    "review_id": f"review-{index}",
                    "action_requests": [],
                    "review_configs": [],
                },
            )
            for index, item in enumerate(routes)
        }
        router = ListingRouter(routes, records, barrier_size=2)
        service = service_with_router(router, concurrency=2)
        if operation == "tasks":
            call = service.list_tasks(
                agent_name=None,
                status=None,
                metadata_filters={},
                cursor=None,
                limit=10,
            )
        elif operation == "reviews":
            call = service.list_reviews(cursor=None, limit=10)
        else:
            call = service.get_review("review-4")

        pending = asyncio.create_task(call)
        expected_entered = 1 if operation == "review" else 2
        for _ in range(100):
            if router.entered == expected_entered:
                break
            await asyncio.sleep(0)
        assert router.entered == expected_entered
        assert router.peak_active == expected_entered
        router.release.set()
        await pending
        assert router.peak_active == expected_entered
        if operation == "review":
            assert router.called == ["task-4"]
        if operation != "tasks":
            assert router.list_routes_calls == 0

    asyncio.run(scenario())


def test_list_tasks_prefilters_durable_route_fields_before_remote_refresh() -> None:
    routes = [
        route(1, agent_name="main", metadata={"channel": "telegram"}),
        route(2, agent_name="other", metadata={"channel": "telegram"}),
        route(3, agent_name="main", metadata={"channel": "feishu"}),
    ]
    records = {
        "task-1": record(1, agent_name="main", root_task_id="root-1"),
        "task-2": record(2, agent_name="other", root_task_id="root-1"),
        "task-3": record(3, agent_name="main", root_task_id="root-1"),
    }
    router = ListingRouter(routes, records)
    service = service_with_router(router)

    response = asyncio.run(
        service.list_tasks(
            agent_name="main",
            status="completed",
            metadata_filters={"channel": "telegram"},
            cursor=None,
            limit=10,
            root_task_id="root-1",
        )
    )

    assert [item.task_id for item in response.items] == ["task-1"]
    assert router.called == ["task-1"]


def test_remote_listing_failure_omits_only_the_failed_route() -> None:
    routes = [route(1), route(2), route(3)]
    records = {item.task_id: record(index) for index, item in enumerate(routes, 1)}
    router = ListingRouter(routes, records, failed_task_ids={"task-2"})
    service = service_with_router(router)

    response = asyncio.run(
        service.list_tasks(
            agent_name=None,
            status=None,
            metadata_filters={},
            cursor=None,
            limit=10,
        )
    )

    assert [item.task_id for item in response.items] == ["task-3", "task-1"]
    assert set(router.called) == {"task-1", "task-2", "task-3"}


def test_remote_listing_concurrency_must_be_positive() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        GatewayTaskModule(
            main_agent_name="main",
            agent_configs={},
            control=object(),  # type: ignore[arg-type]
            remote_listing_concurrency=0,
        )


def test_review_queries_refresh_only_pending_review_owners_among_many_routes() -> None:
    routes = [route(index) for index in range(200)]
    records = {item.task_id: record(index) for index, item in enumerate(routes)}
    for index in (3, 177):
        records[f"task-{index}"] = record(
            index,
            state="waiting_for_human",
            pending_review={
                "review_id": f"review-{index}",
                "action_requests": [],
                "review_configs": [],
            },
        )
    router = ListingRouter(routes, records)
    service = service_with_router(router)

    response = asyncio.run(service.list_reviews(cursor=None, limit=10))

    assert [item.review_id for item in response.items] == [
        "review-177",
        "review-3",
    ]
    assert set(router.called) == {"task-3", "task-177"}
    assert set(router.get_route_calls) == {"task-3", "task-177"}
    assert router.list_routes_calls == 0


def test_exact_review_refreshes_one_owner_not_task_universe() -> None:
    routes = [route(index) for index in range(200)]
    records = {item.task_id: record(index) for index, item in enumerate(routes)}
    records["task-91"] = record(
        91,
        state="waiting_for_human",
        pending_review={
            "review_id": "review-91",
            "action_requests": [],
            "review_configs": [],
        },
    )
    router = ListingRouter(routes, records)
    service = service_with_router(router)

    response = asyncio.run(service.get_review("review-91"))

    assert response.task_id == "task-91"
    assert router.called == ["task-91"]
    assert router.get_route_calls == ["task-91"]
    assert router.list_routes_calls == 0


def test_review_pagination_refreshes_only_current_page_owners() -> None:
    routes = [route(index) for index in range(100)]
    records = {item.task_id: record(index) for index, item in enumerate(routes)}
    for index in range(20):
        records[f"task-{index}"] = record(
            index,
            state="waiting_for_human",
            pending_review={
                "review_id": f"review-{index}",
                "action_requests": [],
                "review_configs": [],
            },
        )
    router = ListingRouter(routes, records)
    service = service_with_router(router)

    response = asyncio.run(service.list_reviews(cursor=None, limit=3))

    assert [item.review_id for item in response.items] == [
        "review-19",
        "review-18",
        "review-17",
    ]
    assert router.called == ["task-19", "task-18", "task-17"]
    assert response.next_cursor is not None
    assert router.list_routes_calls == 0


def test_review_owner_refresh_deduplicates_repeated_owner_records() -> None:
    now = datetime(2026, 8, 29, tzinfo=UTC)
    pending_reviews = [
        PendingReviewRecord(
            review_id=f"review-{index}",
            task_id="task-1",
            root_task_id="task-1",
            payload={"action_requests": [], "review_configs": []},
            created_at=now + timedelta(seconds=index),
            updated_at=now + timedelta(seconds=index),
        )
        for index in range(2)
    ]
    routes = [route(1)]
    records = {"task-1": record(1, state="waiting_for_human")}
    router = ListingRouter(routes, records, pending_reviews=pending_reviews)
    service = service_with_router(router)

    response = asyncio.run(service.list_reviews(cursor=None, limit=10))

    assert {item.review_id for item in response.items} == {"review-0", "review-1"}
    assert router.called == ["task-1"]
    assert router.get_route_calls == ["task-1"]


def test_list_task_reviews_refreshes_only_requested_task() -> None:
    routes = [route(index) for index in range(100)]
    records = {item.task_id: record(index) for index, item in enumerate(routes)}
    records["task-0"] = record(0, root_task_id="task-0")
    for index in (7, 88):
        records[f"task-{index}"] = record(
            index,
            state="waiting_for_human",
            root_task_id="task-0",
            pending_review={
                "review_id": f"review-{index}",
                "action_requests": [],
                "review_configs": [],
            },
        )
    router = ListingRouter(routes, records)
    service = service_with_router(router)

    response = asyncio.run(service.list_task_reviews("task-0"))

    assert {item.task_id for item in response.items} == {"task-7", "task-88"}
    assert router.called == ["task-0"]
    assert set(router.get_route_calls) == {"task-0", "task-7", "task-88"}
    assert router.list_routes_calls == 0


@pytest.mark.parametrize("failure", ["missing_route", "remote_error"])
def test_unavailable_review_owner_has_stable_not_found_semantics(failure: str) -> None:
    routes = [route(1)]
    records = {
        "task-1": record(
            1,
            state="waiting_for_human",
            pending_review={
                "review_id": "review-1",
                "action_requests": [],
                "review_configs": [],
            },
        )
    }
    router = ListingRouter(
        routes,
        records,
        missing_task_ids={"task-1"} if failure == "missing_route" else None,
        failed_task_ids={"task-1"} if failure == "remote_error" else None,
    )
    service = service_with_router(router)

    with pytest.raises(GatewayTaskError) as caught:
        asyncio.run(service.get_review("review-1"))

    assert caught.value.code == "review_not_found"
    listed = asyncio.run(service.list_reviews(cursor=None, limit=10))
    assert listed.items == []
    assert router.list_routes_calls == 0
