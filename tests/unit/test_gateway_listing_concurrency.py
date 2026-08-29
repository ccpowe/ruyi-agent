from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.task_models import TaskRecord, TaskRouteRecord


class ListingRouter:
    def __init__(
        self,
        routes: list[TaskRouteRecord],
        records: dict[str, TaskRecord],
        *,
        barrier_size: int = 0,
        failed_task_ids: set[str] | None = None,
    ) -> None:
        self.routes = routes
        self.records = records
        self.barrier_size = barrier_size
        self.failed_task_ids = failed_task_ids or set()
        self.release = asyncio.Event()
        self.called: list[str] = []
        self.active = 0
        self.peak_active = 0
        self.entered = 0

    async def list_routes(self) -> list[TaskRouteRecord]:
        return list(self.routes)

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
        for _ in range(100):
            if router.entered == 2:
                break
            await asyncio.sleep(0)
        assert router.entered == 2
        assert router.peak_active == 2
        router.release.set()
        await pending
        assert router.peak_active == 2

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
