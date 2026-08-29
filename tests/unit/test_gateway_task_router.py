from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest

from ruyi_agent.gateway.models import TaskRouteRecord
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.task_models import TaskRecord


def _record(
    task_id: str,
    *,
    agent_name: str = "main",
    route_kind: str = "local",
    upstream_task_id: str | None = None,
    parent_task_id: str | None = None,
    root_task_id: str | None = None,
    depth: int = 1,
) -> TaskRecord:
    now = datetime.now(UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name=agent_name,
        state="completed",
        thread_id=task_id,
        parent_task_id=parent_task_id,
        root_task_id=root_task_id or task_id,
        depth=depth,
        created_at=now,
        updated_at=now,
        route_kind=route_kind,
        upstream_task_id=upstream_task_id,
    )


class RecordingControl:
    def __init__(self) -> None:
        self.records: dict[str, TaskRecord] = {}
        self.ensure_calls: list[dict[str, Any]] = []
        self.refresh_calls: list[str] = []

    async def spawn_task(self, agent_name: str, task: str, **kwargs: Any) -> TaskRecord:
        del task
        is_remote = "metadata" in kwargs
        task_id = "remote-local-id" if is_remote else "local-id"
        record = _record(
            task_id,
            agent_name=agent_name,
            route_kind="remote_ref" if is_remote else "local",
            upstream_task_id="upstream-id" if is_remote else None,
        )
        self.records[task_id] = record
        return record

    def get_task_record(self, task_id: str) -> TaskRecord:
        return self.records[task_id]

    def list_persisted_task_records(self) -> list[TaskRecord]:
        return list(self.records.values())

    def ensure_remote_task_record(self, **kwargs: Any) -> TaskRecord:
        self.ensure_calls.append(kwargs)
        record = _record(
            kwargs["task_id"],
            agent_name=kwargs["agent_name"],
            route_kind="remote_ref",
            upstream_task_id=kwargs["upstream_task_id"],
        )
        self.records[record.task_id] = record
        return record

    async def refresh_task(self, task_id: str) -> TaskRecord:
        self.refresh_calls.append(task_id)
        return self.records[task_id]


def test_router_creates_and_persists_local_route() -> None:
    asyncio.run(_create_and_persist_local_route())


async def _create_and_persist_local_route() -> None:
    control = RecordingControl()
    store = GatewayRouteStore(":memory:")
    try:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        routed = await router.create_task(
            agent_name="main",
            route_kind="local",
            input_content="hello",
            metadata={"source": "test"},
            webhook=None,
            delegation_context=None,
        )

        assert routed.route == await router.get_route("local-id")
        assert routed.route.route_kind == "local"
        assert routed.route.upstream_task_id == "local-id"
        assert routed.route.metadata == {"source": "test"}
    finally:
        store.close()


def test_route_store_rejects_identity_rebinding_without_overwrite() -> None:
    store = GatewayRouteStore(":memory:")
    original = TaskRouteRecord(
        task_id="task-1",
        agent_name="main",
        metadata={"version": "original"},
        route_kind="local",
        upstream_task_id="task-1",
    )
    try:
        store.save_route(original)
        with pytest.raises(ValueError, match="binding conflict"):
            store.save_route(
                TaskRouteRecord(
                    task_id="task-1",
                    agent_name="remote",
                    metadata={"version": "replacement"},
                    route_kind="remote_ref",
                    upstream_task_id="upstream-2",
                )
            )

        assert store.get_route("task-1") == original
    finally:
        store.close()


def test_route_store_updates_metadata_for_same_identity_binding() -> None:
    store = GatewayRouteStore(":memory:")
    original = TaskRouteRecord(
        task_id="task-1",
        agent_name="main",
        metadata={"version": "original"},
        route_kind="local",
        upstream_task_id="task-1",
    )
    updated = TaskRouteRecord(
        task_id="task-1",
        agent_name="main",
        metadata={"version": "updated"},
        route_kind="local",
        upstream_task_id="task-1",
        webhook={"url": "https://example.test/hook"},
    )
    try:
        store.save_route(original)
        store.save_route(updated)
        assert store.get_route("task-1") == updated
    finally:
        store.close()


def test_router_rebuilds_and_refreshes_remote_record() -> None:
    asyncio.run(_rebuild_and_refresh_remote_record())


async def _rebuild_and_refresh_remote_record() -> None:
    control = RecordingControl()
    store = GatewayRouteStore(":memory:")
    route = TaskRouteRecord(
        task_id="local-remote-id",
        agent_name="research",
        metadata={},
        route_kind="remote_ref",
        upstream_task_id="upstream-id",
    )
    store.save_route(route)
    try:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        record = await router.get_record(await router.get_route(route.task_id))

        assert record.task_id == route.task_id
        assert control.ensure_calls[0]["upstream_task_id"] == "upstream-id"
        assert control.refresh_calls == [route.task_id]
    finally:
        store.close()


def test_router_discovers_only_descendants_of_gateway_roots() -> None:
    asyncio.run(_discover_only_gateway_descendants())


async def _discover_only_gateway_descendants() -> None:
    control = RecordingControl()
    root = _record("root-id")
    child = _record(
        "child-id",
        agent_name="research",
        parent_task_id=root.task_id,
        root_task_id=root.task_id,
        depth=2,
    )
    orphan = _record("orphan-id")
    control.records = {
        record.task_id: record for record in (root, child, orphan)
    }
    store = GatewayRouteStore(":memory:")
    store.save_route(
        TaskRouteRecord(
            task_id=root.task_id,
            agent_name=root.agent_name,
            metadata={"channel_user": "alice"},
            route_kind="local",
            upstream_task_id=root.task_id,
        )
    )
    try:
        router = TaskRouter(control=control, route_store=store)  # type: ignore[arg-type]

        routes = await router.list_routes()

        assert {route.task_id for route in routes} == {"root-id", "child-id"}
        child_route = await router.get_route("child-id")
        assert child_route.agent_name == "research"
        assert child_route.metadata == {"channel_user": "alice"}
    finally:
        store.close()
