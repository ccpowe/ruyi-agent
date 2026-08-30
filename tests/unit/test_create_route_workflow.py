from __future__ import annotations

import ast
import asyncio
import sqlite3
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ruyi_agent.gateway.create_route_workflow import CreateRouteRequest
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.storage.gateway_route_store import (
    GatewayCreateEvidence,
    GatewayRouteStore,
)
from ruyi_agent.task_models import (
    TaskRecord,
    TaskRouteKind,
    TaskRouteRecord,
    TaskState,
)


_ROOT = Path(__file__).parents[2]
_ROUTING = _ROOT / "src/ruyi_agent/gateway/routing.py"
_WORKFLOW = _ROOT / "src/ruyi_agent/gateway/create_route_workflow.py"


def test_create_workflow_has_typed_boundary_and_acyclic_import_dag() -> None:
    routing_tree = ast.parse(_ROUTING.read_text())
    workflow_tree = ast.parse(_WORKFLOW.read_text())
    workflow_class = next(
        node
        for node in workflow_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "CreateRouteWorkflow"
    )
    request_class = next(
        node
        for node in workflow_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "CreateRouteRequest"
    )
    run = next(
        node
        for node in workflow_class.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "run"
    )
    create = next(
        node
        for node in next(
            node
            for node in routing_tree.body
            if isinstance(node, ast.ClassDef) and node.name == "TaskRouter"
        ).body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "create_task"
    )

    request_decorator = request_class.decorator_list[0]
    assert isinstance(request_decorator, ast.Call)
    assert {keyword.arg for keyword in request_decorator.keywords} == {
        "frozen",
        "slots",
    }
    assert len(run.body) <= 60
    workflow_calls = [
        node
        for node in ast.walk(create)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "run"
    ]
    assert len(workflow_calls) == 1
    assert sum(isinstance(node, ast.Await) for node in ast.walk(create)) == 1
    assert not {
        node.func.attr
        for node in ast.walk(routing_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr
        in {
            "areserve_route",
            "amark_create_effect_started",
            "spawn_task",
        }
    }
    assert not any(
        isinstance(node, ast.ImportFrom) and node.module == "ruyi_agent.gateway.routing"
        for node in workflow_tree.body
    )

    request = CreateRouteRequest(
        agent_name="main",
        route_kind="local",
        input_content="hello",
        metadata={},
        webhook=None,
        delegation_context=None,
    )
    with pytest.raises(FrozenInstanceError):
        request.agent_name = "other"


class _Control:
    def __init__(self, record: TaskRecord) -> None:
        self.record = record
        self.spawn_calls = 0

    async def spawn_task(
        self, agent_name: str, task: str, **kwargs: object
    ) -> TaskRecord:
        del agent_name, task, kwargs
        self.spawn_calls += 1
        return self.record

    def get_task_record(self, task_id: str) -> TaskRecord:
        del task_id
        return self.record


class _MarkerFailureStore(GatewayRouteStore):
    async def amark_create_effect_started(self, task_id: str) -> GatewayCreateEvidence:
        del task_id
        raise sqlite3.OperationalError("marker unavailable")


def _record(
    task_id: str,
    *,
    route_kind: TaskRouteKind,
    upstream_task_id: str | None,
    state: TaskState = "completed",
    run_count: int = 1,
) -> TaskRecord:
    now = datetime.now(UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name="main",
        state=state,
        thread_id=task_id,
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        created_at=now,
        updated_at=now,
        run_count=run_count,
        route_kind=route_kind,
        upstream_task_id=upstream_task_id,
    )


@pytest.mark.parametrize("failure", ["before_effect", "route_marker"])
def test_public_router_boundary_failure_never_spawns(failure: str) -> None:
    async def scenario() -> None:
        control = _Control(
            _record("boundary-task", route_kind="local", upstream_task_id=None)
        )
        store: GatewayRouteStore = (
            _MarkerFailureStore(":memory:")
            if failure == "route_marker"
            else GatewayRouteStore(":memory:")
        )
        try:
            router = TaskRouter(control=control, route_store=store)

            async def before_effect() -> None:
                raise RuntimeError("command effect persistence failed")

            with pytest.raises(GatewayTaskError) as caught:
                await router.create_task(
                    agent_name="main",
                    route_kind="local",
                    input_content="hello",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id="boundary-task",
                    before_effect=(
                        before_effect if failure == "before_effect" else None
                    ),
                )

            route = await router.get_route("boundary-task")
            assert caught.value.code == "route_persistence_failed"
            assert route.route_state == "failed"
            assert control.spawn_calls == 0
        finally:
            store.close()

    asyncio.run(scenario())


def test_public_router_replays_terminal_route_without_spawning() -> None:
    async def scenario() -> None:
        control = _Control(
            _record("terminal-task", route_kind="local", upstream_task_id=None)
        )
        store = GatewayRouteStore(":memory:")
        store.save_route(
            TaskRouteRecord(
                task_id="terminal-task",
                agent_name="main",
                metadata={},
                route_kind="local",
                upstream_task_id="terminal-task",
                route_state="failed",
                route_error="previous create failed",
            )
        )
        try:
            router = TaskRouter(control=control, route_store=store)
            with pytest.raises(GatewayTaskError) as caught:
                await router.create_task(
                    agent_name="main",
                    route_kind="local",
                    input_content="must not retry",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id="terminal-task",
                )
            assert caught.value.code == "task_creation_not_retryable"
            assert caught.value.details["route_state"] == "failed"
            assert control.spawn_calls == 0
        finally:
            store.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("route_kind", "upstream_task_id", "state", "run_count", "code"),
    [
        ("remote_ref", None, "completed", 1, "upstream_gateway_error"),
        ("local", None, "pending", 0, "task_effect_not_durable"),
    ],
)
def test_public_router_rejects_remote_payload_or_nondurable_effect(
    route_kind: TaskRouteKind,
    upstream_task_id: str | None,
    state: TaskState,
    run_count: int,
    code: str,
) -> None:
    async def scenario() -> None:
        task_id = f"effect-{route_kind}"
        control = _Control(
            _record(
                task_id,
                route_kind=route_kind,
                upstream_task_id=upstream_task_id,
                state=state,
                run_count=run_count,
            )
        )
        store = GatewayRouteStore(":memory:")
        try:
            router = TaskRouter(control=control, route_store=store)
            with pytest.raises(GatewayTaskError) as caught:
                await router.create_task(
                    agent_name="main",
                    route_kind=route_kind,
                    input_content="hello",
                    metadata={},
                    webhook=None,
                    delegation_context=None,
                    task_id=task_id,
                )
            route = await router.get_route(task_id)
            assert caught.value.code == code
            assert route.route_state == "uncertain"
            assert control.spawn_calls == 1
        finally:
            store.close()

    asyncio.run(scenario())
