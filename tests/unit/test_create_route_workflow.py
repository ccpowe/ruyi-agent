from __future__ import annotations

import ast
import asyncio
import sqlite3
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ruyi_agent.gateway.create_route_workflow import CreateRouteRequest, RouteRecordPort
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.runtime.delegation.contracts import UnknownAgentTargetError
from ruyi_agent.runtime.delegation.contracts import UnknownWorkerTaskError
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
_BRANCH_NODES = (ast.If, ast.For, ast.While, ast.ExceptHandler, ast.IfExp, ast.BoolOp)


def _named(nodes: list[ast.AST], name: str, kind: type[ast.AST]) -> ast.AST:
    return next(node for node in nodes if isinstance(node, kind) and node.name == name)


def _imported_modules(tree: ast.AST) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    return modules


def test_create_workflow_has_typed_boundary_and_acyclic_import_dag() -> None:
    routing_tree = ast.parse(_ROUTING.read_text())
    workflow_tree = ast.parse(_WORKFLOW.read_text())
    workflow_class = _named(workflow_tree.body, "CreateRouteWorkflow", ast.ClassDef)
    port_class = _named(workflow_tree.body, "RouteRecordPort", ast.ClassDef)
    request_class = _named(workflow_tree.body, "CreateRouteRequest", ast.ClassDef)
    router_class = _named(routing_tree.body, "TaskRouter", ast.ClassDef)
    run = _named(workflow_class.body, "run", ast.AsyncFunctionDef)
    create = _named(router_class.body, "create_task", ast.AsyncFunctionDef)
    ensure = _named(router_class.body, "ensure_record", ast.FunctionDef)
    read = _named(router_class.body, "get_record", ast.AsyncFunctionDef)
    workflow_init = _named(workflow_class.body, "__init__", ast.FunctionDef)

    def span(node: ast.AST) -> int:
        assert node.lineno is not None and node.end_lineno is not None
        return node.end_lineno - node.lineno + 1

    def branches(node: ast.AST) -> int:
        return sum(isinstance(child, _BRANCH_NODES) for child in ast.walk(node))

    callables = [
        node
        for node in ast.walk(workflow_tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    workflow_call = next(
        node
        for node in ast.walk(routing_tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "CreateRouteWorkflow"
    )

    request_decorator = request_class.decorator_list[0]
    assert isinstance(request_decorator, ast.Call)
    assert {keyword.arg for keyword in request_decorator.keywords} == {
        "frozen",
        "slots",
    }
    assert isinstance(port_class.decorator_list[0], ast.Call)
    assert {keyword.arg for keyword in port_class.decorator_list[0].keywords} == {
        "slots"
    }
    assert RouteRecordPort.__slots__ == ("control",)
    assert [arg.arg for arg in workflow_init.args.args] == [
        "self",
        "control",
        "route_store",
    ]
    assert not workflow_init.args.kwonlyargs
    assert span(run) <= 60 and branches(run) <= 4
    assert all(span(node) <= 65 and branches(node) <= 10 for node in callables)
    assert len(callables) <= 26
    assert sum(span(node) for node in callables) <= 360
    assert span(create) <= 45 and branches(create) <= 2
    assert len(ensure.body) == len(read.body) == 1
    assert all(isinstance(node.body[0], ast.Return) for node in (ensure, read))
    assert len(workflow_call.args) == 2 and not workflow_call.keywords
    assert all(
        not isinstance(node, ast.Attribute)
        or not isinstance(node.value, ast.Name)
        or node.value.id != "self"
        for node in workflow_call.args
    )
    assert (
        sum(
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "run"
            for node in ast.walk(create)
        )
        == 1
    )
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
    forbidden_workflow_imports = (
        "ruyi_agent.gateway.routing",
        "ruyi_agent.gateway.application",
        "ruyi_agent.gateway.task_service",
        "ruyi_agent.gateway.commands",
        "ruyi_agent.gateway.channels",
    )
    assert not any(
        imported == forbidden or imported.startswith(f"{forbidden}.")
        for imported in _imported_modules(workflow_tree)
        for forbidden in forbidden_workflow_imports
    )
    source = _WORKFLOW.read_text()
    assert not any(
        token in source
        for token in "getattr(|hasattr(|setattr(|vars(|locals(|globals(|__dict__|except AttributeError|# fmt:|# noqa|coverage: ".split(
            "|"
        )
    )
    assert all(
        "ruyi_agent.gateway.create_route_workflow"
        not in _imported_modules(ast.parse(path.read_text()))
        for tree_path in (
            _ROOT / "src/ruyi_agent/runtime",
            _ROOT / "src/ruyi_agent/storage",
        )
        for path in tree_path.rglob("*.py")
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

    def remote_create_idempotency_guaranteed(self, agent_name: str) -> bool:
        return True

    async def spawn_task(
        self, agent_name: str, task: str, **kwargs: object
    ) -> TaskRecord:
        del agent_name, task, kwargs
        self.spawn_calls += 1
        return self.record

    def get_task_record(self, task_id: str) -> TaskRecord:
        if task_id.startswith("effect-"):
            raise UnknownWorkerTaskError(task_id)
        del task_id
        return self.record

    def ensure_remote_task_record(self, **kwargs: object) -> TaskRecord:
        del kwargs
        raise UnknownAgentTargetError("missing")


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
    private_remote: bool = False,
) -> TaskRecord:
    now = datetime.now(UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name="main",
        state=state,
        thread_id="private-thread" if private_remote else task_id,
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        created_at=now,
        updated_at=now,
        result="private-result" if private_remote else None,
        error="private-downstream-error" if private_remote else None,
        run_count=run_count,
        route_kind=route_kind,
        upstream_task_id=upstream_task_id,
        webhook={"url": "private-url"} if private_remote else None,
        pending_review=(
            {
                "review_id": "private-review",
                "source_task_id": "private-upstream-id",
                "details": "private-review-detail",
            }
            if private_remote
            else None
        ),
        external_operation="private-operation" if private_remote else None,
        external_operation_identity=("private-upstream-id" if private_remote else None),
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
            assert caught.value.kind is None
            assert caught.value.effect_disposition is None
            assert caught.value.message == (
                "Gateway could not durably transition the Task route"
            )
            assert caught.value.details == {
                "task_id": "boundary-task",
                "task_url": "/tasks/boundary-task",
                "route_state": "failed",
                "task_queryable": True,
                "create_retryable": False,
                "effect_outcome": "not_started",
            }
            assert route.route_state == "failed"
            assert route.route_error == (
                "Gateway command effect boundary could not be persisted"
                if failure == "before_effect"
                else "Gateway create effect boundary could not be persisted"
            )
            evidence = store.get_create_evidence("boundary-task")
            assert evidence is not None and evidence.effect_boundary == "reserved"
            queried = await router.get_record(route)
            assert queried.state == "failed"
            assert queried.error == "Gateway Task route is failed"
            assert queried.upstream_task_id is None
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
                private_remote=route_kind == "remote_ref",
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
            if route_kind == "remote_ref":
                assert caught.value.kind == "upstream_failure"
                assert caught.value.effect_disposition is None
                assert caught.value.message == (
                    "Remote Gateway returned an invalid Task payload"
                )
                assert caught.value.details == {
                    "task_id": task_id,
                    "task_url": f"/tasks/{task_id}",
                    "route_state": "uncertain",
                    "task_queryable": True,
                    "create_retryable": False,
                    "effect_outcome": "uncertain",
                }
                assert route.route_error == "Remote Gateway Task creation failed"
                evidence = store.get_create_evidence(task_id)
                assert evidence is not None and evidence.effect_boundary == "started"
                queried = await router.get_record(route)
                assert queried.state == "interrupted"
                assert queried.error == "Gateway Task route is uncertain"
                assert queried.upstream_task_id is None
                for public in (caught.value, route.route_error, queried):
                    assert "private-" not in repr(public)
                route.route_state, route.upstream_task_id = "active", "upstream"
                with pytest.raises(GatewayTaskError, match="Runtime"):
                    router.ensure_record(route)
                route.route_kind = "local"
                route.route_state, route.upstream_task_id = "active", "missing"
                with pytest.raises(GatewayTaskError, match="does not exist"):
                    await router.get_record(route)
        finally:
            store.close()

    asyncio.run(scenario())
