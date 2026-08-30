from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from typing import Any

from ruyi_agent.runtime.delegation import async_runtime
from ruyi_agent.runtime.delegation.task_runtime import TaskRuntime


class _Backend:
    def upload_files(self, files: list[tuple[str, bytes]]) -> list[Any]:
        return list(files)

    def download_files(self, paths: list[str]) -> list[Any]:
        return list(paths)


def _control() -> async_runtime.AgentControl:
    return async_runtime.AgentControl(
        {},
        checkpointer=object(),
        backend=_Backend(),
    )


def test_agent_control_assembles_focused_runtime_components() -> None:
    control = _control()

    assert list(vars(control)) == ["_backend", "_workspace_root", "_task_runtime"]
    assert isinstance(control._task_runtime, TaskRuntime)

    asyncio.run(control.close())


def test_async_runtime_exports_only_the_facade() -> None:
    assert async_runtime.__all__ == ["AgentControl"]
    assert {
        "AgentRegistry",
        "TaskManager",
        "UnknownWorkerTaskError",
        "create_runtime_agent",
        "httpx",
    }.isdisjoint(vars(async_runtime))


def test_agent_control_forwards_only_application_api(
    monkeypatch,
) -> None:
    control = _control()
    sentinel: list[Any] = [object()]
    monkeypatch.setattr(
        control._task_runtime,
        "list_persisted_task_records",
        lambda: sentinel,
    )

    assert control.list_persisted_task_records() is sentinel


def test_agent_control_uses_the_typed_runtime_owner(monkeypatch) -> None:
    control = _control()
    sentinel = object()

    monkeypatch.setattr(
        control._task_runtime,
        "get_task_record",
        lambda task_id: sentinel,
    )

    assert control.get_task_record("task-1") is sentinel


def test_remote_network_effects_do_not_leak_into_task_coordinator() -> None:
    package = Path(async_runtime.__file__).parent
    task_runtime_source = (package / "task_runtime.py").read_text(encoding="utf-8")
    remote_port_source = (package / "remote_port.py").read_text(encoding="utf-8")

    assert "_a2a_client" not in task_runtime_source
    assert "_a2a_client.create_task" in remote_port_source
    assert "_a2a_client.send_input" in remote_port_source
    assert "_a2a_client.cancel_task" in remote_port_source
    assert "_a2a_client.submit_review_decision" in remote_port_source


def test_run_creation_and_mark_running_are_owned_by_supervisor() -> None:
    package = Path(async_runtime.__file__).parent
    supervisor = (package / "run_supervisor.py").read_text(encoding="utf-8")
    notifier = (package / "notifications.py").read_text(encoding="utf-8")
    other_runtime_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in package.glob("*.py")
        if path.name not in {"run_supervisor.py", "task_manager.py", "notifications.py"}
    )

    assert "asyncio.create_task" in supervisor
    assert ".mark_running(" in supervisor
    assert "asyncio.create_task" not in other_runtime_sources
    assert ".mark_running(" not in other_runtime_sources
    assert "asyncio.create_task" not in notifier
    assert "_reconciliation_task" not in notifier
    assert "_wake_tasks" not in notifier


def test_runtime_boundary_modules_and_facade_stay_within_line_budgets() -> None:
    package = Path(async_runtime.__file__).parent
    production_modules = [
        path for path in package.glob("*.py") if path.name != "__init__.py"
    ]
    oversized = {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in production_modules
        if len(path.read_text(encoding="utf-8").splitlines()) >= 1000
    }
    assert oversized == {}

    source = Path(async_runtime.__file__).read_text(encoding="utf-8")
    module = ast.parse(source)
    control_node = next(
        node
        for node in module.body
        if isinstance(node, ast.ClassDef) and node.name == "AgentControl"
    )
    assert control_node.end_lineno is not None
    assert control_node.end_lineno - control_node.lineno + 1 <= 400
    methods = [
        node.name
        for node in control_node.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    assert len(methods) == 24
    assert methods == [
        "__init__",
        "workspace_root",
        "upload_files",
        "download_files",
        "wake_pending_mailbox_tasks",
        "start_mailbox_recovery",
        "close",
        "handle_remote_task_event",
        "list_remote_task_messages",
        "open_remote_task_event_stream",
        "ensure_remote_task_record",
        "refresh_task",
        "get_task_record",
        "open_local_task_event_stream",
        "get_local_task_message_snapshot",
        "list_persisted_task_records",
        "list_pending_reviews",
        "get_pending_review",
        "submit_review_decision",
        "prepare_delegation_metadata",
        "spawn_task",
        "remote_create_idempotency_guaranteed",
        "send_task_input",
        "cancel_task",
    ]


def test_delegation_runtime_regressions_stay_split_by_boundary() -> None:
    unit_tests = Path(__file__).parent
    legacy_monolith = unit_tests / "test_async_subagent_runtime.py"
    boundary_tests = sorted(unit_tests.glob("test_async_subagent_*.py"))

    assert not legacy_monolith.exists()
    assert {path.name for path in boundary_tests} == {
        "test_async_subagent_local_executor.py",
        "test_async_subagent_remote_port.py",
        "test_async_subagent_task_manager_reviews.py",
        "test_async_subagent_task_runtime.py",
        "test_async_subagent_tools_and_delivery.py",
    }
    assert {
        path.name: len(path.read_text(encoding="utf-8").splitlines())
        for path in boundary_tests
        if len(path.read_text(encoding="utf-8").splitlines()) >= 1500
    } == {}
