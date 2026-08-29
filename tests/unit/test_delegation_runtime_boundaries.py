from __future__ import annotations

import ast
import asyncio
from pathlib import Path
from typing import Any

from ruyi_agent.runtime.delegation import async_runtime
from ruyi_agent.runtime.delegation.local_executor import LocalTaskExecutor
from ruyi_agent.runtime.delegation.notifications import SettledRunNotifier
from ruyi_agent.runtime.delegation.policy import DelegationPolicy
from ruyi_agent.runtime.delegation.registry import AgentRegistry
from ruyi_agent.runtime.delegation.remote_port import RemoteTaskPort
from ruyi_agent.runtime.delegation.run_supervisor import RunSupervisor
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.delegation.task_runtime import TaskRuntime
from ruyi_agent.runtime.delegation.tools import DelegationTools


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

    assert isinstance(control._registry, AgentRegistry)
    assert isinstance(control._task_manager, TaskManager)
    assert isinstance(control._local_executor, LocalTaskExecutor)
    assert isinstance(control._remote_port, RemoteTaskPort)
    assert isinstance(control._run_supervisor, RunSupervisor)
    assert isinstance(control._delegation_policy, DelegationPolicy)
    assert isinstance(control._settled_notifier, SettledRunNotifier)
    assert isinstance(control._task_runtime, TaskRuntime)
    assert isinstance(control._delegation_tools, DelegationTools)
    assert control.list_registered_agents_snapshot() == []

    asyncio.run(control.close())


def test_legacy_async_runtime_imports_resolve_to_boundary_types() -> None:
    assert async_runtime.AgentRegistry is AgentRegistry
    assert async_runtime.TaskManager is TaskManager
    assert async_runtime.UnknownWorkerTaskError.__module__.endswith(".contracts")


def test_agent_control_forwards_to_the_structured_task_runtime(
    monkeypatch,
) -> None:
    control = _control()
    sentinel: list[Any] = [object()]
    monkeypatch.setattr(
        control._task_runtime,
        "list_task_records",
        lambda: sentinel,
    )

    assert control.list_task_records() is sentinel


def test_agent_control_forwards_to_local_and_remote_ports(monkeypatch) -> None:
    control = _control()
    local_calls: list[tuple[str, str]] = []

    async def local_start(
        task_id: str,
        message: str,
        *,
        permit: object | None = None,
    ) -> None:
        del permit
        local_calls.append((task_id, message))

    monkeypatch.setattr(
        control._local_executor,
        "_start_run",
        local_start,
    )

    async def remote_refresh(task_id: str) -> object:
        return {"task_id": task_id}

    monkeypatch.setattr(control._remote_port, "refresh_task", remote_refresh)

    asyncio.run(control._start_run("local-1", "hello"))
    assert local_calls == [("local-1", "hello")]
    assert asyncio.run(control.refresh_task("remote-1")) == {"task_id": "remote-1"}


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
    other_runtime_sources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in package.glob("*.py")
        if path.name not in {"run_supervisor.py", "task_manager.py"}
    )

    assert "asyncio.create_task" in supervisor
    assert ".mark_running(" in supervisor
    assert "asyncio.create_task" not in other_runtime_sources
    assert ".mark_running(" not in other_runtime_sources


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
    assert control_node.end_lineno - control_node.lineno + 1 <= 700


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
