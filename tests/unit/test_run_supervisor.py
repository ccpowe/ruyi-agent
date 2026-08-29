from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from functools import wraps
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import ruyi_agent.runtime.bootstrap as bootstrap_module
import ruyi_agent.runtime.delegation.async_runtime as runtime_module
from ruyi_agent.runtime.delegation.contracts import TaskAlreadyRunningError
from ruyi_agent.runtime.delegation.run_supervisor import RuntimeClosingError
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from tests.support.async_subagent_runtime import (
    FakeAgentFactory,
    InterruptingAgentFactory,
    build_specs,
)


def async_test(function: Any) -> Any:
    """Run one async scenario without adding a pytest plugin dependency."""

    @wraps(function)
    def run(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(function(*args, **kwargs))

    return run


class ReleasableAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def ainvoke(self, payload: Any, *, config: Any, version: str) -> dict[str, Any]:
        del payload, config, version
        self.started.set()
        await self.release.wait()
        return {"messages": [{"role": "assistant", "content": "drained"}]}


class MultiBlockingAgent:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.started_count = 0
        self.all_started = asyncio.Event()

    async def ainvoke(self, payload: Any, *, config: Any, version: str) -> dict[str, Any]:
        del payload, config, version
        self.started_count += 1
        if self.started_count == self.expected:
            self.all_started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled run must not return")


def _durable_control(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    factory: Any,
    *,
    mailbox: bool = False,
    shutdown_grace_period: float = 0,
) -> tuple[runtime_module.AgentControl, TaskStore, MailboxStore | None]:
    monkeypatch.setattr(runtime_module, "create_runtime_agent", factory)
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    mailbox_store = (
        MailboxStore(str(tmp_path / "tasks.sqlite")) if mailbox else None
    )
    control = runtime_module.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
        mailbox=AgentMailbox(mailbox_store) if mailbox_store is not None else None,
        shutdown_grace_period=shutdown_grace_period,
    )
    return control, task_store, mailbox_store


@async_test
async def test_initial_mark_running_failure_never_executes_payload_and_rolls_back(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = FakeAgentFactory()
    control, store, mailbox_store = _durable_control(
        monkeypatch,
        tmp_path,
        factory,
    )
    ledger = control._task_manager.event_ledger
    assert ledger is not None
    monkeypatch.setattr(
        ledger,
        "update_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("mark failed")),
    )
    try:
        with pytest.raises(OSError, match="mark failed"):
            await control.spawn_task(
                "background_research",
                "must not run",
                task_id="initial-persist-failure",
            )
        await asyncio.sleep(0)

        record = control.get_task_record("initial-persist-failure")
        assert record.state == "pending"
        assert record.run_count == 0
        assert record.pending_review is None
        assert record.mailbox_suppressed is False
        assert record.mailbox_delivered is False
        assert store.get_task(record.task_id) == record
        assert factory.created == []
        assert control.get_live_run(record.task_id) is None
        assert control._run_supervisor.active_run_count == 0
        assert control._run_supervisor._maintenance == set()
    finally:
        await control.close()
        if mailbox_store is not None:
            mailbox_store.close()
        store.close()


@async_test
async def test_mailbox_mark_running_failure_restores_settled_task_without_rerun(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = FakeAgentFactory()
    control, store, mailbox_store = _durable_control(
        monkeypatch,
        tmp_path,
        factory,
        mailbox=True,
    )
    assert mailbox_store is not None
    try:
        created = await control.spawn_task(
            "background_research",
            "first run",
            task_id="mailbox-persist-failure",
        )
        run = control.get_live_run(created.task_id)
        assert run is not None
        await run
        await asyncio.sleep(0)
        before = deepcopy(control.get_task_record(created.task_id))
        persisted_before = store.get_task(created.task_id)

        ledger = control._task_manager.event_ledger
        assert ledger is not None
        monkeypatch.setattr(
            ledger,
            "update_task",
            lambda *args, **kwargs: (_ for _ in ()).throw(OSError("mark failed")),
        )
        with pytest.raises(OSError, match="mark failed"):
            await control.send_task_input(created.task_id, "queued input")
        await asyncio.sleep(0)

        assert control.get_task_record(created.task_id) == before
        assert store.get_task(created.task_id) == persisted_before
        assert len(factory.created) == 1
        assert len(factory.created[0].calls) == 1
        assert control.get_live_run(created.task_id) is None
        assert control._run_supervisor.active_run_count == 0
        assert control._run_supervisor._maintenance == set()
    finally:
        await control.close()
        mailbox_store.close()
        store.close()


@async_test
async def test_review_resume_mark_running_failure_preserves_pending_review(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = InterruptingAgentFactory()
    control, store, mailbox_store = _durable_control(
        monkeypatch,
        tmp_path,
        factory,
    )
    try:
        created = await control.spawn_task(
            "background_research",
            "needs review",
            task_id="resume-persist-failure",
        )
        run = control.get_live_run(created.task_id)
        assert run is not None
        await run
        waiting = control.get_task_record(created.task_id)
        assert waiting.pending_review is not None
        review_id = waiting.pending_review["review_id"]
        before = deepcopy(waiting)
        persisted_before = store.get_task(created.task_id)

        ledger = control._task_manager.event_ledger
        assert ledger is not None
        monkeypatch.setattr(
            ledger,
            "update_review_transition",
            lambda *args, **kwargs: (_ for _ in ()).throw(OSError("mark failed")),
        )
        with pytest.raises(OSError, match="mark failed"):
            await control.submit_review_decision(
                review_id,
                [{"type": "approve"}],
            )
        await asyncio.sleep(0)

        assert control.get_task_record(created.task_id) == before
        assert store.get_task(created.task_id) == persisted_before
        assert len(factory.created[0].calls) == 1
        assert control.get_live_run(created.task_id) is None
        assert control._run_supervisor.active_run_count == 0
        assert control._run_supervisor._maintenance == set()
    finally:
        await control.close()
        if mailbox_store is not None:
            mailbox_store.close()
        store.close()


@async_test
async def test_supervisor_serializes_competing_schedules() -> None:
    control = runtime_module.AgentControl({}, checkpointer=object(), backend=object())
    control._task_manager.create_task_record(
        "same-task",
        "worker",
        parent_task_id=None,
        root_task_id="same-task",
        depth=1,
    )
    started = asyncio.Event()
    release = asyncio.Event()

    async def runner() -> None:
        started.set()
        await release.wait()
        control._task_manager.mark_completed("same-task", "done")

    try:
        results = await asyncio.gather(
            control._run_supervisor.schedule("same-task", runner),
            control._run_supervisor.schedule("same-task", runner),
            return_exceptions=True,
        )
        assert sum(isinstance(item, asyncio.Task) for item in results) == 1
        assert sum(isinstance(item, TaskAlreadyRunningError) for item in results) == 1
        await started.wait()
        release.set()
        run = control.get_live_run("same-task")
        if run is not None:
            await run
        assert control.get_task_record("same-task").state == "completed"
    finally:
        await control.close()


@async_test
async def test_close_allows_natural_completion_within_grace_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = ReleasableAgent()
    monkeypatch.setattr(runtime_module, "create_runtime_agent", lambda **kwargs: agent)
    control = runtime_module.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0.5,
    )
    record = await control.spawn_task("background_research", "finish during drain")
    await agent.started.wait()
    asyncio.get_running_loop().call_later(0.01, agent.release.set)

    await control.close()
    await control.close()

    assert control.get_task_record(record.task_id).state == "completed"
    assert control._run_supervisor.active_run_count == 0


@async_test
async def test_close_cancels_timeout_run_and_persists_interrupted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    agent = MultiBlockingAgent(expected=1)
    control, store, mailbox_store = _durable_control(
        monkeypatch,
        tmp_path,
        lambda **kwargs: agent,
    )
    try:
        record = await control.spawn_task("background_research", "block")
        await agent.all_started.wait()
        await control.close()

        interrupted = control.get_task_record(record.task_id)
        assert interrupted.state == "interrupted"
        assert interrupted.error is not None
        assert interrupted.error.startswith("Task interrupted: CancelledError")
        assert store.get_task(record.task_id) == interrupted
        assert control.get_live_run(record.task_id) is None
    finally:
        await control.close()
        if mailbox_store is not None:
            mailbox_store.close()
        store.close()


@async_test
async def test_close_interrupts_multiple_active_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = MultiBlockingAgent(expected=2)
    monkeypatch.setattr(runtime_module, "create_runtime_agent", lambda **kwargs: agent)
    control = runtime_module.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    first = await control.spawn_task("background_research", "one")
    second = await control.spawn_task("background_research", "two")
    await agent.all_started.wait()

    await control.close()

    assert control.get_task_record(first.task_id).state == "interrupted"
    assert control.get_task_record(second.task_id).state == "interrupted"
    assert control._run_supervisor.active_run_count == 0


@async_test
async def test_shutdown_rejects_concurrent_new_schedule_without_payload_execution() -> None:
    control = runtime_module.AgentControl(
        {},
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    for task_id in ("active", "late"):
        control._task_manager.create_task_record(
            task_id,
            "worker",
            parent_task_id=None,
            root_task_id=task_id,
            depth=1,
        )
    started = asyncio.Event()
    late_executed = False

    async def active_runner() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            control._task_manager.mark_interrupted(
                "active",
                "Task interrupted: runtime shutdown",
            )
            raise

    async def late_runner() -> None:
        nonlocal late_executed
        late_executed = True

    await control._run_supervisor.schedule("active", active_runner)
    await started.wait()
    closing = asyncio.create_task(control.close())
    while control._run_supervisor.is_accepting:
        await asyncio.sleep(0)

    with pytest.raises(RuntimeClosingError, match="closing"):
        await control._run_supervisor.schedule("late", late_runner)
    await closing

    assert late_executed is False
    assert control.get_task_record("late").state == "pending"
    assert control.get_task_record("late").run_count == 0
    assert control.get_task_record("active").state == "interrupted"


@async_test
async def test_close_waits_for_admitted_mutation_and_rejects_later_mutations() -> None:
    control = runtime_module.AgentControl(
        {},
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    control._task_manager.create_task_record(
        "admitted",
        "worker",
        parent_task_id=None,
        root_task_id="admitted",
        depth=1,
    )
    admitted = asyncio.Event()
    proceed = asyncio.Event()

    async def runner() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            control._task_manager.mark_interrupted(
                "admitted",
                "Task interrupted: runtime shutdown",
            )
            raise

    async def accepted_mutation() -> asyncio.Task[None]:
        admitted.set()
        await proceed.wait()
        return await control._run_supervisor.schedule("admitted", runner)

    async def rejected_mutation() -> None:
        raise AssertionError("rejected mutation payload must not run")

    mutation = asyncio.create_task(
        control._run_supervisor.mutate(accepted_mutation)
    )
    await admitted.wait()
    closing = asyncio.create_task(control.close())
    while control._run_supervisor.is_accepting:
        await asyncio.sleep(0)
    assert closing.done() is False

    with pytest.raises(RuntimeClosingError, match="closing"):
        await control._run_supervisor.mutate(rejected_mutation)
    proceed.set()
    await mutation
    await closing

    assert control.get_task_record("admitted").state == "interrupted"


@async_test
async def test_public_task_mutations_reject_after_close() -> None:
    control = runtime_module.AgentControl({}, checkpointer=object(), backend=object())
    await control.close()

    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.spawn_task("missing", "must not mutate")
    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.send_task_input("missing", "must not mutate")
    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.submit_review_decision("missing", [])


@async_test
async def test_unhandled_run_failure_is_consumed_and_persisted_interrupted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    control = runtime_module.AgentControl({}, checkpointer=object(), backend=object())
    control._task_manager.create_task_record(
        "broken",
        "worker",
        parent_task_id=None,
        root_task_id="broken",
        depth=1,
    )

    async def broken_runner() -> None:
        raise RuntimeError("run exploded")

    await control._run_supervisor.schedule("broken", broken_runner)
    for _ in range(3):
        await asyncio.sleep(0)

    assert control.get_task_record("broken").state == "interrupted"
    assert control.get_live_run("broken") is None
    assert "Task run broken failed" in caplog.text
    await control.close()


@async_test
async def test_interrupted_persistence_failure_is_consumed_during_close(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    agent = MultiBlockingAgent(expected=1)
    control, store, mailbox_store = _durable_control(
        monkeypatch,
        tmp_path,
        lambda **kwargs: agent,
    )
    record = await control.spawn_task("background_research", "block")
    await agent.all_started.wait()

    def fail_interrupted(task_id: str, error: str) -> None:
        del task_id, error
        raise OSError("interrupted write failed")

    monkeypatch.setattr(control._task_manager, "mark_interrupted", fail_interrupted)
    try:
        await control.close()
        assert control.get_task_record(record.task_id).state == "running"
        assert store.get_task(record.task_id).state == "running"
        assert control.get_live_run(record.task_id) is None
        assert "Failed to persist interrupted state" in caplog.text
        assert "interrupted write failed" in caplog.text
    finally:
        await control.close()
        if mailbox_store is not None:
            mailbox_store.close()
        store.close()


@async_test
async def test_bootstrap_closes_control_before_stores_checkpointer_and_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    order: list[str] = []
    open_resources = {
        "checkpointer",
        "backend",
        "route",
        "command",
        "task",
        "mailbox",
        "review",
    }

    class FakeBackendRuntime:
        home_dir = str(tmp_path)
        skills_root = "/skills"
        backend = object()
        kind = "fake"

        def close(self) -> None:
            assert "checkpointer" not in open_resources
            open_resources.remove("backend")
            order.append("backend")

    class FakeStore:
        label = ""

        def __init__(self, path: str) -> None:
            del path

        def close(self) -> None:
            open_resources.remove(self.label)
            order.append(self.label)

    def store_type(label: str) -> type[FakeStore]:
        return type(f"{label.title()}Store", (FakeStore,), {"label": label})

    @asynccontextmanager
    async def fake_checkpointer() -> Any:
        try:
            yield object()
        finally:
            assert not open_resources.intersection(
                {"route", "command", "task", "mailbox", "review"}
            )
            open_resources.remove("checkpointer")
            order.append("checkpointer")

    class FakeSaver:
        @staticmethod
        def from_conn_string(path: str) -> Any:
            del path
            return fake_checkpointer()

    class FakeControl:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs

        async def wake_pending_mailbox_tasks(self) -> None:
            return None

        def start_mailbox_recovery(self) -> None:
            return None

        async def close(self) -> None:
            assert open_resources == {
                "checkpointer",
                "backend",
                "route",
                "command",
                "task",
                "mailbox",
                "review",
            }
            order.append("control")

    class FakeRegistry:
        def __init__(self, configs: Any) -> None:
            del configs

        async def refresh(self) -> Any:
            return SimpleNamespace(server_statuses=[])

    class FakeSkillCatalog:
        def __init__(self, workspace_root: Path) -> None:
            del workspace_root

        def scan(self) -> Any:
            return SimpleNamespace(skills={})

    monkeypatch.setattr(bootstrap_module, "configure_runtime_environment", lambda: None)
    monkeypatch.setattr(
        bootstrap_module,
        "create_backend_runtime",
        lambda: FakeBackendRuntime(),
    )
    monkeypatch.setattr(bootstrap_module, "AsyncSqliteSaver", FakeSaver)
    monkeypatch.setattr(bootstrap_module, "GatewayRouteStore", store_type("route"))
    monkeypatch.setattr(bootstrap_module, "GatewayCommandStore", store_type("command"))
    monkeypatch.setattr(bootstrap_module, "TaskStore", store_type("task"))
    monkeypatch.setattr(bootstrap_module, "MailboxStore", store_type("mailbox"))
    monkeypatch.setattr(bootstrap_module, "ReviewAuditStore", store_type("review"))
    monkeypatch.setattr(bootstrap_module, "AgentMailbox", lambda store: store)
    monkeypatch.setattr(bootstrap_module, "AgentControl", FakeControl)
    monkeypatch.setattr(bootstrap_module, "MCPRegistry", FakeRegistry)
    monkeypatch.setattr(bootstrap_module, "SkillCatalog", FakeSkillCatalog)
    monkeypatch.setattr(bootstrap_module, "SkillSyncer", lambda **kwargs: object())
    monkeypatch.setattr(
        bootstrap_module,
        "load_agent_configs",
        lambda: ("main", {}),
    )
    monkeypatch.setattr(bootstrap_module, "load_llm_provider_configs", lambda: {})
    monkeypatch.setattr(bootstrap_module, "load_permission_config", lambda: {})
    monkeypatch.setattr(
        bootstrap_module,
        "PermissionPolicy",
        lambda config: SimpleNamespace(default_profile="default"),
    )
    monkeypatch.setattr(bootstrap_module, "load_mcp_server_configs", lambda: {})
    monkeypatch.setattr(
        bootstrap_module,
        "build_all_local_worker_specs",
        lambda *args, **kwargs: _async_value({}),
    )
    monkeypatch.setattr(
        bootstrap_module,
        "build_all_remote_refs",
        lambda *args, **kwargs: _async_value({}),
    )
    monkeypatch.setattr(bootstrap_module, "GatewayTaskModule", lambda **kwargs: object())
    monkeypatch.setenv("CHECKPOINT_DB", str(tmp_path / "checkpoints.sqlite"))
    monkeypatch.setenv("GATEWAY_ROUTE_DB", str(tmp_path / "routes.sqlite"))
    monkeypatch.setenv("TASK_DB", str(tmp_path / "tasks.sqlite"))
    monkeypatch.setenv("REVIEW_AUDIT_DB", str(tmp_path / "reviews.sqlite"))

    async with bootstrap_module.bootstrap_application():
        assert order == []

    assert order == [
        "control",
        "review",
        "mailbox",
        "task",
        "command",
        "route",
        "checkpointer",
        "backend",
    ]
    assert open_resources == set()


async def _async_value(value: Any) -> Any:
    return value
