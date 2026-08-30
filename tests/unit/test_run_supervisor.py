from __future__ import annotations

import asyncio
from copy import copy, deepcopy
from functools import wraps
from pathlib import Path
from typing import Any

import pytest

import ruyi_agent.runtime.delegation.run_supervisor as supervisor_module
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.runtime.delegation.contracts import (
    TaskAlreadyRunningError,
    UnknownWorkerTaskError,
)
from ruyi_agent.runtime.delegation.run_supervisor import (
    InvalidRuntimePermitError,
    RuntimeClosingError,
    RunSupervisor,
)
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.delegation.remote_port import RemoteTaskPort
from ruyi_agent.runtime.delegation.local_executor import LocalTaskExecutor
from ruyi_agent.runtime.task_event_ledger import TaskEventLedger
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from tests.support.async_subagent_runtime import (
    FakeAgentFactory,
    InterruptingAgentFactory,
    ResumeBlockingInterruptingAgentFactory,
    build_specs,
    build_test_remote_refs,
    wait_for_task_state,
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

    async def ainvoke(
        self, payload: Any, *, config: Any, version: str
    ) -> dict[str, Any]:
        del payload, config, version
        self.started.set()
        await self.release.wait()
        return {"messages": [{"role": "assistant", "content": "drained"}]}


class MultiBlockingAgent:
    def __init__(self, expected: int) -> None:
        self.expected = expected
        self.started_count = 0
        self.all_started = asyncio.Event()

    async def ainvoke(
        self, payload: Any, *, config: Any, version: str
    ) -> dict[str, Any]:
        del payload, config, version
        self.started_count += 1
        if self.started_count == self.expected:
            self.all_started.set()
        await asyncio.Event().wait()
        raise AssertionError("cancelled run must not return")


class ParentSpawningAgent:
    def __init__(self) -> None:
        self.control: AgentControl | None = None
        self.started = asyncio.Event()
        self.spawn = asyncio.Event()
        self.rejected = asyncio.Event()
        self.child_payload_executed = False

    async def ainvoke(
        self, payload: Any, *, config: Any, version: str
    ) -> dict[str, Any]:
        del config, version
        messages = payload.get("messages", [])
        if messages and messages[0].get("content") == "child":
            self.child_payload_executed = True
            return {"messages": [{"role": "assistant", "content": "child"}]}
        self.started.set()
        await self.spawn.wait()
        assert self.control is not None
        try:
            await self.control.spawn_task("background_research", "child")
        except RuntimeClosingError:
            self.rejected.set()
        return {"messages": [{"role": "assistant", "content": "parent done"}]}


class HangingRemoteA2AClient:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.calls = 0

    async def create_task(self, remote_ref: Any, **kwargs: Any) -> dict[str, Any]:
        del remote_ref, kwargs
        self.calls += 1
        self.started.set()
        await asyncio.Event().wait()
        raise AssertionError("shutdown must cancel remote allocation")


class StateReadBlockingAgent:
    def __init__(self) -> None:
        self.state_read_started = asyncio.Event()

    async def astream(self, *args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        if False:
            yield None

    async def aget_state(self, config: Any) -> Any:
        del config
        self.state_read_started.set()
        await asyncio.Event().wait()


class HangingRemoteOperationClient:
    def __init__(self, operation: str) -> None:
        self.operation = operation
        self.started = asyncio.Event()
        self.get_calls = 0

    async def create_task(self, remote_ref: Any, **kwargs: Any) -> dict[str, Any]:
        del kwargs
        status = "waiting_for_human" if self.operation == "review" else "completed"
        if self.operation == "cancel":
            status = "running"
        payload: dict[str, Any] = {
            "task_id": f"upstream-{self.operation}",
            "agent_name": remote_ref.name,
            "status": status,
            "last_result": "ready" if status == "completed" else None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-08-30T00:00:00Z",
            "updated_at": "2026-08-30T00:00:01Z",
        }
        if status == "waiting_for_human":
            payload["pending_review"] = {
                "review_id": "review-uncertain",
                "action_requests": [],
                "review_configs": [],
            }
        return payload

    async def get_task(self, remote_ref: Any, *, task_id: str) -> dict[str, Any]:
        self.get_calls += 1
        status = "cancelled" if self.operation == "cancel" else "completed"
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": status,
            "last_result": "reconciled" if status == "completed" else None,
            "error": None,
            "run_count": 2,
            "created_at": "2026-08-30T00:00:00Z",
            "updated_at": "2026-08-30T00:00:02Z",
        }

    async def send_input(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        assert self.operation == "send"
        await self._hang()
        raise AssertionError

    async def submit_review_decision(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        assert self.operation == "review"
        await self._hang()
        raise AssertionError

    async def cancel_task(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        assert self.operation == "cancel"
        await self._hang()
        raise AssertionError

    async def _hang(self) -> None:
        self.started.set()
        await asyncio.Event().wait()


class CancelledRemoteCreateClient:
    def __init__(self) -> None:
        self.create_calls = 0

    async def create_task(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        self.create_calls += 1
        raise asyncio.CancelledError


def _durable_control(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    factory: Any,
    *,
    mailbox: bool = False,
    shutdown_grace_period: float = 0,
) -> tuple[AgentControl, TaskStore, MailboxStore | None]:
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    mailbox_store = MailboxStore(str(tmp_path / "tasks.sqlite")) if mailbox else None
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
        mailbox=AgentMailbox(mailbox_store) if mailbox_store is not None else None,
        shutdown_grace_period=shutdown_grace_period,
    )
    return control, task_store, mailbox_store


def _direct_supervisor(
    *, shutdown_grace_period: float = 0
) -> tuple[TaskManager, RunSupervisor]:
    manager = TaskManager()
    return manager, RunSupervisor(
        manager,
        shutdown_grace_period=shutdown_grace_period,
    )


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
    monkeypatch.setattr(
        TaskEventLedger,
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
        await wait_for_task_state(control, created.task_id, states={"completed"})
        before = deepcopy(control.get_task_record(created.task_id))
        persisted_before = store.get_task(created.task_id)

        monkeypatch.setattr(
            TaskEventLedger,
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
        waiting = await wait_for_task_state(
            control, created.task_id, states={"waiting_for_human"}
        )
        assert waiting.pending_review is not None
        review_id = waiting.pending_review["review_id"]
        before = deepcopy(waiting)
        persisted_before = store.get_task(created.task_id)

        monkeypatch.setattr(
            TaskEventLedger,
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
    finally:
        await control.close()
        if mailbox_store is not None:
            mailbox_store.close()
        store.close()


@async_test
async def test_supervisor_serializes_competing_schedules() -> None:
    manager, supervisor = _direct_supervisor()
    manager.create_task_record(
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
        manager.mark_completed("same-task", "done")

    try:
        results = await asyncio.gather(
            supervisor.schedule("same-task", runner),
            supervisor.schedule("same-task", runner),
            return_exceptions=True,
        )
        assert sum(isinstance(item, asyncio.Task) for item in results) == 1
        assert sum(isinstance(item, TaskAlreadyRunningError) for item in results) == 1
        await started.wait()
        release.set()
        while manager.get_task("same-task").state != "completed":
            await asyncio.sleep(0)
    finally:
        await supervisor.close()


@async_test
async def test_close_allows_natural_completion_within_grace_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = ReleasableAgent()
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: agent
    )
    control = AgentControl(
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
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: agent
    )
    control = AgentControl(
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


@async_test
async def test_shutdown_rejects_concurrent_new_schedule_without_payload_execution() -> (
    None
):
    manager, supervisor = _direct_supervisor()
    for task_id in ("active", "late"):
        manager.create_task_record(
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
            manager.mark_interrupted(
                "active",
                "Task interrupted: runtime shutdown",
            )
            raise

    async def late_runner() -> None:
        nonlocal late_executed
        late_executed = True

    await supervisor.schedule("active", active_runner)
    await started.wait()
    closing = asyncio.create_task(supervisor.close())
    while supervisor.is_accepting:
        await asyncio.sleep(0)

    with pytest.raises(RuntimeClosingError, match="closing"):
        await supervisor.schedule("late", late_runner)
    await closing

    assert late_executed is False
    assert manager.get_task("late").state == "pending"
    assert manager.get_task("late").run_count == 0
    assert manager.get_task("active").state == "interrupted"


@async_test
async def test_detached_parent_cannot_spawn_child_after_closing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = ParentSpawningAgent()
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: agent
    )
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0.2,
    )
    agent.control = control
    parent = await control.spawn_task("background_research", "parent")
    await agent.started.wait()

    closing = asyncio.create_task(control.close())
    await asyncio.sleep(0)
    agent.spawn.set()
    await agent.rejected.wait()
    await closing

    assert agent.child_payload_executed is False
    assert control.get_task_record(parent.task_id).state == "completed"
    assert len(control.list_persisted_task_records()) == 1


@async_test
async def test_review_wait_does_not_hold_mutation_and_close_cancels_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ResumeBlockingInterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    record = await control.spawn_task("background_research", "review")
    waiting = await wait_for_task_state(
        control, record.task_id, states={"waiting_for_human"}
    )
    assert waiting.pending_review is not None
    review_id = waiting.pending_review["review_id"]

    submit = asyncio.create_task(
        control.submit_review_decision(
            review_id,
            [{"type": "approve"}],
            wait=True,
        )
    )
    agent = factory.created[0]
    await agent.resume_started.wait()

    await asyncio.wait_for(control.close(), timeout=0.5)
    result = await asyncio.wait_for(submit, timeout=0.5)
    assert result.state == "interrupted"


@async_test
async def test_close_cancels_tracked_hanging_remote_allocation() -> None:
    client = HangingRemoteA2AClient()
    control = AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,
        shutdown_grace_period=0,
    )
    allocation = asyncio.create_task(
        control.spawn_task(
            "remote_code_wiki",
            "hang",
            task_id="remote-hang",
        )
    )
    await client.started.wait()

    await asyncio.wait_for(control.close(), timeout=0.5)
    with pytest.raises(asyncio.CancelledError):
        await allocation
    interrupted = control.get_task_record("remote-hang")
    assert interrupted.state == "interrupted"
    assert interrupted.external_operation == "create"
    assert interrupted.external_operation_identity == "remote-hang"
    assert interrupted.external_outcome_uncertain is True


@async_test
async def test_close_breaks_remote_operation_and_budget_lock_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    client = HangingRemoteA2AClient()
    control = AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,
        shutdown_grace_period=0,
    )
    root = await control.spawn_task("background_research", "root", task_id="root")
    await wait_for_task_state(control, root.task_id, states={"completed"})
    first = asyncio.create_task(
        control.spawn_task(
            "remote_code_wiki",
            "first",
            task_id="remote-first",
            parent_task_id=root.task_id,
        )
    )
    await client.started.wait()
    second = asyncio.create_task(
        control.spawn_task(
            "remote_code_wiki",
            "second",
            task_id="remote-second",
            parent_task_id=root.task_id,
        )
    )
    await asyncio.sleep(0)

    await asyncio.wait_for(control.close(), timeout=0.5)
    with pytest.raises(asyncio.CancelledError):
        await first
    with pytest.raises(asyncio.CancelledError):
        await second
    assert client.calls == 1
    with pytest.raises(UnknownWorkerTaskError):
        control.get_task_record("remote-second")


@async_test
async def test_cancel_task_is_tracked_through_concurrent_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = MultiBlockingAgent(expected=1)
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: agent
    )
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    webhook_started = asyncio.Event()

    async def blocked_webhook(self: RemoteTaskPort, task_id: str) -> None:
        del self, task_id
        webhook_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(RemoteTaskPort, "send_settled_webhook", blocked_webhook)
    record = await control.spawn_task("background_research", "cancel")
    await agent.all_started.wait()
    cancelling = asyncio.create_task(control.cancel_task(record.task_id))
    await webhook_started.wait()

    await asyncio.wait_for(control.close(), timeout=0.5)
    with pytest.raises(asyncio.CancelledError):
        await cancelling
    assert control.get_task_record(record.task_id).state == "cancelled"


@async_test
async def test_terminal_state_does_not_release_run_before_webhook_finishes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    webhook_started = asyncio.Event()

    async def blocked_webhook(self: RemoteTaskPort, task_id: str) -> None:
        del self, task_id
        webhook_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(RemoteTaskPort, "send_settled_webhook", blocked_webhook)
    record = await control.spawn_task("background_research", "first")
    await webhook_started.wait()
    assert control.get_task_record(record.task_id).state == "completed"

    cancelled = await control.cancel_task(record.task_id)
    assert cancelled.state == "completed"

    with pytest.raises(TaskAlreadyRunningError, match="already running"):
        await control.send_task_input(record.task_id, "second")
    await asyncio.wait_for(control.close(), timeout=0.5)

    assert len(factory.created[0].calls) == 1
    assert control.get_task_record(record.task_id).state == "completed"


@async_test
async def test_cancelling_close_caller_still_finishes_internal_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = MultiBlockingAgent(expected=1)
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: agent
    )
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0.02,
    )
    record = await control.spawn_task("background_research", "block")
    await agent.all_started.wait()
    closer = asyncio.create_task(control.close())
    await asyncio.sleep(0)
    closer.cancel()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(closer, timeout=0.5)
    assert control.get_task_record(record.task_id).state == "interrupted"
    await control.close()


@async_test
async def test_review_audit_failure_after_resume_does_not_reverse_success(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    factory = InterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )
    record = await control.spawn_task("background_research", "review")
    waiting = await wait_for_task_state(
        control, record.task_id, states={"waiting_for_human"}
    )
    assert waiting.pending_review is not None
    review_id = waiting.pending_review["review_id"]

    def fail_audit(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise OSError("audit unavailable")

    monkeypatch.setattr(LocalTaskExecutor, "audit_task_review", fail_audit)
    resumed = await control.submit_review_decision(
        review_id,
        [{"type": "approve"}],
        wait=True,
    )

    assert resumed.state == "completed"
    assert len(factory.created[0].calls) == 2
    assert "Non-authoritative review audit failed" in caplog.text
    with pytest.raises(UnknownWorkerTaskError, match="Unknown pending review"):
        await control.submit_review_decision(review_id, [{"type": "approve"}])
    assert len(factory.created[0].calls) == 2
    await control.close()


@async_test
async def test_close_waits_for_admitted_mutation_and_rejects_later_mutations() -> None:
    manager, supervisor = _direct_supervisor()
    manager.create_task_record(
        "admitted",
        "worker",
        parent_task_id=None,
        root_task_id="admitted",
        depth=1,
    )

    async def runner() -> None:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            manager.mark_interrupted(
                "admitted",
                "Task interrupted: runtime shutdown",
            )
            raise

    permit = await supervisor.acquire_mutation()
    closing = asyncio.create_task(supervisor.close())
    while supervisor.is_accepting:
        await asyncio.sleep(0)
    assert closing.done() is False

    with pytest.raises(RuntimeClosingError, match="closing"):
        await supervisor.acquire_mutation()
    with pytest.raises(RuntimeClosingError, match="closing"):
        await supervisor.schedule("admitted", runner, permit=permit)
    await supervisor.release_mutation(permit)
    await closing

    assert manager.get_task("admitted").state == "pending"


@async_test
async def test_runtime_permits_reject_forgery_cross_task_and_reuse() -> None:
    _manager, supervisor = _direct_supervisor()
    forged = object()
    with pytest.raises(InvalidRuntimePermitError, match="not issued"):
        await supervisor.release_mutation(forged)
    with pytest.raises(InvalidRuntimePermitError, match="not issued"):
        await supervisor.promote_to_operation(forged)

    permit = await supervisor.acquire_mutation()
    copied = copy(permit)
    with pytest.raises(InvalidRuntimePermitError, match="forged"):
        await supervisor.release_mutation(copied)

    async def steal() -> None:
        with pytest.raises(InvalidRuntimePermitError, match="another asyncio Task"):
            await supervisor.release_mutation(permit)

    await asyncio.create_task(steal())
    await supervisor.release_mutation(permit)
    with pytest.raises(InvalidRuntimePermitError, match="already consumed"):
        await supervisor.release_mutation(permit)
    await supervisor.close()


@pytest.mark.parametrize("kind", ["mutation", "operation"])
@async_test
async def test_permit_cleanup_survives_double_cancel_while_condition_is_locked(
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    first_acquire_started = asyncio.Event()
    first_acquire_release = asyncio.Event()
    cleanup_acquire_started = asyncio.Event()
    second_cleanup_acquire_started = asyncio.Event()
    cleanup_acquire_release = asyncio.Event()

    class GatedCondition(asyncio.Condition):
        def __init__(self) -> None:
            super().__init__()
            self.base_acquire = self.acquire
            self.acquire = self.gated_acquire
            self.first_acquire = True
            self.gate_cleanup = False
            self.cleanup_acquires = 0

        async def gated_acquire(self) -> bool:
            if self.first_acquire:
                self.first_acquire = False
                first_acquire_started.set()
                await first_acquire_release.wait()
            if self.gate_cleanup:
                self.cleanup_acquires += 1
                if self.cleanup_acquires == 1:
                    cleanup_acquire_started.set()
                elif self.cleanup_acquires == 2:
                    second_cleanup_acquire_started.set()
                await cleanup_acquire_release.wait()
            return await self.base_acquire()

    conditions: list[GatedCondition] = []

    def condition_factory() -> GatedCondition:
        condition = GatedCondition()
        conditions.append(condition)
        return condition

    monkeypatch.setattr(supervisor_module.asyncio, "Condition", condition_factory)
    _manager, supervisor = _direct_supervisor()
    ready = asyncio.Event()

    async def owner() -> None:
        mutation = await supervisor.acquire_mutation()
        permit = mutation
        if kind == "operation":
            permit = await supervisor.promote_to_operation(mutation)
        ready.set()
        try:
            await asyncio.Event().wait()
        finally:
            if kind == "operation":
                await supervisor.cleanup_operation(permit)
            else:
                await supervisor.cleanup_mutation(permit)

    task = asyncio.create_task(owner())
    await first_acquire_started.wait()
    assert len(conditions) == 1
    first_acquire_release.set()
    await ready.wait()
    conditions[0].gate_cleanup = True
    task.cancel()
    await cleanup_acquire_started.wait()
    task.cancel()
    await second_cleanup_acquire_started.wait()
    cleanup_acquire_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    new_permit = await supervisor.acquire_mutation()
    await supervisor.release_mutation(new_permit)
    await supervisor.close()


@async_test
async def test_review_wait_caller_cancel_does_not_cancel_supervised_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ResumeBlockingInterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    record = await control.spawn_task("background_research", "review")
    waiting = await wait_for_task_state(
        control, record.task_id, states={"waiting_for_human"}
    )
    review_id = waiting.pending_review["review_id"]
    observer = asyncio.create_task(
        control.submit_review_decision(
            review_id,
            [{"type": "approve"}],
            wait=True,
        )
    )
    await factory.created[0].resume_started.wait()
    observer.cancel()
    with pytest.raises(asyncio.CancelledError):
        await observer
    await control.close()
    assert control.get_task_record(record.task_id).state == "interrupted"


@pytest.mark.parametrize("explicit_cancel", [True, False])
@async_test
async def test_cancellation_during_agent_state_normalization_is_finalized(
    monkeypatch: pytest.MonkeyPatch,
    explicit_cancel: bool,
) -> None:
    agent = StateReadBlockingAgent()
    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: agent
    )
    control = AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        shutdown_grace_period=0,
    )
    record = await control.spawn_task("background_research", "normalize")
    await agent.state_read_started.wait()
    if explicit_cancel:
        result = await control.cancel_task(record.task_id)
        assert result.state == "cancelled"
    else:
        await control.close()
        assert control.get_task_record(record.task_id).state == "interrupted"
    await control.close()


@pytest.mark.parametrize("operation", ["send", "review", "cancel"])
@async_test
async def test_cancelled_remote_mutation_is_durable_and_refresh_reconciles(
    operation: str,
) -> None:
    client = HangingRemoteOperationClient(operation)
    control = AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,
    )
    record = await control.spawn_task(
        "remote_code_wiki",
        operation,
        task_id=f"remote-{operation}",
    )
    if operation == "send":
        mutation = asyncio.create_task(
            control.send_task_input(record.task_id, "continue")
        )
    elif operation == "review":
        assert record.pending_review is not None
        mutation = asyncio.create_task(
            control.submit_review_decision(
                record.pending_review["review_id"],
                [{"type": "approve"}],
            )
        )
    else:
        mutation = asyncio.create_task(control.cancel_task(record.task_id))
    await client.started.wait()
    mutation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await mutation
    uncertain = control.get_task_record(record.task_id)
    assert uncertain.state == "interrupted"
    assert uncertain.external_operation == operation
    assert uncertain.external_operation_identity
    assert uncertain.external_outcome_uncertain is True

    reconciled = await control.refresh_task(record.task_id)
    assert reconciled.state == ("cancelled" if operation == "cancel" else "completed")
    assert reconciled.external_operation is None
    assert reconciled.external_operation_identity is None
    assert reconciled.external_outcome_uncertain is False
    assert client.get_calls == 1
    await control.close()


@async_test
async def test_webhook_route_binds_uncertain_create_without_replaying() -> None:
    client = CancelledRemoteCreateClient()
    control = AgentControl(
        {},
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=client,
    )
    with pytest.raises(asyncio.CancelledError):
        await control.spawn_task(
            "remote_code_wiki",
            "uncertain",
            task_id="route-reconcile",
        )
    bound = control.ensure_remote_task_record(
        agent_name="remote_code_wiki",
        task_id="route-reconcile",
        upstream_task_id="upstream-discovered",
    )
    assert bound.upstream_task_id == "upstream-discovered"
    assert bound.thread_id == "route-reconcile"
    handled = await control.handle_remote_task_event(
        {
            "task_id": "upstream-discovered",
            "agent_name": "remote_code_wiki",
            "status": "completed",
            "last_result": "webhook reconciled",
            "error": None,
            "run_count": 1,
            "created_at": "2026-08-30T00:00:00Z",
            "updated_at": "2026-08-30T00:00:02Z",
        }
    )
    assert handled is True
    reconciled = control.get_task_record("route-reconcile")
    assert reconciled.state == "completed"
    assert reconciled.external_outcome_uncertain is False
    assert client.create_calls == 1
    await control.close()


@async_test
async def test_public_task_mutations_reject_after_close() -> None:
    control = AgentControl({}, checkpointer=object(), backend=object())
    await control.close()

    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.spawn_task("missing", "must not mutate")
    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.send_task_input("missing", "must not mutate")
    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.submit_review_decision("missing", [])
    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.cancel_task("missing")
    with pytest.raises(RuntimeClosingError, match="closing"):
        control.ensure_remote_task_record(
            agent_name="missing",
            task_id="missing",
            upstream_task_id="upstream",
        )
    with pytest.raises(RuntimeClosingError, match="closing"):
        await control.handle_remote_task_event({"task_id": "upstream"})


@async_test
async def test_unhandled_run_failure_is_consumed_and_persisted_interrupted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager, supervisor = _direct_supervisor()
    manager.create_task_record(
        "broken",
        "worker",
        parent_task_id=None,
        root_task_id="broken",
        depth=1,
    )

    async def broken_runner() -> None:
        raise RuntimeError("run exploded")

    await supervisor.schedule("broken", broken_runner)
    for _ in range(3):
        await asyncio.sleep(0)

    assert manager.get_task("broken").state == "interrupted"
    assert "Task run broken failed" in caplog.text
    await supervisor.close()


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

    def fail_interrupted(
        _manager: TaskManager,
        task_id: str,
        error: str,
    ) -> None:
        del task_id, error
        raise OSError("interrupted write failed")

    monkeypatch.setattr(TaskManager, "mark_interrupted", fail_interrupted)
    try:
        await control.close()
        assert control.get_task_record(record.task_id).state == "running"
        assert store.get_task(record.task_id).state == "running"
        assert "Failed to persist interrupted state" in caplog.text
        assert "interrupted write failed" in caplog.text
    finally:
        await control.close()
        if mailbox_store is not None:
            mailbox_store.close()
        store.close()
