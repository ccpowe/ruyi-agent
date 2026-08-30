from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
import logging
from pathlib import Path
import pytest

from ruyi_agent.task_models import TaskRecord
from ruyi_agent.storage.task_store import (
    TaskRootBudgetExceededError as MaxTasksPerRootError,
)
import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.contracts import MaxDelegationDepthError
from ruyi_agent.runtime.delegation.context import (
    CONTEXT_VERSION,
    CONTEXT_VERSION_FIELD,
    DEPTH_FIELD,
    MAX_DEPTH_FIELD,
    MAX_TASKS_PER_ROOT_FIELD,
    ROOT_ID_FIELD,
    VISITED_NODES_FIELD,
)
from ruyi_agent.runtime.delegation.policy import DelegationPolicy
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.storage.task_store import TaskStore

from tests.support.async_subagent_runtime import (
    FakeAgentFactory,
    InterruptingAgentFactory,
    BlockingAgent,
    RemoteRefreshAfterRestartA2AClient,
    ShouldNotCallRemoteA2AClient,
    SlowRemoteA2AClient,
    RecordingRemoteA2AClient,
    FailOnceRemoteCreateA2AClient,
    CancelledRemoteCreateA2AClient,
    SuccessfulRemoteCreateA2AClient,
    build_specs,
    build_test_remote_refs,
    wait_for_task_state,
)


def test_root_spawn_records_depth_1_and_self_as_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        record = await control.spawn_task("background_research", "root task")
        return await wait_for_task_state(control, record.task_id, states={"completed"})

    record = asyncio.run(scenario())

    assert record.parent_task_id is None
    assert record.root_task_id == record.task_id
    assert record.depth == 1


def test_nested_spawn_inherits_root_and_increments_depth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[
        TaskRecord,
        TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        child = await control.spawn_task(
            "background_research",
            "child task",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        return (
            await wait_for_task_state(control, root.task_id, states={"completed"}),
            await wait_for_task_state(control, child.task_id, states={"completed"}),
        )

    root, child = asyncio.run(scenario())

    assert child.parent_task_id == root.task_id
    assert child.root_task_id == root.task_id
    assert child.depth == 2


def test_spawn_agent_extracts_parent_context_from_configurable_task_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        root = await control.spawn_task("background_research", "root task")
        child = await control.spawn_task(
            "background_research",
            "child task",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        await wait_for_task_state(control, root.task_id, states={"completed"})
        return await wait_for_task_state(control, child.task_id, states={"completed"})

    child = asyncio.run(scenario())

    assert child.depth == 2
    assert child.parent_task_id == child.root_task_id


def test_spawn_agent_falls_back_to_thread_id_when_task_id_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    manager = TaskManager()
    root = manager.create_task_record(
        "root", "background_research", parent_task_id=None, root_task_id="root", depth=1
    )
    policy = DelegationPolicy(
        manager,
        node_id="node-test",
        max_delegation_depth=3,
        max_tasks_per_root=20,
        permission_default_profile="",
    )
    with caplog.at_level(
        logging.WARNING, logger="ruyi_agent.runtime.delegation.policy"
    ):
        parent = policy.extract_parent_task_record(
            {"configurable": {"thread_id": root.thread_id}}
        )

    assert parent is root
    assert "falling back to thread_id" in caplog.text


def test_spawn_rejects_when_depth_exceeds_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        max_delegation_depth=1,
    )

    async def scenario() -> tuple[MaxDelegationDepthError, list[TaskRecord]]:
        root = await control.spawn_task("background_research", "root task")
        with pytest.raises(MaxDelegationDepthError) as exc_info:
            await control.spawn_task(
                "background_research",
                "child task",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
        await wait_for_task_state(control, root.task_id, states={"completed"})
        return exc_info.value, control.list_persisted_task_records()

    denied, records = asyncio.run(scenario())

    assert denied.current_depth == 2
    assert denied.max_depth == 1
    assert len(records) == 1


def test_spawn_rejects_when_root_task_budget_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        max_tasks_per_root=2,
    )

    async def scenario() -> tuple[MaxTasksPerRootError, list[TaskRecord]]:
        root = await control.spawn_task("background_research", "root task")
        first_child = await control.spawn_task(
            "background_research",
            "first child",
            parent_task_id=root.task_id,
        )
        with pytest.raises(MaxTasksPerRootError) as exc_info:
            await control.spawn_task(
                "background_research",
                "second child",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
        await wait_for_task_state(control, root.task_id, states={"completed"})
        await wait_for_task_state(control, first_child.task_id, states={"completed"})
        return exc_info.value, control.list_persisted_task_records()

    denied, records = asyncio.run(scenario())

    assert denied.current_count == 2
    assert denied.max_tasks_per_root == 2
    assert len(records) == 2


def test_remote_ref_spawn_also_enforces_depth_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=ShouldNotCallRemoteA2AClient(),
        max_delegation_depth=1,
    )

    async def scenario() -> MaxDelegationDepthError:
        root = await control.spawn_task("background_research", "root task")
        with pytest.raises(MaxDelegationDepthError) as exc_info:
            await control.spawn_task(
                "remote_code_wiki",
                "remote child",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
        await wait_for_task_state(control, root.task_id, states={"completed"})
        return exc_info.value

    denied = asyncio.run(scenario())

    assert denied.current_depth == 2
    assert denied.max_depth == 1


def test_remote_child_spawn_injects_delegation_context_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    a2a_client = RecordingRemoteA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,
        node_id="node-a",
    )

    async def scenario() -> TaskRecord:
        root = await control.spawn_task("background_research", "root task")
        child = await control.spawn_task(
            "remote_code_wiki",
            "remote child",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
            metadata={"channel": "tg"},
        )
        await wait_for_task_state(control, root.task_id, states={"completed"})
        return child

    child = asyncio.run(scenario())

    assert child.depth == 2
    assert len(a2a_client.created_metadata) == 1
    metadata = a2a_client.created_metadata[0]
    assert metadata["channel"] == "tg"
    assert metadata[CONTEXT_VERSION_FIELD] == CONTEXT_VERSION
    assert metadata[ROOT_ID_FIELD].startswith("node-a:")
    assert metadata[DEPTH_FIELD] == 2
    assert metadata[MAX_DEPTH_FIELD] == 3
    assert metadata[MAX_TASKS_PER_ROOT_FIELD] == 20
    assert metadata[VISITED_NODES_FIELD] == '["node-a"]'


def test_concurrent_remote_spawns_cannot_exceed_root_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    a2a_client = SlowRemoteA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,
        max_tasks_per_root=2,
    )

    async def scenario() -> tuple[list[object], list[TaskRecord]]:
        root = await control.spawn_task("background_research", "root task")
        results = await asyncio.gather(
            control.spawn_task(
                "remote_code_wiki",
                "remote child 1",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            ),
            control.spawn_task(
                "remote_code_wiki",
                "remote child 2",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            ),
            return_exceptions=True,
        )
        await wait_for_task_state(control, root.task_id, states={"completed"})
        return results, control.list_persisted_task_records()

    results, records = asyncio.run(scenario())

    assert sum(isinstance(result, TaskRecord) for result in results) == 1
    failures = [
        result for result in results if isinstance(result, MaxTasksPerRootError)
    ]
    assert len(failures) == 1
    assert failures[0].current_count == 2
    assert a2a_client.create_calls == 1
    assert len(records) == 2


def test_restart_budget_uses_full_persisted_tree_not_lazy_task_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    db_path = tmp_path / "tasks.sqlite"
    first_store = TaskStore(str(db_path))
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=first_store,
        max_delegation_depth=4,
        max_tasks_per_root=3,
        node_id="node-a",
    )

    async def create_tree() -> tuple[str, str]:
        root = await first_control.spawn_task(
            "background_research", "root", task_id="root"
        )
        child = await first_control.spawn_task(
            "background_research",
            "child",
            task_id="child",
            parent_task_id=root.task_id,
        )
        grandchild = await first_control.spawn_task(
            "background_research",
            "grandchild",
            task_id="grandchild",
            parent_task_id=child.task_id,
        )
        for record in (root, child, grandchild):
            await wait_for_task_state(
                first_control, record.task_id, states={"completed"}
            )
        return root.task_id, child.task_id

    root_task_id, child_task_id = asyncio.run(create_tree())
    first_store.close()

    second_store = TaskStore(str(db_path))
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=second_store,
        max_delegation_depth=4,
        max_tasks_per_root=3,
        node_id="node-a",
    )

    async def attempt_after_lazy_restore() -> None:
        second_control.get_task_record(root_task_id)
        second_control.get_task_record(child_task_id)
        assert len(second_control.list_persisted_task_records()) == 3
        await second_control.spawn_task(
            "background_research",
            "must be denied",
            task_id="fourth",
            parent_task_id=root_task_id,
        )

    try:
        with pytest.raises(MaxTasksPerRootError) as exc_info:
            asyncio.run(attempt_after_lazy_restore())
        assert exc_info.value.current_count == 3
        assert second_store.count_tasks_under_root(root_task_id) == 3
    finally:
        second_store.close()


def test_independent_agent_controls_atomically_compete_for_last_task_slot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    db_path = tmp_path / "tasks.sqlite"
    first_store = TaskStore(str(db_path))
    second_store = TaskStore(str(db_path))
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        task_store=first_store,
        max_tasks_per_root=2,
        node_id="node-a",
    )
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        task_store=second_store,
        max_tasks_per_root=2,
        node_id="node-a",
    )

    async def scenario() -> list[object]:
        root = await first_control.spawn_task(
            "background_research", "root", task_id="root"
        )
        await wait_for_task_state(first_control, root.task_id, states={"completed"})
        return await asyncio.gather(
            first_control.spawn_task(
                "background_research",
                "first contender",
                task_id="child-a",
                parent_task_id=root.task_id,
            ),
            second_control.spawn_task(
                "background_research",
                "second contender",
                task_id="child-b",
                parent_task_id=root.task_id,
            ),
            return_exceptions=True,
        )

    try:
        results = asyncio.run(scenario())
        assert sum(isinstance(item, TaskRecord) for item in results) == 1
        failures = [item for item in results if isinstance(item, MaxTasksPerRootError)]
        assert len(failures) == 1
        assert failures[0].current_count == 2
        assert first_store.count_tasks_under_root("root") == 2
    finally:
        first_store.close()
        second_store.close()


def test_failed_remote_create_keeps_explainable_record_and_retries_same_slot(
    tmp_path,
) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    client = FailOnceRemoteCreateA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
        a2a_client=client,
        max_tasks_per_root=1,
        node_id="node-a",
    )

    async def scenario() -> TaskRecord:
        with pytest.raises(A2AClientError):
            await control.spawn_task(
                "remote_code_wiki",
                "remote work",
                task_id="stable-remote",
            )
        failed = store.get_task("stable-remote")
        assert failed is not None
        assert failed.state == "failed"
        assert failed.upstream_task_id is None
        assert failed.error == "Remote Gateway Task creation failed"
        assert store.count_tasks_under_root("stable-remote") == 1

        return await control.spawn_task(
            "remote_code_wiki",
            "remote work",
            task_id="stable-remote",
        )

    try:
        replayed = asyncio.run(scenario())
        assert replayed.upstream_task_id == "remote-task-replayed"
        assert store.count_tasks_under_root("stable-remote") == 1
        assert client.idempotency_keys == ["stable-remote", "stable-remote"]
    finally:
        store.close()


def test_remote_allocation_crash_window_recovers_from_persisted_placeholder(
    tmp_path,
) -> None:
    db_path = tmp_path / "tasks.sqlite"
    first_store = TaskStore(str(db_path))
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=first_store,
        a2a_client=CancelledRemoteCreateA2AClient(),
        max_tasks_per_root=1,
        node_id="node-a",
    )

    async def leave_placeholder() -> None:
        with pytest.raises(asyncio.CancelledError):
            await first_control.spawn_task(
                "remote_code_wiki",
                "remote work",
                task_id="stable-remote",
            )

    asyncio.run(leave_placeholder())
    placeholder = first_store.get_task("stable-remote")
    assert placeholder is not None
    assert placeholder.state == "interrupted"
    assert placeholder.upstream_task_id is None
    assert placeholder.external_operation == "create"
    assert placeholder.external_operation_identity == "stable-remote"
    assert placeholder.external_outcome_uncertain is True
    first_store.close()

    second_store = TaskStore(str(db_path))
    client = SuccessfulRemoteCreateA2AClient()
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(create_idempotency="ruyi_gateway_v1"),
        checkpointer=object(),
        backend=object(),
        task_store=second_store,
        a2a_client=client,
        max_tasks_per_root=1,
        node_id="node-a",
    )

    try:
        recovered = asyncio.run(
            second_control.spawn_task(
                "remote_code_wiki",
                "remote work",
                task_id="stable-remote",
            )
        )
        assert recovered.upstream_task_id == "remote-task-after-restart"
        assert recovered.thread_id == "stable-remote"
        assert second_store.count_tasks_under_root("stable-remote") == 1
        assert client.idempotency_keys == ["stable-remote"]
    finally:
        second_store.close()


def test_uncertain_remote_create_is_not_replayed_without_capability(tmp_path) -> None:
    db_path = tmp_path / "tasks.sqlite"
    first_store = TaskStore(str(db_path))
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=first_store,
        a2a_client=CancelledRemoteCreateA2AClient(),
        node_id="node-a",
    )

    async def leave_uncertain() -> None:
        with pytest.raises(asyncio.CancelledError):
            await first_control.spawn_task(
                "remote_code_wiki",
                "remote work",
                task_id="unsafe-remote",
            )

    asyncio.run(leave_uncertain())
    first_store.close()
    second_store = TaskStore(str(db_path))
    client = SuccessfulRemoteCreateA2AClient()
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=second_store,
        a2a_client=client,
        node_id="node-a",
    )
    try:
        with pytest.raises(RuntimeError, match="does not guarantee"):
            asyncio.run(
                second_control.spawn_task(
                    "remote_code_wiki",
                    "remote work",
                    task_id="unsafe-remote",
                )
            )
        assert client.idempotency_keys == []
        persisted = second_store.get_task("unsafe-remote")
        assert persisted is not None
        assert persisted.external_outcome_uncertain is True
    finally:
        second_store.close()


def test_send_input_reuses_same_agent_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测继续输入：这决定本地 async worker 是一次性任务还是可持续推进的 agent 会话。
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, TaskRecord]:
        started = await control.spawn_task("background_research", "first task")
        task_id = started.task_id
        await wait_for_task_state(control, task_id, states={"completed"})
        await control.send_task_input(task_id, "follow up")
        sent = await wait_for_task_state(control, task_id, states={"completed"})
        return task_id, sent

    task_id, sent = asyncio.run(scenario())
    assert sent.task_id == task_id
    assert sent.result == "done"

    assert len(factory.created) == 1
    assert len(factory.created[0].calls) == 2
    first_thread = factory.created[0].calls[0]["config"]["configurable"]["thread_id"]
    second_thread = factory.created[0].calls[1]["config"]["configurable"]["thread_id"]
    assert first_thread == second_thread == task_id


def test_send_input_after_failed_task_clears_previous_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingThenSuccessfulAgent:
        def __init__(self) -> None:
            self.calls = 0

        async def ainvoke(self, payload, *, config, version):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("first failure")
            return {"messages": [{"role": "assistant", "content": "recovered"}]}

    agent = FailingThenSuccessfulAgent()
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[TaskRecord, TaskRecord, TaskRecord, str | None]:
        started = await control.spawn_task("background_research", "first task")
        task_id = started.task_id
        failed = replace(await wait_for_task_state(control, task_id, states={"failed"}))
        sent = await control.send_task_input(task_id, "retry")
        running_error = control.get_task_record(task_id).error
        recovered = await wait_for_task_state(control, task_id, states={"completed"})
        return failed, sent, recovered, running_error

    failed, sent, recovered, running_error = asyncio.run(scenario())

    assert failed.state == "failed"
    assert failed.error == "RuntimeError: first failure"
    assert sent.task_id == failed.task_id
    assert running_error is None
    assert recovered.state == "completed"
    assert recovered.result == "recovered"


def test_cancel_idle_completed_task_is_noop_and_followup_reuses_same_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, TaskRecord]:
        record = await control.spawn_task("background_research", "first task")
        await wait_for_task_state(control, record.task_id, states={"completed"})
        cancelled = await control.cancel_task(record.task_id)
        assert cancelled.state == "completed"
        await control.send_task_input(record.task_id, "resume after cancel")
        sent = await wait_for_task_state(control, record.task_id, states={"completed"})
        return record.task_id, sent

    task_id, sent = asyncio.run(scenario())

    assert sent.task_id == task_id
    assert len(factory.created[0].calls) == 2
    assert factory.created[0].calls[1]["config"]["configurable"]["thread_id"] == task_id


def test_explicit_cancel_marks_task_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        record = await control.spawn_task("background_research", "block")
        await agent.started.wait()
        return await control.cancel_task(record.task_id)

    record = asyncio.run(scenario())

    assert record.state == "cancelled"


def test_cancel_waiting_for_human_marks_current_run_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = InterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        record = await control.spawn_task("background_research", "needs review")
        await wait_for_task_state(control, record.task_id, states={"waiting_for_human"})
        return await control.cancel_task(record.task_id)

    record = asyncio.run(scenario())

    assert record.state == "cancelled"
    assert record.pending_review is None


def test_passive_run_cancellation_marks_task_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        record = await control.spawn_task("background_research", "block")
        await agent.started.wait()
        await control.close()
        return control.get_task_record(record.task_id)

    record = asyncio.run(scenario())

    assert record.state == "interrupted"
    assert record.error is not None
    assert record.error.startswith("Task interrupted:")


def test_wait_agent_caller_cancellation_does_not_cancel_supervised_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> None:
        record = await control.spawn_task("background_research", "block")
        await agent.started.wait()
        observer = asyncio.create_task(
            wait_for_task_state(control, record.task_id, states={"completed"})
        )
        await asyncio.sleep(0)
        observer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await observer
        assert control.get_task_record(record.task_id).state == "running"
        await control.cancel_task(record.task_id)

    asyncio.run(scenario())


def test_task_store_restores_local_running_task_as_interrupted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    task_store.save_task(
        TaskRecord(
            task_id="local-running-task",
            agent_name="background_research",
            state="running",
            thread_id="local-running-task",
            parent_task_id=None,
            root_task_id="local-running-task",
            parent_thread_id="main-thread",
            depth=1,
            created_at=datetime(2026, 4, 23, tzinfo=UTC),
            updated_at=datetime(2026, 4, 23, tzinfo=UTC),
            run_count=1,
            route_kind="local",
        )
    )

    manager = TaskManager(task_store)
    assert manager.list_tasks() == []

    manager.load_by_parent_thread_id("main-thread")
    restored = manager.get_task("local-running-task")
    assert restored.state == "interrupted"
    assert manager.has_active_run(restored.task_id) is False
    listing = manager.list_tasks()
    assert [record.task_id for record in listing] == ["local-running-task"]
    assert listing[0].state == "interrupted"
    assert manager.event_ledger is not None
    manager.event_ledger.close()
    task_store.close()


def test_task_store_reload_preserves_live_local_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
    )

    async def scenario() -> tuple[str, str | None]:
        record = await control.spawn_task(
            "background_research",
            "block",
            parent_thread_id="main-thread",
        )
        await agent.started.wait()
        reloaded = control.get_task_record(record.task_id)
        state = reloaded.state
        error = reloaded.error
        await control.cancel_task(record.task_id)
        return state, error

    state, error = asyncio.run(scenario())

    assert state == "running"
    assert error is None
    task_store.close()


def test_remote_task_store_recovers_and_refreshes_after_restart(
    tmp_path: Path,
) -> None:
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    first_client = RemoteRefreshAfterRestartA2AClient()
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=first_client,
        task_store=task_store,
        remote_poll_interval=0.01,
    )

    async def spawn_remote() -> str:
        started = await first_control.spawn_task("remote_code_wiki", "remote task")
        return started.task_id

    task_id = asyncio.run(spawn_remote())
    stored = task_store.get_task(task_id)
    assert stored is not None
    assert stored.state == "running"
    assert stored.upstream_task_id == "remote-task-persisted"

    second_client = RemoteRefreshAfterRestartA2AClient()
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=second_client,
        task_store=task_store,
        remote_poll_interval=0.01,
    )

    async def refresh_and_continue() -> tuple[TaskRecord, TaskRecord]:
        status = replace(await second_control.refresh_task(task_id))
        sent = await second_control.send_task_input(task_id, "follow up")
        return status, sent

    status, sent = asyncio.run(refresh_and_continue())

    assert status.state == "completed"
    assert status.result == "remote persisted done"
    assert second_client.get_calls == ["remote-task-persisted"]
    assert sent.task_id == task_id
    assert second_client.sent_inputs == ["follow up"]
    persisted_after = task_store.get_task(task_id)
    assert persisted_after is not None
    assert persisted_after.run_count == 2
    assert persisted_after.result == "remote continued: follow up"
    task_store.close()
