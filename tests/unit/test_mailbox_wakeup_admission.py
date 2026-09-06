"""Durable mailbox input admits one run, even when the input is not consumed."""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing, contextmanager
from datetime import UTC, datetime

import pytest

import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import TaskRecord
from tests.support.async_subagent_runtime import build_specs, build_test_remote_refs


@contextmanager
def runtime(tmp_path, monkeypatch, *, specs=None, unavailable_agents=None):
    path = str(tmp_path / "tasks.sqlite")
    task_store = TaskStore(path)
    mailbox_store = MailboxStore(path)
    mailbox = AgentMailbox(mailbox_store)
    control = AgentControl(
        build_specs() if specs is None else specs,
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
        mailbox=mailbox,
        unavailable_agents=unavailable_agents,
        shutdown_grace_period=0,
    )
    # Keep this regression safe against the old infinite-loop implementation.
    # The first three completion callbacks exercise real automatic wakeups.
    original_finished = control._task_runtime.on_run_finished
    callbacks = 0

    def bounded_finished(task_id, run_task):
        nonlocal callbacks
        callbacks += 1
        if callbacks <= 3:
            original_finished(task_id, run_task)

    monkeypatch.setattr(control._task_runtime, "on_run_finished", bounded_finished)
    try:
        yield control, task_store, mailbox_store, mailbox
    finally:
        task_store.close()
        mailbox_store.close()


def seed(task_store, mailbox, *, agent_name="background_research"):
    now = datetime.now(UTC)
    task_store.save_task(
        TaskRecord(
            task_id="task-1",
            agent_name=agent_name,
            state="failed",
            thread_id="task-1",
            parent_task_id=None,
            root_task_id="task-1",
            depth=1,
            created_at=now,
            updated_at=now,
        )
    )
    mailbox.publish_input(
        recipient_task_id="task-1",
        recipient_thread_id="task-1",
        content="unconsumed input",
        idempotency_key="input-1",
    )


async def flush_callbacks():
    # Fixed event-loop turns, no wall-clock throughput assumption or open loop.
    for _ in range(80):
        await asyncio.sleep(0)


def event_counts(task_store):
    with closing(sqlite3.connect(task_store.db_path)) as conn:
        return dict(
            conn.execute(
                "SELECT event_type, COUNT(*) FROM agent_task_events GROUP BY event_type"
            ).fetchall()
        )


@pytest.mark.parametrize("target", ["missing", "unavailable", "remote"])
def test_invalid_target_is_failed_once_without_starting_run(
    tmp_path, monkeypatch, target
):
    specs = {} if target in {"missing", "unavailable"} else build_specs()
    unavailable = (
        {"background_research": "provider unavailable"}
        if target == "unavailable"
        else None
    )
    agent_name = "remote_code_wiki" if target == "remote" else "background_research"
    with runtime(
        tmp_path, monkeypatch, specs=specs, unavailable_agents=unavailable
    ) as (control, store, _, mailbox):
        seed(store, mailbox, agent_name=agent_name)

        async def scenario():
            try:
                for _ in range(3):
                    await control.wake_pending_mailbox_tasks()
                    await flush_callbacks()
                record = control.get_task_record("task-1")
                assert record.state == "failed"
                assert record.run_count == 1
                assert record.error
                assert event_counts(store).get("task.running", 0) == 0
                assert event_counts(store)["task.failed"] == 1
                assert mailbox.has_triggering_messages("task-1")
            finally:
                await control.close()

        asyncio.run(scenario())


@pytest.mark.parametrize(
    "outcome", ["compile_failure", "execute_failure", "claim_failure", "completed"]
)
def test_unconsumed_input_does_not_restart_settled_run(tmp_path, monkeypatch, outcome):
    calls = []

    class Agent:
        async def ainvoke(self, payload, *, config, version):
            calls.append("execute")
            if outcome == "claim_failure":
                assert mailbox.claim(
                    recipient_task_id="task-1", recipient_thread_id="task-1"
                )
            if outcome in {"execute_failure", "claim_failure"}:
                raise RuntimeError("execution failed")
            return {"messages": [{"role": "assistant", "content": "done"}]}

    def factory(**kwargs):
        calls.append("compile")
        if outcome == "compile_failure":
            raise RuntimeError("compilation failed")
        return Agent()

    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    with runtime(tmp_path, monkeypatch) as (control, store, _, mailbox):
        seed(store, mailbox)

        async def scenario():
            try:
                for _ in range(3):
                    await control.wake_pending_mailbox_tasks()
                    await flush_callbacks()
                record = control.get_task_record("task-1")
                assert record.state == (
                    "completed" if outcome == "completed" else "failed"
                )
                assert record.run_count == 1
                assert calls.count("compile") == 1
                assert calls.count("execute") == (
                    0 if outcome == "compile_failure" else 1
                )
                assert event_counts(store)["task.running"] == 1
                assert mailbox.has_triggering_messages("task-1")
            finally:
                await control.close()

        asyncio.run(scenario())


def test_restart_and_idempotent_replay_do_not_retry_but_committed_new_input_does(
    tmp_path, monkeypatch
):
    calls = []

    class Agent:
        async def ainvoke(self, payload, *, config, version):
            calls.append(config["configurable"]["thread_id"])
            raise RuntimeError("persistent failure")

    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: Agent()
    )
    with runtime(tmp_path, monkeypatch) as (control, store, _, mailbox):
        seed(store, mailbox)

        async def first_process():
            try:
                await control.wake_pending_mailbox_tasks()
                await flush_callbacks()
                assert control.get_task_record("task-1").run_count == 1
            finally:
                await control.close()

        asyncio.run(first_process())

    with runtime(tmp_path, monkeypatch) as (control, _, _, mailbox):

        async def restarted_process():
            try:
                await control.wake_pending_mailbox_tasks()
                await control.send_task_input(
                    "task-1", "unconsumed input", idempotency_key="input-1"
                )
                await flush_callbacks()
                assert control.get_task_record("task-1").run_count == 1
                assert len(calls) == 1
                # A crash after durable publish but before schedule must not lose new input.
                mailbox.publish_input(
                    recipient_task_id="task-1",
                    recipient_thread_id="task-1",
                    content="new input",
                    idempotency_key="input-2",
                )
                await control.wake_pending_mailbox_tasks()
                await flush_callbacks()
                assert control.get_task_record("task-1").run_count == 2
                assert len(calls) == 2
            finally:
                await control.close()

        asyncio.run(restarted_process())


def test_new_input_during_run_is_woken_by_completion_callback(tmp_path, monkeypatch):
    started = asyncio.Event()
    finish = asyncio.Event()
    calls = []

    class Agent:
        async def ainvoke(self, payload, *, config, version):
            calls.append(payload)
            if len(calls) == 1:
                started.set()
                await finish.wait()
            return {"messages": [{"role": "assistant", "content": "done"}]}

    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: Agent()
    )
    with runtime(tmp_path, monkeypatch) as (control, store, _, mailbox):
        seed(store, mailbox)

        async def scenario():
            try:
                await control.wake_pending_mailbox_tasks()
                await asyncio.wait_for(started.wait(), 2)
                await control.send_task_input(
                    "task-1", "arrived during run", idempotency_key="input-2"
                )
                assert control.get_task_record("task-1").run_count == 1
                finish.set()
                await flush_callbacks()
                assert control.get_task_record("task-1").state == "completed"
                assert control.get_task_record("task-1").run_count == 2
                assert len(calls) == 2
            finally:
                await control.close()

        asyncio.run(scenario())


@pytest.mark.parametrize("explicit", [True, False])
def test_cancelled_or_interrupted_run_requires_new_input(
    tmp_path, monkeypatch, explicit
):
    started = asyncio.Event()
    calls = []

    class Agent:
        async def ainvoke(self, payload, *, config, version):
            calls.append(payload)
            if len(calls) == 1:
                started.set()
                await asyncio.Event().wait()
            return {"messages": [{"role": "assistant", "content": "done"}]}

    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: Agent()
    )
    with runtime(tmp_path, monkeypatch) as (control, store, _, mailbox):
        seed(store, mailbox)

        async def scenario():
            try:
                await control.wake_pending_mailbox_tasks()
                await asyncio.wait_for(started.wait(), 2)
                if explicit:
                    await control.cancel_task("task-1")
                else:
                    run = control._task_runtime._supervisor.get_run("task-1")
                    run.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await run
                await flush_callbacks()
                await control.wake_pending_mailbox_tasks()
                await flush_callbacks()
                record = control.get_task_record("task-1")
                assert record.state == ("cancelled" if explicit else "interrupted")
                assert record.run_count == 1
                await control.send_task_input(
                    "task-1", "continue", idempotency_key="input-2"
                )
                await flush_callbacks()
                assert control.get_task_record("task-1").state == "completed"
                assert control.get_task_record("task-1").run_count == 2
            finally:
                await control.close()

        asyncio.run(scenario())


@pytest.mark.parametrize("reject", [False, True])
def test_failed_admission_commit_preserves_signal_for_recovery(
    tmp_path, monkeypatch, reject
):
    from copy import deepcopy

    from ruyi_agent.runtime.task_event_ledger import TaskEventLedger

    calls = []

    class Agent:
        async def ainvoke(self, payload, *, config, version):
            calls.append(payload)
            return {"messages": [{"role": "assistant", "content": "done"}]}

    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: Agent()
    )
    with runtime(tmp_path, monkeypatch, specs={} if reject else None) as (
        control,
        store,
        _,
        mailbox,
    ):
        seed(store, mailbox)

        async def scenario():
            try:
                before = deepcopy(control.get_task_record("task-1"))
                persisted_before = store.get_task("task-1")

                def fail_commit(*args, **kwargs):
                    raise OSError("admission commit failed")

                with monkeypatch.context() as fault:
                    fault.setattr(TaskEventLedger, "update_task", fail_commit)
                    with pytest.raises(OSError, match="admission commit failed"):
                        await control.wake_pending_mailbox_tasks()
                    await flush_callbacks()
                    assert control.get_task_record("task-1") == before
                    assert store.get_task("task-1") == persisted_before
                    assert event_counts(store) == {}
                    assert mailbox.has_triggering_messages("task-1")
                    assert calls == []

                # Retrying admission after the storage fault must see the same input.
                await control.wake_pending_mailbox_tasks()
                await flush_callbacks()
                after = control.get_task_record("task-1")
                assert after.run_count == 1
                assert after.mailbox_wakeup_sequence > before.mailbox_wakeup_sequence
                assert after.state == ("failed" if reject else "completed")
                assert len(calls) == (0 if reject else 1)
                assert event_counts(store) == (
                    {"task.failed": 1}
                    if reject
                    else {"task.running": 1, "task.completed": 1}
                )
                await control.wake_pending_mailbox_tasks()
                await flush_callbacks()
                assert control.get_task_record("task-1").run_count == 1
            finally:
                await control.close()

        asyncio.run(scenario())


def test_rejected_new_run_preserves_previous_completion_and_parent_notification(
    tmp_path, monkeypatch
):
    class Agent:
        async def ainvoke(self, payload, *, config, version):
            return {"messages": [{"role": "assistant", "content": "original result"}]}

    monkeypatch.setattr(
        agent_factory_module, "create_runtime_agent", lambda **kwargs: Agent()
    )
    with runtime(tmp_path, monkeypatch) as (control, store, _, mailbox):

        async def first_process():
            try:
                await control.spawn_task(
                    "background_research",
                    "first run",
                    task_id="task-1",
                    parent_thread_id="parent-thread",
                )
                await flush_callbacks()
                assert control.get_task_record("task-1").state == "completed"
                assert control.get_task_record("task-1").run_count == 1
            finally:
                await control.close()

        asyncio.run(first_process())
        prior_outbox = store.list_settled_outbox()[0]
        assert prior_outbox["settled_status"] == "completed"
        with closing(sqlite3.connect(store.db_path)) as conn:
            prior_events = conn.execute(
                "SELECT * FROM agent_task_events WHERE task_id = 'task-1' AND run_count = 1"
            ).fetchall()

    with runtime(tmp_path, monkeypatch, specs={}) as (control, store, _, mailbox):

        async def second_process():
            try:
                await control.send_task_input(
                    "task-1", "new input", idempotency_key="new-input"
                )
                await flush_callbacks()
                record = control.get_task_record("task-1")
                assert record.state == "failed"
                assert record.run_count == 2
                assert record.result is None
                rows = {row["run_count"]: row for row in store.list_settled_outbox()}
                assert rows[1] == prior_outbox
                assert rows[2]["settled_status"] == "failed"
                assert rows[2]["outbox_key"] != rows[1]["outbox_key"]
                with closing(sqlite3.connect(store.db_path)) as conn:
                    assert (
                        conn.execute(
                            "SELECT * FROM agent_task_events WHERE task_id = 'task-1' AND run_count = 1"
                        ).fetchall()
                        == prior_events
                    )
                    assert conn.execute(
                        "SELECT event_type FROM agent_task_events WHERE task_id = 'task-1' AND run_count = 2"
                    ).fetchall() == [("task.failed",)]
                notifications = mailbox.claim(
                    recipient_task_id="parent-task", recipient_thread_id="parent-thread"
                )
                assert {
                    (message.run_count, message.status) for message in notifications
                } == {(1, "completed"), (2, "failed")}
                assert (
                    next(
                        message.content
                        for message in notifications
                        if message.run_count == 1
                    )
                    == "original result"
                )
            finally:
                await control.close()

        asyncio.run(second_process())
