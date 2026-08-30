from __future__ import annotations

import asyncio
from dataclasses import replace
import pytest

from ruyi_agent.task_models import TaskRecord
from ruyi_agent.task_models import PublishedArtifact
import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.config.loader import LocalWorkerSpec
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.runtime.skills.sync import SkillSyncer

from tests.support.async_subagent_runtime import (
    FakeAgentFactory,
    StreamingAgent,
    StreamingAgentFactory,
    InterruptingAgentFactory,
    SnapshotInterruptAgentFactory,
    ReviewRemoteA2AClient,
    UploadBackend,
    build_specs,
    write_test_skill,
    build_test_remote_refs,
    wait_for_task_state,
)


def test_spawn_task_materializes_skill_view_and_passes_it_to_agent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    frontend = write_test_skill(tmp_path, "frontend")
    backend = UploadBackend()
    specs = {
        "main": LocalWorkerSpec(
            name="main",
            description="main helper",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=["frontend"],
        )
    }

    async def run() -> TaskRecord:
        control = async_subagent_runtime.AgentControl(
            specs,
            {},
            checkpointer=object(),
            backend=backend,
            skill_catalog={"frontend": frontend},
            skill_syncer=SkillSyncer(
                backend=backend,
                views_root="/.ruyi_agent/runtime/skill-views",
            ),
        )
        record = await control.spawn_task("main", "use frontend skill")
        await wait_for_task_state(
            control,
            record.task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )
        return control.get_task_record(record.task_id)

    record = asyncio.run(run())

    assert record.effective_skill_names == ("frontend",)
    assert record.skill_view_path is not None
    assert record.skill_view_hash is not None
    assert f"{record.skill_view_path}/frontend/SKILL.md" in backend.files
    assert (
        factory.created[0].calls[0]["config"]["configurable"]["skill_view_path"]
        == record.skill_view_path
    )


def test_spawn_task_inherits_parent_effective_skills(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    frontend = write_test_skill(tmp_path, "frontend")
    backend = UploadBackend()
    specs = {
        "main": LocalWorkerSpec(
            name="main",
            description="main helper",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=["frontend"],
        ),
        "worker": LocalWorkerSpec(
            name="worker",
            description="worker helper",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills="inherit",
        ),
    }

    async def run() -> tuple[TaskRecord, TaskRecord]:
        control = async_subagent_runtime.AgentControl(
            specs,
            {},
            checkpointer=object(),
            backend=backend,
            skill_catalog={"frontend": frontend},
            skill_syncer=SkillSyncer(
                backend=backend,
                views_root="/.ruyi_agent/runtime/skill-views",
            ),
        )
        parent = await control.spawn_task("main", "parent")
        await wait_for_task_state(
            control,
            parent.task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )
        child = await control.spawn_task(
            "worker",
            "child",
            parent_task_id=parent.task_id,
        )
        await wait_for_task_state(
            control,
            child.task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )
        return (
            control.get_task_record(parent.task_id),
            control.get_task_record(child.task_id),
        )

    parent, child = asyncio.run(run())

    assert child.effective_skill_names == parent.effective_skill_names == ("frontend",)
    assert child.skill_view_path == parent.skill_view_path
    assert child.skill_view_hash == parent.skill_view_hash


def test_spawn_wait_and_check_agent_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测完整生命周期：本地 async subagent 的最核心价值就是统一的 spawn/check/wait 语义。
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=ReviewRemoteA2AClient(),  # type: ignore[arg-type]
    )

    async def scenario() -> tuple[str, TaskRecord, TaskRecord]:
        started = await control.spawn_task("background_research", "research this")
        task_id = started.task_id
        status_before = control.get_task_record(task_id)
        status_after = await wait_for_task_state(
            control,
            task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )
        return task_id, status_before, status_after

    task_id, status_before, status_after = asyncio.run(scenario())

    assert status_before.agent_name == "background_research"
    assert status_before.state in {"pending", "running", "completed"}
    assert status_after.state == "completed"
    assert status_after.result == "done"
    assert len(factory.created) == 1
    configurable = factory.created[0].calls[0]["config"]["configurable"]
    assert configurable["thread_id"] == task_id
    assert configurable["task_id"] == task_id
    assert configurable["parent_task_id"] is None
    assert configurable["root_task_id"] == task_id
    assert configurable["delegation_depth"] == 1


def test_local_runtime_consumes_astream_and_publishes_safe_deltas(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = StreamingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)

    async def scenario() -> tuple[
        TaskRecord,
        list[object],
        StreamingAgent,
    ]:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        control = async_subagent_runtime.AgentControl(
            build_specs(),
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        try:
            record = await control.spawn_task("background_research", "stream this")
            while not factory.created:
                await asyncio.sleep(0)
            agent = factory.created[0]
            await agent.started.wait()
            stream = control.open_local_task_event_stream(
                record.task_id,
                run_count=record.run_count,
                last_event_id=None,
            )
            events: list[object] = [await anext(stream)]
            agent.release.set()
            events.extend(
                [await anext(stream), await anext(stream), await anext(stream)]
            )
            await wait_for_task_state(
                control,
                record.task_id,
                states={"completed", "failed", "cancelled", "interrupted"},
            )
            await stream.aclose()
            return control.get_task_record(record.task_id), events, agent
        finally:
            await control.close()
            store.close()

    record, events, agent = asyncio.run(scenario())

    assert record.state == "completed"
    assert record.result == "stream done"
    assert [event.event_type for event in events] == [
        "task.snapshot",
        "assistant.delta",
        "task.completed",
        "stream.end",
    ]
    assert events[1].data == {"content": "live "}
    assert agent.stream_calls[0]["stream_mode"] == ["messages", "values"]
    assert agent.stream_calls[0]["version"] == "v2"
    assert agent.invoke_calls == 0


def test_local_runtime_uses_checkpoint_when_stream_has_no_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = StreamingAgentFactory(omit_values=True)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)

    async def scenario() -> tuple[TaskRecord, StreamingAgent]:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        control = async_subagent_runtime.AgentControl(
            build_specs(),
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        try:
            record = await control.spawn_task("background_research", "stream this")
            while not factory.created:
                await asyncio.sleep(0)
            agent = factory.created[0]
            await agent.started.wait()
            agent.release.set()
            await wait_for_task_state(
                control,
                record.task_id,
                states={"completed", "failed", "cancelled", "interrupted"},
            )
            return control.get_task_record(record.task_id), agent
        finally:
            await control.close()
            store.close()

    record, agent = asyncio.run(scenario())
    assert record.state == "completed"
    assert record.result == "snapshot done"
    assert agent.state_calls >= 1
    assert agent.invoke_calls == 0


def test_local_runtime_does_not_invoke_again_after_stream_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = StreamingAgentFactory(fail=True)
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)

    async def scenario() -> tuple[TaskRecord, StreamingAgent]:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        control = async_subagent_runtime.AgentControl(
            build_specs(),
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        try:
            record = await control.spawn_task("background_research", "stream this")
            while not factory.created:
                await asyncio.sleep(0)
            agent = factory.created[0]
            await agent.started.wait()
            agent.release.set()
            await wait_for_task_state(
                control,
                record.task_id,
                states={"completed", "failed", "cancelled", "interrupted"},
            )
            return control.get_task_record(record.task_id), agent
        finally:
            await control.close()
            store.close()

    record, agent = asyncio.run(scenario())
    assert record.state == "failed"
    assert "stream exploded" in (record.error or "")
    assert agent.invoke_calls == 0


def test_register_artifact_attaches_manifest_to_task_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        record = await control.spawn_task("background_research", "produce report")
        while not factory.compile_kwargs:
            await asyncio.sleep(0)
        register_artifact = factory.compile_kwargs[0]["register_artifact"]
        assert callable(register_artifact)
        artifact = register_artifact(
            task_id=record.task_id,
            artifact={
                "path": "/workspace/out/report.html",
                "name": "report.html",
                "caption": None,
                "content_type": "text/html",
                "size": 12,
            },
        )
        await wait_for_task_state(
            control,
            record.task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )
        assert artifact["artifact_id"].startswith("art_")
        return control.get_task_record(record.task_id)

    record = asyncio.run(scenario())

    assert record.artifacts == [
        PublishedArtifact(
            artifact_id=record.artifacts[0].artifact_id,
            path="/workspace/out/report.html",
            name="report.html",
            caption=None,
            content_type="text/html",
            size=12,
            run_count=1,
        )
    ]


def test_wait_agent_reports_worker_human_review_without_tool_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = InterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[TaskRecord, TaskRecord]:
        record = await control.spawn_task("background_research", "needs review")
        await wait_for_task_state(
            control,
            record.task_id,
            states={"waiting_for_human", "completed", "failed"},
        )
        waiting = control.get_task_record(record.task_id)
        assert waiting.state == "waiting_for_human"
        status = replace(waiting)
        pending = control.get_task_record(record.task_id)
        updated = await control.submit_review_decision(
            pending.pending_review["review_id"],
            [{"type": "approve"}],
            wait=True,
        )
        return status, updated

    status, record = asyncio.run(scenario())

    assert status.state == "waiting_for_human"
    assert record.state == "completed"
    assert record.result == "resumed done"
    assert record.pending_review is None
    assert len(factory.created[0].calls) == 2
    assert factory.created[0].calls[1]["payload"].resume == {
        "decisions": [{"type": "approve"}]
    }


def test_worker_interrupts_are_read_from_state_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = SnapshotInterruptAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[TaskRecord, TaskRecord]:
        record = await control.spawn_task("background_research", "fetch hn")
        await wait_for_task_state(
            control,
            record.task_id,
            states={"waiting_for_human", "completed", "failed"},
        )
        waiting = control.get_task_record(record.task_id)
        assert waiting.state == "waiting_for_human"
        status = replace(waiting)
        assert status.state == "waiting_for_human"
        pending = control.get_task_record(record.task_id)
        updated = await control.submit_review_decision(
            pending.pending_review["review_id"],
            [{"type": "approve"}],
            wait=True,
        )
        return status, updated

    status, record = asyncio.run(scenario())

    assert status.state == "waiting_for_human"
    assert record.state == "completed"
    assert record.result == "snapshot resumed"
    assert record.pending_review is None
    assert len(factory.created[0].state_calls) >= 1


def test_wait_agent_resolves_human_review_from_config_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = InterruptingAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> TaskRecord:
        record = await control.spawn_task("background_research", "needs review")

        async def resolve_pending_reviews() -> bool:
            pending = control.get_task_record(record.task_id)
            if pending.state != "waiting_for_human" or pending.pending_review is None:
                return False
            await control.submit_review_decision(
                pending.pending_review["review_id"],
                [{"type": "approve"}],
            )
            return True

        await wait_for_task_state(
            control,
            record.task_id,
            states={"waiting_for_human"},
        )
        assert await resolve_pending_reviews() is True
        return await wait_for_task_state(
            control,
            record.task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )

    status = asyncio.run(scenario())

    assert status.state == "completed"
    assert status.result == "resumed done"
    assert len(factory.created[0].calls) == 2
