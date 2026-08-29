from __future__ import annotations

import asyncio
import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.storage.task_store import TaskStore

from tests.support.async_subagent_runtime import (
    FakeAgentFactory,
    ResumeBlockingInterruptingAgentFactory,
    ContentAwareInterruptingAgentFactory,
    ReviewRemoteA2AClient,
    build_specs,
    build_test_remote_refs,
)


def test_child_review_is_mirrored_to_root_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if control.get_live_run(child.task_id) is not None:
            await control.get_live_run(child.task_id)
        return control.get_task_record(root.task_id), control.get_task_record(
            child.task_id
        )

    root, child = asyncio.run(scenario())

    assert child.state == "waiting_for_human"
    assert child.pending_review is not None
    assert root.pending_review is not None
    assert root.pending_review["review_id"] == child.pending_review["review_id"]
    assert root.pending_review["source_task_id"] == child.task_id

def test_submit_review_prefers_waiting_child_over_root_mirror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if control.get_live_run(child.task_id) is not None:
            await control.get_live_run(child.task_id)
        waiting_child = control.get_task_record(child.task_id)
        updated_child = await control.submit_review_decision(
            waiting_child.pending_review["review_id"],
            [{"type": "approve"}],
            wait=True,
        )
        return control.get_task_record(root.task_id), updated_child

    root, child = asyncio.run(scenario())

    assert child.state == "completed"
    assert child.result == "done: needs review"
    assert root.pending_review is None


@pytest.mark.parametrize("decision_order", [(0, 1), (1, 0)])
def test_sibling_reviews_remain_independent_for_any_decision_order(
    monkeypatch: pytest.MonkeyPatch,
    decision_order: tuple[int, int],
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[list[str], list[str], dict | None]:
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        children = []
        for _index in range(2):
            child = await control.spawn_task(
                "background_research",
                "needs review",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
            if control.get_live_run(child.task_id) is not None:
                await control.get_live_run(child.task_id)
            children.append(control.get_task_record(child.task_id))

        pending = control.list_pending_reviews(root_task_id=root.task_id)
        assert {review.task_id for review in pending} == {
            child.task_id for child in children
        }
        ids = [child.pending_review["review_id"] for child in children]

        await control.submit_review_decision(
            ids[decision_order[0]],
            [{"type": "approve"}],
            wait=True,
        )
        remaining = control.list_pending_reviews(root_task_id=root.task_id)
        root_after_first = control.get_task_record(root.task_id)
        assert [review.review_id for review in remaining] == [ids[decision_order[1]]]
        assert root_after_first.pending_review is not None
        assert root_after_first.pending_review["review_id"] == ids[decision_order[1]]

        await control.submit_review_decision(
            ids[decision_order[1]],
            [{"type": "approve"}],
            wait=True,
        )
        return (
            ids,
            [
                review.review_id
                for review in control.list_pending_reviews(root_task_id=root.task_id)
            ],
            control.get_task_record(root.task_id).pending_review,
        )

    ids, remaining_ids, root_projection = asyncio.run(scenario())

    assert len(set(ids)) == 2
    assert remaining_ids == []
    assert root_projection is None


def test_pending_review_set_is_rebuilt_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    db_path = tmp_path / "tasks.sqlite"

    async def seed() -> tuple[str, list[str]]:
        store = TaskStore(str(db_path))
        control = async_subagent_runtime.AgentControl(
            build_specs(),
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        ids = []
        for _index in range(2):
            child = await control.spawn_task(
                "background_research",
                "needs review",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
            if control.get_live_run(child.task_id) is not None:
                await control.get_live_run(child.task_id)
            ids.append(child.pending_review["review_id"])
        await control.close()
        store.close()
        return root.task_id, ids

    root_task_id, review_ids = asyncio.run(seed())

    reopened_store = TaskStore(str(db_path))
    reopened = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        task_store=reopened_store,
    )
    try:
        restored = reopened.list_pending_reviews(root_task_id=root_task_id)
        assert {review.review_id for review in restored} == set(review_ids)
        assert {
            reopened.get_pending_review(review_id).task_id for review_id in review_ids
        } == {review.task_id for review in restored}
    finally:
        asyncio.run(reopened.close())
        reopened_store.close()


def test_root_lifecycle_keeps_child_review_compatibility_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, dict | None]:
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if control.get_live_run(child.task_id) is not None:
            await control.get_live_run(child.task_id)
        review_id = child.pending_review["review_id"]

        await control.send_task_input(root.task_id, "root follow-up")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        return review_id, control.get_task_record(root.task_id).pending_review

    review_id, projection = asyncio.run(scenario())

    assert projection is not None
    assert projection["review_id"] == review_id


def test_review_creation_failure_restores_memory_and_durable_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    manager = async_subagent_runtime.TaskManager(store)
    root = manager.create_task_record(
        "root",
        "background_research",
        parent_task_id=None,
        root_task_id="root",
        depth=1,
    )
    manager.mark_completed(root.task_id, "root done")
    child = manager.create_task_record(
        "child",
        "background_research",
        parent_task_id=root.task_id,
        root_task_id=root.task_id,
        depth=2,
    )

    def fail_append(**kwargs):
        del kwargs
        raise RuntimeError("event append failed")

    monkeypatch.setattr(store, "_append_task_event_locked", fail_append)
    try:
        with pytest.raises(RuntimeError, match="event append failed"):
            manager.mark_waiting_for_human(
                child.task_id,
                {"review_id": "review-create-failure"},
            )

        persisted_child = store.get_task(child.task_id)
        persisted_root = store.get_task(root.task_id)
        assert persisted_child is not None
        assert child.state == persisted_child.state == "pending"
        assert child.pending_review is persisted_child.pending_review is None
        assert persisted_root is not None
        assert root.pending_review is persisted_root.pending_review is None
        assert manager.get_pending_review("review-create-failure") is None
        assert manager.list_pending_reviews(root_task_id=root.task_id) == []
    finally:
        assert manager.event_ledger is not None
        manager.event_ledger.close()
        store.close()


def test_review_decision_failure_restores_memory_and_is_retryable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
    )

    async def scenario() -> tuple[str, str]:
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if control.get_live_run(child.task_id) is not None:
            await control.get_live_run(child.task_id)
        review_id = child.pending_review["review_id"]
        original_append = store._append_task_event_locked

        def fail_append(**kwargs):
            del kwargs
            raise RuntimeError("event append failed")

        monkeypatch.setattr(store, "_append_task_event_locked", fail_append)
        with pytest.raises(RuntimeError, match="event append failed"):
            await control.submit_review_decision(
                review_id,
                [{"type": "approve"}],
            )
        await asyncio.sleep(0)

        in_memory_child = control.get_task_record(child.task_id)
        in_memory_root = control.get_task_record(root.task_id)
        persisted_child = store.get_task(child.task_id)
        persisted_root = store.get_task(root.task_id)
        assert persisted_child is not None
        assert in_memory_child.state == persisted_child.state == "waiting_for_human"
        assert in_memory_child.pending_review == persisted_child.pending_review
        assert in_memory_child.pending_review["review_id"] == review_id
        assert persisted_root is not None
        assert in_memory_root.pending_review == persisted_root.pending_review
        assert in_memory_root.pending_review["review_id"] == review_id
        assert control.get_pending_review(review_id).task_id == child.task_id
        assert control.get_live_run(child.task_id) is None

        monkeypatch.setattr(store, "_append_task_event_locked", original_append)
        retried = await control.submit_review_decision(
            review_id,
            [{"type": "approve"}],
            wait=True,
        )
        assert retried.state == "completed"
        with pytest.raises(async_subagent_runtime.UnknownWorkerTaskError):
            control.get_pending_review(review_id)
        assert control.get_task_record(root.task_id).pending_review is None
        return review_id, retried.task_id

    try:
        review_id, child_task_id = asyncio.run(scenario())
        assert review_id
        assert child_task_id
    finally:
        asyncio.run(control.close())
        store.close()


def test_cleared_root_review_is_replayable_from_durable_cursor(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    async def scenario():
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        control = async_subagent_runtime.AgentControl(
            build_specs(),
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        try:
            root = await control.spawn_task("background_research", "root task")
            if control.get_live_run(root.task_id) is not None:
                await control.get_live_run(root.task_id)
            child = await control.spawn_task(
                "background_research",
                "needs review",
                parent_task_id=root.task_id,
                parent_thread_id=root.thread_id,
            )
            if control.get_live_run(child.task_id) is not None:
                await control.get_live_run(child.task_id)

            root = control.get_task_record(root.task_id)
            assert root.pending_review is not None
            fresh = control.open_local_task_event_stream(
                root.task_id,
                run_count=root.run_count,
                last_event_id=None,
            )
            snapshot = await anext(fresh)
            assert snapshot.data["pending_review"] is not None
            assert snapshot.event_id is not None
            await fresh.aclose()

            await control.submit_review_decision(
                child.pending_review["review_id"],
                [{"type": "approve"}],
                wait=True,
            )
            replay = control.open_local_task_event_stream(
                root.task_id,
                run_count=root.run_count,
                last_event_id=snapshot.event_id,
            )
            return await anext(replay), await anext(replay)
        finally:
            await control.close()
            store.close()

    cleared, ended = asyncio.run(scenario())
    assert cleared.event_type == "task.completed"
    assert cleared.data["pending_review"] is None
    assert ended.event_type == "stream.end"
    assert ended.data == {"reason": "completed"}


def test_submit_review_decision_default_does_not_wait_for_resumed_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ResumeBlockingInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> str:
        record = await control.spawn_task("background_research", "needs review")
        assert control.get_live_run(record.task_id) is not None
        await control.get_live_run(record.task_id)
        waiting = control.get_task_record(record.task_id)
        assert waiting.state == "waiting_for_human"
        updated = await control.submit_review_decision(
            waiting.pending_review["review_id"],
            [{"type": "approve"}],
        )
        assert updated.state == "running"
        assert control.get_live_run(updated.task_id) is not None
        await factory.created[0].resume_started.wait()
        control.get_live_run(updated.task_id).cancel()
        try:
            await control.get_live_run(updated.task_id)
        except asyncio.CancelledError:
            pass
        return "running"

    state_before_cancel = asyncio.run(scenario())

    assert state_before_cancel == "running"
    assert len(factory.created[0].calls) == 2


def test_remote_review_decision_is_forwarded_to_upstream_gateway() -> None:
    a2a_client = ReviewRemoteA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,  # type: ignore[arg-type]
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        record = await control.spawn_task("remote_code_wiki", "needs review")
        assert record.state == "waiting_for_human"
        assert record.pending_review is not None
        pending = control.list_pending_review_records()
        assert [item.task_id for item in pending] == [record.task_id]
        return await control.submit_review_decision(
            "remote-review-1",
            [{"type": "approve"}],
        )

    record = asyncio.run(scenario())

    assert record.state == "completed"
    assert record.result == "remote resumed"
    assert record.pending_review is None
    assert a2a_client.submitted == [
        {
            "task_id": "remote-review-task",
            "review_id": "remote-review-1",
            "decisions": [{"type": "approve"}],
        }
    ]


def test_sync_remote_waiting_review_is_mirrored_to_root_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=ReviewRemoteA2AClient(),  # type: ignore[arg-type]
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        if control.get_live_run(root.task_id) is not None:
            await control.get_live_run(root.task_id)
        child = await control.spawn_task(
            "remote_code_wiki",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        return control.get_task_record(root.task_id), control.get_task_record(
            child.task_id
        )

    root, child = asyncio.run(scenario())

    assert child.pending_review is not None
    assert root.pending_review is not None
    assert root.pending_review["review_id"] == child.pending_review["review_id"]
    assert root.pending_review["source_task_id"] == child.task_id
