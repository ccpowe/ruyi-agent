from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import AsyncIterator, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult

from ruyi_agent.gateway.sse import (
    encode_task_stream_event,
)
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.task_events import (
    InvalidTaskEventCursorError,
    MAX_ASSISTANT_DELTA_TEXT_LENGTH,
    MAX_DURABLE_TASK_EVENT_DATA_BYTES,
    MAX_EVENT_TEXT_LENGTH,
    TaskEventLedger,
    TaskEventsUnavailableError,
    TaskRunMismatchError,
    artifact_event_data,
    lifecycle_event_data,
)
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.task_models import PublishedArtifact, TaskRecord


def _record(*, task_id: str = "task-1") -> TaskRecord:
    now = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name="main",
        state="pending",
        thread_id=task_id,
        parent_task_id=None,
        root_task_id=task_id,
        depth=0,
        created_at=now,
        updated_at=now,
    )


class SequenceModel(BaseChatModel):
    responses: list[AIMessage]
    index: int = 0

    @property
    def _llm_type(self) -> str:
        return "outer-probe"

    def bind_tools(
        self,
        tools: Sequence[Any],
        **kwargs: Any,
    ) -> "SequenceModel":
        del tools, kwargs
        return self

    def _generate(self, messages: Any, **kwargs: Any) -> ChatResult:
        del messages, kwargs
        response = self._next_response()
        return ChatResult(generations=[ChatGeneration(message=response)])

    async def _agenerate(self, messages: Any, **kwargs: Any) -> ChatResult:
        return self._generate(messages, **kwargs)

    async def _astream(
        self,
        messages: Any,
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        del messages, stop, run_manager, kwargs
        response = self._next_response()
        tool_call_chunks = [
            {
                "name": call["name"],
                "args": json.dumps(call["args"]),
                "id": call["id"],
                "index": index,
                "type": "tool_call_chunk",
            }
            for index, call in enumerate(response.tool_calls)
        ]
        yield ChatGenerationChunk(
            message=AIMessageChunk(
                content=response.content,
                tool_call_chunks=tool_call_chunks,
            )
        )

    def _next_response(self) -> AIMessage:
        response = self.responses[self.index]
        if self.index < len(self.responses) - 1:
            self.index += 1
        return response


class SecretStreamingModel(BaseChatModel):
    secret: str

    @property
    def _llm_type(self) -> str:
        return "secret-probe"

    def _generate(self, messages: Any, **kwargs: Any) -> ChatResult:
        del messages, kwargs
        return ChatResult(
            generations=[ChatGeneration(message=AIMessage(content=self.secret))]
        )

    async def _agenerate(self, messages: Any, **kwargs: Any) -> ChatResult:
        return self._generate(messages, **kwargs)

    async def _astream(
        self,
        messages: Any,
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        del messages, stop, run_manager, kwargs
        yield ChatGenerationChunk(message=AIMessageChunk(content=self.secret))


def _message_stream_parts_containing(
    parts: list[dict[str, Any]],
    text: str,
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for part in parts:
        data = part.get("data")
        if (
            part.get("type") == "messages"
            and isinstance(data, tuple)
            and isinstance(data[0], AIMessageChunk)
            and text in str(data[0].content)
        ):
            matches.append(part)
    return matches


def test_task_event_ledger_streams_snapshot_delta_lifecycle_and_end(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, tail_poll_seconds=0.01)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            record.updated_at += timedelta(seconds=1)
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )

            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            snapshot = await anext(stream)
            assert snapshot.event_type == "task.snapshot"
            assert snapshot.event_id
            assert snapshot.data["status"] == "running"

            ledger.publish_assistant_delta(
                task_id=record.task_id,
                run_count=1,
                content="hel",
            )
            record.state = "completed"
            record.result = "hello"
            record.updated_at += timedelta(seconds=1)
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )

            delta = await anext(stream)
            completed = await anext(stream)
            ended = await anext(stream)
            assert (delta.event_type, delta.event_id, delta.data) == (
                "assistant.delta",
                None,
                {"content": "hel"},
            )
            assert completed.event_type == "task.completed"
            assert completed.event_id
            assert completed.data["last_result"] == "hello"
            assert ended.event_type == "stream.end"
            assert ended.event_id is None
            assert ended.data == {"reason": "completed"}
            with pytest.raises(StopAsyncIteration):
                await anext(stream)
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_ledger_rejects_oversized_durable_data_before_persistence(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    ledger = TaskEventLedger(store)
    record = _record()
    try:
        with pytest.raises(ValueError, match="wire budget"):
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data={"content": "x" * (MAX_DURABLE_TASK_EVENT_DATA_BYTES + 1)},
            )
        assert store.get_task(record.task_id) is None
    finally:
        ledger.close()
        store.close()


def test_task_event_reconnect_replays_then_reports_superseded(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, tail_poll_seconds=0.01)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            record.updated_at += timedelta(seconds=1)
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            first = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            cursor = (await anext(first)).event_id
            assert cursor is not None
            await first.aclose()

            record.state = "completed"
            record.result = "run one"
            record.updated_at += timedelta(seconds=1)
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.result = None
            record.run_count = 2
            record.updated_at += timedelta(seconds=1)
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=cursor,
            )
            assert (await anext(replay)).event_type == "task.completed"
            assert (await anext(replay)).data == {"reason": "superseded"}
            with pytest.raises(TaskRunMismatchError):
                ledger.open_stream(
                    task_id=record.task_id,
                    run_count=1,
                    last_event_id=None,
                )
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_future_run_wakeup_does_not_skip_replay_backlog(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            initial = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            cursor = (await anext(initial)).event_id
            assert cursor is not None
            await initial.aclose()

            record.state = "completed"
            record.result = "run one"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=cursor,
            )

            record.state = "running"
            record.result = None
            record.run_count = 2
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )

            assert (await anext(replay)).event_type == "task.completed"
            assert (await anext(replay)).data == {"reason": "superseded"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_late_marker_cannot_skip_multi_batch_replay_backlog(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            initial = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            cursor = (await anext(initial)).event_id
            assert cursor is not None
            await initial.aclose()

            for index in range(101):
                artifact = PublishedArtifact(
                    artifact_id=f"artifact-{index}",
                    path=f"/private/{index}",
                    name=f"artifact-{index}.txt",
                    caption=None,
                    content_type="text/plain",
                    size=index,
                    run_count=1,
                )
                record.artifacts.append(artifact)
                ledger.update_task(
                    record,
                    event_type="task.artifact_published",
                    event_data=artifact_event_data(record, artifact),
                )

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=cursor,
            )
            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )

            replayed = [await anext(replay) for _ in range(102)]
            assert [event.event_type for event in replayed] == [
                *("task.artifact_published" for _ in range(101)),
                "task.completed",
            ]
            assert (await anext(replay)).data == {"reason": "completed"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_terminal_at_batch_boundary_does_not_hide_later_review(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            initial = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            cursor = (await anext(initial)).event_id
            assert cursor is not None
            await initial.aclose()

            for index in range(99):
                artifact = PublishedArtifact(
                    artifact_id=f"artifact-{index}",
                    path=f"/private/{index}",
                    name=f"artifact-{index}.txt",
                    caption=None,
                    content_type="text/plain",
                    size=index,
                    run_count=1,
                )
                record.artifacts.append(artifact)
                ledger.update_task(
                    record,
                    event_type="task.artifact_published",
                    event_data=artifact_event_data(record, artifact),
                )
            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            record.pending_review = {
                "review_id": "review-1",
                "action_requests": [{"name": "execute", "args": {}}],
            }
            ledger.update_task(
                record,
                event_type="task.review_requested",
                event_data=lifecycle_event_data(record),
            )

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=cursor,
            )
            replayed = [await anext(replay) for _ in range(101)]
            assert [event.event_type for event in replayed] == [
                *("task.artifact_published" for _ in range(99)),
                "task.completed",
                "task.review_requested",
            ]
            assert (await anext(replay)).data == {"reason": "review_required"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_review_committed_after_terminal_delivery_precedes_stream_end(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            await anext(stream)

            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            assert (await anext(stream)).event_type == "task.completed"

            record.pending_review = {
                "review_id": "review-1",
                "action_requests": [{"name": "execute", "args": {}}],
            }
            ledger.update_task(
                record,
                event_type="task.review_requested",
                event_data=lifecycle_event_data(record),
            )
            assert (await anext(stream)).event_type == "task.review_requested"
            assert (await anext(stream)).data == {"reason": "review_required"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_replayed_running_state_clears_stale_review_end_reason(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, tail_poll_seconds=60)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            initial = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            cursor = (await anext(initial)).event_id
            assert cursor is not None
            await initial.aclose()

            record.pending_review = {
                "review_id": "review-1",
                "action_requests": [{"name": "execute", "args": {}}],
            }
            ledger.update_task(
                record,
                event_type="task.review_requested",
                event_data=lifecycle_event_data(record),
            )
            record.pending_review = None
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=cursor,
            )
            assert (await anext(replay)).event_type == "task.review_requested"
            assert (await anext(replay)).event_type == "task.running"
            waiting = asyncio.create_task(anext(replay))
            await asyncio.sleep(0)
            assert waiting.done() is False

            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            assert (await asyncio.wait_for(waiting, timeout=1)).event_type == (
                "task.completed"
            )
            assert (await anext(replay)).data == {"reason": "completed"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_stream_end_linearizes_against_late_lifecycle_commit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        updater: threading.Thread | None = None
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            await anext(stream)
            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            terminal = await anext(stream)
            assert terminal.event_id is not None

            review_record = replace(
                record,
                pending_review={
                    "review_id": "review-1",
                    "action_requests": [{"name": "execute", "args": {}}],
                },
                updated_at=record.updated_at + timedelta(seconds=1),
            )
            review_data = lifecycle_event_data(review_record)
            attempted = threading.Event()
            committed = threading.Event()
            original_list = store.list_task_events
            armed = True

            def commit_review() -> None:
                attempted.set()
                ledger.update_task(
                    review_record,
                    event_type="task.review_requested",
                    event_data=review_data,
                )
                committed.set()

            def hooked_list(*args, **kwargs):
                nonlocal armed, updater
                result = original_list(*args, **kwargs)
                if armed and not result:
                    armed = False
                    updater = threading.Thread(target=commit_review)
                    updater.start()
                    assert attempted.wait(timeout=1)
                    assert committed.wait(timeout=0.1) is False
                return result

            monkeypatch.setattr(store, "list_task_events", hooked_list)
            ended = await anext(stream)
            assert ended.data == {"reason": "completed"}
            assert updater is not None
            await asyncio.to_thread(updater.join, 1)
            assert updater.is_alive() is False
            assert committed.is_set()

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=terminal.event_id,
            )
            assert (await anext(replay)).event_type == "task.review_requested"
            assert (await anext(replay)).data == {"reason": "review_required"}
        finally:
            if updater is not None:
                updater.join(timeout=1)
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_durable_wakeups_do_not_grow_the_subscriber_queue(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            assert (await anext(stream)).event_type == "task.snapshot"
            subscriber = stream._subscriber
            assert subscriber is not None

            for _ in range(250):
                ledger.update_task(
                    record,
                    event_type="task.running",
                    event_data=lifecycle_event_data(record),
                )
            assert len(subscriber.items) == 0

            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            replayed = [await anext(stream) for _ in range(251)]
            assert [event.event_type for event in replayed] == [
                *("task.running" for _ in range(250)),
                "task.completed",
            ]
            assert (await anext(stream)).data == {"reason": "completed"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_task_event_cursor_is_bound_to_task_and_run(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        first = _record(task_id="task-1")
        second = _record(task_id="task-2")
        try:
            for record in (first, second):
                ledger.insert_task(
                    record,
                    event_type="task.created",
                    event_data=lifecycle_event_data(record),
                )
            stream = ledger.open_stream(
                task_id="task-1", run_count=0, last_event_id=None
            )
            cursor = (await anext(stream)).event_id
            assert cursor is not None
            await stream.aclose()

            with pytest.raises(InvalidTaskEventCursorError):
                ledger.open_stream(
                    task_id="task-2",
                    run_count=0,
                    last_event_id=cursor,
                )
            with pytest.raises(InvalidTaskEventCursorError):
                ledger.open_stream(
                    task_id="task-1",
                    run_count=0,
                    last_event_id="not-base64",
                )
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_snapshot_registration_race_replays_transition(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, tail_poll_seconds=0.01)
        record = _record()
        ledger.insert_task(
            record,
            event_type="task.created",
            event_data=lifecycle_event_data(record),
        )
        record.state = "running"
        record.run_count = 1
        ledger.update_task(
            record,
            event_type="task.running",
            event_data=lifecycle_event_data(record),
        )
        entered_anchor = threading.Event()
        release_anchor = threading.Event()
        original = store.get_task_with_event_anchor

        def blocked_anchor(**kwargs):
            entered_anchor.set()
            assert release_anchor.wait(timeout=2)
            return original(**kwargs)

        monkeypatch.setattr(store, "get_task_with_event_anchor", blocked_anchor)
        completed = replace(
            record,
            state="completed",
            result="raced result",
            updated_at=record.updated_at + timedelta(seconds=1),
        )

        def finish_run() -> None:
            assert entered_anchor.wait(timeout=2)
            release_anchor.set()
            ledger.update_task(
                completed,
                event_type="task.completed",
                event_data=lifecycle_event_data(completed),
            )

        updater = threading.Thread(target=finish_run)
        updater.start()
        try:
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            await asyncio.to_thread(updater.join, 2)
            assert not updater.is_alive()
            assert (await anext(stream)).event_type == "task.snapshot"
            terminal = await anext(stream)
            assert terminal.event_type == "task.completed"
            assert terminal.data["last_result"] == "raced result"
            assert (await anext(stream)).data == {"reason": "completed"}
        finally:
            release_anchor.set()
            updater.join(timeout=2)
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_terminal_state_race_rechecks_ledger_before_end(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, tail_poll_seconds=0.01)
        record = _record()
        ledger.insert_task(
            record,
            event_type="task.created",
            event_data=lifecycle_event_data(record),
        )
        record.state = "running"
        record.run_count = 1
        ledger.update_task(
            record,
            event_type="task.running",
            event_data=lifecycle_event_data(record),
        )
        stream = ledger.open_stream(
            task_id=record.task_id,
            run_count=1,
            last_event_id=None,
        )
        await anext(stream)

        entered_empty_query = threading.Event()
        release_empty_query = threading.Event()
        original = store.list_task_events

        def blocked_first_empty_query(**kwargs):
            events = original(**kwargs)
            if not entered_empty_query.is_set():
                assert events == []
                entered_empty_query.set()
                assert release_empty_query.wait(timeout=2)
            return events

        monkeypatch.setattr(store, "list_task_events", blocked_first_empty_query)
        completed = replace(
            record,
            state="completed",
            result="committed during empty query",
            updated_at=record.updated_at + timedelta(seconds=1),
        )

        def finish_run() -> None:
            assert entered_empty_query.wait(timeout=2)
            ledger.update_task(
                completed,
                event_type="task.completed",
                event_data=lifecycle_event_data(completed),
            )
            release_empty_query.set()

        updater = threading.Thread(target=finish_run)
        updater.start()
        try:
            terminal = await anext(stream)
            await asyncio.to_thread(updater.join, 2)
            assert not updater.is_alive()
            assert terminal.event_type == "task.completed"
            assert terminal.data["last_result"] == "committed during empty query"
            assert (await anext(stream)).data == {"reason": "completed"}
        finally:
            release_empty_query.set()
            updater.join(timeout=2)
            await stream.aclose()
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_legacy_task_gets_one_reconciled_anchor(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        record = _record()
        record.state = "completed"
        record.result = "legacy"
        record.run_count = 1
        store.insert_task(record)
        ledger = TaskEventLedger(store)
        try:
            first = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            snapshot = await anext(first)
            assert snapshot.data["reconciled"] is True
            assert "observed_at" in snapshot.data
            await first.aclose()

            second = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            await second.aclose()
            events = store.list_task_events(
                task_id=record.task_id,
                run_count=1,
                after_event_id=0,
            )
            assert len(events) == 1
            assert events[0].data["reconciled"] is True
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_restart_marks_active_run_interrupted_with_atomic_event(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        record = _record()
        record.state = "running"
        record.run_count = 1
        store.insert_task_with_event(
            record,
            event_type="task.running",
            event_data=lifecycle_event_data(record),
            event_created_at=record.updated_at,
        )
        first = AgentControl(
            {},
            checkpointer=object(),
            backend=object(),
            task_store=store,
        )
        second: AgentControl | None = None
        try:
            recovered = first.get_task_record(record.task_id)
            assert recovered.state == "interrupted"
            assert "process restarted" in (recovered.error or "").lower()
            events = store.list_task_events(
                task_id=record.task_id,
                run_count=1,
                after_event_id=0,
            )
            assert [event.event_type for event in events] == [
                "task.running",
                "task.interrupted",
            ]

            second = AgentControl(
                {},
                checkpointer=object(),
                backend=object(),
                task_store=store,
            )
            assert second.get_task_record(record.task_id).state == "interrupted"
            assert (
                len(
                    store.list_task_events(
                        task_id=record.task_id,
                        run_count=1,
                        after_event_id=0,
                    )
                )
                == 2
            )
        finally:
            await first.close()
            if second is not None:
                await second.close()
            store.close()

    asyncio.run(scenario())


def test_slow_subscriber_drops_only_transient_deltas(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, max_pending_deltas=1)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=None,
            )
            await anext(stream)
            ledger.publish_assistant_delta(
                task_id=record.task_id, run_count=0, content="first"
            )
            ledger.publish_assistant_delta(
                task_id=record.task_id, run_count=0, content="dropped"
            )
            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )
            assert (await anext(stream)).data == {"content": "first"}
            assert (await anext(stream)).event_type == "task.completed"
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_oversized_delta_is_bounded_without_hiding_terminal_event(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=None,
            )
            await anext(stream)
            ledger.publish_assistant_delta(
                task_id=record.task_id,
                run_count=0,
                content="\U0001f680" * (MAX_ASSISTANT_DELTA_TEXT_LENGTH + 1),
            )
            record.state = "completed"
            record.result = "done"
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=lifecycle_event_data(record),
            )

            delta = await anext(stream)
            assert delta.event_type == "assistant.delta"
            assert len(delta.data["content"]) == MAX_ASSISTANT_DELTA_TEXT_LENGTH
            assert encode_task_stream_event(delta)
            assert (await anext(stream)).event_type == "task.completed"
            assert (await anext(stream)).data == {"reason": "completed"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_unicode_terminal_event_is_wire_safe_and_replayable(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            record.state = "running"
            record.run_count = 1
            ledger.update_task(
                record,
                event_type="task.running",
                event_data=lifecycle_event_data(record),
            )
            live = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=None,
            )
            snapshot_cursor = (await anext(live)).event_id
            assert snapshot_cursor is not None

            record.state = "completed"
            record.result = "\U0001f680" * MAX_EVENT_TEXT_LENGTH
            terminal_data = lifecycle_event_data(record)
            assert terminal_data["last_result_truncated"] is True
            ledger.update_task(
                record,
                event_type="task.completed",
                event_data=terminal_data,
            )

            live_terminal = await anext(live)
            assert live_terminal.event_type == "task.completed"
            assert encode_task_stream_event(live_terminal)
            assert (await anext(live)).data == {"reason": "completed"}

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=1,
                last_event_id=snapshot_cursor,
            )
            replayed_terminal = await anext(replay)
            assert replayed_terminal.event_id == live_terminal.event_id
            assert encode_task_stream_event(replayed_terminal)
            assert (await anext(replay)).data == {"reason": "completed"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_surrogate_terminal_is_normalized_persisted_and_replayable(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        manager = TaskManager(store)
        ledger = manager.event_ledger
        assert ledger is not None
        record = manager.create_task_record(
            "task-surrogate",
            "main",
            parent_task_id=None,
            root_task_id="task-surrogate",
            depth=0,
        )
        try:
            live = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=None,
            )
            snapshot_cursor = (await anext(live)).event_id
            assert snapshot_cursor is not None

            manager.mark_completed(record.task_id, "before\ud800after")
            assert record.result == "before\ufffdafter"
            assert store.get_task(record.task_id).result == "before\ufffdafter"
            terminal = await anext(live)
            assert terminal.data["last_result"] == "before\ufffdafter"
            assert encode_task_stream_event(terminal)
            assert (await anext(live)).data == {"reason": "completed"}

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=snapshot_cursor,
            )
            replayed = await anext(replay)
            assert replayed.data["last_result"] == "before\ufffdafter"
            assert encode_task_stream_event(replayed)
            assert (await anext(replay)).data == {"reason": "completed"}
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_surrogate_delta_artifact_review_and_error_are_stream_safe(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        manager = TaskManager(store)
        ledger = manager.event_ledger
        assert ledger is not None
        record = manager.create_task_record(
            "task-public-text",
            "main",
            parent_task_id=None,
            root_task_id="task-public-text",
            depth=0,
        )
        try:
            live = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=None,
            )
            cursor = (await anext(live)).event_id
            assert cursor is not None

            ledger.publish_assistant_delta(
                task_id=record.task_id,
                run_count=0,
                content="delta\ud800text",
            )
            manager.add_artifact(
                record.task_id,
                PublishedArtifact(
                    artifact_id="artifact\ud800id",
                    path="/private/path",
                    name="artifact\ud800name",
                    caption="caption\ud800text",
                    content_type="text/\ud800plain",
                    size=10,
                    run_count=0,
                ),
            )
            manager.mark_waiting_for_human(
                record.task_id,
                {
                    "review_id": "review\ud800id",
                    "action_requests": [{"name": "action\ud800name"}],
                    "review_configs": [
                        {
                            "action_name": "action\ud800name",
                            "allowed_decisions": ["approve\ud800now"],
                        }
                    ],
                },
            )

            delta = await anext(live)
            artifact = await anext(live)
            review = await anext(live)
            assert delta.data["content"] == "delta\ufffdtext"
            assert artifact.data["artifact"]["name"] == "artifact\ufffdname"
            assert artifact.data["artifact_truncated"] is True
            assert review.data["pending_review"]["review_id"] == "review\ufffdid"
            assert review.data["pending_review_truncated"] is True
            assert all(
                encode_task_stream_event(event) for event in (delta, artifact, review)
            )
            assert (await anext(live)).data == {"reason": "review_required"}

            replay = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=cursor,
            )
            replayed_artifact = await anext(replay)
            replayed_review = await anext(replay)
            assert replayed_artifact.data == artifact.data
            assert replayed_review.data == review.data
            assert (await anext(replay)).data == {"reason": "review_required"}

            failed = manager.create_task_record(
                "task-error",
                "main",
                parent_task_id=None,
                root_task_id="task-error",
                depth=0,
            )
            manager.mark_failed(failed.task_id, "error\ud800text")
            assert failed.error == "error\ufffdtext"
            stored_failed = store.get_task(failed.task_id)
            assert stored_failed is not None
            assert stored_failed.error == "error\ufffdtext"
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())


def test_ledger_close_wakes_live_subscription(tmp_path) -> None:
    async def scenario() -> None:
        store = TaskStore(str(tmp_path / "tasks.sqlite"))
        ledger = TaskEventLedger(store, tail_poll_seconds=60)
        record = _record()
        try:
            ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
            stream = ledger.open_stream(
                task_id=record.task_id,
                run_count=0,
                last_event_id=None,
            )
            await anext(stream)
            waiting = asyncio.create_task(anext(stream))
            await asyncio.sleep(0)
            ledger.close()
            with pytest.raises(TaskEventsUnavailableError):
                await asyncio.wait_for(waiting, timeout=1)
        finally:
            ledger.close()
            store.close()

    asyncio.run(scenario())
