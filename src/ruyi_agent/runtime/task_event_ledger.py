from __future__ import annotations

import asyncio
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ruyi_agent.runtime.task_event_contracts import (
    DEFAULT_EVENT_BATCH_SIZE,
    DEFAULT_MAX_PENDING_DELTAS,
    DEFAULT_TAIL_POLL_SECONDS,
    FULL_STATE_EVENT_TYPES,
    MAX_ASSISTANT_DELTA_TEXT_LENGTH,
    InvalidTaskEventCursorError,
    TaskEventsUnavailableError,
    TaskLifecycleEventType,
    TaskRunMismatchError,
    TaskStreamEvent,
)
from ruyi_agent.runtime.task_event_cursor import (
    decode_task_event_cursor,
    encode_task_event_cursor,
)
from ruyi_agent.runtime.task_event_projection import (
    build_reconciled_anchor,
    end_reason_for_record,
    end_reason_from_data,
    ensure_durable_event_data_fits,
    lifecycle_event_data,
    normalize_task_event_text,
    stored_event_to_stream_event,
    stream_end_event,
)

from ruyi_agent.storage.task_store import StoredTaskEvent, TaskStore
from ruyi_agent.task_models import PendingReviewRecord, TaskRecord


@dataclass(eq=False, slots=True)
class _Subscriber:
    task_id: str
    run_count: int
    loop: asyncio.AbstractEventLoop
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    items: deque[TaskStreamEvent] = field(default_factory=deque)
    pending_delta_count: int = 0
    closed: bool = False


class TaskEventLedger:
    """Durable lifecycle ledger plus best-effort in-process delta fan-out."""

    def __init__(
        self,
        store: TaskStore,
        *,
        tail_poll_seconds: float = DEFAULT_TAIL_POLL_SECONDS,
        max_pending_deltas: int = DEFAULT_MAX_PENDING_DELTAS,
    ) -> None:
        if tail_poll_seconds <= 0:
            raise ValueError("tail_poll_seconds must be positive")
        if max_pending_deltas <= 0:
            raise ValueError("max_pending_deltas must be positive")
        self._store = store
        self._tail_poll_seconds = tail_poll_seconds
        self._max_pending_deltas = max_pending_deltas
        self._lock = threading.RLock()
        self._subscribers: dict[str, set[_Subscriber]] = {}
        self._closed = False

    @property
    def store(self) -> TaskStore:
        return self._store

    def insert_task(
        self,
        record: TaskRecord,
        *,
        event_type: TaskLifecycleEventType,
        event_data: dict[str, Any],
    ) -> StoredTaskEvent:
        ensure_durable_event_data_fits(event_data)
        with self._lock:
            event = self._store.insert_task_with_event(
                record,
                event_type=event_type,
                event_data=event_data,
                event_created_at=record.updated_at,
            )
            subscribers = self._commit_durable_locked(event)
        self._wake_subscribers(subscribers)
        return event

    def update_task(
        self,
        record: TaskRecord,
        *,
        event_type: TaskLifecycleEventType,
        event_data: dict[str, Any],
    ) -> StoredTaskEvent:
        ensure_durable_event_data_fits(event_data)
        with self._lock:
            event = self._store.update_task_with_event(
                record,
                event_type=event_type,
                event_data=event_data,
                event_created_at=record.updated_at,
            )
            subscribers = self._commit_durable_locked(event)
        self._wake_subscribers(subscribers)
        return event

    def update_review_transition(
        self,
        record: TaskRecord,
        *,
        pending_review: PendingReviewRecord | None,
        root_record: TaskRecord | None,
        events: list[tuple[TaskRecord, TaskLifecycleEventType, dict[str, Any]]],
    ) -> list[StoredTaskEvent]:
        """Commit a review transition and publish its durable Task events."""

        for _event_record, _event_type, event_data in events:
            ensure_durable_event_data_fits(event_data)
        with self._lock:
            stored_events = self._store.update_review_transition(
                record,
                pending_review=pending_review,
                root_record=root_record,
                events=[
                    (
                        event_record,
                        event_type,
                        event_data,
                        event_record.updated_at,
                    )
                    for event_record, event_type, event_data in events
                ],
            )
            subscribers = [
                subscriber
                for event in stored_events
                for subscriber in self._commit_durable_locked(event)
            ]
        self._wake_subscribers(subscribers)
        return stored_events

    def publish_assistant_delta(
        self,
        *,
        task_id: str,
        run_count: int,
        content: str,
    ) -> None:
        if not content:
            return
        content = normalize_task_event_text(content)[
            :MAX_ASSISTANT_DELTA_TEXT_LENGTH
        ]
        event = TaskStreamEvent(
            event_type="assistant.delta",
            task_id=task_id,
            run_count=run_count,
            created_at=datetime.now(UTC),
            data={"content": content},
        )
        with self._lock:
            subscribers: list[_Subscriber] = []
            for subscriber in self._subscribers.get(task_id, ()):
                if subscriber.closed or subscriber.run_count != run_count:
                    continue
                if subscriber.pending_delta_count >= self._max_pending_deltas:
                    continue
                subscriber.items.append(event)
                subscriber.pending_delta_count += 1
                subscribers.append(subscriber)
        self._wake_subscribers(subscribers)

    def open_stream(
        self,
        *,
        task_id: str,
        run_count: int,
        last_event_id: str | None,
    ) -> TaskEventSubscription:
        if run_count < 0:
            raise TaskRunMismatchError(requested=run_count, current=0)

        subscriber: _Subscriber | None = None
        anchor_created = False
        anchor_subscribers: list[_Subscriber] = []
        with self._lock:
            if self._closed:
                raise TaskEventsUnavailableError("Task event ledger is closed")
            record = self._store.get_task(task_id)
            if record is None:
                raise KeyError(task_id)

            after_event_id = 0
            initial_events: list[TaskStreamEvent] = []
            historical = False
            if last_event_id is None:
                if record.run_count != run_count:
                    raise TaskRunMismatchError(
                        requested=run_count,
                        current=record.run_count,
                    )
                subscriber = self._register_subscriber_locked(task_id, run_count)
                try:
                    record, anchor, anchor_created = (
                        self._store.get_task_with_event_anchor(
                            task_id=task_id,
                            run_count=run_count,
                            build_anchor=build_reconciled_anchor,
                        )
                    )
                except Exception:
                    self._unsubscribe(subscriber)
                    raise
                after_event_id = anchor.event_id
                if anchor_created:
                    anchor_subscribers = self._commit_durable_locked(anchor)
                initial_events.append(
                    TaskStreamEvent(
                        event_type="task.snapshot",
                        task_id=task_id,
                        run_count=run_count,
                        created_at=datetime.now(UTC),
                        data=(
                            dict(anchor.data)
                            if anchor_created
                            else lifecycle_event_data(record)
                        ),
                        event_id=encode_task_event_cursor(anchor),
                    )
                )
            else:
                cursor_event_id = decode_task_event_cursor(
                    last_event_id,
                    task_id=task_id,
                    run_count=run_count,
                )
                cursor_event = self._store.get_task_event(cursor_event_id)
                if (
                    cursor_event is None
                    or cursor_event.task_id != task_id
                    or cursor_event.run_count != run_count
                ):
                    raise InvalidTaskEventCursorError("Unknown Task event cursor")
                if record.run_count < run_count:
                    raise TaskRunMismatchError(
                        requested=run_count,
                        current=record.run_count,
                    )
                after_event_id = cursor_event_id
                historical = record.run_count > run_count
                if not historical:
                    subscriber = self._register_subscriber_locked(task_id, run_count)

        if anchor_created:
            self._wake_subscribers(anchor_subscribers)
        return TaskEventSubscription(
            ledger=self,
            task_id=task_id,
            run_count=run_count,
            after_event_id=after_event_id,
            initial_events=initial_events,
            subscriber=subscriber,
            historical=historical,
        )

    def close(self) -> None:
        with self._lock:
            self._closed = True
            subscribers = [
                subscriber
                for task_subscribers in self._subscribers.values()
                for subscriber in task_subscribers
            ]
            self._subscribers.clear()
            for subscriber in subscribers:
                subscriber.closed = True
        self._wake_subscribers(subscribers)

    def _register_subscriber_locked(
        self,
        task_id: str,
        run_count: int,
    ) -> _Subscriber:
        subscriber = _Subscriber(
            task_id=task_id,
            run_count=run_count,
            loop=asyncio.get_running_loop(),
        )
        self._subscribers.setdefault(task_id, set()).add(subscriber)
        return subscriber

    def _commit_durable_locked(
        self,
        event: StoredTaskEvent,
    ) -> list[_Subscriber]:
        subscribers: list[_Subscriber] = []
        for subscriber in self._subscribers.get(event.task_id, ()):
            if subscriber.closed:
                continue
            if event.run_count >= subscriber.run_count:
                # Durable data is always read back from SQLite in event_id order.
                # This notification is only a wakeup and must never advance a
                # cursor past backlog that predates the subscriber registration.
                subscribers.append(subscriber)
        return subscribers

    def _wake_subscribers(self, subscribers: list[_Subscriber]) -> None:
        for subscriber in subscribers:
            try:
                subscriber.loop.call_soon_threadsafe(subscriber.wake.set)
            except RuntimeError:
                continue

    def _take_item(
        self,
        subscriber: _Subscriber,
    ) -> TaskStreamEvent | None:
        with self._lock:
            if not subscriber.items:
                subscriber.wake.clear()
                return None
            item = subscriber.items.popleft()
            if item.event_type == "assistant.delta":
                subscriber.pending_delta_count = max(
                    0,
                    subscriber.pending_delta_count - 1,
                )
            if not subscriber.items:
                subscriber.wake.clear()
            return item

    def _unsubscribe(self, subscriber: _Subscriber | None) -> None:
        if subscriber is None:
            return
        with self._lock:
            subscriber.closed = True
            task_subscribers = self._subscribers.get(subscriber.task_id)
            if task_subscribers is not None:
                task_subscribers.discard(subscriber)
                if not task_subscribers:
                    self._subscribers.pop(subscriber.task_id, None)
            subscriber.items.clear()
            subscriber.pending_delta_count = 0
            subscriber.wake.set()


class TaskEventSubscription:
    """One fixed-run stream; cancelling it never affects the Agent run."""

    def __init__(
        self,
        *,
        ledger: TaskEventLedger,
        task_id: str,
        run_count: int,
        after_event_id: int,
        initial_events: list[TaskStreamEvent],
        subscriber: _Subscriber | None,
        historical: bool,
    ) -> None:
        self._ledger = ledger
        self._task_id = task_id
        self._run_count = run_count
        self._after_event_id = after_event_id
        self._pending: deque[TaskStreamEvent] = deque(initial_events)
        self._subscriber = subscriber
        self._historical = historical
        self._end_reason: str | None = None
        self._closed = False

    def __aiter__(self) -> TaskEventSubscription:
        return self

    async def __anext__(self) -> TaskStreamEvent:
        while True:
            if self._pending:
                event = self._pending.popleft()
                self._observe_event(event)
                return event
            if self._end_reason is not None:
                # Lifecycle writes take ledger lock -> store lock.  Hold the same
                # outer lock across the final tail/state decision so a transition
                # either becomes visible here or linearizes after stream.end.
                with self._ledger._lock:
                    if self._queue_stored_events():
                        continue
                    reason = self._end_reason
                    current = self._ledger.store.get_task(self._task_id)
                    if current is not None and current.run_count > self._run_count:
                        reason = "superseded"
                    elif (
                        current is not None
                        and current.run_count == self._run_count
                    ):
                        current_reason = end_reason_for_record(current)
                        if current_reason is None:
                            self._end_reason = None
                            continue
                        reason = current_reason
                    self._end_reason = None
                    self._closed = True
                    self._ledger._unsubscribe(self._subscriber)
                    self._subscriber = None
                    return stream_end_event(
                        task_id=self._task_id,
                        run_count=self._run_count,
                        reason=reason,
                    )
            if self._closed:
                raise StopAsyncIteration

            subscriber = self._subscriber
            if subscriber is not None:
                item = self._ledger._take_item(subscriber)
                if isinstance(item, TaskStreamEvent):
                    return item
                if subscriber.closed:
                    await self.aclose()
                    raise TaskEventsUnavailableError("Task event ledger is closed")

            if self._queue_stored_events():
                continue

            current = self._ledger.store.get_task(self._task_id)
            end_reason: str | None = None
            if current is None:
                end_reason = "error"
            elif current.run_count > self._run_count or self._historical:
                end_reason = "superseded"
            else:
                end_reason = end_reason_for_record(current)
            if end_reason is not None:
                # The Task row and its lifecycle event commit atomically.  A commit
                # may land after the first empty event query but before the Task row
                # is read, so query the ledger again before ending the stream.
                if self._queue_stored_events():
                    continue
                self._end_reason = end_reason
                continue

            if subscriber is None:
                self._end_reason = "superseded"
                continue

            try:
                await asyncio.wait_for(
                    subscriber.wake.wait(),
                    timeout=self._ledger._tail_poll_seconds,
                )
            except TimeoutError:
                pass

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._ledger._unsubscribe(self._subscriber)
        self._subscriber = None

    def _queue_stored_events(self) -> bool:
        stored_events = self._ledger.store.list_task_events(
            task_id=self._task_id,
            run_count=self._run_count,
            after_event_id=self._after_event_id,
            limit=DEFAULT_EVENT_BATCH_SIZE,
        )
        if not stored_events:
            return False
        self._pending.extend(
            stored_event_to_stream_event(event) for event in stored_events
        )
        self._after_event_id = stored_events[-1].event_id
        return True

    def _observe_event(self, event: TaskStreamEvent) -> None:
        if self._historical:
            return
        if event.event_type in FULL_STATE_EVENT_TYPES:
            # A later running/created projection can clear an earlier mirrored
            # review reason, so assigning None is meaningful here.
            self._end_reason = end_reason_from_data(event.data)
