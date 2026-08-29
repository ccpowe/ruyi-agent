from __future__ import annotations

import asyncio
import base64
import binascii
import json
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal

from langchain_core.messages import AIMessageChunk

from ruyi_agent.storage.task_store import StoredTaskEvent, TaskStore
from ruyi_agent.task_models import PublishedArtifact, TaskRecord


TASK_EVENT_CURSOR_VERSION = 1
MAX_TASK_EVENT_CURSOR_LENGTH = 4096
MAX_EVENT_TEXT_LENGTH = 256 * 1024
MAX_ASSISTANT_DELTA_TEXT_LENGTH = 32 * 1024
MAX_DURABLE_TASK_EVENT_DATA_BYTES = 512 * 1024
MAX_LIFECYCLE_TEXT_JSON_BYTES = 128 * 1024
MAX_PENDING_REVIEW_JSON_BYTES = 64 * 1024
MAX_ARTIFACT_LIST_JSON_BYTES = 128 * 1024
MAX_ARTIFACT_CAPTION_JSON_BYTES = 32 * 1024
MAX_SHORT_EVENT_TEXT_LENGTH = 4096
MAX_SHORT_EVENT_TEXT_JSON_BYTES = 8 * 1024
MAX_REVIEW_ITEMS = 100
MAX_REVIEW_DECISIONS = 20
MAX_EVENT_ARTIFACTS = 1000
DEFAULT_EVENT_BATCH_SIZE = 100
DEFAULT_TAIL_POLL_SECONDS = 1.0
DEFAULT_MAX_PENDING_DELTAS = 256
PUBLIC_ASSISTANT_DELTA_NODES = frozenset({"model"})
PUBLIC_ASSISTANT_DELTA_PATH = ("__pregel_pull", "model")

TaskLifecycleEventType = Literal[
    "task.created",
    "task.running",
    "task.review_requested",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
    "task.artifact_published",
]

TERMINAL_EVENT_TYPES = {
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
}
_FULL_STATE_EVENT_TYPES = {
    "task.snapshot",
    "task.created",
    "task.running",
    "task.review_requested",
    *TERMINAL_EVENT_TYPES,
}


class TaskEventsUnavailableError(RuntimeError):
    """Durable Task event streaming requires a TaskStore."""


class InvalidTaskEventCursorError(ValueError):
    """The caller supplied a malformed or mismatched Last-Event-ID."""


class TaskRunMismatchError(ValueError):
    """The requested run cannot be observed as a fresh live stream."""

    def __init__(self, *, requested: int, current: int) -> None:
        super().__init__(
            f"Requested task run {requested} does not match current run {current}"
        )
        self.requested = requested
        self.current = current


@dataclass(frozen=True, slots=True)
class TaskStreamEvent:
    """Transport-neutral event later encoded by the HTTP SSE adapter."""

    event_type: str
    task_id: str
    run_count: int
    created_at: datetime
    data: dict[str, Any]
    event_id: str | None = None


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
        _ensure_durable_event_data_fits(event_data)
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
        _ensure_durable_event_data_fits(event_data)
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
                            build_anchor=_build_reconciled_anchor,
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
                        current_reason = _end_reason_for_record(current)
                        if current_reason is None:
                            self._end_reason = None
                            continue
                        reason = current_reason
                    self._end_reason = None
                    self._closed = True
                    self._ledger._unsubscribe(self._subscriber)
                    self._subscriber = None
                    return _stream_end_event(
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
                end_reason = _end_reason_for_record(current)
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
            _stored_event_to_stream_event(event) for event in stored_events
        )
        self._after_event_id = stored_events[-1].event_id
        return True

    def _observe_event(self, event: TaskStreamEvent) -> None:
        if self._historical:
            return
        if event.event_type in _FULL_STATE_EVENT_TYPES:
            # A later running/created projection can clear an earlier mirrored
            # review reason, so assigning None is meaningful here.
            self._end_reason = _end_reason_from_data(event.data)


def lifecycle_event_type(record: TaskRecord) -> TaskLifecycleEventType:
    return {
        "pending": "task.created",
        "running": "task.running",
        "waiting_for_human": "task.review_requested",
        "completed": "task.completed",
        "failed": "task.failed",
        "cancelled": "task.cancelled",
        "interrupted": "task.interrupted",
    }[record.state]


def lifecycle_event_data(
    record: TaskRecord,
    *,
    reconciled: bool = False,
) -> dict[str, Any]:
    last_result, result_truncated = _bounded_text(record.result)
    error, error_truncated = _bounded_text(record.error)
    pending_review, pending_review_truncated = _project_pending_review(
        record.pending_review
    )
    artifacts, artifacts_truncated = _project_artifacts(record.artifacts)
    data: dict[str, Any] = {
        "status": record.state,
        "last_result": last_result,
        "error": error,
        "updated_at": _isoformat(record.updated_at),
        "pending_review": pending_review,
        "artifacts": artifacts,
    }
    if result_truncated:
        data["last_result_truncated"] = True
    if error_truncated:
        data["error_truncated"] = True
    if pending_review_truncated:
        data["pending_review_truncated"] = True
    if artifacts_truncated:
        data["artifacts_truncated"] = True
    if reconciled:
        data["reconciled"] = True
        data["observed_at"] = _isoformat(datetime.now(UTC))
    _ensure_durable_event_data_fits(data)
    return data


def artifact_event_data(
    record: TaskRecord,
    artifact: PublishedArtifact,
) -> dict[str, Any]:
    projected, truncated = _project_artifact(artifact)
    data = {
        "status": record.state,
        "updated_at": _isoformat(record.updated_at),
        "artifact": projected,
    }
    if truncated:
        data["artifact_truncated"] = True
    _ensure_durable_event_data_fits(data)
    return data


def public_task_event_fingerprint(record: TaskRecord) -> str:
    """Fingerprint public lifecycle fields so remote refresh polling is quiet."""

    data = lifecycle_event_data(record)
    data.pop("updated_at", None)
    return json.dumps(
        {
            "run_count": record.run_count,
            **data,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def assistant_delta_from_stream_part(part: Any) -> str | None:
    """Project only public assistant text from one LangGraph v2 stream part."""

    if (
        not isinstance(part, dict)
        or part.get("type") != "messages"
        or part.get("ns") != ()
    ):
        return None
    data = part.get("data")
    if not isinstance(data, (tuple, list)) or len(data) != 2:
        return None
    message, metadata = data
    if (
        not isinstance(metadata, dict)
        or metadata.get("langgraph_node") not in PUBLIC_ASSISTANT_DELTA_NODES
        or metadata.get("langgraph_path") != PUBLIC_ASSISTANT_DELTA_PATH
    ):
        return None
    if not isinstance(message, AIMessageChunk):
        return None
    content = message.content
    if isinstance(content, str):
        return content or None
    if not isinstance(content, list):
        return None

    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") not in {"text", "output_text"}:
            continue
        text = block.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    return "".join(parts) or None


def normalize_task_event_text(value: str) -> str:
    """Replace Python surrogate code points so public JSON is valid UTF-8."""

    if not any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        return value
    return "".join(
        "\ufffd" if 0xD800 <= ord(character) <= 0xDFFF else character
        for character in value
    )


def encode_task_event_cursor(event: StoredTaskEvent) -> str:
    payload = json.dumps(
        {
            "v": TASK_EVENT_CURSOR_VERSION,
            "task": event.task_id,
            "run": event.run_count,
            "event": event.event_id,
        },
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_task_event_cursor(
    cursor: str,
    *,
    task_id: str,
    run_count: int,
) -> int:
    if not cursor or len(cursor) > MAX_TASK_EVENT_CURSOR_LENGTH:
        raise InvalidTaskEventCursorError("Invalid Task event cursor")
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.b64decode(
            cursor + padding,
            altchars=b"-_",
            validate=True,
        )
        payload = json.loads(raw.decode("utf-8"))
    except (
        binascii.Error,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        raise InvalidTaskEventCursorError("Invalid Task event cursor") from exc
    if not isinstance(payload, dict) or set(payload) != {
        "v",
        "task",
        "run",
        "event",
    }:
        raise InvalidTaskEventCursorError("Invalid Task event cursor")
    version = payload.get("v")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or version != TASK_EVENT_CURSOR_VERSION
    ):
        raise InvalidTaskEventCursorError("Unsupported Task event cursor version")
    bound_run = payload.get("run")
    if (
        payload.get("task") != task_id
        or not isinstance(bound_run, int)
        or isinstance(bound_run, bool)
        or bound_run != run_count
    ):
        raise InvalidTaskEventCursorError("Task event cursor binding mismatch")
    event_id = payload.get("event")
    if (
        not isinstance(event_id, int)
        or isinstance(event_id, bool)
        or event_id <= 0
    ):
        raise InvalidTaskEventCursorError("Invalid Task event cursor position")
    return event_id


def _build_reconciled_anchor(
    record: TaskRecord,
) -> tuple[str, dict[str, Any], datetime]:
    observed_at = datetime.now(UTC)
    return (
        lifecycle_event_type(record),
        lifecycle_event_data(record, reconciled=True),
        observed_at,
    )


def _stored_event_to_stream_event(event: StoredTaskEvent) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type=event.event_type,
        task_id=event.task_id,
        run_count=event.run_count,
        created_at=event.created_at,
        data=dict(event.data),
        event_id=encode_task_event_cursor(event),
    )


def _stream_end_event(
    *,
    task_id: str,
    run_count: int,
    reason: str,
) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type="stream.end",
        task_id=task_id,
        run_count=run_count,
        created_at=datetime.now(UTC),
        data={"reason": reason},
    )


def _end_reason_for_record(record: TaskRecord) -> str | None:
    if record.pending_review:
        return "review_required"
    return {
        "waiting_for_human": "review_required",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
        "interrupted": "interrupted",
    }.get(record.state)


def _end_reason_from_data(data: dict[str, Any]) -> str | None:
    if data.get("pending_review"):
        return "review_required"
    status = data.get("status")
    if status == "waiting_for_human":
        return "review_required"
    if status in {"completed", "failed", "cancelled", "interrupted"}:
        return str(status)
    return None


def _bounded_text(
    value: Any,
    *,
    max_chars: int = MAX_EVENT_TEXT_LENGTH,
    max_json_bytes: int = MAX_LIFECYCLE_TEXT_JSON_BYTES,
) -> tuple[str | None, bool]:
    if not isinstance(value, str):
        return None, False
    normalized = normalize_task_event_text(value)
    candidate = normalized[:max_chars]
    truncated = normalized != value or len(candidate) != len(normalized)
    if _json_encoded_size(candidate) <= max_json_bytes:
        return candidate, truncated

    low = 0
    high = len(candidate)
    while low < high:
        midpoint = (low + high + 1) // 2
        if _json_encoded_size(candidate[:midpoint]) <= max_json_bytes:
            low = midpoint
        else:
            high = midpoint - 1
    return candidate[:low], True


def _project_pending_review(value: Any) -> tuple[dict[str, Any] | None, bool]:
    if not isinstance(value, dict) or not value:
        return None, bool(value)
    projected: dict[str, Any] = {}
    truncated = False
    for key in ("review_id", "source_task_id"):
        item = value.get(key)
        if isinstance(item, str) and item:
            bounded, item_truncated = _bounded_text(
                item,
                max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
                max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
            )
            projected[key] = bounded
            truncated = truncated or item_truncated
        elif item is not None:
            truncated = True

    raw_actions = value.get("action_requests")
    if isinstance(raw_actions, list):
        actions: list[dict[str, str]] = []
        for item in raw_actions[:MAX_REVIEW_ITEMS]:
            if not isinstance(item, dict):
                truncated = True
                continue
            name = item.get("name")
            if isinstance(name, str) and name:
                bounded, item_truncated = _bounded_text(
                    name,
                    max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
                    max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
                )
                candidate = [*actions, {"name": bounded or ""}]
                candidate_projection = {
                    **projected,
                    "action_requests": candidate,
                }
                if (
                    _json_encoded_size(candidate_projection)
                    > MAX_PENDING_REVIEW_JSON_BYTES
                ):
                    truncated = True
                    break
                actions = candidate
                truncated = truncated or item_truncated
            else:
                truncated = True
        if len(raw_actions) > MAX_REVIEW_ITEMS:
            truncated = True
        projected["action_requests"] = actions
    elif raw_actions is not None:
        truncated = True

    raw_configs = value.get("review_configs")
    if isinstance(raw_configs, list):
        configs: list[dict[str, Any]] = []
        for item in raw_configs[:MAX_REVIEW_ITEMS]:
            if not isinstance(item, dict):
                truncated = True
                continue
            action_name = item.get("action_name")
            allowed = item.get("allowed_decisions")
            if not isinstance(action_name, str) or not action_name:
                truncated = True
                continue
            bounded_name, name_truncated = _bounded_text(
                action_name,
                max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
                max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
            )
            decisions: list[str] = []
            if isinstance(allowed, list):
                for choice in allowed[:MAX_REVIEW_DECISIONS]:
                    if not isinstance(choice, str) or not choice:
                        truncated = True
                        continue
                    bounded_choice, choice_truncated = _bounded_text(
                        choice,
                        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
                        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
                    )
                    decisions.append(bounded_choice or "")
                    truncated = truncated or choice_truncated
                if len(allowed) > MAX_REVIEW_DECISIONS:
                    truncated = True
            else:
                truncated = True
            candidate = [
                *configs,
                {
                    "action_name": bounded_name or "",
                    "allowed_decisions": decisions,
                },
            ]
            candidate_projection = {
                **projected,
                "review_configs": candidate,
            }
            if (
                _json_encoded_size(candidate_projection)
                > MAX_PENDING_REVIEW_JSON_BYTES
            ):
                truncated = True
                break
            configs = candidate
            truncated = truncated or name_truncated
        if len(raw_configs) > MAX_REVIEW_ITEMS:
            truncated = True
        projected["review_configs"] = configs
    elif raw_configs is not None:
        truncated = True
    return projected or None, truncated


def _project_artifacts(
    values: list[PublishedArtifact],
) -> tuple[list[dict[str, Any]], bool]:
    projected: list[dict[str, Any]] = []
    encoded_size = 2
    truncated = len(values) > MAX_EVENT_ARTIFACTS
    for value in values[:MAX_EVENT_ARTIFACTS]:
        item, item_truncated = _project_artifact(value)
        item_size = _json_encoded_size(item)
        candidate_size = encoded_size + item_size + (1 if projected else 0)
        if candidate_size > MAX_ARTIFACT_LIST_JSON_BYTES:
            truncated = True
            break
        projected.append(item)
        encoded_size = candidate_size
        truncated = truncated or item_truncated
    if len(projected) < min(len(values), MAX_EVENT_ARTIFACTS):
        truncated = True
    return projected, truncated


def _project_artifact(
    value: PublishedArtifact,
) -> tuple[dict[str, Any], bool]:
    artifact_id, artifact_id_truncated = _bounded_text(
        value.artifact_id,
        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
    )
    name, name_truncated = _bounded_text(
        value.name,
        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
    )
    content_type, content_type_truncated = _bounded_text(
        value.content_type,
        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
    )
    caption, caption_truncated = _bounded_text(
        value.caption,
        max_json_bytes=MAX_ARTIFACT_CAPTION_JSON_BYTES,
    )
    return {
        "artifact_id": artifact_id or "",
        "name": name or "",
        "caption": caption,
        "content_type": content_type or "",
        "size": value.size,
        "run_count": value.run_count,
    }, any(
        (
            artifact_id_truncated,
            name_truncated,
            content_type_truncated,
            caption_truncated,
        )
    )


def _json_encoded_size(value: Any) -> int:
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _ensure_durable_event_data_fits(data: dict[str, Any]) -> None:
    if _json_encoded_size(data) > MAX_DURABLE_TASK_EVENT_DATA_BYTES:
        raise ValueError("Projected durable Task event exceeds its wire budget")


def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()
