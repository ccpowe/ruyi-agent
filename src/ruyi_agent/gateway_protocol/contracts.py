from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal
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
TaskLifecycleEventType = Literal["task.created", "task.running", "task.review_requested", "task.completed", "task.failed", "task.cancelled", "task.interrupted", "task.artifact_published"]
TERMINAL_EVENT_TYPES = {"task.completed", "task.failed", "task.cancelled", "task.interrupted"}
FULL_STATE_EVENT_TYPES = {"task.snapshot", "task.created", "task.running", "task.review_requested", *TERMINAL_EVENT_TYPES}
class TaskEventsUnavailableError(RuntimeError):
    pass
class InvalidTaskEventCursorError(ValueError):
    pass
class TaskRunMismatchError(ValueError):
    def __init__(self, *, requested: int, current: int) -> None:
        super().__init__(f"Requested task run {requested} does not match current run {current}")
        self.requested = requested
        self.current = current
@dataclass(frozen=True, slots=True)
class TaskStreamEvent:
    event_type: str
    task_id: str
    run_count: int
    created_at: datetime
    data: dict[str, Any]
    event_id: str | None = None
