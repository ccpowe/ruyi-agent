from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RuntimeEventKind(str, Enum):
    TURN_STARTED = "turn_started"
    # stream_mode="updates" produces display-sized message updates, not raw token deltas.
    CONTENT_UPDATE = "content_update"
    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_RESULT = "tool_result"
    REVIEW_REQUESTED = "review_requested"
    TASK_UPDATED = "task_updated"
    ERROR_OCCURRED = "error_occurred"
    TURN_COMPLETED = "turn_completed"


@dataclass(slots=True)
class RuntimeEvent:
    kind: RuntimeEventKind
    text: str = ""
    agent_name: str | None = None
    thread_id: str | None = None
    task_id: str | None = None
    review_id: str | None = None
    namespace: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
