from __future__ import annotations
import base64
import binascii
import json
from typing import Any
from ruyi_agent.gateway_protocol.contracts import (
    MAX_TASK_EVENT_CURSOR_LENGTH,
    TASK_EVENT_CURSOR_VERSION,
    InvalidTaskEventCursorError,
)


def encode_task_event_cursor(event: Any) -> str:
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


def decode_task_event_cursor(cursor: str, *, task_id: str, run_count: int) -> int:
    if not cursor or len(cursor) > MAX_TASK_EVENT_CURSOR_LENGTH:
        raise InvalidTaskEventCursorError("Invalid Task event cursor")
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.b64decode(cursor + padding, altchars=b"-_", validate=True)
        payload = json.loads(raw.decode("utf-8"))
    except (
        binascii.Error,
        UnicodeDecodeError,
        json.JSONDecodeError,
        ValueError,
    ) as exc:
        raise InvalidTaskEventCursorError("Invalid Task event cursor") from exc
    if not isinstance(payload, dict) or set(payload) != {"v", "task", "run", "event"}:
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
    if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id <= 0:
        raise InvalidTaskEventCursorError("Invalid Task event cursor position")
    return event_id
