from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal


MAX_JSON_RESPONSE_BODY_BYTES, MAX_JSON_ERROR_BODY_BYTES = 8 << 20, 1 << 16
MAX_JSON_READ_SECONDS, MAX_JSON_DEPTH = 5.0, 100
MAX_GATEWAY_ERROR_TEXT_LENGTH, MAX_GATEWAY_ERROR_DETAILS_BYTES = 4096, 1 << 16
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
FULL_STATE_EVENT_TYPES = {
    "task.snapshot",
    "task.created",
    "task.running",
    "task.review_requested",
    *TERMINAL_EVENT_TYPES,
}


class TaskEventsUnavailableError(RuntimeError):
    pass


class InvalidTaskEventCursorError(ValueError):
    pass


class TaskRunMismatchError(ValueError):
    def __init__(self, *, requested: int, current: int) -> None:
        super().__init__(
            f"Requested task run {requested} does not match current run {current}"
        )
        self.requested = requested
        self.current = current


class BoundedBodyLimitError(ValueError):
    pass


class BoundedBodyTimeoutError(TimeoutError):
    pass


_OMIT = object()


async def read_bounded_bytes(
    chunks: AsyncIterable[bytes],
    *,
    max_bytes: int,
    timeout_seconds: float | None = None,
) -> bytes:
    if max_bytes <= 0:
        raise ValueError("max_bytes must be positive")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    try:
        async with asyncio.timeout(timeout_seconds):
            body = bytearray()
            async for chunk in chunks:
                if not chunk:
                    continue
                remaining = max_bytes + 1 - len(body)
                if len(chunk) >= remaining:
                    body.extend(chunk[:remaining])
                    raise BoundedBodyLimitError("response body exceeds the size limit")
                body.extend(chunk)
    except TimeoutError as exc:
        raise BoundedBodyTimeoutError("response body timed out") from exc
    return bytes(body)


def decode_strict_json_bytes(body: bytes, *, max_bytes: int) -> Any:
    if len(body) > max_bytes:
        raise ValueError("JSON response exceeds the size limit")
    return decode_strict_json_text(body.decode("utf-8", errors="strict"))


def decode_strict_json_text(value: str) -> Any:
    _validate_json_depth(value)
    return json.loads(value, parse_constant=_reject_non_finite_json_number)


def normalize_gateway_error_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or not isinstance(payload.get("error"), dict):
        return {}
    raw_error = payload["error"]
    error: dict[str, Any] = {
        "code": _normalize_error_text(raw_error.get("code"), "gateway_error"),
        "message": _normalize_error_text(
            raw_error.get("message"), "Gateway request failed"
        ),
    }
    if "details" in raw_error:
        details = _normalize_error_details(raw_error["details"], 0)
        if (
            details is not _OMIT
            and details is not None
            and _json_size(details) <= MAX_GATEWAY_ERROR_DETAILS_BYTES
        ):
            error["details"] = details
    return {"error": error}


def _normalize_error_text(value: Any, fallback: str) -> str:
    if not isinstance(value, str):
        return fallback
    return _replace_surrogates(value)[:MAX_GATEWAY_ERROR_TEXT_LENGTH]


def _replace_surrogates(value: str) -> str:
    return re.sub(r"[\ud800-\udfff]", "\ufffd", value)


def _normalize_error_details(value: Any, depth: int) -> Any:
    if depth > MAX_JSON_DEPTH:
        return _OMIT
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, float):
        return value if value == value and abs(value) != float("inf") else _OMIT
    if isinstance(value, str):
        return _replace_surrogates(value)[:MAX_GATEWAY_ERROR_TEXT_LENGTH]
    if isinstance(value, list):
        return [
            normalized
            for item in value
            if (normalized := _normalize_error_details(item, depth + 1)) is not _OMIT
        ]
    if isinstance(value, dict):
        return {
            _replace_surrogates(key)[:MAX_GATEWAY_ERROR_TEXT_LENGTH]: normalized
            for key, item in value.items()
            if isinstance(key, str)
            and (normalized := _normalize_error_details(item, depth + 1)) is not _OMIT
        }
    return _OMIT


def _json_size(value: Any) -> int:
    try:
        return len(
            json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    except (TypeError, UnicodeEncodeError, ValueError):
        return MAX_GATEWAY_ERROR_DETAILS_BYTES + 1


def _reject_non_finite_json_number(value: str) -> None:
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def _validate_json_depth(value: str) -> None:
    depth = 0
    in_string = False
    escaped = False
    for character in value:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > MAX_JSON_DEPTH:
                raise ValueError("JSON nesting exceeds the limit")
        elif character in "]}":
            depth = max(0, depth - 1)


@dataclass(frozen=True, slots=True)
class TaskStreamEvent:
    event_type: str
    task_id: str
    run_count: int
    created_at: datetime
    data: dict[str, Any]
    event_id: str | None = None
