from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ruyi_agent.runtime.task_events import (
    MAX_ASSISTANT_DELTA_TEXT_LENGTH,
    MAX_EVENT_ARTIFACTS,
    MAX_EVENT_TEXT_LENGTH,
    MAX_REVIEW_DECISIONS,
    MAX_REVIEW_ITEMS,
    MAX_SHORT_EVENT_TEXT_LENGTH,
    MAX_TASK_EVENT_CURSOR_LENGTH,
    TaskStreamEvent,
    normalize_task_event_text,
)
from ruyi_agent.task_models import TaskState, parse_task_state


MAX_SSE_LINE_BYTES = 768 * 1024
MAX_SSE_EVENT_BYTES = 768 * 1024
MAX_SSE_ERROR_BODY_BYTES = 64 * 1024
MAX_SSE_ERROR_READ_SECONDS = 5.0
MAX_SSE_HANDSHAKE_SECONDS = 10.0
MAX_SSE_JSON_DEPTH = 100

LIFECYCLE_EVENT_TYPES = {
    "task.created",
    "task.running",
    "task.review_requested",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
}
SUPPORTED_TASK_EVENT_TYPES = LIFECYCLE_EVENT_TYPES | {
    "task.snapshot",
    "task.artifact_published",
    "assistant.delta",
    "stream.error",
    "stream.end",
}
_DURABLE_WIRE_EVENT_TYPES = LIFECYCLE_EVENT_TYPES | {
    "task.snapshot",
    "task.artifact_published",
}
_STREAM_END_REASONS = {
    "review_required",
    "completed",
    "failed",
    "cancelled",
    "interrupted",
    "superseded",
    "error",
}


class SSEProtocolError(ValueError):
    """An SSE peer returned a malformed or unsafe Task event."""


@dataclass(frozen=True, slots=True)
class GatewayTaskEvent:
    """One decoded public SSE record returned by a Gateway client."""

    event_type: str
    data: dict[str, Any]
    event_id: str | None = None


def is_valid_task_event_cursor(value: object) -> bool:
    """Return whether an event id can be sent back as Last-Event-ID unchanged."""

    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= MAX_TASK_EVENT_CURSOR_LENGTH
        and value == value.strip(" ")
        and all(0x20 <= ord(character) <= 0x7E for character in value)
    )


def has_identity_content_encoding(value: str) -> bool:
    """SSE readers reject HTTP content transforms before bounded parsing."""

    return not value.strip() or value.strip().lower() == "identity"


async def read_bounded_sse_error_body(
    chunks: AsyncIterator[bytes],
    *,
    timeout_seconds: float = MAX_SSE_ERROR_READ_SECONDS,
) -> bytes:
    """Read a non-stream error response without inheriting infinite SSE reads."""

    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")

    async def collect() -> bytes:
        body = bytearray()
        async for chunk in chunks:
            if len(body) + len(chunk) > MAX_SSE_ERROR_BODY_BYTES:
                raise SSEProtocolError("SSE error response exceeds the size limit")
            body.extend(chunk)
        return bytes(body)

    try:
        async with asyncio.timeout(timeout_seconds):
            return await collect()
    except TimeoutError as exc:
        raise SSEProtocolError("SSE error response timed out") from exc


def decode_sse_error_json(body: bytes) -> Any:
    """Decode a bounded HTTP error payload with the SSE JSON safety limits."""

    if len(body) > MAX_SSE_ERROR_BODY_BYTES:
        raise SSEProtocolError("SSE error response exceeds the size limit")
    try:
        value = body.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SSEProtocolError("SSE error response is not valid UTF-8") from exc
    return _decode_json_text(value)


def encode_task_stream_event(event: TaskStreamEvent) -> bytes:
    """Encode one transport-neutral Task event as a single SSE record."""

    payload = {
        "task_id": event.task_id,
        "run_count": event.run_count,
        "created_at": _isoformat(event.created_at),
        **event.data,
    }
    return encode_gateway_task_event(
        GatewayTaskEvent(
            event_type=event.event_type,
            event_id=event.event_id,
            data=payload,
        )
    )


def encode_gateway_task_event(event: GatewayTaskEvent) -> bytes:
    if (
        not event.event_type
        or "\n" in event.event_type
        or "\r" in event.event_type
    ):
        raise SSEProtocolError("Invalid SSE event name")
    if event.event_id is not None and not is_valid_task_event_cursor(
        event.event_id
    ):
        raise SSEProtocolError("Invalid SSE event id")
    try:
        encoded_data = json.dumps(
            event.data,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        encoded_data_bytes = encoded_data.encode("utf-8")
    except (TypeError, UnicodeEncodeError, ValueError) as exc:
        raise SSEProtocolError("SSE event data is not JSON serializable") from exc
    if len(encoded_data_bytes) > MAX_SSE_EVENT_BYTES:
        raise SSEProtocolError("SSE event data exceeds the size limit")

    lines: list[str] = []
    if event.event_id is not None:
        lines.append(f"id: {event.event_id}")
    lines.extend((f"event: {event.event_type}", f"data: {encoded_data}"))
    try:
        encoded_lines = [line.encode("utf-8") for line in lines]
    except UnicodeEncodeError as exc:
        raise SSEProtocolError("SSE record is not valid UTF-8") from exc
    if any(len(line) > MAX_SSE_LINE_BYTES for line in encoded_lines):
        raise SSEProtocolError("SSE line exceeds the size limit")
    if sum(len(line) for line in encoded_lines) > MAX_SSE_EVENT_BYTES:
        raise SSEProtocolError("SSE record exceeds the size limit")
    return b"\n".join(encoded_lines) + b"\n\n"


async def iter_utf8_sse_lines(
    chunks: AsyncIterator[bytes],
) -> AsyncIterator[str]:
    """Decode the WHATWG UTF-8 line grammar without unbounded line buffering."""

    buffer = bytearray()
    skip_lf = False
    first_line = True
    async for chunk in chunks:
        for byte in chunk:
            if skip_lf:
                skip_lf = False
                if byte == 0x0A:
                    continue
            if byte in {0x0A, 0x0D}:
                line = _decode_utf8_sse_line(buffer, strip_bom=first_line)
                first_line = False
                buffer.clear()
                yield line
                skip_lf = byte == 0x0D
                continue
            if len(buffer) >= MAX_SSE_LINE_BYTES:
                raise SSEProtocolError("SSE line exceeds the size limit")
            buffer.append(byte)

    if buffer:
        yield _decode_utf8_sse_line(buffer, strip_bom=first_line)


async def iter_gateway_task_events(
    lines: AsyncIterator[str],
) -> AsyncIterator[GatewayTaskEvent]:
    """Incrementally decode the bounded SSE subset emitted by the Gateway."""

    event_type: str | None = None
    event_id: str | None = None
    data_lines: list[str] = []
    event_bytes = 0

    async for line in lines:
        line_bytes = len(line.encode("utf-8"))
        if line_bytes > MAX_SSE_LINE_BYTES:
            raise SSEProtocolError("SSE line exceeds the size limit")
        if line == "":
            if event_type is not None or data_lines or event_id is not None:
                yield _decode_record(event_type, event_id, data_lines, event_bytes)
            event_type = None
            event_id = None
            data_lines = []
            event_bytes = 0
            continue
        if line.startswith(":"):
            continue

        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        event_bytes += line_bytes
        if event_bytes > MAX_SSE_EVENT_BYTES:
            raise SSEProtocolError("SSE record exceeds the size limit")
        if field == "event":
            event_type = value
        elif field == "id":
            if "\x00" in value:
                raise SSEProtocolError("SSE id contains a null character")
            event_id = value
        elif field == "data":
            data_lines.append(value)
        elif field == "retry":
            if value and not value.isdecimal():
                raise SSEProtocolError("SSE retry field is invalid")
        else:
            raise SSEProtocolError("SSE record contains an unsupported field")

    # WHATWG dispatches a record only on a blank line.  A peer that closes in
    # the middle of a record has not delivered that event.


def task_stream_event_from_gateway(
    event: GatewayTaskEvent,
    *,
    expected_task_id: str,
    public_task_id: str,
    run_count: int,
) -> TaskStreamEvent:
    """Validate an untrusted downstream event and rewrite only its Task id."""

    if event.event_type not in SUPPORTED_TASK_EVENT_TYPES:
        raise SSEProtocolError("Remote Gateway returned an unsupported Task event")
    if event.event_type in _DURABLE_WIRE_EVENT_TYPES:
        if not is_valid_task_event_cursor(event.event_id):
            raise SSEProtocolError(
                "Remote durable Task event has an invalid event id"
            )
    elif event.event_id is not None:
        raise SSEProtocolError("Remote transient Task event unexpectedly has an id")

    raw = event.data
    if not isinstance(raw, dict):
        raise SSEProtocolError("Remote Task event data is not an object")
    if raw.get("task_id") != expected_task_id:
        raise SSEProtocolError("Remote Gateway returned an event for the wrong Task")
    raw_run_count = raw.get("run_count")
    if (
        not isinstance(raw_run_count, int)
        or isinstance(raw_run_count, bool)
        or raw_run_count != run_count
    ):
        raise SSEProtocolError("Remote Gateway returned an event for the wrong run")
    created_at = _parse_timestamp(raw.get("created_at"), "created_at")

    event_data = {
        key: value
        for key, value in raw.items()
        if key not in {"task_id", "run_count", "created_at"}
    }
    if event.event_type in LIFECYCLE_EVENT_TYPES | {"task.snapshot"}:
        clean_data = _validate_lifecycle_data(
            event_data,
            event_type=event.event_type,
        )
    elif event.event_type == "task.artifact_published":
        clean_data = _validate_artifact_event_data(event_data)
    elif event.event_type == "assistant.delta":
        _require_keys(event_data, {"content"})
        clean_data = {
            "content": _bounded_required_text(
                event_data.get("content"), "content"
            )[:MAX_ASSISTANT_DELTA_TEXT_LENGTH]
        }
    elif event.event_type == "stream.end":
        _require_keys(event_data, {"reason"})
        reason = event_data.get("reason")
        if not isinstance(reason, str) or reason not in _STREAM_END_REASONS:
            raise SSEProtocolError("Remote stream.end reason is invalid")
        clean_data = {"reason": reason}
    else:
        _require_keys(event_data, {"code", "message"})
        clean_data = {
            "code": _short_required_text(event_data.get("code"), "code"),
            "message": _bounded_required_text(event_data.get("message"), "message"),
        }

    stream_event = TaskStreamEvent(
        event_type=event.event_type,
        task_id=public_task_id,
        run_count=run_count,
        created_at=created_at,
        data=clean_data,
        event_id=event.event_id,
    )
    # The sanitizer is a trust boundary: never return a value the outer
    # Gateway cannot encode under the same public SSE limits.
    encode_task_stream_event(stream_event)
    return stream_event


def _decode_record(
    event_type: str | None,
    event_id: str | None,
    data_lines: list[str],
    event_bytes: int,
) -> GatewayTaskEvent:
    del event_bytes
    if not event_type:
        raise SSEProtocolError("SSE record is missing an event name")
    if not data_lines:
        raise SSEProtocolError("SSE record is missing data")
    data = _decode_json_text("\n".join(data_lines))
    if not isinstance(data, dict):
        raise SSEProtocolError("SSE record data is not an object")
    return GatewayTaskEvent(event_type=event_type, event_id=event_id, data=data)


def _decode_utf8_sse_line(value: bytearray, *, strip_bom: bool) -> str:
    try:
        decoded = bytes(value).decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SSEProtocolError("SSE stream is not valid UTF-8") from exc
    if strip_bom:
        return decoded.removeprefix("\ufeff")
    return decoded


def _reject_non_finite_json_number(value: str) -> None:
    raise ValueError(f"Non-finite JSON number is not allowed: {value}")


def _decode_json_text(value: str) -> Any:
    try:
        _validate_json_nesting(value)
        return json.loads(
            value,
            parse_constant=_reject_non_finite_json_number,
        )
    except (ValueError, RecursionError) as exc:
        raise SSEProtocolError("SSE record data is invalid JSON") from exc


def _validate_json_nesting(value: str) -> None:
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
            if depth > MAX_SSE_JSON_DEPTH:
                raise ValueError("JSON nesting exceeds the limit")
        elif character in "]}":
            depth = max(0, depth - 1)


def _validate_lifecycle_data(
    raw: dict[str, Any],
    *,
    event_type: str,
) -> dict[str, Any]:
    required = {
        "status",
        "last_result",
        "error",
        "updated_at",
        "pending_review",
        "artifacts",
    }
    optional = {
        "artifacts_truncated",
        "last_result_truncated",
        "error_truncated",
        "pending_review_truncated",
        "reconciled",
        "observed_at",
    }
    _require_keys(raw, required, optional)
    status = _parse_remote_task_state(raw.get("status"))
    clean: dict[str, Any] = {
        "status": status,
        "last_result": _bounded_optional_text(raw.get("last_result"), "last_result"),
        "error": _bounded_optional_text(raw.get("error"), "error"),
        "updated_at": _isoformat(_parse_timestamp(raw.get("updated_at"), "updated_at")),
        "pending_review": _validate_pending_review(raw.get("pending_review")),
        "artifacts": _validate_artifacts(raw.get("artifacts")),
    }
    for key in (
        "artifacts_truncated",
        "last_result_truncated",
        "error_truncated",
        "pending_review_truncated",
        "reconciled",
    ):
        if key in raw:
            if raw[key] is not True:
                raise SSEProtocolError(f"Remote Task event field '{key}' is invalid")
            clean[key] = True
    if "observed_at" in raw:
        clean["observed_at"] = _isoformat(
            _parse_timestamp(raw.get("observed_at"), "observed_at")
        )
    if ("reconciled" in raw) != ("observed_at" in raw):
        raise SSEProtocolError("Remote reconciled Task event is incomplete")
    expected_status = {
        "task.created": "pending",
        "task.running": "running",
        "task.completed": "completed",
        "task.failed": "failed",
        "task.cancelled": "cancelled",
        "task.interrupted": "interrupted",
    }.get(event_type)
    if expected_status is not None and clean["status"] != expected_status:
        raise SSEProtocolError("Remote lifecycle event status is inconsistent")
    if event_type == "task.review_requested" and not clean["pending_review"]:
        raise SSEProtocolError("Remote review event has no pending review")
    return clean


def _validate_artifact_event_data(raw: dict[str, Any]) -> dict[str, Any]:
    _require_keys(
        raw,
        {"status", "updated_at", "artifact"},
        {"artifact_truncated"},
    )
    status = _parse_remote_task_state(raw.get("status"))
    clean = {
        "status": status,
        "updated_at": _isoformat(_parse_timestamp(raw.get("updated_at"), "updated_at")),
        "artifact": _validate_artifact(raw.get("artifact")),
    }
    if "artifact_truncated" in raw:
        if raw["artifact_truncated"] is not True:
            raise SSEProtocolError("Remote artifact_truncated field is invalid")
        clean["artifact_truncated"] = True
    return clean


def _parse_remote_task_state(value: object) -> TaskState:
    try:
        return parse_task_state(value, path="Remote Task event status")
    except ValueError as exc:
        raise SSEProtocolError("Remote Task event status is invalid") from exc


def _validate_pending_review(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise SSEProtocolError("Remote pending_review is invalid")
    allowed = {"review_id", "source_task_id", "action_requests", "review_configs"}
    if not set(value) <= allowed:
        raise SSEProtocolError("Remote pending_review contains unsupported fields")
    clean: dict[str, Any] = {}
    for key in ("review_id", "source_task_id"):
        if key in value:
            clean[key] = _short_required_text(value[key], key)
    if "action_requests" in value:
        items = value["action_requests"]
        if not isinstance(items, list) or len(items) > MAX_REVIEW_ITEMS:
            raise SSEProtocolError("Remote review action_requests is invalid")
        clean["action_requests"] = []
        for item in items:
            if not isinstance(item, dict) or set(item) != {"name"}:
                raise SSEProtocolError("Remote review action request is invalid")
            clean["action_requests"].append(
                {"name": _short_required_text(item.get("name"), "name")}
            )
    if "review_configs" in value:
        items = value["review_configs"]
        if not isinstance(items, list) or len(items) > MAX_REVIEW_ITEMS:
            raise SSEProtocolError("Remote review_configs is invalid")
        clean["review_configs"] = []
        for item in items:
            if not isinstance(item, dict) or set(item) != {
                "action_name",
                "allowed_decisions",
            }:
                raise SSEProtocolError("Remote review config is invalid")
            decisions = item.get("allowed_decisions")
            if (
                not isinstance(decisions, list)
                or len(decisions) > MAX_REVIEW_DECISIONS
            ):
                raise SSEProtocolError("Remote review decisions are invalid")
            clean["review_configs"].append(
                {
                    "action_name": _short_required_text(
                        item.get("action_name"), "action_name"
                    ),
                    "allowed_decisions": [
                        _short_required_text(choice, "allowed_decisions")
                        for choice in decisions
                    ],
                }
            )
    return clean


def _validate_artifacts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > MAX_EVENT_ARTIFACTS:
        raise SSEProtocolError("Remote artifacts field is invalid")
    return [_validate_artifact(item) for item in value]


def _validate_artifact(value: Any) -> dict[str, Any]:
    keys = {"artifact_id", "name", "caption", "content_type", "size", "run_count"}
    if not isinstance(value, dict) or set(value) != keys:
        raise SSEProtocolError("Remote artifact projection is invalid")
    size = value.get("size")
    artifact_run = value.get("run_count")
    if (
        not isinstance(size, int)
        or isinstance(size, bool)
        or size < 0
        or not isinstance(artifact_run, int)
        or isinstance(artifact_run, bool)
        or artifact_run < 0
    ):
        raise SSEProtocolError("Remote artifact numeric fields are invalid")
    caption = value.get("caption")
    if caption is not None and not isinstance(caption, str):
        raise SSEProtocolError("Remote artifact caption is invalid")
    if caption is not None:
        caption = normalize_task_event_text(caption)
    return {
        "artifact_id": _short_required_text(value.get("artifact_id"), "artifact_id"),
        "name": _short_required_text(value.get("name"), "name"),
        "caption": caption,
        "content_type": _short_required_text(
            value.get("content_type"), "content_type"
        ),
        "size": size,
        "run_count": artifact_run,
    }


def _require_keys(
    value: dict[str, Any],
    required: set[str],
    optional: set[str] | None = None,
) -> None:
    allowed = required | (optional or set())
    if not required <= set(value) or not set(value) <= allowed:
        raise SSEProtocolError("Remote Task event fields do not match the schema")


def _bounded_optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _bounded_required_text(value, field)


def _bounded_required_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) > MAX_EVENT_TEXT_LENGTH:
        raise SSEProtocolError(f"Remote Task event field '{field}' is invalid")
    return normalize_task_event_text(value)


def _short_required_text(value: Any, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > MAX_SHORT_EVENT_TEXT_LENGTH
    ):
        raise SSEProtocolError(f"Remote Task event field '{field}' is invalid")
    return normalize_task_event_text(value)


def _parse_timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or len(value) > 128:
        raise SSEProtocolError(f"Remote Task event field '{field}' is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SSEProtocolError(
            f"Remote Task event field '{field}' is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise SSEProtocolError(f"Remote Task event field '{field}' has no timezone")
    return parsed


def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()
