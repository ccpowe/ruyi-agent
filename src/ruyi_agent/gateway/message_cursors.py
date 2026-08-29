"""Opaque, task-bound cursors for Gateway message history pages."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import Mapping

from ruyi_agent.gateway.errors import GatewayTaskError

TASK_MESSAGE_CURSOR_VERSION = 1
MAX_TASK_MESSAGE_CURSOR_LENGTH = 4096


def encode_task_message_cursor(
    *,
    task_id: str,
    checkpoint_id: str,
    offset: int,
) -> str:
    payload = json.dumps(
        {
            "checkpoint_id": checkpoint_id,
            "offset": offset,
            "task_id": task_id,
            "version": TASK_MESSAGE_CURSOR_VERSION,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def decode_task_message_cursor(
    cursor: str | None,
    *,
    task_id: str,
) -> tuple[str | None, int]:
    if cursor is None:
        return None, 0
    if not cursor or len(cursor) > MAX_TASK_MESSAGE_CURSOR_LENGTH:
        raise invalid_message_cursor()
    try:
        padding = b"=" * (-len(cursor) % 4)
        raw = base64.b64decode(
            cursor.encode("ascii") + padding,
            altchars=b"-_",
            validate=True,
        )
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, binascii.Error, UnicodeError) as exc:
        raise invalid_message_cursor() from exc
    if not isinstance(payload, Mapping) or set(payload) != {
        "checkpoint_id",
        "offset",
        "task_id",
        "version",
    }:
        raise invalid_message_cursor()
    version = payload.get("version")
    checkpoint_id = payload.get("checkpoint_id")
    bound_task_id = payload.get("task_id")
    offset = payload.get("offset")
    if (
        version != TASK_MESSAGE_CURSOR_VERSION
        or isinstance(version, bool)
        or not isinstance(checkpoint_id, str)
        or not checkpoint_id
        or len(checkpoint_id) > 512
        or bound_task_id != task_id
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
    ):
        raise invalid_message_cursor()
    return checkpoint_id, offset


def invalid_message_cursor() -> GatewayTaskError:
    return GatewayTaskError(
        code="invalid_request",
        message="Query parameter 'cursor' is invalid",
    )
