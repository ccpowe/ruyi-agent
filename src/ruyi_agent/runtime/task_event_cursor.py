"""Compatibility facade for neutral opaque Task event cursors."""

from ruyi_agent.gateway_protocol.contracts import InvalidTaskEventCursorError
from ruyi_agent.gateway_protocol.cursor import (
    decode_task_event_cursor,
    encode_task_event_cursor,
)

__all__ = [
    "InvalidTaskEventCursorError",
    "decode_task_event_cursor",
    "encode_task_event_cursor",
]
