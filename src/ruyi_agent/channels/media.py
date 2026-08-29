from __future__ import annotations

from collections.abc import AsyncIterable, Mapping, Sequence


class MediaLimitError(ValueError):
    """A remote media body violated the configured hard size contract."""


def validate_content_length(
    headers: Mapping[str, str],
    *,
    max_bytes: int,
    values: Sequence[str] | None = None,
) -> int | None:
    if max_bytes <= 0:
        raise ValueError("media max bytes must be positive")
    raw_values = list(values or ())
    if not raw_values:
        raw = headers.get("content-length")
        if raw is None:
            return None
        raw_values = [raw]
    if len(raw_values) != 1:
        raise MediaLimitError("media download rejected: invalid Content-Length")
    raw_value = raw_values[0].strip()
    if not raw_value.isascii() or not raw_value.isdigit():
        raise MediaLimitError("media download rejected: invalid Content-Length")
    declared = int(raw_value)
    if declared > max_bytes:
        raise MediaLimitError(
            f"media download rejected: payload exceeds {max_bytes} bytes"
        )
    return declared


async def read_bounded_media(
    chunks: AsyncIterable[bytes],
    *,
    max_bytes: int,
) -> bytes:
    """Read at most ``max_bytes + 1`` bytes before rejecting the body."""

    if max_bytes <= 0:
        raise ValueError("media max bytes must be positive")
    body = bytearray()
    async for chunk in chunks:
        if not chunk:
            continue
        remaining_probe = max_bytes + 1 - len(body)
        if remaining_probe > 0:
            body.extend(chunk[:remaining_probe])
        if len(body) > max_bytes or len(chunk) > remaining_probe:
            raise MediaLimitError(
                f"media download rejected: payload exceeds {max_bytes} bytes"
            )
    return bytes(body)
