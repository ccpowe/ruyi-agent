"""Gateway-internal models that never cross the public protocol boundary."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PreparedInput:
    content: str
    attachment_metadata: list[dict[str, str]]


@dataclass(slots=True)
class GatewayArtifact:
    path: str
    filename: str
    content: bytes
    content_type: str
