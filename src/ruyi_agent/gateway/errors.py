from __future__ import annotations

from enum import StrEnum
from typing import Any


class GatewayEffectDisposition(StrEnum):
    """Authoritative classification of whether a Gateway effect was sent."""

    NOT_DISPATCHED = "not_dispatched"
    AUTHORITATIVE_REJECTION = "authoritative_rejection"
    OUTCOME_UNKNOWN = "outcome_unknown"


class GatewayTaskError(Exception):
    """A transport-neutral failure exposed by the Gateway Task Interface."""

    def __init__(
        self,
        *,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        kind: str | None = None,
        effect_disposition: GatewayEffectDisposition | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details
        self.kind = kind
        self.effect_disposition = effect_disposition
