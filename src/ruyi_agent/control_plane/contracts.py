"""Shared human-review contracts used by runtime and channel Adapters."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ReviewModel(BaseModel):
    """Strict base model for persisted and user-supplied review data."""

    model_config = ConfigDict(extra="forbid")


class ReviewDecisionKind(str, Enum):
    APPROVE = "approve"
    REJECT = "reject"
    EDIT = "edit"


class ReviewDecision(ReviewModel):
    """One user decision for a pending tool action."""

    action_id: str | None = None
    decision: ReviewDecisionKind
    message: str | None = None
    edited_args: dict[str, Any] | None = None


class ReviewActionSnapshot(ReviewModel):
    """User-visible projection of one pending tool action."""

    action_id: str | None = None
    tool_name: str
    args: dict[str, Any] = Field(default_factory=dict)
    description: str | None = None
    risk: str | None = None
    reason: str | None = None
    allowed_decisions: list[ReviewDecisionKind] = Field(default_factory=list)


class ReviewSnapshot(ReviewModel):
    """Stable view of one pending or resolved review."""

    review_id: str
    task_id: str
    agent_name: str | None = None
    thread_id: str | None = None
    status: Literal["pending", "resolved"] = "pending"
    actions: list[ReviewActionSnapshot] = Field(default_factory=list)
    allowed_decisions: list[ReviewDecisionKind] = Field(default_factory=list)
    risk: str | None = None
    reason: str | None = None
    created_at: datetime
    updated_at: datetime
