"""Gateway HTTP transport request and probe schemas."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from ruyi_agent.gateway.models import AttachmentInput, MetadataScalar


class TaskInput(BaseModel):
    content: str = ""
    attachments: list[AttachmentInput] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def require_content_or_attachments(self) -> "TaskInput":
        if self.content.strip() or self.attachments:
            return self
        raise ValueError("input.content or input.attachments is required")


class HealthProbeResponse(BaseModel):
    status: Literal["ok"]


class ReadyProbeResponse(BaseModel):
    status: Literal["ready"]


class NotReadyProbeResponse(BaseModel):
    status: Literal["not_ready"]


class CreateTaskRequest(BaseModel):
    input: TaskInput
    metadata: dict[str, MetadataScalar] = Field(default_factory=dict)
    webhook: dict[str, MetadataScalar] | None = None


class SendInputRequest(BaseModel):
    input: TaskInput


class ReviewDecisionInput(BaseModel):
    decisions: list[dict[str, Any]] = Field(min_length=1)


class ArtifactDownloadRequest(BaseModel):
    path: str = Field(min_length=1)
