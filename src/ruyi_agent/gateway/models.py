from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

MetadataScalar = str | int | float | bool | None


@dataclass(slots=True)
class TaskRouteRecord:
    task_id: str
    agent_name: str
    metadata: dict[str, MetadataScalar]
    route_kind: str
    upstream_task_id: str
    webhook: dict[str, MetadataScalar] | None = None


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


class AgentRefResponse(BaseModel):
    name: str
    kind: Literal["local", "remote_ref"]
    public: bool
    description: str
    is_default: bool
    available: bool = True
    unavailable_reason: str | None = None


class AgentListResponse(BaseModel):
    items: list[AgentRefResponse]


class AttachmentInput(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    data_base64: str = Field(min_length=1)
    content_type: str | None = Field(default=None, max_length=255)
    kind: Literal["image", "document", "audio", "video", "file"] = "file"


class TaskResponse(BaseModel):
    task_id: str
    agent_name: str
    parent_task_id: str | None
    root_task_id: str
    depth: int
    status: Literal[
        "pending",
        "running",
        "waiting_for_human",
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    ]
    last_result: str | None
    error: str | None
    run_count: int
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, MetadataScalar]
    pending_review: dict[str, Any] | None = None
    artifacts: list["PublishedArtifactResponse"] = Field(default_factory=list)


class PublishedArtifactResponse(BaseModel):
    artifact_id: str
    path: str
    name: str
    caption: str | None = None
    content_type: str
    size: int
    run_count: int


class TaskListResponse(BaseModel):
    items: list[TaskResponse]
    next_cursor: str | None


class ReviewResponse(BaseModel):
    review_id: str
    task_id: str
    thread_id: str | None
    agent_name: str
    route_kind: str
    status: Literal["pending"]
    action_requests: list[dict[str, Any]]
    review_configs: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, MetadataScalar]


class ReviewListResponse(BaseModel):
    items: list[ReviewResponse]
    next_cursor: str | None


class TaskWebhookEvent(BaseModel):
    event_id: str
    event_type: str
    task_id: str
    agent_name: str
    status: Literal[
        "pending",
        "running",
        "waiting_for_human",
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    ]
    last_result: str | None = None
    error: str | None = None
    run_count: int
    created_at: datetime
    updated_at: datetime
