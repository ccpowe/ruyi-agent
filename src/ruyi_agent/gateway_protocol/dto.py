from __future__ import annotations
from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeAlias, TypeVar
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    model_validator,
)
from ruyi_agent.task_models import MetadataScalar, TaskState, parse_task_state

ValueT = TypeVar("ValueT")
ParsedTaskState: TypeAlias = Annotated[TaskState, BeforeValidator(parse_task_state)]


class GatewayDTO(BaseModel, Mapping[str, Any]):
    model_config = ConfigDict(extra="ignore", frozen=True)

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as exc:
            raise KeyError(key) from exc

    def __iter__(self) -> Iterator[str]:
        return iter(type(self).model_fields)

    def __len__(self) -> int:
        return len(type(self).model_fields)

    def get(self, key: str, default: ValueT | None = None) -> Any | ValueT | None:
        return getattr(self, key, default)

    def to_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class GatewayAgent(GatewayDTO):
    name: StrictStr = Field(min_length=1)
    public: StrictBool
    description: StrictStr = ""
    kind: Literal["local", "remote_ref"] | None = None
    is_default: StrictBool = False
    available: StrictBool = True
    unavailable_reason: StrictStr | None = None


class GatewayPendingReview(GatewayDTO):
    review_id: StrictStr = Field(min_length=1)
    action_requests: list[dict[str, Any]] = Field(default_factory=list)
    review_configs: list[dict[str, Any]] = Field(default_factory=list)
    source_task_id: StrictStr | None = None


class GatewayPublishedArtifact(GatewayDTO):
    artifact_id: StrictStr = Field(min_length=1)
    path: StrictStr = Field(min_length=1)
    name: StrictStr = Field(min_length=1)
    caption: StrictStr | None = None
    content_type: StrictStr = Field(min_length=1)
    size: StrictInt = Field(ge=0)
    run_count: StrictInt = Field(ge=0)


class GatewayTask(GatewayDTO):
    task_id: StrictStr = Field(min_length=1)
    agent_name: StrictStr = ""
    parent_task_id: StrictStr | None = None
    root_task_id: StrictStr = ""
    depth: StrictInt = Field(default=0, ge=0)
    status: ParsedTaskState
    last_result: StrictStr | None = None
    error: StrictStr | None = None
    run_count: StrictInt = Field(ge=0)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    metadata: dict[str, MetadataScalar] = Field(default_factory=dict)
    pending_review: GatewayPendingReview | None = None
    artifacts: list[GatewayPublishedArtifact] = Field(default_factory=list)

    @property
    def has_pending_review(self) -> bool:
        return self.pending_review is not None


class GatewayReview(GatewayDTO):
    review_id: StrictStr = Field(min_length=1)
    task_id: StrictStr = Field(min_length=1)
    thread_id: StrictStr | None = None
    agent_name: StrictStr = Field(min_length=1)
    route_kind: Literal["local", "remote_ref"]
    status: Literal["pending"]
    action_requests: list[dict[str, Any]] = Field(default_factory=list)
    review_configs: list[dict[str, Any]] = Field(default_factory=list)
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, MetadataScalar] = Field(default_factory=dict)


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


class PublishedArtifactResponse(BaseModel):
    artifact_id: str
    path: str
    name: str
    caption: str | None = None
    content_type: str
    size: int
    run_count: int


class TaskResponse(BaseModel):
    task_id: str
    agent_name: str
    parent_task_id: str | None
    root_task_id: str
    depth: int
    status: ParsedTaskState
    last_result: str | None
    error: str | None
    run_count: int
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, MetadataScalar]
    pending_review: dict[str, Any] | None = None
    artifacts: list[PublishedArtifactResponse] = Field(default_factory=list)


class TaskMessageToolCallResponse(BaseModel):
    tool_call_id: str
    name: str
    arguments: dict[str, Any]


class TaskMessageResponse(BaseModel):
    sequence: int
    message_id: str
    role: Literal["user", "assistant", "tool"]
    content: str
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: list[TaskMessageToolCallResponse] = Field(default_factory=list)
    status: Literal["success", "error"] | None = None


class TaskMessageListResponse(BaseModel):
    task_id: str
    items: list[TaskMessageResponse]
    next_cursor: str | None


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
    status: ParsedTaskState
    last_result: str | None = None
    error: str | None = None
    run_count: int
    created_at: datetime
    updated_at: datetime


class TaskInput(BaseModel):
    content: str = ""
    attachments: list[AttachmentInput] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def require_content_or_attachments(self) -> "TaskInput":
        if self.content.strip() or self.attachments:
            return self
        raise ValueError("input.content or input.attachments is required")


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


class HealthProbeResponse(BaseModel):
    status: Literal["ok"]


class ReadyProbeResponse(BaseModel):
    status: Literal["ready"]


class NotReadyProbeResponse(BaseModel):
    status: Literal["not_ready"]
