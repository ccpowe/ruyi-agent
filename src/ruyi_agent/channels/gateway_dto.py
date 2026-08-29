"""Transport-neutral Gateway payloads consumed by Channel policy."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from datetime import datetime
from typing import Any, Literal, TypeVar

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr

from ruyi_agent.task_models import MetadataScalar


ValueT = TypeVar("ValueT")


class GatewayDTO(BaseModel, Mapping[str, Any]):
    """Validated Gateway boundary object with temporary mapping compatibility."""

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
    status: Literal[
        "pending",
        "running",
        "waiting_for_human",
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    ]
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
