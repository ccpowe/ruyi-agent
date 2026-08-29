"""Persistence-safe models shared by Task runtime and control-plane layers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, TypeAlias, cast


TaskState: TypeAlias = Literal[
    "pending",
    "running",
    "waiting_for_human",
    "completed",
    "failed",
    "cancelled",
    "interrupted",
]
TaskRouteKind: TypeAlias = Literal["local", "remote_ref"]
TaskRouteState: TypeAlias = Literal["pending", "active", "failed", "uncertain"]
TASK_ROUTE_STATES: frozenset[TaskRouteState] = frozenset(
    {"pending", "active", "failed", "uncertain"}
)
MetadataScalar: TypeAlias = str | int | float | bool | None

ACTIVE_TASK_STATES: frozenset[TaskState] = frozenset(
    {"pending", "running", "waiting_for_human"}
)
SETTLED_TASK_STATES: frozenset[TaskState] = frozenset(
    {"completed", "failed", "cancelled", "interrupted"}
)
TASK_STATES: frozenset[TaskState] = ACTIVE_TASK_STATES | SETTLED_TASK_STATES
EXECUTING_TASK_STATES: frozenset[TaskState] = ACTIVE_TASK_STATES - {
    "waiting_for_human"
}
RESUMABLE_TASK_STATES = SETTLED_TASK_STATES


def parse_task_state(value: object, *, path: str = "task state") -> TaskState:
    """Validate a persisted/transport value against the canonical Task states."""

    if not isinstance(value, str):
        allowed = ", ".join(sorted(TASK_STATES))
        raise ValueError(f"{path} must be one of: {allowed}")
    normalized = str(value)
    if normalized not in TASK_STATES:
        allowed = ", ".join(sorted(TASK_STATES))
        raise ValueError(f"{path} must be one of: {allowed}")
    return cast(TaskState, normalized)


@dataclass(frozen=True, slots=True)
class PublishedArtifact:
    """A small file published by a Task for channel delivery."""

    artifact_id: str
    path: str
    name: str
    caption: str | None
    content_type: str
    size: int
    run_count: int


@dataclass(slots=True)
class TaskRecord:
    """Durable state for one local or remote Gateway Task.

    Process-local execution handles deliberately do not belong here. Live
    ``asyncio.Task`` objects are owned by ``LiveRunRegistry`` in the runtime
    layer and are keyed by ``task_id``.
    """

    task_id: str
    agent_name: str
    state: TaskState
    thread_id: str
    parent_task_id: str | None
    root_task_id: str
    depth: int
    created_at: datetime
    updated_at: datetime
    result: str | None = None
    error: str | None = None
    run_count: int = 0
    route_kind: TaskRouteKind = "local"
    upstream_task_id: str | None = None
    parent_thread_id: str | None = None
    mailbox_suppressed: bool = False
    mailbox_delivered: bool = False
    webhook: dict[str, Any] | None = None
    delegation_root_id: str | None = None
    delegation_max_depth: int | None = None
    delegation_max_tasks_per_root: int | None = None
    delegation_visited_nodes: tuple[str, ...] = ()
    permission_profile: str = ""
    effective_skill_names: tuple[str, ...] = ()
    skill_view_path: str | None = None
    skill_view_hash: str | None = None
    pending_review: dict[str, Any] | None = None
    artifacts: list[PublishedArtifact] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PendingReviewRecord:
    """One durable human review owned by a Task within a delegation root."""

    review_id: str
    task_id: str
    root_task_id: str
    payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    ingest_sequence: int = 0


@dataclass(slots=True)
class TaskRouteRecord:
    """Durable binding between a local Task identity and its execution route."""

    task_id: str
    agent_name: str
    metadata: dict[str, MetadataScalar]
    route_kind: TaskRouteKind
    upstream_task_id: str | None
    webhook: dict[str, MetadataScalar] | None = None
    route_state: TaskRouteState = "active"
    route_error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
