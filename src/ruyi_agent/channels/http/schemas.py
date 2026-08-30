"""Compatibility import facade for the neutral Gateway DTOs."""

from ruyi_agent.gateway_protocol.dto import (
    ArtifactDownloadRequest,
    CreateTaskRequest,
    HealthProbeResponse,
    NotReadyProbeResponse,
    ReadyProbeResponse,
    ReviewDecisionInput,
    SendInputRequest,
    TaskInput,
)

__all__ = [
    "ArtifactDownloadRequest",
    "CreateTaskRequest",
    "HealthProbeResponse",
    "NotReadyProbeResponse",
    "ReadyProbeResponse",
    "ReviewDecisionInput",
    "SendInputRequest",
    "TaskInput",
]
