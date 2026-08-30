"""Compatibility facade for neutral public Task event projections."""

from ruyi_agent.gateway_protocol.projection import (
    artifact_event_data,
    assistant_delta_from_stream_part,
    build_reconciled_anchor,
    end_reason_for_record,
    end_reason_from_data,
    ensure_durable_event_data_fits,
    lifecycle_event_data,
    lifecycle_event_type,
    normalize_task_event_text,
    public_task_event_fingerprint,
    stored_event_to_stream_event,
    stream_end_event,
)

__all__ = [
    "artifact_event_data",
    "assistant_delta_from_stream_part",
    "build_reconciled_anchor",
    "end_reason_for_record",
    "end_reason_from_data",
    "ensure_durable_event_data_fits",
    "lifecycle_event_data",
    "lifecycle_event_type",
    "normalize_task_event_text",
    "public_task_event_fingerprint",
    "stored_event_to_stream_event",
    "stream_end_event",
]
