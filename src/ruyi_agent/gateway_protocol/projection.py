from __future__ import annotations
import json
from datetime import UTC, datetime
from typing import Any
from ruyi_agent.gateway_protocol.contracts import MAX_ARTIFACT_CAPTION_JSON_BYTES, MAX_ARTIFACT_LIST_JSON_BYTES, MAX_DURABLE_TASK_EVENT_DATA_BYTES, MAX_EVENT_ARTIFACTS, MAX_EVENT_TEXT_LENGTH, MAX_LIFECYCLE_TEXT_JSON_BYTES, MAX_PENDING_REVIEW_JSON_BYTES, MAX_REVIEW_DECISIONS, MAX_REVIEW_ITEMS, MAX_SHORT_EVENT_TEXT_JSON_BYTES, MAX_SHORT_EVENT_TEXT_LENGTH, PUBLIC_ASSISTANT_DELTA_NODES, PUBLIC_ASSISTANT_DELTA_PATH, TaskLifecycleEventType, TaskStreamEvent
from ruyi_agent.gateway_protocol.cursor import encode_task_event_cursor
from ruyi_agent.task_models import SETTLED_TASK_STATES, PublishedArtifact, TaskRecord, parse_task_state
def lifecycle_event_type(record: TaskRecord) -> TaskLifecycleEventType:
    return {
        "pending": "task.created",
        "running": "task.running",
        "waiting_for_human": "task.review_requested",
        "completed": "task.completed",
        "failed": "task.failed",
        "cancelled": "task.cancelled",
        "interrupted": "task.interrupted",
    }[record.state]
def lifecycle_event_data(
    record: TaskRecord,
    *,
    reconciled: bool = False,
) -> dict[str, Any]:
    last_result, result_truncated = _bounded_text(record.result)
    error, error_truncated = _bounded_text(record.error)
    pending_review, pending_review_truncated = _project_pending_review(
        record.pending_review
    )
    artifacts, artifacts_truncated = _project_artifacts(record.artifacts)
    data: dict[str, Any] = {
        "status": record.state,
        "last_result": last_result,
        "error": error,
        "updated_at": _isoformat(record.updated_at),
        "pending_review": pending_review,
        "artifacts": artifacts,
    }
    if result_truncated:
        data["last_result_truncated"] = True
    if error_truncated:
        data["error_truncated"] = True
    if pending_review_truncated:
        data["pending_review_truncated"] = True
    if artifacts_truncated:
        data["artifacts_truncated"] = True
    if reconciled:
        data["reconciled"] = True
        data["observed_at"] = _isoformat(datetime.now(UTC))
    ensure_durable_event_data_fits(data)
    return data
def artifact_event_data(
    record: TaskRecord,
    artifact: PublishedArtifact,
) -> dict[str, Any]:
    projected, truncated = _project_artifact(artifact)
    data = {
        "status": record.state,
        "updated_at": _isoformat(record.updated_at),
        "artifact": projected,
    }
    if truncated:
        data["artifact_truncated"] = True
    ensure_durable_event_data_fits(data)
    return data
def public_task_event_fingerprint(record: TaskRecord) -> str:
    data = lifecycle_event_data(record)
    data.pop("updated_at", None)
    return json.dumps(
        {"run_count": record.run_count, **data},
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
def assistant_delta_from_stream_part(part: Any) -> str | None:
    if (
        not isinstance(part, dict)
        or part.get("type") != "messages"
        or part.get("ns") != ()
    ):
        return None
    data = part.get("data")
    if not isinstance(data, (tuple, list)) or len(data) != 2:
        return None
    message, metadata = data
    if (
        not isinstance(metadata, dict)
        or metadata.get("langgraph_node") not in PUBLIC_ASSISTANT_DELTA_NODES
        or metadata.get("langgraph_path") != PUBLIC_ASSISTANT_DELTA_PATH
    ):
        return None
    if (
        type(message).__name__ != "AIMessageChunk"
        or not type(message).__module__.startswith("langchain_core.messages")
        or getattr(message, "type", None) != "AIMessageChunk"
    ):
        return None
    content = getattr(message, "content", None)
    if isinstance(content, str):
        return content or None
    if not isinstance(content, list):
        return None
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict) or block.get("type") not in {
            "text",
            "output_text",
        }:
            continue
        text = block.get("text")
        if isinstance(text, str) and text:
            parts.append(text)
    return "".join(parts) or None
def normalize_task_event_text(value: str) -> str:
    if not any(0xD800 <= ord(character) <= 0xDFFF for character in value):
        return value
    return "".join(
        "\ufffd" if 0xD800 <= ord(character) <= 0xDFFF else character
        for character in value
    )
def build_reconciled_anchor(
    record: TaskRecord,
) -> tuple[str, dict[str, Any], datetime]:
    observed_at = datetime.now(UTC)
    return (
        lifecycle_event_type(record),
        lifecycle_event_data(record, reconciled=True),
        observed_at,
    )
def stored_event_to_stream_event(event: Any) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type=event.event_type,
        task_id=event.task_id,
        run_count=event.run_count,
        created_at=event.created_at,
        data=dict(event.data),
        event_id=encode_task_event_cursor(event),
    )
def stream_end_event(
    *,
    task_id: str,
    run_count: int,
    reason: str,
) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type="stream.end",
        task_id=task_id,
        run_count=run_count,
        created_at=datetime.now(UTC),
        data={"reason": reason},
    )
def end_reason_for_record(record: TaskRecord) -> str | None:
    if record.pending_review:
        return "review_required"
    return {
        "waiting_for_human": "review_required",
        "completed": "completed",
        "failed": "failed",
        "cancelled": "cancelled",
        "interrupted": "interrupted",
    }.get(record.state)
def end_reason_from_data(data: dict[str, Any]) -> str | None:
    if data.get("pending_review"):
        return "review_required"
    try:
        status = parse_task_state(data.get("status"))
    except ValueError:
        return None
    if status == "waiting_for_human":
        return "review_required"
    if status in SETTLED_TASK_STATES:
        return status
    return None
def ensure_durable_event_data_fits(data: dict[str, Any]) -> None:
    if _json_encoded_size(data) > MAX_DURABLE_TASK_EVENT_DATA_BYTES:
        raise ValueError("Projected durable Task event exceeds its wire budget")
def _bounded_text(
    value: Any,
    *,
    max_chars: int = MAX_EVENT_TEXT_LENGTH,
    max_json_bytes: int = MAX_LIFECYCLE_TEXT_JSON_BYTES,
) -> tuple[str | None, bool]:
    if not isinstance(value, str):
        return None, False
    normalized = normalize_task_event_text(value)
    candidate = normalized[:max_chars]
    truncated = normalized != value or len(candidate) != len(normalized)
    if _json_encoded_size(candidate) <= max_json_bytes:
        return candidate, truncated
    low = 0
    high = len(candidate)
    while low < high:
        midpoint = (low + high + 1) // 2
        if _json_encoded_size(candidate[:midpoint]) <= max_json_bytes:
            low = midpoint
        else:
            high = midpoint - 1
    return candidate[:low], True
def _project_pending_review(value: Any) -> tuple[dict[str, Any] | None, bool]:
    if not isinstance(value, dict) or not value:
        return None, bool(value)
    projected: dict[str, Any] = {}
    truncated = False
    for key in ("review_id", "source_task_id"):
        item = value.get(key)
        if isinstance(item, str) and item:
            bounded, item_truncated = _bounded_text(
                item,
                max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
                max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
            )
            projected[key] = bounded
            truncated = truncated or item_truncated
        elif item is not None:
            truncated = True
    raw_actions = value.get("action_requests")
    if isinstance(raw_actions, list):
        actions: list[dict[str, str]] = []
        for item in raw_actions[:MAX_REVIEW_ITEMS]:
            if not isinstance(item, dict):
                truncated = True
                continue
            name = item.get("name")
            if not isinstance(name, str) or not name:
                truncated = True
                continue
            bounded, item_truncated = _bounded_text(
                name,
                max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
                max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
            )
            candidate = [*actions, {"name": bounded or ""}]
            if _json_encoded_size(
                {**projected, "action_requests": candidate}
            ) > MAX_PENDING_REVIEW_JSON_BYTES:
                truncated = True
                break
            actions = candidate
            truncated = truncated or item_truncated
        if len(raw_actions) > MAX_REVIEW_ITEMS:
            truncated = True
        projected["action_requests"] = actions
    elif raw_actions is not None:
        truncated = True
    raw_configs = value.get("review_configs")
    if isinstance(raw_configs, list):
        configs, configs_truncated = _project_review_configs(raw_configs, projected)
        projected["review_configs"] = configs
        truncated = truncated or configs_truncated
    elif raw_configs is not None:
        truncated = True
    return projected or None, truncated
def _project_review_configs(
    raw_configs: list[Any],
    projected: dict[str, Any],
) -> tuple[list[dict[str, Any]], bool]:
    configs: list[dict[str, Any]] = []
    truncated = len(raw_configs) > MAX_REVIEW_ITEMS
    for item in raw_configs[:MAX_REVIEW_ITEMS]:
        if not isinstance(item, dict):
            truncated = True
            continue
        action_name = item.get("action_name")
        allowed = item.get("allowed_decisions")
        if not isinstance(action_name, str) or not action_name:
            truncated = True
            continue
        bounded_name, name_truncated = _bounded_text(
            action_name,
            max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
            max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
        )
        decisions, decisions_truncated = _project_review_decisions(allowed)
        candidate = [
            *configs,
            {
                "action_name": bounded_name or "",
                "allowed_decisions": decisions,
            },
        ]
        if _json_encoded_size(
            {**projected, "review_configs": candidate}
        ) > MAX_PENDING_REVIEW_JSON_BYTES:
            truncated = True
            break
        configs = candidate
        truncated = truncated or name_truncated or decisions_truncated
    return configs, truncated
def _project_review_decisions(value: Any) -> tuple[list[str], bool]:
    if not isinstance(value, list):
        return [], True
    decisions: list[str] = []
    truncated = len(value) > MAX_REVIEW_DECISIONS
    for choice in value[:MAX_REVIEW_DECISIONS]:
        if not isinstance(choice, str) or not choice:
            truncated = True
            continue
        bounded, choice_truncated = _bounded_text(
            choice,
            max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
            max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
        )
        decisions.append(bounded or "")
        truncated = truncated or choice_truncated
    return decisions, truncated
def _project_artifacts(
    values: list[PublishedArtifact],
) -> tuple[list[dict[str, Any]], bool]:
    projected: list[dict[str, Any]] = []
    encoded_size = 2
    truncated = len(values) > MAX_EVENT_ARTIFACTS
    for value in values[:MAX_EVENT_ARTIFACTS]:
        item, item_truncated = _project_artifact(value)
        item_size = _json_encoded_size(item)
        candidate_size = encoded_size + item_size + (1 if projected else 0)
        if candidate_size > MAX_ARTIFACT_LIST_JSON_BYTES:
            truncated = True
            break
        projected.append(item)
        encoded_size = candidate_size
        truncated = truncated or item_truncated
    if len(projected) < min(len(values), MAX_EVENT_ARTIFACTS):
        truncated = True
    return projected, truncated
def _project_artifact(
    value: PublishedArtifact,
) -> tuple[dict[str, Any], bool]:
    artifact_id, artifact_id_truncated = _bounded_text(
        value.artifact_id,
        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
    )
    name, name_truncated = _bounded_text(
        value.name,
        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
    )
    content_type, content_type_truncated = _bounded_text(
        value.content_type,
        max_chars=MAX_SHORT_EVENT_TEXT_LENGTH,
        max_json_bytes=MAX_SHORT_EVENT_TEXT_JSON_BYTES,
    )
    caption, caption_truncated = _bounded_text(
        value.caption,
        max_json_bytes=MAX_ARTIFACT_CAPTION_JSON_BYTES,
    )
    return {
        "artifact_id": artifact_id or "",
        "name": name or "",
        "caption": caption,
        "content_type": content_type or "",
        "size": value.size,
        "run_count": value.run_count,
    }, any(
        (
            artifact_id_truncated,
            name_truncated,
            content_type_truncated,
            caption_truncated,
        )
    )
def _json_encoded_size(value: Any) -> int:
    return len(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
def _isoformat(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()
