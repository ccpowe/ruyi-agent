from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from ruyi_agent.task_models import (
    PendingReviewRecord,
    PublishedArtifact,
    TaskRecord,
    parse_task_state,
)


TASK_WRITE_COLUMNS = (
    "task_id",
    "agent_name",
    "state",
    "thread_id",
    "parent_task_id",
    "root_task_id",
    "depth",
    "created_at",
    "updated_at",
    "result",
    "error",
    "run_count",
    "route_kind",
    "upstream_task_id",
    "parent_thread_id",
    "mailbox_suppressed",
    "mailbox_delivered",
    "webhook_json",
    "delegation_root_id",
    "delegation_max_depth",
    "delegation_max_tasks_per_root",
    "delegation_visited_nodes_json",
    "permission_profile",
    "effective_skill_names_json",
    "skill_view_path",
    "skill_view_hash",
    "pending_review_json",
    "artifacts_json",
    "external_operation",
    "external_operation_identity",
    "external_operation_run_count",
    "external_outcome_uncertain",
)
TASK_SELECT_COLUMNS = ", ".join(TASK_WRITE_COLUMNS)


def serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def serialize_json_object(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True)


def row_to_pending_review(row: tuple[Any, ...]) -> PendingReviewRecord:
    payload = json.loads(row[3])
    if not isinstance(payload, dict):
        raise ValueError("Stored pending review payload is not a JSON object")
    return PendingReviewRecord(
        review_id=str(row[0]),
        task_id=str(row[1]),
        root_task_id=str(row[2]),
        payload=payload,
        created_at=parse_datetime(str(row[4])),
        updated_at=parse_datetime(str(row[5])),
        ingest_sequence=int(row[6]),
        cursor_order_updated_at=parse_datetime(str(row[7])),
    )


def task_insert_sql(*, upsert: bool) -> str:
    column_list = ", ".join(TASK_WRITE_COLUMNS)
    placeholders = ", ".join("?" for _ in TASK_WRITE_COLUMNS)
    sql = f"INSERT INTO agent_tasks ({column_list}) VALUES ({placeholders})"
    if not upsert:
        return sql
    assignments = ", ".join(
        f"{column} = excluded.{column}" for column in TASK_WRITE_COLUMNS[1:]
    )
    return f"{sql} ON CONFLICT(task_id) DO UPDATE SET {assignments}"


def task_record_values(record: TaskRecord) -> tuple[Any, ...]:
    return (
        record.task_id,
        record.agent_name,
        record.state,
        record.thread_id,
        record.parent_task_id,
        record.root_task_id,
        record.depth,
        serialize_datetime(record.created_at),
        serialize_datetime(record.updated_at),
        record.result,
        record.error,
        record.run_count,
        record.route_kind,
        record.upstream_task_id,
        record.parent_thread_id,
        int(record.mailbox_suppressed),
        int(record.mailbox_delivered),
        serialize_json_object(record.webhook) if record.webhook is not None else None,
        record.delegation_root_id,
        record.delegation_max_depth,
        record.delegation_max_tasks_per_root,
        json.dumps(list(record.delegation_visited_nodes), ensure_ascii=True),
        record.permission_profile,
        json.dumps(list(record.effective_skill_names), ensure_ascii=True),
        record.skill_view_path,
        record.skill_view_hash,
        (
            serialize_json_object(record.pending_review)
            if record.pending_review is not None
            else None
        ),
        json.dumps(
            [_artifact_to_dict(item) for item in record.artifacts],
            ensure_ascii=True,
            sort_keys=True,
        ),
        record.external_operation,
        record.external_operation_identity,
        record.external_operation_run_count,
        int(record.external_outcome_uncertain),
    )


def row_to_task_record(row: tuple[Any, ...]) -> TaskRecord:
    webhook = json.loads(row[17]) if row[17] else None
    if not isinstance(webhook, dict):
        webhook = None

    visited_nodes_raw = json.loads(row[21]) if row[21] else []
    visited_nodes = tuple(item for item in visited_nodes_raw if isinstance(item, str))
    return TaskRecord(
        task_id=row[0],
        agent_name=row[1],
        state=parse_task_state(row[2], path="Stored Task state"),
        thread_id=row[3],
        parent_task_id=row[4],
        root_task_id=row[5],
        depth=row[6],
        created_at=parse_datetime(row[7]),
        updated_at=parse_datetime(row[8]),
        result=row[9],
        error=row[10],
        run_count=row[11],
        route_kind=row[12],
        upstream_task_id=row[13],
        parent_thread_id=row[14],
        mailbox_suppressed=bool(row[15]),
        mailbox_delivered=bool(row[16]),
        webhook=webhook,
        delegation_root_id=row[18],
        delegation_max_depth=row[19],
        delegation_max_tasks_per_root=row[20],
        delegation_visited_nodes=visited_nodes,
        permission_profile=row[22],
        effective_skill_names=tuple(
            item
            for item in (json.loads(row[23]) if row[23] else [])
            if isinstance(item, str)
        ),
        skill_view_path=row[24],
        skill_view_hash=row[25],
        pending_review=json.loads(row[26]) if row[26] else None,
        artifacts=_parse_artifacts(row[27]),
        external_operation=row[28],
        external_operation_identity=row[29],
        external_operation_run_count=row[30],
        external_outcome_uncertain=bool(row[31]),
    )


def _artifact_to_dict(value: PublishedArtifact) -> dict[str, Any]:
    return {
        "artifact_id": value.artifact_id,
        "path": value.path,
        "name": value.name,
        "caption": value.caption,
        "content_type": value.content_type,
        "size": value.size,
        "run_count": value.run_count,
    }


def _parse_artifacts(value: str | None) -> list[PublishedArtifact]:
    if not value:
        return []
    try:
        raw_items = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(raw_items, list):
        return []
    artifacts: list[PublishedArtifact] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        artifact_id = item.get("artifact_id")
        path = item.get("path")
        name = item.get("name")
        content_type = item.get("content_type")
        size = item.get("size")
        run_count = item.get("run_count")
        if not (
            isinstance(artifact_id, str)
            and artifact_id
            and isinstance(path, str)
            and path
            and isinstance(name, str)
            and name
            and isinstance(content_type, str)
            and content_type
            and isinstance(size, int)
            and isinstance(run_count, int)
        ):
            continue
        caption = item.get("caption")
        artifacts.append(
            PublishedArtifact(
                artifact_id=artifact_id,
                path=path,
                name=name,
                caption=caption if isinstance(caption, str) and caption else None,
                content_type=content_type,
                size=size,
                run_count=run_count,
            )
        )
    return artifacts
