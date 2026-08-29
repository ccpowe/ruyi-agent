"""Durable intent state for cancellable remote Gateway operations."""

from __future__ import annotations

import uuid
from typing import Any

from ruyi_agent.runtime.delegation.contracts import (
    _now,
    _parse_task_timestamp,
    _validate_remote_task_state,
)
from ruyi_agent.runtime.task_events import (
    normalize_task_event_text,
    public_task_event_fingerprint,
)
from ruyi_agent.task_models import PendingReviewRecord, TaskRecord


def begin_external_operation(
    manager: Any,
    task_id: str,
    *,
    operation: str,
    identity: str,
) -> None:
    record = manager.get_task(task_id)
    root = manager._root_record(record)
    with manager._review_memory_transaction(record, root):
        record.external_operation = normalize_task_event_text(operation)
        record.external_operation_identity = normalize_task_event_text(identity)
        record.external_outcome_uncertain = False
        record.updated_at = _now()
        manager._save(record)


def clear_external_operation(manager: Any, task_id: str) -> None:
    record = manager.get_task(task_id)
    root = manager._root_record(record)
    with manager._review_memory_transaction(record, root):
        record.external_operation = None
        record.external_operation_identity = None
        record.external_outcome_uncertain = False
        manager._save(record)


def mark_external_outcome_uncertain(
    manager: Any,
    task_id: str,
    *,
    operation: str,
    identity: str,
) -> None:
    record = manager.get_task(task_id)
    root = manager._root_record(record)
    with manager._review_memory_transaction(record, root):
        record.state = "interrupted"
        record.error = normalize_task_event_text(
            f"Remote {operation} outcome is uncertain; refresh is required"
        )
        record.external_operation = normalize_task_event_text(operation)
        record.external_operation_identity = normalize_task_event_text(identity)
        record.external_outcome_uncertain = True
        record.updated_at = _now()
        manager._clear_pending_review_and_save(record)


def bind_uncertain_remote_task(
    manager: Any,
    task_id: str,
    upstream_task_id: str,
) -> TaskRecord:
    record = manager.get_task(task_id)
    if not (
        record.route_kind == "remote_ref"
        and record.upstream_task_id is None
        and record.external_outcome_uncertain
        and record.external_operation == "create"
    ):
        raise ValueError(f"Task '{task_id}' cannot accept an upstream binding")
    root = manager._root_record(record)
    with manager._review_memory_transaction(record, root):
        record.upstream_task_id = upstream_task_id
        record.thread_id = record.task_id
        record.updated_at = _now()
        manager._save(record)
    return record


def set_remote_webhook_if_missing(
    manager: Any,
    task_id: str,
    webhook: dict[str, Any],
) -> TaskRecord:
    record = manager.get_task(task_id)
    if record.webhook is not None:
        return record
    root = manager._root_record(record)
    with manager._review_memory_transaction(record, root):
        record.webhook = dict(webhook)
        record.updated_at = _now()
        manager._save(record)
    return record


def bind_and_sync_remote_task(
    manager: Any,
    task_id: str,
    upstream_task_id: str,
    payload: dict[str, Any],
) -> TaskRecord:
    record = manager.get_task(task_id)
    root = manager._root_record(record)
    with manager._review_memory_transaction(record, root):
        record.upstream_task_id = upstream_task_id
        record.thread_id = record.task_id
        return sync_remote_task(manager, task_id, payload)


def sync_remote_task(
    manager: Any,
    task_id: str,
    payload: dict[str, Any],
) -> TaskRecord:
    """Apply one validated remote payload inside the manager transaction."""

    record = manager.get_task(task_id)
    current_review = manager._review_for_task(task_id)
    previous_fingerprint = public_task_event_fingerprint(record)
    previous_run_count = record.run_count
    status, run_count = _validate_remote_task_state(task_id, payload)

    last_result = payload.get("last_result")
    error = payload.get("error")
    pending_review = payload.get("pending_review")
    public_pending_review = (
        dict(pending_review) if isinstance(pending_review, dict) else None
    )
    if (
        public_pending_review is not None
        and "source_task_id" in public_pending_review
    ):
        public_pending_review["source_task_id"] = record.task_id
    record.external_operation = None
    record.external_operation_identity = None
    record.external_outcome_uncertain = False
    record.state = status
    record.thread_id = record.task_id
    record.result = (
        normalize_task_event_text(last_result)
        if isinstance(last_result, str)
        else None
    )
    record.error = (
        "Remote Gateway Task failed"
        if status in {"failed", "interrupted"} and isinstance(error, str)
        else None
    )
    record.pending_review = public_pending_review
    record.run_count = run_count
    if run_count != previous_run_count:
        record.mailbox_suppressed = False
        record.mailbox_delivered = False
    record.created_at = _parse_task_timestamp(
        payload.get("created_at"),
        fallback=record.created_at,
    )
    record.updated_at = _parse_task_timestamp(
        payload.get("updated_at"),
        fallback=record.updated_at,
    )
    manager._live_runs.discard(task_id)
    record_changed = public_task_event_fingerprint(record) != previous_fingerprint
    if record.state == "waiting_for_human" and record.pending_review is not None:
        review_id = record.pending_review.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            review_id = str(uuid.uuid4())
        else:
            review_id = normalize_task_event_text(review_id)
        record.pending_review["review_id"] = review_id
        matching_review = manager.get_pending_review(review_id)
        if matching_review is not None and matching_review.task_id != record.task_id:
            raise ValueError(f"Pending review already exists: {review_id}")
        review = PendingReviewRecord(
            review_id=review_id,
            task_id=record.task_id,
            root_task_id=record.root_task_id,
            payload=dict(record.pending_review),
            created_at=(
                current_review.created_at
                if current_review is not None and current_review.review_id == review_id
                else record.updated_at
            ),
            updated_at=record.updated_at,
            ingest_sequence=manager._review_ingest_sequence(
                current_review,
                review_id,
            ),
        )
        reviews = [
            item
            for item in manager.list_pending_reviews(
                root_task_id=record.root_task_id
            )
            if item.task_id != record.task_id
        ]
        reviews.append(review)
        root, root_changed = manager._project_root_review(record, reviews)
        manager._persist_review_transition(
            record,
            pending_review=review,
            root=root,
            root_changed=root_changed,
            record_changed=record_changed,
        )
        if current_review is not None:
            manager._pending_reviews.pop(current_review.review_id, None)
        review = manager._persisted_review(review)
        manager._pending_reviews[review.review_id] = review
    elif current_review is not None:
        remaining = [
            review
            for review in manager.list_pending_reviews(
                root_task_id=record.root_task_id
            )
            if review.review_id != current_review.review_id
        ]
        root, root_changed = manager._project_root_review(record, remaining)
        manager._persist_review_transition(
            record,
            pending_review=None,
            root=root,
            root_changed=root_changed,
            record_changed=record_changed,
        )
        manager._pending_reviews.pop(current_review.review_id, None)
    elif record_changed:
        manager._save_lifecycle(record)
    else:
        manager._save(record)
    return record
