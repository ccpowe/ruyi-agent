from __future__ import annotations

from ruyi_agent.storage.review_audit import ReviewAuditStore


def test_review_audit_store_appends_and_filters_events(tmp_path: Path) -> None:
    store = ReviewAuditStore(str(tmp_path / "audit.sqlite"))
    try:
        store.append(
            "review_requested",
            source="local",
            review_id="review-1",
            task_id="task-1",
            thread_id="thread-1",
            agent_name="worker",
            profile_name="standard",
            backend_kind="local",
            workspace_root="/tmp/project",
            tool_name="execute",
            tool_call_id="call-1",
            policy_decision="require_approval",
            risk="execute_default",
            reason="needs review",
            payload={"args": {"command": "python -V"}},
        )
        store.append(
            "review_decision",
            source="local",
            review_id="review-1",
            task_id="task-1",
            payload={"decisions": [{"type": "approve"}]},
        )

        events = store.list_events(review_id="review-1")
    finally:
        store.close()

    assert [event.event_type for event in events] == [
        "review_decision",
        "review_requested",
    ]
    assert events[1].tool_name == "execute"
    assert events[1].payload == {"args": {"command": "python -V"}}
