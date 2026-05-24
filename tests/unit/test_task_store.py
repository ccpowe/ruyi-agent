from __future__ import annotations

from datetime import UTC, datetime

from ruyi_agent.runtime.delegation.async_runtime import PublishedArtifact, TaskRecord
from ruyi_agent.storage.task_store import TaskStore


def test_task_store_persists_skill_view_fields(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    record = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="completed",
        thread_id="thread-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 5, 23, tzinfo=UTC),
        updated_at=datetime(2026, 5, 23, tzinfo=UTC),
        effective_skill_names=("frontend", "repo-workflow"),
        skill_view_path="/.ruyi_agent/runtime/skill-views/abc",
        skill_view_hash="abc",
    )

    store.save_task(record)
    loaded = store.get_task("task-1")

    assert loaded is not None
    assert loaded.effective_skill_names == ("frontend", "repo-workflow")
    assert loaded.skill_view_path == "/.ruyi_agent/runtime/skill-views/abc"
    assert loaded.skill_view_hash == "abc"
    store.close()


def test_task_store_persists_published_artifacts(tmp_path) -> None:
    store = TaskStore(str(tmp_path / "tasks.sqlite"))
    record = TaskRecord(
        task_id="task-1",
        agent_name="main",
        state="completed",
        thread_id="thread-1",
        parent_task_id=None,
        root_task_id="task-1",
        depth=0,
        created_at=datetime(2026, 5, 23, tzinfo=UTC),
        updated_at=datetime(2026, 5, 23, tzinfo=UTC),
        artifacts=[
            PublishedArtifact(
                artifact_id="art_1",
                path="/workspace/out/report.xlsx",
                name="report.xlsx",
                caption="Report",
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                size=123,
                run_count=1,
            )
        ],
    )

    store.save_task(record)
    loaded = store.get_task("task-1")

    assert loaded is not None
    assert loaded.artifacts == record.artifacts
    store.close()
