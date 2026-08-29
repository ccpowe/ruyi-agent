from __future__ import annotations

import asyncio
from typing import Any

from ruyi_agent.channels.gateway_dto import GatewayTask
from ruyi_agent.channels.presentation import (
    ChannelDeliveryCoordinator,
    ReviewPresenter,
    TerminalPresenter,
)


def task(
    status: str = "completed",
    *,
    run_count: int = 1,
    review: dict[str, Any] | None = None,
) -> GatewayTask:
    return GatewayTask.model_validate(
        {
            "task_id": "task-1",
            "status": status,
            "run_count": run_count,
            "last_result": "done",
            "pending_review": review,
        }
    )


def test_review_presenter_projects_actions_and_missing_details() -> None:
    presenter = ReviewPresenter()
    review_task = task(
        "waiting_for_human",
        review={
            "review_id": "review-1",
            "action_requests": [
                {"name": "shell", "args": {"command": "pwd"}},
                {"args": {"path": "report.md"}},
            ],
            "review_configs": [{}, {"action_name": "write_file"}],
        },
    )

    assert presenter.format(review_task) == (
        "任务等待人工审批。\n"
        "review_id=review-1\n"
        "task_id=task-1\n"
        "1. shell args={'command': 'pwd'}\n"
        "2. write_file args={'path': 'report.md'}\n\n"
        "快速批准：y\n"
        "快速拒绝：n\n"
        "指定批准：/approve review-1\n"
        "指定拒绝：/reject review-1 原因"
    )
    assert presenter.format(task("waiting_for_human")) == (
        "任务等待审批，但缺少审批详情。\n\ntask_id=task-1"
    )


def test_terminal_presenter_formats_all_existing_status_contracts() -> None:
    presenter = TerminalPresenter()

    assert presenter.format(task()) == "done\n\ntask_id=task-1"
    failed = task("failed").model_copy(update={"error": "boom"})
    assert presenter.format(failed) == "任务失败：boom\n\ntask_id=task-1"
    assert presenter.format(task("cancelled")) == "任务已取消。\n\ntask_id=task-1"
    assert presenter.format(task("interrupted")) == (
        "任务结束，状态=interrupted\n\ntask_id=task-1"
    )


def test_terminal_delivery_is_recorded_only_after_all_platform_sends_succeed() -> None:
    async def exercise() -> None:
        presenter = TerminalPresenter()
        sent: list[str] = []
        artifact_attempts = 0

        async def send_message(_: GatewayTask) -> None:
            sent.append("message")

        async def send_artifacts(_: GatewayTask) -> None:
            nonlocal artifact_attempts
            artifact_attempts += 1
            if artifact_attempts == 1:
                raise RuntimeError("platform artifact send failed")
            sent.append("artifacts")

        try:
            await presenter.present(
                task(),
                send_message=send_message,
                send_artifacts=send_artifacts,
            )
        except RuntimeError as exc:
            assert str(exc) == "platform artifact send failed"
        else:
            raise AssertionError("first platform failure must propagate")

        assert presenter.delivered_run_counts == {}
        assert await presenter.present(
            task(),
            send_message=send_message,
            send_artifacts=send_artifacts,
        )
        assert presenter.delivered_run_counts == {"task-1": 1}
        assert sent == ["message", "message", "artifacts"]

    asyncio.run(exercise())


def test_terminal_delivery_duplicate_uses_platform_cleanup_without_resending() -> None:
    async def exercise() -> None:
        presenter = TerminalPresenter()
        events: list[str] = []

        async def record(name: str, _: GatewayTask) -> None:
            events.append(name)

        assert await presenter.present(
            task(),
            send_message=lambda item: record("message", item),
            send_artifacts=lambda item: record("artifacts", item),
            on_delivered=lambda item: record("delivered", item),
        )
        assert not await presenter.present(
            task(),
            send_message=lambda item: record("message", item),
            send_artifacts=lambda item: record("artifacts", item),
            on_duplicate=lambda item: record("duplicate", item),
        )
        assert events == ["message", "artifacts", "delivered", "duplicate"]

    asyncio.run(exercise())


def test_delivery_coordinator_only_composes_existing_watch_hooks() -> None:
    class WatchProbe:
        def __init__(self) -> None:
            self.call: dict[str, Any] | None = None

        def ensure(self, **kwargs: Any) -> None:
            self.call = kwargs

        def is_active(self, *, task_id: str, run_count: int) -> bool:
            return (task_id, run_count) == ("task-1", 1)

        async def wait(self) -> None:
            return None

    async def ignore(_: GatewayTask) -> None:
        return None

    watch = WatchProbe()
    coordinator = ChannelDeliveryCoordinator(task_watch=watch)  # type: ignore[arg-type]
    coordinator.ensure_watch(
        task_id="task-1",
        run_count=1,
        on_pending_review=ignore,
        on_terminal=ignore,
    )

    assert watch.call is not None
    assert watch.call["task_id"] == "task-1"
    assert watch.call["run_count"] == 1
    assert watch.call["hooks"].on_pending_review is ignore
    assert watch.call["hooks"].on_terminal is ignore
    assert coordinator.is_active(task_id="task-1", run_count=1)
