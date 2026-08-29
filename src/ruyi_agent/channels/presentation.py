from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ruyi_agent.channels.gateway_client import gateway_task_from_payload
from ruyi_agent.channels.gateway_dto import GatewayTask
from ruyi_agent.channels.task_watch import TaskWatchHooks, TaskWatchManager


TaskCallback = Callable[[GatewayTask], Awaitable[None]]


class ReviewPresenter:
    """Transport-neutral text projection for a Pending Review."""

    @staticmethod
    def format(task: GatewayTask) -> str:
        task_id = task.task_id
        pending_review = task.pending_review
        if pending_review is None:
            return f"任务等待审批，但缺少审批详情。\n\ntask_id={task_id}"
        review_id = pending_review.review_id
        actions = pending_review.action_requests
        configs = pending_review.review_configs
        action_lines: list[str] = []
        for index, action in enumerate(actions, start=1):
            config = configs[index - 1] if index - 1 < len(configs) else {}
            tool_name = action.get("name") or config.get("action_name") or "tool"
            action_lines.append(f"{index}. {tool_name} args={action.get('args')}")
        actions_text = "\n".join(action_lines) if action_lines else "(no actions)"
        return (
            "任务等待人工审批。\n"
            f"review_id={review_id}\n"
            f"task_id={task_id}\n"
            f"{actions_text}\n\n"
            "快速批准：y\n"
            "快速拒绝：n\n"
            f"指定批准：/approve {review_id}\n"
            f"指定拒绝：/reject {review_id} 原因"
        )


class TerminalPresenter:
    """Format and deduplicate terminal Task presentation per run."""

    def __init__(self) -> None:
        self.delivered_run_counts: dict[str, int] = {}

    @staticmethod
    def format(task: GatewayTask) -> str:
        status = task.status
        task_id = task.task_id
        if status == "completed":
            result = task.last_result or "(empty result)"
            return f"{result}\n\ntask_id={task_id}"
        if status == "failed":
            error = task.error or "unknown error"
            return f"任务失败：{error}\n\ntask_id={task_id}"
        if status == "cancelled":
            return f"任务已取消。\n\ntask_id={task_id}"
        return f"任务结束，状态={status}\n\ntask_id={task_id}"

    async def present(
        self,
        task: GatewayTask | dict[str, Any],
        *,
        send_message: TaskCallback,
        send_artifacts: TaskCallback,
        on_duplicate: TaskCallback | None = None,
        on_delivered: TaskCallback | None = None,
    ) -> bool:
        parsed_task = gateway_task_from_payload(task)
        delivered_run_count = self.delivered_run_counts.get(parsed_task.task_id, 0)
        if parsed_task.run_count <= delivered_run_count:
            if on_duplicate is not None:
                await on_duplicate(parsed_task)
            return False
        await send_message(parsed_task)
        await send_artifacts(parsed_task)
        self.delivered_run_counts[parsed_task.task_id] = parsed_task.run_count
        if on_delivered is not None:
            await on_delivered(parsed_task)
        return True


class ChannelDeliveryCoordinator:
    """Compose existing Task Watch policy with shared Task presenters.

    Retry, persistence, and startup recovery intentionally remain outside this
    structural boundary and are unchanged in this refactor.
    """

    def __init__(self, *, task_watch: TaskWatchManager) -> None:
        self.task_watch = task_watch
        self.review_presenter = ReviewPresenter()
        self.terminal_presenter = TerminalPresenter()

    def ensure_watch(
        self,
        *,
        task_id: str,
        run_count: int,
        on_pending_review: TaskCallback,
        on_terminal: TaskCallback,
        on_superseded: TaskCallback | None = None,
        on_error: Callable[[Exception], Awaitable[None]] | None = None,
    ) -> None:
        self.task_watch.ensure(
            task_id=task_id,
            run_count=run_count,
            hooks=TaskWatchHooks(
                on_pending_review=on_pending_review,
                on_terminal=on_terminal,
                on_superseded=on_superseded,
                on_error=on_error,
            ),
        )

    def is_active(self, *, task_id: str, run_count: int) -> bool:
        return self.task_watch.is_active(task_id=task_id, run_count=run_count)

    async def wait(self) -> None:
        await self.task_watch.wait()
