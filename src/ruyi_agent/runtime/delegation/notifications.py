"""Settled-run mailbox notification policy."""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from ruyi_agent.task_models import SETTLED_TASK_STATES, TaskRecord


class NotificationHost(Protocol):
    _mailbox: Any
    _task_manager: Any

    async def _ensure_task_awake(self, task_id: str) -> TaskRecord: ...
    def _is_settled_record(self, record: TaskRecord) -> bool: ...


class SettledRunNotifier:
    """Publish or suppress one settled result for the current task run."""

    def __init__(self, control: NotificationHost) -> None:
        self._control = control

    def _is_settled_record(self, record: TaskRecord) -> bool:
        """
        判断 Task 当前一轮 run 是否已经 settled

        Args:
            record: 任务记录

        Returns:
            completed、failed、cancelled 或 interrupted 返回 True
        """
        return record.state in SETTLED_TASK_STATES

    def _maybe_publish_settled_message(self, task_id: str) -> None:
        """
        尝试向父 thread 发布当前 run 的 settled mailbox 消息

        只有配置了 mailbox、任务有 parent_thread_id、未被 suppress、未投递过且
        当前 run 已经 settled 时才会发布。Task 会话本身仍可继续输入。

        Args:
            task_id: 当前 runtime 内部任务 ID
        """
        # 当前 run 结束后发布消息到 mailbox；Task 本身仍然保持可恢复。
        record = self._control._task_manager.get_task(task_id)
        if (
            self._control._mailbox is None
            or record.parent_thread_id is None
            or record.mailbox_suppressed
            or record.mailbox_delivered
            or not self._control._is_settled_record(record)
        ):
            return
        status = record.state
        if status not in SETTLED_TASK_STATES:
            return
        content = record.result or record.error or f"Task run ended with state={status}"
        # content 支持 执行结果 已知错误 或者未知状态
        published = self._control._mailbox.publish_settled(
            recipient_thread_id=record.parent_thread_id,
            recipient_task_id=record.parent_task_id,
            child_task_id=record.task_id,
            child_agent_name=record.agent_name,
            run_count=record.run_count,
            status=status,
            content=content,
        )
        if published is not None:
            self._control._task_manager.mark_mailbox_delivered(record.task_id)
            if record.parent_task_id is not None:
                asyncio.create_task(self._control._ensure_task_awake(record.parent_task_id))

    def _suppress_mailbox_delivery(self, record: TaskRecord) -> None:
        """
        禁止或撤回任务当前 run 的 mailbox 投递

        当调用方已经通过 wait/check 主动获取结果时，后续不应再把同一个结果
        作为 mailbox 消息注入父 agent。

        Args:
            record: 需要抑制 mailbox 投递的任务记录
        """
        self._control._task_manager.mark_mailbox_suppressed(record.task_id)
        if self._control._mailbox is not None and record.parent_thread_id is not None:
            self._control._mailbox.retract(
                recipient_thread_id=record.parent_thread_id,
                child_task_id=record.task_id,
                run_count=record.run_count,
            )
