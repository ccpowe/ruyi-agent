"""
Agent Mailbox - agent 间终态消息通道

这个模块实现了进程内 mailbox，用于把子 agent 任务的终态结果投递回父
thread。

核心功能：
1. 记录子任务完成、失败、取消后的通知消息
2. 按父 thread 聚合并一次性取出待投递消息
3. 对同一子任务的同一终态通知进行幂等去重
4. 将 mailbox 消息渲染为可注入模型上下文的文本

使用场景：
- 后台委托任务完成后通知父 agent
- 远程 agent webhook 同步到终态后通知本地父 thread
- 显式 wait/check 已拿到结果时撤回待投递通知

数据流：
  子任务终态 → AgentMailbox.publish_terminal → 父 thread 队列
  父 agent 调用前 → AgentMailbox.drain → render_mailbox_messages → 模型上下文

关键概念：
- recipient_thread_id: 接收 mailbox 消息的父 thread
- child_task_id: 已进入终态的委托任务
- 终态消息: completed、failed、cancelled 三类任务最终状态通知
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import threading
import uuid
from typing import Literal


TaskTerminalStatus = Literal["completed", "failed", "cancelled"]


@dataclass(slots=True)
class InterAgentMessage:
    """
    agent 间 mailbox 消息

    存储一条子任务终态通知，供父 thread 在下一轮模型调用前读取。

    Attributes:
        message_id: mailbox 消息唯一标识符
        recipient_thread_id: 接收消息的父 thread ID
        child_task_id: 产生消息的子任务 ID
        child_agent_name: 执行子任务的 agent 名称
        status: 子任务终态（completed/failed/cancelled）
        content: 子任务终态输出或错误说明
        created_at: 消息创建时间（UTC）
    """

    message_id: str
    recipient_thread_id: str
    child_task_id: str
    child_agent_name: str
    status: TaskTerminalStatus
    content: str
    created_at: datetime


class AgentMailbox:
    """
    进程内 agent mailbox

    负责在子 agent 任务和父 thread 之间传递终态通知。消息只保存在当前
    进程内存中，适合 runtime 内部的短生命周期协作。

    主要功能：
    - publish_terminal: 发布子任务终态消息
    - drain: 取出并清空某个父 thread 的待投递消息
    - retract: 撤回某个子任务的待投递消息

    设计要点：
    - 使用线程锁保护内部 dict 和 set，避免 worker 并发发布时产生竞争
    - 使用 `(recipient_thread_id, child_task_id, status)` 做幂等去重
    - drain 采用 pop 语义，确保同一批消息只注入模型上下文一次

    Attributes:
        _lock: 保护 mailbox 内部状态的线程锁
        _messages_by_recipient: 按父 thread ID 分组的待投递消息
        _seen_message_keys: 已发布终态消息的去重键集合
    """

    def __init__(self) -> None:
        """初始化空 mailbox"""
        self._lock = threading.Lock()
        self._messages_by_recipient: dict[str, list[InterAgentMessage]] = {}
        self._seen_message_keys: set[tuple[str, str, str]] = set()

    def publish_terminal(
        self,
        *,
        recipient_thread_id: str,
        child_task_id: str,
        child_agent_name: str,
        status: TaskTerminalStatus,
        content: str,
    ) -> InterAgentMessage | None:
        """
        发布子任务终态消息

        将子 agent 任务的最终状态投递到父 thread 的 mailbox 队列中。
        同一父 thread、子任务和终态组合只会发布一次。

        Args:
            recipient_thread_id: 接收消息的父 thread ID
            child_task_id: 已进入终态的子任务 ID
            child_agent_name: 执行子任务的 agent 名称
            status: 子任务终态
            content: 子任务终态输出或错误说明

        Returns:
            新创建的 mailbox 消息；如果重复发布则返回 None
        """
        key = (recipient_thread_id, child_task_id, status)
        with self._lock:
            # 为什么要去重：本地终态同步、远端 webhook 或状态轮询可能重复触发发布。
            if key in self._seen_message_keys:
                return None
            self._seen_message_keys.add(key)
            message = InterAgentMessage(
                message_id=str(uuid.uuid4()),
                recipient_thread_id=recipient_thread_id,
                child_task_id=child_task_id,
                child_agent_name=child_agent_name,
                status=status,
                content=content,
                created_at=datetime.now(UTC),
            )
            self._messages_by_recipient.setdefault(recipient_thread_id, []).append(
                message
            )
            return message

    def drain(self, recipient_thread_id: str) -> list[InterAgentMessage]:
        """
        取出并清空某个父 thread 的全部待投递消息

        Args:
            recipient_thread_id: 接收消息的父 thread ID

        Returns:
            当前等待投递给该父 thread 的 mailbox 消息列表
        """
        with self._lock:
            return self._messages_by_recipient.pop(recipient_thread_id, [])

    def retract(self, *, recipient_thread_id: str, child_task_id: str) -> None:
        """
        撤回某个子任务的待投递消息

        当调用方已经通过 wait/check 同步拿到子任务结果时，删除 mailbox 中
        仍未投递的通知，避免父 agent 后续收到重复提醒。

        Args:
            recipient_thread_id: 接收消息的父 thread ID
            child_task_id: 需要撤回通知的子任务 ID
        """
        with self._lock:
            messages = self._messages_by_recipient.get(recipient_thread_id)
            if not messages:
                return
            # 只撤回指定子任务，保留同一父 thread 下其他已完成委托的消息。
            remaining = [
                message
                for message in messages
                if message.child_task_id != child_task_id
            ]
            if remaining:
                self._messages_by_recipient[recipient_thread_id] = remaining
            else:
                self._messages_by_recipient.pop(recipient_thread_id, None)


def render_mailbox_messages(messages: list[InterAgentMessage]) -> str:
    """
    将 mailbox 消息渲染为模型可读文本

    输出文本会被注入父 agent 的 conversation，使父 agent 能识别哪个委托
    任务已结束、由哪个 agent 执行，以及终态内容是什么。

    Args:
        messages: 待渲染的 mailbox 消息列表

    Returns:
        可作为模型输入内容的多段文本
    """
    sections: list[str] = []
    for message in messages:
        sections.append(
            "\n".join(
                [
                    "[mailbox] Delegated agent task finished.",
                    f"task_id={message.child_task_id}",
                    f"agent={message.child_agent_name}",
                    f"status={message.status}",
                    "message:",
                    message.content,
                ]
            )
        )
    return "\n\n".join(sections)
