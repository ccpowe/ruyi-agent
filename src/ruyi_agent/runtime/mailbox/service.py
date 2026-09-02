"""
Agent Mailbox - agent 间 run settled 消息通道

这个模块实现了进程内 mailbox，用于把子 agent 当前一轮 run 的 settled
结果投递回父 thread。Task 本身是可继续输入的长期 agent 会话。

核心功能：
1. 记录子任务每一轮完成、失败、取消或中断后的通知消息
2. 按父 thread 聚合并一次性取出待投递消息
3. 对同一子任务同一 run 的 settled 通知进行幂等去重
4. 将 mailbox 消息渲染为可注入模型上下文的文本

使用场景：
- 后台委托任务完成后通知父 agent
- 远程 agent webhook 同步到当前 run settled 后通知本地父 thread
- 显式 wait/check 已拿到结果时撤回待投递通知

数据流：
  子任务 run settled → AgentMailbox.publish_settled → 父 thread 队列
  父 agent 调用前 → AgentMailbox.drain → render_mailbox_messages → 模型上下文

关键概念：
- recipient_thread_id: 接收 mailbox 消息的父 thread
- child_task_id: 产生通知的长期委托任务
- run_count: 该 Task 中已经 settled 的 run 序号
- settled 消息: completed、failed、cancelled、interrupted 的本轮结果通知
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
import threading
import uuid
from typing import TypeAlias

from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.settled_outbox import SettledOutboxIntent
from ruyi_agent.task_models import SETTLED_TASK_STATES, TaskState, parse_task_state


TaskSettledStatus: TypeAlias = TaskState


_ACTIVE_MAILBOX_RUN: ContextVar[tuple[int, str] | None] = ContextVar(
    "active_mailbox_run",
    default=None,
)


@dataclass(slots=True)
class InterAgentMessage:
    """
    agent 间 mailbox 消息

    存储一条子任务 run settled 通知，供父 thread 在下一轮模型调用前读取。

    Attributes:
        message_id: mailbox 消息唯一标识符
        recipient_thread_id: 接收消息的父 thread ID
        child_task_id: 产生消息的子任务 ID
        child_agent_name: 执行子任务的 agent 名称
        run_count: 子任务当前已结束的 run 序号
        status: 本轮状态（completed/failed/cancelled/interrupted）
        content: 本轮输出或错误说明
        created_at: 消息创建时间（UTC）
    """

    message_id: str
    recipient_thread_id: str
    child_task_id: str | None
    child_agent_name: str | None
    run_count: int | None
    status: TaskSettledStatus | None
    content: str
    created_at: datetime
    recipient_task_id: str | None = None
    sender_task_id: str | None = None
    sender_agent_name: str | None = None
    trigger_run: bool = True
    claim_token: str | None = None


class AgentMailbox:
    """
    进程内 agent mailbox

    负责在子 agent 任务和父 thread 之间传递 run settled 通知。消息只保存在当前
    进程内存中，适合 runtime 内部的短生命周期协作。

    主要功能：
    - publish_settled: 发布子任务本轮 settled 消息
    - drain: 取出并清空某个父 thread 的待投递消息
    - retract: 撤回某个子任务的待投递消息

    设计要点：
    - 使用线程锁保护内部 dict 和 set，避免 worker 并发发布时产生竞争
    - 使用 `(recipient_thread_id, child_task_id, run_count)` 做幂等去重
    - drain 采用 pop 语义，确保同一批消息只注入模型上下文一次

    Attributes:
        _lock: 保护 mailbox 内部状态的线程锁
        _messages_by_recipient: 按父 thread ID 分组的待投递消息
        _seen_message_keys: 已发布 run settled 消息的去重键集合
    """

    def __init__(self, store: MailboxStore | None = None) -> None:
        """初始化空 mailbox"""
        self._store = store
        self._lock = threading.RLock()
        self._messages_by_recipient: dict[str, list[InterAgentMessage]] = {}
        self._seen_message_keys: set[tuple[str, str, int]] = set()
        self._seen_input_idempotency_keys: set[str] = set()
        self._active_run_claims: dict[str, dict[str, str]] = {}
        self._legacy_claim_tokens: dict[str, list[str]] = {}

    @property
    def is_durable(self) -> bool:
        """Whether input identities survive through a backing MailboxStore."""

        return self._store is not None

    def shares_database(self, db_path: str) -> bool:
        return self._store is not None and self._store.shares_database(db_path)

    @contextmanager
    def run_scope(self, run_id: str) -> Iterator[None]:
        """Bind durable mailbox claims to one in-process executor run."""

        if self._store is None:
            yield
            return
        if not run_id:
            raise ValueError("Mailbox run identity must not be empty")
        with self._lock:
            if run_id in self._active_run_claims:
                raise RuntimeError(f"Mailbox run is already active: {run_id}")
            self._active_run_claims[run_id] = {}
        context_token = _ACTIVE_MAILBOX_RUN.set((id(self), run_id))
        try:
            yield
        finally:
            try:
                _ACTIVE_MAILBOX_RUN.reset(context_token)
            except ValueError:
                # A coroutine can be finalized from a different Context after
                # its task has gone away.  The run registry below is still the
                # authority for maintenance, so it must be removed either way.
                pass
            with self._lock:
                self._active_run_claims.pop(run_id, None)

    def publish_settled(
        self,
        *,
        recipient_thread_id: str,
        recipient_task_id: str | None = None,
        child_task_id: str,
        child_agent_name: str,
        run_count: int,
        status: TaskSettledStatus,
        content: str,
    ) -> InterAgentMessage | None:
        """
        发布子任务本轮 settled 消息

        将子 agent 当前 run 的结果投递到父 thread 的 mailbox 队列中。
        同一父 thread、子任务和 run 只会发布一次。

        Args:
            recipient_thread_id: 接收消息的父 thread ID
            child_task_id: 产生消息的长期子任务 ID
            child_agent_name: 执行子任务的 agent 名称
            run_count: 已结束的 run 序号
            status: 本轮 settled 状态
            content: 本轮输出或错误说明

        Returns:
            新创建的 mailbox 消息；如果重复发布则返回 None
        """
        parsed_status = parse_task_state(status, path="Mailbox settled status")
        if parsed_status not in SETTLED_TASK_STATES:
            raise ValueError("Mailbox settled status must be a settled Task state")
        status = parsed_status
        key = (recipient_thread_id, child_task_id, run_count)
        with self._lock:
            # 为什么要去重：本地同步、远端 webhook 或状态轮询可能重复发布同一轮结果。
            if key in self._seen_message_keys:
                return None
            message = InterAgentMessage(
                message_id=str(uuid.uuid4()),
                recipient_thread_id=recipient_thread_id,
                child_task_id=child_task_id,
                child_agent_name=child_agent_name,
                run_count=run_count,
                status=status,
                content=content,
                created_at=datetime.now(UTC),
                recipient_task_id=recipient_task_id,
                sender_task_id=child_task_id,
                sender_agent_name=child_agent_name,
            )
            if self._store is not None:
                published = self._store.publish(
                    self._message_values(
                        message,
                        idempotency_key=(
                            f"settled:{recipient_thread_id}:{child_task_id}:{run_count}"
                        ),
                    )
                )
                # A duplicate durable row is also authoritative evidence that
                # this process may safely suppress another identical attempt.
                self._seen_message_keys.add(key)
                return message if published else None
            self._messages_by_recipient.setdefault(recipient_thread_id, []).append(
                message
            )
            self._seen_message_keys.add(key)
            return message

    def publish_claimed_settled_outbox(
        self,
        intent: SettledOutboxIntent,
    ) -> bool:
        """Publish a claimed durable intent through its fenced transaction."""

        if self._store is None:
            raise RuntimeError("Settled outbox delivery requires a durable mailbox")
        delivered = self._store.publish_claimed_settled_outbox(intent)
        if delivered:
            with self._lock:
                self._seen_message_keys.add(
                    (
                        intent.recipient_thread_id,
                        intent.task_id,
                        intent.run_count,
                    )
                )
        return delivered

    def retract_settled_outbox(self, intent: SettledOutboxIntent) -> bool:
        if self._store is None:
            return False
        return self._store.retract_settled_outbox(intent)

    def settled_outbox_needs_wake(self, intent: SettledOutboxIntent) -> bool:
        if self._store is None:
            return False
        return self._store.settled_outbox_needs_wake(intent)

    def pending_trigger_recipient_task_ids(self) -> list[str]:
        if self._store is None:
            return []
        return self._store.list_pending_trigger_recipient_task_ids()

    def publish_input(
        self,
        *,
        recipient_task_id: str,
        recipient_thread_id: str,
        content: str,
        sender_task_id: str | None = None,
        sender_agent_name: str | None = None,
        trigger_run: bool = True,
        idempotency_key: str | None = None,
        message_id: str | None = None,
    ) -> InterAgentMessage | None:
        """Publish a user or agent input for delivery to a Gateway Task."""
        message = InterAgentMessage(
            message_id=message_id or str(uuid.uuid4()),
            recipient_task_id=recipient_task_id,
            recipient_thread_id=recipient_thread_id,
            sender_task_id=sender_task_id,
            sender_agent_name=sender_agent_name,
            child_task_id=None,
            child_agent_name=None,
            run_count=None,
            status=None,
            content=content,
            created_at=datetime.now(UTC),
            trigger_run=trigger_run,
        )
        if self._store is not None:
            published = self._store.publish(
                self._message_values(message, idempotency_key=idempotency_key)
            )
            return message if published else None
        with self._lock:
            if (
                idempotency_key is not None
                and idempotency_key in self._seen_input_idempotency_keys
            ):
                return None
            if idempotency_key is not None:
                self._seen_input_idempotency_keys.add(idempotency_key)
            self._messages_by_recipient.setdefault(recipient_thread_id, []).append(
                message
            )
        return message

    def claim(
        self,
        *,
        recipient_task_id: str | None,
        recipient_thread_id: str,
        run_id: str | None = None,
    ) -> list[InterAgentMessage]:
        """Claim pending messages before the recipient's next model call."""
        if self._store is None:
            return self.drain(recipient_thread_id)
        active_run_id = self._resolve_active_run_id(run_id)
        rows = self._store.claim(
            recipient_task_id=recipient_task_id,
            recipient_thread_id=recipient_thread_id,
        )
        messages = [self._message_from_row(row) for row in rows]
        if not messages:
            return messages
        with self._lock:
            active_claims = (
                self._active_run_claims.get(active_run_id)
                if active_run_id is not None
                else None
            )
            for message in messages:
                if message.claim_token is None:
                    continue
                if active_claims is not None:
                    active_claims[message.message_id] = message.claim_token
                else:
                    self._legacy_claim_tokens.setdefault(message.message_id, []).append(
                        message.claim_token
                    )
        return messages

    def acknowledge(self, messages: list[InterAgentMessage] | list[str]) -> None:
        """Acknowledge exact claims supplied by message objects or local claim ids."""

        if self._store is None or not messages:
            return
        claims = self._claim_pairs(messages)
        if not claims:
            return
        self._store.acknowledge(claims)
        self._forget_claim_pairs(claims)

    def acknowledge_run(self, run_id: str) -> None:
        """Acknowledge only token batches injected during one successful run."""

        if self._store is None:
            return
        with self._lock:
            claims = self._active_run_claims.get(run_id)
            if not claims:
                return
            self._store.acknowledge_claim_tokens(list(set(claims.values())))
            claims.clear()

    def release_run(self, run_id: str) -> None:
        """Release only token batches injected during one failed or cancelled run."""

        if self._store is None:
            return
        with self._lock:
            claims = self._active_run_claims.get(run_id)
            if not claims:
                return
            self._store.release_claim_tokens(list(set(claims.values())))
            claims.clear()

    def has_triggering_messages(self, recipient_task_id: str) -> bool:
        if self._store is not None:
            return self._store.has_triggering(recipient_task_id)
        with self._lock:
            return any(
                message.recipient_task_id == recipient_task_id and message.trigger_run
                for messages in self._messages_by_recipient.values()
                for message in messages
            )

    def recover_claims(self) -> None:
        """Renew live run claims, then release every expired durable lease."""

        if self._store is not None:
            with self._lock:
                tokens = {
                    claim_token
                    for claims in self._active_run_claims.values()
                    for claim_token in claims.values()
                }
                live_tokens = self._store.renew_claim_tokens(list(tokens))
                if live_tokens != tokens:
                    for claims in self._active_run_claims.values():
                        for message_id, claim_token in tuple(claims.items()):
                            if claim_token not in live_tokens:
                                claims.pop(message_id, None)
                self._store.recover_claims()

    def drain(self, recipient_thread_id: str) -> list[InterAgentMessage]:
        """
        取出并清空某个父 thread 的全部待投递消息

        Args:
            recipient_thread_id: 接收消息的父 thread ID

        Returns:
            当前等待投递给该父 thread 的 mailbox 消息列表
        """
        with self._lock:
            if self._store is not None:
                messages = self.claim(
                    recipient_task_id=None,
                    recipient_thread_id=recipient_thread_id,
                )
                self.acknowledge(messages)
                return messages
            return self._messages_by_recipient.pop(recipient_thread_id, [])

    def retract(
        self,
        *,
        recipient_thread_id: str,
        child_task_id: str,
        run_count: int,
    ) -> None:
        """
        撤回某个子任务当前 run 的待投递消息

        当调用方已经通过 wait/check 同步拿到子任务结果时，删除 mailbox 中
        仍未投递的通知，避免父 agent 后续收到重复提醒。

        Args:
            recipient_thread_id: 接收消息的父 thread ID
            child_task_id: 需要撤回通知的子任务 ID
            run_count: 需要撤回通知的 run 序号
        """
        with self._lock:
            if self._store is not None:
                self._store.retract_settled(
                    recipient_thread_id=recipient_thread_id,
                    child_task_id=child_task_id,
                    child_run_count=run_count,
                )
                return
            messages = self._messages_by_recipient.get(recipient_thread_id)
            if not messages:
                return
            # 只撤回指定 run，保留同一 Task 的其它轮次和其它已完成委托消息。
            remaining = [
                message
                for message in messages
                if not (
                    message.child_task_id == child_task_id
                    and message.run_count == run_count
                )
            ]
            if remaining:
                self._messages_by_recipient[recipient_thread_id] = remaining
            else:
                self._messages_by_recipient.pop(recipient_thread_id, None)

    @staticmethod
    def _message_values(
        message: InterAgentMessage,
        *,
        idempotency_key: str | None,
    ) -> dict[str, object]:
        return {
            "message_id": message.message_id,
            "idempotency_key": idempotency_key,
            "recipient_task_id": message.recipient_task_id,
            "recipient_thread_id": message.recipient_thread_id,
            "sender_task_id": message.sender_task_id,
            "sender_agent_name": message.sender_agent_name,
            "child_task_id": message.child_task_id,
            "child_agent_name": message.child_agent_name,
            "child_run_count": message.run_count,
            "settled_status": message.status,
            "content": message.content,
            "trigger_run": message.trigger_run,
            "created_at": message.created_at,
        }

    @staticmethod
    def _message_from_row(row: dict[str, object]) -> InterAgentMessage:
        created_at = datetime.fromisoformat(str(row["created_at"]))
        return InterAgentMessage(
            message_id=str(row["message_id"]),
            recipient_task_id=(
                str(row["recipient_task_id"])
                if row.get("recipient_task_id") is not None
                else None
            ),
            recipient_thread_id=str(row["recipient_thread_id"]),
            sender_task_id=(
                str(row["sender_task_id"])
                if row.get("sender_task_id") is not None
                else None
            ),
            sender_agent_name=(
                str(row["sender_agent_name"])
                if row.get("sender_agent_name") is not None
                else None
            ),
            child_task_id=(
                str(row["child_task_id"])
                if row.get("child_task_id") is not None
                else None
            ),
            child_agent_name=(
                str(row["child_agent_name"])
                if row.get("child_agent_name") is not None
                else None
            ),
            run_count=(
                int(row["child_run_count"])
                if row.get("child_run_count") is not None
                else None
            ),
            status=(
                str(row["settled_status"])  # type: ignore[arg-type]
                if row.get("settled_status") is not None
                else None
            ),
            content=str(row["content"]),
            created_at=created_at,
            trigger_run=bool(row["trigger_run"]),
            claim_token=(
                str(row["claim_token"])
                if row.get("claim_token") is not None
                else None
            ),
        )

    def _resolve_active_run_id(self, run_id: str | None) -> str | None:
        candidate = run_id
        if candidate is None:
            current = _ACTIVE_MAILBOX_RUN.get()
            if current is not None and current[0] == id(self):
                candidate = current[1]
        if candidate is None:
            return None
        with self._lock:
            return candidate if candidate in self._active_run_claims else None

    def _claim_pairs(
        self,
        messages: list[InterAgentMessage] | list[str],
    ) -> list[tuple[str, str]]:
        active_run_id = self._resolve_active_run_id(None)
        with self._lock:
            active_claims = (
                self._active_run_claims.get(active_run_id)
                if active_run_id is not None
                else None
            )
            pairs: list[tuple[str, str]] = []
            for item in messages:
                if isinstance(item, InterAgentMessage):
                    if item.claim_token is not None:
                        pairs.append((item.message_id, item.claim_token))
                    continue
                message_id = item
                claim_token = (
                    active_claims.get(message_id)
                    if active_claims is not None
                    else None
                )
                if claim_token is None:
                    legacy_tokens = self._legacy_claim_tokens.get(message_id)
                    if legacy_tokens:
                        claim_token = legacy_tokens[0]
                if claim_token is not None:
                    pairs.append((message_id, claim_token))
            return list(dict.fromkeys(pairs))

    def _forget_claim_pairs(self, claims: list[tuple[str, str]]) -> None:
        with self._lock:
            for message_id, claim_token in claims:
                for active_claims in self._active_run_claims.values():
                    if active_claims.get(message_id) == claim_token:
                        active_claims.pop(message_id, None)
                legacy_tokens = self._legacy_claim_tokens.get(message_id)
                if legacy_tokens is not None:
                    remaining = [
                        token for token in legacy_tokens if token != claim_token
                    ]
                    if remaining:
                        self._legacy_claim_tokens[message_id] = remaining
                    else:
                        self._legacy_claim_tokens.pop(message_id, None)


def render_mailbox_messages(messages: list[InterAgentMessage]) -> str:
    """
    将 mailbox 消息渲染为模型可读文本

    输出文本会被注入父 agent 的 conversation，使父 agent 能识别哪个委托
    哪个 Task 的哪一轮已结束、由哪个 agent 执行，以及本轮结果是什么。

    Args:
        messages: 待渲染的 mailbox 消息列表

    Returns:
        可作为模型输入内容的多段文本
    """
    sections: list[str] = []
    for message in messages:
        if message.child_task_id is None:
            header = [
                "[mailbox] New input received.",
                f"message_id={message.message_id}",
            ]
            if message.sender_task_id:
                header.append(f"sender_task_id={message.sender_task_id}")
            if message.sender_agent_name:
                header.append(f"sender_agent={message.sender_agent_name}")
            sections.append("\n".join([*header, "message:", message.content]))
            continue
        sections.append(
            "\n".join(
                [
                    "[mailbox] Delegated agent run finished.",
                    f"task_id={message.child_task_id}",
                    f"agent={message.child_agent_name}",
                    f"run_count={message.run_count}",
                    f"status={message.status}",
                    "message:",
                    message.content,
                ]
            )
        )
    return "\n\n".join(sections)
