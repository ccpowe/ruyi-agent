"""Authoritative in-process task state manager.

TaskManager is the single mutation boundary for TaskRecord lifecycle state. It
coordinates persistence, lifecycle events, pending reviews, and process-local
run handles without owning execution or delegation policy.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import fields
from typing import Any

from ruyi_agent.runtime.delegation.context import DelegationContext
from ruyi_agent.runtime.delegation.contracts import (
    UnknownWorkerTaskError,
    _now,
)
from ruyi_agent.runtime.delegation.live_runs import LiveRunRegistry
from ruyi_agent.runtime.delegation import remote_reconciliation
from ruyi_agent.runtime.task_events import (
    TaskEventLedger,
    TaskLifecycleEventType,
    artifact_event_data,
    lifecycle_event_data,
    lifecycle_event_type,
    normalize_task_event_text,
)
from ruyi_agent.storage.task_store import (
    TaskRootBudgetExceededError as MaxTasksPerRootError,
    TaskStore,
    task_record_for_restart,
)
from ruyi_agent.storage.settled_outbox import (
    LegacySettlementMigrationBatch,
    SettledOutboxIntent,
    build_settled_outbox_intent,
)
from ruyi_agent.task_models import (
    PendingReviewRecord,
    PublishedArtifact,
    TaskRecord,
)

class TaskManager:
    """
    委托任务状态管理器

    负责维护所有 TaskRecord，并提供任务创建、查询、状态变更和远端状态同步
    的唯一写入口。

    主要功能：
    - 创建 pending 任务记录
    - 标记 running/completed/failed/cancelled 状态
    - 根据 thread_id 或 upstream_task_id 反查任务
    - 将远端网关 payload 同步到本地 TaskRecord

    设计要点：
    - 状态写入集中在 TaskManager，避免调用方直接分散修改字段
    - 本地和远端任务共用同一种 TaskRecord 视图

    Attributes:
        _tasks: 以 task_id 索引的任务记录表
    """

    def __init__(
        self,
        store: TaskStore | None = None,
        *,
        settled_outbox_enabled: bool = False,
    ) -> None:
        """初始化空任务表"""
        # 为什么有 task manager：异步子任务的状态、结果、取消和等待必须由一个中心层统一管理。
        self._tasks: dict[str, TaskRecord] = {}
        self._live_runs = LiveRunRegistry()
        self._store = store
        self._settled_outbox_enabled = settled_outbox_enabled and store is not None
        self._event_ledger = TaskEventLedger(store) if store is not None else None
        self._pending_reviews: dict[str, PendingReviewRecord] = {
            review.review_id: review
            for review in (store.list_pending_reviews() if store is not None else [])
        }
        self._next_review_ingest_sequence = (
            max(
                (review.ingest_sequence for review in self._pending_reviews.values()),
                default=0,
            )
            + 1
        )

    @property
    def event_ledger(self) -> TaskEventLedger | None:
        return self._event_ledger

    def load_by_parent_thread_id(self, parent_thread_id: str) -> None:
        """
        按当前激活 agent thread 加载它创建的任务

        这个方法用于 agent middleware。每个 agent run 激活时，只恢复
        parent_thread_id 等于当前 thread_id 的 task，避免启动时全量加载历史。
        """
        if self._store is None:
            return
        for stored in self._store.list_tasks_by_parent_thread_id(parent_thread_id):
            current = self._tasks.get(stored.task_id)
            if current is not None and self._has_live_active_run(current):
                continue
            self._restore_stored_record(stored)

    def load_task_for_parent_thread(
        self,
        *,
        task_id: str,
        parent_thread_id: str,
    ) -> TaskRecord | None:
        """
        按 task_id 和父 thread 懒加载单个 task

        agent tool 使用这个方法恢复当前调用方可见的 task；Gateway 路径不走
        parent_thread_id 过滤，会用 load_task_by_id。
        """
        if self._store is None:
            return None
        stored = self._store.get_task_by_parent_thread_id(
            task_id=task_id,
            parent_thread_id=parent_thread_id,
        )
        if stored is None:
            return None
        current = self._tasks.get(stored.task_id)
        if current is not None and self._has_live_active_run(current):
            return current
        return self._restore_stored_record(stored)

    def load_task_by_id(self, task_id: str) -> TaskRecord | None:
        """
        按 task_id 懒加载单个 task

        这个入口用于 Gateway HTTP、webhook 和其它控制面路径，不做 agent
        parent_thread_id 可见性过滤。
        """
        if self._store is None:
            return None
        stored = self._store.get_task(task_id)
        if stored is None:
            return None
        current = self._tasks.get(stored.task_id)
        if current is not None and self._has_live_active_run(current):
            return current
        return self._restore_stored_record(stored)

    def _restore_stored_record(self, stored: TaskRecord) -> TaskRecord:
        record = task_record_for_restart(stored)
        self._tasks[record.task_id] = record
        if record.external_operation is not None:
            self._save_uncertain_external_operation(record)
        elif record.state != stored.state or record.error != stored.error:
            self._save_lifecycle(record)
        return record

    def _has_live_active_run(self, record: TaskRecord) -> bool:
        return self._live_runs.is_active(record.task_id)

    def get_live_run(self, task_id: str) -> asyncio.Task[None] | None:
        """Return the process-local run handle without polluting TaskRecord."""

        return self._live_runs.get_task(task_id)

    def has_active_run(self, task_id: str) -> bool:
        return self._live_runs.is_active(task_id)

    def was_cancel_requested(self, task_id: str) -> bool:
        return self._live_runs.was_cancel_requested(task_id)

    def request_cancel(self, task_id: str) -> asyncio.Task[None] | None:
        return self._live_runs.request_cancel(task_id)

    def discard_live_run(self, task_id: str) -> None:
        """Release one process-local handle after supervised finalization."""

        self._live_runs.discard(task_id)

    def _save(self, record: TaskRecord) -> None:
        """把当前任务记录写入持久化存储"""
        if self._store is not None:
            self._store.update_task(record)

    def _save_uncertain_external_operation(self, record: TaskRecord) -> None:
        if self._store is not None:
            self._store.update_uncertain_external_operation(record)

    def _save_rejected_external_operation(self, record: TaskRecord) -> None:
        if self._store is not None:
            self._store.update_rejected_external_operation(
                record,
                settled_outbox_intent=self._settled_outbox_intent(record),
            )

    def _save_lifecycle(self, record: TaskRecord) -> None:
        """Atomically persist one public lifecycle transition and its event."""

        if self._event_ledger is not None:
            self._event_ledger.update_task(
                record,
                event_type=lifecycle_event_type(record),
                event_data=lifecycle_event_data(record),
                settled_outbox_intent=self._settled_outbox_intent(record),
            )
            return
        self._save(record)

    @property
    def settled_outbox_enabled(self) -> bool:
        return self._settled_outbox_enabled

    def _settled_outbox_intent(
        self,
        record: TaskRecord,
    ) -> SettledOutboxIntent | None:
        if not self._settled_outbox_enabled:
            return None
        return build_settled_outbox_intent(record)

    def reconcile_settled_outbox(self) -> int:
        if not self._settled_outbox_enabled or self._store is None:
            return 0
        return self._store.reconcile_settled_outbox()

    def reconcile_settled_outbox_batch(self) -> LegacySettlementMigrationBatch:
        if not self._settled_outbox_enabled or self._store is None:
            return LegacySettlementMigrationBatch(inserted=0, completed=True)
        return self._store.reconcile_settled_outbox_batch()

    def claim_settled_outbox(self) -> list[SettledOutboxIntent]:
        if not self._settled_outbox_enabled or self._store is None:
            return []
        return self._store.claim_settled_outbox()

    def release_settled_outbox_claim(
        self,
        intent: SettledOutboxIntent,
        *,
        error: str,
    ) -> bool:
        if not self._settled_outbox_enabled or self._store is None:
            return False
        return self._store.release_settled_outbox_claim(intent, error=error)

    def list_suppressed_settled_outbox(self) -> list[SettledOutboxIntent]:
        if not self._settled_outbox_enabled or self._store is None:
            return []
        return self._store.list_suppressed_settled_outbox()

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        """List authoritative pending reviews in stable local ingest order."""

        if self._store is not None:
            reviews = self._store.list_pending_reviews(
                root_task_id=root_task_id,
                task_id=task_id,
            )
            for review in reviews:
                self._pending_reviews[review.review_id] = review
            return reviews
        reviews = [
            review
            for review in self._pending_reviews.values()
            if (root_task_id is None or review.root_task_id == root_task_id)
            and (task_id is None or review.task_id == task_id)
        ]
        return sorted(
            reviews,
            key=lambda item: (item.ingest_sequence, item.review_id),
        )

    def get_pending_review(self, review_id: str) -> PendingReviewRecord | None:
        """Return one authoritative pending review by identity."""

        if self._store is not None:
            review = self._store.get_pending_review(review_id)
            if review is not None:
                self._pending_reviews[review.review_id] = review
            return review
        return self._pending_reviews.get(review_id)

    def _review_for_task(self, task_id: str) -> PendingReviewRecord | None:
        reviews = self.list_pending_reviews(task_id=task_id)
        return reviews[0] if reviews else None

    def _review_ingest_sequence(
        self,
        current_review: PendingReviewRecord | None,
        review_id: str,
    ) -> int:
        if (
            current_review is not None
            and current_review.review_id == review_id
            and current_review.ingest_sequence > 0
        ):
            return current_review.ingest_sequence
        if self._store is not None:
            return 0
        sequence = self._next_review_ingest_sequence
        self._next_review_ingest_sequence += 1
        return sequence

    def _persisted_review(
        self,
        review: PendingReviewRecord,
    ) -> PendingReviewRecord:
        if self._store is None:
            return review
        return self._store.get_pending_review(review.review_id) or review

    def _root_record(self, record: TaskRecord) -> TaskRecord | None:
        if record.root_task_id == record.task_id:
            return record
        root = self._tasks.get(record.root_task_id)
        if root is None:
            root = self.load_task_by_id(record.root_task_id)
        return root

    def _project_root_review(
        self,
        record: TaskRecord,
        reviews: list[PendingReviewRecord],
    ) -> tuple[TaskRecord | None, bool]:
        """Update the legacy root ``pending_review`` projection in memory."""

        root = self._root_record(record)
        if root is None:
            return None, False
        previous = root.pending_review
        candidates = sorted(
            (
                review
                for review in reviews
                if review.root_task_id == record.root_task_id
            ),
            key=lambda item: (item.created_at, item.review_id),
        )
        projected: dict[str, Any] | None = None
        if candidates:
            selected = candidates[0]
            projected = dict(selected.payload)
            if selected.task_id != root.task_id:
                projected["source_task_id"] = selected.task_id
        if previous == projected:
            return root, False
        root.pending_review = projected
        root.updated_at = _now()
        return root, True

    def _persist_review_transition(
        self,
        record: TaskRecord,
        *,
        pending_review: PendingReviewRecord | None,
        root: TaskRecord | None,
        root_changed: bool,
        record_changed: bool = True,
    ) -> None:
        events: list[
            tuple[TaskRecord, TaskLifecycleEventType, dict[str, Any]]
        ] = []
        if record_changed:
            events.append(
                (record, lifecycle_event_type(record), lifecycle_event_data(record))
            )
        if root_changed and root is not None and root.task_id != record.task_id:
            root_event_type = (
                "task.review_requested"
                if root.pending_review is not None
                else lifecycle_event_type(root)
            )
            events.append((root, root_event_type, lifecycle_event_data(root)))
        if self._event_ledger is not None:
            self._event_ledger.update_review_transition(
                record,
                pending_review=pending_review,
                root_record=(
                    root
                    if root_changed
                    and root is not None
                    and root.task_id != record.task_id
                    else None
                ),
                events=events,
                settled_outbox_intent=self._settled_outbox_intent(record),
            )
        else:
            self._save(record)
            if root_changed and root is not None and root.task_id != record.task_id:
                self._save(root)

    @contextmanager
    def _review_memory_transaction(
        self,
        *records: TaskRecord | None,
    ) -> Iterator[None]:
        """Restore process-local review state if its durable transaction fails."""

        unique_records = {
            record.task_id: record for record in records if record is not None
        }
        record_snapshots = {
            task_id: deepcopy(record)
            for task_id, record in unique_records.items()
        }
        review_snapshot = deepcopy(self._pending_reviews)
        next_review_ingest_sequence = self._next_review_ingest_sequence
        live_run_snapshots = {
            task_id: self._live_runs.snapshot(task_id) for task_id in unique_records
        }
        try:
            yield
        except BaseException:
            for task_id, record in unique_records.items():
                snapshot = record_snapshots[task_id]
                for field_info in fields(TaskRecord):
                    setattr(
                        record,
                        field_info.name,
                        deepcopy(getattr(snapshot, field_info.name)),
                    )
                self._live_runs.restore(task_id, live_run_snapshots[task_id])
            self._pending_reviews.clear()
            self._pending_reviews.update(review_snapshot)
            self._next_review_ingest_sequence = next_review_ingest_sequence
            raise

    def _clear_pending_review_and_save(self, record: TaskRecord) -> None:
        current = self._review_for_task(record.task_id)
        if current is None:
            if record.task_id == record.root_task_id:
                self._project_root_review(
                    record,
                    self.list_pending_reviews(root_task_id=record.root_task_id),
                )
            else:
                record.pending_review = None
            self._save_lifecycle(record)
            return
        record.pending_review = None
        remaining = [
            review
            for review in self.list_pending_reviews(root_task_id=record.root_task_id)
            if review.review_id != current.review_id
        ]
        root, root_changed = self._project_root_review(record, remaining)
        self._persist_review_transition(
            record,
            pending_review=None,
            root=root,
            root_changed=root_changed,
        )
        self._pending_reviews.pop(current.review_id, None)

    def create_task_record(
        self,
        task_id: str,
        agent_name: str,
        *,
        parent_task_id: str | None,
        root_task_id: str,
        depth: int,
        route_kind: str = "local",
        upstream_task_id: str | None = None,
        parent_thread_id: str | None = None,
        webhook: dict[str, Any] | None = None,
        delegation_context: DelegationContext | None = None,
        permission_profile: str = "",
        effective_skill_names: Sequence[str] = (),
        skill_view_path: str | None = None,
        skill_view_hash: str | None = None,
    ) -> TaskRecord:
        """
        创建任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID
            agent_name: 执行任务的 agent 名称
            parent_task_id: 父任务 ID
            root_task_id: 委托树根任务 ID
            depth: 当前任务深度
            route_kind: 执行路由（local/remote_ref）
            upstream_task_id: 远端网关原始任务 ID
            parent_thread_id: 父 agent thread ID
            webhook: 当前 run settled 后的 webhook 配置
            delegation_context: 跨网关委托上下文

        Returns:
            新创建的任务记录
        """
        # 为什么先创建 task record：异步任务一旦被调度，就需要立即进入可追踪状态。
        record = TaskRecord(
            task_id=task_id,
            agent_name=agent_name,
            state="pending",
            # A remote Gateway's Task id is a private transport binding.  The
            # durable/public thread identity of this proxy remains its local
            # Task id across refresh, restart, review, and webhook paths.
            thread_id=(
                task_id if route_kind == "remote_ref" else upstream_task_id or task_id
            ),
            parent_task_id=parent_task_id,
            root_task_id=root_task_id,
            depth=depth,
            created_at=_now(),
            updated_at=_now(),
            route_kind=route_kind,
            upstream_task_id=upstream_task_id,
            parent_thread_id=parent_thread_id,
            webhook=webhook,
            delegation_root_id=(
                delegation_context.root_id if delegation_context is not None else None
            ),
            delegation_max_depth=(
                delegation_context.max_depth if delegation_context is not None else None
            ),
            delegation_max_tasks_per_root=(
                delegation_context.max_tasks_per_root
                if delegation_context is not None
                else None
            ),
            delegation_visited_nodes=(
                delegation_context.visited_nodes
                if delegation_context is not None
                else ()
            ),
            permission_profile=permission_profile,
            effective_skill_names=tuple(effective_skill_names),
            skill_view_path=skill_view_path,
            skill_view_hash=skill_view_hash,
        )
        if task_id in self._tasks:
            raise ValueError(f"Task already exists: {task_id}")
        if self._store is None and record.delegation_max_tasks_per_root is not None:
            current_count = self.count_tasks_under_root(root_task_id)
            if current_count >= record.delegation_max_tasks_per_root:
                raise MaxTasksPerRootError(
                    root_task_id=root_task_id,
                    current_count=current_count,
                    max_tasks_per_root=record.delegation_max_tasks_per_root,
                )
        if self._event_ledger is not None:
            self._event_ledger.insert_task(
                record,
                event_type="task.created",
                event_data=lifecycle_event_data(record),
            )
        elif self._store is not None:
            self._store.insert_task(record)
        self._tasks[task_id] = record
        return record

    def get_task(self, task_id: str) -> TaskRecord:
        """
        获取任务记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            对应的任务记录

        Raises:
            UnknownWorkerTaskError: task_id 不存在
        """
        # 为什么集中读取 task：所有状态读写都应经过一个统一入口，避免分散状态判断。
        try:
            return self._tasks[task_id]
        except KeyError:
            loaded = self.load_task_by_id(task_id)
            if loaded is not None:
                return loaded
            raise UnknownWorkerTaskError(f"Unknown worker task: {task_id}")

    def list_tasks(self) -> list[TaskRecord]:
        """
        列出所有任务记录

        Returns:
            当前 runtime 已追踪的任务记录列表
        """
        # 为什么暴露任务列表：主 agent 和 UI 都需要知道当前 runtime 中有哪些活动任务。
        return list(self._tasks.values())

    def list_persisted_tasks(self) -> list[TaskRecord]:
        """List all known tasks, lazily restoring persisted records when needed."""
        if self._store is None:
            return self.list_tasks()
        for stored in self._store.list_tasks():
            current = self._tasks.get(stored.task_id)
            if current is not None and self._has_live_active_run(current):
                continue
            self._restore_stored_record(stored)
        return self.list_tasks()

    def find_by_review_id(self, review_id: str) -> TaskRecord | None:
        review = self.get_pending_review(review_id)
        if review is None:
            return None
        try:
            return self.get_task(review.task_id)
        except UnknownWorkerTaskError:
            return None

    def find_by_thread_id(self, thread_id: str) -> TaskRecord | None:
        """
        通过 thread_id 查找任务

        Args:
            thread_id: agent 会话 thread ID

        Returns:
            匹配的任务记录；找不到则返回 None
        """
        # 为什么提供 thread_id 反查：部分工具调用路径可能只保留 thread_id，
        # 这里作为 task_id 上下文缺失时的防御性兜底。
        for record in self._tasks.values():
            if record.thread_id == thread_id:
                return record
        return None

    def count_tasks_under_root(self, root_task_id: str) -> int:
        """
        统计某棵委托树下的任务数量

        Args:
            root_task_id: 委托树根任务 ID

        Returns:
            root_task_id 相同的任务数量
        """
        if self._store is not None:
            return self._store.count_tasks_under_root(root_task_id)
        return sum(
            1 for record in self._tasks.values() if record.root_task_id == root_task_id
        )

    def mark_running(
        self,
        task_id: str,
        run_task: asyncio.Task[None],
        *,
        mailbox_wakeup_sequence: int | None = None,
    ) -> None:
        """
        标记任务进入 running 状态

        Args:
            task_id: 当前 runtime 内部任务 ID
            run_task: 本地执行的 asyncio task
        """
        # 为什么单独标记 running：异步任务真正开始执行的时点需要被明确记录。
        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            if mailbox_wakeup_sequence is not None:
                record.mailbox_wakeup_sequence = max(
                    record.mailbox_wakeup_sequence, mailbox_wakeup_sequence
                )
            record.state = "running"
            record.updated_at = _now()
            self._live_runs.register(task_id, run_task)
            record.run_count += 1
            record.mailbox_suppressed = False
            record.mailbox_delivered = False
            record.error = None
            self._clear_pending_review_and_save(record)

    def add_artifact(self, task_id: str, artifact: PublishedArtifact) -> None:
        """Append a published artifact manifest to a task."""
        record = self.get_task(task_id)
        record.artifacts.append(artifact)
        record.updated_at = _now()
        if self._event_ledger is not None:
            self._event_ledger.update_task(
                record,
                event_type="task.artifact_published",
                event_data=artifact_event_data(record, artifact),
            )
        else:
            self._save(record)

    def mark_mailbox_delivered(self, task_id: str) -> None:
        """记录当前 run 的 mailbox 消息已经成功入队。"""
        record = self.get_task(task_id)
        record.mailbox_delivered = True
        self._save(record)

    def observe_mailbox_delivered(self, task_id: str, *, run_count: int) -> None:
        """Mirror a delivery already committed by the outbox transaction."""

        record = self.get_task(task_id)
        if record.run_count == run_count:
            record.mailbox_delivered = True

    def mark_mailbox_suppressed(self, task_id: str) -> None:
        """记录当前 run 的 mailbox 消息不再需要投递。"""
        record = self.get_task(task_id)
        with self._review_memory_transaction(record):
            record.mailbox_suppressed = True
            if self._settled_outbox_enabled and self._store is not None:
                self._store.suppress_settled_delivery(record)
            else:
                self._save(record)

    def mark_waiting_for_human(
        self,
        task_id: str,
        pending_review: dict[str, Any],
    ) -> None:
        """标记本地 worker 暂停在人工审批点。"""
        record = self.get_task(task_id)
        pending_review = dict(pending_review)
        review_id = pending_review.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            review_id = str(uuid.uuid4())
        else:
            review_id = normalize_task_event_text(review_id)
        pending_review["review_id"] = review_id
        existing = self.get_pending_review(review_id)
        current_review = self._review_for_task(record.task_id)
        if existing is not None and existing.task_id != record.task_id:
            raise ValueError(f"Pending review already exists: {review_id}")
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            record.state = "waiting_for_human"
            record.updated_at = _now()
            record.pending_review = pending_review
            record.error = None
            review = PendingReviewRecord(
                review_id=review_id,
                task_id=record.task_id,
                root_task_id=record.root_task_id,
                payload=dict(pending_review),
                created_at=(
                    current_review.created_at
                    if current_review is not None
                    and current_review.review_id == review_id
                    else record.updated_at
                ),
                updated_at=record.updated_at,
                ingest_sequence=self._review_ingest_sequence(
                    current_review,
                    review_id,
                ),
            )
            reviews = [
                item
                for item in self.list_pending_reviews(
                    root_task_id=record.root_task_id
                )
                if item.task_id != record.task_id
            ]
            reviews.append(review)
            root, root_changed = self._project_root_review(record, reviews)
            self._persist_review_transition(
                record,
                pending_review=review,
                root=root,
                root_changed=root_changed,
            )
            if current_review is not None:
                self._pending_reviews.pop(current_review.review_id, None)
            review = self._persisted_review(review)
            self._pending_reviews[review.review_id] = review

    def mark_completed(self, task_id: str, result: str) -> None:
        """
        标记任务成功完成

        Args:
            task_id: 当前 runtime 内部任务 ID
            result: 最终结果摘要
        """
        # 为什么单独标记 completed：wait/check 依赖稳定的本轮状态和结果。
        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            record.state = "completed"
            record.result = normalize_task_event_text(result)
            record.error = None
            record.updated_at = _now()
            self._clear_pending_review_and_save(record)

    def mark_failed(self, task_id: str, error: str) -> None:
        """
        标记任务失败

        Args:
            task_id: 当前 runtime 内部任务 ID
            error: 失败错误摘要
        """
        # 为什么单独标记 failed：失败应保留为结构化状态，而不是只在日志中消失。
        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            record.state = "failed"
            record.error = normalize_task_event_text(error)
            record.updated_at = _now()
            self._clear_pending_review_and_save(record)

    def reject_mailbox_run(
        self, task_id: str, error: str, *, mailbox_wakeup_sequence: int
    ) -> None:
        """Commit one rejected admission without rewriting the preceding run."""
        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            record.run_count += 1
            record.mailbox_wakeup_sequence = max(
                record.mailbox_wakeup_sequence, mailbox_wakeup_sequence
            )
            record.mailbox_suppressed = False
            record.mailbox_delivered = False
            record.state = "failed"
            record.result = None
            record.error = normalize_task_event_text(error)
            record.updated_at = _now()
            self._clear_pending_review_and_save(record)

    def mark_cancelled(self, task_id: str) -> None:
        """
        标记任务取消

        Args:
            task_id: 当前 runtime 内部任务 ID
        """
        # 为什么单独标记 cancelled：主动取消当前 run 不应和失败混在一起。
        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            record.state = "cancelled"
            record.updated_at = _now()
            record.error = None
            self._clear_pending_review_and_save(record)

    def mark_interrupted(self, task_id: str, error: str) -> None:
        """
        标记任务被运行时中断

        这个状态用于进程退出、事件循环关闭或其它非业务取消场景。它区别于
        用户显式 cancel_agent/cancel_task 产生的 cancelled。
        """
        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            record.state = "interrupted"
            record.error = normalize_task_event_text(error)
            record.updated_at = _now()
            self._clear_pending_review_and_save(record)

    def begin_external_operation(
        self,
        task_id: str,
        *,
        operation: str,
        identity: str,
        allow_replay: bool = False,
    ) -> None:
        remote_reconciliation.begin_external_operation(
            self,
            task_id,
            operation=operation,
            identity=identity,
            allow_replay=allow_replay,
        )

    def reject_external_operation(
        self,
        task_id: str,
        *,
        operation: str,
        identity: str,
    ) -> None:
        remote_reconciliation.reject_external_operation(
            self,
            task_id,
            operation=operation,
            identity=identity,
        )

    def mark_external_outcome_uncertain(
        self,
        task_id: str,
        *,
        operation: str,
        identity: str,
    ) -> None:
        remote_reconciliation.mark_external_outcome_uncertain(
            self,
            task_id,
            operation=operation,
            identity=identity,
        )

    def bind_uncertain_remote_task(
        self,
        task_id: str,
        upstream_task_id: str,
    ) -> TaskRecord:
        return remote_reconciliation.bind_uncertain_remote_task(
            self,
            task_id,
            upstream_task_id,
        )

    def set_remote_webhook_if_missing(
        self,
        task_id: str,
        webhook: dict[str, Any],
    ) -> TaskRecord:
        return remote_reconciliation.set_remote_webhook_if_missing(
            self,
            task_id,
            webhook,
        )

    def bind_and_sync_remote_task(
        self,
        task_id: str,
        upstream_task_id: str,
        payload: dict[str, Any],
    ) -> TaskRecord:
        return remote_reconciliation.bind_and_sync_remote_task(
            self,
            task_id,
            upstream_task_id,
            payload,
        )

    def sync_remote_task(self, task_id: str, payload: dict[str, Any]) -> TaskRecord:
        """Synchronize a remote Task with strong in-memory exception safety."""

        record = self.get_task(task_id)
        root = self._root_record(record)
        with self._review_memory_transaction(record, root):
            return remote_reconciliation.sync_remote_task(self, task_id, payload)

    def find_by_upstream_task_id(self, upstream_task_id: str) -> TaskRecord | None:
        """
        通过远端 upstream_task_id 查找本地任务

        Args:
            upstream_task_id: 远端网关返回的任务 ID

        Returns:
            匹配的本地任务记录；找不到则返回 None
        """
        for record in self._tasks.values():
            if record.upstream_task_id == upstream_task_id:
                return record
        return None
