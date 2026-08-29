"""Durable settled-run notification coordination."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any, Protocol

from ruyi_agent.storage.settled_outbox import SettledOutboxIntent
from ruyi_agent.task_models import SETTLED_TASK_STATES, TaskRecord


logger = logging.getLogger(__name__)


class NotificationHost(Protocol):
    _mailbox: Any
    _task_manager: Any

    async def _ensure_task_awake(self, task_id: str) -> TaskRecord: ...
    def _is_settled_record(self, record: TaskRecord) -> bool: ...


class SettledRunNotifier:
    """Commit, dispatch, reconcile, and suppress settled Task notifications."""

    def __init__(
        self,
        control: NotificationHost,
        *,
        reconciliation_interval: float = 1.0,
    ) -> None:
        self._control = control
        self._reconciliation_interval = reconciliation_interval
        self._reconciliation_task: asyncio.Task[None] | None = None
        self._wake_tasks: set[asyncio.Task[None]] = set()
        self._last_error: BaseException | None = None
        self._legacy_reconciliation_complete = False

    @property
    def last_error(self) -> BaseException | None:
        return self._last_error

    def _is_settled_record(self, record: TaskRecord) -> bool:
        return (
            record.state in SETTLED_TASK_STATES
            and record.external_operation is None
            and not record.external_outcome_uncertain
        )

    def _maybe_publish_settled_message(self, task_id: str) -> None:
        """Attempt immediate delivery without making Task execution depend on it."""

        record = self._control._task_manager.get_task(task_id)
        if (
            self._control._mailbox is None
            or record.parent_thread_id is None
            or record.mailbox_suppressed
            or record.mailbox_delivered
            or not self._is_settled_record(record)
        ):
            return
        if self._control._task_manager.settled_outbox_enabled:
            wake_ids = self._dispatch_available()
            self._schedule_wakeups(wake_ids)
            return
        self._publish_direct(record)

    def _publish_direct(self, record: TaskRecord) -> None:
        """Preserve the non-durable compatibility path with safe retry state."""

        assert self._control._mailbox is not None
        content = (
            record.result
            or record.error
            or f"Task run ended with state={record.state}"
        )
        try:
            published = self._control._mailbox.publish_settled(
                recipient_thread_id=record.parent_thread_id,
                recipient_task_id=record.parent_task_id,
                child_task_id=record.task_id,
                child_agent_name=record.agent_name,
                run_count=record.run_count,
                status=record.state,
                content=content,
            )
        except Exception as exc:
            self._record_error(exc, context=f"direct publish task={record.task_id}")
            return
        if published is None:
            return
        try:
            self._control._task_manager.mark_mailbox_delivered(record.task_id)
        except Exception as exc:
            self._record_error(
                exc,
                context=f"record direct delivery task={record.task_id}",
            )
        if record.parent_task_id is not None:
            self._schedule_wakeups([record.parent_task_id])

    def _dispatch_available(self) -> list[str]:
        """Drain currently claimable intents through fenced mailbox commits."""

        mailbox = self._control._mailbox
        if mailbox is None:
            return []
        wake_ids: list[str] = []
        self._retract_suppressed()
        try:
            intents = self._control._task_manager.claim_settled_outbox()
        except Exception as exc:
            self._record_error(exc, context="claim settled outbox")
            return wake_ids
        for intent in intents:
            try:
                delivered = mailbox.publish_claimed_settled_outbox(intent)
            except Exception as exc:
                self._record_error(exc, context=f"dispatch {intent.outbox_key}")
                self._release_failed_claim(intent, exc)
                continue
            if not delivered:
                continue
            try:
                self._control._task_manager.observe_mailbox_delivered(
                    intent.task_id,
                    run_count=intent.run_count,
                )
            except Exception as exc:
                self._record_error(
                    exc,
                    context=f"observe delivery {intent.outbox_key}",
                )
            if intent.recipient_task_id is not None:
                try:
                    needs_wake = mailbox.settled_outbox_needs_wake(intent)
                except Exception as exc:
                    self._record_error(
                        exc,
                        context=f"inspect wake requirement {intent.outbox_key}",
                    )
                else:
                    if needs_wake:
                        wake_ids.append(intent.recipient_task_id)
        return wake_ids

    def _release_failed_claim(
        self,
        intent: SettledOutboxIntent,
        exc: BaseException,
    ) -> None:
        try:
            self._control._task_manager.release_settled_outbox_claim(
                intent,
                error=f"{type(exc).__name__}: {exc}",
            )
        except Exception as release_exc:
            self._record_error(
                release_exc,
                context=f"release failed claim {intent.outbox_key}",
            )

    def _retract_suppressed(self) -> None:
        mailbox = self._control._mailbox
        if mailbox is None:
            return
        try:
            intents = self._control._task_manager.list_suppressed_settled_outbox()
        except Exception as exc:
            self._record_error(exc, context="list suppressed settled outbox")
            return
        for intent in intents:
            try:
                mailbox.retract_settled_outbox(intent)
            except Exception as exc:
                self._record_error(exc, context=f"retract {intent.outbox_key}")

    async def reconcile(self) -> None:
        """Repair legacy/missed intents, delivery, retraction, and parent wakeups."""

        if not self._control._task_manager.settled_outbox_enabled:
            return
        if not self._legacy_reconciliation_complete:
            try:
                migration = (
                    self._control._task_manager.reconcile_settled_outbox_batch()
                )
                self._legacy_reconciliation_complete = migration.completed
            except Exception as exc:
                self._record_error(exc, context="reconcile legacy settlements")
        wake_ids = set(self._dispatch_available())
        mailbox = self._control._mailbox
        if mailbox is not None:
            try:
                wake_ids.update(mailbox.pending_trigger_recipient_task_ids())
            except Exception as exc:
                self._record_error(exc, context="list pending mailbox wakeups")
        for task_id in sorted(wake_ids):
            await self._wake_parent(task_id)

    def start(self) -> None:
        """Start one supervised periodic reconciler for this runtime."""

        if (
            not self._control._task_manager.settled_outbox_enabled
            or self._reconciliation_task is not None
        ):
            return

        async def run() -> None:
            while True:
                await self.reconcile()
                await asyncio.sleep(self._reconciliation_interval)

        self._reconciliation_task = asyncio.create_task(run())

    async def close(self) -> None:
        """Stop and consume every notifier-owned background task."""

        task = self._reconciliation_task
        self._reconciliation_task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        wake_tasks = list(self._wake_tasks)
        self._wake_tasks.clear()
        if wake_tasks:
            for wake_task in wake_tasks:
                wake_task.cancel()
            await asyncio.gather(*wake_tasks, return_exceptions=True)

    def _suppress_mailbox_delivery(self, record: TaskRecord) -> None:
        """Fence and retract the current run when wait/check consumed it."""

        self._control._task_manager.mark_mailbox_suppressed(record.task_id)
        mailbox = self._control._mailbox
        if mailbox is None or record.parent_thread_id is None:
            return
        if self._control._task_manager.settled_outbox_enabled:
            self._retract_suppressed()
        # Also covers pre-outbox rows and the in-memory compatibility path.
        try:
            mailbox.retract(
                recipient_thread_id=record.parent_thread_id,
                child_task_id=record.task_id,
                run_count=record.run_count,
            )
        except Exception as exc:
            self._record_error(
                exc,
                context=f"retract compatibility task={record.task_id}",
            )

    def _schedule_wakeups(self, task_ids: list[str]) -> None:
        for task_id in set(task_ids):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return
            self._track_task(self._wake_parent(task_id))

    async def _wake_parent(self, task_id: str) -> None:
        try:
            await self._control._ensure_task_awake(task_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_error(exc, context=f"wake parent task={task_id}")

    def _track_task(self, coroutine: Coroutine[Any, Any, None]) -> None:
        task = asyncio.create_task(coroutine)
        self._wake_tasks.add(task)

        def consume(done: asyncio.Task[None]) -> None:
            self._wake_tasks.discard(done)
            if done.cancelled():
                return
            exc = done.exception()
            if exc is not None:
                self._record_error(exc, context="settled parent wake task")

        task.add_done_callback(consume)

    def _record_error(self, exc: BaseException, *, context: str) -> None:
        self._last_error = exc
        logger.warning(
            "Settled notification %s failed: %s",
            context,
            exc,
            exc_info=(type(exc), exc, exc.__traceback__),
        )
