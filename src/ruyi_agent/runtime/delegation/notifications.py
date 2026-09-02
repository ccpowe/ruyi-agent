"""Durable settled-run notification coordination."""

from __future__ import annotations

import logging

from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.safe_errors import safe_error_text, safe_exception_summary
from ruyi_agent.storage.settled_outbox import SettledOutboxIntent
from ruyi_agent.task_models import SETTLED_TASK_STATES, TaskRecord


logger = logging.getLogger(__name__)


class SettledRunNotifier:
    """Commit, dispatch, reconcile, and suppress settled Task notifications."""

    def __init__(
        self,
        task_manager: TaskManager,
        mailbox: AgentMailbox | None,
    ) -> None:
        self._task_manager = task_manager
        self._mailbox = mailbox
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

    def publish_settled_message(self, task_id: str) -> list[str]:
        """Attempt immediate delivery without making Task execution depend on it."""

        record = self._task_manager.get_task(task_id)
        if (
            self._mailbox is None
            or record.parent_thread_id is None
            or record.mailbox_suppressed
            or record.mailbox_delivered
            or not self._is_settled_record(record)
        ):
            return []
        if self._task_manager.settled_outbox_enabled:
            return self._dispatch_available()
        return self._publish_direct(record)

    def _publish_direct(self, record: TaskRecord) -> list[str]:
        """Preserve the non-durable compatibility path with safe retry state."""

        assert self._mailbox is not None
        content = record.result or (
            safe_error_text(record.error)
            if record.error
            else f"Task run ended with state={record.state}"
        )
        try:
            published = self._mailbox.publish_settled(
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
            return []
        if published is None:
            return []
        try:
            self._task_manager.mark_mailbox_delivered(record.task_id)
        except Exception as exc:
            self._record_error(
                exc,
                context=f"record direct delivery task={record.task_id}",
            )
        return [record.parent_task_id] if record.parent_task_id is not None else []

    def _dispatch_available(self) -> list[str]:
        """Drain currently claimable intents through fenced mailbox commits."""

        mailbox = self._mailbox
        if mailbox is None:
            return []
        wake_ids: list[str] = []
        self._retract_suppressed()
        try:
            intents = self._task_manager.claim_settled_outbox()
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
                self._task_manager.observe_mailbox_delivered(
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
            self._task_manager.release_settled_outbox_claim(
                intent,
                error=safe_exception_summary(exc),
            )
        except Exception as release_exc:
            self._record_error(
                release_exc,
                context=f"release failed claim {intent.outbox_key}",
            )

    def _retract_suppressed(self) -> None:
        mailbox = self._mailbox
        if mailbox is None:
            return
        try:
            intents = self._task_manager.list_suppressed_settled_outbox()
        except Exception as exc:
            self._record_error(exc, context="list suppressed settled outbox")
            return
        for intent in intents:
            try:
                mailbox.retract_settled_outbox(intent)
            except Exception as exc:
                self._record_error(exc, context=f"retract {intent.outbox_key}")

    async def reconcile(self) -> list[str]:
        """Repair legacy/missed intents, delivery, retraction, and parent wakeups."""

        if not self._task_manager.settled_outbox_enabled:
            return []
        if not self._legacy_reconciliation_complete:
            try:
                migration = self._task_manager.reconcile_settled_outbox_batch()
                self._legacy_reconciliation_complete = migration.completed
            except Exception as exc:
                self._record_error(exc, context="reconcile legacy settlements")
        wake_ids = set(self._dispatch_available())
        mailbox = self._mailbox
        if mailbox is not None:
            try:
                wake_ids.update(mailbox.pending_trigger_recipient_task_ids())
            except Exception as exc:
                self._record_error(exc, context="list pending mailbox wakeups")
        return sorted(wake_ids)

    def suppress_mailbox_delivery(self, record: TaskRecord) -> None:
        """Fence and retract the current run when wait/check consumed it."""

        self._task_manager.mark_mailbox_suppressed(record.task_id)
        mailbox = self._mailbox
        if mailbox is None or record.parent_thread_id is None:
            return
        if self._task_manager.settled_outbox_enabled:
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

    def _record_error(self, exc: BaseException, *, context: str) -> None:
        self._last_error = exc
        logger.warning(
            "Settled notification %s failed: %s",
            context,
            safe_exception_summary(exc),
        )
