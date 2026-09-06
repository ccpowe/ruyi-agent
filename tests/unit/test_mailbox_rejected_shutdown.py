"""Rejected mailbox attempts retain shutdown ownership through settlement."""

import asyncio

import pytest

from tests.unit.test_mailbox_wakeup_admission import event_counts, runtime, seed


def test_close_cancels_rejected_admission_webhook(tmp_path, monkeypatch):
    with runtime(tmp_path, monkeypatch, specs={}) as (control, store, _, mailbox):
        seed(store, mailbox)

        async def scenario():
            entered = asyncio.Event()
            cancelled = asyncio.Event()

            async def hanging_webhook(task_id):
                entered.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()

            monkeypatch.setattr(
                control._task_runtime._remote_port,
                "send_settled_webhook",
                hanging_webhook,
            )
            submission = asyncio.create_task(control.send_task_input("task-1", "new"))
            try:
                await asyncio.wait_for(entered.wait(), timeout=5)
                await control.close()
                assert submission.done()
                assert submission.cancelled()
                assert cancelled.is_set()
                assert not control._task_runtime._supervisor._operations
                assert store.get_task("task-1").state == "failed"
                assert event_counts(store).get("task.running", 0) == 0
                assert event_counts(store)["task.failed"] == 1
                assert mailbox.has_triggering_messages("task-1")
            finally:
                submission.cancel()
                await asyncio.gather(submission, return_exceptions=True)
                await control.close()

        asyncio.run(scenario())


def test_cancellation_after_rejected_operation_promotion_cleans_permit(
    tmp_path, monkeypatch
):
    with runtime(tmp_path, monkeypatch, specs={}) as (control, store, _, mailbox):
        seed(store, mailbox)
        supervisor = control._task_runtime._supervisor
        original_cleanup = supervisor.cleanup_mutation
        injected = False

        async def cancelled_cleanup(permit):
            nonlocal injected
            await original_cleanup(permit)
            if supervisor._operations and not injected:
                injected = True
                raise asyncio.CancelledError

        monkeypatch.setattr(supervisor, "cleanup_mutation", cancelled_cleanup)

        async def scenario():
            try:
                with pytest.raises(asyncio.CancelledError):
                    await control.send_task_input("task-1", "new")
                assert injected
                assert not supervisor._operations
                assert not supervisor._permits
                assert store.get_task("task-1").state == "failed"
                assert mailbox.has_triggering_messages("task-1")
            finally:
                await control.close()

        asyncio.run(scenario())
