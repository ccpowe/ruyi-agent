from __future__ import annotations

import asyncio
import logging
import random
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from ruyi_agent.channels.gateway_client import gateway_task_from_payload
from ruyi_agent.channels.gateway_dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.task_watch import TaskWatchHooks, TaskWatchManager
from ruyi_agent.storage.channel_delivery_store import (
    ChannelDeliveryIntent,
    ChannelDeliverySchedule,
    DELIVERY_KIND_REVIEW,
    DELIVERY_KIND_TERMINAL,
    DELIVERY_STATE_DELIVERED,
    DELIVERY_STATE_ERROR,
    DELIVERY_STATE_SUPERSEDED,
    DELIVERY_STATE_TERMINAL_GRACE,
    ChannelDeliveryStore,
)


TaskCallback = Callable[[GatewayTask], Awaitable[None]]
ArtifactCallback = Callable[[GatewayTask, GatewayPublishedArtifact], Awaitable[None]]
Effect = Callable[[], Awaitable[None]]
Random = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]


_LOGGER = logging.getLogger(__name__)


class ChannelDeliveryLeaseLost(RuntimeError):
    """The fenced delivery lease expired or was claimed by another owner."""


@dataclass(frozen=True, slots=True)
class ChannelDeliveryRedrivePolicy:
    """Persistent-query-error backoff and bounded reconciliation settings."""

    base_delay: float = 60.0
    max_delay: float = 3600.0
    jitter_ratio: float = 0.2
    scan_interval: float = 30.0
    batch_limit: int = 100

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError("delivery redrive base delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("delivery redrive max delay must not be below base delay")
        if self.jitter_ratio < 0:
            raise ValueError("delivery redrive jitter ratio must not be negative")
        if self.scan_interval <= 0:
            raise ValueError("delivery reconciliation scan interval must be positive")
        if self.batch_limit <= 0:
            raise ValueError("delivery reconciliation batch limit must be positive")

    def delay(self, redrive_count: int, *, random_value: float) -> float:
        exponent = min(31, max(0, redrive_count - 1))
        base = min(self.max_delay, self.base_delay * (2**exponent))
        jitter = base * self.jitter_ratio * max(0.0, min(1.0, random_value))
        previous = 0.0
        if exponent:
            previous_base = min(self.max_delay, self.base_delay * (2 ** (exponent - 1)))
            previous = min(self.max_delay, previous_base * (1 + self.jitter_ratio))
        return min(self.max_delay, max(1e-06, previous, base + jitter))


# A short name is useful for embedders while retaining one implementation.
RedrivePolicy = ChannelDeliveryRedrivePolicy


@dataclass(slots=True)
class ChannelDeliveryHooks:
    send_review: TaskCallback
    send_terminal_message: TaskCallback
    send_artifact: ArtifactCallback
    on_superseded: TaskCallback | None = None
    on_terminal_delivered: TaskCallback | None = None
    on_error: Callable[[Exception], Awaitable[None]] | None = None


HooksFactory = Callable[[ChannelDeliveryIntent], ChannelDeliveryHooks]


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
    """Shared durable Task Watch, retry, recovery, and delivery state machine."""

    def __init__(
        self,
        *,
        task_watch: TaskWatchManager,
        store: ChannelDeliveryStore | None = None,
        platform: str | None = None,
        owner_id: str | None = None,
        lease_seconds: float = 30.0,
        lease_heartbeat_interval: float | None = None,
        redrive_policy: ChannelDeliveryRedrivePolicy | None = None,
        redrive_base_delay: float | None = None,
        redrive_max_delay: float | None = None,
        redrive_jitter_ratio: float | None = None,
        reconcile_scan_interval: float | None = None,
        reconcile_batch_limit: int | None = None,
        reconciler_scan_interval: float | None = None,
        reconciler_batch_limit: int | None = None,
        reconcile_sleep: Sleep = asyncio.sleep,
        random_value: Random = random.random,
    ) -> None:
        self.task_watch = task_watch
        self.review_presenter = ReviewPresenter()
        self.terminal_presenter = TerminalPresenter()
        self._store = store
        self._platform = platform
        self._owner_id = owner_id or uuid.uuid4().hex
        self._lease_seconds = lease_seconds
        self._lease_heartbeat_interval = (
            lease_seconds / 3
            if lease_heartbeat_interval is None
            else lease_heartbeat_interval
        )
        if self._lease_heartbeat_interval <= 0:
            raise ValueError("Channel delivery heartbeat interval must be positive")
        if reconcile_scan_interval is not None and reconciler_scan_interval is not None:
            if reconcile_scan_interval != reconciler_scan_interval:
                raise ValueError("Conflicting reconciliation scan intervals")
        if reconcile_batch_limit is not None and reconciler_batch_limit is not None:
            if reconcile_batch_limit != reconciler_batch_limit:
                raise ValueError("Conflicting reconciliation batch limits")
        scan_interval = (
            reconcile_scan_interval
            if reconcile_scan_interval is not None
            else reconciler_scan_interval
        )
        batch_limit = (
            reconcile_batch_limit
            if reconcile_batch_limit is not None
            else reconciler_batch_limit
        )
        base_policy = redrive_policy or ChannelDeliveryRedrivePolicy()
        base_delay = base_policy.base_delay
        max_delay = base_policy.max_delay
        jitter_ratio = base_policy.jitter_ratio
        policy_scan_interval = base_policy.scan_interval
        policy_batch_limit = base_policy.batch_limit
        if redrive_base_delay is not None:
            base_delay = redrive_base_delay
        if redrive_max_delay is not None:
            max_delay = redrive_max_delay
        if redrive_jitter_ratio is not None:
            jitter_ratio = redrive_jitter_ratio
        if scan_interval is not None:
            policy_scan_interval = scan_interval
        if batch_limit is not None:
            policy_batch_limit = batch_limit
        self._redrive_policy = ChannelDeliveryRedrivePolicy(
            base_delay=base_delay,
            max_delay=max_delay,
            jitter_ratio=jitter_ratio,
            scan_interval=policy_scan_interval,
            batch_limit=policy_batch_limit,
        )
        # Adapters enable the supervisor from ``recover()``.  A caller that
        # explicitly supplies redrive settings may also use the coordinator
        # directly, so give that form the same in-process behavior without
        # changing the historical default-only direct composition path.
        self._auto_start_reconciler = any(
            value is not None
            for value in (
                redrive_policy,
                redrive_base_delay,
                redrive_max_delay,
                redrive_jitter_ratio,
                reconcile_scan_interval,
                reconcile_batch_limit,
                reconciler_scan_interval,
                reconciler_batch_limit,
            )
        )
        self._reconcile_sleep = reconcile_sleep
        self._random_value = random_value
        self._tokens: dict[tuple[str, int], tuple[str, str]] = {}
        self._restart_requests: dict[
            tuple[str, int], tuple[ChannelDeliveryIntent, ChannelDeliveryHooks]
        ] = {}
        self._hooks_by_intent: dict[str, ChannelDeliveryHooks] = {}
        self._hooks_factory: HooksFactory | None = None
        self._delivery_events: dict[str, asyncio.Event] = {}
        self._reconcile_wakeup = asyncio.Event()
        self._reconciler: asyncio.Task[None] | None = None
        self._reconciler_enabled = False
        self._closed = False

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
        """Backward-compatible in-memory composition for external embedders."""

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

    def ensure_delivery(
        self,
        *,
        session_key: str,
        chat_id: str,
        task_id: str,
        run_count: int,
        hooks: ChannelDeliveryHooks,
    ) -> bool:
        if self._closed:
            raise RuntimeError("ChannelDeliveryCoordinator is closed")
        if self._store is None or self._platform is None:
            self.ensure_watch(
                task_id=task_id,
                run_count=run_count,
                on_pending_review=hooks.send_review,
                on_terminal=self._legacy_terminal_hook(hooks),
                on_superseded=hooks.on_superseded,
                on_error=hooks.on_error,
            )
            return True
        intent = self._store.ensure_watch(
            platform=self._platform,
            session_key=session_key,
            chat_id=chat_id,
            task_id=task_id,
            run_count=run_count,
        )
        self._hooks_by_intent[intent.intent_id] = hooks
        key = (intent.task_id, intent.run_count)
        if self.task_watch.is_active(
            task_id=intent.task_id, run_count=intent.run_count
        ):
            self._restart_requests[key] = (intent, hooks)
            if self._auto_start_reconciler:
                self._start_reconciler()
            return False
        started = self._start_intent(intent, hooks=hooks)
        if self._auto_start_reconciler:
            self._start_reconciler()
        return started

    async def ensure_terminal_delivery(
        self,
        *,
        task: GatewayTask,
        session_key: str,
        chat_id: str,
        task_id: str,
        run_count: int,
        hooks: ChannelDeliveryHooks,
    ) -> None:
        """Wait until the durable terminal steps are acknowledged or fail."""

        if self._store is None or self._platform is None:
            await self._legacy_terminal_hook(hooks)(task)
            return
        intent = self._store.ensure_watch(
            platform=self._platform,
            session_key=session_key,
            chat_id=chat_id,
            task_id=task_id,
            run_count=run_count,
        )
        self._hooks_by_intent[intent.intent_id] = hooks
        event = self._delivery_events.setdefault(intent.intent_id, asyncio.Event())
        event.clear()
        self._start_intent(intent, hooks=hooks)
        if self._auto_start_reconciler:
            self._start_reconciler()
        while True:
            if self._closed:
                raise RuntimeError("ChannelDeliveryCoordinator is closed")
            current = self._store.get(intent.intent_id)
            if current is not None and current.state in {
                DELIVERY_STATE_TERMINAL_GRACE,
                DELIVERY_STATE_DELIVERED,
            }:
                return
            if current is None or (
                current.state in {DELIVERY_STATE_ERROR, DELIVERY_STATE_SUPERSEDED}
                and not self.is_active(task_id=task_id, run_count=run_count)
                and current.lease_token is None
            ):
                detail = current.last_error if current is not None else "missing intent"
                raise RuntimeError(f"Terminal delivery did not complete: {detail}")
            try:
                await asyncio.wait_for(event.wait(), timeout=0.1)
            except TimeoutError:
                if not self.is_active(task_id=task_id, run_count=run_count):
                    self._start_intent(current, hooks=hooks)
                continue
            event.clear()

    async def recover(
        self,
        hooks_for: HooksFactory,
    ) -> int:
        if self._closed:
            raise RuntimeError("ChannelDeliveryCoordinator is closed")
        if self._store is None or self._platform is None:
            return 0
        recovered = 0
        pending_hooks: dict[str, ChannelDeliveryHooks] = {}
        try:
            intents = await self._store.alist_recoverable(platform=self._platform)
            now = self._store.now()
            for intent in intents:
                hooks = hooks_for(intent)
                pending_hooks[intent.intent_id] = hooks
                if (
                    intent.state == DELIVERY_STATE_ERROR
                    and intent.next_attempt_at is not None
                    and intent.next_attempt_at > now
                ):
                    # A persistent query-error backoff survives a restart.  The
                    # reconciler owns the eventual due claim; explicit delivery
                    # calls still use _start_intent for an immediate attempt.
                    continue
                if self._start_intent(intent, hooks=hooks):
                    recovered += 1
            if self._closed:
                raise RuntimeError("ChannelDeliveryCoordinator is closed")
            self._hooks_by_intent.clear()
            self._hooks_by_intent.update(pending_hooks)
            self._hooks_factory = hooks_for
            self._start_reconciler()
            return recovered
        except BaseException:
            self._restart_requests.clear()
            reconciler = self._reconciler
            if reconciler is not None and not reconciler.done():
                reconciler.cancel()
                await asyncio.gather(reconciler, return_exceptions=True)
            for (task_id, run_count), (intent_id, token) in list(self._tokens.items()):
                await self.task_watch.cancel(task_id=task_id, run_count=run_count)
                try:
                    self._store.release(intent_id, token=token)
                except BaseException:
                    pass
            self._tokens.clear()
            self._restart_requests.clear()
            self._hooks_by_intent.clear()
            self._hooks_factory = None
            self._reconciler_enabled = False
            self._reconcile_wakeup.set()
            raise

    def _start_reconciler(self) -> None:
        if self._store is None or self._platform is None or self._closed:
            return
        self._reconciler_enabled = True
        if self._reconciler is not None and not self._reconciler.done():
            return
        self._reconciler = asyncio.create_task(
            self._reconcile_loop(),
            name=f"channel-delivery-reconciler:{self._platform}",
        )
        self._reconciler.add_done_callback(self._consume_reconciler_result)

    async def _reconcile_loop(self) -> None:
        discovery_attempt = 0
        try:
            while not self._closed:
                try:
                    if self._store is None or self._platform is None:
                        return
                    schedule = await self._store.aschedule_snapshot(
                        platform=self._platform,
                        limit=self._redrive_policy.batch_limit,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    discovery_attempt += 1
                    self._log_reconcile_failure(exc, discovery_attempt)
                    await self._wait_for_schedule(
                        None,
                        self._discovery_delay(discovery_attempt),
                        backoff=True,
                    )
                    continue
                failed = False
                progressed = False
                for intent in schedule.due_errors:
                    try:
                        outcome = await self._redrive_due(intent, schedule.observed_at)
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        failed = True
                        self._log_reconcile_failure(exc, discovery_attempt + 1)
                        continue
                    if outcome:
                        progressed = True
                    elif outcome is None:
                        failed = True
                if failed:
                    discovery_attempt += 1
                    await self._wait_for_schedule(
                        schedule,
                        self._discovery_delay(discovery_attempt),
                        backoff=True,
                    )
                elif progressed:
                    discovery_attempt = 0
                    await asyncio.sleep(0)
                else:
                    if schedule.earliest_due_at is not None:
                        timeout = min(
                            self._redrive_policy.scan_interval,
                            max(0.0, schedule.earliest_due_at - schedule.observed_at),
                        )
                        discovery_attempt = 0
                    else:
                        discovery_attempt += 1
                        timeout = self._discovery_delay(discovery_attempt)
                    await self._wait_for_schedule(schedule, timeout)
        finally:
            if self._reconciler is asyncio.current_task():
                self._reconciler = None

    async def _redrive_due(
        self,
        intent: ChannelDeliveryIntent,
        observed_at: float,
    ) -> bool | None:
        if self._store is None:
            return False
        token = self._store.claim_due_error(
            intent.intent_id,
            owner=self._owner_id,
            lease_seconds=self._lease_seconds,
            now=observed_at,
        )
        if token is None:
            return None
        hooks = self._hooks_by_intent.get(intent.intent_id)
        started = False
        try:
            if hooks is None:
                if self._hooks_factory is None:
                    return None
                hooks = self._hooks_factory(intent)
            if not self._start_intent(intent, hooks=hooks, token=token):
                return None
            started = True
            self._hooks_by_intent.setdefault(intent.intent_id, hooks)
            return True
        finally:
            if not started:
                try:
                    self._store.release(intent.intent_id, token=token)
                except BaseException:
                    pass

    def _discovery_delay(self, attempt: int) -> float:
        base = min(1.0, self._redrive_policy.scan_interval / 4.0)
        return min(
            self._redrive_policy.scan_interval,
            base * (2 ** min(5, max(0, attempt - 1))),
        )

    def _log_reconcile_failure(self, exc: Exception, attempt: int) -> None:
        _LOGGER.warning(
            "Channel delivery reconciliation failed; retrying with backoff",
            extra={
                "platform": self._platform,
                "owner_id": self._owner_id,
                "failure_attempt": attempt,
                "error_type": type(exc).__name__,
            },
            exc_info=True,
        )

    async def _wait_for_schedule(
        self,
        schedule: ChannelDeliverySchedule | None,
        timeout: float,
        *,
        backoff: bool = False,
    ) -> None:
        if self._closed:
            return
        if timeout <= 0:
            await asyncio.sleep(0)
            return
        if not backoff and self._reconcile_wakeup.is_set():
            self._reconcile_wakeup.clear()
            return
        if backoff:
            self._reconcile_wakeup.clear()
        wake = asyncio.create_task(self._reconcile_wakeup.wait())
        timer = asyncio.create_task(self._reconcile_sleep(timeout))
        try:
            await asyncio.wait(
                {wake, timer},
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            for task in (wake, timer):
                if not task.done():
                    task.cancel()
            await asyncio.gather(wake, timer, return_exceptions=True)
        await asyncio.sleep(0)

    @staticmethod
    def _consume_reconciler_result(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            return

    def _start_intent(
        self,
        intent: ChannelDeliveryIntent,
        *,
        hooks: ChannelDeliveryHooks,
        token: str | None = None,
    ) -> bool:
        if self._store is None:
            return False
        if self._closed:
            if token is not None:
                self._store.release(intent.intent_id, token=token)
            return False
        key = (intent.task_id, intent.run_count)
        if self.task_watch.is_active(
            task_id=intent.task_id, run_count=intent.run_count
        ):
            if token is not None:
                self._store.release(intent.intent_id, token=token)
            return False
        if token is None:
            token = self._store.claim(
                intent.intent_id,
                owner=self._owner_id,
                lease_seconds=self._lease_seconds,
            )
        if token is None:
            return False
        self._tokens[key] = (intent.intent_id, token)
        redrive_delay: float | None = None

        async def observed(_: GatewayTask) -> None:
            if not self._store.renew(
                intent.intent_id,
                token=token,
                lease_seconds=self._lease_seconds,
            ):
                raise ChannelDeliveryLeaseLost("Channel delivery lease was lost")
            self._require_owned(
                self._store.mark_watching(intent.intent_id, token=token)
            )

        async def retry(exc: Exception, attempt: int, delay: float) -> None:
            self._require_owned(
                self._store.mark_retry(
                    intent.intent_id,
                    token=token,
                    attempt=attempt,
                    delay=delay,
                    error=str(exc),
                )
            )
            self._require_owned(
                self._store.renew(
                    intent.intent_id,
                    token=token,
                    lease_seconds=max(self._lease_seconds, delay + 1.0),
                )
            )

        async def exhausted(exc: Exception, auto_redrive: bool) -> None:
            del exc
            nonlocal redrive_delay
            redrive_delay = None
            if not auto_redrive or not self._reconciler_enabled:
                return
            current = self._store.get(intent.intent_id)
            count = (
                current.redrive_count if current is not None else intent.redrive_count
            )
            redrive_delay = self._redrive_policy.delay(
                count + 1,
                random_value=self._random_value(),
            )

        async def failed(exc: Exception) -> None:
            owned = self._store.mark_error(
                intent.intent_id,
                token=token,
                error=str(exc),
                redrive_delay=redrive_delay,
            )
            self._tokens.pop(key, None)
            self._restart_requests.pop(key, None)
            self._signal_delivery(intent.intent_id)
            if owned and redrive_delay is not None:
                self._reconcile_wakeup.set()
            if owned and hooks.on_error is not None:
                await hooks.on_error(exc)

        async def superseded(task: GatewayTask) -> None:
            self._require_owned(
                self._store.mark_superseded(intent.intent_id, token=token)
            )
            self._tokens.pop(key, None)
            self._restart_requests.pop(key, None)
            self._signal_delivery(intent.intent_id)
            if hooks.on_superseded is not None:
                await hooks.on_superseded(task)

        async def stopped() -> None:
            self._store.release(intent.intent_id, token=token)
            self._tokens.pop(key, None)
            restart = self._restart_requests.pop(key, None)
            if restart is not None and not self._closed:
                self._start_intent(restart[0], hooks=restart[1])

        try:
            self.task_watch.ensure(
                task_id=intent.task_id,
                run_count=intent.run_count,
                hooks=TaskWatchHooks(
                    on_pending_review=lambda task: self._deliver_review(
                        intent=intent,
                        token=token,
                        task=task,
                        hooks=hooks,
                    ),
                    on_terminal=lambda task: self._deliver_terminal(
                        intent=intent,
                        token=token,
                        task=task,
                        hooks=hooks,
                    ),
                    on_terminal_grace_complete=lambda task: self._complete_terminal(
                        intent=intent,
                        token=token,
                        task=task,
                    ),
                    on_superseded=superseded,
                    on_observed=observed,
                    on_retry=retry,
                    on_exhausted=exhausted,
                    on_error=failed,
                    on_stopped=stopped,
                ),
            )
        except BaseException:
            self._tokens.pop(key, None)
            self._store.release(intent.intent_id, token=token)
            raise
        return True

    async def _deliver_review(
        self,
        *,
        intent: ChannelDeliveryIntent,
        token: str,
        task: GatewayTask,
        hooks: ChannelDeliveryHooks,
    ) -> None:
        if self._store is None:
            await hooks.send_review(task)
            return
        review_id = task.pending_review.review_id if task.pending_review else "missing"
        self._require_owned(
            self._store.mark_delivering(
                intent.intent_id,
                token=token,
                delivery_kind=DELIVERY_KIND_REVIEW,
                review_id=review_id,
            )
        )
        step = f"review:{review_id}:message"
        if not self._store.step_delivered(intent.intent_id, step_key=step):
            await self._run_effect(
                intent_id=intent.intent_id,
                token=token,
                effect=lambda: hooks.send_review(task),
            )
            self._require_owned(
                self._store.mark_step_delivered(
                    intent.intent_id,
                    token=token,
                    step_key=step,
                )
            )
        self._require_owned(
            self._store.mark_review_waiting(intent.intent_id, token=token)
        )

    async def _deliver_terminal(
        self,
        *,
        intent: ChannelDeliveryIntent,
        token: str,
        task: GatewayTask,
        hooks: ChannelDeliveryHooks,
    ) -> None:
        if self._store is None:
            await self._legacy_terminal_hook(hooks)(task)
            return
        self._require_owned(
            self._store.mark_delivering(
                intent.intent_id,
                token=token,
                delivery_kind=DELIVERY_KIND_TERMINAL,
                review_id=None,
            )
        )
        message_step = f"terminal:{task.run_count}:message"
        if not self._store.step_delivered(intent.intent_id, step_key=message_step):
            await self._run_effect(
                intent_id=intent.intent_id,
                token=token,
                effect=lambda: hooks.send_terminal_message(task),
            )
            self._require_owned(
                self._store.mark_step_delivered(
                    intent.intent_id,
                    token=token,
                    step_key=message_step,
                )
            )
        for artifact in task.artifacts:
            if artifact.run_count != task.run_count:
                continue
            artifact_step = f"terminal:{task.run_count}:artifact:{artifact.artifact_id}"
            if self._store.step_delivered(intent.intent_id, step_key=artifact_step):
                continue
            await self._run_effect(
                intent_id=intent.intent_id,
                token=token,
                effect=lambda artifact=artifact: hooks.send_artifact(task, artifact),
            )
            self._require_owned(
                self._store.mark_step_delivered(
                    intent.intent_id,
                    token=token,
                    step_key=artifact_step,
                )
            )
        delivered_hook_step = f"terminal:{task.run_count}:delivered-hook"
        if hooks.on_terminal_delivered is not None and not self._store.step_delivered(
            intent.intent_id, step_key=delivered_hook_step
        ):
            delivered_callback = hooks.on_terminal_delivered
            await self._run_effect(
                intent_id=intent.intent_id,
                token=token,
                effect=lambda: delivered_callback(task),
            )
            self._require_owned(
                self._store.mark_step_delivered(
                    intent.intent_id,
                    token=token,
                    step_key=delivered_hook_step,
                )
            )
        self._require_owned(
            self._store.mark_terminal_steps_done(intent.intent_id, token=token)
        )
        self.terminal_presenter.delivered_run_counts[task.task_id] = task.run_count
        self._signal_delivery(intent.intent_id)

    async def _complete_terminal(
        self,
        *,
        intent: ChannelDeliveryIntent,
        token: str,
        task: GatewayTask,
    ) -> None:
        del task
        if self._store is None:
            return
        self._require_owned(self._store.mark_delivered(intent.intent_id, token=token))
        self._signal_delivery(intent.intent_id)

    async def _run_effect(
        self,
        *,
        intent_id: str,
        token: str,
        effect: Effect,
    ) -> None:
        if self._store is None:
            await effect()
            return
        operation = asyncio.create_task(effect())
        heartbeat = asyncio.create_task(self._heartbeat(intent_id, token))
        try:
            done, _ = await asyncio.wait(
                {operation, heartbeat}, return_when=asyncio.FIRST_COMPLETED
            )
            if heartbeat in done:
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
                await heartbeat
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            await operation
        finally:
            for pending in (operation, heartbeat):
                if not pending.done():
                    pending.cancel()
            await asyncio.gather(operation, heartbeat, return_exceptions=True)
        if not self._store.renew(
            intent_id,
            token=token,
            lease_seconds=self._lease_seconds,
        ):
            raise ChannelDeliveryLeaseLost("Channel delivery lease was lost")

    async def _heartbeat(self, intent_id: str, token: str) -> None:
        if self._store is None:  # pragma: no cover - caller invariant
            return
        while True:
            await asyncio.sleep(self._lease_heartbeat_interval)
            if not self._store.renew(
                intent_id,
                token=token,
                lease_seconds=self._lease_seconds,
            ):
                raise ChannelDeliveryLeaseLost("Channel delivery lease was lost")

    def _signal_delivery(self, intent_id: str) -> None:
        event = self._delivery_events.get(intent_id)
        if event is not None:
            event.set()

    @staticmethod
    def _legacy_terminal_hook(hooks: ChannelDeliveryHooks) -> TaskCallback:
        async def deliver(task: GatewayTask) -> None:
            await hooks.send_terminal_message(task)
            for artifact in task.artifacts:
                if artifact.run_count == task.run_count:
                    await hooks.send_artifact(task, artifact)
            if hooks.on_terminal_delivered is not None:
                await hooks.on_terminal_delivered(task)

        return deliver

    @staticmethod
    def _require_owned(updated: bool) -> None:
        if not updated:
            raise ChannelDeliveryLeaseLost("Channel delivery lease was lost")

    def is_active(self, *, task_id: str, run_count: int) -> bool:
        return self.task_watch.is_active(task_id=task_id, run_count=run_count)

    async def wait(self) -> None:
        await self.task_watch.wait()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._reconciler_enabled = False
        self._reconcile_wakeup.set()
        reconciler = self._reconciler
        if reconciler is not None and not reconciler.done():
            reconciler.cancel()
        if reconciler is not None:
            await asyncio.gather(reconciler, return_exceptions=True)
        await self.task_watch.close()
        try:
            if self._store is not None:
                self._store.release_owner(self._owner_id)
        finally:
            self._tokens.clear()
            self._restart_requests.clear()
            self._hooks_by_intent.clear()
            self._hooks_factory = None
            for event in self._delivery_events.values():
                event.set()
            self._delivery_events.clear()


def delivery_session_key(
    task: GatewayTask,
    *,
    platform: str,
    chat_id: str,
) -> str:
    metadata: Mapping[str, Any] = task.metadata
    value = metadata.get("channel_session_key")
    return str(value) if value else f"{platform}:chat:{chat_id}"
