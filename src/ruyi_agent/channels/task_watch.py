from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx

from ruyi_agent.channels.gateway_client import (
    GatewayClientError,
    GatewayTaskClient,
    gateway_task_from_payload,
)
from ruyi_agent.channels.gateway_dto import GatewayTask
from ruyi_agent.task_models import SETTLED_TASK_STATES


TERMINAL_TASK_STATES = SETTLED_TASK_STATES

TaskHook = Callable[[GatewayTask], Awaitable[None]]
ErrorHook = Callable[[Exception], Awaitable[None]]
RetryHook = Callable[[Exception, int, float], Awaitable[None]]
ObservedHook = Callable[[GatewayTask], Awaitable[None]]
StoppedHook = Callable[[], Awaitable[None]]
Sleep = Callable[[float], Awaitable[None]]
Random = Callable[[], float]


@dataclass(frozen=True, slots=True)
class WatchRetryPolicy:
    max_attempts: int = 5
    base_delay: float = 0.5
    max_delay: float = 30.0
    jitter_ratio: float = 0.2

    def delay(self, attempt: int, *, random_value: float) -> float:
        base = min(self.max_delay, self.base_delay * (2 ** max(0, attempt - 1)))
        jitter = base * self.jitter_ratio * max(0.0, min(1.0, random_value))
        return min(self.max_delay, base + jitter)


@dataclass(slots=True)
class TaskWatchHooks:
    on_pending_review: TaskHook
    on_terminal: TaskHook
    on_superseded: TaskHook | None = None
    on_observed: ObservedHook | None = None
    on_retry: RetryHook | None = None
    on_error: ErrorHook | None = None
    on_stopped: StoppedHook | None = None


def is_retryable_gateway_error(exc: Exception) -> bool:
    if isinstance(exc, GatewayClientError):
        return exc.status_code in {408, 429} or 500 <= exc.status_code <= 599
    if isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
        return True
    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    # Embedders historically exposed transport failures as RuntimeError.
    return isinstance(exc, RuntimeError)


class TaskWatchManager:
    """Observe Gateway Task runs with bounded retry and owned shutdown."""

    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        poll_interval: float = 2.0,
        terminal_review_grace_checks: int = 3,
        retry_policy: WatchRetryPolicy | None = None,
        sleep: Sleep = asyncio.sleep,
        random_value: Random = random.random,
    ) -> None:
        self._gateway_client = gateway_client
        self._poll_interval = poll_interval
        self._terminal_review_grace_checks = max(0, terminal_review_grace_checks)
        self._retry_policy = retry_policy or WatchRetryPolicy()
        self._sleep = sleep
        self._random_value = random_value
        self._watches: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._closed = False

    def ensure(
        self,
        *,
        task_id: str,
        run_count: int,
        hooks: TaskWatchHooks,
    ) -> None:
        if self._closed:
            raise RuntimeError("TaskWatchManager is closed")
        key = (task_id, run_count)
        existing = self._watches.get(key)
        if existing is not None and not existing.done():
            return
        watch = asyncio.create_task(
            self._watch(
                task_id=task_id,
                expected_run_count=run_count,
                hooks=hooks,
            ),
            name=f"channel-task-watch:{task_id}:{run_count}",
        )
        watch.add_done_callback(self._consume_watch_result)
        self._watches[key] = watch

    def is_active(self, *, task_id: str, run_count: int) -> bool:
        watch = self._watches.get((task_id, run_count))
        return watch is not None and not watch.done()

    async def wait(self) -> None:
        active = [watch for watch in self._watches.values() if not watch.done()]
        if active:
            await asyncio.gather(*active)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        watches = list(self._watches.values())
        for watch in watches:
            if not watch.done():
                watch.cancel()
        if watches:
            await asyncio.gather(*watches, return_exceptions=True)
        self._watches.clear()

    async def _watch(
        self,
        *,
        task_id: str,
        expected_run_count: int,
        hooks: TaskWatchHooks,
    ) -> None:
        key = (task_id, expected_run_count)
        terminal_sent = False
        grace_checks_remaining = self._terminal_review_grace_checks
        query_attempt = 0
        delivery_attempt = 0
        try:
            while True:
                try:
                    payload = await self._gateway_client.get_task(task_id=task_id)
                    task = gateway_task_from_payload(payload)
                    if hooks.on_observed is not None:
                        await hooks.on_observed(task)
                except Exception as exc:
                    query_attempt += 1
                    if not is_retryable_gateway_error(exc) or (
                        query_attempt > self._retry_policy.max_attempts
                    ):
                        await self._report_error(hooks, exc)
                        return
                    delay = self._retry_policy.delay(
                        query_attempt,
                        random_value=self._random_value(),
                    )
                    if hooks.on_retry is not None:
                        await hooks.on_retry(exc, query_attempt, delay)
                    await self._sleep(delay)
                    continue
                query_attempt = 0
                try:
                    if task.run_count > expected_run_count:
                        if hooks.on_superseded is not None:
                            await hooks.on_superseded(task)
                        return
                    if task.has_pending_review:
                        await hooks.on_pending_review(task)
                        return
                    if task.status in TERMINAL_TASK_STATES:
                        if not terminal_sent:
                            await hooks.on_terminal(task)
                            terminal_sent = True
                            delivery_attempt = 0
                        if grace_checks_remaining <= 0:
                            return
                        grace_checks_remaining -= 1
                    else:
                        delivery_attempt = 0
                    await self._sleep(self._poll_interval)
                except Exception as exc:
                    delivery_attempt += 1
                    if delivery_attempt > self._retry_policy.max_attempts:
                        await self._report_error(hooks, exc)
                        return
                    delay = self._retry_policy.delay(
                        delivery_attempt,
                        random_value=self._random_value(),
                    )
                    if hooks.on_retry is not None:
                        await hooks.on_retry(exc, delivery_attempt, delay)
                    await self._sleep(delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # hooks must never leak an unobserved task failure
            await self._report_error(hooks, exc)
        finally:
            if hooks.on_stopped is not None:
                try:
                    await hooks.on_stopped()
                except BaseException:
                    pass
            if self._watches.get(key) is asyncio.current_task():
                self._watches.pop(key, None)

    @staticmethod
    async def _report_error(hooks: TaskWatchHooks, exc: Exception) -> None:
        if hooks.on_error is None:
            return
        try:
            await hooks.on_error(exc)
        except Exception:
            return

    @staticmethod
    def _consume_watch_result(task: asyncio.Task[Any]) -> None:
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            return
