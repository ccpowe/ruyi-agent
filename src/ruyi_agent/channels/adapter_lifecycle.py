from __future__ import annotations

import asyncio
import threading
from collections.abc import Awaitable, Callable


Startup = Callable[[], Awaitable[int]]
Shutdown = Callable[[], Awaitable[None]]


class ChannelAdapterLifecycle:
    """Single-flight startup and close-wins lifecycle for Channel adapters."""

    def __init__(self, *, adapter_name: str) -> None:
        self._adapter_name = adapter_name
        self._lock = threading.Lock()
        self._startup: asyncio.Task[int] | None = None
        self._shutdown: asyncio.Task[None] | None = None
        self._shutdown_complete = False
        self._started = False
        self._closed = False

    @property
    def started(self) -> bool:
        with self._lock:
            return self._started

    @property
    def closed(self) -> bool:
        with self._lock:
            return self._closed

    async def start(self, startup: Startup) -> int:
        with self._lock:
            if self._closed:
                raise RuntimeError(f"{self._adapter_name} is closed")
            if self._started:
                return 0
            in_flight = self._startup
            if in_flight is None:
                in_flight = asyncio.create_task(self._run_startup(startup))
                in_flight.add_done_callback(self._consume_task_result)
                self._startup = in_flight
        try:
            return await asyncio.shield(in_flight)
        except asyncio.CancelledError:
            if self.closed:
                raise RuntimeError(
                    f"{self._adapter_name} closed during startup"
                ) from None
            raise

    async def close(self, shutdown: Shutdown) -> None:
        with self._lock:
            if self._shutdown_complete:
                return
            in_flight = self._shutdown
            if in_flight is None:
                self._closed = True
                in_flight = asyncio.create_task(self._run_shutdown(shutdown))
                in_flight.add_done_callback(self._consume_task_result)
                self._shutdown = in_flight
        await asyncio.shield(in_flight)

    async def _run_startup(self, startup: Startup) -> int:
        current = asyncio.current_task()
        try:
            recovered = await startup()
            with self._lock:
                if not self._closed:
                    self._started = True
            return recovered
        finally:
            with self._lock:
                if self._startup is current:
                    self._startup = None

    async def _run_shutdown(self, shutdown: Shutdown) -> None:
        current = asyncio.current_task()
        with self._lock:
            startup = self._startup
        if startup is not None and not startup.done():
            startup.cancel()
        if startup is not None:
            await asyncio.gather(startup, return_exceptions=True)
        try:
            await shutdown()
        finally:
            with self._lock:
                self._started = False
                self._shutdown_complete = True
                if self._shutdown is current:
                    self._shutdown = None

    @staticmethod
    def _consume_task_result(task: asyncio.Task) -> None:
        try:
            task.result()
        except (asyncio.CancelledError, Exception):
            return
