"""Gateway Task SSE transport route and ASGI lifetime management."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import StreamingResponse

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway_protocol.sse import encode_task_stream_event, is_valid_task_event_cursor
from ruyi_agent.gateway_protocol.contracts import TaskStreamEvent

from .context import GatewayHttpContext

TASK_EVENT_HEARTBEAT_SECONDS = 15.0


class ManagedStreamingResponse(StreamingResponse):
    """Close an already-open stream context on every ASGI exit path."""

    def __init__(
        self,
        content: AsyncIterator[bytes],
        *,
        close: Callable[[], Awaitable[Any]],
        media_type: str,
        headers: dict[str, str],
    ) -> None:
        super().__init__(content, media_type=media_type, headers=headers)
        self._close_stream_context = close
        self._close_lock = asyncio.Lock()
        self._closed = False

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._close_once()

    async def _close_once(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            try:
                close_iterator = getattr(self.body_iterator, "aclose", None)
                if close_iterator is not None:
                    await close_iterator()
            finally:
                await self._close_stream_context()


def attach_event_routes(app: FastAPI, context: GatewayHttpContext) -> None:
    @app.get("/tasks/{task_id}/events")
    async def stream_task_events(
        request: Request,
        task_id: str,
        last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
        _: None = Depends(context.require_bearer),
    ) -> StreamingResponse:
        run_count = parse_run_count(request.query_params.get("run_count"))
        if last_event_id is not None and not is_valid_task_event_cursor(last_event_id):
            raise GatewayTaskError(
                code="invalid_request",
                message="Header 'Last-Event-ID' is invalid",
            )
        stream_context = context.service(request).open_task_event_stream(
            task_id,
            run_count=run_count,
            last_event_id=last_event_id,
        )
        events = await stream_context.__aenter__()
        body = _stream_body(events, task_id=task_id, run_count=run_count)
        return ManagedStreamingResponse(
            body,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
            close=lambda: stream_context.__aexit__(None, None, None),
        )


async def _stream_body(
    events: AsyncIterator[TaskStreamEvent],
    *,
    task_id: str,
    run_count: int,
) -> AsyncIterator[bytes]:
    next_event: asyncio.Task[TaskStreamEvent] | None = None
    try:
        while True:
            if next_event is None:
                next_event = asyncio.create_task(anext(events))
            done, _ = await asyncio.wait(
                {next_event},
                timeout=TASK_EVENT_HEARTBEAT_SECONDS,
            )
            if not done:
                yield b": heartbeat\n\n"
                continue
            try:
                event = next_event.result()
            except StopAsyncIteration as exc:
                raise RuntimeError("Task event source ended without stream.end") from exc
            finally:
                next_event = None
            yield encode_task_stream_event(event)
            if event.event_type == "stream.end":
                return
    except asyncio.CancelledError:
        raise
    except Exception:
        yield encode_task_stream_event(
            _stream_fault_event(task_id=task_id, run_count=run_count)
        )
        yield encode_task_stream_event(
            _stream_end_error_event(task_id=task_id, run_count=run_count)
        )
    finally:
        if next_event is not None and not next_event.done():
            next_event.cancel()
            with suppress(asyncio.CancelledError):
                await next_event


def parse_run_count(raw: str | None) -> int:
    if raw is None:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'run_count' is required",
        )
    try:
        run_count = int(raw)
    except ValueError as exc:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'run_count' must be a non-negative integer",
        ) from exc
    if run_count < 0:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'run_count' must be a non-negative integer",
        )
    return run_count


def _stream_fault_event(*, task_id: str, run_count: int) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type="stream.error",
        event_id=None,
        task_id=task_id,
        run_count=run_count,
        created_at=datetime.now(UTC),
        data={
            "code": "task_stream_error",
            "message": "Task event stream ended unexpectedly",
        },
    )


def _stream_end_error_event(*, task_id: str, run_count: int) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type="stream.end",
        event_id=None,
        task_id=task_id,
        run_count=run_count,
        created_at=datetime.now(UTC),
        data={"reason": "error"},
    )
