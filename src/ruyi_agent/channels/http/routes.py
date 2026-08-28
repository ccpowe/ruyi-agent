"""FastAPI Adapter for the Gateway Task Module."""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Awaitable
from urllib.parse import quote

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    Response,
    StreamingResponse,
)
from pydantic import BaseModel, Field, model_validator

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.sse import (
    encode_task_stream_event,
    is_valid_task_event_cursor,
)
from ruyi_agent.gateway.models import (
    AgentListResponse,
    AgentRefResponse,
    AttachmentInput,
    MetadataScalar,
    ReviewListResponse,
    ReviewResponse,
    TaskListResponse,
    TaskMessageListResponse,
    TaskResponse,
    TaskWebhookEvent,
)
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.runtime.task_events import TaskStreamEvent


TASK_EVENT_HEARTBEAT_SECONDS = 15.0


class _ManagedStreamingResponse(StreamingResponse):
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


class TaskInput(BaseModel):
    content: str = ""
    attachments: list[AttachmentInput] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def require_content_or_attachments(self) -> "TaskInput":
        if self.content.strip() or self.attachments:
            return self
        raise ValueError("input.content or input.attachments is required")


class CreateTaskRequest(BaseModel):
    input: TaskInput
    metadata: dict[str, MetadataScalar] = Field(default_factory=dict)
    webhook: dict[str, MetadataScalar] | None = None


class SendInputRequest(BaseModel):
    input: TaskInput


class ReviewDecisionInput(BaseModel):
    decisions: list[dict[str, Any]] = Field(min_length=1)


class ArtifactDownloadRequest(BaseModel):
    path: str = Field(min_length=1)


HTTP_STATUS_BY_ERROR = {
    "unauthorized": 401,
    "invalid_request": 400,
    "invalid_attachment": 400,
    "invalid_delegation_context": 400,
    "agent_not_public": 403,
    "delegation_depth_exceeded": 403,
    "delegation_loop_detected": 403,
    "workspace_path_forbidden": 403,
    "agent_not_found": 404,
    "artifact_not_found": 404,
    "review_not_found": 404,
    "task_not_found": 404,
    "review_task_mismatch": 409,
    "task_already_running": 409,
    "task_run_mismatch": 409,
    "idempotency_in_progress": 409,
    "idempotency_key_reused": 409,
    "attachment_too_large": 413,
    "artifact_too_large": 413,
    "remote_executor_not_implemented": 422,
    "delegation_budget_exhausted": 429,
    "upstream_gateway_error": 502,
    "agent_unavailable": 503,
    "attachment_upload_failed": 503,
    "runtime_unavailable": 503,
    "task_history_unavailable": 503,
    "task_events_unavailable": 503,
    "idempotency_unavailable": 503,
}

HTTP_STATUS_BY_ERROR_KIND = {
    "upstream_failure": 502,
}


def attach_gateway_routes(
    app: FastAPI,
    *,
    service_getter: Callable[[Request], GatewayTaskModule],
    bearer_token: str,
) -> None:
    """Attach the authenticated Gateway HTTP Interface to a FastAPI app."""

    console_root = Path(__file__).resolve().parents[2] / "web" / "team_console"

    @app.get("/debug/team", response_class=HTMLResponse, include_in_schema=False)
    async def team_console() -> HTMLResponse:
        return HTMLResponse((console_root / "index.html").read_text(encoding="utf-8"))

    @app.get("/debug/team/app.css", include_in_schema=False)
    async def team_console_css() -> FileResponse:
        return FileResponse(console_root / "app.css", media_type="text/css")

    @app.get("/debug/team/app.js", include_in_schema=False)
    async def team_console_js() -> FileResponse:
        return FileResponse(console_root / "app.js", media_type="text/javascript")

    @app.exception_handler(GatewayTaskError)
    async def handle_gateway_error(
        request: Request,
        exc: GatewayTaskError,
    ) -> JSONResponse:
        del request
        payload: dict[str, Any] = {
            "error": {
                "code": exc.code,
                "message": exc.message,
            }
        }
        if exc.details:
            payload["error"]["details"] = exc.details
        status_code = HTTP_STATUS_BY_ERROR_KIND.get(
            exc.kind,
            HTTP_STATUS_BY_ERROR.get(exc.code, 500),
        )
        headers = {"Retry-After": "1"} if exc.code == "idempotency_in_progress" else None
        return JSONResponse(status_code=status_code, content=payload, headers=headers)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        del request, exc
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "Internal gateway error",
                }
            },
        )

    async def require_bearer(
        authorization: str | None = Header(default=None),
    ) -> None:
        expected = f"Bearer {bearer_token}"
        if authorization is None or not secrets.compare_digest(authorization, expected):
            raise GatewayTaskError(
                code="unauthorized",
                message="Missing or invalid bearer token",
            )

    @app.get("/agents", response_model=AgentListResponse)
    async def list_agents(
        request: Request,
        _: None = Depends(require_bearer),
    ) -> AgentListResponse:
        service = service_getter(request)
        return AgentListResponse(items=service.list_agents())

    @app.get("/agents/{agent_name}", response_model=AgentRefResponse)
    async def get_agent(
        request: Request,
        agent_name: str,
        _: None = Depends(require_bearer),
    ) -> AgentRefResponse:
        return service_getter(request).get_agent(agent_name)

    @app.post(
        "/agents/{agent_name}/tasks",
        response_model=TaskResponse,
        status_code=201,
    )
    async def create_task(
        request: Request,
        agent_name: str,
        payload: CreateTaskRequest,
        idempotency_key: str | None = Header(
            default=None,
            alias="Idempotency-Key",
        ),
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        outcome = await service_getter(request).create_task_command(
            agent_name=agent_name,
            input_content=payload.input.content,
            attachments=payload.input.attachments,
            metadata=payload.metadata,
            webhook=payload.webhook,
            idempotency_key=idempotency_key,
        )
        task = outcome.task
        response_headers = {"Location": f"/tasks/{task.task_id}"}
        if outcome.replayed:
            response_headers["Idempotency-Replayed"] = "true"
        return JSONResponse(
            status_code=201,
            headers=response_headers,
            content=task.model_dump(mode="json"),
        )

    @app.get("/tasks/{task_id}", response_model=TaskResponse)
    async def get_task(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> TaskResponse:
        return await service_getter(request).get_task(task_id)

    @app.get(
        "/tasks/{task_id}/messages",
        response_model=TaskMessageListResponse,
    )
    async def list_task_messages(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> TaskMessageListResponse:
        limit_raw = request.query_params.get("limit")
        limit = 20 if limit_raw is None else _parse_limit(limit_raw)
        return await service_getter(request).list_task_messages(
            task_id,
            cursor=request.query_params.get("cursor"),
            limit=limit,
        )

    @app.get("/tasks/{task_id}/events")
    async def stream_task_events(
        request: Request,
        task_id: str,
        last_event_id: str | None = Header(
            default=None,
            alias="Last-Event-ID",
        ),
        _: None = Depends(require_bearer),
    ) -> StreamingResponse:
        run_count = _parse_run_count(request.query_params.get("run_count"))
        if last_event_id is not None and not is_valid_task_event_cursor(
            last_event_id
        ):
            raise GatewayTaskError(
                code="invalid_request",
                message="Header 'Last-Event-ID' is invalid",
            )

        stream_context = service_getter(request).open_task_event_stream(
            task_id,
            run_count=run_count,
            last_event_id=last_event_id,
        )
        events = await stream_context.__aenter__()

        async def body() -> AsyncIterator[bytes]:
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
                        raise RuntimeError(
                            "Task event source ended without stream.end"
                        ) from exc
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

        return _ManagedStreamingResponse(
            body(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-transform",
                "X-Accel-Buffering": "no",
            },
            close=lambda: stream_context.__aexit__(None, None, None),
        )

    @app.get("/reviews", response_model=ReviewListResponse)
    async def list_reviews(
        request: Request,
        _: None = Depends(require_bearer),
    ) -> ReviewListResponse:
        return await service_getter(request).list_reviews(
            cursor=request.query_params.get("cursor"),
            limit=int(request.query_params.get("limit", "20")),
        )

    @app.get("/reviews/{review_id}", response_model=ReviewResponse)
    async def get_review(
        request: Request,
        review_id: str,
        _: None = Depends(require_bearer),
    ) -> ReviewResponse:
        return await service_getter(request).get_review(review_id)

    @app.get("/tasks/{task_id}/reviews", response_model=ReviewListResponse)
    async def list_task_reviews(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> ReviewListResponse:
        return await service_getter(request).list_task_reviews(task_id)

    @app.post("/tasks/{task_id}/input", response_model=TaskResponse, status_code=202)
    async def send_input(
        request: Request,
        task_id: str,
        payload: SendInputRequest,
        idempotency_key: str | None = Header(
            default=None,
            alias="Idempotency-Key",
        ),
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        outcome = await service_getter(request).send_input_command(
            task_id=task_id,
            input_content=payload.input.content,
            attachments=payload.input.attachments,
            idempotency_key=idempotency_key,
        )
        headers = {"Idempotency-Replayed": "true"} if outcome.replayed else None
        return JSONResponse(
            status_code=202,
            headers=headers,
            content=outcome.task.model_dump(mode="json"),
        )

    @app.post("/artifacts/download")
    async def download_artifact(
        request: Request,
        payload: ArtifactDownloadRequest,
        _: None = Depends(require_bearer),
    ) -> Response:
        artifact = await service_getter(request).download_artifact(payload.path)
        return Response(
            content=artifact.content,
            media_type=artifact.content_type,
            headers={
                "Content-Disposition": _attachment_content_disposition(
                    artifact.filename
                ),
                "X-Artifact-Path": _header_path_value(artifact.path),
            },
        )

    @app.get("/tasks/{task_id}/artifacts/{artifact_id}/download")
    async def download_task_artifact(
        request: Request,
        task_id: str,
        artifact_id: str,
        _: None = Depends(require_bearer),
    ) -> Response:
        artifact = await service_getter(request).download_task_artifact(
            task_id=task_id,
            artifact_id=artifact_id,
        )
        return Response(
            content=artifact.content,
            media_type=artifact.content_type,
            headers={
                "Content-Disposition": _attachment_content_disposition(
                    artifact.filename
                ),
                "X-Artifact-Path": _header_path_value(artifact.path),
                "X-Artifact-Id": artifact_id,
            },
        )

    @app.post("/tasks/{task_id}/cancel", response_model=TaskResponse, status_code=202)
    async def cancel_task(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        task = await service_getter(request).cancel_task(task_id)
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))

    @app.post(
        "/tasks/{task_id}/reviews/{review_id}/decision",
        response_model=TaskResponse,
        status_code=202,
    )
    async def submit_review_decision(
        request: Request,
        task_id: str,
        review_id: str,
        payload: ReviewDecisionInput,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        task = await service_getter(request).submit_review_decision(
            task_id=task_id,
            review_id=review_id,
            decisions=payload.decisions,
        )
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))

    @app.post("/webhooks/tasks", status_code=202)
    async def receive_task_webhook(
        request: Request,
        payload: TaskWebhookEvent,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        result = await service_getter(request).handle_task_webhook(payload)
        return JSONResponse(status_code=202, content=result)

    @app.get("/tasks", response_model=TaskListResponse)
    async def list_tasks(
        request: Request,
        _: None = Depends(require_bearer),
    ) -> TaskListResponse:
        metadata_filters = {
            key.removeprefix("metadata."): value
            for key, value in request.query_params.items()
            if key.startswith("metadata.")
        }
        limit_raw = request.query_params.get("limit")
        limit = 20 if limit_raw is None else _parse_limit(limit_raw)
        return await service_getter(request).list_tasks(
            agent_name=request.query_params.get("agent_name"),
            status=request.query_params.get("status"),
            metadata_filters=metadata_filters,
            cursor=request.query_params.get("cursor"),
            limit=limit,
            root_task_id=request.query_params.get("root_task_id"),
        )


def create_gateway_app(*, service: GatewayTaskModule, bearer_token: str) -> FastAPI:
    app = FastAPI(title="ruyi-agent Gateway")
    attach_gateway_routes(
        app,
        service_getter=lambda request: service,
        bearer_token=bearer_token,
    )
    return app


def _attachment_content_disposition(filename: str) -> str:
    safe_filename = _header_filename(filename)
    if safe_filename.isascii():
        return f'attachment; filename="{_quote_header_filename(safe_filename)}"'
    fallback = _ascii_filename_fallback(safe_filename)
    encoded = quote(safe_filename, safe="")
    return (
        f'attachment; filename="{_quote_header_filename(fallback)}"; '
        f"filename*=UTF-8''{encoded}"
    )


def _header_filename(filename: str) -> str:
    cleaned = PurePosixPath(filename.replace("\\", "/")).name.strip()
    cleaned = cleaned.replace("\r", "").replace("\n", "")
    return cleaned or "artifact"


def _quote_header_filename(filename: str) -> str:
    return filename.replace("\\", "\\\\").replace('"', '\\"')


def _ascii_filename_fallback(filename: str) -> str:
    suffix = PurePosixPath(filename).suffix
    if not suffix.isascii():
        suffix = ""
    return f"artifact{suffix}" if suffix else "artifact"


def _header_path_value(path: str) -> str:
    return quote(path, safe="/-._~")


def _parse_limit(raw: str) -> int:
    try:
        return int(raw)
    except ValueError as exc:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'limit' must be an integer",
        ) from exc


def _parse_run_count(raw: str | None) -> int:
    if raw is None:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'run_count' is required",
        )
    try:
        value = int(raw)
    except ValueError as exc:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'run_count' must be a non-negative integer",
        ) from exc
    if value < 0:
        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'run_count' must be a non-negative integer",
        )
    return value


def _stream_fault_event(*, task_id: str, run_count: int) -> TaskStreamEvent:
    return TaskStreamEvent(
        event_type="stream.error",
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
        task_id=task_id,
        run_count=run_count,
        created_at=datetime.now(UTC),
        data={"reason": "error"},
    )
