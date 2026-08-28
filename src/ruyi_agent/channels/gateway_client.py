from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx

from ruyi_agent.gateway.sse import (
    GatewayTaskEvent,
    MAX_SSE_ERROR_READ_SECONDS,
    MAX_SSE_HANDSHAKE_SECONDS,
    SSEProtocolError,
    decode_sse_error_json,
    has_identity_content_encoding,
    iter_gateway_task_events,
    iter_utf8_sse_lines,
    read_bounded_sse_error_body,
)
from ruyi_agent.runtime.task_events import (
    MAX_SHORT_EVENT_TEXT_LENGTH,
    normalize_task_event_text,
)


@dataclass(slots=True)
class GatewayArtifact:
    kind: str
    filename: str
    content_type: str | None
    content: bytes


class GatewayClientError(Exception):
    def __init__(self, *, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class GatewayTaskClient(Protocol):
    async def list_agents(self) -> list[dict[str, Any]]: ...

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
        root_task_id: str | None = None,
    ) -> list[dict[str, Any]]: ...

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]: ...

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]: ...

    async def download_artifact(self, *, path: str) -> GatewayArtifact: ...

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact: ...

    async def get_task(self, *, task_id: str) -> dict[str, Any]: ...

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]: ...


def _filename_from_content_disposition(value: str) -> str | None:
    match = re.search(r'filename="([^"]+)"', value)
    if match:
        return match.group(1)
    match = re.search(r"filename=([^;]+)", value)
    if match:
        return match.group(1).strip()
    return None


class GatewayHTTPClient:
    def __init__(
        self,
        *,
        base_url: str,
        bearer_token: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._timeout = timeout
        self._transport = transport

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
        root_task_id: str | None = None,
    ) -> list[dict[str, Any]]:
        params = {
            "limit": str(limit),
            **{f"metadata.{key}": value for key, value in metadata.items()},
        }
        if agent_name is not None:
            params["agent_name"] = agent_name
        if root_task_id is not None:
            params["root_task_id"] = root_task_id
        payload = await self._request("GET", "/tasks", params=params)
        items = payload.get("items")
        return items if isinstance(items, list) else []

    async def list_agents(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/agents")
        items = payload.get("items")
        return items if isinstance(items, list) else []

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        input_payload: dict[str, Any] = {"content": content}
        if attachments:
            input_payload["attachments"] = attachments
        return await self._request(
            "POST",
            f"/agents/{agent_name}/tasks",
            json={"input": input_payload, "metadata": metadata},
            idempotency_key=idempotency_key,
        )

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        input_payload: dict[str, Any] = {"content": content}
        if attachments:
            input_payload["attachments"] = attachments
        return await self._request(
            "POST",
            f"/tasks/{task_id}/input",
            json={"input": input_payload},
            idempotency_key=idempotency_key,
        )

    async def download_artifact(self, *, path: str) -> GatewayArtifact:
        response = await self._request_raw(
            "POST",
            "/artifacts/download",
            json={"path": path},
        )
        content_disposition = response.headers.get("content-disposition", "")
        filename = _filename_from_content_disposition(content_disposition) or Path(path).name
        return GatewayArtifact(
            kind="file",
            filename=filename or "artifact",
            content_type=response.headers.get("content-type"),
            content=response.content,
        )

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact:
        response = await self._request_raw(
            "GET",
            f"/tasks/{task_id}/artifacts/{artifact_id}/download",
        )
        content_disposition = response.headers.get("content-disposition", "")
        filename = _filename_from_content_disposition(content_disposition) or artifact_id
        return GatewayArtifact(
            kind="file",
            filename=filename or "artifact",
            content_type=response.headers.get("content-type"),
            content=response.content,
        )

    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/tasks/{task_id}")

    async def list_task_messages(
        self,
        *,
        task_id: str,
        cursor: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        params = {"limit": str(limit)}
        if cursor is not None:
            params["cursor"] = cursor
        return await self._request(
            "GET",
            f"/tasks/{task_id}/messages",
            params=params,
        )

    @asynccontextmanager
    async def stream_task_events(
        self,
        *,
        task_id: str,
        run_count: int,
        last_event_id: str | None = None,
    ) -> AsyncIterator[AsyncIterator[GatewayTaskEvent]]:
        """Open an opt-in Task SSE stream without expanding adapter protocols."""

        headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Accept": "text/event-stream",
            "Accept-Encoding": "identity",
        }
        if last_event_id is not None:
            headers["Last-Event-ID"] = last_event_id
        timeout = httpx.Timeout(self._timeout, read=None)
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=timeout,
            headers=headers,
            transport=self._transport,
        ) as client:
            response_context = client.stream(
                "GET",
                f"/tasks/{task_id}/events",
                params={"run_count": str(run_count)},
            )
            try:
                async with asyncio.timeout(
                    min(
                        max(self._timeout, 0.001),
                        MAX_SSE_HANDSHAKE_SECONDS,
                    )
                ):
                    response = await response_context.__aenter__()
            except (TimeoutError, httpx.HTTPError) as exc:
                raise GatewayClientError(
                    status_code=502,
                    code="gateway_error",
                    message="Gateway Task event stream failed",
                ) from exc
            try:
                try:
                    if response.status_code != 200:
                        if not has_identity_content_encoding(
                            response.headers.get("content-encoding", "")
                        ):
                            raise GatewayClientError(
                                status_code=502,
                                code="gateway_error",
                                message="Gateway returned an invalid Task event error",
                            )
                        chunks = (
                            response.aiter_bytes()
                            if response.is_stream_consumed
                            else response.aiter_raw()
                        )
                        body = await read_bounded_sse_error_body(
                            chunks,
                            timeout_seconds=min(
                                max(self._timeout, 0.001),
                                MAX_SSE_ERROR_READ_SECONDS,
                            ),
                        )
                        try:
                            payload = decode_sse_error_json(body)
                        except SSEProtocolError as exc:
                            raise GatewayClientError(
                                status_code=502,
                                code="gateway_error",
                                message="Gateway returned invalid JSON",
                            ) from exc
                        if not isinstance(payload, dict):
                            raise GatewayClientError(
                                status_code=502,
                                code="gateway_error",
                                message="Gateway returned invalid payload",
                            )
                        error = payload.get("error")
                        if isinstance(error, dict):
                            raw_code = error.get("code")
                            raw_message = error.get("message")
                            raise GatewayClientError(
                                status_code=response.status_code,
                                code=(
                                    normalize_task_event_text(raw_code)[
                                        :MAX_SHORT_EVENT_TEXT_LENGTH
                                    ]
                                    if isinstance(raw_code, str)
                                    else "gateway_error"
                                ),
                                message=(
                                    normalize_task_event_text(raw_message)[
                                        :MAX_SHORT_EVENT_TEXT_LENGTH
                                    ]
                                    if isinstance(raw_message, str)
                                    else "Gateway request failed"
                                ),
                            )
                        raise GatewayClientError(
                            status_code=response.status_code,
                            code="gateway_error",
                            message="Gateway request failed",
                        )
                    content_type = response.headers.get("content-type", "")
                    if content_type.partition(";")[0].strip().lower() != (
                        "text/event-stream"
                    ) or not has_identity_content_encoding(
                        response.headers.get("content-encoding", "")
                    ):
                        raise GatewayClientError(
                            status_code=502,
                            code="gateway_error",
                            message="Gateway returned an invalid Task event stream",
                        )
                except GatewayClientError:
                    raise
                except (httpx.HTTPError, SSEProtocolError) as exc:
                    raise GatewayClientError(
                        status_code=502,
                        code="gateway_error",
                        message="Gateway Task event stream failed",
                    ) from exc
                yield _iter_task_events(response)
            finally:
                await response_context.__aexit__(None, None, None)

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/tasks/{task_id}/reviews/{review_id}/decision",
            json={"decisions": decisions},
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Accept": "application/json",
        }
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
            transport=self._transport,
        ) as client:
            response = await client.request(method, path, params=params, json=json)
        payload = self._decode_json(response)
        if response.is_success:
            return payload
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            raise GatewayClientError(
                status_code=response.status_code,
                code=str(error.get("code", "gateway_error")),
                message=str(error.get("message", "Gateway request failed")),
            )
        raise GatewayClientError(
            status_code=response.status_code,
            code="gateway_error",
            message="Gateway request failed",
        )

    async def _request_raw(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Accept": "*/*",
        }
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
            transport=self._transport,
        ) as client:
            response = await client.request(method, path, json=json)
        if response.is_success:
            return response
        payload = self._decode_json(response)
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            raise GatewayClientError(
                status_code=response.status_code,
                code=str(error.get("code", "gateway_error")),
                message=str(error.get("message", "Gateway request failed")),
            )
        raise GatewayClientError(
            status_code=response.status_code,
            code="gateway_error",
            message="Gateway request failed",
        )

    def _decode_json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid payload",
            )
        return payload


async def _iter_task_events(
    response: httpx.Response,
) -> AsyncIterator[GatewayTaskEvent]:
    try:
        chunks = (
            response.aiter_bytes()
            if response.is_stream_consumed
            else response.aiter_raw()
        )
        lines = iter_utf8_sse_lines(chunks)
        async for event in iter_gateway_task_events(lines):
            yield event
            if event.event_type == "stream.end":
                return
        raise SSEProtocolError("Gateway Task event stream ended without stream.end")
    except (httpx.HTTPError, SSEProtocolError) as exc:
        raise GatewayClientError(
            status_code=502,
            code="gateway_error",
            message="Gateway Task event stream failed",
        ) from exc
