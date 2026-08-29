from __future__ import annotations

import json as json_module
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx
from pydantic import ValidationError

from ruyi_agent.channels.gateway_dto import GatewayAgent, GatewayTask
from ruyi_agent.channels.media import (
    MediaLimitError,
    read_bounded_media,
    validate_content_length,
)
from ruyi_agent.gateway._http_transport import (
    GatewayHTTPTransport,
    GatewayTransportHTTPStatusError,
    GatewayTransportInvalidJSONError,
    GatewayTransportInvalidPayloadError,
    GatewayTransportInvalidStreamError,
    GatewayTransportStreamError,
    gateway_bearer_auth_headers,
)
from ruyi_agent.gateway.sse import (
    GatewayTaskEvent,
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
    async def list_agents(self) -> list[GatewayAgent]: ...

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
        root_task_id: str | None = None,
    ) -> list[GatewayTask]: ...

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> GatewayTask: ...

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> GatewayTask: ...

    async def download_artifact(self, *, path: str) -> GatewayArtifact: ...

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact: ...

    async def get_task(self, *, task_id: str) -> GatewayTask: ...

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> GatewayTask: ...


def gateway_task_from_payload(payload: Any) -> GatewayTask:
    if isinstance(payload, GatewayTask):
        return payload
    try:
        return GatewayTask.model_validate(payload)
    except ValidationError as exc:
        raise _invalid_gateway_payload("Task") from exc


def gateway_agent_from_payload(payload: Any) -> GatewayAgent:
    if isinstance(payload, GatewayAgent):
        return payload
    try:
        return GatewayAgent.model_validate(payload)
    except ValidationError as exc:
        raise _invalid_gateway_payload("Agent") from exc


def _invalid_gateway_payload(kind: str) -> GatewayClientError:
    return GatewayClientError(
        status_code=502,
        code="gateway_error",
        message=f"Gateway returned invalid {kind} payload",
    )


def _gateway_list_items(payload: dict[str, Any], *, kind: str) -> list[Any]:
    items = payload.get("items")
    if not isinstance(items, list):
        raise _invalid_gateway_payload(f"{kind} list")
    return items


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
        max_download_bytes: int | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._timeout = timeout
        self._transport = transport
        if max_download_bytes is not None and max_download_bytes <= 0:
            raise ValueError("max_download_bytes must be positive")
        self._max_download_bytes = max_download_bytes
        self._http = GatewayHTTPTransport(
            base_url=self._base_url,
            timeout=timeout,
            headers=gateway_bearer_auth_headers(bearer_token),
            transport=transport,
        )

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
        root_task_id: str | None = None,
    ) -> list[GatewayTask]:
        params = {
            "limit": str(limit),
            **{f"metadata.{key}": value for key, value in metadata.items()},
        }
        if agent_name is not None:
            params["agent_name"] = agent_name
        if root_task_id is not None:
            params["root_task_id"] = root_task_id
        payload = await self._request("GET", "/tasks", params=params)
        return [
            gateway_task_from_payload(item)
            for item in _gateway_list_items(payload, kind="Task")
        ]

    async def list_agents(self) -> list[GatewayAgent]:
        payload = await self._request("GET", "/agents")
        return [
            gateway_agent_from_payload(item)
            for item in _gateway_list_items(payload, kind="Agent")
        ]

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> GatewayTask:
        input_payload: dict[str, Any] = {"content": content}
        if attachments:
            input_payload["attachments"] = attachments
        return gateway_task_from_payload(
            await self._request(
                "POST",
                f"/agents/{agent_name}/tasks",
                json={"input": input_payload, "metadata": metadata},
                idempotency_key=idempotency_key,
            )
        )

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> GatewayTask:
        input_payload: dict[str, Any] = {"content": content}
        if attachments:
            input_payload["attachments"] = attachments
        return gateway_task_from_payload(
            await self._request(
                "POST",
                f"/tasks/{task_id}/input",
                json={"input": input_payload},
                idempotency_key=idempotency_key,
            )
        )

    async def download_artifact(self, *, path: str) -> GatewayArtifact:
        headers, content = await self._download_raw(
            "POST",
            "/artifacts/download",
            json={"path": path},
        )
        content_disposition = headers.get("content-disposition", "")
        filename = (
            _filename_from_content_disposition(content_disposition) or Path(path).name
        )
        return GatewayArtifact(
            kind="file",
            filename=filename or "artifact",
            content_type=headers.get("content-type"),
            content=content,
        )

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact:
        headers, content = await self._download_raw(
            "GET",
            f"/tasks/{task_id}/artifacts/{artifact_id}/download",
        )
        content_disposition = headers.get("content-disposition", "")
        filename = (
            _filename_from_content_disposition(content_disposition) or artifact_id
        )
        return GatewayArtifact(
            kind="file",
            filename=filename or "artifact",
            content_type=headers.get("content-type"),
            content=content,
        )

    async def get_task(self, *, task_id: str) -> GatewayTask:
        return gateway_task_from_payload(
            await self._request("GET", f"/tasks/{task_id}")
        )

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

        try:
            async with self._http.stream_task_events(
                f"/tasks/{task_id}/events",
                run_count=run_count,
                last_event_id=last_event_id,
            ) as events:
                yield _map_task_event_errors(events)
        except GatewayTransportHTTPStatusError as exc:
            raise _gateway_http_status_error(exc, sanitize=True) from exc
        except GatewayTransportInvalidJSONError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid JSON",
            ) from exc
        except GatewayTransportInvalidPayloadError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid payload",
            ) from exc
        except GatewayTransportInvalidStreamError as exc:
            label = "error" if exc.phase == "error_response" else "stream"
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message=f"Gateway returned an invalid Task event {label}",
            ) from exc
        except GatewayTransportStreamError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway Task event stream failed",
            ) from exc

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> GatewayTask:
        return gateway_task_from_payload(
            await self._request(
                "POST",
                f"/tasks/{task_id}/reviews/{review_id}/decision",
                json={"decisions": decisions},
            )
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
        try:
            return await self._http.request_json(
                method,
                path,
                params=params,
                json=json,
                idempotency_key=idempotency_key,
            )
        except GatewayTransportHTTPStatusError as exc:
            raise _gateway_http_status_error(exc) from exc
        except GatewayTransportInvalidJSONError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid JSON",
            ) from exc
        except GatewayTransportInvalidPayloadError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid payload",
            ) from exc

    async def _request_raw(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        try:
            return await self._http.request_raw(method, path, json=json)
        except GatewayTransportHTTPStatusError as exc:
            raise _gateway_http_status_error(exc) from exc
        except GatewayTransportInvalidJSONError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid JSON",
            ) from exc
        except GatewayTransportInvalidPayloadError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid payload",
            ) from exc

    async def _download_raw(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> tuple[httpx.Headers, bytes]:
        if self._max_download_bytes is None:
            response = await self._request_raw(method, path, json=json)
            return response.headers, response.content
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers={
                **gateway_bearer_auth_headers(self._bearer_token),
                "Accept": "*/*",
            },
            transport=self._transport,
        ) as client:
            async with client.stream(method, path, json=json) as response:
                if not response.is_success:
                    try:
                        error_body = await read_bounded_media(
                            response.aiter_bytes(),
                            max_bytes=min(self._max_download_bytes, 64 * 1024),
                        )
                        payload = json_module.loads(error_body)
                    except MediaLimitError:
                        raise
                    except (UnicodeDecodeError, ValueError) as exc:
                        raise GatewayClientError(
                            status_code=502,
                            code="gateway_error",
                            message="Gateway returned invalid JSON",
                        ) from exc
                    raise _gateway_http_status_error(
                        GatewayTransportHTTPStatusError(
                            status_code=response.status_code,
                            payload=payload,
                        )
                    )
                validate_content_length(
                    response.headers,
                    max_bytes=self._max_download_bytes,
                    values=response.headers.get_list("content-length"),
                )
                content = await read_bounded_media(
                    response.aiter_bytes(),
                    max_bytes=self._max_download_bytes,
                )
                return response.headers, content


async def _map_task_event_errors(
    events: AsyncIterator[GatewayTaskEvent],
) -> AsyncIterator[GatewayTaskEvent]:
    try:
        async for event in events:
            yield event
    except GatewayTransportStreamError as exc:
        raise GatewayClientError(
            status_code=502,
            code="gateway_error",
            message="Gateway Task event stream failed",
        ) from exc


def _gateway_http_status_error(
    exc: GatewayTransportHTTPStatusError,
    *,
    sanitize: bool = False,
) -> GatewayClientError:
    if not isinstance(exc.payload, dict):
        return GatewayClientError(
            status_code=502,
            code="gateway_error",
            message="Gateway returned invalid payload",
        )
    error = exc.payload.get("error")
    if isinstance(error, dict):
        raw_code = error.get("code")
        raw_message = error.get("message")
        if sanitize:
            code = (
                normalize_task_event_text(raw_code)[:MAX_SHORT_EVENT_TEXT_LENGTH]
                if isinstance(raw_code, str)
                else "gateway_error"
            )
            message = (
                normalize_task_event_text(raw_message)[:MAX_SHORT_EVENT_TEXT_LENGTH]
                if isinstance(raw_message, str)
                else "Gateway request failed"
            )
        else:
            code = str(error.get("code", "gateway_error"))
            message = str(error.get("message", "Gateway request failed"))
        return GatewayClientError(
            status_code=exc.status_code,
            code=code,
            message=message,
        )
    return GatewayClientError(
        status_code=exc.status_code,
        code="gateway_error",
        message="Gateway request failed",
    )
