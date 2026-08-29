from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Literal

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


class GatewayTransportError(Exception):
    """Base error for the domain-neutral Ruyi Gateway HTTP transport."""


class GatewayTransportInvalidJSONError(GatewayTransportError):
    """A Gateway response body was not valid JSON."""


class GatewayTransportInvalidPayloadError(GatewayTransportError):
    """A Gateway JSON response was not an object."""


class GatewayTransportHTTPStatusError(GatewayTransportError):
    """A Gateway returned a non-success HTTP response with a JSON body."""

    def __init__(self, *, status_code: int, payload: Any) -> None:
        super().__init__(f"Gateway returned HTTP {status_code}")
        self.status_code = status_code
        self.payload = payload


class GatewayTransportStreamError(GatewayTransportError):
    """A Task Event Stream failed during the handshake or established read."""


class GatewayTransportInvalidStreamError(GatewayTransportError):
    """A Task Event Stream response used an invalid media/encoding contract."""

    def __init__(self, *, phase: Literal["error_response", "stream"]) -> None:
        super().__init__(f"Gateway returned an invalid Task event {phase}")
        self.phase = phase


def gateway_bearer_auth_headers(bearer_token: str | None) -> dict[str, str]:
    """Build the shared Ruyi Gateway Bearer authentication header."""

    if bearer_token is None:
        return {}
    return {"Authorization": f"Bearer {bearer_token}"}


class GatewayHTTPTransport:
    """HTTP and SSE mechanics shared by Channel and remote-ref clients."""

    def __init__(
        self,
        *,
        base_url: str,
        timeout: float,
        headers: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._base_url = base_url
        self._timeout = timeout
        self._headers = dict(headers or {})
        self._transport = transport

    async def request_json(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        json: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        response = await self._request(
            method,
            path,
            params=params,
            json=json,
            headers=self._request_headers(
                accept="application/json",
                idempotency_key=idempotency_key,
            ),
        )
        payload = _decode_json(response)
        if response.is_success:
            if not isinstance(payload, dict):
                raise GatewayTransportInvalidPayloadError
            return payload
        raise GatewayTransportHTTPStatusError(
            status_code=response.status_code,
            payload=payload,
        )

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        response = await self._request(
            method,
            path,
            json=json,
            headers=self._request_headers(accept="*/*"),
        )
        if response.is_success:
            return response
        payload = _decode_json(response)
        raise GatewayTransportHTTPStatusError(
            status_code=response.status_code,
            payload=payload,
        )

    @asynccontextmanager
    async def stream_task_events(
        self,
        path: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AsyncIterator[AsyncIterator[GatewayTaskEvent]]:
        headers = self._request_headers(accept="text/event-stream")
        headers["Accept-Encoding"] = "identity"
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
                path,
                params={"run_count": str(run_count)},
            )
            try:
                async with asyncio.timeout(self._bounded_handshake_timeout()):
                    response = await response_context.__aenter__()
            except (TimeoutError, httpx.HTTPError) as exc:
                raise GatewayTransportStreamError from exc
            try:
                await self._validate_stream_response(response)
                yield _iter_task_events(response)
            finally:
                await response_context.__aexit__(None, None, None)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str] | None = None,
        json: Mapping[str, Any] | None = None,
        headers: Mapping[str, str],
    ) -> httpx.Response:
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
            transport=self._transport,
        ) as client:
            return await client.request(method, path, params=params, json=json)

    async def _validate_stream_response(self, response: httpx.Response) -> None:
        if response.status_code != 200:
            if not has_identity_content_encoding(
                response.headers.get("content-encoding", "")
            ):
                raise GatewayTransportInvalidStreamError(phase="error_response")
            chunks = (
                response.aiter_bytes()
                if response.is_stream_consumed
                else response.aiter_raw()
            )
            try:
                body = await read_bounded_sse_error_body(
                    chunks,
                    timeout_seconds=self._bounded_error_read_timeout(),
                )
            except (httpx.HTTPError, SSEProtocolError) as exc:
                raise GatewayTransportStreamError from exc
            try:
                payload = decode_sse_error_json(body)
            except SSEProtocolError as exc:
                raise GatewayTransportInvalidJSONError from exc
            if not isinstance(payload, dict):
                raise GatewayTransportInvalidPayloadError
            raise GatewayTransportHTTPStatusError(
                status_code=response.status_code,
                payload=payload,
            )

        content_type = response.headers.get("content-type", "")
        if content_type.partition(";")[0].strip().lower() != (
            "text/event-stream"
        ) or not has_identity_content_encoding(
            response.headers.get("content-encoding", "")
        ):
            raise GatewayTransportInvalidStreamError(phase="stream")

    def _request_headers(
        self,
        *,
        accept: str,
        idempotency_key: str | None = None,
    ) -> dict[str, str]:
        headers = {**self._headers, "Accept": accept}
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key
        return headers

    def _bounded_handshake_timeout(self) -> float:
        return min(max(self._timeout, 0.001), MAX_SSE_HANDSHAKE_SECONDS)

    def _bounded_error_read_timeout(self) -> float:
        return min(max(self._timeout, 0.001), MAX_SSE_ERROR_READ_SECONDS)


def _decode_json(response: httpx.Response) -> Any:
    try:
        payload = response.json()
    except ValueError as exc:
        raise GatewayTransportInvalidJSONError from exc
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
        raise GatewayTransportStreamError from exc
