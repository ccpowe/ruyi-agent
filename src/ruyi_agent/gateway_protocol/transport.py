from __future__ import annotations
import asyncio
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any, Literal

import httpx

from ruyi_agent.gateway_protocol.contracts import (
    MAX_JSON_ERROR_BODY_BYTES,
    MAX_JSON_RESPONSE_BODY_BYTES,
    BoundedBodyLimitError,
    BoundedBodyTimeoutError,
    decode_strict_json_bytes,
    normalize_gateway_error_payload,
    read_bounded_bytes,
)
from ruyi_agent.gateway_protocol.sse import (
    GatewayTaskEvent,
    MAX_SSE_ERROR_READ_SECONDS,
    MAX_SSE_HANDSHAKE_SECONDS,
    SSEProtocolError,
    has_identity_content_encoding,
    iter_gateway_task_events,
    iter_utf8_sse_lines,
)


class GatewayTransportError(Exception):
    pass


class GatewayTransportInvalidJSONError(GatewayTransportError):
    pass


class GatewayTransportInvalidPayloadError(GatewayTransportError):
    pass


class GatewayTransportResponseTimeoutError(GatewayTransportError):
    def __init__(self, *, effect_boundary: Literal["possibly_dispatched"]) -> None:
        super().__init__("Gateway response timed out")
        self.effect_boundary = effect_boundary


class GatewayTransportHTTPStatusError(GatewayTransportError):
    def __init__(self, *, status_code: int, payload: Any) -> None:
        super().__init__(f"Gateway returned HTTP {status_code}")
        self.status_code = status_code
        self.payload = payload


class GatewayTransportStreamError(GatewayTransportError):
    def __init__(
        self,
        *,
        effect_boundary: Literal[
            "not_dispatched", "possibly_dispatched"
        ] = "possibly_dispatched",
    ) -> None:
        super().__init__("Gateway Task event stream failed")
        self.effect_boundary = effect_boundary


class GatewayTransportInvalidStreamError(GatewayTransportError):
    def __init__(self, *, phase: Literal["error_response", "stream"]) -> None:
        super().__init__(f"Gateway returned an invalid Task event {phase}")
        self.phase = phase


def gateway_bearer_auth_headers(bearer_token: str | None) -> dict[str, str]:
    if bearer_token is None:
        return {}
    return {"Authorization": f"Bearer {bearer_token}"}


class GatewayHTTPTransport:
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
        headers = self._request_headers(
            accept="application/json",
            idempotency_key=idempotency_key,
        )
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
            transport=self._transport,
        ) as client:
            async with client.stream(
                method, path, params=params, json=json
            ) as response:
                payload = await self._read_json_response(
                    response,
                    max_bytes=(
                        MAX_JSON_RESPONSE_BODY_BYTES
                        if response.is_success
                        else MAX_JSON_ERROR_BODY_BYTES
                    ),
                )
                if response.is_success:
                    if not isinstance(payload, dict):
                        raise GatewayTransportInvalidPayloadError
                    return payload
                raise GatewayTransportHTTPStatusError(
                    status_code=response.status_code,
                    payload=normalize_gateway_error_payload(payload),
                )

    async def request_raw(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        async with self.stream_raw(method, path, json=json) as response:
            await response.aread()
            return response

    @asynccontextmanager
    async def stream_raw(
        self,
        method: str,
        path: str,
        *,
        json: Mapping[str, Any] | None = None,
    ) -> AsyncIterator[httpx.Response]:
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=self._request_headers(accept="*/*"),
            transport=self._transport,
        ) as client:
            async with client.stream(method, path, json=json) as response:
                if not response.is_success:
                    await self._raise_response_status(response)
                yield response

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
            except TimeoutError as exc:
                raise GatewayTransportStreamError(
                    effect_boundary="not_dispatched"
                ) from exc
            except httpx.HTTPError as exc:
                raise GatewayTransportStreamError(
                    effect_boundary=_http_error_effect_boundary(exc)
                ) from exc
            try:
                await self._validate_stream_response(response)
                yield _iter_task_events(response)
            finally:
                await response_context.__aexit__(None, None, None)

    async def _validate_stream_response(self, response: httpx.Response) -> None:
        if response.status_code != 200:
            if not has_identity_content_encoding(
                response.headers.get("content-encoding", "")
            ):
                raise GatewayTransportInvalidStreamError(phase="error_response")
            try:
                payload = await self._read_json_response(
                    response,
                    max_bytes=MAX_JSON_ERROR_BODY_BYTES,
                )
            except GatewayTransportResponseTimeoutError:
                raise
            except (httpx.HTTPError, SSEProtocolError) as exc:
                raise GatewayTransportStreamError from exc
            if not isinstance(payload, dict):
                raise GatewayTransportInvalidPayloadError
            raise GatewayTransportHTTPStatusError(
                status_code=response.status_code,
                payload=normalize_gateway_error_payload(payload),
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

    async def _read_json_response(
        self,
        response: httpx.Response,
        *,
        max_bytes: int,
    ) -> Any:
        _validate_content_length(response, max_bytes=max_bytes)
        try:
            body = await read_bounded_bytes(
                response.aiter_bytes(),
                max_bytes=max_bytes,
                timeout_seconds=self._bounded_error_read_timeout(),
            )
        except BoundedBodyLimitError as exc:
            raise GatewayTransportInvalidPayloadError from exc
        except BoundedBodyTimeoutError as exc:
            raise GatewayTransportResponseTimeoutError(
                effect_boundary="possibly_dispatched"
            ) from exc
        try:
            return decode_strict_json_bytes(body, max_bytes=max_bytes)
        except ValueError as exc:
            raise GatewayTransportInvalidJSONError from exc

    async def _raise_response_status(self, response: httpx.Response) -> None:
        payload = await self._read_json_response(
            response,
            max_bytes=MAX_JSON_ERROR_BODY_BYTES,
        )
        raise GatewayTransportHTTPStatusError(
            status_code=response.status_code,
            payload=normalize_gateway_error_payload(payload),
        )


def _validate_content_length(response: httpx.Response, *, max_bytes: int) -> None:
    values = response.headers.get_list("content-length")
    if not values:
        return
    if len(values) != 1:
        raise GatewayTransportInvalidPayloadError
    raw = values[0].strip()
    if not raw.isascii() or not raw.isdigit():
        raise GatewayTransportInvalidPayloadError
    try:
        declared = int(raw)
    except ValueError as exc:
        raise GatewayTransportInvalidPayloadError from exc
    if declared > max_bytes:
        raise GatewayTransportInvalidPayloadError


def _sse_chunks(response: httpx.Response) -> AsyncIterator[bytes]:
    return (
        response.aiter_bytes() if response.is_stream_consumed else response.aiter_raw()
    )


async def _iter_task_events(
    response: httpx.Response,
) -> AsyncIterator[GatewayTaskEvent]:
    try:
        lines = iter_utf8_sse_lines(_sse_chunks(response))
        async for event in iter_gateway_task_events(lines):
            yield event
            if event.event_type == "stream.end":
                return
        raise SSEProtocolError("Gateway Task event stream ended without stream.end")
    except (httpx.HTTPError, SSEProtocolError) as exc:
        raise GatewayTransportStreamError from exc


def _http_error_effect_boundary(
    exc: httpx.HTTPError,
) -> Literal["not_dispatched", "possibly_dispatched"]:
    if isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.PoolTimeout,
            httpx.InvalidURL,
            httpx.LocalProtocolError,
            httpx.UnsupportedProtocol,
        ),
    ):
        return "not_dispatched"
    return "possibly_dispatched"
