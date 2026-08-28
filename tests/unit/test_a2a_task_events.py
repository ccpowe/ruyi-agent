from __future__ import annotations

import asyncio

import httpx
import pytest

from ruyi_agent.config.loader import RemoteRef
from ruyi_agent.gateway.sse import MAX_SSE_ERROR_BODY_BYTES
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError


class NeverRespondingTransport(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class HangingErrorStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self):
        yield b'{"error":{"code":"invalid_request"'
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


class ErrorStreamTransport(httpx.AsyncBaseTransport):
    def __init__(self, stream: httpx.AsyncByteStream) -> None:
        self.stream = stream

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(
            400,
            headers={"content-type": "application/json"},
            stream=self.stream,
        )


def _remote_ref() -> RemoteRef:
    return RemoteRef(
        name="remote",
        description="remote",
        url="https://remote.test",
        remote_agent_name="worker",
        auth=None,
    )


@pytest.mark.parametrize(
    ("status_code", "code", "expected_status", "expected_code"),
    [
        (400, "invalid_request", 400, "invalid_request"),
        (409, "task_run_mismatch", 409, "task_run_mismatch"),
        (401, "unauthorized", 502, "upstream_gateway_error"),
        (404, "task_not_found", 502, "upstream_gateway_error"),
    ],
)
def test_a2a_task_event_handshake_error_mapping(
    status_code: int,
    code: str,
    expected_status: int,
    expected_code: str,
) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            status_code,
            json={"error": {"code": code, "message": "downstream error"}},
        )
    )
    client = A2AClient(transports={"https://remote.test": transport})

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=2,
            last_event_id=None,
        ):
            raise AssertionError("error response must fail before yielding")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == expected_status
    assert exc_info.value.code == expected_code


def test_a2a_task_event_handshake_sanitizes_message_and_details() -> None:
    body = (
        b'{"error":{"code":"task_run_mismatch",'
        b'"message":"bad\\ud800message",'
        b'"details":{"requested_run_count":1,"current_run_count":2,'
        b'"private":{"token":"secret"}}}}'
    )
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(409, content=body)
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("an error response must not yield")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.message == "bad\ufffdmessage"
    assert exc_info.value.details == {
        "requested_run_count": 1,
        "current_run_count": 2,
    }


def test_a2a_task_event_stream_forwards_last_event_id_byte_for_byte() -> None:
    requests: list[httpx.Request] = []
    body = (
        'event: stream.end\n'
        'data: {"task_id":"task-1","run_count":2,'
        '"created_at":"2026-08-28T12:00:00+00:00",'
        '"reason":"completed"}\n\n'
    ).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=body,
        )

    client = A2AClient(
        transports={"https://remote.test": httpx.MockTransport(handler)}
    )

    async def scenario():
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=2,
            last_event_id="opaque.Downstream cursor-123",
        ) as events:
            return [event async for event in events]

    events = asyncio.run(scenario())
    assert events[0].event_type == "stream.end"
    assert requests[0].headers["last-event-id"] == "opaque.Downstream cursor-123"
    assert requests[0].headers["accept"] == "text/event-stream"
    assert requests[0].headers["accept-encoding"] == "identity"


def test_a2a_task_event_stream_rejects_wrong_content_type_before_yield() -> None:
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(200, json={"status": "completed"})
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("wrong content type must fail before yielding")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_a2a_task_event_stream_rejects_content_encoding_before_yield() -> None:
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    headers={
                        "content-type": "text/event-stream",
                        "content-encoding": "gzip",
                    },
                    content=b"compressed bytes are not parsed",
                )
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("encoded stream must fail before yielding")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_a2a_task_event_stream_maps_established_protocol_fault() -> None:
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    content=b"event: task.completed\ndata: not-json\n\n",
                )
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ) as events:
            await anext(events)

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "upstream_gateway_error"


def test_a2a_task_event_stream_bounds_the_handshake() -> None:
    client = A2AClient(
        timeout=0.01,
        transports={"https://remote.test": NeverRespondingTransport()},
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("a hanging handshake must not yield")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_a2a_task_event_stream_bounds_a_hanging_error_body() -> None:
    stream = HangingErrorStream()
    client = A2AClient(
        timeout=0.01,
        transports={
            "https://remote.test": ErrorStreamTransport(stream),
        },
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("a hanging error body must not yield")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502
    assert stream.closed is True


def test_a2a_task_event_stream_rejects_oversized_error_body() -> None:
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(
                    400,
                    content=b"x" * (MAX_SSE_ERROR_BODY_BYTES + 1),
                )
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("an oversized error body must not yield")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_a2a_task_event_stream_maps_deep_error_json_to_client_error() -> None:
    body = (
        b'{"error":'
        + (b"[" * 10_000)
        + b"0"
        + (b"]" * 10_000)
        + b"}"
    )
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(400, content=body)
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ):
            raise AssertionError("deep JSON must not yield")

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_a2a_task_event_stream_requires_stream_end_before_clean_eof() -> None:
    body = b'event: assistant.delta\ndata: {"content":"partial"}\n\n'
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    content=body,
                )
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ) as events:
            assert (await anext(events)).event_type == "assistant.delta"
            await anext(events)

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_a2a_task_event_stream_maps_json_resource_failure() -> None:
    body = (
        "event: assistant.delta\ndata: {\"value\":"
        + ("1" * 5000)
        + "}\n\n"
    ).encode()
    client = A2AClient(
        transports={
            "https://remote.test": httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    content=body,
                )
            )
        }
    )

    async def scenario() -> None:
        async with client.open_task_event_stream(
            _remote_ref(),
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ) as events:
            await anext(events)

    with pytest.raises(A2AClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502
