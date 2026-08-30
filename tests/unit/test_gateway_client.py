from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest

from ruyi_agent.channels.gateway_client import (
    GatewayClientError,
    GatewayHTTPClient,
    _filename_from_content_disposition,
)
from ruyi_agent.gateway_protocol.dto import GatewayTask
from ruyi_agent.gateway_protocol.sse import MAX_SSE_ERROR_BODY_BYTES


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


class CloseTrackingStream(httpx.AsyncByteStream):
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.closed = False

    async def __aiter__(self):
        yield self.body

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


class FakeGatewayHTTPClient(GatewayHTTPClient):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(base_url="http://gateway.test", bearer_token="token")
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def _request_raw(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        self.requests.append({"method": method, "path": path, "json": json})
        return self.response


def test_filename_from_content_disposition_parses_quoted_and_bare_values() -> None:
    assert (
        _filename_from_content_disposition('attachment; filename="report.pdf"')
        == "report.pdf"
    )
    assert (
        _filename_from_content_disposition("attachment; filename=report.pdf")
        == "report.pdf"
    )
    assert _filename_from_content_disposition("attachment") is None


def test_download_artifact_uses_content_disposition_filename() -> None:
    client = FakeGatewayHTTPClient(
        httpx.Response(
            200,
            content=b"artifact",
            headers={
                "content-disposition": 'attachment; filename="report.html"',
                "content-type": "text/html",
            },
        )
    )

    artifact = asyncio.run(client.download_artifact(path="/workspace/out/index.html"))

    assert artifact.filename == "report.html"
    assert artifact.content_type == "text/html"
    assert artifact.content == b"artifact"
    assert client.requests == [
        {
            "method": "POST",
            "path": "/artifacts/download",
            "json": {"path": "/workspace/out/index.html"},
        }
    ]


def test_download_artifact_falls_back_to_path_name_without_header() -> None:
    client = FakeGatewayHTTPClient(httpx.Response(200, content=b"artifact"))

    artifact = asyncio.run(client.download_artifact(path="/workspace/out/index.html"))

    assert artifact.filename == "index.html"
    assert artifact.content == b"artifact"


def test_download_task_artifact_uses_task_scoped_endpoint() -> None:
    client = FakeGatewayHTTPClient(
        httpx.Response(
            200,
            content=b"artifact",
            headers={
                "content-disposition": 'attachment; filename="report.html"',
                "content-type": "text/html",
            },
        )
    )

    artifact = asyncio.run(
        client.download_task_artifact(task_id="task-1", artifact_id="art_1")
    )

    assert artifact.filename == "report.html"
    assert artifact.content_type == "text/html"
    assert artifact.content == b"artifact"
    assert client.requests == [
        {
            "method": "GET",
            "path": "/tasks/task-1/artifacts/art_1/download",
            "json": None,
        }
    ]


def test_gateway_http_client_sends_idempotency_header_for_mutations() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"task_id": "task-1", "status": "running", "run_count": 1},
        )

    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        await client.create_task(
            agent_name="main",
            content="hello",
            metadata={},
            idempotency_key="channel-event-1",
        )
        await client.send_input(
            task_id="task-1",
            content="follow up",
            idempotency_key="channel-event-2",
        )

    asyncio.run(scenario())

    assert [request.headers["idempotency-key"] for request in requests] == [
        "channel-event-1",
        "channel-event-2",
    ]


def test_gateway_http_client_preserves_task_http_contract() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"task_id": "task-1", "status": "running", "run_count": 1},
        )

    client = GatewayHTTPClient(
        base_url="http://gateway.test/",
        bearer_token="token",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        await client.create_task(
            agent_name="main",
            content="hello",
            metadata={"channel": "telegram"},
            attachments=[{"path": "report.txt"}],
            idempotency_key="create-1",
        )
        await client.send_input(
            task_id="task-1",
            content="continue",
            attachments=[{"path": "more.txt"}],
            idempotency_key="input-1",
        )
        await client.get_task(task_id="task-1")
        await client.list_task_messages(
            task_id="task-1",
            cursor="opaque-cursor",
            limit=7,
        )
        await client.submit_review_decision(
            task_id="task-1",
            review_id="review-1",
            decisions=[{"type": "approve"}],
        )

    asyncio.run(scenario())

    assert [request.method for request in requests] == [
        "POST",
        "POST",
        "GET",
        "GET",
        "POST",
    ]
    assert [request.url.path for request in requests] == [
        "/agents/main/tasks",
        "/tasks/task-1/input",
        "/tasks/task-1",
        "/tasks/task-1/messages",
        "/tasks/task-1/reviews/review-1/decision",
    ]
    assert requests[0].headers["authorization"] == "Bearer token"
    assert requests[0].headers["accept"] == "application/json"
    assert requests[0].headers["idempotency-key"] == "create-1"
    assert requests[1].headers["idempotency-key"] == "input-1"
    assert "idempotency-key" not in requests[2].headers
    assert requests[0].read() == (
        b'{"input":{"content":"hello","attachments":[{"path":"report.txt"}]},'
        b'"metadata":{"channel":"telegram"}}'
    )
    assert requests[1].read() == (
        b'{"input":{"content":"continue","attachments":[{"path":"more.txt"}]}}'
    )
    assert dict(requests[3].url.params) == {
        "cursor": "opaque-cursor",
        "limit": "7",
    }
    assert requests[4].read() == b'{"decisions":[{"type":"approve"}]}'


def test_gateway_http_client_returns_validated_task_dto() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"task_id": "task-1", "status": "running", "run_count": 2},
            )
        ),
    )

    task = asyncio.run(client.get_task(task_id="task-1"))

    assert isinstance(task, GatewayTask)
    assert task.task_id == "task-1"
    assert task.run_count == 2


@pytest.mark.parametrize(
    "path,payload",
    [
        (
            "/tasks",
            {"items": [{"task_id": "task-1", "status": "running", "run_count": "1"}]},
        ),
        (
            "/tasks",
            {
                "items": [
                    {
                        "task_id": "task-1",
                        "status": "running",
                        "run_count": 1,
                        "pending_review": {},
                    }
                ]
            },
        ),
        ("/agents", {"items": [{"name": "main"}]}),
    ],
)
def test_gateway_http_client_rejects_malformed_list_elements(
    path: str,
    payload: dict[str, Any],
) -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=payload)
        ),
    )

    with pytest.raises(GatewayClientError) as exc_info:
        if path == "/tasks":
            asyncio.run(client.list_tasks(metadata={}))
        else:
            asyncio.run(client.list_agents())

    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "gateway_error"


def test_gateway_http_client_rejects_malformed_published_artifact() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "task_id": "task-1",
                    "status": "completed",
                    "run_count": 1,
                    "artifacts": [
                        {
                            "artifact_id": "artifact-1",
                            "path": "/workspace/report.txt",
                            "name": "report.txt",
                            "content_type": "text/plain",
                            "run_count": 1,
                        }
                    ],
                },
            )
        ),
    )

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(client.get_task(task_id="task-1"))

    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "gateway_error"


def test_gateway_http_client_maps_non_json_error_to_gateway_error() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(503, content=b"temporarily unavailable")
        ),
    )

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(client.get_task(task_id="task-1"))

    assert exc_info.value.status_code == 502
    assert exc_info.value.code == "gateway_error"
    assert exc_info.value.message == "Gateway returned invalid JSON"


def test_gateway_http_client_returns_complete_message_page() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "task_id": "task-1",
                "items": [
                    {
                        "sequence": 1,
                        "message_id": "message-2",
                        "role": "assistant",
                        "content": "answer",
                        "tool_calls": [],
                    }
                ],
                "next_cursor": "next-page",
            },
        )

    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(handler),
    )

    page = asyncio.run(
        client.list_task_messages(
            task_id="task-1",
            cursor="current-page",
            limit=7,
        )
    )

    assert page["next_cursor"] == "next-page"
    assert len(page["items"]) == 1
    assert requests[0].url.path == "/tasks/task-1/messages"
    assert dict(requests[0].url.params) == {
        "cursor": "current-page",
        "limit": "7",
    }


def test_gateway_http_client_streams_task_events_and_forwards_cursor() -> None:
    requests: list[httpx.Request] = []
    body = (
        "id: opaque-cursor\n"
        "event: task.completed\n"
        'data: {"task_id":"task-1","run_count":2,'
        '"created_at":"2026-08-28T12:00:00+00:00",'
        '"status":"completed","last_result":"done","error":null,'
        '"updated_at":"2026-08-28T12:00:00+00:00",'
        '"pending_review":null,"artifacts":[]}\n\n'
        "event: stream.end\n"
        'data: {"task_id":"task-1","run_count":2,'
        '"created_at":"2026-08-28T12:00:01+00:00",'
        '"reason":"completed"}\n\n'
    ).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream; charset=utf-8"},
            content=body,
        )

    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(handler),
    )

    async def scenario():
        async with client.stream_task_events(
            task_id="task-1",
            run_count=2,
            last_event_id="previous-cursor",
        ) as events:
            return [event async for event in events]

    events = asyncio.run(scenario())
    assert [event.event_type for event in events] == [
        "task.completed",
        "stream.end",
    ]
    assert events[0].event_id == "opaque-cursor"
    assert events[0].data["last_result"] == "done"
    assert requests[0].headers["authorization"] == "Bearer token"
    assert requests[0].headers["last-event-id"] == "previous-cursor"
    assert requests[0].headers["accept"] == "text/event-stream"
    assert requests[0].headers["accept-encoding"] == "identity"
    assert dict(requests[0].url.params) == {"run_count": "2"}


def test_gateway_http_client_closes_unconsumed_task_event_stream() -> None:
    stream = CloseTrackingStream(b'event: stream.end\ndata: {"reason":"completed"}\n\n')
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                stream=stream,
            )
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            assert stream.closed is False

    asyncio.run(scenario())
    assert stream.closed is True


def test_gateway_http_client_rejects_non_sse_success_response() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"status": "completed"})
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("invalid stream must fail before yielding")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_gateway_http_client_rejects_content_encoded_sse() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={
                    "content-type": "text/event-stream",
                    "content-encoding": "gzip",
                },
                content=b"compressed bytes are not parsed",
            )
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("encoded stream must fail before yielding")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_gateway_http_client_bounds_the_stream_handshake() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        timeout=0.01,
        transport=NeverRespondingTransport(),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("a hanging handshake must not yield")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_gateway_http_client_bounds_a_hanging_error_body() -> None:
    stream = HangingErrorStream()
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        timeout=0.01,
        transport=ErrorStreamTransport(stream),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("a hanging error body must not yield")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502
    assert stream.closed is True


def test_gateway_http_client_rejects_oversized_error_body() -> None:
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                400,
                content=b"x" * (MAX_SSE_ERROR_BODY_BYTES + 1),
            )
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("an oversized error body must not yield")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_gateway_http_client_sanitizes_error_text() -> None:
    body = b'{"error":{"code":"invalid\\ud800request","message":"bad\\ud800message"}}'
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(400, content=body)
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("an error response must not yield")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.code == "invalid\ufffdrequest"
    assert exc_info.value.message == "bad\ufffdmessage"


def test_gateway_http_client_maps_deep_error_json_to_client_error() -> None:
    body = b'{"error":' + (b"[" * 10_000) + b"0" + (b"]" * 10_000) + b"}"
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(400, content=body)
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(task_id="task-1", run_count=1):
            raise AssertionError("deep JSON must not yield")

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_gateway_http_client_requires_stream_end_before_clean_eof() -> None:
    body = b'event: assistant.delta\ndata: {"content":"partial"}\n\n'
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(
            task_id="task-1",
            run_count=1,
        ) as events:
            assert (await anext(events)).event_type == "assistant.delta"
            await anext(events)

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502


def test_gateway_http_client_discards_unterminated_stream_end() -> None:
    body = b'event: stream.end\ndata: {"reason":"completed"}'
    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=body,
            )
        ),
    )

    async def scenario() -> None:
        async with client.stream_task_events(
            task_id="task-1",
            run_count=1,
        ) as events:
            await anext(events)

    with pytest.raises(GatewayClientError) as exc_info:
        asyncio.run(scenario())
    assert exc_info.value.status_code == 502
