from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import pytest

from ruyi_agent.config.loader import RemoteRef
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError


class HangingSuccessStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.started, self.closed = asyncio.Event(), False

    async def __aiter__(self):
        self.started.set()
        yield b'event: assistant.delta\ndata: {"content":"partial"}\n\n'
        await asyncio.Event().wait()

    async def aclose(self) -> None:
        self.closed = True


def _remote_ref(**kwargs: Any) -> RemoteRef:
    return RemoteRef(
        name="remote",
        description="remote",
        url="https://remote.test/a2a",
        remote_agent_name="worker",
        **kwargs,
    )


def test_a2a_client_preserves_task_http_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"task_id": "task-1"})

    monkeypatch.setenv("REMOTE_GATEWAY_TOKEN", "remote-token")
    remote_ref = _remote_ref(
        auth={"type": "bearer", "token_env": "REMOTE_GATEWAY_TOKEN"}
    )
    client = A2AClient(transports={remote_ref.url: httpx.MockTransport(handler)})

    async def scenario() -> None:
        await client.create_task(
            remote_ref,
            input_content="hello",
            metadata={"origin": "node-a"},
            attachments=[{"path": "report.txt"}],
            webhook={"url": "https://node-a.test/hooks/task"},
            idempotency_key="create-1",
        )
        await client.send_input(
            remote_ref,
            task_id="task-1",
            input_content="continue",
            attachments=[{"path": "more.txt"}],
            idempotency_key="input-1",
        )
        await client.get_task(remote_ref, task_id="task-1")
        await client.list_task_messages(
            remote_ref,
            task_id="task-1",
            cursor="opaque-cursor",
            limit=7,
        )
        await client.submit_review_decision(
            remote_ref,
            task_id="task-1",
            review_id="review-1",
            decisions=[{"type": "approve"}],
        )
        await client.cancel_task(remote_ref, task_id="task-1")

    asyncio.run(scenario())

    assert [request.method for request in requests] == [
        "POST",
        "POST",
        "GET",
        "GET",
        "POST",
        "POST",
    ]
    assert [request.url.path for request in requests] == [
        "/a2a/agents/worker/tasks",
        "/a2a/tasks/task-1/input",
        "/a2a/tasks/task-1",
        "/a2a/tasks/task-1/messages",
        "/a2a/tasks/task-1/reviews/review-1/decision",
        "/a2a/tasks/task-1/cancel",
    ]
    assert requests[0].headers["authorization"] == "Bearer remote-token"
    assert requests[0].headers["accept"] == "application/json"
    assert requests[0].headers["idempotency-key"] == "create-1"
    assert requests[1].headers["idempotency-key"] == "input-1"
    assert "idempotency-key" not in requests[2].headers
    assert requests[0].read() == (
        b'{"input":{"content":"hello","attachments":[{"path":"report.txt"}]},'
        b'"metadata":{"origin":"node-a"},'
        b'"webhook":{"url":"https://node-a.test/hooks/task"}}'
    )
    assert requests[1].read() == (
        b'{"input":{"content":"continue","attachments":[{"path":"more.txt"}]}}'
    )
    assert dict(requests[3].url.params) == {
        "cursor": "opaque-cursor",
        "limit": "7",
    }
    assert requests[4].read() == b'{"decisions":[{"type":"approve"}]}'


def test_declared_create_idempotency_requires_and_forwards_a_key() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(201, json={"task_id": "task-1"})

    remote_ref = _remote_ref(create_idempotency="ruyi_gateway_v1")
    client = A2AClient(transports={remote_ref.url: httpx.MockTransport(handler)})

    async def scenario() -> None:
        with pytest.raises(ValueError, match="requires an Idempotency-Key"):
            await client.create_task(
                remote_ref,
                input_content="unsafe",
                metadata={},
            )
        await client.create_task(
            remote_ref,
            input_content="safe",
            metadata={},
            idempotency_key="gateway-create-1",
        )

    asyncio.run(scenario())

    assert len(requests) == 1
    assert requests[0].headers["idempotency-key"] == "gateway-create-1"


def test_a2a_client_preserves_error_mapping() -> None:
    malicious = json.dumps(
        {
            "error": {
                "code": "\ud800" + "c" * 5000,
                "message": "\ud800" + "m" * 5000,
                "details": {"ok": 1, "secret": "drop"},
            },
            "unexpected": "drop",
        }
    ).encode()
    responses = iter(
        [
            httpx.Response(
                409,
                json={
                    "error": {
                        "code": "task_conflict",
                        "message": "Task is busy",
                        "details": {"task_id": "task-1"},
                    }
                },
            ),
            httpx.Response(503, json={"unexpected": True}),
            httpx.Response(503, json=["unexpected"]),
            httpx.Response(503, content=b"temporarily unavailable"),
            httpx.Response(400, content=malicious),
        ]
    )
    remote_ref = _remote_ref()
    client = A2AClient(
        transports={
            remote_ref.url: httpx.MockTransport(lambda request: next(responses))
        }
    )

    async def scenario() -> list[A2AClientError]:
        errors: list[A2AClientError] = []
        for _ in range(5):
            try:
                await client.get_task(remote_ref, task_id="task-1")
            except A2AClientError as exc:
                errors.append(exc)
        return errors

    errors = asyncio.run(scenario())
    assert [error.status_code for error in errors] == [409, 502, 502, 502, 400]
    assert [error.code for error in errors] == [
        "task_conflict",
        "upstream_gateway_error",
        "upstream_gateway_error",
        "upstream_gateway_error",
        "\ufffd" + "c" * 4095,
    ]
    assert errors[0].details == {"task_id": "task-1"}
    assert (
        errors[1].message
        == errors[2].message
        == "Remote gateway request failed for 'remote'"
    )
    assert errors[3].message == "Remote gateway for 'remote' returned invalid JSON"
    assert len(errors[4].code) == len(errors[4].message) == 4096
    assert errors[4].code[0] == errors[4].message[0] == "\ufffd"
    assert errors[4].details == {"ok": 1, "secret": "drop"}


def test_a2a_client_marks_missing_credentials_before_transport_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []
    remote_ref = _remote_ref(
        auth={"type": "bearer", "token_env": "MISSING_REMOTE_TOKEN"}
    )
    monkeypatch.delenv("MISSING_REMOTE_TOKEN", raising=False)
    client = A2AClient(
        transports={
            remote_ref.url: httpx.MockTransport(
                lambda request: requests.append(request) or httpx.Response(200)
            )
        }
    )

    async def scenario() -> A2AClientError:
        with pytest.raises(A2AClientError) as raised:
            await client.get_task(remote_ref, task_id="task-1")
        return raised.value

    error = asyncio.run(scenario())
    assert error.effect_boundary == "not_dispatched" and requests == []


def test_a2a_client_marks_invalid_url_before_transport_dispatch() -> None:
    remote_ref = _remote_ref()
    remote_ref.url = "not-a-valid-http-url"
    client = A2AClient()

    async def scenario() -> A2AClientError:
        with pytest.raises(A2AClientError) as raised:
            await client.get_task(remote_ref, task_id="task-1")
        return raised.value

    error = asyncio.run(scenario())
    assert error.effect_boundary == "not_dispatched"


@pytest.mark.parametrize(
    ("error_type", "expected_boundary"),
    [
        (httpx.ConnectError, "not_dispatched"),
        (httpx.ConnectTimeout, "not_dispatched"),
        (httpx.PoolTimeout, "not_dispatched"),
        (httpx.ReadTimeout, "possibly_dispatched"),
        (httpx.WriteTimeout, "possibly_dispatched"),
    ],
)
def test_a2a_client_preserves_http_effect_boundary(
    error_type: type[httpx.HTTPError],
    expected_boundary: str,
) -> None:
    remote_ref = _remote_ref()

    def fail(request: httpx.Request) -> httpx.Response:
        raise error_type("injected transport failure", request=request)

    client = A2AClient(transports={remote_ref.url: httpx.MockTransport(fail)})

    async def scenario() -> A2AClientError:
        with pytest.raises(A2AClientError) as raised:
            await client.get_task(remote_ref, task_id="task-1")
        return raised.value

    error = asyncio.run(scenario())
    assert error.effect_boundary == expected_boundary


def test_a2a_client_closes_task_event_stream_when_consumer_is_cancelled() -> None:
    stream = HangingSuccessStream()
    remote_ref = _remote_ref()
    client = A2AClient(
        transports={
            remote_ref.url: httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    headers={"content-type": "text/event-stream"},
                    stream=stream,
                )
            )
        }
    )

    async def consume() -> None:
        async with client.open_task_event_stream(
            remote_ref,
            task_id="task-1",
            run_count=1,
            last_event_id=None,
        ) as events:
            assert (await anext(events)).event_type == "assistant.delta"
            await anext(events)

    async def scenario() -> None:
        task = asyncio.create_task(consume())
        await stream.started.wait()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert stream.closed is True


@pytest.mark.parametrize("mode", ["timeout", "cancel"])
def test_a2a_json_timeout_cancel_closes_and_preserves_effect(mode: str) -> None:
    stream = HangingSuccessStream()
    remote_ref = _remote_ref()
    client = A2AClient(
        timeout=0.01 if mode == "timeout" else 5,
        transports={
            remote_ref.url: httpx.MockTransport(
                lambda request: httpx.Response(200, stream=stream)
            )
        },
    )

    async def scenario() -> None:
        task = asyncio.create_task(client.get_task(remote_ref, task_id="task-1"))
        await stream.started.wait()
        if mode == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(A2AClientError) as exc_info:
                await task
            assert exc_info.value.effect_boundary == "possibly_dispatched"

    asyncio.run(scenario())
    assert stream.closed is True
