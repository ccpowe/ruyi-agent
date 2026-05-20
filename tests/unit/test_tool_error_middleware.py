from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from langchain_core.messages import ToolMessage

from ruyi_agent.runtime.middleware.tool_error import ToolErrorMiddleware


class ConnectError(Exception):
    pass


class BrokenResourceError(Exception):
    pass


def build_request(
    *,
    tool_name: str = "web_search_exa",
    tool_call_id: str = "call-1",
) -> SimpleNamespace:
    return SimpleNamespace(
        tool=SimpleNamespace(name=tool_name),
        tool_call={"id": tool_call_id, "name": tool_name, "args": {"query": "demo"}},
        state={},
        runtime=None,
    )


def test_awrap_tool_call_falls_back_when_tool_call_id_is_missing() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request()
    request.tool_call = {"name": "web_search_exa", "args": {"query": "demo"}}

    async def handler(_request):
        raise RuntimeError("boom")

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert result.tool_call_id == "unknown"


def test_awrap_tool_call_returns_structured_error_message() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request()

    async def handler(_request):
        raise RuntimeError("boom")

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert result.tool_call_id == "call-1"
    assert result.name == "web_search_exa"
    assert "tool=web_search_exa" in str(result.content)
    assert "category=unexpected" in str(result.content)
    assert "retriable=false" in str(result.content)
    assert "RuntimeError: boom" in str(result.content)


def test_awrap_tool_call_classifies_source_not_available() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request(tool_name="web_fetch_exa")

    async def handler(_request):
        raise RuntimeError(
            "Error fetching URL(s): https://www.wsj.com: SOURCE_NOT_AVAILABLE"
        )

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "tool=web_fetch_exa" in str(result.content)
    assert "category=source_unavailable" in str(result.content)
    assert "retriable=false" in str(result.content)
    assert "alternate accessible sources" in str(result.content)


def test_awrap_tool_call_flattens_nested_exception_group() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request()

    async def handler(_request):
        raise ExceptionGroup(
            "outer",
            [
                ExceptionGroup("inner", [ConnectError("dns failed")]),
                RuntimeError("secondary"),
            ],
        )

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "category=network" in str(result.content)
    assert "retriable=true" in str(result.content)
    assert "ConnectError: dns failed" in str(result.content)
    assert "RuntimeError: secondary" in str(result.content)
    assert "sub-exception" not in str(result.content)


def test_awrap_tool_call_reraises_pure_cancelled_error_group() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request()

    async def handler(_request):
        raise BaseExceptionGroup(
            "outer",
            [
                asyncio.CancelledError(),
            ],
        )

    with pytest.raises(BaseExceptionGroup):
        asyncio.run(middleware.awrap_tool_call(request, handler))


def test_awrap_tool_call_converts_mixed_mcp_transport_cancellation() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request(tool_name="web_fetch_exa")

    async def handler(_request):
        raise BaseExceptionGroup(
            "mcp transport failed",
            [
                asyncio.CancelledError(),
                BrokenResourceError("stream closed"),
            ],
        )

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert isinstance(result, ToolMessage)
    assert result.status == "error"
    assert "tool=web_fetch_exa" in str(result.content)
    assert "category=network" in str(result.content)
    assert "retriable=true" in str(result.content)
    assert "CancelledError: CancelledError" in str(result.content)
    assert "BrokenResourceError: stream closed" in str(result.content)


def test_awrap_tool_call_retries_once_for_retriable_error() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request()
    attempts = {"count": 0}

    async def handler(_request):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise ConnectError("temporary network failure")
        return ToolMessage(
            content="ok",
            tool_call_id="call-1",
            name="web_search_exa",
        )

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert attempts["count"] == 2
    assert isinstance(result, ToolMessage)
    assert result.status == "success"
    assert result.content == "ok"


def test_awrap_tool_call_does_not_touch_existing_tool_error_message() -> None:
    middleware = ToolErrorMiddleware()
    request = build_request()
    expected = ToolMessage(
        content="business error",
        tool_call_id="call-1",
        name="web_search_exa",
        status="error",
    )

    async def handler(_request):
        return expected

    result = asyncio.run(middleware.awrap_tool_call(request, handler))

    assert result is expected


def test_parallel_tool_calls_keep_success_and_failure_isolated() -> None:
    middleware = ToolErrorMiddleware()
    failed_request = build_request(tool_name="web_search_exa", tool_call_id="call-1")
    ok_request = build_request(tool_name="web_fetch_exa", tool_call_id="call-2")

    async def failing_handler(_request):
        raise ExceptionGroup("outer", [ConnectError("dns failed")])

    async def ok_handler(_request):
        return ToolMessage(
            content="page body",
            tool_call_id="call-2",
            name="web_fetch_exa",
        )

    async def scenario():
        return await asyncio.gather(
            middleware.awrap_tool_call(failed_request, failing_handler),
            middleware.awrap_tool_call(ok_request, ok_handler),
        )

    failed_result, ok_result = asyncio.run(scenario())

    assert isinstance(failed_result, ToolMessage)
    assert failed_result.status == "error"
    assert "ConnectError: dns failed" in str(failed_result.content)
    assert isinstance(ok_result, ToolMessage)
    assert ok_result.status == "success"
    assert ok_result.content == "page body"
