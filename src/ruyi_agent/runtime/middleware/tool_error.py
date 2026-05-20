from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.types import Command

logger = logging.getLogger(__name__)

MAX_TOOL_RETRY_ATTEMPTS = 2
MAX_TOOL_RETRY_WINDOW_SECONDS = 15.0
_RETRY = object()


def _iter_leaf_exceptions(exc: BaseException) -> list[BaseException]:
    if isinstance(exc, BaseExceptionGroup):
        leaves: list[BaseException] = []
        for sub_exc in exc.exceptions:
            leaves.extend(_iter_leaf_exceptions(sub_exc))
        return leaves
    return [exc]


def _contains_unrecoverable_exception(exc: BaseException) -> bool:
    leaves = _iter_leaf_exceptions(exc)
    if any(isinstance(leaf, (KeyboardInterrupt, SystemExit)) for leaf in leaves):
        return True
    # A plain cancellation means the run itself is being stopped and must
    # propagate. MCP transports can also emit mixed groups such as
    # CancelledError + ConnectTimeout/BrokenResourceError; those are tool
    # transport failures and should become ToolMessage errors instead.
    return bool(leaves) and all(isinstance(leaf, asyncio.CancelledError) for leaf in leaves)


def _flatten_exception_messages(exc: BaseException) -> list[str]:
    messages: list[str] = []
    for leaf in _iter_leaf_exceptions(exc):
        message = str(leaf).strip() or leaf.__class__.__name__
        rendered = f"{leaf.__class__.__name__}: {message}"
        if rendered not in messages:
            messages.append(rendered)
    return messages


def _read_status_code(exc: BaseException) -> int | None:
    status_code = getattr(exc, "status_code", None)
    if isinstance(status_code, int):
        return status_code

    response = getattr(exc, "response", None)
    response_status = getattr(response, "status_code", None)
    if isinstance(response_status, int):
        return response_status
    return None


def _classify_exception(exc: BaseException) -> tuple[str, bool, str]:
    leaves = _iter_leaf_exceptions(exc)
    status_codes = [code for code in (_read_status_code(leaf) for leaf in leaves) if code]
    lower_messages = [str(leaf).lower() for leaf in leaves]

    if any(code in {401, 403} for code in status_codes):
        return ("auth", False, "Check credentials or server authorization.")
    if any(500 <= code <= 599 for code in status_codes):
        return ("unavailable", True, "Retry later because the service may be temporary unavailable.")
    if any(400 <= code <= 499 for code in status_codes):
        return ("invalid_input", False, "Check tool arguments and request format before retrying.")

    lower_names = [leaf.__class__.__name__.lower() for leaf in leaves]
    if any("source_not_available" in message for message in lower_messages):
        return (
            "source_unavailable",
            False,
            "Use search snippets or alternate accessible sources instead of fetching this URL directly.",
        )
    if any("timeout" in name for name in lower_names):
        return ("timeout", True, "Retry once later or reduce concurrency.")
    if any(
        name
        in {
            "brokenresourceerror",
            "connecterror",
            "networkerror",
            "remoteprotocolerror",
        }
        for name in lower_names
    ):
        return ("network", True, "Retry once later or reduce concurrency.")
    if any(isinstance(leaf, (ValueError, TypeError)) for leaf in leaves):
        return ("invalid_input", False, "Check tool arguments and request format before retrying.")
    return ("unexpected", False, "Inspect the tool error and adjust the plan before retrying.")


def _is_retriable_exception(exc: BaseException) -> bool:
    _, retriable, _ = _classify_exception(exc)
    return retriable


def _read_tool_name(request: Any) -> str:
    tool = getattr(request, "tool", None)
    name = getattr(tool, "name", None)
    if isinstance(name, str) and name:
        return name

    tool_call = getattr(request, "tool_call", None)
    if isinstance(tool_call, dict):
        raw_name = tool_call.get("name")
        if isinstance(raw_name, str) and raw_name:
            return raw_name
    return "unknown_tool"


def _read_tool_call_id(request: Any) -> str:
    tool_call = getattr(request, "tool_call", None)
    if isinstance(tool_call, dict):
        tool_call_id = tool_call.get("id")
        if isinstance(tool_call_id, str) and tool_call_id:
            return tool_call_id
    logger.error("Tool call request is missing tool_call.id; using fallback id.")
    return "unknown"


def _format_error_content(tool_name: str, exc: BaseException) -> str:
    category, retriable, suggestion = _classify_exception(exc)
    summary = " | ".join(_flatten_exception_messages(exc))
    retriable_text = "true" if retriable else "false"
    return "\n".join(
        [
            "Tool execution failed.",
            f"tool={tool_name}",
            f"category={category}",
            f"retriable={retriable_text}",
            f"message={summary}",
            f"suggestion={suggestion}",
        ]
    )


def _build_error_tool_message(request: Any, tool_name: str, exc: BaseException) -> ToolMessage:
    return ToolMessage(
        content=_format_error_content(tool_name, exc),
        name=tool_name,
        tool_call_id=_read_tool_call_id(request),
        status="error",
    )


def _handle_tool_exception(
    *,
    request: Any,
    tool_name: str,
    exc: BaseException,
    attempt: int,
    started_at: float,
) -> object | ToolMessage:
    if _contains_unrecoverable_exception(exc):
        raise exc

    should_retry = (
        attempt + 1 < MAX_TOOL_RETRY_ATTEMPTS
        and _is_retriable_exception(exc)
        and time.monotonic() - started_at < MAX_TOOL_RETRY_WINDOW_SECONDS
    )
    if should_retry:
        logger.warning(
            "Retrying tool call after recoverable error: tool=%s attempt=%s error=%s",
            tool_name,
            attempt + 1,
            " | ".join(_flatten_exception_messages(exc)),
        )
        return _RETRY

    category, retriable, _ = _classify_exception(exc)
    logger.warning(
        "Tool execution failed: tool=%s category=%s retriable=%s error=%s",
        tool_name,
        category,
        retriable,
        " | ".join(_flatten_exception_messages(exc)),
    )
    return _build_error_tool_message(request, tool_name, exc)


class ToolErrorMiddleware(AgentMiddleware):
    """Convert unexpected tool exceptions into structured ToolMessage errors."""

    def wrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], ToolMessage | Command[Any]],
    ) -> ToolMessage | Command[Any]:
        tool_name = _read_tool_name(request)
        started_at = time.monotonic()

        for attempt in range(MAX_TOOL_RETRY_ATTEMPTS):
            try:
                return handler(request)
            except BaseException as exc:
                outcome = _handle_tool_exception(
                    request=request,
                    tool_name=tool_name,
                    exc=exc,
                    attempt=attempt,
                    started_at=started_at,
                )
                if outcome is _RETRY:
                    continue
                return outcome

        raise AssertionError("Tool retry loop exited unexpectedly")

    async def awrap_tool_call(
        self,
        request: Any,
        handler: Callable[[Any], Awaitable[ToolMessage | Command[Any]]],
    ) -> ToolMessage | Command[Any]:
        tool_name = _read_tool_name(request)
        started_at = time.monotonic()

        for attempt in range(MAX_TOOL_RETRY_ATTEMPTS):
            try:
                return await handler(request)
            except BaseException as exc:
                outcome = _handle_tool_exception(
                    request=request,
                    tool_name=tool_name,
                    exc=exc,
                    attempt=attempt,
                    started_at=started_at,
                )
                if outcome is _RETRY:
                    continue
                return outcome

        raise AssertionError("Tool retry loop exited unexpectedly")
