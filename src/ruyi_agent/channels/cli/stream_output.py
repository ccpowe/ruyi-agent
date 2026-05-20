from __future__ import annotations

import json
from typing import Any

MAX_TOOL_ARG_PREVIEW = 120
MAX_TOOL_RESULT_PREVIEW = 160
MAX_TOOL_ARG_VALUE_PREVIEW = 60


def _read_field(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def _truncate(text: str, *, limit: int) -> str:
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _stringify_content(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for block in content:
            if isinstance(block, str):
                parts.append(block)
                continue
            if isinstance(block, dict):
                text = block.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
        return "\n".join(parts).strip()
    if content is None:
        return ""
    return str(content)


def _normalize_text(text: Any) -> str:
    return " ".join(str(text).split())


def _truncate_tool_arg_value(value: Any) -> Any:
    if isinstance(value, str):
        return _truncate(value, limit=MAX_TOOL_ARG_VALUE_PREVIEW)
    if isinstance(value, dict):
        return {
            str(key): _truncate_tool_arg_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_truncate_tool_arg_value(item) for item in value]
    return value


def _format_tool_args(args: Any) -> str:
    if isinstance(args, dict):
        rendered = json.dumps(
            _truncate_tool_arg_value(args),
            ensure_ascii=False,
            separators=(",", ":"),
        )
    else:
        rendered = str(args)
    return _truncate(rendered, limit=MAX_TOOL_ARG_PREVIEW)


def _format_tool_call(tool_call: Any) -> str:
    name = _read_field(tool_call, "name") or "unknown_tool"
    args = _format_tool_args(_read_field(tool_call, "args"))
    return f"{name}({args})"


def _extract_messages(payload: Any) -> list[Any]:
    messages = _read_field(payload, "messages")
    if isinstance(messages, list):
        return messages
    return []


def _summarize_model_event(payload: Any) -> str | None:
    messages = _extract_messages(payload)
    if not messages:
        return None

    message = messages[-1]
    tool_calls = _read_field(message, "tool_calls")
    if isinstance(tool_calls, list) and tool_calls:
        rendered = " | ".join(_format_tool_call(call) for call in tool_calls)
        return f"调用工具: {rendered}"

    content = _normalize_text(_stringify_content(_read_field(message, "content")))
    return content or None


def _summarize_tools_event(payload: Any) -> str | None:
    messages = _extract_messages(payload)
    if not messages:
        return None

    message = messages[-1]
    name = _read_field(message, "name") or "tool"
    content = _truncate(
        _stringify_content(_read_field(message, "content")),
        limit=MAX_TOOL_RESULT_PREVIEW,
    )
    if not content:
        return f"工具结果 {name}"
    return f"工具结果 {name}: {content}"


def summarize_update_data(data: Any) -> str | None:
    if not isinstance(data, dict):
        text = _normalize_text(data)
        return text or None

    for event_name, payload in data.items():
        if event_name == "model":
            return _summarize_model_event(payload)
        if event_name == "tools":
            return _summarize_tools_event(payload)
    return None


def _format_namespace(ns: Any) -> str:
    if isinstance(ns, (list, tuple)):
        parts = [str(part) for part in ns if part]
        return " / ".join(parts)
    return str(ns)


def format_stream_chunk(chunk: dict[str, Any]) -> str | None:
    if chunk.get("type") != "updates":
        return None

    summary = summarize_update_data(chunk.get("data"))
    if not summary:
        return None

    ns = chunk.get("ns")
    if ns:
        return f"[subagent: {_format_namespace(ns)}] {summary}"
    return f"[main agent] {summary}"
