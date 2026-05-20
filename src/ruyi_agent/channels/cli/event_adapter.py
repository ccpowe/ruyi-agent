from __future__ import annotations

from typing import Any

from langgraph.types import Command

from ruyi_agent.runtime.events import RuntimeEvent, RuntimeEventKind
from ruyi_agent.runtime.agent_turn import extract_review_payloads
from ruyi_agent.channels.cli.stream_output import summarize_update_data


def _read_field(value: Any, field: str) -> Any:
    if isinstance(value, dict):
        return value.get(field)
    return getattr(value, field, None)


def _read_messages(payload: Any) -> list[Any]:
    messages = _read_field(payload, "messages")
    return messages if isinstance(messages, list) else []


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


def _format_namespace(ns: Any) -> str | None:
    if not ns:
        return None
    if isinstance(ns, (list, tuple)):
        parts = [str(part) for part in ns if part]
        return " / ".join(parts) or None
    return str(ns)


def extract_interrupt_requests(value: Any) -> list[dict[str, Any]]:
    return extract_review_payloads(value)


def runtime_event_from_stream_chunk(chunk: dict[str, Any]) -> RuntimeEvent | None:
    if chunk.get("type") != "updates":
        return None

    data = chunk.get("data")
    if not isinstance(data, dict):
        text = summarize_update_data(data)
        if text:
            return RuntimeEvent(RuntimeEventKind.CONTENT_UPDATE, text=text)
        return None

    namespace = _format_namespace(chunk.get("ns"))
    if "model" in data:
        messages = _read_messages(data["model"])
        if not messages:
            return None
        message = messages[-1]
        tool_calls = _read_field(message, "tool_calls")
        if isinstance(tool_calls, list) and tool_calls:
            text = summarize_update_data(data) or "tool call"
            return RuntimeEvent(
                RuntimeEventKind.TOOL_CALL_STARTED,
                text=text,
                namespace=namespace,
                payload={"tool_calls": tool_calls},
            )
        content = _stringify_content(_read_field(message, "content"))
        if content:
            return RuntimeEvent(
                RuntimeEventKind.CONTENT_UPDATE,
                text=content,
                namespace=namespace,
            )
        return None

    if "tools" in data:
        text = summarize_update_data(data) or "tool result"
        return RuntimeEvent(
            RuntimeEventKind.TOOL_RESULT,
            text=text,
            namespace=namespace,
            payload={"tools": data["tools"]},
        )

    text = summarize_update_data(data)
    if text:
        return RuntimeEvent(
            RuntimeEventKind.CONTENT_UPDATE,
            text=text,
            namespace=namespace,
        )
    return None


async def stream_agent_events(
    agent: Any,
    payload: Any,
    config: dict[str, Any],
):
    interrupt_requests: list[dict[str, Any]] = []
    async for chunk in agent.astream(
        payload,
        config=config,
        stream_mode="updates",
        version="v2",
    ):
        interrupt_requests.extend(extract_interrupt_requests(chunk))
        if interrupt_requests:
            continue
        event = runtime_event_from_stream_chunk(chunk)
        if event is not None:
            yield event
    return


def resume_command_from_decisions(decisions: list[dict[str, Any]]) -> Command:
    return Command(resume={"decisions": decisions})
