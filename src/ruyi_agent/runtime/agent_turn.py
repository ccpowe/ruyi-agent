from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class AgentTurnOutcome:
    content: str = ""
    review_payloads: list[dict[str, Any]] | None = None
    has_unresolved_tool_calls: bool = False

    def __post_init__(self) -> None:
        if self.review_payloads is None:
            self.review_payloads = []


def _read_message_content(message: Any) -> Any:
    if isinstance(message, dict):
        return message.get("content")
    return getattr(message, "content", None)


def _read_message_role(message: Any) -> Any:
    role = getattr(message, "type", None) or getattr(message, "role", None)
    if role is None and isinstance(message, dict):
        role = message.get("type") or message.get("role")
    return role


def _read_messages(result: Any) -> list[Any] | None:
    value = getattr(result, "value", None)
    if value is not None:
        return _read_messages(value)
    if not isinstance(result, dict):
        return None
    messages = result.get("messages")
    return messages if isinstance(messages, list) else None


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


def _interrupt_value(item: Any) -> Any:
    return getattr(item, "value", item)


def extract_review_payloads(value: Any) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    if isinstance(value, dict):
        if "__interrupt__" in value:
            raw_interrupt = value["__interrupt__"]
            if isinstance(raw_interrupt, (list, tuple)):
                for item in raw_interrupt:
                    payloads.extend(extract_review_payloads(_interrupt_value(item)))
            else:
                payloads.extend(extract_review_payloads(_interrupt_value(raw_interrupt)))
            return payloads
        if "action_requests" in value and "review_configs" in value:
            payloads.append(value)
            return payloads
        for item in value.values():
            payloads.extend(extract_review_payloads(item))
    elif isinstance(value, (list, tuple)):
        for item in value:
            payloads.extend(extract_review_payloads(_interrupt_value(item)))
    return payloads


def has_unresolved_tool_calls(result: Any) -> bool:
    messages = _read_messages(result)
    if not messages:
        return False

    tool_message_ids: set[str] = set()
    for message in messages:
        tool_call_id = getattr(message, "tool_call_id", None)
        if tool_call_id is None and isinstance(message, dict):
            tool_call_id = message.get("tool_call_id")
        if isinstance(tool_call_id, str):
            tool_message_ids.add(tool_call_id)

    for message in reversed(messages):
        if _read_message_role(message) not in {"ai", "assistant"}:
            continue
        tool_calls = getattr(message, "tool_calls", None)
        if tool_calls is None and isinstance(message, dict):
            tool_calls = message.get("tool_calls")
        if not isinstance(tool_calls, list):
            return False
        return any(
            isinstance(call, dict)
            and isinstance(call.get("id"), str)
            and call["id"] not in tool_message_ids
            for call in tool_calls
        )
    return False


def _extract_result_content(result: Any) -> str:
    messages = _read_messages(result)
    if messages is None:
        if isinstance(result, dict):
            return ""
        return _stringify_content(result)
    for message in reversed(messages):
        if _read_message_role(message) not in {"ai", "assistant"}:
            continue
        content = _read_message_content(message)
        if content is None:
            continue
        text = _stringify_content(content)
        if text:
            return text
    return ""


def normalize_agent_turn_result(result: Any) -> AgentTurnOutcome:
    value = getattr(result, "value", None)
    if value is not None:
        return normalize_agent_turn_result(value)
    review_payloads = extract_review_payloads(result)
    unresolved_tool_calls = has_unresolved_tool_calls(result)
    return AgentTurnOutcome(
        content=_extract_result_content(result),
        review_payloads=review_payloads,
        has_unresolved_tool_calls=unresolved_tool_calls,
    )


async def normalize_agent_turn(
    agent: Any,
    config: dict[str, Any],
    result: Any,
) -> AgentTurnOutcome:
    outcome = normalize_agent_turn_result(result)
    if outcome.review_payloads:
        return outcome

    aget_state = getattr(agent, "aget_state", None)
    if aget_state is None:
        return outcome
    try:
        snapshot = await aget_state(config)
    except Exception:
        logger.exception("Failed to inspect agent turn state snapshot")
        return outcome
    interrupts = getattr(snapshot, "interrupts", None)
    if not interrupts:
        return outcome
    return AgentTurnOutcome(
        content=outcome.content,
        review_payloads=extract_review_payloads({"__interrupt__": interrupts}),
        has_unresolved_tool_calls=outcome.has_unresolved_tool_calls,
    )
