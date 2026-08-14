from __future__ import annotations

from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ContextT, ResponseT
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langgraph.types import Overwrite


def _tool_call_id(message: BaseMessage) -> str | None:
    value = getattr(message, "tool_call_id", None)
    return value if isinstance(value, str) and value else None


def repair_tool_call_protocol(messages: list[BaseMessage]) -> list[BaseMessage]:
    """Return an OpenAI-compatible history with contiguous tool responses.

    Providers require every assistant ``tool_calls`` message to be followed
    immediately by one response for every call.  Runtime messages (notably a
    mailbox HumanMessage) may otherwise be inserted between the call and its
    result.  Reorder existing results into the required slot and synthesize a
    cancellation result only when no result exists anywhere in the history.
    """

    tool_messages: dict[str, tuple[int, ToolMessage]] = {}
    for index, message in enumerate(messages):
        if isinstance(message, ToolMessage):
            call_id = _tool_call_id(message)
            if call_id and call_id not in tool_messages:
                tool_messages[call_id] = (index, message)

    repaired: list[BaseMessage] = []
    for message in messages:
        if isinstance(message, ToolMessage):
            # Tool messages are emitted beside their owning AI message below.
            # Orphan results are omitted because providers reject them too.
            continue

        if isinstance(message, AIMessage) and message.invalid_tool_calls:
            # A provider may return a truncated/invalid JSON argument payload.
            # LangChain preserves it as invalid_tool_calls, but sending that
            # malformed call back to strict OpenAI-compatible providers poisons
            # every continuation.  It was never executable, so retain an audit
            # note in content and remove the protocol-level call.
            note = (
                "[Invalid tool call was discarded because its arguments were "
                "not valid JSON. Reissue the operation if it is still needed.]"
            )
            content = message.content
            if isinstance(content, str):
                content = f"{content}\n{note}" if content else note
            else:
                content = [*content, {"type": "text", "text": note}]
            message = message.model_copy(
                update={"content": content, "invalid_tool_calls": []}
            )

        repaired.append(message)
        if not isinstance(message, AIMessage) or not message.tool_calls:
            continue

        for call in message.tool_calls:
            call_id = call.get("id")
            if not isinstance(call_id, str) or not call_id:
                continue
            existing = tool_messages.get(call_id)
            if existing is not None:
                _, tool_message = existing
                repaired.append(tool_message)
                continue
            tool_name = call.get("name")
            repaired.append(
                ToolMessage(
                    content=(
                        "Tool call was cancelled before a result was recorded. "
                        "Re-evaluate whether the operation still needs to run."
                    ),
                    name=tool_name if isinstance(tool_name, str) else None,
                    tool_call_id=call_id,
                    status="error",
                )
            )

    return repaired


class ToolCallProtocolMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Repair tool-call adjacency immediately before every model request."""

    def before_model(self, state: object, runtime: Any) -> dict[str, Any] | None:
        return self._repair(state)

    async def abefore_model(
        self, state: object, runtime: Any
    ) -> dict[str, Any] | None:
        return self._repair(state)

    @staticmethod
    def _repair(state: object) -> dict[str, Any] | None:
        if not isinstance(state, dict):
            return None
        messages = state.get("messages")
        if not isinstance(messages, list) or not messages:
            return None
        repaired = repair_tool_call_protocol(messages)
        if repaired == messages:
            return None
        return {"messages": Overwrite(repaired)}
