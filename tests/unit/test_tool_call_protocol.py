from __future__ import annotations

import asyncio

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.types import Overwrite

from ruyi_agent.runtime.middleware.tool_call_protocol import (
    ToolCallProtocolMiddleware,
    repair_tool_call_protocol,
)


def test_repair_moves_tool_result_before_intervening_mailbox_input() -> None:
    assistant = AIMessage(
        content="",
        tool_calls=[{"id": "call-1", "name": "read_file", "args": {}}],
    )
    mailbox = HumanMessage(content="new direction")
    result = ToolMessage(content="contents", tool_call_id="call-1")

    repaired = repair_tool_call_protocol([assistant, mailbox, result])

    assert repaired == [assistant, result, mailbox]


def test_repair_synthesizes_result_for_dangling_tool_call() -> None:
    assistant = AIMessage(
        content="",
        tool_calls=[{"id": "call-1", "name": "execute", "args": {}}],
    )
    mailbox = HumanMessage(content="continue")

    repaired = repair_tool_call_protocol([assistant, mailbox])

    assert repaired[0] is assistant
    assert isinstance(repaired[1], ToolMessage)
    assert repaired[1].tool_call_id == "call-1"
    assert repaired[1].status == "error"
    assert repaired[2] is mailbox


def test_middleware_repairs_on_every_model_boundary() -> None:
    assistant = AIMessage(
        content="",
        tool_calls=[{"id": "call-1", "name": "grep", "args": {}}],
    )
    mailbox = HumanMessage(content="mailbox input")
    result = ToolMessage(content="matches", tool_call_id="call-1")
    middleware = ToolCallProtocolMiddleware()

    update = asyncio.run(
        middleware.abefore_model(
            {"messages": [assistant, mailbox, result]},
            None,
        )
    )

    assert update is not None
    assert isinstance(update["messages"], Overwrite)
    assert update["messages"].value == [assistant, result, mailbox]


def test_repair_discards_unexecutable_invalid_tool_call_for_continuation() -> None:
    assistant = AIMessage(
        content="drafting",
        invalid_tool_calls=[
            {
                "id": "bad-call",
                "name": "write_file",
                "args": '{"path":"unterminated',
                "error": "invalid json",
            }
        ],
    )
    followup = HumanMessage(content="answer the decision only")

    repaired = repair_tool_call_protocol([assistant, followup])

    assert isinstance(repaired[0], AIMessage)
    assert repaired[0].invalid_tool_calls == []
    assert "Invalid tool call was discarded" in str(repaired[0].content)
    assert repaired[1] is followup
