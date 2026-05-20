from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any, Sequence

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool

from ruyi_agent.runtime.agent_factory import create_runtime_agent


class ConnectError(Exception):
    pass


class FakeToolCallingModel(BaseChatModel):
    responses: list[Any]
    i: int = 0

    @property
    def _llm_type(self) -> str:
        return "fake-tool-calling-model"

    def bind_tools(
        self,
        tools: Sequence[Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ):
        return self

    def _generate(
        self,
        messages,
        stop=None,
        run_manager=None,
        **kwargs,
    ) -> ChatResult:
        response = self.responses[self.i]
        if self.i < len(self.responses) - 1:
            self.i += 1
        generation = ChatGeneration(message=response)
        return ChatResult(generations=[generation])

    async def _agenerate(
        self,
        messages,
        stop=None,
        run_manager=None,
        **kwargs,
    ) -> ChatResult:
        return self._generate(
            messages,
            stop=stop,
            run_manager=run_manager,
            **kwargs,
        )


@tool
async def ok_tool(query: str) -> str:
    """Return successful tool output."""
    await asyncio.sleep(0.01)
    return f"OK:{query}"


@tool
async def bad_tool(query: str) -> str:
    """Raise a transient network-style error."""
    await asyncio.sleep(0.01)
    raise ConnectError(f"dns failed for {query}")


def test_agent_survives_parallel_tool_failure_and_keeps_tool_metadata() -> None:
    model = FakeToolCallingModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ok_tool",
                        "args": {"query": "alpha"},
                        "id": "call-1",
                        "type": "tool_call",
                    },
                    {
                        "name": "bad_tool",
                        "args": {"query": "beta"},
                        "id": "call-2",
                        "type": "tool_call",
                    },
                ],
            ),
            AIMessage(content="final answer"),
        ]
    )
    agent = create_runtime_agent(
        model=model,
        tools=[ok_tool, bad_tool],
        system_prompt="Test runtime agent",
    )

    async def scenario():
        return await agent.ainvoke(
            {"messages": [{"role": "user", "content": "run both tools"}]},
            version="v2",
        )

    result = asyncio.run(scenario())
    messages = result.value["messages"]

    tool_messages = [message for message in messages if isinstance(message, ToolMessage)]
    assert len(tool_messages) == 2

    ok_message = next(message for message in tool_messages if message.name == "ok_tool")
    bad_message = next(message for message in tool_messages if message.name == "bad_tool")

    assert ok_message.status == "success"
    assert ok_message.tool_call_id == "call-1"
    assert ok_message.content == "OK:alpha"

    assert bad_message.status == "error"
    assert bad_message.tool_call_id == "call-2"
    assert "tool=bad_tool" in str(bad_message.content)
    assert "category=network" in str(bad_message.content)
    assert "ConnectError: dns failed for beta" in str(bad_message.content)

    final_message = messages[-1]
    assert isinstance(final_message, AIMessage)
    assert final_message.content == "final answer"
