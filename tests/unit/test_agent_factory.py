from __future__ import annotations

import asyncio
from typing import Any, Sequence

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.channels import DeltaChannel
from pydantic import Field

from ruyi_agent.runtime.agent_factory import create_runtime_agent


class FakeModel(BaseChatModel):
    seen_contents: list[list[str]] = Field(default_factory=list)

    @property
    def _llm_type(self) -> str:
        return "fake-model"

    def bind_tools(
        self,
        tools: Sequence[Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> "FakeModel":
        return self

    def _generate(
        self,
        messages,
        stop=None,
        run_manager=None,
        **kwargs,
    ) -> ChatResult:
        self.seen_contents.append(
            [str(getattr(message, "content", "")) for message in messages]
        )
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="ok"))])

    async def _agenerate(
        self,
        messages,
        stop=None,
        run_manager=None,
        **kwargs,
    ) -> ChatResult:
        return self._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def test_runtime_agent_uses_delta_channel_for_messages_without_dropping_middleware_state() -> None:
    agent = create_runtime_agent(
        model=FakeModel(),
        tools=[],
        system_prompt="Test runtime agent",
    )

    assert isinstance(agent.channels["messages"], DeltaChannel)
    assert "todos" in agent.channels
    assert "files" in agent.channels
    assert "skills_metadata" in agent.channels


def test_runtime_agent_resumes_messages_from_sqlite_delta_checkpoints(tmp_path) -> None:
    model = FakeModel()

    async def scenario() -> None:
        async with AsyncSqliteSaver.from_conn_string(
            str(tmp_path / "checkpoints.sqlite")
        ) as checkpointer:
            agent = create_runtime_agent(
                model=model,
                tools=[],
                system_prompt="Test runtime agent",
                checkpointer=checkpointer,
            )
            config = {"configurable": {"thread_id": "thread-1"}}

            await agent.ainvoke(
                {"messages": [{"role": "user", "content": "first"}]},
                config=config,
                version="v2",
            )
            await agent.ainvoke(
                {"messages": [{"role": "user", "content": "second"}]},
                config=config,
                version="v2",
            )

    asyncio.run(scenario())

    assert any("first" in contents for contents in model.seen_contents[1:])
    assert any("second" in contents for contents in model.seen_contents[1:])
