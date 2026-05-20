from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import StructuredTool

from ruyi_agent.integrations.mcp.registry import ToolInfo, ToolSourceSummary
from ruyi_agent.runtime.agent_factory import create_runtime_agent
from ruyi_agent.runtime.middleware.tool_search import ToolSearchMiddleware


def get_weather(location: str) -> str:
    """Get current weather for one city."""
    return f"weather({location})=sunny"


class FakeRegistry:
    def __init__(self) -> None:
        self.tool_info = ToolInfo(
            server_name="local",
            name="get_weather",
            qualified_name="local.get_weather",
            description="Get current weather for one city.",
            args_schema={
                "type": "object",
                "properties": {"location": {"type": "string"}},
                "required": ["location"],
            },
        )
        self.tool = StructuredTool.from_function(
            get_weather,
            name="get_weather",
            description="Get current weather for one city.",
        )
        self.search_calls: list[dict[str, Any]] = []
        self.validate_calls: list[dict[str, Any]] = []

    async def search_tools(
        self,
        query: str,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
        limit: int = 8,
    ) -> list[ToolInfo]:
        self.search_calls.append(
            {
                "query": query,
                "server_names": server_names,
                "tool_names": tool_names,
                "limit": limit,
            }
        )
        allowed = self.resolve_scope_qualified_names(
            server_names=server_names,
            tool_names=tool_names,
        )
        if allowed is not None and self.tool_info.qualified_name not in allowed:
            return []
        return [self.tool_info]

    async def get_tool(self, tool_name: str) -> Any:
        if tool_name != "local.get_weather":
            raise ValueError(f"Unknown tool: {tool_name}")
        return self.tool

    def resolve_scope_qualified_names(
        self,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
    ) -> set[str] | None:
        if server_names is None and tool_names is None:
            return None
        allowed: set[str] = set()
        if server_names and "local" in server_names:
            allowed.add("local.get_weather")
        if tool_names:
            allowed.update(tool_names)
        return allowed

    def validate_tool_arguments(
        self,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> None:
        self.validate_calls.append({"tool_name": tool_name, "arguments": arguments})
        if tool_name != "local.get_weather":
            raise ValueError(f"Unknown tool: {tool_name}")
        if "location" not in arguments:
            raise ValueError(
                "Invalid arguments for tool local.get_weather: "
                "'location' is a required property"
            )
        if not isinstance(arguments["location"], str):
            raise ValueError(
                "Invalid arguments for tool local.get_weather: "
                "location must be a string"
            )

    def summarize_tool_sources(
        self,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
    ) -> list[ToolSourceSummary]:
        allowed = self.resolve_scope_qualified_names(
            server_names=server_names,
            tool_names=tool_names,
        )
        if allowed is not None and self.tool_info.qualified_name not in allowed:
            return []
        return [
            ToolSourceSummary(
                name="local",
                description="1 tool available. Capabilities include: Get current weather for one city.",
                tool_count=1,
            )
        ]


class FakeToolCallingModel(BaseChatModel):
    responses: list[Any]
    i: int = 0

    @property
    def _llm_type(self) -> str:
        return "fake-tool-search-model"

    def bind_tools(self, tools, *, tool_choice=None, **kwargs):  # noqa: ANN001, ARG002
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):  # noqa: ANN001, ARG002
        response = self.responses[self.i]
        if self.i < len(self.responses) - 1:
            self.i += 1
        return ChatResult(generations=[ChatGeneration(message=response)])

    async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs):  # noqa: ANN001, ARG002
        return self._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def test_tool_search_returns_metadata_and_instruction() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(
        registry=registry,  # type: ignore[arg-type]
        server_names=["local"],
        max_results=3,
    )

    result = asyncio.run(middleware.search(query="weather", limit=2))
    payload = json.loads(result)

    assert registry.search_calls == [
        {
            "query": "weather",
            "server_names": ["local"],
            "tool_names": [],
            "limit": 2,
        }
    ]
    assert payload["tools"][0]["qualified_name"] == "local.get_weather"
    assert payload["tools"][0]["args_schema"]["required"] == ["location"]
    assert payload["scope"]["server_names"] == ["local"]
    assert payload["scope"]["requires_qualified_name"] is True
    assert "call_tool" in payload["instruction"]
    assert middleware.system_prompt is not None
    assert "Available MCP tool catalog through `tool_search`" in middleware.system_prompt
    assert "- local: 1 tool available." in middleware.system_prompt


def test_call_tool_executes_registry_tool() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(
        registry=registry,  # type: ignore[arg-type]
        server_names=["local"],
    )

    result = asyncio.run(
        middleware.call_tool(
            qualified_name="local.get_weather",
            arguments={"location": "Shanghai"},
        )
    )

    assert result == "weather(Shanghai)=sunny"
    assert registry.validate_calls == [
        {"tool_name": "local.get_weather", "arguments": {"location": "Shanghai"}}
    ]


def test_call_tool_default_scope_is_empty_allowlist() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(registry=registry)  # type: ignore[arg-type]

    result = asyncio.run(middleware.search(query="weather"))
    payload = json.loads(result)

    assert payload["tools"] == []
    assert payload["scope"]["server_names"] == []
    assert payload["scope"]["tool_names"] == []
    with pytest.raises(ValueError, match="outside this agent"):
        asyncio.run(
            middleware.call_tool(
                qualified_name="local.get_weather",
                arguments={"location": "Shanghai"},
            )
        )


def test_call_tool_rejects_bare_tool_name() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(registry=registry)  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="qualified_name"):
        asyncio.run(
            middleware.call_tool(
                qualified_name="get_weather",
                arguments={"location": "Shanghai"},
            )
        )


def test_call_tool_rejects_out_of_scope_tool() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(
        registry=registry,  # type: ignore[arg-type]
        server_names=[],
        tool_names=[],
    )

    with pytest.raises(ValueError, match="outside this agent"):
        asyncio.run(
            middleware.call_tool(
                qualified_name="local.get_weather",
                arguments={"location": "Shanghai"},
            )
        )


def test_search_respects_empty_allowlist() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(
        registry=registry,  # type: ignore[arg-type]
        server_names=[],
        tool_names=[],
    )

    result = asyncio.run(middleware.search(query="weather"))
    payload = json.loads(result)

    assert payload["tools"] == []
    assert payload["scope"]["server_names"] == []
    assert payload["scope"]["tool_names"] == []


def test_call_tool_uses_underlying_argument_validation() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(
        registry=registry,  # type: ignore[arg-type]
        server_names=["local"],
    )

    with pytest.raises(ValueError, match="location"):
        asyncio.run(
            middleware.call_tool(
                qualified_name="local.get_weather",
                arguments={},
            )
        )


def test_middleware_exposes_stable_search_and_executor_tools() -> None:
    registry = FakeRegistry()
    middleware = ToolSearchMiddleware(registry=registry)  # type: ignore[arg-type]

    assert [tool.name for tool in middleware.tools] == ["tool_search", "call_tool"]


def test_call_tool_runs_through_agent_tool_runtime_path() -> None:
    registry = FakeRegistry()
    model = FakeToolCallingModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "call_tool",
                        "args": {
                            "qualified_name": "local.get_weather",
                            "arguments": {"location": "Shanghai"},
                        },
                        "id": "call-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="done"),
        ]
    )
    agent = create_runtime_agent(
        model=model,
        tools=[],
        system_prompt="Test tool search middleware",
        tool_search_registry=registry,  # type: ignore[arg-type]
        tool_search_server_names=["local"],
        tool_search_tool_names=[],
    )

    async def scenario():
        return await agent.ainvoke(
            {"messages": [{"role": "user", "content": "run weather"}]},
            version="v2",
        )

    result = asyncio.run(scenario())
    messages = result.value["messages"]
    tool_messages = [
        message
        for message in messages
        if isinstance(message, ToolMessage) and message.name == "call_tool"
    ]

    assert tool_messages
    assert tool_messages[-1].content == "weather(Shanghai)=sunny"
