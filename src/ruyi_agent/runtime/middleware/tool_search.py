from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from deepagents.middleware._utils import append_to_system_message
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ContextT,
    ModelRequest,
    ModelResponse,
    ResponseT,
)
from langchain_core.messages import ToolMessage
from langchain_core.tools import BaseTool, StructuredTool
from langgraph.prebuilt import ToolRuntime
from pydantic import BaseModel, Field

from ruyi_agent.integrations.mcp.registry import (
    MCPRegistry,
    ToolInfo,
    ToolSourceSummary,
)


TOOL_SEARCH_SYSTEM_PROMPT = """## MCP Tool Search

You have access to a stable tool discovery and execution pair:

- `tool_search`: Search the MCP tool catalog. Results include `qualified_name`,
  description, and argument schema.
- `call_tool`: Execute one tool returned by `tool_search`.

Workflow:
1. Use `tool_search` when you need an MCP capability that is not already exposed
   as a direct tool.
2. Choose a result by its exact `qualified_name`.
3. Call `call_tool` with that `qualified_name` and an `arguments` object matching
   the returned `args_schema`.

Critical rules:
- Do not call searched tool names directly. Use `call_tool`.
- Do not invent `qualified_name` values.
- If `call_tool` reports an argument error, revise the arguments against the
  `args_schema` returned by `tool_search`.
"""

TOOL_SEARCH_SYSTEM_PROMPT_MARKER = "## MCP Tool Search"


class ToolSearchSchema(BaseModel):
    query: str = Field(description="Natural language search query for the needed tool.")
    limit: int = Field(
        default=8,
        ge=1,
        le=50,
        description="Maximum number of matching tools to return.",
    )


class CallToolSchema(BaseModel):
    qualified_name: str = Field(
        description="Exact qualified_name returned by tool_search, for example 'exa.web_search_exa'."
    )
    arguments: dict[str, Any] = Field(
        default_factory=dict,
        description="Arguments object matching the selected tool's args_schema.",
    )


class ToolSearchMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Expose stable tool_search/call_tool tools over a larger MCP catalog."""

    def __init__(
        self,
        *,
        registry: MCPRegistry,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
        max_results: int = 8,
        system_prompt: str | None = TOOL_SEARCH_SYSTEM_PROMPT,
        ) -> None:
        super().__init__()
        self._registry = registry
        self._server_names = list(server_names or [])
        self._tool_names = list(tool_names or [])
        self._max_results = max(1, min(max_results, 50))
        catalog_summary = (
            self._render_catalog_summary() if system_prompt is not None else ""
        )
        self.system_prompt = _with_catalog_summary(
            system_prompt,
            catalog_summary,
        )

        async def tool_search(query: str, limit: int = self._max_results) -> str:
            """Search the MCP tool catalog."""
            return await self.search(query=query, limit=limit)

        async def call_tool(
            qualified_name: str,
            arguments: dict[str, Any],
            runtime: ToolRuntime = None,  # type: ignore[assignment]
        ) -> str:
            """Execute a tool returned by tool_search."""
            config = getattr(runtime, "config", None) if runtime is not None else None
            return await self.call_tool(
                qualified_name=qualified_name,
                arguments=arguments,
                config=config,
            )

        self.tools = [
            StructuredTool.from_function(
                coroutine=tool_search,
                name="tool_search",
                description=(
                    "Search MCP tools by goal and return matching qualified_name, "
                    "description, and args_schema metadata."
                ),
                infer_schema=False,
                args_schema=ToolSearchSchema,
            ),
            StructuredTool.from_function(
                coroutine=call_tool,
                name="call_tool",
                description=(
                    "Execute one MCP tool returned by tool_search. Pass the exact "
                    "qualified_name and arguments matching that tool's args_schema."
                ),
                infer_schema=False,
                args_schema=CallToolSchema,
            ),
        ]

    async def search(self, *, query: str, limit: int | None = None) -> str:
        results = await self._registry.search_tools(
            query,
            server_names=self._server_names,
            tool_names=self._tool_names,
            limit=limit or self._max_results,
        )
        payload = {
            "tools": [_format_tool_info(tool_info) for tool_info in results],
            "scope": {
                "server_names": self._server_names,
                "tool_names": self._tool_names,
                "requires_qualified_name": True,
            },
            "instruction": (
                "To execute a result, call call_tool with the exact qualified_name "
                "and an arguments object matching args_schema."
            ),
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True)

    async def call_tool(
        self,
        *,
        qualified_name: str,
        arguments: dict[str, Any],
        config: dict[str, Any] | None = None,
    ) -> str:
        if not isinstance(arguments, dict):
            raise TypeError("arguments must be an object")
        self._validate_qualified_name_in_scope(qualified_name)
        self._registry.validate_tool_arguments(qualified_name, arguments)
        tool = await self._registry.get_tool(qualified_name)
        result = await _ainvoke_tool(tool, arguments, config=config)
        return _serialize_tool_result(result)

    def _validate_qualified_name_in_scope(self, qualified_name: str) -> None:
        if "." not in qualified_name:
            raise ValueError(
                "call_tool requires an exact qualified_name returned by tool_search."
            )
        allowed = self._registry.resolve_scope_qualified_names(
            server_names=self._server_names,
            tool_names=self._tool_names,
        )
        if allowed is not None and qualified_name not in allowed:
            raise ValueError(
                f"Tool is outside this agent's tool_search scope: {qualified_name}"
            )

    def _render_catalog_summary(self) -> str:
        source_summaries = self._registry.summarize_tool_sources(
            server_names=self._server_names,
            tool_names=self._tool_names,
        )
        return _render_catalog_summary(source_summaries)

    def wrap_model_call(
        self,
        request: ModelRequest[ContextT],
        handler: Callable[[ModelRequest[ContextT]], ModelResponse[ResponseT]],
    ) -> ModelResponse[ResponseT]:
        if self.system_prompt is None:
            return handler(request)
        return handler(self._modify_request(request))

    async def awrap_model_call(
        self,
        request: ModelRequest[ContextT],
        handler: Callable[
            [ModelRequest[ContextT]], Awaitable[ModelResponse[ResponseT]]
        ],
    ) -> ModelResponse[ResponseT]:
        if self.system_prompt is None:
            return await handler(request)
        return await handler(self._modify_request(request))

    def _modify_request(self, request: ModelRequest[ContextT]) -> ModelRequest[ContextT]:
        if _system_message_contains(request.system_message, TOOL_SEARCH_SYSTEM_PROMPT_MARKER):
            return request
        new_system_message = append_to_system_message(
            request.system_message,
            self.system_prompt,
        )
        return request.override(system_message=new_system_message)


def _format_tool_info(tool_info: ToolInfo) -> dict[str, Any]:
    return {
        "qualified_name": tool_info.qualified_name,
        "server_name": tool_info.server_name,
        "name": tool_info.name,
        "description": tool_info.description,
        "args_schema": tool_info.args_schema,
    }


def _with_catalog_summary(
    system_prompt: str | None,
    catalog_summary: str,
) -> str | None:
    if system_prompt is None:
        return None
    parts = [system_prompt.strip(), catalog_summary.strip()]
    return "\n\n".join(part for part in parts if part)


def _render_catalog_summary(source_summaries: list[ToolSourceSummary]) -> str:
    lines = ["Available MCP tool catalog through `tool_search`:"]
    if not source_summaries:
        lines.append("- None currently available in this agent's tool_search scope.")
    else:
        for source in source_summaries:
            description = source.description or f"{source.tool_count} tools available."
            lines.append(f"- {source.name}: {description}")
    lines.append(
        "Use `tool_search` for exact tool metadata and schema before `call_tool`."
    )
    return "\n".join(lines)


def _system_message_contains(system_message: Any, text: str) -> bool:
    if system_message is None:
        return False
    for block in getattr(system_message, "content_blocks", []):
        if isinstance(block, dict) and text in str(block.get("text", "")):
            return True
    return False


async def _ainvoke_tool(
    tool: BaseTool,
    arguments: dict[str, Any],
    *,
    config: dict[str, Any] | None,
) -> Any:
    ainvoke = getattr(tool, "ainvoke", None)
    if callable(ainvoke):
        return await ainvoke(arguments, config=config)

    invoke = getattr(tool, "invoke", None)
    if callable(invoke):
        return await asyncio.to_thread(invoke, arguments, config=config)

    raise TypeError(f"Tool {getattr(tool, 'name', '<unknown>')} is not invokable")


def _serialize_tool_result(result: Any) -> str:
    if isinstance(result, ToolMessage):
        return _serialize_tool_result(result.content)
    if isinstance(result, str):
        return result
    if isinstance(result, bytes):
        return result.decode("utf-8", errors="replace")
    model_dump = getattr(result, "model_dump", None)
    if callable(model_dump):
        result = model_dump()
    try:
        return json.dumps(result, ensure_ascii=False, default=str)
    except TypeError:
        return str(result)
