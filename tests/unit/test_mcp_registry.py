from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

import pytest

import ruyi_agent.integrations.mcp.registry as mcp_registry


@dataclass(slots=True)
class FakeTool:
    name: str
    description: str = ""
    args_schema: object | None = None


class FakeArgsSchema:
    def __init__(self, payload: dict[str, object]) -> None:
        # 为什么保留这个假对象：用最小代价覆盖 registry 对 Pydantic 风格 schema 的兼容路径。
        self._payload = payload

    def model_json_schema(self) -> dict[str, object]:
        # 为什么暴露这个方法：模拟真实工具对象常见的 schema 读取入口。
        return self._payload


class FakeMCPClient:
    def __init__(
        self,
        server_configs: dict[str, dict[str, object]],
        responses: dict[str, object],
    ) -> None:
        # 为什么用假 client：测试只验证 registry 逻辑，不依赖真实网络和外部 MCP server。
        self.server_configs = server_configs
        self.responses = responses
        self.calls: list[str] = []

    async def get_tools(self, server_name: str | None = None):
        # 为什么记录调用：需要确认 refresh 是按 server 单独拉取，而不是一次全量请求。
        assert server_name is not None
        self.calls.append(server_name)
        response = self.responses[server_name]
        if isinstance(response, Exception):
            raise response
        return response


def build_registry(
    monkeypatch: pytest.MonkeyPatch,
    responses: dict[str, object],
) -> tuple[mcp_registry.MCPRegistry, dict[str, FakeMCPClient]]:
    # 为什么封装这个构造器：让每个测试专注断言行为，而不是重复搭建 monkeypatch 环境。
    created: dict[str, FakeMCPClient] = {}

    def factory(server_configs: dict[str, dict[str, object]]) -> FakeMCPClient:
        # 为什么拦截 client 工厂：把 registry 的外部依赖替换成可控的测试替身。
        client = FakeMCPClient(server_configs, responses)
        created["client"] = client
        return client

    monkeypatch.setattr(mcp_registry, "MultiServerMCPClient", factory)

    registry = mcp_registry.MCPRegistry(
        {
            "exa": {"transport": "http", "url": "https://exa.invalid/mcp"},
            "deepwiki": {"transport": "http", "url": "https://deepwiki.invalid/mcp"},
        }
    )
    return registry, created


def test_list_servers_returns_configured_servers_in_order() -> None:
    # 为什么测这个：确认最基础的配置读取不会被后续缓存或刷新逻辑污染。
    registry = mcp_registry.MCPRegistry(
        {
            "exa": {"transport": "http"},
            "deepwiki": {"transport": "http"},
        }
    )

    assert registry.list_servers() == ["exa", "deepwiki"]


def test_pre_refresh_access_raises_runtime_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测预刷新错误：避免调用方在未建立缓存时误以为 registry 可用。
    registry, _ = build_registry(monkeypatch, responses={})

    with pytest.raises(RuntimeError, match="Call `await refresh\\(\\)` first"):
        asyncio.run(registry.list_tools())

    with pytest.raises(RuntimeError, match="Call `await refresh\\(\\)` first"):
        asyncio.run(registry.pick_tools(["web_search_exa"]))

    with pytest.raises(RuntimeError, match="Call `await refresh\\(\\)` first"):
        asyncio.run(registry.resolve_tools(server_names=["exa"]))

    with pytest.raises(RuntimeError, match="Call `await refresh\\(\\)` first"):
        registry.get_server_statuses()


def test_refresh_populates_statuses_and_inventory(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测刷新结果：这是 registry 最核心的职责，既要收工具，也要沉淀 server 状态。
    responses = {
        "exa": [
            FakeTool(
                name="web_search_exa",
                description="Search the web",
                args_schema={"type": "object", "properties": {"query": {"type": "string"}}},
            ),
            FakeTool(
                name="web_fetch_exa",
                description="Fetch a page",
                args_schema=FakeArgsSchema({"type": "object", "title": "FetchArgs"}),
            ),
        ],
        "deepwiki": RuntimeError("deepwiki unavailable"),
    }
    registry, created = build_registry(monkeypatch, responses)

    result = asyncio.run(registry.refresh())
    client = created["client"]

    assert client.calls == ["exa", "deepwiki"]
    assert result.total_servers == 2
    assert result.success_servers == 1
    assert result.failed_servers == 1
    assert result.total_tools == 2
    assert [status.server_name for status in result.server_statuses] == [
        "exa",
        "deepwiki",
    ]

    exa_status, deepwiki_status = asyncio.run(_get_statuses(registry))
    assert exa_status.ok is True
    assert exa_status.tool_count == 2
    assert exa_status.error is None
    assert deepwiki_status.ok is False
    assert deepwiki_status.tool_count == 0
    assert "deepwiki unavailable" in (deepwiki_status.error or "")

    tools = asyncio.run(registry.list_tools())
    assert [tool.qualified_name for tool in tools] == [
        "exa.web_search_exa",
        "exa.web_fetch_exa",
    ]
    assert tools[0].args_schema == {
        "type": "object",
        "properties": {"query": {"type": "string"}},
    }
    assert tools[1].args_schema == {"type": "object", "title": "FetchArgs"}


def test_tool_name_resolution_supports_raw_and_qualified_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测两种名称：配置层未来可能同时使用裸名和 qualified_name。
    responses = {
        "exa": [FakeTool(name="web_search_exa", description="Search the web")],
        "deepwiki": [FakeTool(name="ask_question", description="Ask about a repo")],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    raw_tool = asyncio.run(registry.pick_tools(["web_search_exa"]))[0]
    qualified_tool = asyncio.run(registry.pick_tools(["exa.web_search_exa"]))[0]

    assert raw_tool is qualified_tool
    assert raw_tool.name == "web_search_exa"
    assert raw_tool.description == "Search the web"


def test_duplicate_raw_name_requires_qualified_name(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测裸名歧义：跨 server 聚合后，同名工具必须强制用户写清楚来源。
    responses = {
        "exa": [FakeTool(name="search", description="Search exa")],
        "deepwiki": [FakeTool(name="search", description="Search deepwiki")],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    with pytest.raises(ValueError, match="Ambiguous tool name: search"):
        asyncio.run(registry.pick_tools(["search"]))


def test_resolve_tools_merges_server_and_tool_names_without_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测组合解析：运行时最常见的场景就是“整组 + 少量补充工具”。
    responses = {
        "exa": [
            FakeTool(name="web_search_exa", description="Search the web"),
            FakeTool(name="web_fetch_exa", description="Fetch a page"),
        ],
        "deepwiki": [FakeTool(name="ask_question", description="Ask about a repo")],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    tools = asyncio.run(
        registry.resolve_tools(
            server_names=["exa"],
            tool_names=["deepwiki.ask_question", "exa.web_search_exa"],
        )
    )

    assert [tool.name for tool in tools] == [
        "web_search_exa",
        "web_fetch_exa",
        "ask_question",
    ]


def test_search_tools_returns_ranked_scoped_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [
            FakeTool(name="web_search_exa", description="Search public web pages"),
            FakeTool(name="web_fetch_exa", description="Fetch a page by URL"),
        ],
        "deepwiki": [
            FakeTool(name="ask_question", description="Ask about GitHub repositories")
        ],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    tools = asyncio.run(registry.search_tools("github repository question", limit=2))

    assert [tool.qualified_name for tool in tools] == ["deepwiki.ask_question"]

    scoped_tools = asyncio.run(
        registry.search_tools(
            "search",
            server_names=["exa"],
            limit=5,
        )
    )

    assert [tool.qualified_name for tool in scoped_tools] == ["exa.web_search_exa"]


def test_search_tools_handles_unicode_query(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [FakeTool(name="web_search_exa", description="Search public web pages")],
        "deepwiki": [FakeTool(name="get_weather", description="查询城市天气")],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    tools = asyncio.run(registry.search_tools("城市天气", limit=5))

    assert [tool.qualified_name for tool in tools] == ["deepwiki.get_weather"]


def test_search_tools_treats_empty_scope_as_empty_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [FakeTool(name="web_search_exa", description="Search public web pages")],
        "deepwiki": [FakeTool(name="ask_question", description="Ask repos")],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    all_tools = asyncio.run(registry.search_tools("search"))
    empty_scoped_tools = asyncio.run(
        registry.search_tools(
            "search",
            server_names=[],
            tool_names=[],
        )
    )

    assert [tool.qualified_name for tool in all_tools] == ["exa.web_search_exa"]
    assert empty_scoped_tools == []


def test_summarize_tool_sources_returns_scoped_light_catalog(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [
            FakeTool(name="web_search_exa", description="Search public web pages"),
            FakeTool(name="web_fetch_exa", description="Fetch a page by URL"),
        ],
        "deepwiki": [
            FakeTool(name="ask_question", description="Ask about GitHub repositories")
        ],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    summaries = registry.summarize_tool_sources(server_names=["exa"])

    assert [(summary.name, summary.tool_count) for summary in summaries] == [("exa", 2)]
    assert summaries[0].description == (
        "2 tools available. Tool names include: web_search_exa, web_fetch_exa."
    )
    assert "args_schema" not in summaries[0].description


def test_summarize_tool_sources_does_not_echo_tool_description_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [
            FakeTool(
                name="web_search_exa",
                description='Search. Schema: {"type": "object", "properties": {}}',
            )
        ],
        "deepwiki": [],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    summaries = registry.summarize_tool_sources(server_names=["exa"])

    assert summaries[0].description == (
        "1 tool available. Tool names include: web_search_exa."
    )
    assert "Schema" not in summaries[0].description
    assert "{" not in summaries[0].description


def test_summarize_tool_sources_prefers_configured_server_description(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [FakeTool(name="web_search_exa", description="Tool description")],
        "deepwiki": [],
    }
    registry, created = build_registry(monkeypatch, responses)
    registry._server_configs["exa"]["description"] = "Search and fetch web content."

    asyncio.run(registry.refresh())

    client = created["client"]
    summaries = registry.summarize_tool_sources(server_names=["exa"])

    assert "description" not in client.server_configs["exa"]
    assert summaries[0].description == "Search and fetch web content"


def test_validate_tool_arguments_checks_json_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [
            FakeTool(
                name="web_search_exa",
                description="Search public web pages",
                args_schema={
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
            )
        ],
        "deepwiki": [],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    registry.validate_tool_arguments("exa.web_search_exa", {"query": "LangChain"})

    with pytest.raises(ValueError, match="Invalid arguments"):
        registry.validate_tool_arguments("exa.web_search_exa", {})

    with pytest.raises(ValueError, match="Invalid arguments"):
        registry.validate_tool_arguments("exa.web_search_exa", {"query": 3})


def test_resolve_scope_qualified_names_preserves_none_vs_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "exa": [FakeTool(name="web_search_exa", description="Search public web pages")],
        "deepwiki": [],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    assert registry.resolve_scope_qualified_names() is None
    assert registry.resolve_scope_qualified_names(server_names=[], tool_names=[]) == set()
    assert registry.resolve_scope_qualified_names(server_names=["exa"]) == {
        "exa.web_search_exa"
    }


def test_get_tool_resolves_qualified_name(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = {
        "exa": [FakeTool(name="web_search_exa", description="Search public web pages")],
        "deepwiki": [],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    tool = asyncio.run(registry.get_tool("exa.web_search_exa"))

    assert tool.name == "web_search_exa"


def test_injection_name_conflict_is_reported_for_cross_server_same_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测注入冲突：内部能区分 qualified_name，不代表 agent 运行时能安全接收同名工具。
    responses = {
        "exa": [FakeTool(name="search", description="Search exa")],
        "deepwiki": [FakeTool(name="search", description="Search deepwiki")],
    }
    registry, _ = build_registry(monkeypatch, responses)

    asyncio.run(registry.refresh())

    with pytest.raises(ValueError, match="Tool name conflict for agent injection"):
        asyncio.run(registry.resolve_tools(server_names=["exa", "deepwiki"]))


async def _get_statuses(
    registry: mcp_registry.MCPRegistry,
) -> list[mcp_registry.ServerLoadStatus]:
    # 为什么保留这个小辅助：让异步接口的断言表达保持简洁，测试重点留给行为本身。
    return registry.get_server_statuses()
