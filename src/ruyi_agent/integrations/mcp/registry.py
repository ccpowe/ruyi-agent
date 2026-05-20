from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import re
from typing import Any

from jsonschema import exceptions as jsonschema_exceptions
from jsonschema.validators import validator_for
from langchain_mcp_adapters.client import MultiServerMCPClient

MCP_SERVER_METADATA_KEYS = {"description"}


@dataclass(slots=True)
class ToolInfo:
    server_name: str
    name: str
    qualified_name: str
    description: str
    args_schema: dict[str, Any] | None = None


@dataclass(slots=True)
class ToolSourceSummary:
    name: str
    description: str | None
    tool_count: int


@dataclass(slots=True)
class ServerLoadStatus:
    server_name: str
    ok: bool
    tool_count: int
    error: str | None = None
    refreshed_at: datetime | None = None


@dataclass(slots=True)
class RefreshResult:
    total_servers: int
    success_servers: int
    failed_servers: int
    total_tools: int
    refreshed_at: datetime
    server_statuses: list[ServerLoadStatus]


class MCPRegistry:
    def __init__(self, server_configs: dict[str, dict[str, Any]]) -> None:
        # 为什么有这个初始化：把 MCP 配置和运行期缓存集中收口，避免上层直接依赖底层 client。
        self._server_configs = server_configs
        self._client: MultiServerMCPClient | None = None
        self._tool_inventory_by_server: dict[str, list[ToolInfo]] = {}
        self._tool_info_by_qualified_name: dict[str, ToolInfo] = {}
        self._tool_by_qualified_name: dict[str, Any] = {}
        self._qualified_name_by_raw_name: dict[str, str] = {}
        self._duplicate_raw_names: set[str] = set()
        self._server_statuses: dict[str, ServerLoadStatus] = {}
        self._last_refresh_result: RefreshResult | None = None

    def list_servers(self) -> list[str]:
        # 为什么提供这个接口：让展示层和运行时先知道配置里声明了哪些 MCP server。
        return list(self._server_configs.keys())

    def get_server_statuses(self) -> list[ServerLoadStatus]:
        # 为什么暴露 server 状态：让调用方能区分“工具为空”和“某个 server 刷新失败”。
        self._ensure_refreshed()
        return [
            self._server_statuses[server_name]
            for server_name in self.list_servers()
            if server_name in self._server_statuses
        ]

    async def refresh(self) -> RefreshResult:
        # 为什么显式刷新：把远端拉取、错误收集和缓存重建放在一个确定的入口里，便于控制时机。
        client = MultiServerMCPClient(self._connection_configs())
        refreshed_at = datetime.now()
        tool_inventory_by_server: dict[str, list[ToolInfo]] = {}
        tool_info_by_qualified_name: dict[str, ToolInfo] = {}
        tool_by_qualified_name: dict[str, Any] = {}
        raw_name_to_qualified_names: dict[str, list[str]] = {}
        server_statuses: dict[str, ServerLoadStatus] = {}

        for server_name in self.list_servers():
            try:
                tools = await client.get_tools(server_name=server_name)
                tool_infos: list[ToolInfo] = []

                for tool in tools:
                    qualified_name = self._build_qualified_name(server_name, tool.name)
                    args_schema = self._extract_args_schema(tool)
                    tool_info = ToolInfo(
                        server_name=server_name,
                        name=tool.name,
                        qualified_name=qualified_name,
                        description=getattr(tool, "description", "") or "",
                        args_schema=args_schema,
                    )
                    tool_infos.append(tool_info)
                    tool_info_by_qualified_name[qualified_name] = tool_info
                    tool_by_qualified_name[qualified_name] = tool
                    raw_name_to_qualified_names.setdefault(tool.name, []).append(
                        qualified_name
                    )

                tool_inventory_by_server[server_name] = tool_infos
                server_statuses[server_name] = ServerLoadStatus(
                    server_name=server_name,
                    ok=True,
                    tool_count=len(tool_infos),
                    refreshed_at=refreshed_at,
                )
            except Exception as exc:
                tool_inventory_by_server[server_name] = []
                server_statuses[server_name] = ServerLoadStatus(
                    server_name=server_name,
                    ok=False,
                    tool_count=0,
                    error=str(exc),
                    refreshed_at=refreshed_at,
                )

        qualified_name_by_raw_name: dict[str, str] = {}
        duplicate_raw_names: set[str] = set()
        for raw_name, qualified_names in raw_name_to_qualified_names.items():
            if len(qualified_names) == 1:
                qualified_name_by_raw_name[raw_name] = qualified_names[0]
            else:
                duplicate_raw_names.add(raw_name)

        result = RefreshResult(
            total_servers=len(self._server_configs),
            success_servers=sum(1 for status in server_statuses.values() if status.ok),
            failed_servers=sum(1 for status in server_statuses.values() if not status.ok),
            total_tools=sum(
                len(tool_infos) for tool_infos in tool_inventory_by_server.values()
            ),
            refreshed_at=refreshed_at,
            server_statuses=[
                server_statuses[server_name] for server_name in self.list_servers()
            ],
        )

        self._client = client
        self._tool_inventory_by_server = tool_inventory_by_server
        self._tool_info_by_qualified_name = tool_info_by_qualified_name
        self._tool_by_qualified_name = tool_by_qualified_name
        self._qualified_name_by_raw_name = qualified_name_by_raw_name
        self._duplicate_raw_names = duplicate_raw_names
        self._server_statuses = server_statuses
        self._last_refresh_result = result
        return result

    async def list_tools(self, server_name: str | None = None) -> list[ToolInfo]:
        # 为什么返回 ToolInfo：展示和配置关心的是结构化目录，而不是直接拿可执行 tool 对象。
        self._ensure_refreshed()
        if server_name is None:
            return [
                tool_info
                for name in self.list_servers()
                for tool_info in self._tool_inventory_by_server.get(name, [])
            ]
        self._ensure_known_server(server_name)
        return list(self._tool_inventory_by_server.get(server_name, []))

    async def pick_tools(self, tool_names: list[str]) -> list[Any]:
        # 为什么单独提供按名称选择：让 agent 配置可以精确声明“只要这几个工具”。
        self._ensure_refreshed()
        seen_qualified_names: set[str] = set()
        ordered_qualified_names: list[str] = []

        for tool_name in tool_names:
            qualified_name = self._resolve_tool_name(tool_name)
            if qualified_name in seen_qualified_names:
                continue
            seen_qualified_names.add(qualified_name)
            ordered_qualified_names.append(qualified_name)

        self._validate_injection_name_conflicts(ordered_qualified_names)
        return [
            self._tool_by_qualified_name[qualified_name]
            for qualified_name in ordered_qualified_names
        ]

    async def get_tool(self, tool_name: str) -> Any:
        # 为什么提供单工具入口：tool_search/call_tool 执行器需要按搜索结果精确取回真实工具。
        self._ensure_refreshed()
        qualified_name = self._resolve_tool_name(tool_name)
        if qualified_name not in self._tool_by_qualified_name:
            raise ValueError(f"Unknown tool: {tool_name}")
        return self._tool_by_qualified_name[qualified_name]

    def get_tool_info(self, tool_name: str) -> ToolInfo:
        self._ensure_refreshed()
        qualified_name = self._resolve_tool_name(tool_name)
        if qualified_name not in self._tool_info_by_qualified_name:
            raise ValueError(f"Unknown tool: {tool_name}")
        return self._tool_info_by_qualified_name[qualified_name]

    def validate_tool_arguments(
        self,
        tool_name: str,
        arguments: dict[str, Any],
    ) -> None:
        if not isinstance(arguments, dict):
            raise TypeError("arguments must be an object")

        tool_info = self.get_tool_info(tool_name)
        if tool_info.args_schema is None:
            return

        try:
            validator_cls = validator_for(tool_info.args_schema)
            validator_cls.check_schema(tool_info.args_schema)
            validator_cls(tool_info.args_schema).validate(arguments)
        except jsonschema_exceptions.SchemaError as exc:
            raise ValueError(
                f"Invalid args_schema for tool {tool_info.qualified_name}: {exc.message}"
            ) from exc
        except jsonschema_exceptions.ValidationError as exc:
            raise ValueError(
                f"Invalid arguments for tool {tool_info.qualified_name}: {exc.message}"
            ) from exc

    async def search_tools(
        self,
        query: str,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
        limit: int = 8,
    ) -> list[ToolInfo]:
        # 为什么 registry 直接支持搜索：tool_search 需要基于同一份 MCP 缓存做目录查询，
        # 避免 middleware 复制名称解析、scope 过滤和 schema 读取逻辑。
        self._ensure_refreshed()
        limit = max(1, min(limit, 50))
        candidates = self._filter_tool_inventory(
            server_names=server_names,
            tool_names=tool_names,
        )
        terms = _tokenize_search_query(query)
        if not terms:
            return candidates[:limit]

        scored = [
            (self._score_tool_info(tool_info, terms), index, tool_info)
            for index, tool_info in enumerate(candidates)
        ]
        scored = [item for item in scored if item[0] > 0]
        scored.sort(key=lambda item: (-item[0], item[1]))
        return [tool_info for _, _, tool_info in scored[:limit]]

    def summarize_tool_sources(
        self,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
    ) -> list[ToolSourceSummary]:
        self._ensure_refreshed()
        candidates = self._filter_tool_inventory(
            server_names=server_names,
            tool_names=tool_names,
        )
        tools_by_server: dict[str, list[ToolInfo]] = {}
        for tool_info in candidates:
            tools_by_server.setdefault(tool_info.server_name, []).append(tool_info)

        summaries: list[ToolSourceSummary] = []
        for server_name in self.list_servers():
            tools = tools_by_server.get(server_name)
            if not tools:
                continue
            summaries.append(
                ToolSourceSummary(
                    name=server_name,
                    description=self._summarize_server_tools(server_name, tools),
                    tool_count=len(tools),
                )
            )
        return summaries

    async def resolve_tools(
        self,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
    ) -> list[Any]:
        # 为什么要有组合解析：运行时通常既要整组 server 工具，也要补充少量精确工具。
        self._ensure_refreshed()
        seen_qualified_names: set[str] = set()
        ordered_qualified_names: list[str] = []

        for server_name in server_names or []:
            self._ensure_known_server(server_name)
            for tool_info in self._tool_inventory_by_server.get(server_name, []):
                if tool_info.qualified_name in seen_qualified_names:
                    continue
                seen_qualified_names.add(tool_info.qualified_name)
                ordered_qualified_names.append(tool_info.qualified_name)

        for tool_name in tool_names or []:
            qualified_name = self._resolve_tool_name(tool_name)
            if qualified_name in seen_qualified_names:
                continue
            seen_qualified_names.add(qualified_name)
            ordered_qualified_names.append(qualified_name)

        self._validate_injection_name_conflicts(ordered_qualified_names)
        return [
            self._tool_by_qualified_name[qualified_name]
            for qualified_name in ordered_qualified_names
        ]

    def _filter_tool_inventory(
        self,
        *,
        server_names: list[str] | None,
        tool_names: list[str] | None,
    ) -> list[ToolInfo]:
        allowed_qualified_names = self.resolve_scope_qualified_names(
            server_names=server_names,
            tool_names=tool_names,
        )

        tools = [
            tool_info
            for server_name in self.list_servers()
            for tool_info in self._tool_inventory_by_server.get(server_name, [])
        ]
        if allowed_qualified_names is None:
            return tools
        return [
            tool_info
            for tool_info in tools
            if tool_info.qualified_name in allowed_qualified_names
        ]

    def resolve_scope_qualified_names(
        self,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
    ) -> set[str] | None:
        # None 表示调用方没有设置 scope，保持向后兼容的“全量目录”语义；
        # 空列表表示调用方显式设置了空 allowlist，不能退化成全量可见。
        self._ensure_refreshed()
        if server_names is None and tool_names is None:
            return None

        allowed_qualified_names: set[str] = set()
        for server_name in server_names or []:
            self._ensure_known_server(server_name)
            allowed_qualified_names.update(
                tool_info.qualified_name
                for tool_info in self._tool_inventory_by_server.get(server_name, [])
            )
        for tool_name in tool_names or []:
            allowed_qualified_names.add(self._resolve_tool_name(tool_name))
        return allowed_qualified_names

    def _score_tool_info(self, tool_info: ToolInfo, terms: list[str]) -> int:
        haystack = _build_search_text(tool_info)
        score = 0
        for term in terms:
            if term == tool_info.qualified_name.lower():
                score += 30
            if term == tool_info.name.lower():
                score += 20
            if term in haystack:
                score += 1 + haystack.count(term)
        return score

    def _ensure_refreshed(self) -> None:
        # 为什么先检查刷新状态：避免上层在缓存未建立时读到不完整或误导性的结果。
        if self._last_refresh_result is None:
            raise RuntimeError(
                "MCPRegistry is not loaded. Call `await refresh()` first."
            )

    def _ensure_known_server(self, server_name: str) -> None:
        # 为什么单独校验 server：尽早把配置错误暴露给调用方，而不是在更深层失败。
        if server_name not in self._server_configs:
            raise ValueError(f"Unknown MCP server: {server_name}")

    def _connection_configs(self) -> dict[str, dict[str, Any]]:
        return {
            server_name: {
                key: value
                for key, value in server_config.items()
                if key not in MCP_SERVER_METADATA_KEYS
            }
            for server_name, server_config in self._server_configs.items()
        }

    def _build_qualified_name(self, server_name: str, tool_name: str) -> str:
        # 为什么要构造内部唯一名：跨多个 MCP server 聚合时必须有稳定且不冲突的标识。
        return f"{server_name}.{tool_name}"

    def _extract_args_schema(self, tool: Any) -> dict[str, Any] | None:
        # 为什么抽出参数 schema：后续展示、调试和配置验证都需要统一读取工具入参结构。
        args_schema = getattr(tool, "args_schema", None)
        if isinstance(args_schema, dict):
            return args_schema
        if args_schema is None:
            return None
        schema_method = getattr(args_schema, "model_json_schema", None)
        if callable(schema_method):
            return schema_method()
        schema_method = getattr(args_schema, "schema", None)
        if callable(schema_method):
            return schema_method()
        return None

    def _resolve_tool_name(self, tool_name: str) -> str:
        # 为什么做名称解析：配置层既可能写原始名，也可能写 qualified_name，需要统一落到唯一工具。
        if tool_name in self._tool_by_qualified_name:
            return tool_name
        if tool_name in self._duplicate_raw_names:
            raise ValueError(
                f"Ambiguous tool name: {tool_name}. Use qualified name."
            )
        qualified_name = self._qualified_name_by_raw_name.get(tool_name)
        if qualified_name is None:
            raise ValueError(f"Unknown tool: {tool_name}")
        return qualified_name

    def _summarize_server_tools(
        self, server_name: str, tool_infos: list[ToolInfo]
    ) -> str | None:
        server_config = self._server_configs.get(server_name, {})
        configured_description = server_config.get("description")
        if isinstance(configured_description, str) and configured_description.strip():
            return _one_line(configured_description)

        tool_label = "tool" if len(tool_infos) == 1 else "tools"
        tool_names = [tool_info.name for tool_info in tool_infos[:3]]
        if not tool_names:
            return f"{len(tool_infos)} {tool_label} available."
        suffix = "" if len(tool_infos) <= 3 else ", ..."
        return (
            f"{len(tool_infos)} {tool_label} available. Tool names include: "
            f"{', '.join(tool_names)}{suffix}."
        )

    def _validate_injection_name_conflicts(
        self, qualified_names: list[str]
    ) -> None:
        # 为什么在注入前检查冲突：agent 最终看到的是原始 tool.name，同名工具会让调用语义变得不可靠。
        tool_names_to_qualified_names: dict[str, list[str]] = {}
        for qualified_name in qualified_names:
            tool = self._tool_by_qualified_name[qualified_name]
            raw_name = tool.name
            tool_names_to_qualified_names.setdefault(raw_name, []).append(
                qualified_name
            )

        conflicts = {
            raw_name: qualified_names
            for raw_name, qualified_names in tool_names_to_qualified_names.items()
            if len(set(qualified_names)) > 1
        }
        if not conflicts:
            return

        conflict_descriptions = [
            f"{raw_name}: {', '.join(sorted(set(qualified_names)))}"
            for raw_name, qualified_names in sorted(conflicts.items())
        ]
        raise ValueError(
            "Tool name conflict for agent injection: "
            + "; ".join(conflict_descriptions)
        )


def _tokenize_search_query(query: str) -> list[str]:
    terms = re.findall(r"[\w.-]+", query.lower())
    unique_terms: list[str] = []
    for term in terms:
        if term not in unique_terms:
            unique_terms.append(term)
    return unique_terms


def _build_search_text(tool_info: ToolInfo) -> str:
    parts = [
        tool_info.server_name,
        tool_info.name,
        tool_info.qualified_name,
        tool_info.description,
    ]
    if tool_info.args_schema is not None:
        parts.append(json.dumps(tool_info.args_schema, sort_keys=True, default=str))
    return " ".join(parts).lower()


def _one_line(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().rstrip(".")
