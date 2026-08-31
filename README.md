# Ruyi Agent

Ruyi Agent 是一个面向工程任务的 Python Agent Runtime。它通过统一的运行时和配置，提供 HTTP Gateway、Telegram、Feishu/Lark 入口，以及可委派的本地或远程 Agent、模型 Provider、MCP 工具、人工审批、工作区文件和持久化任务状态。

## 主要能力

- `ruyi` CLI 可启动 Gateway、Telegram、Feishu/Lark，或启动所有已配置的入口。
- FastAPI Gateway 提供 Agent 发现、任务创建与续写、任务消息和事件、审批、附件/产物以及 Team Console。
- TOML 配置支持 local Agent、`remote_ref`、worker、模型 Provider、MCP server、skills 和权限 profile。
- 运行时使用 SQLite 保存任务、checkpoint、路由、审批审计和 channel session 等状态。
- backend 支持 `local` 和 `daytona`；`local` 适合开发和测试，Daytona 可用于隔离执行环境。

## 运行要求

- Python `>=3.13`
- [`uv`](https://docs.astral.sh/uv/)
- 一个已配置的模型 Provider 凭据；starter 配置默认使用 OpenRouter。

## 安装

在源码目录执行：

```bash
uv sync --dev
uv run ruyi --help
```

## 最小初始化和启动

显式指定一个独立的配置目录，避免覆盖仓库中的本地配置：

```bash
RUYI_HOME=/path/to/ruyi-home uv run ruyi --init
```

初始化会生成 `ruyi.toml` 和 `config/` 下的 starter 配置。默认的
`config/agents.toml` 使用 OpenRouter 的 `qwen/qwen3.6-plus`；请在生成的
`ruyi.toml` 的 `[model_credentials]` 中填写 `openrouter_api_key`，或通过
`OPENROUTER_API_KEY` 提供凭据，然后启动 Gateway：

```bash
RUYI_HOME=/path/to/ruyi-home uv run ruyi --gateway
```

Gateway 默认监听 `http://127.0.0.1:8000`。默认 bearer token `dev-token` 只适合
本机调试；对外监听前，必须在 `ruyi.toml` 的 `[gateway]` 中改为强随机 token，
并配置 HTTPS 和网络访问控制。

配置目录的选择顺序是：显式的 `RUYI_HOME`，当前目录中已有配置的
`.ruyi_agent/`，最后是用户目录下的 `~/.ruyi_agent/`。执行时可用
`--workspace PATH` 指定 Agent 工作区。

## 配置速览

- `ruyi.toml`：backend、Gateway、存储路径和 channel 凭据。
- `config/agents.toml`：主 Agent、local/remote Agent、worker、模型、MCP/工具和 skills 范围。
- `config/llm_providers.toml`：Provider 类型、地址和凭据环境变量名。
- `config/mcp_servers.toml`：MCP server 连接信息。
- `config/permissions.toml`：工具和 shell 命令的权限策略。

启用 Telegram 时配置 `[channels.telegram].bot_token`；启用 Feishu/Lark 时配置
`[channels.feishu].app_id` 和 `app_secret`。对应入口命令是：

```bash
uv run ruyi --telegram
uv run ruyi --feishu
uv run ruyi --all
```

`--all` 会跳过缺少必要凭据的 Telegram/Feishu 入口，但仍启动 Gateway。

## 常用命令和代码入口

```bash
uv run pytest
uv run pytest tests/unit
uv run pytest tests/integration
uv run ruff check src tests scripts
uv build
```

- [CLI 入口](src/ruyi_agent/entrypoints/main.py)
- [运行时配置](src/ruyi_agent/config/runtime_settings.py)
- [配置加载与模型](src/ruyi_agent/config/loader.py)
- [Gateway HTTP 路由](src/ruyi_agent/channels/http/)
- [starter 配置模板](src/ruyi_agent/templates/ruyi_home/)
- [测试](tests/)
- [文档目录](docs/)

## License

MIT License，见 [LICENSE](LICENSE)。
