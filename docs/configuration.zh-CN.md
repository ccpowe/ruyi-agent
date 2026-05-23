# 配置指南

Ruyi Agent 的配置统一放在 `.ruyi_agent/`：

- `.ruyi_agent/ruyi.toml`：运行参数、凭据、backend、storage、Gateway、Telegram、Feishu。
- `.ruyi_agent/config/agents.toml`：agent、worker、模型、MCP 工具、权限 profile、委派关系。
- `.ruyi_agent/config/llm_providers.toml`：模型供应商和 API key 名称映射。
- `.ruyi_agent/config/mcp_servers.toml`：MCP server 声明。
- `.ruyi_agent/config/permissions.toml`：工具调用、shell 命令和 HITL 审批策略。

源码开发时，当前项目下存在有效 `.ruyi_agent/` 配置就优先使用它；只有 `.ruyi_agent/runtime/` 这类运行态目录不会被当成配置根。uv tool 安装后，通常使用用户目录下的 `~/.ruyi_agent/`。`uv tool install` 不会执行项目代码，普通启动命令也不会自动生成配置模板；首次使用前需要运行 `ruyi --init` 显式创建配置目录。如果旧安装留下了空白或损坏的 starter config，可运行 `ruyi --init --force` 用当前包内模板覆盖生成文件。TUI 默认 workspace 是当前目录，显式 `ruyi --workspace PATH` 会覆盖 `ruyi.toml` 里的 backend workspace。

## 运行配置

`.ruyi_agent/ruyi.toml` 的最小配置通常只需要填模型或 channel 凭据：

```toml
[model_credentials]
openrouter_api_key = "<your-openrouter-api-key>"

[channels.telegram]
bot_token = "<your-telegram-bot-token>"

[channels.feishu]
app_id = "<your-feishu-app-id>"
app_secret = "<your-feishu-app-secret>"
```

TOML 不支持未加引号的 URL 或 Windows 路径；这类值要写成字符串，例如：

```toml
[backend.daytona]
api_key = "<your-daytona-api-key>"
api_url = "https://daytona.example/api"
target = "us"
sandbox_name = "learn-deepagents"

[backend]
workspace = "C:/Users/name/project"
```

常用字段：

| 字段 | 说明 |
| --- | --- |
| `backend.kind` | 工具执行 backend。开发环境建议 `local`，隔离环境可用 `daytona`。 |
| `backend.workspace` | `local` backend 暴露给 agent 的工作目录。空值表示启动命令的当前目录。 |
| `backend.local.timeout` | 本地命令执行超时秒数。 |
| `backend.daytona.api_key` / `api_url` / `target` / `sandbox_name` | Daytona backend 配置。也兼容同表内 `DAYTONA_API_KEY` 等 env 风格键名，但推荐使用小写字段。 |
| `gateway.host` / `gateway.port` | Gateway 监听地址和端口。 |
| `gateway.base_url` | Telegram / Feishu adapter 调用 Gateway 的地址。 |
| `gateway.bearer_token` | Gateway HTTP API 认证 token。本地默认 `dev-token`；对外暴露前必须改强 token。 |
| `storage.*` | SQLite 状态文件路径。相对路径按 `.ruyi_agent/` 解析。 |
| `runtime.max_delegation_depth` | 每个 root task 下最大委派深度。 |
| `runtime.max_tasks_per_root` | 每个 root task 下最多创建的 subagent task 数量。 |
| `channels.telegram.bot_token` | Telegram channel 必填。 |
| `channels.feishu.app_id` / `channels.feishu.app_secret` | Feishu/Lark channel 必填。 |

## Agent 配置

`.ruyi_agent/config/agents.toml` 定义所有本地 agent 和远端 agent 引用：

```toml
main_agent = "main"

[agents.main]
kind = "local"
public = true
name = "main"
description = "General entry agent that can delegate focused research tasks."
system_prompt = "You are a practical engineering assistant. Be concise, accurate, and explicit about assumptions."
provider = "openrouter"
model = "qwen/qwen3.6-plus"
memory = []
skills = "inherit"
server_names = []
tool_names = []
workers = ["background_research"]
permission_profile = "standard"
```

关键字段：

| 字段 | 说明 |
| --- | --- |
| `kind` | `local` 表示本地可执行 agent；`remote_ref` 表示远端 Gateway 暴露的 agent。 |
| `public` | 是否通过 Gateway `/agents` 暴露给外部调用。入口 agent 通常设为 `true`。 |
| `provider` | 引用 `.ruyi_agent/config/llm_providers.toml` 中的 provider 名称。 |
| `model` | 传给 provider 的模型名。 |
| `server_names` | 注入该 agent 的 MCP server 名称列表。 |
| `tool_names` | 只注入指定工具；留空表示使用 server scope 下可用工具。 |
| `workers` | 当前 agent 可以委派的 local worker 或 remote_ref 名称。 |
| `permission_profile` | 引用 `.ruyi_agent/config/permissions.toml` 中的权限 profile。 |
| `memory` | 映射到 backend workspace 的 memory 路径。starter 默认留空。 |
| `skills` | 控制 agent 可见的 skill 名称。可设为 `"inherit"`、`"none"` 或 `["skill-name"]`。 |

## Skills

Ruyi 从固定目录扫描 skills，并在创建任务时按 agent 配置同步成 backend 内部 skill view。扫描目录按优先级从高到低为：

```text
workspace/.agents/skills
~/.agents/skills
~/.ruyi_agent/skills
```

`skills` 字段现在表示可见性，不再表示目录：

```toml
skills = ["repo-workflow", "frontend"]
skills = "inherit"
skills = "none"
```

## Provider 配置

`.ruyi_agent/config/llm_providers.toml` 把 provider 名称映射到模型初始化参数：

```toml
[providers.openrouter]
kind = "openrouter"
api_key_env = "OPENROUTER_API_KEY"
```

`api_key_env` 是 provider 内部引用名；用户把真实值填在 `.ruyi_agent/ruyi.toml` 的 `[model_credentials]` 中。OpenAI Codex OAuth provider 可用于本机实验，认证文件默认保存到 `~/.ruyi_agent/openai_codex_auth.json`。

## MCP Server 配置

`.ruyi_agent/config/mcp_servers.toml` 声明可用 MCP server：

```toml
[mcp_servers.deepwiki]
transport = "http"
url = "https://mcp.deepwiki.com/mcp"
description = "Answer questions about GitHub repositories using DeepWiki."
```

Agent 通过 `server_names = ["deepwiki"]` 选择要注入的 server。

## 权限配置

`.ruyi_agent/config/permissions.toml` 定义工具和命令权限。默认 profile 是：

```toml
default_profile = "standard"
```

`standard` 更适合开源默认值：读文件、搜索、列目录默认允许；写文件、编辑文件、执行 shell、调用外部 MCP 工具默认需要 HITL 审批；高风险命令如 `rm`、`git reset --hard`、`git clean` 默认拒绝。

`yolo` 适合可信本地实验，不建议作为公开服务默认权限。

## 常见启动组合

TUI 本地调试：

```bash
ruyi
ruyi --tui
ruyi --workspace /path/to/workspace
```

Gateway：

```bash
ruyi --gateway
```

Telegram adapter 与 Gateway：

```bash
ruyi --telegram
```

Feishu adapter 与 Gateway：

```bash
ruyi --feishu
```

所有已配置的非 TUI channel：

```bash
ruyi --all
```
