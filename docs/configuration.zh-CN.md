# 配置指南

Ruyi Agent 的运行配置分为四层：

- `.env`：运行环境、密钥、数据库路径、Gateway/Telegram/Feishu 启动参数。
- `config/agents.toml`：agent、worker、模型、MCP 工具、权限 profile、委派关系。
- `config/llm_providers.toml`：模型供应商和 API key 环境变量映射。
- `config/permissions.toml`：工具调用、shell 命令和 HITL 审批策略。

## 环境变量

先复制模板：

```bash
cp .env.example .env
```

默认 `config/agents.toml` 使用 OpenRouter，因此最小本地 Gateway/TUI 运行通常只需要：

```bash
OPENROUTER_API_KEY="<your-openrouter-api-key>"
```

Telegram 额外需要：

```bash
TELEGRAM_BOT_TOKEN="<your-telegram-bot-token>"
```

Feishu/Lark 额外需要：

```bash
FEISHU_APP_ID="<your-feishu-app-id>"
FEISHU_APP_SECRET="<your-feishu-app-secret>"
```

常用变量：

| 变量 | 说明 |
| --- | --- |
| `APP_MODE` | 不传命令参数时的默认模式。建议默认 `gateway`。 |
| `AGENT_NODE_ID` | 当前节点稳定 ID，用于远端委派环路检测。单节点本地默认 `local-dev`。 |
| `BACKEND_KIND` | 工具执行 backend。开发环境建议 `local`，隔离环境可用 `daytona`。 |
| `LOCAL_BACKEND_ROOT` | `BACKEND_KIND=local` 时暴露给 agent 的工作目录。留空表示当前目录。 |
| `OPENROUTER_API_KEY` | `provider = "openrouter"` 时使用。 |
| `KIMI_API_KEY` | `provider = "kimi"` 时使用。 |
| `GATEWAY_BEARER_TOKEN` | Gateway HTTP API 认证 token。本地默认 `dev-token`；不要使用默认值对外暴露服务。 |
| `GATEWAY_BASE_URL` | Telegram / Feishu adapter 调用 Gateway 的地址。 |
| `CHECKPOINT_DB` | LangGraph checkpoint SQLite 路径，默认 `data/checkpoints.sqlite`。 |
| `TASK_DB` | async task 状态 SQLite 路径，默认 `data/tasks.sqlite`。 |
| `REVIEW_AUDIT_DB` | HITL 审批审计 SQLite 路径，默认 `data/review_audit.sqlite`。 |
| `AGENT_MAX_DELEGATION_DEPTH` | 每个 root task 下最大委派深度。 |
| `AGENT_MAX_TASKS_PER_ROOT` | 每个 root task 下最多创建的 subagent task 数量。 |
| `TELEGRAM_BOT_TOKEN` | Telegram mode 必填。 |
| `FEISHU_APP_ID` / `FEISHU_APP_SECRET` | Feishu mode 必填。 |

完整变量说明见 `.env.example`。

## Agent 配置

`config/agents.toml` 定义所有本地 agent 和远端 agent 引用。仓库默认配置是一个安全 starter：

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
skills = []
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
| `provider` | 引用 `config/llm_providers.toml` 中的 provider 名称。 |
| `model` | 传给 provider 的模型名。 |
| `server_names` | 注入该 agent 的 MCP server 名称列表。 |
| `tool_names` | 只注入指定工具；留空表示使用 server scope 下可用工具。 |
| `workers` | 当前 agent 可以委派的 local worker 或 remote_ref 名称。 |
| `permission_profile` | 引用 `config/permissions.toml` 中的权限 profile。 |
| `memory` / `skills` | 映射到 backend workspace 的 memory 和 skill 路径。starter 默认留空。 |

一个带 MCP 搜索 worker 的例子：

```toml
[agents.news_research]
kind = "local"
public = false
name = "news_research"
description = "Searches web content and summarizes findings."
system_prompt = "You are a background research worker. Summarize findings with sources."
provider = "openrouter"
model = "qwen/qwen3.6-plus"
memory = []
skills = []
server_names = ["exa"]
tool_names = []
workers = []
permission_profile = "standard"
```

远端 Gateway 引用例子：

```toml
[agents.remote_research]
kind = "remote_ref"
public = false
name = "remote_research"
description = "Remote research worker exposed by another ruyi-agent Gateway."
url = "https://example.com"
remote_agent_name = "background_research"

[agents.remote_research.auth]
type = "bearer"
token_env = "REMOTE_RESEARCH_TOKEN"
```

## Provider 配置

`config/llm_providers.toml` 把 provider 名称映射到模型初始化参数：

```toml
[providers.openrouter]
kind = "openrouter"
api_key_env = "OPENROUTER_API_KEY"
```

Agent 通过 `provider = "openrouter"` 引用它，实际密钥从 `.env` 的 `OPENROUTER_API_KEY` 读取。

OpenAI Codex OAuth provider 可用于本机实验：

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json \
  --list-models \
  --live-response
```

然后在 agent 中使用：

```toml
provider = "openai_codex"
model = "gpt-5.3-codex"
```

## MCP Server 配置

`config/mcp_servers.toml` 声明可用 MCP server：

```toml
[mcp_servers.deepwiki]
transport = "http"
url = "https://mcp.deepwiki.com/mcp"
description = "Answer questions about GitHub repositories using DeepWiki."
```

Agent 通过 `server_names = ["deepwiki"]` 选择要注入的 server。

## 权限配置

`config/permissions.toml` 定义工具和命令权限。默认 profile 是：

```toml
default_profile = "standard"
```

`standard` 更适合开源默认值：

- 读文件、搜索、列目录默认允许。
- 写文件、编辑文件、执行 shell、调用外部 MCP 工具默认需要 HITL 审批。
- 高风险命令如 `rm`、`git reset --hard`、`git clean` 默认拒绝。

`yolo` 适合可信本地实验，不建议作为公开服务默认权限：

- 大部分工具直接允许。
- 高影响操作仍按 risk review 或 deny 规则拦截。

Agent 可以显式选择 profile：

```toml
[agents.main]
permission_profile = "standard"
```

## 常见启动组合

Gateway：

```bash
uv run python main.py gateway
```

Telegram adapter：

```bash
uv run python main.py telegram
```

Feishu adapter：

```bash
uv run python main.py feishu
```

TUI 本地调试：

```bash
uv run python main.py tui
```

Telegram 和 Feishu adapter 都依赖 Gateway，因此通常需要先启动 Gateway，再启动对应 adapter。
