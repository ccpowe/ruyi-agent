# Ruyi Agent

`ruyi-agent` 是一个面向工程化场景的 Agent Runtime。它把 TUI、HTTP Gateway、Telegram Bot、Feishu/Lark Bot 接到同一套任务控制面，支持多 Agent 委派、MCP 工具、skills、HITL 审批、SQLite 状态持久化、附件上传和任务产物下载。

项目仍在快速演进阶段，适合研究、二次开发和小规模自托管验证。生产环境使用前请重点检查权限策略、Gateway 暴露方式、backend 隔离和密钥管理。

## Demos

![Ruyi Agent 简短演示](docs/简短演示.gif)

- [PPT generation demo](https://github.com/ccpowe/ruyi-agent/releases/download/%E6%BC%94%E7%A4%BA/ruyi_ppt.mp4)：通过 agent workflow 生成演示 PPT。
- [Snake web app demo](https://github.com/ccpowe/ruyi-agent/releases/download/%E6%BC%94%E7%A4%BA/ruyi_snake.mp4)：通过 agent workflow 生成并运行 Web 贪吃蛇应用。

## 核心能力

- 多入口接入：本地 TUI、FastAPI Gateway、Telegram、Feishu/Lark。
- 多 Agent 委派：支持本地 worker 和远端 `remote_ref`，统一通过 task 控制面调度。
- MCP 工具治理：支持多 MCP server、工具搜索、schema 校验、按 agent scope 注入。
- Skills：按 agent 配置控制 skill 可见性，并同步到 backend 内部 skill view。
- HITL 审批：把 root agent 和 worker/subagent 的审批请求统一投影为 review 资源。
- 权限策略：用 `permissions.toml` 声明工具白名单、shell 命令规则、审批策略和拒绝策略。
- 任务状态：基于 SQLite 保存 task、route、checkpoint、review audit、channel session 等状态。
- 文件流转：Gateway 支持附件上传，任务可发布 artifact，HTTP/Telegram/Feishu 可返回产物文件。
- Backend 隔离：支持 `local` 和 `daytona` backend；生产或高风险任务建议使用隔离 backend。

## 安装

前置要求：

- Python `>=3.13`
- [`uv`](https://docs.astral.sh/uv/)

推荐把 Ruyi 安装成 `uv tool`。安装后会得到 `ruyi` 命令。

```bash
uv tool install https://github.com/ccpowe/ruyi-agent/releases/download/v0.1.0/ruyi_agent-0.1.0-py3-none-any.whl
```

如果使用 Python package index 发布版，也可以直接用包名安装：

```bash
uv tool install ruyi-agent
```

如果希望直接跟随 GitHub 源码版本：

```bash
uv tool install git+https://github.com/ccpowe/ruyi-agent.git
```

本地开发使用源码环境：

```bash
uv sync
uv run ruyi --init
uv run ruyi
```

也可以把当前源码目录以可编辑 tool 安装：

```bash
uv tool install --editable .
```

## 初始化

首次使用需要生成运行配置：

```bash
ruyi --init
```

如果需要用当前包内模板覆盖已生成的 starter config：

```bash
ruyi --init --force
```

`--force` 会覆盖生成文件。真实密钥和自定义 agent 配置也可能被覆盖，执行前先确认不需要保留。

Ruyi 的配置目录选择规则：

- 如果设置了 `RUYI_HOME`，使用 `RUYI_HOME`。
- 否则，如果当前目录存在 `.ruyi_agent/ruyi.toml` 或 `.ruyi_agent/config/`，使用当前项目的 `.ruyi_agent/`。
- 否则，使用 `~/.ruyi_agent/`。

## 配置

主要配置文件：

- `ruyi.toml`：运行参数、凭据、Gateway、channel、backend、storage 设置。
- `config/agents.toml`：agent、worker、remote_ref、模型和权限 profile。
- `config/llm_providers.toml`：模型 provider 声明。
- `config/mcp_servers.toml`：MCP server 声明。
- `config/permissions.toml`：工具和 shell 命令权限策略。

starter config 默认使用 OpenRouter provider 和 `qwen/qwen3.6-plus` 模型。运行 TUI 或 Gateway 时，最小配置是填入模型 key：

```toml
[model_credentials]
openrouter_api_key = "<your-openrouter-api-key>"
```

如果你在 `config/agents.toml` 中改用了 DeepSeek、Kimi、Z.AI、OpenAI 或 Anthropic，则改填对应字段：

```toml
[model_credentials]
deepseek_api_key = "<your-deepseek-api-key>"
kimi_api_key = "<your-kimi-api-key>"
zai_api_key = "<your-zai-api-key>"
openai_api_key = "<your-openai-api-key>"
anthropic_api_key = "<your-anthropic-api-key>"
```

Telegram 模式还需要：

```toml
[channels.telegram]
bot_token = "<your-telegram-bot-token>"
default_agent = "main"
```

Feishu/Lark 模式还需要：

```toml
[channels.feishu]
app_id = "<your-feishu-app-id>"
app_secret = "<your-feishu-app-secret>"
domain = "feishu"
default_agent = "main"
```

`domain = "feishu"` 对应飞书中国站；国际版 Lark 使用 `domain = "lark"`。群聊默认关闭，如需群聊可设置 `group_policy = "open"`，并保持 `require_mention = true` 或配置访问白名单。

Gateway 默认 token 是 `dev-token`，只适合本地调试。对外暴露 Gateway 前必须改成强随机 token，并放在 TLS、反向代理和网络访问控制之后。

详细配置见 [配置指南](docs/configuration.zh-CN.md)。

## 使用

本地 TUI：

```bash
ruyi
ruyi --tui
```

指定工作区：

```bash
ruyi --workspace /path/to/workspace
```

启动 Gateway：

```bash
ruyi --gateway
```

默认监听：

```text
http://127.0.0.1:8000
```

启动 Telegram：

```bash
ruyi --telegram
```

启动 Feishu/Lark：

```bash
ruyi --feishu
```

启动所有已配置的非 TUI channel：

```bash
ruyi --all
```

`--all` 会自动跳过缺少必要凭据的 Telegram/Feishu channel。

## Gateway API

所有 Gateway API 都需要 Bearer Token：

```bash
export GATEWAY_BEARER_TOKEN=dev-token
```

列出 public agent：

```bash
curl -H "Authorization: Bearer $GATEWAY_BEARER_TOKEN" \
  http://127.0.0.1:8000/agents
```

创建任务：

```bash
curl -X POST \
  -H "Authorization: Bearer $GATEWAY_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  http://127.0.0.1:8000/agents/main/tasks \
  -d '{
    "input": {
      "content": "分析这个项目的 Agent Runtime 架构，并总结核心模块。"
    },
    "metadata": {
      "source": "readme-example"
    }
  }'
```

查询任务：

```bash
curl -H "Authorization: Bearer $GATEWAY_BEARER_TOKEN" \
  http://127.0.0.1:8000/tasks/{task_id}
```

提交 HITL 审批：

```bash
curl -X POST \
  -H "Authorization: Bearer $GATEWAY_BEARER_TOKEN" \
  -H "Content-Type: application/json" \
  http://127.0.0.1:8000/tasks/{task_id}/reviews/{review_id}/decision \
  -d '{
    "decisions": [
      { "type": "approve" }
    ]
  }'
```

下载任务发布的 artifact：

```bash
curl -L \
  -H "Authorization: Bearer $GATEWAY_BEARER_TOKEN" \
  -o artifact.out \
  http://127.0.0.1:8000/tasks/{task_id}/artifacts/{artifact_id}/download
```

## Skills

Ruyi 会从固定目录扫描 skills，并在创建任务时按 agent 配置同步成 backend 内部 skill view。扫描目录按优先级从高到低为：

```text
workspace/.agents/skills
~/.agents/skills
~/.ruyi_agent/skills
```

`config/agents.toml` 中的 `skills` 字段表示可见性：

```toml
skills = ["repo-workflow", "frontend"]  # 只允许这些 skill name
skills = "inherit"                      # 继承父任务 effective skills；根任务使用扫描到的 skills
skills = "none"                         # 禁用 skills
```

## 模型 Provider

OpenRouter、DeepSeek、Kimi/Moonshot、LiteLLM/Z.AI、OpenAI、Anthropic 和 OpenAI Codex OAuth provider 通过 `config/llm_providers.toml` 配置，agent 在 `config/agents.toml` 中通过 `provider` 字段引用。

示例：

```toml
provider = "deepseek"
model = "deepseek-v4-pro"

provider = "zai"
model = "zai/glm-5.1"
```

OpenAI Codex OAuth provider 可用于本机实验。先完成一次 device login：

```bash
UV_CACHE_DIR=/tmp/uv-cache uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json \
  --list-models \
  --live-response
```

认证会保存到 `~/.ruyi_agent/openai_codex_auth.json`。之后可在 `config/agents.toml` 中配置：

```toml
provider = "openai_codex"
model = "gpt-5.3-codex"
```

## 开发

运行测试：

```bash
uv run pytest
```

构建 wheel 和 sdist：

```bash
uv build
```

`dist/` 是本地构建产物，不需要提交到 git。

## 安全提示

- 不要提交真实凭据、SQLite 数据库、日志、运行工作区、OAuth token 或 bot token。
- `BACKEND_KIND=local` 会在本机执行 shell 命令，不提供 Daytona 级别的进程隔离。
- 对公网暴露 Gateway 前，必须修改默认 `dev-token`。
- 开源发布前请脱敏 `.ruyi_agent/ruyi.toml` 和 `.ruyi_agent/config/agents.toml`。

## License

MIT License. See [LICENSE](LICENSE).
