# OpenAI Codex Probe 脚本使用说明

本文档说明 `scripts/probe_openai_codex.py` 的功能、典型用法、参数和常见错误处理。

这个脚本用于验证 Ruyi 的 `openai_codex` provider 是否能通过 ChatGPT/Codex OAuth 凭据访问 Codex backend。它不是 OpenAI API key 测试脚本，而是使用 ChatGPT device login 得到的 OAuth token。

## 脚本位置

```bash
scripts/probe_openai_codex.py
```

推荐从项目根目录运行：

```bash
uv run python scripts/probe_openai_codex.py
```

## 它做什么

脚本支持四类操作：

- 读取现有 Codex OAuth token，并输出 token 摘要
- 通过 device login 重新登录 ChatGPT/OpenAI 账号
- 使用 refresh token 刷新 access token
- 调 Codex backend 的模型列表和 Responses streaming 接口做真实连通性测试

默认认证文件：

```text
~/.ruyi_agent/openai_codex_auth.json
```

默认 Codex backend：

```text
https://chatgpt.com/backend-api/codex
```

默认测试模型：

```text
gpt-5.3-codex
```

## 最常用命令

### 1. 查看当前认证状态

```bash
uv run python scripts/probe_openai_codex.py
```

这是 dry-run，只读取 token，不访问远端接口。

你会看到：

```json
{
  "mode": "dry-run",
  "has_access_token": true,
  "has_refresh_token": true,
  "auth_source": "/root/.ruyi_agent/openai_codex_auth.json"
}
```

### 2. 首次登录或换账号

```bash
uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json
```

脚本会打印：

```text
OpenAI Codex device login:
  URL:  https://auth.openai.com/codex/device
  Code: XXXXXXXX
Waiting for browser authorization...
```

在浏览器打开 URL，输入 Code，并登录你要使用的 ChatGPT/OpenAI 账号。授权成功后，脚本会把 token 保存到：

```text
~/.ruyi_agent/openai_codex_auth.json
```

换账号前建议备份旧认证文件：

```bash
cp ~/.ruyi_agent/openai_codex_auth.json ~/.ruyi_agent/openai_codex_auth.json.bak
```

如果浏览器总是自动用旧账号授权，可以用无痕窗口打开 device URL，或者先退出旧账号。

### 3. 刷新 token

```bash
uv run python scripts/probe_openai_codex.py --refresh
```

这个命令会使用 refresh token 请求新 access token。注意：当前脚本会在输出里展示刷新结果，但只有 `--device-login --save-auth-json` 会写认证文件；普通 `--refresh` 不会把刷新后的 token 保存回 `auth_json`。

### 4. 列出可用模型

```bash
uv run python scripts/probe_openai_codex.py --list-models
```

它会请求：

```text
GET https://chatgpt.com/backend-api/codex/models?client_version=1.0.0
```

输出中的关键字段：

```json
{
  "models_result": {
    "ok": true,
    "model_ids": ["..."],
    "model_count": 1
  }
}
```

如果想看到完整模型接口返回：

```bash
uv run python scripts/probe_openai_codex.py \
  --list-models \
  --raw-models
```

### 5. 发送一次真实 Codex 响应请求

```bash
uv run python scripts/probe_openai_codex.py --live-response
```

它会请求：

```text
POST https://chatgpt.com/backend-api/codex/responses
```

并使用 SSE streaming 读取输出。

默认 prompt：

```text
Reply with exactly: codex backend ok
```

自定义 prompt：

```bash
uv run python scripts/probe_openai_codex.py \
  --live-response \
  --prompt "Reply with exactly: ok"
```

指定模型：

```bash
uv run python scripts/probe_openai_codex.py \
  --live-response \
  --model gpt-5.3-codex
```

### 6. 完整验证

```bash
uv run python scripts/probe_openai_codex.py \
  --refresh \
  --list-models \
  --live-response
```

这个命令会：

1. 尝试刷新 token
2. 列出模型
3. 发一次 streaming response

如果 `refresh_result.ok`、`models_result.ok`、`response_result.ok` 都是 `true`，说明认证和 Codex backend 访问都正常。

## 典型输出字段

### 顶层字段

- `mode`: `dry-run` 或 `live`
- `base_url`: 当前 Codex backend URL
- `auth_source`: token 来源
- `has_access_token`: 是否读到 access token
- `has_refresh_token`: 是否读到 refresh token
- `last_refresh`: 认证文件记录的最后刷新时间
- `auth_mode`: 通常是 `chatgpt`

### `access_token_summary`

解析 JWT access token 后的摘要：

- `jwt`: 是否是可解析 JWT
- `expires_at`: access token 过期时间
- `expires_in_seconds`: 距离过期秒数
- `has_chatgpt_account_id`: token 中是否包含 ChatGPT account id
- `scopes`: OAuth scopes

### `codex_headers`

脚本请求 Codex backend 时会带：

```text
User-Agent: codex_cli_rs/0.0.0 (ruyi-agent)
originator: codex_cli_rs
ChatGPT-Account-ID: ...
```

输出中不会展示真实 `ChatGPT-Account-ID`，只显示 `present`。

### `refresh_result`

只有传 `--refresh` 时出现。

成功：

```json
{
  "ok": true,
  "raw_keys": ["access_token", "..."],
  "access_token_summary": {}
}
```

失败：

```json
{
  "ok": false,
  "error": "..."
}
```

### `models_result`

只有传 `--list-models` 时出现。

常见字段：

- `status_code`: HTTP 状态码
- `ok`: HTTP 是否成功
- `model_ids`: 提取出的模型 ID
- `model_count`: 模型数量
- `body`: 失败时或传 `--raw-models` 时显示接口响应

### `response_result`

只有传 `--live-response` 时出现。

成功时：

```json
{
  "ok": true,
  "model": "gpt-5.3-codex",
  "detail": {
    "status": "completed",
    "output_text": "codex backend ok",
    "usage": {}
  }
}
```

失败时：

```json
{
  "ok": false,
  "model": "gpt-5.3-codex",
  "error": "..."
}
```

## Token 查找顺序

不使用 `--device-login` 时，脚本按下面顺序找 token：

1. 环境变量 `OPENAI_CODEX_ACCESS_TOKEN` / `OPENAI_CODEX_REFRESH_TOKEN`
2. `--auth-json` 指定的文件，默认 `~/.ruyi_agent/openai_codex_auth.json`
3. 如果传 `--allow-codex-cli-auth`，再读取 `${CODEX_HOME:-~/.codex}/auth.json`

## 认证文件格式

脚本保存的是 Ruyi 自己使用的 provider 格式：

```json
{
  "active_provider": "openai-codex",
  "providers": {
    "openai-codex": {
      "auth_mode": "chatgpt",
      "last_refresh": "...",
      "tokens": {
        "access_token": "...",
        "refresh_token": "...",
        "id_token": "...",
        "account_id": "..."
      }
    }
  },
  "version": 1
}
```

文件权限会以 owner-only 方式写入，也就是 `0600`。

## 与 Ruyi 配置集成

`config/llm_providers.toml` 中需要有：

```toml
[providers.openai_codex]
kind = "openai_codex"

[providers.openai_codex.init_kwargs]
auth_json = "~/.ruyi_agent/openai_codex_auth.json"
```

agent 配置中使用：

```toml
provider = "openai_codex"
model = "gpt-5.6-sol"
```

这个 provider 不需要在 `.ruyi_agent/ruyi.toml` 里配置 OpenAI API key。它使用的是 OAuth 认证文件。

## 参数说明

| 参数 | 默认值 | 作用 |
|---|---:|---|
| `--base-url` | `https://chatgpt.com/backend-api/codex` | Codex backend base URL，也可用 `RUYI_CODEX_BASE_URL` 设置 |
| `--auth-json` | `~/.ruyi_agent/openai_codex_auth.json` | 读取或保存认证文件路径，也可用 `RUYI_CODEX_AUTH_JSON` 设置 |
| `--allow-codex-cli-auth` | false | 如果 Ruyi auth 文件不存在，允许读取 Codex CLI 的 `${CODEX_HOME:-~/.codex}/auth.json` |
| `--access-token-env` | `OPENAI_CODEX_ACCESS_TOKEN` | access token 环境变量名 |
| `--refresh-token-env` | `OPENAI_CODEX_REFRESH_TOKEN` | refresh token 环境变量名 |
| `--timeout` | `20.0` | HTTP 请求超时时间，单位秒 |
| `--model` | 空 | 指定 live response 使用的模型；为空时从模型列表中自动选择，最后回退到 `gpt-5.3-codex` |
| `--instructions` | 内置最小系统提示 | live response 的 instructions |
| `--prompt` | `Reply with exactly: codex backend ok` | live response 的用户输入 |
| `--refresh` | false | 使用 refresh token 刷新 access token |
| `--device-login` | false | 发起 ChatGPT device login |
| `--device-timeout` | `900` | device login 等待浏览器授权的最长时间，单位秒 |
| `--save-auth-json` | false | 配合 `--device-login`，把登录得到的 token 写入 `--auth-json` |
| `--list-models` | false | 请求 Codex models 接口 |
| `--raw-models` | false | 展示完整 models 响应 body |
| `--live-response` | false | 请求 Codex responses streaming 接口 |
| `--client-request-id` | 空 | 指定 `x-client-request-id` header |
| `--session-id` | 空 | 指定 `session_id` header；为空时使用 request id |

## 常见错误

### `refresh_token_invalidated`

示例：

```json
{
  "code": "refresh_token_invalidated",
  "message": "Your refresh token has been invalidated. Please try signing in again."
}
```

含义：refresh token 已被 OpenAI 作废，不能靠 `--refresh` 修复。

处理：

```bash
mv ~/.ruyi_agent/openai_codex_auth.json ~/.ruyi_agent/openai_codex_auth.json.invalidated

uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json
```

### `token_invalidated`

示例：

```json
{
  "code": "token_invalidated",
  "message": "Your authentication token has been invalidated. Please try signing in again."
}
```

含义：access token 已被作废。通常 refresh token 也可能失效。

处理：重新 device login。

### `401 Unauthorized`

含义：

- token 失效
- 授权账号不对
- 没有 Codex backend 访问权限
- `ChatGPT-Account-ID` 不匹配或缺失

处理顺序：

1. 重新 device login
2. 用无痕窗口确保授权的是正确账号
3. 运行 `--list-models` 确认账号可见模型
4. 再运行 `--live-response`

### `No access token available`

含义：脚本没有找到 access token。

检查：

```bash
ls -l ~/.ruyi_agent/openai_codex_auth.json
```

或者重新登录：

```bash
uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json
```

### device login 卡住或超时

默认等待 15 分钟。可以延长：

```bash
uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json \
  --device-timeout 1800
```

也要确认浏览器授权页面登录的是正确账号。

## 推荐工作流

首次配置：

```bash
uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json

uv run python scripts/probe_openai_codex.py \
  --list-models \
  --live-response
```

日常检查：

```bash
uv run python scripts/probe_openai_codex.py --refresh
```

换账号：

```bash
cp ~/.ruyi_agent/openai_codex_auth.json ~/.ruyi_agent/openai_codex_auth.json.bak

uv run python scripts/probe_openai_codex.py \
  --device-login \
  --save-auth-json

uv run python scripts/probe_openai_codex.py \
  --list-models \
  --live-response
```

排查 401：

```bash
uv run python scripts/probe_openai_codex.py \
  --refresh \
  --list-models \
  --live-response \
  --raw-models
```
