# `src/ruyi_agent` 当前目录结构

本文按 Module 职责说明当前源码布局。

```text
src/ruyi_agent/
├── entrypoints/         进程入口和启动模式
├── gateway/             Gateway Task Module
├── channels/            HTTP 与平台 Adapter、共享 Channel 策略
├── runtime/             Agent 构建、执行、Subagent、Middleware、Skills
├── control_plane/       Permission 与 Review 契约
├── integrations/        A2A、Backend、MCP、OpenAI Codex 集成
├── storage/             SQLite 持久化 Adapter
├── config/              配置加载与运行环境
└── templates/           初始化配置模板
```

## `entrypoints`

`entrypoints/main.py` 解析 `--gateway`、`--telegram`、`--feishu`、`--all` 和 `--init`，并管理 Uvicorn 与 Channel Adapter 的并发生命周期。项目没有 TUI 入口。

## `gateway`

- `tasks.py`：`GatewayTaskModule`，提供 transport-neutral Gateway Task Interface。
- `routing.py`：内部 `TaskRouter`，负责 Local/Remote 路由、Subagent Route 发现与继承、恢复、刷新和 Webhook。
- `models.py`：Gateway Task、Agent、Review、Artifact 和 Route 模型。
- `errors.py`：与 HTTP 解耦的 `GatewayTaskError`。

`gateway` 可以依赖 `AgentControl` 和 Store；它不能依赖 FastAPI 或具体 Channel Adapter。

## `channels`

- `http/routes.py`：FastAPI Adapter、认证、HTTP 输入模型和错误状态映射。
- `gateway_client.py`：Telegram、飞书和测试使用的 Gateway HTTP Client。
- `turn.py`：共享 Channel Turn 策略。
- `task_watch.py`：共享 Task Watch 策略。
- `telegram/adapter.py`：Telegram 平台 Adapter。
- `feishu/adapter.py`：飞书平台 Adapter。

Channel Adapter 不直接管理 Task 状态，也不直接调用 `AgentControl`。

## `runtime`

- `bootstrap.py`：进程级对象装配和资源生命周期。
- `agent_factory.py`：把 Agent Spec 构造成可执行 Agent。
- `agent_turn.py`：Agent 单轮执行辅助逻辑。
- `delegation/async_runtime.py`：`AgentControl`、TaskRecord、Task 状态机和 Subagent 工具。
- `delegation/context.py`：跨 Gateway Delegation Context。
- `mailbox/`：用户/Agent 输入与父子 Task settled 结果的持久投递和安全注入。
- `middleware/`：Skills、Permission Review、Mailbox、Artifact、Tool Search、错误处理和 Worker Delegation。
- `skills/`：Skill 扫描、解析和 Backend 视图同步。

## `control_plane`

- `permissions.py`：Permission profile 与工具决策。
- `reviews.py`：Review 控制和审计协作。
- `contracts.py`：Review Decision、Action 和 Snapshot 模型。

这里不再包含 `ProtocolController` 或 Gateway command/event/snapshot 原型。

## `integrations`

- `a2a/client.py`：Remote Gateway HTTP Client。
- `backend/runtime.py`：Backend Runtime 创建和关闭。
- `mcp/registry.py`：MCP Server 初始化、状态和工具发现。
- `openai_codex.py`：OpenAI Codex 模型接入支持。

## `storage`

- `task_store.py`：TaskRecord 生命周期。
- `gateway_route_store.py`：Local/Remote Route。
- `channel_session_store.py`：Channel identity、Active Agent 和当前 Task。
- `review_audit.py`：Review 决策审计。

Telegram/飞书的事件去重 Store 当前定义在各自 Adapter 相关实现中。

## 依赖方向

```text
entrypoints
  -> runtime/bootstrap
  -> channels adapters

HTTP / Channel Adapters
  -> Gateway Task Interface

GatewayTaskModule
  -> TaskRouter
  -> AgentControl
  -> Storage Adapters

AgentControl
  -> Agent Runtime / Middleware / Integrations
```

禁止重新引入的依赖方向：

- `gateway` 依赖 FastAPI。
- Channel Adapter 直接调用 `AgentControl`。
- Storage Adapter 依赖 HTTP 模型。
- 为 public Agent 单独创建第二套任务运行时。
- 通过兼容层恢复已删除的 TUI 或 `ProtocolController`。
