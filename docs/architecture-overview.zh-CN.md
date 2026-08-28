# Ruyi Agent 当前系统架构

本文描述当前代码实现，不保留已经删除的 TUI、`ProtocolController`、`gateway_control` 或旧 Gateway Runtime Adapter。

## 架构结论

Ruyi Agent 以 **Gateway Task** 作为统一长会话模型，以 `AgentControl` 作为唯一任务运行时。HTTP、Telegram 和飞书是接入 Adapter，不拥有任务状态机。

```text
用户 / 外部系统
  ├── HTTP Gateway Adapter
  ├── Telegram Channel Adapter
  └── Feishu Channel Adapter
              │
              ▼
       Gateway Task Module
              │
       ┌──────┴───────────┐
       ▼                  ▼
  上层任务策略         Task Router
  Agent/Review/         Local/Remote 路由
  Attachment/Artifact  恢复/刷新/Webhook
       └──────┬───────────┘
              ▼
          AgentControl
              │
       ┌──────┴───────────┐
       ▼                  ▼
   Local Agent        Remote Ref
   LangGraph          A2A Gateway
```

## 核心领域模型

### Gateway Task

Gateway Task 是一个 Agent 的独立、持久、可继续交互的长会话。一次 Run 结束不关闭 Task；后续 `send_input` 会在同一 `task_id` 上启动下一轮，并增加 `run_count`。

关键身份：

- `task_id`：Gateway 和用户侧稳定会话身份。
- `thread_id`：LangGraph checkpoint 恢复身份。
- `parent_task_id`、`root_task_id`、`depth`：Subagent 任务树关系。
- `route_kind`、`upstream_task_id`：Local/Remote 路由关系。

Task 状态描述当前或最近一次 Run：`pending`、`running`、`waiting_for_human`、`completed`、`failed`、`cancelled`、`interrupted`。

### Subagent

Subagent 是另一个独立 Gateway Task，不是隐藏在父会话中的临时模型调用。父 Agent 通过 `spawn_agent`、`wait_agent`、`check_agent`、`send_input`、`cancel_agent` 和 `list_agents` 与其交互。

```text
Root Gateway Task
  ├── Subagent Task A
  │     └── Subagent Task C
  └── Subagent Task B
```

Gateway 会为 public Root Task 的 Subagent 自动恢复并持久化 Route，继承 Root Task 的 Channel metadata。Subagent 因此可以通过现有 `GET /tasks/{task_id}`、`GET /tasks/{task_id}/messages`、`POST /tasks/{task_id}/input` 和 `/resume <task_id>` 独立进入和继续。未归属于 Gateway Root Route 的内部 Task 不会被自动暴露。

## Gateway Task Module

`src/ruyi_agent/gateway/tasks.py` 是与传输协议无关的 Gateway Task Module。它的公开 Interface 包括：

- 公开 Agent 查询与可用性判断。
- Task 创建、读取、列表、继续和取消。
- Task Message Transcript 快照分页查询。
- Review 查询与提交。
- Attachment 输入处理。
- Artifact 查询与下载。
- Remote Task Webhook 入口。

调用者不需要了解 Task 是 Local 还是 Remote，也不需要了解 `GatewayRouteStore`、A2A 状态刷新或远程记录重建。

### Task Router

`src/ruyi_agent/gateway/routing.py` 是 Gateway Task Module 的内部深 Module，集中负责：

- Local/Remote Task 创建和 Route 持久化。
- Task Route 查询与更新。
- Remote TaskRecord 在重启后的重建。
- Remote 状态刷新。
- Local checkpoint transcript 分页与 Remote transcript 透明代理。
- Routed `send_input`、`cancel` 和 review submission。
- Delegation 与运行时异常转换。
- Remote Webhook 投递。

`TaskRouter` 不处理 HTTP，不构建 Pydantic 响应，也不解码 Attachment 内容。

## AgentControl 与执行

`src/ruyi_agent/runtime/delegation/async_runtime.py` 中的 `AgentControl` 是唯一任务运行时。所有 public Agent、内部 Worker 和 Remote Ref 都登记在同一个运行时中。

它负责：

- Task 状态机和 Run 调度。
- TaskRecord 持久化与恢复。
- Subagent 工具和任务树限制。
- Remote Ref 执行。
- 通过轻量只读 state graph 恢复 Local Task 的精确消息 checkpoint。
- Task Mailbox 持久化投递、安全模型边界注入与空闲 Task 唤醒。
- Permission Review 恢复。

Public 可见性属于 Gateway Task Module 的入口策略，不通过创建第二个 `AgentControl` 表达。

## HTTP 与 Channel Adapter

`src/ruyi_agent/channels/http/routes.py` 是 FastAPI Adapter，只负责：

- Bearer Token。
- HTTP 请求模型和参数解析。
- HTTP 状态码、Header 和 JSON 错误封装。
- 调用 Gateway Task Interface。

领域错误由 `gateway/errors.py` 定义，HTTP Adapter 再将错误代码或语义类型映射为状态码。

Telegram 和飞书通过 `GatewayHTTPClient` 调用同一个 HTTP Interface，不直接调用 `AgentControl`。

共享 Channel 策略：

- `channels/turn.py`：Channel Turn、Session 绑定、`/new`、`/agent`、`/resume` 和 Review Command。
- `channels/task_watch.py`：Run 观察、终态通知、审批通知和并发去重。
- 平台 Adapter：事件解析、平台身份、群聊/thread/topic、格式化、附件和消息发送。

## MCP、Skills 与权限

启动时 `MCPRegistry.refresh()` 发现 MCP 工具，再按 Agent 配置筛选可见工具。

Skills 按以下优先级扫描并同步到 Backend：

1. `~/.ruyi_agent/skills`
2. `~/.agents/skills`
3. Workspace `.agents/skills`

TaskRecord 持久化实际生效的 skill 名称、视图路径和内容指纹。

Permission Middleware 根据 profile 决定工具调用是直接允许还是进入 `waiting_for_human`。Review 决策恢复同一个 Gateway Task，而不是创建新会话。

## 持久化与恢复

| Store | 职责 |
|---|---|
| LangGraph Checkpointer | Agent 执行现场、中断恢复和 Local Task Message Transcript 真源 |
| TaskStore | Gateway Task 生命周期 |
| GatewayRouteStore | Local/Remote Route 和 upstream 映射 |
| ChannelSessionStore | 平台身份、Active Agent 和当前 Task |
| ReviewAuditStore | 审批审计 |
| TelegramUpdateStore / FeishuEventStore | 入站事件去重 |

重启后，已完成 Task 可继续查询；Local 活跃 Run 被规范化为可解释的中断状态；Remote Task 可根据 Route 重建本地 TaskRecord 并刷新远端状态。Message Transcript 首次查询先固定最新完整 checkpoint，后续游标始终读取同一 checkpoint；Remote Task 的游标由下游 Gateway 拥有并原样透传。

## 启动模式

统一入口位于 `entrypoints/main.py`：

```text
ruyi --gateway
ruyi --telegram
ruyi --feishu
ruyi --all
ruyi --init
```

Telegram 和飞书模式会同时启动 Gateway。裸 `ruyi` 不再进入 TUI，而是要求显式选择入口。

## 当前明确未实现的产品能力

- 图形化浏览和切换 Subagent Task Tree 的界面；命令式 `/resume <task_id>` 已可使用。
- 外部 MCP 网络调用的统一重试、调用证据和超时审计。
- 更完整的 Task ownership 与多租户授权模型。

这些属于后续产品能力，不是当前已确认的任务状态机 Bug。
