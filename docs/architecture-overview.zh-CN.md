# Ruyi Agent 架构认知手册

这份文档用于快速恢复对 `ruyi-agent` 内部设计的整体认知。它不是 API 使用说明，而是项目作者在面试、重构或排障前可以翻阅的系统地图。

## 一句话定位

`ruyi-agent` 是一个面向工程化任务的多入口 Agent Runtime。它把 TUI、HTTP Gateway、Telegram、Feishu/Lark 等入口接到同一套任务控制面，支持：

- root agent 和 worker/subagent 的委托执行
- local worker 和远端 `remote_ref`
- HITL 人工审批
- MCP 工具和 tool search
- skills 可见性控制
- 权限策略和审计
- SQLite 持久化
- 附件、artifact、webhook、mailbox 通知

核心思想是：入口层不要直接调用 LangGraph agent 做一次性请求，而是通过任务控制面管理长生命周期任务。

## 分层地图

```text
CLI / TUI / HTTP Gateway / Telegram / Feishu
  -> channel adapter / GatewayService / ProtocolController
  -> AgentControl / AsyncSubagentRuntime
  -> LangGraph runtime agent
  -> middleware stack
  -> model + tools + backend + checkpointer
  -> TaskStore / GatewayRouteStore / ChannelSessionStore / ReviewAuditStore
```

更具体地看：

```text
entrypoints/main.py
  -> configure_runtime_environment
  -> bootstrap_application
       -> create_backend_runtime
       -> load configs
       -> MCPRegistry.refresh
       -> SkillCatalog.scan + SkillSyncer
       -> AsyncSqliteSaver
       -> TaskStore / GatewayRouteStore / ReviewAuditStore
       -> build LocalWorkerSpec / RemoteRef
       -> AgentControl(worker_control)
       -> GatewayService
       -> AppRuntime
```

## 核心模块职责

### `entrypoints`

`src/ruyi_agent/entrypoints/main.py`

负责命令行入口：

- `ruyi` / `ruyi --tui` 启动本地 TUI
- `ruyi --gateway` 启动 FastAPI Gateway
- `ruyi --telegram` / `--feishu` / `--all` 启动 channel adapter
- `ruyi --init` 初始化配置模板

入口层只决定启动哪些 channel，不装配 agent runtime 的细节。

### `runtime/bootstrap.py`

这是进程级装配中心。它负责把配置文件和环境变量变成可运行对象。

主要产物是 `AppRuntime`：

- `gateway_service`: HTTP 入口使用的任务门面
- `worker_control`: 内部完整任务控制面，登记所有 local agent 和 remote_ref
- `gateway_control`: 只登记 public target 的控制面，目前主要表达 Gateway 可见范围
- `local_agent_specs`: 所有本地 agent 的可执行 spec
- `skill_catalog` / `skill_syncer`
- SQLite 路径和审计 store

一个容易忘的点：`GatewayService` 目前用 `AgentControlGatewayRuntime(worker_control)` 执行任务。public 暴露由 `GatewayService` 根据 `agent_configs` 校验；实际运行仍落在完整 `worker_control` 里，这样 public root agent spawn 出来的内部 worker 能和父任务处在同一个 TaskManager 里。

### `config`

配置解析主要在 `src/ruyi_agent/config/loader.py`。

`agents.toml` 里的 agent 有两种：

- `kind = "local"`: 本地可执行 agent，会被构造成 `LocalWorkerSpec`
- `kind = "remote_ref"`: 远端 Gateway 上的 agent 引用，只保存 URL、remote agent name 和 auth

`LocalWorkerSpec` 包含：

- name / description / system_prompt
- model
- tools 或 tool search 配置
- memory 路径
- skills 可见性
- permission_profile
- delegation scope

`workers` 字段不是直接创建 worker，而是定义当前 agent 可以委托哪些 target。bootstrap 会把 scope 注入为 `build_delegation_tools`，agent 创建时再变成 `spawn_agent`、`wait_agent` 等工具。

### `runtime/agent_factory.py`

`create_runtime_agent` 是项目自己的 LangGraph agent 工厂。

它复用 deepagents 的模型解析，但不使用 deepagents 默认 subagent/task/general-purpose 机制。项目自己构造 middleware stack，并直接调用 LangChain `create_agent`。

这么做的原因是：Ruyi 要自己掌控 subagent 任务状态、审批、持久化、mailbox、Gateway 映射和权限。

### `runtime/middleware`

middleware stack 在 `runtime/middleware/stack.py` 中集中组装。

大致顺序：

- `TodoListMiddleware`
- `ToolErrorMiddleware`
- `TaskHydrationMiddleware`
- `MailboxMiddleware`
- `ToolSearchMiddleware`
- `ArtifactPublishingMiddleware`
- `RuyiSkillsMiddleware`
- Filesystem / summarization / patch tool calls
- `WorkerDelegationMiddleware`
- `HumanApprovalMiddleware`
- Anthropic prompt caching
- Memory

几个关键 middleware：

- `WorkerDelegationMiddleware`: 给 agent 注入 worker delegation tools
- `HumanApprovalMiddleware`: 根据 permission policy 拦截工具调用并产生 HITL review
- `RuyiSkillsMiddleware`: 把当前任务的 skill view 暴露给模型
- `MailboxMiddleware`: 在父 agent 下一轮模型调用前注入已完成子任务的终态消息
- `ArtifactPublishingMiddleware`: 记录并发布任务产物
- `TaskHydrationMiddleware`: agent run 开始时按当前 thread 加载历史 task

### `runtime/delegation/async_runtime.py`

这是 subagent / worker / remote_ref 的任务状态机核心。文件名里叫 async runtime，领域上可以理解为 `AgentControl`。

它包含几个核心对象：

- `AgentRegistry`: 管理 local worker 和 remote_ref 的注册项
- `TaskRecord`: 一条任务的完整控制面状态
- `TaskManager`: `TaskRecord` 的唯一写入口，并负责同步到 `TaskStore`
- `AgentControl`: 对外暴露 spawn、wait、check、send_input、cancel、review resume 等能力

它同时服务两种调用者：

- 模型工具层：返回人类可读文本，例如 `Started worker task: task_id=...`
- Gateway / 控制面层：返回结构化 `TaskRecord`

## Root Agent、Worker、Remote Ref

### Root Agent

root agent 是用户直接交互的 agent。它可以来自：

- TUI 本地交互
- Gateway 创建 public agent task
- Telegram/Feishu channel 创建 Gateway Task

root agent 本质上也是一个 local agent spec，只是在某个入口中作为顶层执行对象。

### Worker / Subagent

worker 是被另一个 agent 通过 `spawn_agent` 委托出来的 local agent task。

worker 有独立：

- `task_id`
- `thread_id`
- checkpointer thread
- TaskRecord
- run_count
- permission profile
- skill view

worker 可以继续 spawn 自己 scope 内允许的 worker 或 remote_ref。

### Remote Ref

`remote_ref` 是远端 Gateway 上 agent 的引用。它不能在本地编译执行，只能通过 A2A HTTP client 调远端 Gateway。

本地仍会创建一个 `TaskRecord`，用本地 `task_id` 追踪；远端返回的任务 ID 记录在 `upstream_task_id`。

## 运行链路

### TUI 链路

```text
ruyi / ruyi --tui
  -> run_interactive
  -> bootstrap_application
  -> runtime.get_local_agent(agent_name)
  -> agent.ainvoke / stream events
  -> ReviewControl 处理 root review 和 task review
```

TUI 当前主要直接调用本地 agent，并用 `ReviewControl` 统一处理审批。`ProtocolController` 是更通用的控制面协议层，但 TUI 代码里有自己的 streaming/event 适配。

### HTTP Gateway 创建任务

```text
POST /agents/{agent_name}/tasks
  -> GatewayService.create_task
  -> 校验 agent 存在且 public
  -> 解析 DelegationContext metadata
  -> local: _spawn_local_task
       -> AgentControl.spawn_task
       -> TaskManager.create_task_record
       -> _start_run
  -> remote_ref: _create_remote_task
       -> AgentControl.spawn_task
       -> A2AClient.create_task
       -> sync_remote_task
  -> GatewayRouteStore.save_route
  -> TaskResponse
```

Gateway 负责 HTTP 契约、public 暴露、附件、webhook、route store 和错误码；真正的任务生命周期仍由 `AgentControl` 管。

### Root Agent Spawn Worker

```text
root agent
  -> spawn_agent tool
  -> AgentControl.spawn_agent
  -> 从 RunnableConfig 提取 parent_task_id / parent_thread_id
  -> AgentControl.spawn_task
  -> AgentRegistry 校验 agent_name
  -> 生成 task_id
  -> 计算 root_task_id / depth / DelegationContext
  -> 解析 permission_profile
  -> 解析 effective skills + materialize skill view
  -> root_task_id 维度加锁，检查 depth 和 task budget
  -> TaskManager.create_task_record(state=pending)
  -> _start_run
  -> TaskManager.mark_running
  -> worker agent.ainvoke
```

worker 真正执行时，runtime 会把这些字段放进 LangGraph config：

```text
thread_id
task_id
parent_task_id
root_task_id
delegation_depth
agent_name
permission_profile
effective_skill_names
skill_view_path
skill_view_hash
```

### Worker 进入人工审批

```text
worker agent tool call
  -> HumanApprovalMiddleware 判断需要审批
  -> LangGraph/DeepAgents interrupt
  -> normalize_agent_turn 提取 review_payloads
  -> TaskManager.mark_waiting_for_human
       state = waiting_for_human
       active_run = None
       pending_review.review_id = existing or uuid
  -> mirror pending review 到 root task
  -> ReviewControl / Gateway / TUI 可以查询和提交决策
```

提交审批：

```text
POST /tasks/{task_id}/reviews/{review_id}/decision
  -> GatewayService.submit_review_decision
  -> AgentControl.submit_review_decision
  -> _resume_run
  -> Command(resume={"decisions": ...})
  -> TaskManager.mark_running
  -> worker 从同一个 thread_id 恢复执行
```

## 任务状态机

状态定义：

```text
active:
  pending
  running
  waiting_for_human

terminal:
  completed
  failed
  cancelled
  interrupted
```

状态含义：

- `pending`: TaskRecord 已创建，还未标记开始运行
- `running`: 有活跃 `asyncio.Task` 正在执行
- `waiting_for_human`: worker 暂停在人工审批点，不是终态
- `completed`: 成功完成，`result` 有最终文本
- `failed`: 执行异常、未解决 tool calls、非法多 review 等失败
- `cancelled`: 用户显式取消
- `interrupted`: 非业务取消，例如本地进程重启或事件循环中断

典型转移：

```text
spawn_task
  pending -> running

worker 正常返回
  running -> completed

worker 抛异常
  running -> failed

worker 触发 HITL
  running -> waiting_for_human

审批通过/拒绝/编辑后 resume
  waiting_for_human -> running

用户 cancel
  running -> cancelled
  waiting_for_human -> cancelled
  completed/failed/interrupted -> cancelled  # 当前 cancel_task 对无 active_run 的非 cancelled 任务会标记 cancelled

进程重启恢复本地 running/pending
  pending/running -> interrupted
```

避免重复运行的机制：

- `TaskRecord.active_run` 保存当前 asyncio task
- `_start_run` 和 `_resume_run` 会检查 active run 未结束则抛 `TaskAlreadyRunningError`
- `send_task_input` 不允许给 active 状态继续输入
- review resume 只能用于 `waiting_for_human`
- 本地任务重启恢复时不保留旧 active_run，并把 pending/running 标记为 `interrupted`

## Worker 工具语义

agent 能看到的 worker 工具有四类主要操作。

### `spawn_agent`

启动一个委托任务，立即返回 task_id。适合：

- 并行拆分任务
- 后台执行
- 把专门任务交给专门 worker

它不等待结果。

### `wait_agent`

等待某个 task 进入终态。适合当前回答依赖 worker 结果的场景。

如果 worker 进入 `waiting_for_human`，且当前 config 没有自动处理 pending review 的回调，`wait_agent` 会返回当前状态而不是永久阻塞。对模型展示时，`waiting_for_human` 会被格式化成 `running`，避免模型误以为应该重复委托或绕开审批。

### `check_agent`

非阻塞查询状态。适合 root agent 同时开多个 worker 后轮询：

- 哪个完成了
- 哪个失败了
- 哪个还在跑
- 哪个 remote_ref 刷新失败

### `send_input`

给已有 task 发送后续输入。它复用同一个 task/thread，不创建新 task，也不增加 depth。

适合任务已经终态后，继续在同一上下文上追加要求。它不用于恢复人工审批；审批必须走 review decision。

### `cancel_agent`

取消不再需要的 worker。

本地任务会 cancel active asyncio task；远端任务会转发 A2A cancel 请求。

## ID 语义

### `task_id`

控制面任务 ID。所有查询、取消、继续输入、审批关联都围绕它。

本地 task：`task_id` 是本地 runtime 主键。

remote_ref task：本地也生成自己的 `task_id`，远端返回的 ID 放进 `upstream_task_id`。

### `thread_id`

LangGraph/checkpointer 的会话线程 ID。用于恢复同一个 agent 的上下文。

本地 worker 默认：

```text
thread_id = task_id
```

root agent 的 thread_id 通常来自 TUI/控制面/会话，不一定等于某个 task_id。

### `parent_task_id`

当前 task 的直接父任务。

```text
root task: parent_task_id = None
root spawn worker: parent_task_id = root.task_id
worker spawn worker: parent_task_id = parent_worker.task_id
```

### `root_task_id`

整棵委托树的根 task。

```text
root task: root_task_id = task_id
child / grandchild: root_task_id = root.task_id
```

用途：

- 任务树聚合
- 最大深度限制
- 单棵树最大任务数预算
- 审计和排障定位

### `parent_thread_id`

父 agent 的 LangGraph thread。用于 mailbox 把子任务终态消息投递回父 agent。

### `upstream_task_id`

远端 Gateway 返回的 task ID。只对 `route_kind = remote_ref` 有意义。

### `review_id`

一次待审批请求的 ID。可能来自 runtime payload，也可能由 `TaskManager.mark_waiting_for_human` 补 uuid。

worker review 绑定 task；root review 不一定有 task_id，因此 `ReviewControl` 对 root review 有单独的内存注册表。

## 通知机制

### 主动查询

root agent 可以：

- `wait_agent(task_id)` 同步等待结果
- `check_agent(task_id)` 非阻塞查看状态

HTTP 用户可以：

- `GET /tasks/{task_id}`
- `GET /reviews`
- `GET /tasks/{task_id}/reviews`

### Mailbox

当子任务后台终态，而父 agent 没有通过 wait/check 主动拿结果时，runtime 会向 `parent_thread_id` 的 mailbox 发布终态消息。

下一轮父 agent 模型调用前，`MailboxMiddleware` 会 drain mailbox，把消息作为 human message 注入上下文：

```text
[mailbox] Delegated agent task finished.
task_id=...
agent=...
status=...
message:
...
```

如果 root 已经 `wait_agent` 或 `check_agent` 拿到终态，runtime 会 suppress/retract mailbox，避免重复提醒。

### Webhook

Gateway 创建任务时可带 webhook。任务终态后 runtime 会发送：

```text
task.completed
task.failed
task.cancelled
task.interrupted
```

remote_ref 的 webhook 事件进来后，会按 `upstream_task_id` 找到本地 TaskRecord，同步状态，再触发 mailbox/webhook。

## Review / HITL

Ruyi 把审批分成两类，但统一暴露成 review：

- root review: root agent 当前 thread 中断产生，不一定有 task_id
- task review: worker/subagent task 进入 `waiting_for_human`

`ReviewControl` 的职责：

- 从 root interrupt 注册 root review
- 从 TaskRecord.pending_review 生成 task review snapshot
- 聚合所有 pending reviews
- 将 approve/reject/edit 转成 runtime resume payload
- 根据 review_id 找到 root review 或 task review
- 调 root runner 或 `AgentControl.submit_review_decision` 恢复执行

`ReviewAuditStore` 记录权限审批和任务审批相关事件，便于排障和审计。

## Gateway 模型

HTTP Gateway 主要在 `channels/http/api.py`。

主要资源：

- `/agents`: list public agents
- `/agents/{agent_name}/tasks`: 创建任务
- `/tasks/{task_id}`: 查询任务
- `/tasks/{task_id}/input`: 给任务继续输入
- `/tasks/{task_id}/cancel`: 取消任务
- `/reviews`: 查询 pending reviews
- `/tasks/{task_id}/reviews`: 查询某 task 的 reviews
- `/tasks/{task_id}/reviews/{review_id}/decision`: 提交审批决策
- artifact 下载接口
- task webhook 接收接口

Gateway 层做：

- Bearer token 鉴权
- public agent 校验
- metadata 和 DelegationContext 解析
- 附件上传到 backend inbox
- local / remote_ref 路由
- route 持久化
- HTTP 错误码转换
- webhook 处理

Gateway 不应该自己实现 agent 状态机。

## Channel Adapter 模型

Telegram 和 Feishu/Lark adapter 的领域语言见 `CONTEXT.md` 和 `docs/channel-turn-module.zh-CN.md`。

重要概念：

- Channel Adapter: 平台收发层，只处理平台 SDK、mention、thread、格式、附件等
- Channel Turn: 一次平台用户输入在 Ruyi 策略下的处理
- Channel Session: 平台身份和当前 agent/task 的持久绑定
- Gateway Task: channel 通过 Gateway 创建、继续、审批、取消或查询的任务
- Review Command: channel 上的审批命令
- Task Watch: 观察当前 task run，直到 pending review、terminal 或更新 run
- Active Agent: channel session 默认使用的 agent

当前 `docs/channel-turn-module.zh-CN.md` 记录了一个目标方向：把 Telegram/Feishu 中重复的 turn policy 抽成共享 Channel Turn Module，让平台 adapter 只保留平台相关能力。

## 存储模型

默认 SQLite 路径在 `runtime/bootstrap.py`：

```text
data/checkpoints.sqlite
data/gateway_routes.sqlite
data/tasks.sqlite
data/review_audit.sqlite
```

另有 channel session store，具体路径由 channel adapter 配置/使用决定。

### Checkpointer

`AsyncSqliteSaver` 保存 LangGraph checkpoint。

用途：

- root agent thread 恢复
- worker task thread 恢复
- review resume 继续同一条 graph thread

### TaskStore

`TaskStore` 保存 `agent_tasks`。

关键字段：

- task identity: `task_id`, `agent_name`, `thread_id`
- tree: `parent_task_id`, `root_task_id`, `depth`
- lifecycle: `state`, `run_count`, `result`, `error`
- route: `route_kind`, `upstream_task_id`
- notification: `parent_thread_id`, `mailbox_suppressed`, `mailbox_delivered`, `webhook_json`
- delegation limits: `delegation_root_id`, `delegation_max_depth`, `delegation_max_tasks_per_root`, `delegation_visited_nodes_json`
- permissions: `permission_profile`
- skills: `effective_skill_names_json`, `skill_view_path`, `skill_view_hash`
- review: `pending_review_json`
- artifacts: `artifacts_json`

重启语义：

- local `pending/running` 会恢复成 `interrupted`
- 其他状态保留
- `active_run` 永远不会从数据库恢复

### GatewayRouteStore

保存 Gateway task route：

- `task_id`: 本地 Gateway 对外暴露的 ID
- `agent_name`
- `metadata_json`
- `route_kind`: local / remote_ref
- `upstream_task_id`: local 时通常等于 task_id，remote_ref 时是远端 task_id
- `webhook_json`

它让 Gateway 可以通过本地 task_id 查询远端状态，也可以通过 upstream_task_id 处理远端 webhook。

### ChannelSessionStore

保存 channel session：

- `session_key`
- `platform`
- `agent_name`
- `current_task_id`
- `chat_id`
- `user_id`
- `thread_id`
- timestamps

它把平台身份和当前 Gateway Task / Active Agent 绑定起来。

### ReviewAuditStore

追加式审计日志。记录：

- event_type
- source
- review_id / task_id / thread_id
- agent_name
- profile_name
- backend_kind / workspace_root
- tool_name / tool_call_id
- policy_decision / risk / reason
- payload_json

用于权限、审批、任务恢复和排障追踪。

## Skills

skills 发现路径按有效优先级：

```text
workspace/.agents/skills
~/.agents/skills
~/.ruyi_agent/skills
```

实现里扫描顺序是反过来写入 dict，后扫描覆盖前扫描，所以 workspace 优先级最高。

每个 skill 目录需要有 `SKILL.md`，并且 frontmatter 中有 `name` 和 `description`。

`agents.toml` 的 `skills` 表示可见性：

```toml
skills = ["frontend", "repo-workflow"]
skills = "inherit"
skills = "none"
```

任务创建时，runtime 会把 effective skills materialize 到 backend 内部 skill view：

```text
/.ruyi_agent/runtime/skill-views
```

root agent 也会通过 `resolve_root_skill_config` 得到自己的 skill view 配置。

## 权限和审批策略

权限配置来自 `config/permissions.toml`，被解析成 `PermissionPolicy`。

运行时通过 `HumanApprovalMiddleware` 使用它：

- 对工具调用、shell 执行等行为做 allow/review/deny 决策
- 需要 review 时产生 HITL interrupt
- 审批事件写入 `ReviewAuditStore`
- 根据 `permission_profile` 区分不同 agent 或任务权限

`permission_profile` 会写入 TaskRecord，并传入 agent config，保证同一个任务 run 和 review resume 使用同一套权限语义。

## Backend

backend 由 `integrations/backend/runtime.py` 创建。

两种主要模式：

- `daytona`: 默认隔离 backend，文件和命令在 sandbox 内执行
- `local`: 本地开发模式，虚拟根目录映射到 `LOCAL_BACKEND_ROOT`

backend 决定：

- 文件工具访问哪里
- artifact 放哪里
- memory 路径如何映射
- skill view 写到哪里
- shell 命令在哪个环境执行

安全上要记住：`BACKEND_KIND=local` 没有 Daytona 的进程隔离。

## Delegation Context

`runtime/delegation/context.py` 定义跨网关委托上下文。

它被编码进 metadata，字段包括：

- `_deepagents_context_version`
- `_deepagents_root_id`
- `_deepagents_depth`
- `_deepagents_max_depth`
- `_deepagents_max_tasks_per_root`
- `_deepagents_visited_nodes`

用途：

- 防止跨 Gateway 循环委托
- 限制最大深度
- 限制单棵委托树最大任务数
- 下游 Gateway 不能放宽上游限制，只能取更严格值

本地任务树也使用 root/depth/budget 概念；跨 Gateway 时这些信息通过 metadata 传递。

## ProtocolController 的位置

`control_plane/contracts.py` 定义稳定的 command/event/snapshot 协议。

`ProtocolController` 做：

- `SendUserMessageCommand`
- `SendTaskInputCommand`
- `SubmitReviewDecisionCommand`
- `CancelTaskCommand`
- `SwitchThreadCommand`
- 将 runtime/root runner 结果转成 `ProtocolEvent`
- 聚合 `RuntimeSnapshot`

它是一个面向多入口 UI 的统一控制面翻译层。当前 HTTP Gateway 主要走 `GatewayService` 的 REST 资源模型；TUI 也有自己的 streaming 适配。但 `ProtocolController` 表达的是项目想要收敛出的通用控制面协议。

## 为什么不是入口层直接调 LangGraph

因为 Ruyi 管的不是单轮 agent response，而是长生命周期任务。

如果入口层直接调 LangGraph，会很快遇到这些问题：

- HTTP、TUI、Bot 各自实现一套任务状态
- worker 和 remote_ref 无法统一查询、取消、继续输入
- review resume 找不到稳定 thread/task
- 进程重启后 pending/running 状态不可解释
- Gateway route 和 upstream_task_id 无处持久化
- mailbox/webhook/artifact 没有统一触发点
- 权限、skills、memory、backend、审计散落在入口层
- 多 agent 委托树的 depth/budget/loop control 无法集中治理

所以设计上：

```text
LangGraph agent = 执行引擎
AgentControl / TaskManager = 长生命周期任务状态机
GatewayService / ProtocolController = 入口控制面适配
Store = 可恢复边界
```

## 面试时可以抓住的主线

如果被问“这个系统怎么跑”，先从这条主线答：

```text
用户入口
  -> GatewayService / TUI / Channel Adapter
  -> AgentControl 创建 TaskRecord
  -> LangGraph agent 按 thread_id 执行
  -> middleware 注入权限、skills、worker tools、mailbox、filesystem
  -> worker 可继续 spawn 子任务
  -> 状态写 TaskStore
  -> 审批写 pending_review，终态写 result/error
  -> 用户通过 wait/check/API/webhook/mailbox 得到变化
```

如果被问“ID 怎么设计”，答：

```text
task_id: 控制面任务主键
thread_id: LangGraph checkpoint 线程
parent_task_id: 直接父任务
root_task_id: 委托树根任务
parent_thread_id: mailbox 回投目标
upstream_task_id: remote_ref 远端任务 ID
review_id: 人工审批请求 ID
```

如果被问“状态机怎么设计”，答：

```text
pending -> running -> completed/failed/waiting_for_human
waiting_for_human -> running via review decision
running -> cancelled by cancel
pending/running -> interrupted on restart
```

如果被问“为什么需要控制面”，答：

```text
因为任务可异步、可审批、可恢复、可委托、可远端路由、可跨入口查询。
入口层直接调用 LangGraph 只能处理单轮对话，不能可靠管理这些长生命周期状态。
```

## 重点源码索引

- `src/ruyi_agent/entrypoints/main.py`: CLI 入口
- `src/ruyi_agent/runtime/bootstrap.py`: 进程级装配
- `src/ruyi_agent/runtime/agent_factory.py`: runtime agent 工厂
- `src/ruyi_agent/runtime/middleware/stack.py`: middleware 顺序
- `src/ruyi_agent/runtime/delegation/async_runtime.py`: AgentControl、TaskManager、TaskRecord、worker 状态机
- `src/ruyi_agent/runtime/delegation/context.py`: DelegationContext
- `src/ruyi_agent/channels/http/api.py`: GatewayService 和 REST API
- `src/ruyi_agent/control_plane/contracts.py`: 控制面协议模型
- `src/ruyi_agent/control_plane/controller.py`: ProtocolController
- `src/ruyi_agent/control_plane/reviews.py`: ReviewControl
- `src/ruyi_agent/storage/task_store.py`: TaskStore
- `src/ruyi_agent/storage/gateway_route_store.py`: GatewayRouteStore
- `src/ruyi_agent/storage/channel_session_store.py`: ChannelSessionStore
- `src/ruyi_agent/storage/review_audit.py`: ReviewAuditStore
- `src/ruyi_agent/runtime/mailbox/service.py`: AgentMailbox
- `src/ruyi_agent/runtime/skills/catalog.py`: skill discovery
- `src/ruyi_agent/runtime/skills/sync.py`: skill view materialization
- `src/ruyi_agent/integrations/backend/runtime.py`: local/daytona backend
- `src/ruyi_agent/integrations/mcp/registry.py`: MCP tool registry
