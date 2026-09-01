# Runtime 委派系统

本文描述 Ruyi Agent runtime 当前的 worker delegation：本地 worker、远端
`remote_ref`、委派树上下文、模型工具的调用者作用域，以及远端 proxy 的恢复和
reconciliation。事实以当前代码和行为测试为准；跨子系统的装配背景见
[架构总览](../architecture.md)，本地 Task run 的通用执行语义见
[Task execution runtime](task-execution.md)。本文不定义未来的委派协议，也不把
逐函数说明当作公共契约。

## 负责什么，不负责什么

runtime delegation 拥有以下行为：

- `AgentRegistry` 的统一目标命名空间，以及 local worker、`remote_ref` 和
  unavailable 目标的解析；
- `DelegationContext` 的 root、depth、预算和 visited-node 传播与校验；
- `DelegationPolicy` 的委派树、权限 profile 继承和深度/数量准入；
- `DelegationTools` 暴露给模型的 `spawn_agent`、`wait_agent`、`check_agent`、
  `send_input`、`cancel_agent`、`list_agents`，以及目标 allowlist、注册目标展示和
  调用者可见性；
- `TaskRuntime` 对本地 child Task 和远端 proxy Task 的创建、输入、取消、等待、
  review continuation、状态同步和生命周期收口；
- `TaskManager` 对 `TaskRecord`、parent/root 关系、pending review、Task lifecycle
  和 remote uncertainty 的权威状态写入；
- `RemoteTaskPort` 对远端 Task 的 A2A effect、identity 校验、refresh、事件/webhook
  同步和不确定结果 reconciliation。

下列行为属于其他边界：

| 行为或状态 | 所属边界 | runtime delegation 的关系 |
| --- | --- | --- |
| 对外 HTTP 认证、command claim、public Task/Review/Artifact response | `channels/http`、Gateway application/projection | runtime 只提供 `AgentControl` facade，不把工具文本当成 HTTP contract |
| 公开 Gateway remote create 的 `task_id`、route reservation/activation、route recovery | `TaskRouter`、`CreateRouteWorkflow`、`GatewayRouteStore` | Gateway 可调用 runtime 发起效果，但 public route 仍由 Gateway 拥有 |
| A2A HTTP/JSON/SSE、Bearer 凭据和 transport 错误 | `integrations/a2a/A2AClient` 与 `gateway_protocol` | A2A 是 transport，不是第三个 Task owner |
| SQLite 连接寿命、schema 和 checkpoint 连接 | bootstrap、`storage`、LangGraph checkpointer | runtime 使用 `TaskStore`/checkpoint，不拥有进程级连接装配 |
| middleware、skills、backend 和具体模型调用语义 | runtime 对应 middleware/skills/integrations 边界 | LocalTaskExecutor 只接收已选 worker 和显式 payload |
| mailbox、settled outbox 的 claim、投递、recovery 细节 | runtime mailbox/storage 边界 | 本文只说明 child settlement 与 parent 协作的接口和抑制点 |
| Telegram/Feishu session、receipt、delivery | channels | channel 通过 Gateway HTTP，不直接调用 delegation runtime |

### 两条“远端”路径必须分开

公开 Gateway remote create 与 runtime 内部 worker delegation 都可能经由 A2A，
但 ownership 不相同：

1. **公开 Gateway remote create** 由
   [`TaskRouter`](../../src/ruyi_agent/gateway/routing.py)、
   [`CreateRouteWorkflow`](../../src/ruyi_agent/gateway/create_route_workflow.py) 和
   [`GatewayRouteStore`](../../src/ruyi_agent/storage/gateway_route_store.py) 拥有。
   它们先为 public `task_id` 预留并激活 route，再让 runtime 通过 remote port
   触发上游效果；route ledger 负责 public identity 与 upstream identity 的绑定。
2. **runtime internal delegation** 由 `TaskRuntime` + `RemoteTaskPort` 拥有。它
   创建一个本地 `TaskRecord(route_kind="remote_ref")` 作为 proxy，直接通过 A2A
   调用远端，不经过 `TaskRouter`，也不创建 `GatewayRouteStore` route。proxy 的
   `task_id`、parent/root 关系、状态、预算和 uncertainty 都是 runtime 的状态。
3. `A2AClient` 只负责跨进程 transport、认证、wire error 和 stream 边界；它既不
   决定委派预算，也不再拥有一份独立的 Task 状态。上游 Task ID 是 proxy 的绑定
   身份，不取代本地 Task identity。

## 装配与入口调用方

[`bootstrap`](../../src/ruyi_agent/runtime/bootstrap.py) 创建 stores、checkpoint、
backend、A2A client 和 [`AgentControl`](../../src/ruyi_agent/runtime/delegation/async_runtime.py)。
`AgentControl` 是 Gateway 和 runtime 内部调用方共用的稳定 facade；它把调用转给
`TaskRuntime`，而不让调用方直接持有 `TaskManager`、`RemoteTaskPort` 或
`RunSupervisor`。

当前调用者有四类：

1. Gateway task service/route workflow 通过 `AgentControl` 创建、读取、输入、取消
   和 refresh Task。Gateway 的 route/command 选择和 public projection 仍留在
   Gateway；runtime 只执行 facade 请求或维护 proxy。Gateway 与 runtime 的全局
   依赖方向见 [架构总览的依赖方向](../architecture.md#5-依赖方向)。
2. 本地 Agent run 中的 delegation middleware 将带有 LangGraph `configurable`
   上下文的调用交给 `DelegationTools`。工具再以 `TaskCommandPort` 调用同一个
   `TaskRuntime`，所以模型不能绕过作用域或直接写 Task。
3. 远端状态 webhook/事件入口把 payload 交给
   `AgentControl.handle_remote_task_event`；远端读取、message page 和 Task event
   stream 也经同一 facade 进入 `RemoteTaskPort`。这是 runtime proxy 的同步入口，
   不是创建 public Gateway route 的入口。
4. Channel adapter 只调用 Gateway 的
   [`GatewayHTTPClient`](../../src/ruyi_agent/channels/gateway_client.py)。即使
   Gateway、channel 和 runtime 在同一进程，adapter 也不取得 `AgentControl`，不
   直接调用 delegation tools。

可以把调用拓扑压缩为：

```text
bootstrap
  └─ AgentControl
      └─ TaskRuntime
          ├─ AgentRegistry ── target resolution
          ├─ DelegationPolicy ── context/tree/budget
          ├─ DelegationTools ── target allowlist + caller scope
          │    └─ TaskCommandPort ──┘
          ├─ TaskManager ── TaskStore / TaskEventLedger
          ├─ RunSupervisor ── LocalTaskExecutor ── local worker
          └─ RemoteTaskPort ── A2AClient ── remote Gateway

Gateway TaskRouter/CreateRouteWorkflow ── AgentControl
```

## AgentRegistry 与目标作用域

### 目标命名空间

启动时，已解析的 `LocalWorkerSpec` 和 `RemoteRef` 被放入同一个
`AgentRegistry`。目标名称不能同时被模型当作两个不同 namespace 的名称：

| registry entry | 执行形态 | 可见/可用表现 |
| --- | --- | --- |
| `LocalWorkerEntry` | 当前 runtime 编译并运行 local Agent | `kind=worker`，有 model/tools/memory/skills 和 delegation 配置 |
| `RemoteRefEntry` | `RemoteTaskPort` 经 A2A 建立远端任务 | `kind=remote_ref`，显示 `runtime=remote_gateway` |
| unavailable local target | 配置存在但 local spec 构造失败，例如 provider credential 缺失 | 不出现在可执行 entry；保留原因，spawn 返回明确 unavailable 错误 |
| 未登记名称 | 没有对应配置 | spawn 返回 unknown target，并给出当前可用名称 |

bootstrap 构造 local spec 时可以按 Agent 粒度捕获构造错误并登记
`unavailable_agents`；一个不可用 worker 不会把其他 worker 或 remote ref 从 registry
中删除。`get_spec` 只允许 local worker；对 `remote_ref` 的本地执行请求会被拒绝，
避免把 transport target 当成 local model。

每个已编译的 local Agent（包括 `main`）都通过 `build_tools_for(agent_name)` 按
自身的 `delegation_targets` 构造 delegation tools；白名单同时覆盖 local worker
和 remote ref，并在 tool description 中分别列出两类目标。`build_tools()` 使用
全量目标，仅是兼容/直接入口，不代表已编译 Agent 的默认 scope。
`AgentRegistry` 负责目标解析和注册项，`DelegationTools` 负责 spawn 的目标
allowlist 与注册目标展示。已有 Task 的操作 scope 由已解析 caller 的 parent/child
关系决定；只有没有 caller、退回 `parent_thread_id` 的路径才同时按 allowlist 过滤
可见 Task。

### 调用者与 Task 可见性

工具从 `RunnableConfig.configurable` 解析 `task_id` 和 `thread_id`：

- 有效 `task_id` 优先映射到 caller Task；
- `task_id` 缺失或无法映射时，可按 `thread_id` 找到当前 Task，作为受限兜底；
- 两者都没有时，没有 caller Task，只有带同一 `parent_thread_id` 的可懒加载 child
  可见；无 thread context 则不向模型泄露 Task 列表。

有 caller Task 时，`list_agents` 和 scope 解析最多涉及 caller、它的直接 parent
以及 `parent_task_id == caller.task_id` 的直接 children。具体控制关系是：

| 工具 | 当前 caller 可作用的 Task |
| --- | --- |
| `wait_agent` | 直接 child；等待当前 run settlement |
| `check_agent` | 直接 child；非阻塞读取当前状态 |
| `cancel_agent` | 直接 child；取消 child 当前 run |
| `send_input` | 直接 child，或直接 parent；不能发送给 sibling/祖先的其他层级 |
| `list_agents` | 当前 scope 内的注册目标和 caller/parent/children 中可见记录 |
| `spawn_agent` | 目标必须在当前 Agent 的 `delegation_targets` 中；新 Task 的 parent 是 caller |

这里的目标 allowlist 只约束 `spawn_agent` 及注册目标的展示；caller Task 已经解析
后，`wait_agent`、`check_agent`、`send_input`、`cancel_agent` 和已跟踪 Task 的
`list_agents` 不再按 allowlist 过滤，而只按上表的 caller/parent/child 关系作用。
没有 caller 而按 `parent_thread_id` fallback 时，Task 列表才会再与 allowlist 求交。
因此 child 可以用 `send_input` 向 direct parent 发送澄清，但不能对 parent 使用
`wait_agent`、`check_agent` 或 `cancel_agent`。sibling、其他 thread 的 Task 和未
授权目标不会因为 Task ID 可猜测而变得可见。越权/未知 ID 的工具结果只列出当前
scope 的允许 IDs，不回显无关 Task 的状态。

`waiting_for_human` 是 runtime 的真实控制面状态；工具文本将其表现为
`state=running`，以免模型把“等待用户 review”误判为已经结束并重新委派。HTTP
或 Gateway 读取仍保留真实 `waiting_for_human` 状态。

## DelegationContext 与 DelegationPolicy

### Context 字段与传播

一棵委派树有一个不可变的 `DelegationContext`：

| 字段 | 语义 |
| --- | --- |
| `root_id` | 跨网关链路的委派树根身份；本地根通常由 `node_id:root_task_id` 构成 |
| `depth` | 当前 Task 深度；根为 1，child 在 parent 基础上加 1 |
| `max_depth` | 本链路允许的深度上限 |
| `max_tasks_per_root` | root 下所有 Task（含 root）共享的累计数量上限 |
| `visited_nodes` | 已经过的 Gateway node ID，防止跨网关回到同一节点 |

local run 的 `configurable` 传播至少包含 `thread_id`、`task_id`、
`parent_task_id`、`root_task_id`、`delegation_depth`、`agent_name`、权限 profile
和 skill view 信息。工具据此恢复 parent；若直接传入的 `task_id` 不可用，才以
`thread_id` 做防御性 fallback。`parent_thread_id` 单独保存在 child Task 上，用于
parent-child visibility 和 settlement 协作；它不是 Task identity。

跨 Gateway 时，`RemoteTaskPort` 把 context 写入保留 metadata 字段
`_deepagents_context_version`、`_deepagents_root_id`、`_deepagents_depth`、
`_deepagents_max_depth`、`_deepagents_max_tasks_per_root` 和
`_deepagents_visited_nodes`。出站注入会先删除调用方 metadata 中的同名字段，调用方
不能覆盖 root、depth、预算或访问路径。入站如果发现任意保留字段，就要求整组字段
完整、版本为当前版本、整数/节点 ID/JSON visited list 合法；解析后剥离保留字段，
避免协议控制字段进入用户 metadata。

入站 node 取上游预算与本地预算的较小值：下游不能放宽上游已经施加的
`max_depth` 或 `max_tasks_per_root`。当前 node 已在 `visited_nodes` 时拒绝环路；
接受后才把当前 node 追加到路径。visited list 有长度上限，node/root 字符串也有
输入长度校验，因此 metadata 不是绕过预算的自由格式。

### Policy 的职责

`DelegationPolicy` 只计算和守护委派树结构约束，不调度 run，也不执行 transport
I/O；目标名称解析和目标 allowlist 不是它的职责，而由 `AgentRegistry` 与
`DelegationTools` 完成。它：

- 从 run config 解析 caller Task/thread；
- 为 root 创建 context，为 child 继承 root/预算/visited 并递增 depth；
- 按 local/remote entry 和 parent profile 解析有效 permission profile；
- 调用入站 metadata 校验，并把本地限制与上游限制合并；
- 在创建 Task 前检查 `depth <= max_depth`，并为每个 root 提供预算锁。

数量预算是整棵树的累计预算，而不是每个 parent 的 child 数量。持久化模式下，
`TaskStore`/repository 在 immediate transaction 中按 `root_task_id` 统计已存在的
记录并再次执行上限检查；因此 lazy restore、多个 runtime instance 或并发 spawn
不能通过各自的内存缓存超卖最后一个 slot。无持久 store 的测试/进程内模式仍由
TaskManager 统计当前记录。

## 两类 child 流程

两条流程共享目标解析、caller scope、context、permission、root budget 和 Task
identity 检查；区别从 Task 建档后的 execution leg 开始。

### Local worker child

```text
模型 spawn_agent
  -> AgentRegistry/DelegationTools: target resolution + allowlist
  -> TaskRuntime + DelegationPolicy: parent/tree/context/budget
  -> TaskManager: pending local TaskRecord
  -> RunSupervisor: admission + mark running + run_count += 1
  -> LocalTaskExecutor: 编译/调用 local worker
  -> TaskManager: completed / failed / waiting_for_human / cancelled / interrupted
  -> parent 协作通知（若配置）
```

1. `spawn_agent` 从 config 取 caller Task/thread，并把 caller 作为
   `parent_task_id`；没有 caller 时创建 root Task。TaskManager 先建立 `pending`
   记录，写入 root/depth/parent/context 等字段。
2. `RunSupervisor` 在 release gate 下把 Task 标为 `running`、登记 process-local
   run handle 并递增 `run_count`；状态写入成功前不会释放 Agent payload，避免
   “模型已运行但 Task 还没有 running 记录”。
3. `LocalTaskExecutor` 按 registry 的 local spec 编译 Agent，使用 Task 的
   `thread_id` 和 run config 执行一次；child 本身使用自己的 Task/thread identity，
   parent relationship 由 `parent_task_id`/`parent_thread_id` 表达。
4. 一次 run 只允许一个 review payload；单个 review 进入
   `waiting_for_human`，多个 review 或未解析 tool call 进入 `failed`。review
   decision/resume 或 settled follow-up 会在同一 Task 上开启下一代并递增
   `run_count`，不会隐式创建新的 sibling。
5. Task settled 后，runtime 通过 mailbox-facing notifier 将 child settlement 暴露
   给 parent。`wait_agent` 在等待开始时即抑制当前 run 后续 settlement 的 parent
   回投；`check_agent` 只有在读取到 settled 状态时才接管这次回投，避免同一结果
   同时走主动等待和后台通知。mailbox 与 settled outbox 的 claim/settle/recovery
   状态不属于本文。

### Remote-ref child / local proxy

```text
模型 spawn_agent
  -> AgentRegistry/DelegationTools: target resolution + allowlist
  -> TaskRuntime + DelegationPolicy: parent/tree/context/budget
  -> TaskManager: pending remote_ref proxy（本地 task_id）
  -> RemoteTaskPort: persist create intent
  -> A2AClient: remote create + injected context
  -> validate payload -> bind upstream_task_id -> sync proxy
  -> refresh / event / webhook / input / review / cancel
  -> TaskManager: verified state 或 interrupted + uncertain
```

1. runtime 先建立本地 `TaskRecord`，`route_kind="remote_ref"`，保留本地
   `task_id`、parent/root/context 和 `parent_thread_id`。proxy 的 `thread_id` 也保持
   本地 Task ID；远端 Task ID 只写入 `upstream_task_id`，不会把远端私有 ID 当成本地
   public/runtime identity。
2. `RemoteTaskPort.allocate_task` 在 A2A 请求前持久化 operation=`create`、稳定
   operation identity 和当前 run count。它用 `inject_context_metadata` 透传委派
   context，并按 remote ref 能力传递 idempotency key、attachments 和远端 webhook
   配置。
3. create 响应必须包含非空 upstream `task_id`，且 status/run_count 符合 Task
   contract；通过后才绑定 upstream identity 并同步 proxy。后续 input、review、
   cancel、refresh 和 Task event 都使用这个绑定，不允许调用方把另一个 upstream
   Task 的 payload 写进 proxy。
4. 远端 settled 状态仍触发 parent 协作接口和配置的 caller webhook；远端事件按
   upstream identity 找到本地 proxy，再由 TaskManager 同步。RemoteTaskPort 也可
   通过 A2A 打开远端 Task event stream 或读取 message page；stream/cursor 的公共
   wire 投影仍由 Gateway/protocol 边界校验，runtime 不把 upstream cursor 当 Task ID。
5. remote proxy 不调用 `TaskRouter`，不建立 `GatewayRouteStore` route。若同一进程
   同时存在 public Gateway remote route，那是另一条 Task ownership 链路，不能把两
   个 TaskRecord/route ledger 合并解释。

## 工具行为：spawn、wait、check、send、cancel、list

工具是给模型的控制接口，不是额外的 Task owner。tool wrapper 先做目标/Task scope
校验，再调用 runtime facade；参数、未知目标、预算和 A2A 错误通常转换为模型可读
文本，避免单个工具参数错误打断整轮 Agent。

| 工具 | 阻塞/效果 | 结果和安全边界 |
| --- | --- | --- |
| `spawn_agent` | 创建 local child 或 remote proxy，立即返回 task ID/route | 目标必须注册且在 caller 白名单；深度/树预算在建档前拒绝；不等待结果 |
| `wait_agent` | local 等 process-local run；remote 周期 refresh | 等待开始即接管当前 run eventual settlement delivery；settled 返回状态/结果；review 无 resolver 时返回 agent-facing `running`，不会无限等待；等待者取消不会取消被监督 run |
| `check_agent` | 非阻塞读取；remote 先 refresh | 只有本次读取到 settled 才接管 settlement delivery；返回最近已知状态；远端暂时不可用时保留该状态并附 warning，不伪造失败 |
| `send_input` | 向 direct parent/child 发送后续 input | local settled Task 复用同一 Task/thread 开新 run；active local run 通过 mailbox 在安全边界接收；remote 转发 A2A input；同 Task 并发 run 被拒绝 |
| `cancel_agent` | 取消 direct child 的当前 run | local 设置 cancel marker 并取消 handle；remote 转发 cancel；settled Task 是 no-op；cancel 不销毁长期会话 |
| `list_agents` | 无外部 Task effect | 列出当前目标 scope 和可见 caller/parent/children；不列 sibling、其他 thread 或无关 Task |

`wait_agent` 在等待开始时就调用 notifier 的抑制入口，接管当前 run 的 eventual
settlement delivery；`check_agent` 只有在本次读取确认 child 已 settled 时才调用
该入口。抑制只表示主动观察已经接管 child settlement 的 parent 协作，不改变
Task state，也不取消远端 webhook；mailbox/outbox 的具体状态机在其所属边界。

## 状态、并发、预算与恢复

### Task 状态与代次

本地 proxy 与 local Task 共用 canonical Task states：

| 状态 | 委派语义 |
| --- | --- |
| `pending` | 已建立 Task/proxy，但当前代次尚未开始或远端 create 尚未同步 |
| `running` | local run 已被 supervisor 接纳，或 proxy 观察到远端运行 |
| `waiting_for_human` | 当前 Task 有 pending review；普通 `send_input` 不能绕过 review |
| `completed` / `failed` | 当前代次正常结束或发生权威执行/输入错误；两者均可继续同 Task input |
| `cancelled` | 调用方显式取消当前代次；Task identity 和会话仍保留 |
| `interrupted` | 被动取消、shutdown/restart，或 remote effect 结果未证明；可能带 uncertainty marker |

`run_count` 是 Task 内的 execution generation：local 在 `mark_running` 时递增；
remote 不自行猜测，而采用已验证 payload 的非负 `run_count`。同一 Task 的 review
resume 或 settled follow-up 不改变 `task_id`/thread identity，只进入下一代。Task
record、lifecycle event、parent settlement 和工具状态都必须以这一代为界，避免旧
run 的尾部结果覆盖新 run。

### 并发与 admission

所有可能改变 Task 或开始 remote effect 的 mutation 通过 `RunSupervisor` admission：

- 短 mutation 持有与当前 asyncio caller 绑定的 mutation permit；等待 root budget
  lock 或网络 I/O 时提升为受跟踪的 operation permit；shutdown 可定位并取消这些
  operation；
- root budget lock 串行化同一委派树的建档；持久 store 再用数据库 immediate
  transaction 做跨 runtime 的最后 slot 检查；
- local Task 每次只允许一个活跃 run handle。`mark_running` 写入失败会关闭 release
  gate、回收 handle 并恢复受保护状态，payload 不会偷偷执行；已有 active run 时
  新 schedule 得到 `TaskAlreadyRunningError`；
- wait 使用 shield 观察被监督的 run，观察者取消不会把 child 误取消；显式
  `cancel_agent` 才设置 cancel marker。runtime close 则是被动 interruption，不等同
  业务 cancel；
- remote proxy 在同一时间只允许一个 unresolved external operation。外部 operation
  的 identity 和 run count 先落在 TaskRecord，防止并行 input/review/cancel 把不同
  effect 混成一个结果。

### 恢复与关闭

TaskStore 保存 Task identity、parent/root/depth/context、route kind、upstream binding、
state/run_count、review 和 remote operation marker；process-local `asyncio.Task`、
permit、compiled Agent cache 不在 `TaskRecord` 中。

- 恢复 local `pending`/executing state 时，由于原 run handle 已不可恢复，Task 变为
  `interrupted`，并保留可读的 restart/interruption error；不会假装 local Agent 仍在
  运行。
- 恢复带 unresolved remote operation 的 proxy 时，保留 operation/identity，变为
  `interrupted` + uncertain，要求 refresh/reconciliation；没有证明 effect 前不盲目
  重新 create/send/review/cancel。
- 已绑定 upstream 的 remote proxy 可以在新 runtime 中按本地 proxy ID refresh；
  remote response 先做 identity/state/run_count 校验，成功后再清除 uncertainty。
  uncertain create 只有在 remote ref 声明 `ruyi_gateway_v1` capability 时才可以
  复用持久 idempotency key 重试；没有该 capability 时保持待 reconciliation，拒绝
  可能重复的 create。
- root budget 从持久 Task tree 统计，不依赖本次进程是否已 lazy-load 全部 child。
  bootstrap/runtime 的 mailbox recovery hook 可以重新唤醒有待处理协作输入的 local
  Task；这不等于恢复失去的 process-local run handle。
- close 先停止 accepting 新 mutation，取消受跟踪的 external operation，等待短
  mutation 清空；local run 在 grace period 内自然完成，超时后取消并把未收口 run
  标为 `interrupted`，再清理本地 handles/cache。已提交的 Task state 不因尾部协作
  通知失败而倒写。

## RemoteTaskPort：effect boundary、identity 与 reconciliation

### Effect boundary

RemoteTaskPort 不把网络异常当作“肯定没创建”。每项 create/input/review/cancel
先调用 TaskManager 记录 operation、identity 和 run count，再进入 A2A：

| A2A 结果 | 本地 proxy 处理 |
| --- | --- |
| `not_dispatched`，例如缺少 credential、无效 URL、连接尚未建立 | 清除匹配的 external intent；create 等已知拒绝按 operation 语义处理，不重写成成功 |
| authority 明确拒绝，例如已知 4xx code | 清除 intent；create 记为 remote create failed；其他操作保留原权威 Task state 并把错误返回调用方 |
| `possibly_dispatched`、read/write timeout、server/malformed response、调用者取消 | Task 置 `interrupted`，保留 operation/identity 和 `external_outcome_uncertain=true`；禁止普通网络重试造成重复 effect |
| 后续 refresh/event/webhook 证明 effect | 按已验证远端 payload 同步 state/run_count/review/result，清除 uncertainty marker |

`send` 使用同一持久 operation identity 时可以做精确 replay/reconcile；remote
create 只有声明了 `ruyi_gateway_v1` 的 reference 才有同等重试依据。这里的“可重试”
是对同一 identity 的查询或幂等 replay，不是对未知 create 重新生成一个 Task。

### Identity validation 与安全投影

RemoteTaskPort 在 get/send/review/cancel 的响应写入前，要求 payload 的 `task_id`
等于已绑定的 `upstream_task_id`；所有远端状态 payload 还必须使用 canonical
state，且 `run_count` 是非负整数。不同 Task 的 payload、非法 state/run_count 或
与已有本地 review 冲突的 identity 在进入 TaskManager 前拒绝，因此不会污染本地
proxy。

远端 review payload 若带 `source_task_id`，进入本地 pending review 时改为本地 proxy
`task_id`；proxy 的 `thread_id` 保持本地 identity。远端错误只保留受限的公共摘要，
不把 bearer、remote reference credential 或上游私有 Task identity 写入工具文本、
本地 webhook 或 Task public projection。公开 Gateway remote route 的完整顶层 public
rewrite 则由 Gateway router/projection 所属边界完成。

### Refresh、事件和 webhook

- `refresh_task` 通过 remote ref 的 upstream ID 查询；runtime 对临时 A2A status
  failure 按配置的有限次数和间隔重试。`wait_agent` 对 remote proxy 循环 refresh，
  `check_agent` 只做一次 facade read；失败时都保留最后已知记录并说明 status
  temporarily unavailable。
- `handle_remote_task_event` 先按已绑定 upstream ID 查找 proxy，找不到则忽略未知
  事件；找到后走同一 identity/state/run_count 校验和 TaskManager sync。settled
  后触发 parent 协作接口及已配置的 caller webhook，二者是已提交 Task state 之后
  的非权威尾部效果。
- `RemoteTaskPort` 也提供远端 message page 和 Task event stream 的访问；A2A client
  透传不透明 cursor，Gateway/protocol 在其 public projection 边界校验 fresh/resume、
  event identity 和 stream lifecycle。runtime 不把 SSE cursor 解码为 delegation
  context，也不把 stream 当成另一份 Task store。

## 错误安全与一致性边界

错误处理遵循“先保护 Task identity/authority，再处理协作尾部效果”：

- unknown/unavailable agent、目标白名单、parent scope、深度/预算和 TaskAlreadyRunning
  在 effect 之前拒绝；不会为越权调用启动 Agent 或网络请求；
- metadata 缺字段、版本错误、类型错误、visited loop 或超深 context 不能进入
  runtime；Gateway 对公开入口再把这些异常投影为其自己的错误 envelope；
- TaskManager 的 Task/review/lifecycle transition 通过同一 TaskStore UoW 或受保护
  的内存 transaction 写入。状态/event 写失败时，不留下“工具已返回但 Task 未建档”
  的半状态；`mark_running` 失败时 run payload 仍被 gate 拦住；
- local Agent 异常在 Task 仍 running 时规范化为 `failed`；显式取消为 `cancelled`，
  被动取消/重启为 `interrupted`。已 settled 后的 audit、mailbox、webhook、wakeup
  或 transient fan-out 失败只记录/日志，不把已提交状态倒回；
- remote response 的 identity/state/run_count 校验失败时不调用 sync；操作 effect
  是否发生不确定时，本地保留 uncertainty 和查询 identity，而不是把异常伪装成
  authoritative failed 或 completed；
- `wait_agent`/`check_agent` 的远端 status 不可用只影响观察结果，不擦除最后已知
  Task；tool scope 错误只暴露当前 caller 允许的关系和 IDs；凭据值永不进入错误。

Task row、lifecycle event、checkpoint、Gateway route、mailbox/outbox 和远端 effect
不构成一个跨系统 two-phase transaction。runtime 可以保证自身 Task/事件边界和
remote intent 的可查询性，但不宣称“Task + route + checkpoint + A2A + mailbox”
原子或 exactly-once。关于 Task/event 的一般 UoW 和 runtime shutdown 背景见
[Task execution runtime 的一致性边界](task-execution.md#durable-一致性与事务边界)。

## 测试证据

下列文件是当前行为证据入口；它们是契约索引，不表示本文编辑时执行过测试：

| 关注点 | 证据 |
| --- | --- |
| root/child context、保留 metadata 清理、有效预算、loop/depth/visited 校验 | [`test_delegation_context.py`](../../tests/unit/test_delegation_context.py) |
| root identity、parent/depth、local/remote child、树预算、并发 slot、same-thread continuation、cancel/interruption、restart 和 remote refresh | [`test_async_subagent_task_runtime.py`](../../tests/unit/test_async_subagent_task_runtime.py) |
| model tools、目标白名单、caller visibility、direct-parent send 与 parent cancel/wait/check 拒绝；settlement/suppression 协作入口 | [`test_async_subagent_tools_and_delivery.py`](../../tests/unit/test_async_subagent_tools_and_delivery.py) |
| local executor、run config context、local stream/review/resume 和 safe completion | [`test_async_subagent_local_executor.py`](../../tests/unit/test_async_subagent_local_executor.py) |
| RemoteTaskPort A2A create、refresh retry、last-known status、remote event、caller webhook relay | [`test_async_subagent_remote_port.py`](../../tests/unit/test_async_subagent_remote_port.py) |
| remote payload identity、public/local projection、uncertain operation、webhook/outbox 安全边界 | [`test_remote_record_trust_boundary.py`](../../tests/unit/test_remote_record_trust_boundary.py) |
| A2A HTTP method/credential/error/effect-boundary 和 response close | [`test_a2a_client.py`](../../tests/unit/test_a2a_client.py) |
| A2A Task-event SSE 的 cursor、handshake、stream error/end、取消清理 | [`test_a2a_task_events.py`](../../tests/unit/test_a2a_task_events.py) |
| AgentControl facade、runtime ownership、remote network isolation 和 boundary split | [`test_delegation_runtime_boundaries.py`](../../tests/unit/test_delegation_runtime_boundaries.py) |
| admission permit、单 Task 并发、mark-running rollback、caller cancellation、graceful/forced close | [`test_run_supervisor.py`](../../tests/unit/test_run_supervisor.py) |
| TaskRecord 不携带 process-local handle，以及 canonical state/storage contract | [`test_task_model_boundaries.py`](../../tests/unit/test_task_model_boundaries.py)、[`test_task_state_contracts.py`](../../tests/unit/test_task_state_contracts.py) |

## 同步触发

以下稳定行为变化必须同步本文，并调整相应证据链接：

- `LocalWorkerSpec`、`RemoteRef`、AgentRegistry 的目标 namespace、unavailable 处理、
  `delegation_targets` scope 或目标 allowlist/注册目标展示改变；
- `DelegationContext` 的字段、metadata 保留名、版本/长度/visited 校验，或
  `DelegationPolicy` 的 root/depth/task budget、permission inheritance 改变；
- delegation tool 名称、参数/返回语义、spawn/target 展示 allowlist、caller
  visibility、direct parent/child 控制关系、wait/check suppression 时机改变；
- `TaskRecord` 的 parent/root/depth/route/upstream/uncertainty 字段、canonical state、
  `run_count` 或 local/remote child lifecycle 改变；
- `TaskRuntime`、`TaskManager`、`RemoteTaskPort` 的 admission、concurrency、refresh、
  webhook/event、identity validation、effect disposition、reconciliation、restart
  或 close 语义改变；
- runtime 与 parent mailbox/settled outbox 的协作接口改变，或 Gateway remote create
  与 runtime internal delegation 的 ownership 边界改变。

仅仅改变 mailbox/outbox 的内部 claim/settle/recovery、A2A wire codec、Gateway route
ledger 或 channel delivery 细节时，应更新其所属实现/文档；除非同时改变本文列出的
runtime delegation contract，不在本文复制那些状态机。实现与行为测试是事实权威；
当前文档链接只指向仓库中已存在的文件。
