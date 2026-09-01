# Gateway Task 控制面

本文描述当前 Gateway 的 transport-neutral Task 控制面：它如何把 Agent
catalog、Task command、route、runtime effect、查询投影和恢复组织成一个稳定的
应用 facade。本文以当前代码和行为测试为事实来源；跨子系统的装配背景见
[架构总览](../architecture.md)，HTTP 与 `gateway_protocol` 的传输边界见
[Gateway HTTP 与 `gateway_protocol`](gateway-http-protocol.md)，Agent 执行面见
[Task execution runtime](task-execution.md)。本文记录现有行为，不定义未来版本。

Gateway 是外部 Task 的控制面，runtime 是 Agent 执行与协调面。两者可以在同一进程
中由 bootstrap 装配，但各自拥有不同的状态、错误和恢复边界。

## 1. 负责范围与明确边界

Gateway Task 控制面负责把一次应用级操作安全地编排到正确的 local 或 public
remote route，并给调用方一个以 Gateway public identity 为控制面边界的结果。它负责：

- 以 [`GatewayTaskModule`](../../src/ruyi_agent/gateway/tasks.py) 作为稳定 facade，
  组合 Agent、Task、command、listing、review 和 artifact application service；
- 从已解析的 Agent 配置建立 public catalog，并表达 Agent 的 kind、default 和
  bootstrap availability；
- 编排 create、input、cancel、Task 查询/列表、message/event 观察、review 和
  artifact 操作；
- 预留并激活 route，维护 public Task id 与 local/runtime 或 upstream Task 的绑定，
  处理 route reconciliation 和 effect disposition；
- 通过 [`GatewayCommandStore`](../../src/ruyi_agent/storage/gateway_command_store.py)
  为带 `Idempotency-Key` 的 create/input 做 claim、基于持久化记录的安全重投影、冲突
  和不确定效果处理；
- 通过 [`GatewayProjection`](../../src/ruyi_agent/gateway/application.py) 生成稳定的
  Agent、Task、Review 和 Artifact response projection，并参与 remote boundary 的
  错误和控制面 identity sanitization。

以下行为不属于本控制面：

| 不负责的行为 | 权威边界 |
| --- | --- |
| HTTP auth、HTTP status、header/envelope、request parsing、SSE framing 与连接生命周期 | `channels/http` 和 [`gateway_protocol`](../../src/ruyi_agent/gateway_protocol/sse.py)；整体传输说明见 [Gateway HTTP 与 `gateway_protocol`](gateway-http-protocol.md) |
| DTO 的 wire serialization、SSE/event cursor 的 wire codec | [`gateway_protocol`](../../src/ruyi_agent/gateway_protocol/contracts.py)、[`cursor.py`](../../src/ruyi_agent/gateway_protocol/cursor.py) 和 [`sse.py`](../../src/ruyi_agent/gateway_protocol/sse.py)；HTTP client/transport 的组合属于传输文档，本页不拥有；listing/review/local-message cursor 的应用语义和 codec 属于 Gateway service |
| Agent turn 执行、run admission、Task canonical state machine、middleware 和 checkpoint 生命周期 | [`AgentControl`](../../src/ruyi_agent/runtime/delegation/async_runtime.py) 及 runtime；见 [Task execution runtime](task-execution.md) |
| Task row、lifecycle event、pending review 和 LangGraph checkpoint 的持久化 | runtime 的 TaskStore、TaskEventLedger 与 checkpointer；Gateway 只通过 runtime facade 读写或订阅 |
| A2A HTTP/SSE transport 的实现、认证读取和下游 wire codec | [`A2A client`](../../src/ruyi_agent/integrations/a2a/client.py) 与 runtime remote port；Gateway 只使用 remote port 并执行 public projection |
| Telegram/Feishu session、event receipt、delivery、平台消息发送和 channel state | channel adapter 与 channel stores；Gateway Task idempotency 不替代 channel receipt/delivery |
| tool permission、Agent 内部 delegation policy、skills、mailbox 和 settled outbox 的执行语义 | runtime、permission、skills 与 mailbox 边界 |
| runtime 内部 worker delegation | runtime 的 `TaskRuntime`/`RemoteTaskPort`；它不是 public Gateway route，也不经过本页的 route ledger |

因此，“Gateway 负责投影 Task”不等于 Gateway 负责执行 Task；“Gateway 调用远端”
也不等于 Gateway 拥有 A2A transport。

Cursor 的所有权按用途划分，而不是由 `gateway_protocol` 统一拥有：Task listing 的
ordering/offset 语义和 codec 由 [`listing.py`](../../src/ruyi_agent/gateway/listing.py)
负责，pending review 的稳定分页 cursor 由
[`reviews.py`](../../src/ruyi_agent/gateway/reviews.py) 负责，local message 的
Task/checkpoint/offset 绑定和 codec 由
[`message_cursors.py`](../../src/ruyi_agent/gateway/message_cursors.py) 负责。
`gateway_protocol` 只负责 DTO wire codec 以及 SSE/event cursor 的 wire codec。

## 2. 入口调用方与依赖方向

[`bootstrap_application`](../../src/ruyi_agent/runtime/bootstrap.py) 创建
`AgentControl`、route/command/task stores、checkpointer、backend 和
`GatewayTaskModule`，并在 lifespan 中保持它们的生命周期。HTTP route composition
从 application state 取得 facade；channel adapter 通过 `GatewayHTTPClient` 访问
Gateway 的公开 HTTP 面；测试和其他 in-process embedder 可以直接调用 facade。调用
方不取得 Gateway 的 SQLite connection，也不绕过 route/command 进入 runtime。

```text
HTTP server adapter / channel Gateway client / in-process embedder
                              │  typed application call
                              ▼
                     GatewayTaskModule
          ┌───────────────┼────────────────┐
          ▼               ▼                ▼
   Agent/catalog      Task services    command/list/review/artifact
          │               │                │
          └───────────────┴────────────────┘
                          ▼
                    TaskRouter
              ┌───────────┴───────────┐
              ▼                       ▼
       CreateRouteWorkflow       route/command ledgers
              │
              ▼
         AgentControl (runtime port)
          ┌───────────┴───────────┐
          ▼                       ▼
    local Agent runtime     remote port -> A2A boundary
           │                         │
           └──── local TaskRecord / remote proxy TaskRecord ────┘
```

同进程只表示共享 bootstrap/lifespan；它不改变调用方向。channel 仍通过 Gateway
client 访问公开面，runtime worker 仍通过 runtime 自己的 delegation port 访问
runtime。public remote route 与内部 worker delegation 的差异见第 10 节。

## 3. 组件与所有权

| 组件 | 控制面职责 | 不替代什么 |
| --- | --- | --- |
| `GatewayTaskModule` | 稳定 facade，组合应用服务和共享 context；向 caller 暴露 Agent/Task/Review/Artifact 的应用操作 | 不解析 HTTP，不持有 transport connection |
| `GatewayApplicationContext` | 保存 typed Agent config、主 Agent、`AgentControl`、`TaskRouter`、stores、limits 和 availability map | 不承载业务行为或状态机 |
| `GatewayAgentService` | 过滤 public catalog，投影 Agent，执行 public/available admission | 不探测每次 run 的实时健康，不执行 Agent |
| `GatewayProjection` | 把 `TaskRecord`、route metadata、pending review 和 artifact manifest 组装成稳定 public response，并参与 remote boundary 的 sanitization | 不编码 [`gateway_protocol`](../../src/ruyi_agent/gateway_protocol/projection.py) event，也不持久化 response |
| `GatewayTaskService` | 组织 create effect、Task read、input/cancel、message/event 观察、附件准备和远端 webhook | 不决定 runtime Task 状态迁移 |
| `GatewayCommandService` | 为 create/input 建立 idempotent claim，调用 effect，保存可安全重投影的成功/终态结果或释放/失败 claim | 不把没有 key 的旧路径变成自动幂等 |
| `GatewayListingService` | 从 route ledger 收集 Task，按 durable route 字段预过滤，刷新 local/remote record，排序和分页 | 远端刷新失败项静默省略；普通 response 没有 partial/completeness 标记，调用方不能据此推断全量 snapshot |
| `GatewayReviewService` | 列出/读取 pending review，刷新 review owner，校验 Task/root 归属并提交 decision | 不拥有 review transition 的 Task UoW |
| `GatewayArtifactService` 与附件 service | 验证 workspace 边界、准备 inbound attachment、按登记的 artifact manifest 读取受限 bytes | 不实现 binary HTTP framing 或远端 A2A artifact transport |
| `TaskRouter` | 以 public Task id 查 route，选择 local/remote operation，恢复缺失 route，投影 runtime/upstream error | 不拥有 Task state machine 或 A2A wire 实现 |
| `CreateRouteWorkflow` | 在 create effect 前持久化 reservation/evidence，启动 effect，验证 durable binding，再 activation；负责 create crash window | 不把 route、Task row、checkpoint 和外部 effect 合成一个事务 |
| `GatewayRouteStore` | 保存 public route、upstream binding、route state、create evidence 和 safe route error | 不保存 Task lifecycle 或 command response |
| `GatewayCommandStore` | 保存 principal/key/request hash、claim token、effect marker、可安全重投影的 response/error replay record | 不保存 route 或 runtime Task state |
| `AgentControl` | Gateway 使用的 runtime port：spawn/send/cancel、record、review、message snapshot、event stream 和 remote proxy | 不拥有 Gateway public route/command 或 response projection |

`GatewayTaskModule` 的 facade 分解由
[`test_gateway_boundaries.py`](../../tests/unit/test_gateway_boundaries.py) 约束；
应用 service 的签名不依赖 FastAPI，但它们可以使用 `gateway_protocol` 的 typed
value model。DTO/SSE/event cursor 的 wire 编码、解析和 SSE 细节仍留在 transport
层；listing/review/local-message cursor 的应用 codec 仍由 Gateway service 持有。

## 4. Agent catalog 与 availability

bootstrap 从配置加载 Agent，并把声明式输入转为 typed `AgentConfig`；Gateway
边界只做一次必要的 legacy normalization/validation。catalog 中的每个 Agent 带
`local` 或 `remote_ref` kind、name、description 和 public 标记；主 Agent 名称只
用于 `is_default`，不改变 route 规则。

- catalog 列表只返回 `public=true` 的 Agent。private Agent 不是“暂时不可用”，而
  是不能被 public Gateway 调用。
- bootstrap 构建 local provider/runtime spec 时记录 unavailable reason。该 Agent
  仍可出现在 public catalog，带 `available=false` 和稳定的摘要原因；Gateway
  不因一个 Agent unavailable 而拒绝整个 catalog。
- create admission 先检查 public，再检查 availability；private target 产生
  `agent_not_public`，已登记但未能运行的 target 产生 `agent_unavailable`。远端
  target 若未注册到 runtime，则在 effect 边界得到 `runtime_unavailable`。
- `available=true` 是 bootstrap 时的可调用资格，不是对未来 provider、backend 或
  upstream effect 成功的承诺；effect 仍须经过 route/effect recovery。

这使 catalog identity、public policy 和 effect availability 分开：列表可以说明
事实，create 才决定当前是否允许启动 effect。

## 5. Public Task identity 与 route model

`TaskRecord.task_id` 是 Gateway 对外的 Task identity。create 在任何 local/remote
effect 之前确定它：带 command key 时使用 claim 的 proposed id，没有 key 时生成新的
id。这个 id 在 query、list、message、review、artifact 和错误 details 中保持稳定。

`TaskRouteRecord` 把这个 public id 绑定到 route：

| 字段/概念 | 语义 |
| --- | --- |
| `route_kind=local` | public Task 由本地 runtime 执行；route 内部可用相同的 Task id 作为本地 binding，不产生另一个 upstream public identity |
| `route_kind=remote_ref` | public Task 由配置的 remote Agent/上游 Gateway 执行；仍经 `AgentControl`/`TaskRuntime` 建立 local proxy `TaskRecord`，route 在 effect 返回并验证后额外保存 `upstream_task_id` |
| `task_id` | Gateway public identity，不能被上游 response 或 event 中的 id 覆盖 |
| `upstream_task_id` | 仅供 public remote route 与上游交互和校验；不能替代 public Task id，也不能作为控制面 identity 出现在未经清理的 public error/projection |
| `parent_task_id`、`root_task_id`、`depth` | runtime delegation tree 的 Task 关系；它们不是 route binding 或 channel session key |

route state 是独立于 runtime Task state 的控制面状态：

| route state | 控制面含义 | 对操作的影响 |
| --- | --- | --- |
| `pending` | public identity 已 reservation，但 effect 尚未形成可安全使用的 binding | 可查询 reservation；不能安全 dispatch 依赖 route 的 input/cancel/message/event/review |
| `active` | local effect 已有 durable run，或 remote effect 已有经验证的 upstream id | 允许按 route 执行和刷新 |
| `failed` | create 被权威拒绝、确认未开始，或 create route 已终止 | 保留 queryable identity；不重新 dispatch |
| `uncertain` | effect 或 route transition 的结果无法证明，或恢复找不到可信 binding | 只保留查询/恢复线索；`TaskRouter` 拒绝安全要求的 operation |

`GatewayRouteStore` 只允许受限 transition：reservation 可到 active、failed 或
uncertain，active 只能继续 active 或降为 uncertain；failed/uncertain 不被普通
调用重新激活。route binding 与 upstream id 不能被另一 Agent、另一 kind 或另一
upstream id 重绑。只有 reconciliation 在拥有充分 durable evidence 时才会推进一个
仍处于 pending 的 route。

## 6. 主流程

### 6.1 Create：先有 public identity，再有 effect

create 的应用编排如下，transport caller 只看到最终 typed response 或
`GatewayTaskError`：

1. `GatewayTaskService` 校验 Agent public/availability，交给 `TaskRouter` 解析
   delegation metadata；local attachment 先经过 workspace/inbox 准备，remote
   attachment 作为下游输入传递。
2. 若 caller 提供 `Idempotency-Key`，`GatewayCommandService` 以
   `(principal, key, operation, target, request_hash)` claim 一个固定的 public
   Task id；没有 key 的调用不经过 command ledger，每次调用都是新的 create。
3. `CreateRouteWorkflow` 在 GatewayRouteStore 中写入 `pending` reservation 和
   create evidence。此时 route 已可查询，但尚未可路由；reservation 失败时不会
   调用 runtime spawn。
4. 在调用 effect 前，command（若存在）在同一 command marker 更新中一起提交
   `effect_started`/`replay_safe`，route 另提交自己的 effect boundary marker。
   随后 `AgentControl`/`TaskRuntime` 建立 local `TaskRecord`；local path 启动本地
   runtime Task，remote path 将这个 record 作为同一 public id 的 remote proxy，由
   `RemoteTaskPort` 保存 external-operation/reconciliation marker 后再向上游发起
   create。远端若
   caller 没有 key 且 typed remote reference 声明已验证的 `ruyi_gateway_v1` capability，
   workflow 会生成稳定的下游 key；这不把无 key 的 Gateway call 变成可由 caller
   自动 replay 的 command。
5. 返回 record 必须证明 effect：local 要有非零 run 且不再是 pending；remote proxy
   要经 runtime reconciliation 验证 upstream Task id。验证失败会把 route 留在安全的
   失败/不确定分类，而不把一个 effect-less record 当作 active。
6. workflow 把 reservation activation 为 active，并写入已验证的 upstream binding
   （remote）或 local binding；随后 `GatewayProjection` 生成 public Task response。

如果同一个 key 重试而 route 已 active，workflow 读取既有 record 并安全重投影，
不会再次 spawn，也不承诺响应字段逐字相同。route 已 failed/uncertain 时，重试得到
不可安全创建的 terminal 结果，而不是以同一 public id 盲目创建第二个 effect。

### 6.2 Input：按 active route 发送一次后续 effect

input 先按 public Task id 找 route，并要求 active。带 key 时，command ledger 以
输入 body 的 request hash claim；local input 准备附件并生成稳定的 mailbox/message
identity，remote input 把 caller key 作为下游 idempotency key。effect 返回后，
command 保存持久化记录；同 key 的成功 response 从安全字段重投影并返回 `replayed=true`，
不重复 effect，也不承诺响应字段逐字相同。没有 key 的旧 input 路径直接发送，不能被
文档解释为自动去重。

runtime 会拒绝一个仍有 active run 的 Task；Gateway 只把这个 admission/error 作为
public Task command 结果投影，不自行改变 runtime state。remote proxy record 返回前
会经 runtime reconciliation、`public_errors` 和 Gateway projection 共同处理；raw
上游 error 和控制面 identity 不直接越过边界，normalized payload 仍是可能含下游
字节/标识的不可信任务内容。

### 6.3 Cancel：控制已绑定的 route

cancel 读取并 reconciliation route，只有 active route 才调用 `AgentControl` 的
cancel port。local/remote 的取消结果都由 `GatewayProjection` 统一成 public Task
response；upstream failure 由 public error projection 处理。cancel 当前不走
`GatewayCommandStore`，所以不能把它描述成与 create/input 相同的 idempotent
command；Task 是否变成 `cancelled` 或 `interrupted` 仍由 runtime authority 决定。

### 6.4 Query 与 list：先稳定身份，再刷新观察

单 Task query 先从 route ledger 取得 public route。pending/failed/uncertain route
会先执行不启动 effect 的 reconciliation；active route 再通过 `AgentControl`
读取 local record，或刷新 remote proxy。effect-less reservation 也可以返回一个
只表达 route state 的 synthetic record，保证 identity 可查询但不可路由。

列表由 `GatewayListingService` 从 route ledger 收集候选，先按 durable `agent_name`
和 metadata 做过滤，再按需要读取 record；remote refresh 使用有界并发，单个远端
失败项静默省略，返回普通 list response，不带 `partial` 或 `completeness` 标记；
调用方不能据此推断这是全量 snapshot。结果按稳定的更新时间和 Task id 排序并分页。
`TaskRouter` 还可从 runtime 已持久化的 descendant record 恢复缺失 route，但只接受
Gateway root 的 ancestor；孤立 Task 不能凭空获得 public route。

### 6.5 Message 与 event observation

message page 要求 active route。local page 由 runtime 从精确或 latest checkpoint
snapshot 建立文本 projection，cursor 绑定 local Task/checkpoint/offset；Gateway 不
持久化 checkpoint。remote page 先验证下游顶层 `task_id` 等于 route 保存的
upstream id，再只把顶层 identity 改写为 public Task id，并保留 `items` 与 opaque
cursor。保留的 message items/cursor 是不可信任务内容，可能含下游字节或标识；它们
不能被 Gateway 当成控制面 public identity，也不能在本地重新解释 remote cursor。

event observation 同样要求 active route。local stream 来自 runtime 的
TaskEventLedger；remote stream 由 runtime remote port 打开，Gateway 校验 Task/run/
event 形状、fresh stream 的 snapshot 与 resume stream 的边界，并把已验证的事件
投影到 public Task id。SSE line、heartbeat、frame 和 response close 不属于本页，
由 [`gateway_protocol`](../../src/ruyi_agent/gateway_protocol/sse.py) 与 HTTP adapter
负责。`assistant.delta` 仍是 transient 观察，不成为 Gateway recovery 的依据。

这里的 event cursor 仅涉及 protocol 的 wire codec；listing、review 和 local-message
cursor 的语义与 codec 仍分别归 Gateway 的 listing、review 和 message-cursor service。

### 6.6 Review：以 review owner 和 root 归属编排

`GatewayReviewService` 从 runtime 的 pending review set 读取 review，并只刷新这些
review 的 owner route；remote owner 的状态先经 public record refresh。列表/单项查询
按稳定 cursor 与有界 owner refresh 组织 response，不能因为一个不可用 owner 而重建
整个 Task universe。

decision 先验证 public Task 与 review owner/root 的关系，再要求 owner route active，
最后交给 `AgentControl` 的 review port。runtime 负责 review transition、resume run
和 Task state；Gateway 负责归属检查、route 选择和 `ReviewResponse`/Task response
projection。normalized review payload 是不可信任务内容，可能保留下游字节或标识；
review decision 没有被包装成 command-store idempotency，其 audit 和后续执行属于
runtime 的非 transport 语义。

### 6.7 Artifact 与 remote event webhook

artifact metadata 随 Task projection 返回；下载前由 artifact service 校验绝对且在
runtime workspace 内的 path，task-scoped 下载还必须先在该 public Task 的已登记
manifest 中找到 artifact id，再通过 `AgentControl` 读取 bytes，并执行 Gateway 的
artifact size limit。binary response 的 framing/content disposition 属于 transport；
Gateway 不实现 A2A 下游的 artifact download protocol。

remote Task webhook 是一个应用级 event ingest：Gateway 把 typed event 交给 router/
runtime，先按已知 local/upstream binding 找到 public route，再调用 remote event
handlers。它返回已投递到本地 runtime 的数量，不把 channel delivery 或 webhook
transport status 当成 Task state authority。

## 7. 三个 durable ledger 与一致性

### 7.1 三种状态各自回答不同问题

| ledger | 保存什么 | 权威问题 |
| --- | --- | --- |
| Gateway route ledger | public Task → local/remote route、Agent/kind、metadata、webhook、upstream id、route state/error、create key scope/replay policy/effect boundary | 这个 public identity 现在能否安全路由，是否有可信 upstream binding |
| Gateway command ledger | principal、Idempotency-Key、operation/target/request hash、claim token、Task/mailbox identity、同一 command marker 中的 `effect_started`/`replay_safe`、成功 response 或 terminal error | 这个 caller command 是否已被 claim、完成、可从持久化记录安全重投影，或必须保守地标为 uncertain |
| runtime Task/event ledger | `TaskRecord`、Task state/run_count、review/artifact projection、durable lifecycle event；remote proxy 另保存 external-operation/uncertainty marker；checkpoint 另有自己的 DB | runtime 对 Task/run 的状态和生命周期事实是什么 |

route state 与 Task state 不能互相替代。例如 Task row 可能已存在但 route 仍
uncertain；此时 Task 可以被查询为 interrupted/synthetic observation，却不能被 Gateway
拿来发送 input。反过来，route active 也不代表当前进程仍有可取消的 process-local
run handle；这由 runtime supervisor 决定。

### 7.2 明确没有跨库/跨 ledger 事务

TaskStore 自己可以在同一个 SQLite Unit of Work 内原子提交 Task row 与 lifecycle
event；review transition 也受 runtime 自己的事务边界保护。但当前没有把 route、
command、Task、checkpoint 或外部 effect 组成 two-phase commit：

- `GatewayRouteStore` 的 reservation/activation 与 TaskStore 的 Task row/event 是
  不同 ledger；二者不在一个事务中。public remote create 的 route、Gateway command
  （若有 key）和 runtime local proxy `TaskRecord`/external-operation marker 也分别
  提交；三者没有一个共同事务。
- bootstrap 当前把 `GatewayRouteStore` 指向 `gateway_route_db`，并把
  `GatewayCommandStore` 与 `TaskStore` 配置为 `task_db`。command 与 Task 即使物理上
  共用 SQLite 路径，仍是不同 table/store、不同 claim/UoW 操作；共址不产生跨
  ledger atomicity。
- LangGraph checkpoint DB 与 TaskStore 分离；checkpoint commit 与 Task lifecycle
  commit 没有跨库事务。
- command claim、route transition、runtime spawn、remote request、webhook 和
  transport response 之间也没有统一 rollback。

因此 `route + command + Task + checkpoint + external side effect` 不是一个原子
动作。Gateway 以“reservation → effect boundary → effect → binding/activation →
projection”的顺序缩小窗口，用 durable evidence、query 和 reconciliation 处理
窗口，而不以事务措辞掩盖未知结果。

### 7.3 Route create evidence

route ledger 保存不含 secret 的 create evidence：

| evidence | 当前值 | 用途 |
| --- | --- | --- |
| `create_key_scope` | `none`、`external`、`generated`、`legacy_unknown` | 说明是否有 caller key 或 workflow 生成的下游 key |
| `create_replay_policy` | `never`、`local_task_identity`、`ruyi_gateway_v1`、`legacy_unknown` | 说明同一 identity 是否有可验证的 replay contract |
| `create_effect_boundary` | `reserved`、`started`、`legacy_unknown` | 说明 effect call 前后最后一个已提交的本地边界 |

证据用于恢复分类，不用于把一个 route 强行升级为 active。local durable run 或
remote verified upstream id 是 effect proof；单独一个 reservation 不是。

## 8. GatewayCommandStore：claim、replay 与 uncertain effect

带 key 的 create/input 会先校验 key，再以 canonical operation、target 和 request
body hash 做 claim。command identity 还按 principal 分区：同一 key 可以被不同
principal 独立使用；同一 principal 下只要 operation、target 或 request hash 不同，
就产生 `idempotency_key_reused`，不能覆盖旧命令。

一次 claim 的应用结果可概括为：

| claim result | 语义 | Gateway 行为 |
| --- | --- | --- |
| `acquired` | 当前调用取得 claim token | 执行 effect 并尝试 durable complete |
| `busy` | 同一命令仍由其他调用处理 | 有界等待；超时报告 in-progress，不并发 dispatch |
| `replay` | 已保存成功 response | 读取并重新投影，返回 `replayed=true`，不重复 effect |
| `terminal` | 已保存终态错误 | 从安全字段重建 public error，不重复 effect |

成功 response 和终态 error 都是 ledger 中的 replay record。effect 前，command store
在同一个 command marker 更新中一起持久化 `effect_started` 与 `replay_safe`；effect
只有在这个 marker 提交后才开始。这样“是否可以再次调用”不依赖 caller 对网络错误
的猜测：

- **明确未 dispatch**：route evidence 证明 effect boundary 仍是 reserved，或
  transport 明确没有发出请求。Gateway 将 route 恢复到可重试的 reservation，并
  可释放/重开相应 command；相同 public identity 可以在无重复 dispatch 的前提下
  重新执行。
- **权威拒绝**：已知 Agent/runtime/upstream 拒绝且 effect outcome 可分类为
  not-started。create route 进入 failed，command 保存 terminal error；后续同 key
  从安全字段重建并 replay 该终态错误，不重复 effect。
- **结果未知**：timeout、response 丢失、取消或 exception 发生在 effect boundary
  之后。local 或没有已验证 remote replay contract 的 effect 不能盲目重试：route
  进入 uncertain，unsafe started command 在重启时成为
  `idempotency_outcome_uncertain` terminal，并保留 public Task id。
- **已验证可 replay 的 remote create**：只有 caller key、remote reference 的
  `ruyi_gateway_v1` contract 和已提交 started evidence 同时成立时，command/route
  recovery 才能用同一 public identity 和同一下游 key 再次尝试。无 key 的一次性
  Gateway call 即使 workflow 为下游生成 key，也没有 caller-visible command claim，
  不能承诺无条件安全 replay。

command store 在进程重新打开时会释放可 replay-safe 的中断 claim；可能已触达不可
幂等下游的 started claim 会变成 terminal，而不是重新排队。若 route ledger 后来证明
该 effect 实际未 dispatch，Gateway 才按两本 ledger 的证据重开 terminal create
command。这是跨 ledger recovery 协调，不是跨库事务。

## 9. 恢复、错误与 public projection

### 9.1 Route recovery

`TaskRouter` 每次读取 route 时都可以对 pending create 做不产生新 effect 的
reconciliation：

- pending local 若找到有 durable run 的同一 Task id，可以 activation；没有 effect
  且 boundary 为 reserved 时变为 failed；存在开始过但无法证明 durable effect 的
  窗口时保持 conservative uncertain。
- pending remote 若仍为 reserved，可以等待原始 create；若 external key、
  `ruyi_gateway_v1` policy 和 started evidence 都存在，可以按相同 identity 重试；
  其它 started/legacy-unknown 情况变为 uncertain。
- active remote 没有 upstream binding 时，store 初始化/读取会将其修复为
  uncertain；不会用 public Task id 猜一个 upstream id。
- 缺失 descendant route 只有在能找到 active ancestor 且 child 已证明有 durable
  effect 时才恢复为 active；否则 route 为 uncertain，且不调用 remote ensure/refresh。

route recovery 保留查询身份与错误线索，但不把“可查询”升级为“可 dispatch”。
`task_route_unavailable` 是这一边界的应用错误；调用方应先 query/reconcile，不能
仅凭上一次 HTTP status 或网络重试猜测 route。

调用者取消也不能跳过 durable cleanup：route reset、command release/fail 和
activation 的短事务会被 shield，先完成记录再把 cancellation 传播回 caller。这样
不会出现 caller 已返回取消、ledger 却仍允许一个不安全的 duplicate create 的窗口。

### 9.2 Effect disposition 与错误分类

Gateway 使用 [`GatewayEffectDisposition`](../../src/ruyi_agent/gateway/errors.py) 表达
应用级效果边界，而不是把网络状态码当成效果事实：

| disposition | 含义 | public 处理 |
| --- | --- | --- |
| `NOT_DISPATCHED` | 权威证据说明 effect 未发出 | 可 reset evidence 并按 command policy 重试 |
| `AUTHORITATIVE_REJECTION` | runtime/upstream 已明确拒绝 | route failed 或返回稳定拒绝，不自动重试 effect |
| `OUTCOME_UNKNOWN` | effect 可能已发出但结果未知 | 保留 identity；按 replay contract 选择安全 replay 或 uncertain terminal |

remote boundary 的保证必须收窄理解：raw upstream error payload、异常文本、私有 URL
和控制面 identity 不直接透传到 public response、route error 或 command replay。
[`RemoteTaskPort`](../../src/ruyi_agent/runtime/delegation/remote_port.py) 的
reconciliation 先验证远端 Task identity/state/run 并记录 external-operation 的不确定
性；[`public_errors.py`](../../src/ruyi_agent/gateway/public_errors.py) 再把有限的
upstream error code 和 operation-level message 映射为安全字段；最后
`GatewayProjection` 负责 public response 的顶层 identity/projection。这三层共同完成
sanitization，不能把全部保证归给 `GatewayProjection`。

normalized Task result、review payload、message items、opaque cursor 和用户输入仍是
不可信的保留任务内容，可能含下游字节或标识；它们不能被 Gateway 当成控制面
identity。错误 details 只从安全字段重建 public `task_id`、可查询 URL、route state、
retryability 和 effect outcome 等必要字段；`public_remote_record` 还会把 pending
review 的 source identity 一并 relabel。

## 10. 安全与信任边界

### 10.1 Public remote route 与 runtime internal delegation 严格分开

两者都可能在更底层使用 A2A，也可以复用 `AgentControl`/`TaskRuntime`/`RemoteTaskPort`
的 effect path；真正的控制面差异是 public identity 和 route ownership，delegation
context 仍属于 runtime：

| 维度 | public Gateway remote route | runtime internal worker delegation |
| --- | --- | --- |
| 入口 | public Gateway application caller 经 facade/`TaskRouter`/`CreateRouteWorkflow`，再进入 `AgentControl` | runtime delegation tool/`TaskCommandPort` 经 `AgentControl` |
| identity | public Task id、Gateway route/upstream binding，以及由 runtime 建立的 local proxy `TaskRecord` | runtime 内部 proxy `TaskRecord`、parent/root/delegation context；不建立 public Gateway identity |
| durable ledger | runtime TaskStore 的 proxy record + `GatewayRouteStore` +（若有 key）`GatewayCommandStore`；三者独立提交 | runtime TaskStore、mailbox/settled outbox 与 delegation reconciliation |
| effect owner | `AgentControl`/`TaskRuntime`/`RemoteTaskPort` 发起 external operation 并 reconciliation；Gateway 额外编排 route reservation/activation/projection | `TaskRuntime`/`RemoteTaskPort` 负责 proxy state、run 和 recovery |
| 不经过的边界 | 不把 upstream id 变成 public id；不跳过 Gateway route reservation | 不经过 Gateway route reservation，不凭空建立 public route |
| unknown effect | runtime external-operation marker 与 Gateway route/command evidence 共同分类；public id 保持可查询，route 可为 uncertain | runtime proxy 以 `interrupted`/uncertain marker 和 delegation reconciliation 表达 |

public remote create 不是 Gateway 绕过 runtime 直接调用 A2A：`AgentControl.spawn_task`
进入 `TaskRuntime.spawn_task`，remote branch 先建立 local proxy `TaskRecord`，由
`RemoteTaskPort` 在下游 effect 前保存 external-operation marker，并在返回或失败后
reconcile。Gateway 在此之外拥有 public route 和可选 command ledger；route、command
和 runtime Task ledger 之间没有跨库事务。internal worker delegation 可以复用同一
runtime effect path，但只拥有 runtime identity/ledger，不拥有 Gateway route。Gateway
文档不能把 internal worker child Task 写成 public remote route，也不能把 public remote
create 的 route state 写成 runtime delegation state。具体 A2A transport 仍归
[`A2A client`](../../src/ruyi_agent/integrations/a2a/client.py) 和 runtime integration
边界。

### 10.2 输入、凭据与 workspace

- Agent public/availability admission 防止 private 或启动失败的 target 直接接收
  public effect；route binding 防止 identity rebinding 和跨 Task 复用。
- upstream credential 在配置/integration 边界从命名 environment variable 读取，
  不进入 public DTO、Task projection、event data 或错误 details。
- attachment filename 先清理，decoded bytes 受 Gateway attachment limit 约束，并
  只能写入 workspace 下的 Gateway inbox；artifact path 必须是绝对、规范化且位于
  workspace 根内，artifact bytes 受服务端大小限制。
- Gateway 不决定 tool permission；不可信任务需要的 backend isolation 由 backend/
  runtime 选择，local backend 的 shell 权限不因 Gateway route 而获得额外隔离。
- HTTP bearer、Team Console session、same-origin 和 cookie 安全属于 HTTP channel
  boundary，不在本页复制；详见 [Gateway HTTP 与 `gateway_protocol`](gateway-http-protocol.md)。

## 11. 测试证据

下列文件是当前代码库中与本控制面契约直接对应的行为证据入口；它们不是本次文档
编辑实际执行过的测试命令。本次只做静态链接与 diff 检查，不运行测试。

| 关注点 | 行为证据 |
| --- | --- |
| facade 组合、typed Agent config、public catalog、availability projection 与 transport-neutral service boundary | [`test_gateway_boundaries.py`](../../tests/unit/test_gateway_boundaries.py)、[`test_gateway_http_core.py`](../../tests/unit/test_gateway_http_core.py) |
| route reservation before effect、binding 不可重写、local/remote state、activation failure、descendant recovery | [`test_gateway_task_router.py`](../../tests/unit/test_gateway_task_router.py)、[`test_gateway_route_crash_evidence.py`](../../tests/unit/test_gateway_route_crash_evidence.py) |
| command claim/release/restart、principal/key uniqueness、成功/终态 replay、unsafe started effect | [`test_gateway_command_store.py`](../../tests/unit/test_gateway_command_store.py)、[`test_gateway_command_route_boundary.py`](../../tests/unit/test_gateway_command_route_boundary.py) |
| caller cancellation cleanup、NOT_DISPATCHED/OUTCOME_UNKNOWN、safe remote replay 与同 identity recovery | [`test_gateway_create_effect_disposition.py`](../../tests/unit/test_gateway_create_effect_disposition.py)、[`test_gateway_route_security_recovery.py`](../../tests/unit/test_gateway_route_security_recovery.py)、[`test_gateway_idempotency_flow.py`](../../tests/integration/test_gateway_idempotency_flow.py) |
| remote public/upstream identity、local proxy/external-operation reconciliation、错误 sanitization、remote route persistence 与非 active route 拒绝 dispatch | [`test_gateway_http_remote.py`](../../tests/unit/test_gateway_http_remote.py)、[`test_async_subagent_task_runtime.py`](../../tests/unit/test_async_subagent_task_runtime.py)、[`test_a2a_client.py`](../../tests/unit/test_a2a_client.py) |
| list prefilter、bounded remote refresh、review owner refresh、cursor snapshot 与 unavailable owner 处理 | [`test_gateway_listing_concurrency.py`](../../tests/unit/test_gateway_listing_concurrency.py) |
| local/remote message identity 与 opaque cursor、event disconnect/resume 的跨 Gateway 观察结果 | [`test_gateway_message_history_flow.py`](../../tests/integration/test_gateway_message_history_flow.py)、[`test_gateway_sse_flow.py`](../../tests/integration/test_gateway_sse_flow.py)、[`test_a2a_task_events.py`](../../tests/unit/test_a2a_task_events.py) |

Task state、Task/event UoW、checkpoint 和 runtime interruption 的详细证据与所有权见
[`task-execution.md`](task-execution.md)；本页不把这些 runtime 测试重新解释为
Gateway route/command ledger 测试。

## 12. 同步触发

以下变化应同时审阅本文及对应行为证据：

- `GatewayTaskModule` facade 的应用操作、service 组合、Agent public/availability
  admission 或 `GatewayProjection` 的稳定字段/identity 语义改变；
- local/remote route 选择、reservation、activation、binding、route state、create
  evidence、descendant recovery 或 reconciliation 改变；
- public Task id 与 upstream Task id 的绑定、runtime remote proxy/external-operation
  reconciliation、remote record/error sanitization、message/review/artifact public
  projection 改变；
- listing/review/local-message cursor 的语义或 codec，或 SSE/event cursor 的 wire
  codec 改变；
- `GatewayCommandStore` 的 principal/key/request-hash claim、基于安全字段的 replay
  projection、同一 marker 的 effect_started/replay_safe、uncertain terminal、
  cross-ledger reopen 或 no-key 行为改变；
- bootstrap 的 route/command/task store wiring、物理 DB placement 或任何 ledger/UoW
  边界改变；
- runtime `AgentControl`、Task state/event/message/review/artifact 语义变化到达
  Gateway 应用观察面时，同时更新 [Task execution runtime](task-execution.md)。

以下变化通常由所属文档负责：HTTP auth/status/header、DTO/JSON transport、SSE
framing、SSE/event cursor wire codec、channel session/receipt/delivery、tool permission
或 A2A wire implementation；listing/review/local-message cursor 仍由本页的 Gateway
service 所属代码负责。只有这些变化同时改变本页的 public application semantics、
identity 或恢复分类时，才需要同步更新本页，并连同
[Gateway HTTP 与 `gateway_protocol`](gateway-http-protocol.md) 或 runtime 文档一起
核对。实现与测试是最终事实来源，本文不扩展为逐 route 或逐函数的 API 清单。
