# Gateway Task 控制面

本文描述 Gateway 的 transport-neutral Task 控制面：它如何把 Agent catalog、Task
command、route、runtime effect、查询投影和恢复组织成稳定的 application facade。事实
以当前代码、测试和[架构总览](../architecture.md)为准；HTTP wire 边界见
[Gateway HTTP 与 `gateway_protocol`](gateway-http-protocol.md)，Agent 执行面见
[Task execution runtime](task-execution.md)。

Gateway 是外部 Task 的控制面，runtime 是 Agent 执行与协调面；同进程 bootstrap 不会
合并两者的 ownership、状态、错误或恢复边界。

## 负责范围与边界

Gateway Task 控制面通过 [`GatewayTaskModule`](../../src/ruyi_agent/gateway/tasks.py)
负责：

- 组合 Agent、Task、command、listing、review、artifact application service，并生成
  public Agent catalog；
- 编排 create、input、cancel、Task 查询/列表、message/event 观察、review 和
  artifact 操作；
- 预留/激活 local 或 public remote route，绑定 Gateway public Task id 与
  local/runtime 或 upstream Task，执行 reconciliation 和 effect disposition；
- 由 [`GatewayCommandStore`](../../src/ruyi_agent/storage/gateway_command_store.py)
  为带 `Idempotency-Key` 的 create/input claim、replay、冲突和 uncertain effect；
- 由 [`GatewayProjection`](../../src/ruyi_agent/gateway/application.py) 生成稳定的
  Agent、Task、Review、Artifact response，并参与 remote identity/error sanitization。

它不拥有 HTTP auth/status/header、DTO/SSE framing、Agent turn/run admission、Task
canonical state machine、Task/event/checkpoint 持久化、A2A transport、channel
session/receipt/delivery、tool permission、skills、mailbox 或 settled outbox；这些
分别属于 HTTP/protocol、runtime、storage/checkpointer、A2A、channel 和 runtime
对应边界。Gateway 只通过 runtime facade 读写/订阅，不让 caller 取得 SQLite connection
或绕过 route/command 进入 runtime。

Cursor ownership 也按用途划分：listing 的 ordering/offset、review 分页、local
message 的 Task/checkpoint/offset 绑定由 Gateway application service 持有；
`gateway_protocol` 只负责 DTO wire codec 以及 SSE/event cursor wire codec。

## 入口、组件与 catalog

bootstrap 创建 `AgentControl`、route/command/Task stores、checkpointer、backend 和
`GatewayTaskModule`，HTTP route composition 从 application state 取得 facade；channel
adapter 通过 `GatewayHTTPClient`，in-process embedder 可直接调用 facade。

```text
HTTP / channel client / embedder
              -> GatewayTaskModule
                 -> application services + projection
                 -> TaskRouter + route/command ledgers
                 -> AgentControl
                    -> local runtime 或 remote A2A port
```

组件 ownership 的最小划分是：`GatewayAgentService` 只做 public catalog 和 admission；
`GatewayTaskService` 组织 effect、read、input/cancel、message/event、附件和 webhook；
`GatewayListingService` 负责 durable route 预过滤、刷新、排序、分页；
`GatewayReviewService` 负责 owner/root 归属和 decision；`GatewayArtifactService` 负责
workspace/path 与 manifest 读取；`TaskRouter` 负责 public id 查 route、local/remote
选择、恢复和错误投影；`CreateRouteWorkflow` 负责 reservation/effect/binding/activation
窗口；stores 各自只保存其对应 ledger。

catalog 中每个 Agent 带 local 或 `remote_ref` kind、name、description、public 和
bootstrap availability。列表只返回 `public=true`；private agent 是 policy 拒绝而非
暂时 unavailable。local 构建失败的 Agent 仍可在 catalog 中以 `available=false` 和
稳定摘要出现，指向它的 create 返回 `agent_unavailable`；availability 只表示
bootstrap 时的调用资格，不承诺未来 provider/backend/upstream effect 成功。

## Public Task identity 与 route

`TaskRecord.task_id` 是 Gateway public identity。create 在任何 effect 前确定它：带
command key 时使用 claim 的 proposed id，否则生成新 id；该 id 在 query、list、
message、review、artifact 和错误 details 中保持稳定。

| route 概念 | 语义 |
| --- | --- |
| `local` | public Task 由本地 runtime 执行；route 可使用同一 Task id 作为 local binding |
| `remote_ref` | public Task 由 remote Agent/上游 Gateway 执行；runtime 先建立同一 public id 的 local proxy `TaskRecord`，验证后保存 `upstream_task_id` |
| `upstream_task_id` | 仅用于与上游交互/校验；不能替代 public id，也不能未经清理出现在 public projection/error |
| `parent_task_id`/`root_task_id`/`depth` | runtime delegation tree 关系，不是 route binding 或 channel session key |

route state 独立于 runtime Task state：

| route state | 控制面含义 |
| --- | --- |
| `pending` | identity 已 reservation，effect/binding 尚未安全可用；可查询但不能 dispatch 依赖 route 的 operation |
| `active` | local 有 durable run，或 remote 有已验证 upstream id；允许操作和刷新 |
| `failed` | 权威拒绝/确认未开始/route 已终止；保留 identity，不重新 dispatch |
| `uncertain` | effect 或 transition 无法证明，或找不到可信 binding；只保留查询/恢复线索 |

reservation 可到 active、failed 或 uncertain，active 可继续 active 或降为 uncertain；
failed/uncertain 不由普通调用重新激活，binding 不能被另一 Agent/kind/upstream id 重绑。
route state 与 Task state 不能互相替代，HTTP status 也不能推断 route/effect state。

## 应用操作

### Create：先有 identity，再有 effect

1. 校验 Agent public/availability，解析 delegation metadata；local attachment 写入
   inbox，remote attachment 作为下游输入。
2. 有 `Idempotency-Key` 时以 `(principal, key, operation, target, request_hash)` claim
   固定 public Task id；无 key 的调用不经过 command ledger，每次都是新的 create。
3. 在 route ledger 写 `pending` reservation 和 create evidence；reservation 失败不
   调 runtime spawn。
4. effect 前提交 command 的 `effect_started`/`replay_safe` marker（若有 key）和 route
   boundary marker。随后 `AgentControl` 建立 local `TaskRecord`；remote path 由
   `RemoteTaskPort` 先保存 external-operation/reconciliation marker，再向上游 create。
   无 caller key 时，只有已验证 `ruyi_gateway_v1`/create capability 才生成稳定下游 key。
5. local 必须有非零 run 且不再 pending；remote proxy 必须经 reconciliation 验证
   upstream id。验证失败保持 failed/uncertain，不把 effect-less record 当 active。
6. activation 写入 local binding 或 verified upstream binding，再由 `GatewayProjection`
   生成 public response。

同 key 重试在 active route 上安全重投影，不再次 spawn；failed/uncertain 不以同一 id
盲目创建第二个 effect。无 key 的下游 generated key 不能变成 caller-visible command。

### Input、cancel 与观察

input 先查 public id 并要求 active；带 key 时 command ledger 按 body hash claim，成功
结果从安全字段 replay，不重复 effect；无 key 的旧路径直接发送。runtime 拒绝 active
run 等 admission 错误由 Gateway 投影，Gateway 不自行改变 runtime state。

cancel 只对 reconciliation 后的 active route 调用 `AgentControl`，目前不走 command
store，不能描述为与 create/input 相同的幂等 command；最终 `cancelled` 或 `interrupted`
由 runtime authority 决定。

单 Task query 先从 route ledger 取得 identity。pending/failed/uncertain 只做不启动
effect 的 reconciliation；effect-less reservation 可返回 synthetic record。active
route 再刷新 local record 或 remote proxy。list 从 route ledger 预过滤后有界刷新
local/remote record；单个远端刷新失败静默省略，普通 response 没有 `partial` 或
`completeness` 标记，不能据此推断是全量 snapshot，结果按稳定更新时间/Task id 排序分页。

message page 的 local cursor 绑定 Task/checkpoint/offset；remote page 先验证下游顶层
`task_id` 等于保存的 upstream id，再只重写顶层为 public id，保留 `items` 和 opaque
cursor，不能重新解释 remote cursor。event observation 使用 local TaskEventLedger
或 remote port；校验 Task/run/event shape、fresh snapshot/resume 边界和 public id。
SSE framing/cursor wire codec 属于 protocol；`assistant.delta` 是 transient 观察，不是
Gateway recovery 依据。

review 从 runtime pending set 读取，只刷新 review owner route；decision 先验证
public Task 与 owner/root 归属，再要求 active route，最后交给 runtime review port。
review list/decision 不包装成 command-store idempotency。artifact download 先按 public
Task/manifest 校验 workspace path，再由 `AgentControl` 取 bytes；remote Task webhook
按已知 local/upstream binding 找 public route 后交给 runtime，不把 webhook transport
status 当作 Task state authority。

## Ledgers、一致性与恢复

| ledger | 保存什么 | 回答的问题 |
| --- | --- | --- |
| Gateway route | public route、Agent/kind、metadata、webhook、upstream id、route state/error、create evidence | public identity 能否安全路由，是否有可信 binding |
| Gateway command | principal/key/operation/target/hash、claim token、identity、effect marker、可安全 replay 的 response/error | command 是否已 claim、完成、可 replay，还是必须标 uncertain |
| runtime Task/event | `TaskRecord`、state/run、review/artifact、durable lifecycle；remote proxy 另有 external-operation marker；checkpoint 另有 DB | runtime 对 Task/run/lifecycle 的事实 |

Task row 与 lifecycle event 可在 TaskStore 自己的 SQLite UoW 内原子提交；review
transition 也受 runtime 事务保护。但 route、command、Task、checkpoint、外部 effect
没有 two-phase commit。即使 command 和 Task 物理共用 `task_db`，仍是不同 store/table
与 claim/UoW；route 通常在 `gateway_route_db`，checkpoint 另库。reservation、command
claim、runtime spawn、remote request、webhook 和 transport response 之间没有统一
rollback。系统用“reservation → effect boundary → effect → binding/activation →
projection”顺序、durable evidence、query 和 reconciliation 缩小窗口，不能以事务措辞
掩盖未知结果。

route create evidence 是不含 secret 的 `create_key_scope`
(`none`/`external`/`generated`/`legacy_unknown`)、`create_replay_policy`
(`never`/`local_task_identity`/`ruyi_gateway_v1`/`legacy_unknown`) 和
`create_effect_boundary` (`reserved`/`started`/`legacy_unknown`)。单独 reservation
不是 effect proof；local durable run 或 verified upstream id 才是。

### Command replay 与 effect disposition

command claim 结果为 acquired、busy、replay、terminal；同一 principal 下 key 与
operation/target/hash 不同即 conflict，不覆盖旧命令。effect 前 marker 必须持久化，
成功 response/终态 error 才可安全重投影。

| disposition | 含义与处理 |
| --- | --- |
| `NOT_DISPATCHED` | 有权威证据未发出；可 reset evidence，按 policy 重试 |
| `AUTHORITATIVE_REJECTION` | runtime/upstream 明确拒绝；route failed 或稳定拒绝，不自动重发 |
| `OUTCOME_UNKNOWN` | 可能已发出但结果未知；保留 public identity，按 replay contract 安全重试或进入 uncertain terminal |

进程重开时可释放 replay-safe 的中断 claim；started 且可能触达不可幂等下游的 command
变为 terminal，不重新排队。只有 route evidence 证明实际未 dispatch 时，跨 ledger
recovery 才能重开。timeout、response 丢失、取消或 boundary 后 exception 不能盲目重试
local effect；remote create 只有 caller key、已验证 `ruyi_gateway_v1`、started evidence
同时成立时才可用同一 identity/下游 key replay。

pending route reconciliation 只做不产生新 effect 的检查：local 找到同 id durable run
可 activation；reserved 且无 effect 可 failed；started 但证据不足保持 uncertain。remote
reserved 可等待；满足 key/capability/evidence 才重试；其它 started/legacy 情况 uncertain。
active remote 缺 upstream binding 时修为 uncertain，不能猜 upstream id。缺失 descendant
route 只有在 active Gateway ancestor 且 child 有 durable effect 时才恢复。取消路径会
shield route reset、command release/fail 和 activation 的短事务，再传播 caller cancellation。

remote raw error、异常文本、私有 URL、upstream/control-plane identity 不直接越过
public boundary：`RemoteTaskPort` 先做 identity/state/run reconciliation，
`public_errors.py` 映射有限 code/message，`GatewayProjection` 再做顶层 identity/projection。
Task result、review payload、message items、opaque cursor 和 user input 仍是不可信任务
内容，可能含下游字节或标识，不能被当作 Gateway identity。

## Public remote route 与 internal delegation

public remote create 不是 Gateway 绕过 runtime 直接调用 A2A：`AgentControl.spawn_task`
进入 `TaskRuntime`，remote branch 先建立 local proxy `TaskRecord`，由 `RemoteTaskPort`
保存 external-operation marker 并 reconciliation。Gateway 额外拥有 public route 和可选
command ledger；三本 ledger 独立提交。

| 维度 | public Gateway remote route | runtime internal worker delegation |
| --- | --- | --- |
| identity/ledger | public Task + Gateway route/upstream binding + runtime local proxy；route、command、Task ledger 均参与 | runtime proxy、parent/root/delegation context；不建立 public route |
| effect owner | runtime remote port 发起/reconcile，Gateway 编排 reservation/activation/projection | runtime TaskRuntime/RemoteTaskPort 负责 proxy/recovery |
| unknown effect | runtime marker 与 route/command evidence 共同分类，public id 保持可查询 | runtime interrupted/uncertain marker 与 delegation reconciliation |

internal child Task 不能写成 public remote route，public route state 也不能写成 runtime
delegation state；A2A wire implementation 归 integration 边界。

## 安全边界

public/availability admission 防止 private 或 bootstrap 失败的 target 接收 effect；
route binding 防止 identity rebinding。upstream credential 从命名 environment variable
读取，不进入 DTO、Task projection、event 或 error details。attachment/artifact path
由各自 service 检查 workspace，local shell 的宿主权限不因 Gateway route 获得额外隔离。
HTTP bearer、Team Console session 和 same-origin 约束属于
[HTTP 文档](gateway-http-protocol.md)，不在本页重复定义。

## 测试证据入口

行为证据的 canonical 入口为：

- [Gateway boundaries/core](../../tests/unit/test_gateway_boundaries.py)、[HTTP core](../../tests/unit/test_gateway_http_core.py)：facade、catalog、availability、public projection 和 transport-neutral boundary。
- [Task router/crash evidence](../../tests/unit/test_gateway_task_router.py)、[route security/recovery](../../tests/unit/test_gateway_route_crash_evidence.py)：reservation、binding、route state、activation 和 descendant recovery。
- [Command store/route boundary](../../tests/unit/test_gateway_command_store.py)、[effect disposition](../../tests/unit/test_gateway_create_effect_disposition.py)：principal/key claim、replay、unsafe started effect、cancellation cleanup 和 uncertain 分类。
- [Remote HTTP/A2A](../../tests/unit/test_gateway_http_remote.py)、[runtime remote](../../tests/unit/test_async_subagent_task_runtime.py)、[A2A client](../../tests/unit/test_a2a_client.py)：public/upstream identity、runtime proxy ledger、error sanitization 和 remote recovery。
- [Listing/message/SSE flows](../../tests/unit/test_gateway_listing_concurrency.py)、[message history](../../tests/integration/test_gateway_message_history_flow.py)、[SSE flow](../../tests/integration/test_gateway_sse_flow.py)：bounded refresh、partial listing 语义、opaque cursor 与跨 Gateway 观察。

## 何时同步本文

仅在以下稳定边界变化时同步：

- `GatewayTaskModule` facade、Agent public/availability admission、response projection、
  local/remote route reservation/activation/binding/state/recovery 或 public/upstream identity；
- listing/review/local-message cursor、remote proxy/external-operation reconciliation、
  error sanitization 或 message/event/artifact public semantics；
- command principal/key/hash claim、effect marker、replay/uncertain policy、跨 ledger
  consistency 或 store wiring；
- public remote route 与 internal delegation 的 trust/ownership boundary。

HTTP auth/status/header、DTO/SSE framing、channel receipt/delivery、tool permission 和
A2A wire implementation 由其所属文档负责；只有它们改变本页 application identity、
state 或 recovery semantics 时才一并核对。
