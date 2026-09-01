# Task Mailbox 与 child settlement

本文记录当前 runtime 中两条相互衔接、但持久化边界不同的流程：Task input
mailbox，以及 child Task run settled 后向 parent mailbox 的通知。事实来源是当前
代码和行为测试；跨层结构可参见[架构总览](../architecture.md)。本文不把
mailbox 当作通用事件总线，也不把一个 Task 的 settled run 当作 Task 会话的终结。

## 负责范围与边界

本系统负责：

- 为已有 Gateway Task 保存输入，按可选 idempotency key 去重，并在安全的 model
  boundary 注入；
- 为本地 Task 的 triggering input 做唤醒，使用 claim lease，并在 Agent 执行器
  成功返回后确认消费；
- 将 child Task 的 settled state、lifecycle event 和 settled outbox intent
  协调在同一个 TaskStore Unit of Work；
- 由 fenced outbox claim 把 intent 发布到 parent Task/thread 的 mailbox，维护
  `mailbox_delivered`，处理 wait/check 的抑制与撤回，并在启动和周期维护时重试、
  对账和唤醒 recipient。

下列行为明确不属于本文：

| 边界 | 所属组件或说明 |
| --- | --- |
| Channel inbound event receipt、Channel Turn receipt | Channel adapter 与其 session/receipt store |
| ChannelDeliveryStore、Telegram/Feishu 平台发送 | Channel presentation/delivery；外部发送结果不由 mailbox 证明 |
| external webhook 的请求、重试和认证 | remote/delegation integration 的尾部效果；settlement 不依赖 webhook 成功 |
| Task 通用状态机、run admission 和执行句柄 | `TaskManager`、`RunSupervisor` 与 Task runtime；本文只描述它们进入/消费 mailbox 的边界 |
| delegation scope、parent/child 可见性和预算 | `DelegationPolicy` 与 delegation tools；本文不扩大调用方权限 |
| 模型、工具、平台外部效果的 exactly-once | mailbox 只保证 durable identity、lease/fence 与可重试；模型调用和外部效果仍可能重复 |

### 入口调用方和依赖方向

`bootstrap_application` 创建 `TaskStore`、`MailboxStore` 和 `AgentControl`。Gateway
Task Module 通过 `AgentControl` 调用 `TaskRuntime.send_task_input`；runtime 内的
delegation tools 通过同一 command port 调用它。Channel adapter 只经
`GatewayHTTPClient` 访问 Gateway，不直接取得这些 stores。具体 composition 见
[`runtime/bootstrap.py`](../../src/ruyi_agent/runtime/bootstrap.py) 和
[`async_runtime.py`](../../src/ruyi_agent/runtime/delegation/async_runtime.py)。

Task input 的消费者是 `MailboxMiddleware`，它在每次 model call 前从当前
`task_id`/`thread_id` 读取；本地执行器在 graph invocation 返回后确认 claim。child
settlement 的协调者是 `TaskManager`、`SettledRunNotifier` 和
`SettledOutboxRepository`，分别拥有 Task 写入口、提交后的 dispatch/reconcile 尾部
和 outbox lease。

### Durable 存储前提

生产 bootstrap 用 settings 的同一个 `task_db` 构造 `TaskStore(task_db)` 和
`MailboxStore(task_db)`。当 `AgentControl` 同时收到 TaskStore 与 mailbox 时，
composition root 要求二者指向同一个 SQLite 数据库；不同文件、独立的
`:memory:` 连接或 in-memory AgentMailbox 的组合会被拒绝；若省略 mailbox，则
durable settlement 被关闭而不是被伪造成可投递。这里的“同库”是同一个 SQLite
文件/共享 URI，不是同一个 Python connection：两种 store 仍各自拥有连接和锁。

这个要求是 child settlement 的安全前提，因为 dispatch transaction 需要同时看见
`agent_tasks`、settled outbox 和 mailbox row；它不是把 TaskStore、MailboxStore
或 Channel store 合并成一个 ownership。没有 TaskStore 的纯进程内测试 mailbox
可以工作，但不能宣称跨重启恢复；带 idempotency key 的 local input 需要持久
mailbox。若使用非 durable compatibility mailbox，settled notifier 的 direct publish
也不具备下文 Task/event/intent 与 mailbox 的同库原子保证。

## 一眼看清的两条流程

```text
Task input:
caller -> publish_input/persistent row -> triggering wakeup
       -> claim(owner/token/lease) -> before_model injection
       -> graph invocation returns -> acknowledge_task

Child settlement:
child Task state + lifecycle event + outbox intent (one TaskStore UoW)
       -> claim/fence -> same-DB publish to parent mailbox
       -> outbox delivered + child mailbox_delivered=1
       -> best-effort recipient wakeup -> parent claims at its next model boundary
```

两条流程共享 recipient identity 和 SQLite 文件，但不要混淆：input claim 的确认
发生在消费它的 Agent invocation 后；settled outbox 的 delivered 表示通知已写入
parent mailbox，不表示 parent 已经调用模型或读到了通知。

## 流程一：Task input mailbox

### 输入发布与幂等

`TaskRuntime.send_task_input` 对 local Task 将输入交给
[`AgentMailbox.publish_input`](../../src/ruyi_agent/runtime/mailbox/service.py)，
保存 recipient Task/thread、content、可选 sender identity、`trigger_run` 和
`idempotency_key`。持久实现是 [`MailboxStore`](../../src/ruyi_agent/storage/mailbox_store.py)
的 `agent_mailbox_messages`；新行从 `pending` 开始。

- `idempotency_key` 是可选的。持久表对它施加唯一约束，重复发布会被忽略，因而
  网络/调用重试不会产生第二个逻辑 input row；`message_id` 也必须保持唯一。
- mailbox 自身只做 identity 去重，不做 request hash 冲突投影，也不证明 input
  对模型、工具或外部系统产生 exactly-once effect。Gateway command 的更高层
  idempotency/replay 仍属于 Gateway 边界。
- `send_task_input` 传入 idempotency key 时，如果 runtime 没有持久 mailbox，会
  返回 `DurableTaskMailboxRequiredError`，不伪造可跨崩溃兑现的保证。
- `trigger_run=True`（默认）表示 settled/resumable recipient 在没有 active
  handle 时需要被唤醒；`False` 的 input 仍可在下一次已有 run 的 model boundary
  被 claim，但不会单独触发 idle Task。

### Trigger 与 wakeup

发布提交后，runtime 先尝试 `_ensure_task_awake`：它只为 local、没有 active run、
处于 `RESUMABLE_TASK_STATES` 且存在 pending triggering input 的 Task 调度一个
空 payload run。这个空 payload 不是业务输入；真正的输入由 middleware 在 model
前读取。active run 不会被第二次调度，输入留在 mailbox，等待该 run 的下一个安全
model boundary。

run 结束时，`on_run_finished` 还会排一个受 supervisor 管理的 maintenance wakeup，
用于覆盖“输入到达时 Task 正在收尾”的窗口。wakeup 只是推进器：它失败、进程在
发布后退出或 parent 当时正在运行，都不改变 pending row；后续扫描仍以 SQLite
中的 row 为准。

### Claim lease 与 model 前注入

[`MailboxStore.claim`](../../src/ruyi_agent/storage/mailbox_store.py) 在一次
SQLite `BEGIN IMMEDIATE` 事务中释放已过期 claim，按创建序选择 pending rows，写入
当前 owner、随机 claim token、claimed time 和 expiry，再返回这一 token 所属的行。
recipient Task 精确匹配；没有 `recipient_task_id` 的兼容 row 才按
`recipient_thread_id` 兜底。两个 store connection 并发 claim 时只有一个能成功，
旧 owner/token 不能代替新 owner 完成确认。

[`MailboxMiddleware`](../../src/ruyi_agent/runtime/middleware/mailbox.py) 的
`before_model`/`abefore_model` 从 LangGraph configurable context 取得当前
`task_id` 和 `thread_id`，执行 claim，并把结果渲染为一个带
`source=agent_mailbox` 和 message id 列表的 `HumanMessage`。普通 input 会保留
sender identity；child settlement row 会以独立的 settled-notification 文本呈现。
claim 只代表暂时占有，不能单独算作已消费。

状态图（只画本 mailbox 的 durable 边界）：

```mermaid
stateDiagram-v2
    [*] --> pending: publish_input commit
    pending --> claimed: before_model claim
    claimed --> delivered: graph invocation returns
    claimed --> pending: run error / process loss / lease expiry
    pending --> claimed: retry or wake at next boundary
    note right of pending
      trigger_run=1 才会唤醒 idle Task
      duplicate idempotency key 不新增 row
    end note
```

### Ack、失败与恢复

`LocalTaskExecutor.execute` 只有在 Agent graph invocation 正常返回后才调用
`acknowledge_task(task_id, thread_id)`；它确认当前 MailboxStore owner 对该
recipient 的 claims。模型调用抛错、进程崩溃或 ack 前取消时，row 保持 claimed，
待 lease 到期后回到 pending；下一轮可再次注入。runtime 的 Task failed/interrupted
写入和 mailbox ack 不是同一个跨对象事务：即使 ack 已提交，之后的 Task outcome
normalization 或 lifecycle 写入仍可能失败，反之亦然。

`MailboxStore.recover_claims` 在维护循环中延长本进程仍持有的 live claims，并释放
已经过期的 claims；重启后的新 owner 不会冒充旧 token，过期后才重新取得 row。
因此可恢复依据是 mailbox row/status/lease，不是进程内的 asyncio handle。

## 流程二：child settlement 到 parent mailbox

### Settlement 资格与 intent identity

local run 在 `TaskRuntime` 中经 `TaskManager.mark_completed`、`mark_failed`、
`mark_cancelled` 或 `mark_interrupted` 进入本轮 settled；经过验证的 remote
payload 同样可同步为 settled。`waiting_for_human`、`pending` 和 `running` 不会
产生本轮 settled intent。

[`build_settled_outbox_intent`](../../src/ruyi_agent/storage/settled_outbox.py)
只为同时满足以下条件的当前 `(task_id, run_count)` 建立 intent：Task 是 settled，
有 `parent_thread_id`，没有 `mailbox_suppressed`/`mailbox_delivered`，且没有未解决
的 external operation 或 uncertain marker。identity 是确定的
`settled:<parent_thread_id>:<task_id>:<run_count>`，并由它派生稳定 message id；
content 取本轮 result、error 或状态摘要。新的 Task run 会递增 `run_count` 并得到
独立 identity，旧 run 的 delivery/suppression 不会覆盖新 run。

### TaskStore UoW：state、event、intent

启用 durable settlement 时，TaskManager 的 lifecycle 写入口经
[`TaskEventLedger`](../../src/ruyi_agent/runtime/task_event_ledger.py) 调用
[`TaskStore.update_task_with_event`](../../src/ruyi_agent/storage/task_store.py)。
`TaskLifecycleUnitOfWork` 在同一个 TaskStore SQLite transaction 中更新 Task row、
追加 public lifecycle event，并插入 settled outbox intent；review transition
路径也会把适用的 Task/event/outbox 写入同一个 UoW。任一项失败，Task state、event
和 intent 一起 rollback，进程内受保护的 Task snapshot 也恢复。

这条原子边界不延伸到 LangGraph checkpoint、Gateway route/command、MailboxStore
的后续 dispatch、recipient wakeup 或 webhook。Task/event/intent 的提交是权威
事实；`TaskEventLedger` 的 subscriber fan-out 和 notifier 的尾部动作都发生在
commit 之后。

```mermaid
stateDiagram-v2
    [*] --> task_active
    task_active --> settled: TaskManager transition
    settled --> outbox_pending: same TaskStore UoW
    outbox_pending --> outbox_claimed: claim(owner/token/expiry)
    outbox_claimed --> outbox_pending: dispatch failure or expiry
    outbox_claimed --> outbox_delivered: fenced same-DB publish
    outbox_pending --> outbox_suppressed: wait/check suppression
    outbox_claimed --> outbox_suppressed: suppression fences claim
    outbox_delivered --> outbox_suppressed: later wait/check
    outbox_suppressed --> outbox_suppressed: retraction reconciliation

    note right of outbox_delivered
      parent mailbox row 已持久化
      child.mailbox_delivered=1
      不等于 parent 已读
    end note
```

### Claim、fence、publish 与 delivered flag

[`SettledOutboxRepository.claim_pending`](../../src/ruyi_agent/storage/settled_outbox.py)
在 TaskStore 数据库中以 immediate transaction：

1. 先清理带 unresolved remote effect 的无效旧 settlement；
2. 释放过期 outbox leases；
3. 只选择 `pending` 且对应 Task run 未 suppressed、未 delivered 的 intent，写入
   owner/token/expiry，并增加 attempt count。

`SettledRunNotifier` 随后把 claim 交给
[`MailboxStore.publish_claimed_settled_outbox`](../../src/ruyi_agent/storage/mailbox_store.py)。
该方法在同一个 SQLite 文件上开启自己的 immediate transaction，核验 outbox key、
`status=claimed` 和 claim token，并再次核验 child run 没有 suppression 或 uncertain
external operation。核验通过后，它会按 deterministic idempotency identity 找到或
插入 parent mailbox row，同时把 outbox 设为 `delivered`、清空 lease，并把 child
Task 的 `mailbox_delivered=1` 写入；这些写入彼此原子。token 过期、被 fence 或
suppression 已先提交时，publish 返回 false，不产生新的 authoritative mailbox
delivery。

publish transaction 成功后，notifier 只把 `mailbox_delivered` 镜像到当前内存记录，
再检查该 parent row 是否仍是 pending triggering input。它返回需要唤醒的
`recipient_task_id`；parent 有 active run 时不另起并发 run，parent 已 settled 且
有 trigger 时才调度空 payload，让 parent 在 model 前 claim。没有 recipient Task
identity 的 thread-only row 仍可被 parent thread claim，但没有定向 Task wakeup。

### wait/check 的 suppression 与 retraction

`wait_agent` 在等待/刷新前就抑制当前 child run 的 mailbox delivery；`check_agent`
在看到 settled run 后抑制。调用方既然已同步取得该结果，就不应再收到同一 run 的
异步通知。

`TaskStore.suppress_settled_delivery` 在同一 TaskStore SQLite transaction 中：

- 持久化 Task 的 `mailbox_suppressed=1`；
- 把对应 outbox 的 pending/claimed（以及已跨越 mailbox 边界的 delivered）标为
  suppressed，并清除 claim；
- 将同一 child/run 的 mailbox row 中仍为 pending/claimed 的记录标为 retracted，
  并记录 outbox 的 retraction watermark。

后续 dispatch 会因 Task/outbox suppression 检查失败而被 fence。已经是 delivered 的
parent mailbox row 不会被伪造为未发送；它保持 delivered，而 suppressed outbox 的
retraction reconciliation 只确认这项抑制已经收尾。`SettledRunNotifier.reconcile`
和 `MailboxStore.retract_settled_outbox` 负责补齐尚未撤回的 pending/claimed rows；
旧的非 outbox 兼容 row 也由 `mailbox.retract` 按 parent thread、child task 和
run_count 处理。

### 启动、周期对账与 recipient wakeup

bootstrap 在 Gateway readiness 打开前先调用
`worker_control.wake_pending_mailbox_tasks()`：

- 对尚未完成的 legacy settlement migration 做有界、可重启的 watermark 扫描，
  为漏掉的 settled Task 补建 intent，并修正已 suppressed/delivered 的旧记录；
- dispatch 当前可 claim 的 outbox，撤回 suppressed row；
- 释放过期 mailbox claims，读取 pending triggering recipient IDs，并推进可唤醒
  的 Task。

随后 `start_mailbox_recovery()` 启动 supervisor 管理的两条维护循环：settled outbox
的 reconcile/dispatch/wakeup 约每 1 秒运行一次，mailbox claim recovery 和 pending
trigger 扫描约每 5 秒运行一次。legacy watermark 完成后，不再周期性全表扫描旧
Task；周期 tail 仍可发现 pending outbox、suppression retraction 和 recipient
wakeup。

dispatch、reconcile 和 wakeup 是提交后的非权威 tail。它们超时、抛错或在进程退出
前未运行时，已提交的 Task/event/intent 或 parent mailbox row 仍按 SQLite 状态保留，
下次 startup/periodic reconciliation 再推进；反之，wakeup 成功也不能替代 parent
真正 claim 和模型 ack。

## 事务边界与故障窗口

| 窗口 | 权威结果与恢复动作 |
| --- | --- |
| Task settled UoW 中 event 或 intent 写失败 | Task row、lifecycle event、intent 一起 rollback；不会留下一个可 dispatch 的 settled intent |
| outbox 已 claim，dispatch 前进程失败 | claim 到期后回到 pending；新的 owner 取得不同 token。显式 release 失败也不改变这一恢复路径 |
| mailbox publish 写入失败 | same-DB transaction rollback，outbox 仍是 claimed；notifier 尝试 release，之后由 lease/reconcile 重试 |
| publish transaction 已提交但 wakeup 未运行 | mailbox row、outbox delivered 和 child `mailbox_delivered=1` 已存在；pending trigger 扫描会再次唤醒 recipient |
| parent claim 后模型调用/进程失败 | claim lease 到期后可再次注入；模型调用或工具效果可能重复，不能称 exactly-once |
| wait/check 与 dispatch 竞争 | 先提交的一方由同库 suppression/token 检查决定；suppression 胜出时 stale publish 返回 false，publish 胜出时已 delivered row 不被伪造撤销 |
| remote external operation 未证明 effect | 本地保留 operation identity 与 uncertain marker，不创建/发布新的 authoritative settlement；只有 refresh/权威 payload 证明 effect 后才清除 marker、同步状态并生成相应 intent |

其中 TaskStore 与 MailboxStore 的“同库”只使上表的 same-DB publish/suppression
transaction 成立；它不使 checkpoint、route、外部 webhook、模型调用或平台发送
加入 two-phase commit。

## 恢复、关闭与安全

### 恢复与关闭

Task mailbox 的可恢复对象是 SQLite row、claim expiry、outbox status 和
`run_count`；`LiveRunRegistry`/supervisor 中的 asyncio handle 是 process-local，
不能因为 mailbox wakeup 就当作已恢复。local executing Task 在重启/关闭时按 runtime
规则收尾为 interrupted；settled Task 可在同一 Task/thread 开启下一代 input。

FastAPI lifespan 先把 readiness 置为 false，再调用 `AgentControl.close()`。close
先停止 recovery/maintenance，等待关闭宽限期、取消仍在运行的 handles 并持久化
interruption，之后才按 bootstrap 的反向创建顺序关闭 `MailboxStore`、`TaskStore`
等 stores。这样不会在 claim recovery、outbox dispatch 或 Task interruption 仍可能
访问数据库时提前关闭连接；完整启动/关闭顺序见
[`runtime-configuration-and-bootstrap.md`](runtime-configuration-and-bootstrap.md)。

### 安全约束

- claim owner、token、expiry 和每个 outbox 的 deterministic identity 防止过期
  worker 覆盖新 owner；recipient Task/thread matching 也不能被旧 token 绕过。
- mailbox content 会被作为 model input 注入，应按不可信的跨 agent/user 内容处理；
  sender 字段是上下文身份，不是 authorization grant。调用方能否访问 parent/child
  由 delegation/Gateway 边界另行校验。
- 共享 Task DB 是一个信任边界；不要把不相关 runtime 或含凭据的数据库文件交给
  mailbox。secret 不应进入 mailbox content、Task result、日志或 public projection。
- remote upstream identity、状态和 run_count 必须先经 remote reconciliation
  验证；uncertain operation 不得先投影成 completed/failed settlement，也不得依
  普通网络重试猜测外部 effect。

## 测试证据

以下是当前已有的行为证据入口：

- [`test_mailbox.py`](../../tests/unit/test_mailbox.py)：持久 mailbox reopen、输入
  idempotency、跨 connection 原子 claim、before-model 注入，以及 invocation
  成功后的 acknowledgement。
- [`test_settled_outbox.py`](../../tests/unit/test_settled_outbox.py)：四类 settled
  transition 的 deterministic intent、Task/event/outbox rollback、dispatch retry、
  lease reclaim、parent wakeup、suppression/retraction、new run identity、同库
  guard 和 remote settlement rollback/conflict。
- [`test_settled_outbox_recovery.py`](../../tests/unit/test_settled_outbox_recovery.py)：
  重启 adoption、legacy mailbox identity、claim/suppression race、bounded migration
  和 watermark 后的周期行为。
- [`test_remote_operation_reconciliation.py`](../../tests/unit/test_remote_operation_reconciliation.py)：
  external operation uncertain 时清理/封锁旧 settlement，只有已证明的远端 run 才
  能生成新的 settled intent。
- [`test_runtime_bootstrap_shutdown.py`](../../tests/unit/test_runtime_bootstrap_shutdown.py)：
  bootstrap 的 recovery、readiness 和关闭时序；Task 的状态/生命周期基础证据还见
  [`task-execution.md`](task-execution.md)。

## 文档同步触发

下列当前行为发生变化时，应同步更新本文、相关测试证据和架构链接：

- `MailboxStore`/`AgentMailbox` 的 publish、idempotency key、recipient matching、
  claim lease、ack/recovery 或 mailbox schema；
- `MailboxMiddleware` 的 model 前注入，`TaskRuntime` 的 trigger/wakeup、run-finish
  maintenance、startup/periodic recovery 或 close 顺序；
- `TaskLifecycleUnitOfWork`、`TaskStore`、`SettledOutboxRepository` 的 Task/event/
  intent 原子边界、claim/fence、delivered/suppression/retraction 或 migration；
- `SettledRunNotifier` 的 dispatch、reconcile、recipient wakeup，或 remote
  reconciliation 对 uncertain operation 的资格判断；
- Channel receipt/delivery、external webhook、Gateway Task state machine 或
  delegation scope 的 ownership 变化。此类变化还必须同步本文的“不负责”边界，
  不能只改流程段落而保留旧的责任声明。
