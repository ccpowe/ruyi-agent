# Channel 系统

本文描述当前 Channel 入口层的共同模型、边界和生命周期。Channel 是平台
ingress 与 Gateway HTTP 之间的适配层：它把 Telegram、Feishu/Lark 的事件
规范化为统一的 turn，维护平台自己的会话和收据，并把 Gateway Task 的状态
呈现回平台。本文以当前代码和行为测试为准；[架构总览](../architecture.md)
提供跨子系统的上下文。

## 负责什么，不负责什么

Channel 拥有以下行为：

- 平台事件解析、身份与会话 key、active agent 选择，以及统一的 turn policy；
- 平台事件 receipt、turn receipt 和 Channel delivery 的 durable 状态；
- Task watch、review/terminal presentation、平台消息/文件/反应的发送；
- Telegram/Feishu 外部连接的启动、关闭、重试和平台 API 差异。

Channel 明确不拥有以下行为：

| 不负责的状态或效果 | 所属边界 |
| --- | --- |
| Gateway Task、route、command 及其效果编排 | Gateway task service、router、route/command stores |
| Gateway Task event ledger 及事件公共协议 | runtime event ledger 与 `gateway_protocol` |
| Agent 执行、run supervisor、middleware 和 LangGraph checkpoint | runtime / delegation / bootstrap |
| Gateway 的稳定 Agent/Task/Review/Artifact response projection | `GatewayProjection` |
| child settlement 到 parent mailbox 的 runtime settled outbox | runtime delegation / mailbox / `SettledOutboxRepository` |

Channel 只通过 [`GatewayHTTPClient`](../../src/ruyi_agent/channels/gateway_client.py)
（其内部使用 Gateway protocol client）调用 Gateway 的公开 HTTP 面。Adapter 不
接收 `AppRuntime` 或 Gateway service，不持有 Gateway/runtime 的 `TaskStore`、
route/command/event stores，也不调用 Agent executor 或 checkpoint。Adapter 拥有
自己的 session、平台 event receipt 和 delivery stores；这些 stores 即使与
Gateway 同进程，也不是 Gateway/runtime stores。

## 进程、入口与调用方

`ruyi` 的 [`run_channels`](../../src/ruyi_agent/entrypoints/main.py) 在选择
Telegram 或 Feishu 时，总是把 Gateway 与选中的 adapter 放进同一个
`asyncio.TaskGroup`。`--all` 只保留配置了必要凭据的 adapter。这个事实只说明
它们共享进程和生命周期，不改变依赖方向：

```text
CLI
  └─ TaskGroup
       ├─ Gateway FastAPI + bootstrap/runtime
       └─ adapter
            ├─ platform client (Telegram long poll / Feishu WebSocket)
            ├─ ChannelSessionStore
            ├─ platform EventReceiptStore
            ├─ ChannelDeliveryStore
            └─ GatewayHTTPClient ──HTTP──> Gateway endpoint
```

[`run_telegram_adapter`](../../src/ruyi_agent/channels/telegram/runner.py) 和
[`run_feishu_adapter`](../../src/ruyi_agent/channels/feishu/runner.py) 都从不可变
`RuntimeSettings` 读取 `gateway.base_url` 与 Gateway bearer token，创建
`GatewayHTTPClient`，再创建各自的平台 client、stores 和 adapter。starter/default
的 [`gateway.base_url`](../../src/ruyi_agent/templates/ruyi_home/ruyi.toml) 是
`http://127.0.0.1:8000`（loopback）；同进程 TaskGroup
并不要求 endpoint 必须 loopback，也不允许 adapter 因为同进程而绕过 HTTP。

平台 client 的 inbound callback 是 adapter 的直接调用方：Telegram 的轮询循环
取得一批 update 后调用 `handle_message`，Feishu 的 WebSocket 回调解析消息后
调用 `handle_message`。两个 adapter 随后共用
[`ChannelTurnHandler`](../../src/ruyi_agent/channels/turn.py)、
[`TaskWatchManager`](../../src/ruyi_agent/channels/task_watch.py) 和
[`ChannelDeliveryCoordinator`](../../src/ruyi_agent/channels/presentation.py)。

这里有两个独立的 API leg：平台 client 负责平台 ingress 与 outbound send，
`GatewayHTTPClient` 负责把规范化 turn 映射到 Gateway 的公开 Task/Agent/Review/
Artifact HTTP 能力。平台 API 的凭据、错误和 payload 不穿透为 Gateway 内部对象；
Gateway 的公开 response 也由 `GatewayProjection` 产生后才被 Channel 解析。

## 从平台事件到 Task

### 规范化输入与 key

平台事件先保留平台身份信息，再归一化为 [`InboundTurn`](../../src/ruyi_agent/channels/turn.py)：
`platform`、`session_key`、`agent_name`、文本、metadata、chat/user/thread、附件、
`force_new` 和平台事件产生的 idempotency key。`metadata` 会带上平台、chat、user、
`channel_session_key`，并在可用时带 thread；它用于 Gateway Task 的可查询归属，
不是 Gateway Task identity 的替代品。

会话 key 的共同规则是：

| 会话 | 形式（抽象写法） | 目的 |
| --- | --- | --- |
| DM | `agent:<agent>:<platform>:dm:<chat>` | 同一个 agent 在一个私聊中续用同一 Task |
| group | `agent:<agent>:<platform>:group:<chat>:user:<user>` | 群聊按发言者隔离上下文，避免成员共用 session |
| thread/topic group | 在 group 中插入 `:thread:<thread>` | 不把不同 topic/thread 的对话混在一起 |

Telegram 的 group 段保留其 `group`/`supergroup` 类型；Feishu 的群聊统一为
`group`。DM 不额外加入 user，因为平台 chat 已是私聊身份。identity key 是
同一结构去掉 `agent:` 前缀的稳定身份 key；它只定位“谁在什么平台会话中”，
agent-scoped session 才真正绑定 Task。具体生成规则见
[`telegram/identity.py`](../../src/ruyi_agent/channels/telegram/identity.py) 和
[`feishu/identity.py`](../../src/ruyi_agent/channels/feishu/identity.py)。

### active agent 与 agent-scoped session

`ChannelSessionStore` 保存两类相关记录：identity 记录保存当前 active agent，
`agent:<agent>:...` 记录保存该 agent 的 `current_task_id`。没有 identity 记录时
使用配置的 default agent（当前 starter 为 `main`）。因此切换 agent 不会把一个
agent 的 Task 当成另一个 agent 的上下文：

1. `/agent` 只列 Gateway 返回的 public agents；私有 agent 不会暴露给平台用户。
2. `/agent <name>` 更新 identity 的 active agent，并解除新 agent 的旧 session；
   若带首条消息，则在该 agent-scoped session 创建新 Task。
3. 后续普通消息用新的 agent-scoped session 查找/创建/续写 Task。
4. `/resume` 可列出候选 Task；恢复指定 Task 前必须用平台、chat、user、thread
   metadata 做归属检查，成功后同时绑定 identity 和对应 agent-scoped session。

## Turn 选择

所有平台都进入同一个 [`ChannelTurnHandler.handle`](../../src/ruyi_agent/channels/turn.py)，
差异只在事件和发送能力。选择顺序如下：

| 结果 | 条件 | Channel 动作 |
| --- | --- | --- |
| `create` | session 没有可用 current Task，或 `/new` 设置 `force_new` | 通过 Gateway HTTP 创建 Task，并绑定 session |
| `input` | current Task 已 settled（completed/failed/cancelled/interrupted） | 先保证上一 run 的 terminal delivery 已处理，再通过 Gateway HTTP 发送 input |
| `active` | Task 处于 `pending`/`running` 等执行中状态 | 不重复发 input；建立或复用 watch，返回处理中提示/平台 ack |
| `review` | Task 有 `pending_review` | 不发送普通 input；呈现 review。`approve`/`reject` 再提交 Gateway review decision |
| `resume` | 用户显式列出或指定恢复 Task | 列表只作候选；读取 Task 后验证归属，再绑定 session，settled Task 可继续 input |

若 session 指向的 Task 返回 401/403/404/410，Channel 会解除该 session，再按
metadata 查询最近候选；找不到才走 `create`。`before_continue` 保证旧的终态
消息和附件在续写前有机会完成。`active` 或 `pending_review` 不会因为普通新消息
而偷偷启动另一个 run。

平台事件的 idempotency key 同时传给 Gateway command：Telegram 使用
`telegram:update:<update_id>`，Feishu 使用
`feishu:event:<event_id 或 message_id>`。这使平台事件重试时，Gateway 自己的
command 幂等边界仍可生效；Channel 不因此拥有 Gateway command store。

## 三类 Channel 状态

这三类记录解决不同故障窗口，不能互相替代。

### 1. Platform event receipt：占有 inbound 事件

共享的 [`ChannelEventReceiptStore`](../../src/ruyi_agent/channels/event_receipts.py)
为每个平台的既有表提供 claim/lease/fencing：

```text
absent ──claim──> claimed ──handler 成功──> processed
                    │  │
          并发重复 busy  │ handler 失败/release 或 lease 过期
                    │  └──────────────────────────────> 可再次 claimed
                    └─旧 token 被 fencing，不能完成或释放新 owner 的 claim
```

- Telegram 的稳定 key 是整数 `update_id`，持久化在
  `telegram_processed_updates`；轮询 offset 只有在事件标记 processed 后才前进。
  handler 失败或 live claim 会保留 offset，下一轮重试。
- Feishu 优先使用 `event_id`，否则使用 `message_id`，持久化在
  `feishu_processed_events`；WebSocket callback 在 handler 成功后标记 processed。
  没有任一稳定 key 的消息只能直接处理，不能提供持久去重。
- 同一事件被另一 adapter 实例同时看到时，胜者为 `claimed`，并发者为 `busy`；
  已完成的是 `processed`。过期 lease 可被回收，但旧 claim token 不能写入新
  owner 的结果。

Event receipt 只表示“本 adapter 已占有并完成 inbound 处理”，不表示 Gateway
Task 已完成，也不表示平台 outbound message 已发送。

### 2. Turn receipt：保护 Gateway mutation 重试

`ChannelSessionStore` 中的 [`ChannelTurnReceipt`](../../src/ruyi_agent/storage/channel_session_store.py)
没有单独的执行中状态，而是一个与 session binding 同事务写入的已完成记录：

```text
无 receipt ──Gateway create/input 返回──> receipt(operation=create|send, hash, response)
                                            │
                              同 key + 同 platform/session/hash ──> replay
                              同 key + 不同请求/会话/平台 ────────> conflict
```

adapter 在选择 create/input 之前先查 receipt。Gateway 返回成功后，receipt 保存
请求 hash、operation、Task id 和可重放 response；随后同事件再次到达时直接返回
该 response，不重复选择或调用 Gateway。不同请求复用同 key 抛出
`ChannelTurnIdempotencyConflictError`。receipt 解决的是“Gateway effect 已提交、
但平台 event receipt 还未 processed”的窗口；它不把未知的外部发送结果变成
exactly-once。

### 3. Delivery intent 与 step：投递 Task observation

[`ChannelDeliveryStore`](../../src/ruyi_agent/storage/channel_delivery_store.py) 将
一个 `(platform, session_key, task_id, run_count)` 形成 durable delivery intent，
并以 `(intent_id, step_key)` 保存已确认的发送步骤。intent 的当前状态包括：

| intent state | 含义 |
| --- | --- |
| `watching` | 正在观察该 Task/run |
| `retry_wait` | 查询或 hook 失败，等待本次 bounded retry |
| `error` | 本轮失败；可带 `last_error` 与持久 redrive 时间 |
| `delivering` | 正在执行 review/terminal message、artifact 或 delivered hook |
| `review_waiting` | review 通知的 step 已确认，等待用户决定或下一次观察 |
| `terminal_grace` | terminal message/artifact steps 已确认，仍做 grace checks 以捕获迟到 review |
| `delivered` | terminal steps 与 grace 完成 |
| `superseded` | 观察的旧 run 已被更高 `run_count` 取代 |

当前使用的稳定 step key 是：

- `review:<review_id>:message`；
- `terminal:<run_count>:message`；
- `terminal:<run_count>:artifact:<artifact_id>`；
- 可选的 `terminal:<run_count>:delivered-hook`。

每次外部 effect 的顺序是“检查 step marker → 调用平台 send/upload → 成功后写
marker”。外部发送与 marker 写入不是一个原子事务：平台已经接受消息而进程随后
崩溃、超时或 lease 丢失时，重试可能再次发送。因而 Channel 只承诺跳过已有的
durable step marker，不承诺跨进程、崩溃或网络边界的绝对去重；使用方必须容忍
重复消息/文件。

Delivery 使用 owner/token/expiry 和 fence 的 fenced lease。长时间平台发送期间
有 heartbeat 续租；失去 lease 的旧 owner 不能更新 step 或终态。多个 adapter
实例可共享同一 delivery DB，由 lease 决定唯一当前 owner。

## Task watch 与 presentation

### watch 的事实路径

[`TaskWatchManager`](../../src/ruyi_agent/channels/task_watch.py) 为每个
`(task_id, run_count)` 建立一个 asyncio watcher，按 `task_poll_interval` 定时
调用 Gateway client 的 `get_task`，解析公开 Task projection：

1. Gateway 返回的 `run_count` 小于预期时视为 stale snapshot，继续等待，不把旧状态
   归给新 run；大于预期时触发 `superseded` 并结束旧 watch。
2. 发现 `pending_review` 就调用 review hook 并结束本次 watch。
3. 发现 settled status 时只调用一次 terminal hook，然后按
   `terminal_review_grace_checks` 再检查若干次；grace 内若出现迟到 review，review
   优先于完成旧 terminal lifecycle。
4. 408/429/5xx、网络/超时等可重试 Gateway 查询采用 bounded exponential backoff
   和 jitter；不可重试错误或耗尽后通过 error hook 报告。

当前 adapter watch **不是 SSE 驱动**：它不调用 `stream_task_events`，也不依赖
`assistant.delta`。Gateway protocol/client 仍提供可选 Task SSE 能力，但 SSE 的
连接、cursor 和 transient delta 语义属于 Gateway/protocol 边界；未来若把 Channel
watch 改为 SSE，必须同步更新本文件的恢复、cursor 和测试契约。

### presentation 的共同顺序

[`ReviewPresenter`](../../src/ruyi_agent/channels/presentation.py) 把公开
`pending_review` 投影为平台可读内容：review id、task id、动作及参数，并给出
跨平台的批准/拒绝提示。缺少 review 详情时明确显示缺失，而不猜测动作。

`TerminalPresenter` 对 completed/failed/cancelled/interrupted 生成文本，并附
`task_id`。终态投递先发 terminal message，再逐个下载并发送当前 run 的
published artifacts；所有步骤成功后才进入 `terminal_grace`，grace 完成才是
`delivered`。artifact 只按该 run 的 `artifact_id` 建 step，旧 run 的 artifact
不会混入本次投递。

平台格式化、分片、markdown fallback、文件上传和 reaction/ack 是 adapter hook：
它们不改变 Gateway Task 状态。Telegram 失败的 MarkdownV2 发送可退回纯文本，
Feishu 失败的 interactive markdown 可退回文本；这类 fallback 仍属于一次外部
effect，marker 只在 hook 成功后记录。

## 平台差异矩阵

下表只列影响 Channel ownership、key、receipt、呈现和恢复的差异；平台 API 的
具体 endpoint/payload 仍由各自 client 封装，不在此重复列举。

| 能力 | Telegram | Feishu/Lark |
| --- | --- | --- |
| inbound transport | Bot API 长轮询；按 `offset` 消费 update 批次 | SDK WebSocket；事件 callback 解析消息 |
| inbound receipt key | `update_id`（整数）；失败不前进 offset | `event_id`，缺失时退回 `message_id`；两者都缺失则无 durable key |
| outbound text/file | 文本消息；图片/文档上传；默认 MarkdownV2，可 fallback；长文本分片 | 文本或 interactive markdown card，可 fallback；文件先上传再发消息；长文本分片 |
| media 边界 | inbound Telegram 文件先受限下载，再作为 Gateway attachment；artifact 按图片/文档发送；默认上限 50 MiB | 当前 inbound message 不提取附件；artifact 作为文件发送；默认上限 30 MiB |
| DM key | `agent:<a>:telegram:dm:<chat>` | `agent:<a>:feishu:dm:<chat>`（`p2p`/private/dm 视为 DM） |
| group/thread key | 支持 `group`/`supergroup`；按 chat + user，topic 有 `message_thread_id` | 非 DM chat 视为 group；按 chat + user，thread 有 `thread_id` |
| group admission | 不设统一 mention gate；不支持的 chat type 会被拒绝 | 默认禁用 group；可 `open`/`allowlist`，默认要求 mention；启用 mention gate 时需配置 bot identity |
| processing ack | 立即发送“已收到”或“处理中”文本 | `ack_mode` 可为 reaction、message 或 off；reaction 可在终态删除并标记失败 |
| platform credentials | bot token | app id + app secret；domain 可选 Feishu/Lark |
| channel DB 默认拆分 | session/delivery 共用 channel session DB；update receipt 独立 update DB | session/delivery 共用 channel session DB；event receipt 独立 event DB |
| watch/recovery | 统一 `TaskWatchManager` + `ChannelDeliveryStore`，通过 Gateway `get_task` 轮询 | 同左；WebSocket 只负责 inbound，不替代 Task watch |

两种 adapter 均使用同一套 `/agent`、`/resume`、review command 词汇和统一
`ChannelTurnHandler`，但平台特有的身份字段、消息引用、格式化和发送失败处理
只留在对应 client/adapter 中。

## 启动、关闭与错误恢复

### adapter startup/close

[`ChannelAdapterLifecycle`](../../src/ruyi_agent/channels/adapter_lifecycle.py)
实现 single-flight startup 与 close-wins：

1. `start()` 首次调用执行 `delivery.recover()`，从 durable intent 恢复
   `watching`、`retry_wait`、`error`、`delivering`、`review_waiting` 和
   `terminal_grace` 等可恢复状态；并发的第二个 `start()` 等待同一 startup task。
2. recovery 为 intent 重建平台 hooks；未来到期的 query-error redrive 留给协调器
   的 reconciler，到期后再 claim 并启动 watch。恢复失败会取消已建立的 watcher、
   释放本 owner 的 lease、清理内存映射，且 adapter 保持未 started，可重试。
3. `run_forever()` 在进入 Telegram 轮询或 Feishu WebSocket 前调用 `start()`，在
   ingress 返回或异常时进入 `finally` 调用 `close()`。
4. `close()` 先停止 delivery reconciler、取消并 await 所有 watcher/effect，再按
   owner 释放 lease；runner 最后关闭由它创建的 session、event/update 和 delivery
   DB。关闭可重复调用；close 与 startup 竞争时以关闭为准，关闭后不能再次 start。

### 故障窗口与处理

| 故障 | 结果与恢复 |
| --- | --- |
| 平台 inbound handler 在 event receipt 后失败 | 普通失败 release claim；取消时可能保留 lease 到期（Feishu 对清理做 shield，Telegram 依赖过期 reclaim）；Telegram 保留 offset，Feishu 由平台重投，随后可重新 claim |
| event claim 过期或旧实例恢复 | 新 owner 可 reclaim；旧 token 被拒绝，不能覆盖新结果 |
| Gateway create/input 调用失败 | turn 不写成功 receipt；平台事件可重试，已带 key 的 Gateway command 负责其自身 effect 判定 |
| Gateway get_task 暂时不可用 | watch 采用 bounded retry；耗尽后 delivery intent 记录 `error`，可由持久 redrive/重启恢复 |
| 平台 send/upload 失败 | delivery step 不写 marker；本轮按 TaskWatch 的 bounded retry 处理，重启/显式重新 ensure 可恢复；若外部结果不确定，重试可能重复 |
| lease 在外部发送期间丢失 | heartbeat/fence 让旧 owner 失败；新 owner 可恢复未标记 step，但不能证明旧 send 未被平台接受 |
| terminal 后迟到 review | `terminal_grace` 期间继续轮询；review step 可发送，已确认 terminal message 不重复发送 |
| adapter close 或进程重启 | 取消内存 watch，但 durable intent/step 留存；下次 startup recovery 继续；未持久 marker 的 effect 按“可能重复”处理 |

## 安全、凭据与边界

- `RuntimeSettings` 是配置入口；adapter 不自行解析原始 TOML。Gateway bearer 只
  用于 `GatewayHTTPClient` 到配置的 `gateway.base_url`，Telegram bot token 只
  用于 Telegram client，Feishu app id/secret 只用于 Feishu SDK。平台凭据与
  Gateway bearer 不能互换，也不能放入 Task metadata、receipt response、错误
  详情或日志。
- Gateway 业务 HTTP 面需要 bearer；starter 的 `dev-token` 只适合 loopback。若
  `gateway.base_url` 指向远端或 Gateway 对外监听，应使用非默认 token、HTTPS 和
  网络访问控制。[Gateway HTTP context](../../src/ruyi_agent/channels/http/context.py)
  负责认证；Channel 不绕开它。
- `/agent` 只展示 public Agent；`/resume <task_id>` 先核对 channel/chat/user/
  thread metadata，不能凭 Task id 任意跨会话接管。
- Feishu group 由 `group_policy` 与 mention gate 控制；allowlist 和 bot identity
  在配置边界验证。Telegram 只接受实现支持的 private/group/supergroup chat type。
- 外部媒体按配置上限流式读取；Telegram 默认 50 MiB、Feishu 默认 30 MiB。artifact
  下载与平台上传失败应变成 channel-level warning/retry，不能把任意本地路径当作
  平台媒体目录。已废弃的 `media_root` 会被忽略；Channel 不把媒体写入调用方
  指定目录。
- session、receipt、delivery 数据库是运行数据，应按部署权限保护；它们可能包含
  chat/user identity、Task id 和可重放 response，但不应保存平台 secret。

## 测试证据

定向行为证据覆盖以下契约：

- turn 的 create/input/active/review/resume、active agent、agent-scoped session、
  receipt replay/conflict：[`test_channel_turn.py`](../../tests/unit/test_channel_turn.py)。
- DM/group/thread key 与平台身份解析：[`test_telegram_identity_network.py`](../../tests/unit/test_telegram_identity_network.py)、
  [`test_feishu_client_identity.py`](../../tests/unit/test_feishu_client_identity.py)。
- event receipt 的 claim/busy/processed、lease reclaim、fencing 与 schema：
  [`test_channel_event_receipts.py`](../../tests/unit/test_channel_event_receipts.py)、
  [`test_telegram_receipts.py`](../../tests/unit/test_telegram_receipts.py)、
  [`test_feishu_receipts.py`](../../tests/unit/test_feishu_receipts.py)。
- Task watch 的 `get_task` polling、stale run、superseded、review、terminal grace、
  bounded retry 和 close：[`test_task_watch.py`](../../tests/unit/test_task_watch.py)。
- delivery intent 的 durable step、fenced lease、部分 artifact 失败、重启恢复、
  redrive 与 terminal/review 竞争：[`test_channel_delivery.py`](../../tests/unit/test_channel_delivery.py)。
- Review/terminal presentation、artifact 顺序及 duplicate cleanup：
  [`test_channel_presentation.py`](../../tests/unit/test_channel_presentation.py)、
  [`test_telegram_formatting.py`](../../tests/unit/test_telegram_formatting.py)、
  [`test_feishu_presentation.py`](../../tests/unit/test_feishu_presentation.py)。
- adapter startup single-flight、recovery compensation、close-wins 与双平台共享
  生命周期：[`test_channel_delivery_adapter_lifecycle.py`](../../tests/unit/test_channel_delivery_adapter_lifecycle.py)。
- 真实 Gateway HTTP + SQLite 下 Telegram turn 复用 Task、run_count 和 terminal
  presentation：[`test_channel_gateway_flow.py`](../../tests/integration/test_channel_gateway_flow.py)。

这些测试证明 Channel 与 Gateway 的边界协作，不把 Channel 测试误当成 Gateway
Task、runtime execution 或 settled outbox 的所有权证明；后者应看其各自的
Gateway/runtime 测试。

## 同步触发

以下变更必须重新核对并同步本文、相关实现链接和对应测试证据：

- 改变 CLI TaskGroup、runner 注入方式、`gateway.base_url`、Gateway bearer 或
  adapter 是否能直接拿到 runtime/service；
- 改变 identity/session key 的 DM、group、thread 组成，active agent 或 resume
  的归属检查；
- 改变 event receipt、turn receipt、delivery intent/step、lease、retry、redrive
  或 recovery 状态；
- 把 Task watch 从 `get_task` polling 改为 SSE（或反向改变），包括 cursor、断线
  恢复、transient delta 的处理；
- 改变 review/terminal/artifact presentation、平台发送 fallback、ack/reaction、
  media 上限或平台 ingress transport；
- 新增平台 adapter：沿共享 turn/receipt/delivery 边界接入，在本矩阵添加差异并
  增加对应 identity、receipt、presentation、lifecycle 和 integration 证据，避免
  复制一套完整的 Telegram/Feishu 章节；
- 改变 Channel store 的路径、schema 或凭据配置时，同时核对
  [`RuntimeSettings`](../../src/ruyi_agent/config/runtime_settings.py)、starter
  配置、[`README.md`](../../README.md) 和 runner。

本仓库当前没有可依赖的 `doc-sync` 命令。文档变更至少应检查 active links、
相关静态实现和 `git diff --check`；跨 Gateway/runtime 的契约变更再按
[`AGENTS.md`](../../AGENTS.md) 的证据匹配规则运行相应定向测试。
