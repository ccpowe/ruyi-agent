# Channel 系统

本文描述 Telegram、Feishu/Lark 入口层的共同模型、边界和生命周期。Channel 是
平台 ingress 与 Gateway HTTP 之间的适配层：它把事件规范化为统一 turn，维护平台
自己的会话、收据和投递状态，并把 Gateway Task 状态呈现回平台。事实以当前代码、
测试和[架构总览](../architecture.md)为准。

## 负责范围与调用边界

Channel 拥有平台事件解析、identity/session key、active-agent 选择、turn policy、
event/turn receipt、delivery intent/step、Task watch、review/terminal presentation、
平台消息/文件/反应发送，以及 Telegram/Feishu 外部连接的生命周期和重试。

Gateway Task/route/command/effect、runtime Task event ledger、Agent execution/run
supervisor/checkpoint、稳定 Agent/Task/Review/Artifact projection 和 runtime settled
outbox 分别属于 Gateway、runtime 和其持久化/投影边界。Channel 不持有 Gateway/runtime
的 Task、route、command、event store，不接收 `AppRuntime`，也不调用 Agent executor。
它只通过 [`GatewayHTTPClient`](../../src/ruyi_agent/channels/gateway_client.py) 访问
公开 HTTP；自己的 session、event receipt、delivery store 即使与 Gateway 同进程也
不改变 ownership。

## 进程、入口与两个 API leg

`run_channels` 在选择 Telegram/Feishu 时把 Gateway 和 adapter 放入同一
`asyncio.TaskGroup`；`--all` 过滤未配置必要凭据的 adapter。共享进程和生命周期不
改变依赖方向：

```text
CLI -> TaskGroup
       ├─ Gateway FastAPI + bootstrap/runtime
       └─ adapter
           ├─ platform client
           ├─ session/event-receipt/delivery stores
           └─ GatewayHTTPClient ──HTTP──> Gateway
```

adapter 从 immutable `RuntimeSettings` 读取 Gateway URL/bearer，平台 client 只负责
平台 ingress/outbound send，Gateway client 负责公开 Task/Agent/Review/Artifact 能力。
同进程也不允许 adapter 绕过 HTTP。平台凭据、错误和 payload 不变成 Gateway 内部对象；
Gateway response 先由 `GatewayProjection` 投影，再由 Channel 解析。

## 输入、identity 与 turn 选择

平台事件先保留平台身份，再归一化为 `InboundTurn`：platform、session key、agent、
text、metadata、chat/user/thread、attachments、`force_new` 和平台 idempotency key。
metadata 可携带 platform/chat/user/thread 与 `channel_session_key`，用于归属查询，
不是 Gateway Task identity 的替代品。

会话 key 的抽象规则是：

| 会话 | key 形状 | 语义 |
| --- | --- | --- |
| DM | `agent:<agent>:<platform>:dm:<chat>` | 一个 agent 在一个私聊中续用 Task |
| group | `agent:<agent>:<platform>:group:<chat>:user:<user>` | 群聊按发言者隔离上下文 |
| thread/topic | 在 group key 加 `:thread:<thread>` | 不混合不同 topic/thread |

Telegram 保留 `group`/`supergroup` 类型；Feishu 非 DM 统一为 group。identity key
去掉 `agent:` 前缀，只说明谁在什么平台会话，agent-scoped session 才绑定 Task。
`ChannelSessionStore` 保存 active agent 的 identity 记录和各 agent 的
`current_task_id`。默认 agent 来自配置（starter 为 `main`）；`/agent <name>` 只列
public agents、切换 identity 并解除新 agent 的旧 session；`/resume` 在平台/chat/user/
thread metadata 归属检查后才绑定 Task。

统一 `ChannelTurnHandler` 的选择规则：

| 条件 | 动作 |
| --- | --- |
| 无可用 current Task，或 `/new`/`force_new` | 通过 Gateway create 并绑定 session |
| current Task 已 settled | 先给上一 run 做 terminal delivery，再发送 input |
| Task pending/running | 不重复发 input，建立/复用 watch 并返回处理中 ack |
| pending review | 不发普通 input，呈现 review；决定后提交 review decision |
| 显式 `/resume` | 先列候选，再读取并校验归属，成功后绑定 session |

若 session 指向 Task 返回 401/403/404/410，Channel 解除 session 后按 metadata 查找
候选，找不到才 create；`active`/`pending_review` 不因普通新消息偷偷启动新 run。
Telegram 使用 `telegram:update:<update_id>`，Feishu 使用
`feishu:event:<event_id 或 message_id>` 作为传给 Gateway command 的 key。

普通 `InboundTurn` 的 turn receipt 只覆盖 create/input（store operation 为
`create`/`send`）；`/agent` 带首条消息只把平台 key 给 Gateway create，不读写 turn
receipt；无首条 `/agent`、`/resume` 和 review command 也不使用 turn receipt。

## Receipt、delivery 与故障窗口

### Event receipt：占有 inbound 事件

`ChannelEventReceiptStore` 用 claim/lease/fencing 管理平台事件：

```text
absent --claim--> claimed --handler 成功--> processed
                    ├─并发重复 -> busy
                    └─失败/release/lease 过期 -> 可再次 claimed
```

Telegram 的 durable key 是 `update_id`；只有 handler 标记 processed 后轮询 offset 才
前进，失败或 live claim 会保留 offset。Feishu 优先 `event_id`，否则 `message_id`；
WebSocket callback 成功后标记 processed，二者都没有时只能直接处理、没有持久去重。
过期 lease 可 reclaim，旧 token 不能完成或释放新 owner 的 claim。同一事件被两个
adapter 实例看到时胜者为 claimed，另一方 busy。receipt 只证明 adapter 占有并完成
inbound 处理，不证明 Gateway Task 或 outbound message 已完成。

### Turn receipt：保护 Gateway mutation retry

普通 turn 在 create/input 成功返回后，将 request hash、operation、Task id 和 replay
response 与 session binding 同事务保存；同 platform/session/hash 的重复事件直接
replay，不重复选择或调用 Gateway；不同请求复用同 key 为 conflict。Gateway effect
到 receipt 提交之间仍依赖同一 Gateway idempotency key/command store；turn receipt
不把未知的外部发送结果变成 exactly-once。

### Delivery intent/step：投递 Task observation

`ChannelDeliveryStore` 以 `(platform, session_key, task_id, run_count)` 保存 durable
intent，以 `(intent_id, step_key)` 保存确认的发送步骤。intent 可在 watching、retry_wait、
error、delivering、review_waiting、terminal_grace、delivered、superseded 间恢复；稳定
step key 包括 `review:<review_id>:message`、`terminal:<run_count>:message`、
`terminal:<run_count>:artifact:<artifact_id>` 和可选 delivered hook。accepted/running
ack、命令回复和直接 pending-review 回复是即时响应，不创建 durable delivery step。

每个 durable delivery effect 都是“检查 marker → 平台 send/upload → 成功后写 marker”。
外部发送与 marker 写入不是原子事务：平台已接受但进程崩溃、超时或 lease 丢失时，
恢复可能再次发送。Channel 只承诺跳过已有 marker，不能承诺跨进程、崩溃或网络边界
的绝对去重，使用方须容忍重复消息/文件。owner/token/expiry/fence lease 与 heartbeat
防止失去 lease 的旧 owner 更新 step；新 owner 可恢复未标记 step，但不能证明旧 send
没有被平台接受。

## Watch 与 presentation

`TaskWatchManager` 当前通过 Gateway `get_task` 轮询，而不是 SSE，也不依赖
`assistant.delta`。stale `run_count` 继续等待；更高 run 令旧 watch superseded；发现
pending review 先调用 review hook；settled status 只调用一次 terminal hook，然后按
`terminal_review_grace_checks` 检查迟到 review，grace 内 review 优先。408/429/5xx、网络
和超时采用带 jitter 的 bounded exponential backoff，耗尽后记录 delivery error，交给
持久 redrive/重启恢复。

`ReviewPresenter` 展示 review id、task id、动作/参数，缺少详情时明确显示缺失而不猜。
`TerminalPresenter` 对 completed/failed/cancelled/interrupted 先发 terminal message，
再按当前 run 逐个发送 published artifacts；所有 marker 成功后进入 terminal_grace，
grace 结束才 delivered。旧 run 的 artifact 不混入。Telegram MarkdownV2、Feishu
interactive markdown 的失败可退回纯文本；fallback 仍是一次外部 effect，成功后才记
marker。格式化、分片、上传和 reaction/ack 不改变 Gateway Task state。

## 平台差异

| 能力 | Telegram | Feishu/Lark |
| --- | --- | --- |
| inbound | Bot API 长轮询；按 offset 批量消费 | SDK WebSocket callback |
| receipt key | `update_id` | `event_id`，否则 `message_id` |
| text/file | 默认 MarkdownV2，可 fallback；图片/文档上传，长文本分片 | text/interactive markdown，可 fallback；文件先上传，长文本分片 |
| media | inbound 文件先受限下载；artifact 默认 50 MiB | 当前 inbound 不提取附件；artifact 默认 30 MiB |
| group | 支持 group/supergroup，按 chat+user；topic 用 `message_thread_id`；无统一 mention gate | 非 DM 为 group，按 chat+user；thread 用 `thread_id`；默认禁用 group，可 open/allowlist，默认要求 mention |
| ack/credentials | 立即文本 ack；bot token | reaction/message/off（reaction 可在终态删除并记录失败）；app id + secret，domain 可选 |
| storage/watch | session/delivery 共用 channel DB，receipt 独立 update DB；统一 polling watch | session/delivery 共用 channel DB，receipt 独立 event DB；WebSocket 不替代 polling watch |

平台身份字段、消息引用、格式化和发送失败处理留在各 adapter；共享的 `/agent`、
`/resume`、review command 走同一 handler。

## 启动、关闭与安全

`ChannelAdapterLifecycle.start()` single-flight：首次先 `delivery.recover()`，恢复可用
intent/step 并重建 hooks；并发 start 等同一 startup task。recovery 失败会取消已建
watch、释放本 owner lease、清理内存，adapter 保持未 started。`run_forever()` 在平台
ingress 前 start，在返回/异常时 finally close；close 先停止 reconciler、取消并等待
watch/effect，再释放 lease，最后由 runner 关闭其 DB/client。close-wins，关闭后不能再次
start；进程重启只丢内存 watch，durable intent/step 留待下次恢复。

Gateway bearer 只用于配置的 `gateway.base_url`，Telegram/Feishu credentials 只用于
各平台 client，不能互换或进入 metadata、receipt response、错误详情、日志。对远端或
非 loopback Gateway 使用非默认 token、HTTPS 和网络控制；Channel 不绕过 Gateway auth。
`/resume` 必须匹配 platform/chat/user/thread 归属；Feishu group policy/mention gate
和 Telegram 支持的 chat type 在配置/adapter 边界执行。外部媒体按平台上限流式读取，
artifact 不能借任意本地 path 作为媒体目录；session/receipt/delivery DB 需按运行数据
保护，不保存平台 secret。

## 测试证据入口

行为证据的 canonical 入口为：

- [turn/session](../../tests/unit/test_channel_turn.py)、[identity](../../tests/unit/test_telegram_identity_network.py)、[Feishu identity](../../tests/unit/test_feishu_client_identity.py)：turn 选择、agent-scoped session、DM/group/thread key 与 resume 归属。
- [event receipts](../../tests/unit/test_channel_event_receipts.py)、[Telegram receipts](../../tests/unit/test_telegram_receipts.py)、[Feishu receipts](../../tests/unit/test_feishu_receipts.py)：claim/busy/processed、lease、fence 与平台 receipt。
- [watch](../../tests/unit/test_task_watch.py)、[delivery](../../tests/unit/test_channel_delivery.py)：polling、stale/superseded、review/terminal grace、durable step、lease、retry/redrive 与 recovery。
- [presentation](../../tests/unit/test_channel_presentation.py)、[Telegram formatting](../../tests/unit/test_telegram_formatting.py)、[Feishu presentation](../../tests/unit/test_feishu_presentation.py)：review/terminal/artifact 顺序、fallback 与平台输出。
- [adapter lifecycle](../../tests/unit/test_channel_delivery_adapter_lifecycle.py)、[Gateway flow](../../tests/integration/test_channel_gateway_flow.py)：single-flight/close-wins、重启恢复，以及真实 Gateway HTTP + SQLite 的 turn/run/delivery 协作。

## 何时同步本文

仅在以下稳定边界变化时同步：

- Channel 与 Gateway/runtime 的调用 ownership、TaskGroup/runner 的生命周期或配置的
  Gateway endpoint/auth boundary；
- identity/session key、active agent、resume 归属或 turn create/input/review semantics；
- event/turn receipt、delivery intent/step、lease、retry、redrive、watch polling/SSE 选择
  或恢复保证；
- review/terminal/artifact presentation、平台 ingress/ack/fallback、媒体上限或新增
  adapter 的信任边界。

平台具体 API 仍由 adapter 封装；只在上述 public contract、ownership、state/recovery
或 trust boundary 变化时更新本文及相应 canonical evidence。
