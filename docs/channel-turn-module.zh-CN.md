# Channel Turn Module 简化设计

## 背景

Telegram 和 Feishu adapter 当前都包含一套相似的对话轮次逻辑：解析 `/new`、`/agent`、`/resume`、审批命令，查找和绑定 session，判断 task 是否 running、waiting review 或 terminal，再创建 task、发送 input 或启动 watcher。

这些逻辑不是平台能力，而是 Ruyi Agent 的跨平台 channel 策略。平台 adapter 应该主要负责接收和发送消息，Channel Turn Module 负责统一的对话状态机。

当前迁移已经完成：`channels/turn.py` 中的 `ChannelTurnHandler` 接管普通
消息的 Channel Session 恢复、Gateway Task 状态判断、create/send_input 和
会话绑定，并统一解析和提交 Review Command、处理 `/agent` 与 `/resume`。
`channels/task_watch.py` 中的 `TaskWatchManager` 接管共享的 Task Watch 状态机和
并发生命周期。

## 目标

- 把跨平台一致的 **Channel Turn** 规则集中到一个 Module。
- 让 Telegram、Feishu，以及未来的 Slack、Discord、Web UI 复用同一套 `/new`、`/agent`、`/resume`、review 和 task watch 语义。
- 保留平台 adapter 对平台细节的控制，例如 mention、thread、Markdown、卡片、文件发送和 reaction。

## 责任拆分

### Channel Turn Module 接管

- `/new`：强制创建新的 Gateway Task。
- `/agent`：切换当前 Active Agent。
- `/resume`：恢复已有 Channel Session 或 Gateway Task。
- `/approve`、`/reject`、`y`、`n`：提交 Review Command。
- 读取和更新 Channel Session。
- 查找当前 session 绑定的 Gateway Task。
- task 有 pending review 时优先提示审批。
- task 仍在 running 时拒绝继续输入或提示等待。
- task 已 terminal 时允许继续 `send_input`。
- 创建新的 Gateway Task。
- 向已有 Gateway Task 发送 input。
- 启动 Task Watch，观察当前 run 的后续状态。

### Channel Adapter 保留

- 接收平台原始消息。
- 计算平台语义下的 `identity_key` 和 `session_key`。
- 处理平台 mention、群聊、thread/topic 规则。
- 下载用户上传附件，并转为 Gateway attachment。
- 发送文本、图片、文件或卡片。
- 处理平台格式，例如 Telegram MarkdownV2、Feishu markdown card。
- 处理平台反馈，例如 Feishu reaction。

## 目标结构

```text
Telegram / Feishu raw event
  -> Channel Adapter
  -> InboundTurn
  -> ChannelTurnHandler
  -> GatewayTaskClient / ChannelSessionStore
  -> TaskWatchManager
  -> Channel Adapter send hooks
```

`ChannelTurnHandler` 不直接依赖 Telegram 或 Feishu SDK。它只依赖稳定的端口：

```text
InboundTurn:
  channel
  session_key
  agent_name
  text
  metadata / fallback_metadata
  attachments

ChannelTurnResult:
  kind = pending_review | active | started
  task
  created
```

当前通过 `before_continue` hook 保留平台对上一轮 settled 结果的投递时机。
Task Watch 的轮询、重试、持久化 intent、启动恢复、lease/fencing 与逐步 delivery
ledger 由共享 Module 处理；文本、reaction 和 artifact 的实际平台发送仍由
Channel Adapter hook 处理。

## 关键决定

`identity_key` 和 `session_key` 由平台 adapter 生成，不由 Channel Turn Module 生成。第一阶段只有普通消息路由需要 `session_key`；`identity_key` 仍留在 Adapter，用于 Active Agent 命令。

原因是 session key 的组成依赖平台语义：Telegram 有 DM、group、supergroup topic；Feishu 有 chat、thread、mention 和不同用户 ID。把这些规则放进共享 Module 会污染它的 Interface。

Channel Turn Module 只相信 adapter 给出的 key，并负责用这些 key 读写 Channel Session。

## Task Watch

Task Watch 属于共享策略，而不是平台 adapter 的私有逻辑。

它负责：

- 轮询当前 Gateway Task。
- 检测 `run_count` 是否已经进入更新的一轮。
- 检测 pending review 并触发平台 hook。
- 检测 terminal 状态并且每次 watch 只触发一次 terminal hook。
- 支持 terminal 后的短暂 grace checks，捕获延迟出现的 mirrored review。
- 管理相同 task/run 的并发去重、活跃状态和等待生命周期。
- 对瞬时 Gateway 查询错误执行有界指数退避，不把查询错误展示为 Task failed。
- 持久化 watch/delivery intent，并在 adapter 启动时恢复。
- 用 fenced lease 协调重复进程，用逐步 ledger 恢复 terminal/review/artifact 投递。

平台 adapter 通过 hook 发送结果、artifacts 和可选 reaction；跨 watch 的
terminal/review delivery 去重由共享 durable coordinator 维护。平台 API 不接受
幂等键时，send 成功而 ledger 提交前崩溃仍可能产生重复，具体边界见
`docs/adr/2026-08-30-durable-channel-delivery.md`。

## 迁移顺序

1. ~~提取 `InboundTurn`、`ChannelTurnResult` 和 `ChannelTurnHandler`，迁移普通消息的 Session/Task 路由。~~ 已完成。
2. ~~把 Review Command 解析、待审批 Task 恢复、review_id 校验和决策提交迁入 Channel Turn Module。~~ 已完成。
3. ~~迁入 session lookup/bind、`/new`、create/send input、`/agent` 和 `/resume`。~~ 已完成。
4. ~~迁入 Task Watch 的轮询状态机和并发生命周期。~~ 已完成。
5. ~~Telegram 和 Feishu adapter 删除重复 orchestration，只保留平台 parsing 和 send hooks。~~ 已完成。

## 测试策略

- 共享 Channel Turn tests 覆盖 `/new`、`/agent`、`/resume`、review command、running task、terminal continue 和 pending review。
- 共享 Task Watch tests 覆盖 run supersede、pending review、terminal、grace checks、
  瞬时错误重试、并发 fenced claim、关闭等待和启动恢复。
- Telegram tests 保留 MarkdownV2、附件下载、session key、topic、Bot API 发送行为。
- Feishu tests 保留 mention、群聊策略、thread、card、reaction 和 SDK 发送行为。
- `ChannelSessionStore` tests 只关注存储 CRUD，不再承载 channel turn policy。

## 预期收益

- 新增平台 adapter 时只实现平台收发，不复制完整对话状态机。
- `/resume`、review、task watch 在所有平台行为一致。
- task/session/review 相关 bug 集中在 Channel Turn Module 修复。
- 测试从重复的平台大场景，收缩为共享策略测试加平台 adapter 小测试。
