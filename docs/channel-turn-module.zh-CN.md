# Channel Turn Module 简化设计

## 背景

Telegram 和 Feishu adapter 当前都包含一套相似的对话轮次逻辑：解析 `/new`、`/agent`、`/resume`、审批命令，查找和绑定 session，判断 task 是否 running、waiting review 或 terminal，再创建 task、发送 input 或启动 watcher。

这些逻辑不是平台能力，而是 Ruyi Agent 的跨平台 channel 策略。平台 adapter 应该主要负责接收和发送消息，Channel Turn Module 负责统一的对话状态机。

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
  -> TaskWatch
  -> Channel Adapter send hooks
```

`ChannelTurnHandler` 不直接依赖 Telegram 或 Feishu SDK。它只依赖稳定的端口：

```text
InboundTurn:
  channel
  identity_key
  session_key
  text
  attachments
  reply_ref

TurnAdapterPort:
  send_text
  send_artifact
  ack_task_accepted
  ack_running_task
  mark_watch_started
  mark_watch_done
```

## 关键决定

`identity_key` 和 `session_key` 由平台 adapter 生成，不由 Channel Turn Module 生成。

原因是 session key 的组成依赖平台语义：Telegram 有 DM、group、supergroup topic；Feishu 有 chat、thread、mention 和不同用户 ID。把这些规则放进共享 Module 会污染它的 Interface。

Channel Turn Module 只相信 adapter 给出的 key，并负责用这些 key 读写 Channel Session。

## Task Watch

Task Watch 也属于共享策略，而不是平台 adapter 的私有逻辑。

它负责：

- 轮询当前 Gateway Task。
- 检测 `run_count` 是否已经进入更新的一轮。
- 检测 pending review 并通知用户。
- 检测 terminal 状态并发送结果。
- 发送当前 run 产生的 artifacts。
- 避免同一 run 的 terminal 消息重复发送。
- 支持 terminal 后的短暂 grace checks，捕获延迟出现的 mirrored review。

平台 adapter 只提供发送能力和可选 reaction hook。

## 迁移顺序

1. 提取只读的 `InboundTurn`、`TurnAdapterPort`、`ChannelTurnHandler` 类型和空实现。
2. 先把 review command 解析、review 提交和 pending review 提示迁入 Channel Turn Module。
3. 再迁入 session lookup/bind、`/new`、create task 和 send input。
4. 最后迁入 Task Watch 和 terminal/artifact delivery 策略。
5. Telegram 和 Feishu adapter 删除重复 orchestration，只保留平台 parsing 和 send hooks。

## 测试策略

- 新增共享 Channel Turn tests，覆盖 `/new`、`/agent`、`/resume`、review command、running task、terminal continue、pending review 和 task watch。
- Telegram tests 保留 MarkdownV2、附件下载、session key、topic、Bot API 发送行为。
- Feishu tests 保留 mention、群聊策略、thread、card、reaction 和 SDK 发送行为。
- `ChannelSessionStore` tests 只关注存储 CRUD，不再承载 channel turn policy。

## 预期收益

- 新增平台 adapter 时只实现平台收发，不复制完整对话状态机。
- `/resume`、review、task watch 在所有平台行为一致。
- task/session/review 相关 bug 集中在 Channel Turn Module 修复。
- 测试从重复的平台大场景，收缩为共享策略测试加平台 adapter 小测试。
