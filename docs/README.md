# 文档索引

这里是当前文档入口。先读[架构总览](architecture.md)了解进程级装配、子系统边界和
依赖方向，再按工作内容进入下表的系统文档；这些文档描述当前实现，不复制各篇正文。

## 当前架构与系统文档

| 文档 | 负责什么；何时读 |
| --- | --- |
| [架构总览](architecture.md) | 负责跨子系统的装配、边界、主流程和信任语境；需要理解整体 ownership 或依赖方向时先读。 |
| [运行时配置与 Bootstrap](systems/runtime-configuration-and-bootstrap.md) | 负责路径发现、配置解析和进程生命周期；修改 CLI、配置、启动或关闭流程时读。 |
| [Gateway Task 控制面](systems/gateway-task-control-plane.md) | 负责 Agent/Task command、route、效果编排、投影和恢复；修改任务控制面或幂等语义时读。 |
| [Gateway HTTP 与 gateway_protocol](systems/gateway-http-protocol.md) | 负责 HTTP 适配、wire DTO、游标和 SSE；修改认证、传输协议或事件编码时读。 |
| [Task execution runtime](systems/task-execution.md) | 负责 Task run 的准入、状态、事件、checkpoint 和恢复；修改本地执行生命周期时读。 |
| [Agent graph 与 tool runtime](systems/agent-tool-runtime.md) | 负责 Agent graph、middleware、权限闸门和工具调用；修改模型调用或 tool runtime 组合时读。 |
| [Runtime 委派系统](systems/task-delegation.md) | 负责 local/remote worker、委派策略、proxy 和 reconciliation；修改 spawn、远端委派或委派预算时读。 |
| [Task Mailbox 与 child settlement](systems/task-mailbox-settlement.md) | 负责输入 mailbox、child settlement 和 settled outbox；修改 parent-child 通知或结算恢复时读。 |
| [Channel 系统](systems/channels.md) | 负责 Telegram/Feishu turn、身份、session、receipt 和 delivery；修改平台入口或消息投递时读。 |
| [Model Provider 与 MCP 工具集成](systems/model-and-tool-integrations.md) | 负责 Provider/Codex、MCP registry、tool scope 和刷新错误；修改模型凭据、MCP 或工具发现/调用时读。 |
| [Execution backend 与 workspace 边界](systems/execution-backends-and-workspace.md) | 负责 local/Daytona backend、workspace、attachment 和 artifact 路径边界；修改文件、shell 或执行隔离时读。 |
| [Skills 系统](systems/skills.md) | 负责 skill catalog、effective skills、backend view 和模型可见性；修改 skills 配置或同步时读。 |

## 决策与指南

`docs/decisions/` 和 `docs/guides/` 当前保留目录骨架（仅有 `.gitkeep` 占位），分别
用于架构决策记录和操作/开发指南；它们不替代上面的 active current docs。
