# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

FastAPI 托管的原生 HTML、CSS 与 JavaScript。Web 调试台直接复用现有 Gateway HTTP API，不引入 Node 构建链。

## Users

主要用户是 Ruyi 的开发者和架构设计者。他们在调试多 Agent 协作时，需要提出一个需求、观察主持 Agent 如何委派和收敛，并在必要时直接进入独立的架构 Agent Session 追问。

## Product Purpose

Ruyi 是支持长期独立 Task/Session、Subagent 委派、双向 Mailbox 通信、工具执行与人工审批的 Agent Runtime。当前 Web Surface 首先是开发调试台，用于验证双架构 Agent 与主持 Agent 的协作收敛流程；成功意味着用户能够看清任务关系、消息流、讨论轮次、失败恢复和最终决策，而不必从原始日志中还原过程。

## Positioning

Ruyi 将每个 Agent 保持为可持久化、可继续交互的独立 Session，并以 Task ID 作为统一通信地址。主持、架构分析、执行与验证是建立在现有 Gateway Task 和 Mailbox 之上的编排角色，而不是共享上下文中的角色扮演。

## Operating Context

- 用户通过 Gateway 创建主持 Task 并提交需求。
- 主持 Agent 将相同问题独立委派给 Codex 与 DeepSeek 架构 Agent。
- 主持 Agent 识别共识与分歧，并通过同一子 Task 的后续 Run 继续质询。
- 用户可以进入任一子 Session 单独追问或补充约束。
- 工具调用、Mailbox 消息和错误恢复是调试证据，默认折叠、按需展开。
- 最终结果包括统一方案、未决 Human 选择、风险、验证标准和执行 DAG。

## Capabilities and Constraints

- 当前配置包含 Codex 主持人、Codex 架构师和 DeepSeek 架构师。
- Task 是长期 Session；Run 是 Task 上的一次执行。
- 父子 Task 通过 `send_input(task_id, message)` 双向通信。
- 运行中输入在安全模型边界注入，空闲 Task 可被新消息唤醒。
- Mailbox 和 Task 状态持久化到 SQLite，并支持重启恢复。
- 当前没有独立的 Workflow Definition/Instance；讨论轮次与收敛协议由主持提示词管理。
- 第一版 Web UI 是开发调试工具，不是对外产品界面。
- 第一版不增加 Node 构建链。

## Brand Commitments

- 产品名称为 Ruyi。
- 术语使用 Agent、Gateway Task、Run、Mailbox、Subagent 和 Human Review。
- 界面文案应准确、克制并面向工程调试，不把推测包装成确定事实。

## Evidence on Hand

- Gateway、Task、Mailbox 与 Channel 的现有实现及测试。
- 已完成真实 Codex + DeepSeek 主持收敛 E2E：两个独立子 Task、第二轮 `send_input`、主持裁决和执行 DAG 均成功。
- 没有现成品牌资产、Logo、用户案例或商业数据；界面不得虚构这些内容。

## Product Principles

1. 独立 Session 必须在界面中保持独立，不能压扁成一段混合聊天记录。
2. 默认展示决策过程，底层工具和 Mailbox 证据按需展开。
3. 用户始终可以进入子 Session 直接追问，同时保留其与主持流程的关系。
4. 失败、恢复、等待和人工决策必须是明确状态，而不是沉默或无限加载。
5. Web UI 复用 Gateway 领域语义，不在前端创造第二套 Task 状态机。

## Accessibility & Inclusion

调试台需要完整键盘可操作、清晰焦点状态、足够文本对比度，并在桌面与移动浏览器中保持可读和可操作。
