# System Tools 与父子 Task 通信设计

## 背景

Ruyi 当前按代码条件隐式装配系统工具：配置 `workers` 的 Agent 获得整套委派工具，Backend 自动带来 Filesystem 工具，Artifact、Tool Search、Skills 和 Memory 又分别由不同条件装配。这些规则缺少统一配置入口和可观察的最终结果。

Task Mailbox 已支持运行中的输入、安全模型边界注入、空闲 Task 唤醒与持久恢复，但子 Agent 主动联系父 Agent 的工具链尚未接通：没有 Worker 的 Agent 不会获得 `send_input`，有 Worker 的 Agent 也只能操作自己的直接子 Task。

## 目标

1. Ruyi 内建系统工具可以按 Agent 配置。
2. 系统根据 Agent 拓扑与 Runtime 能力自动推导合理默认工具。
3. 显式配置与自动推导合并，并允许显式禁用。
4. 父子 Task 双向通信继续统一使用 `send_input(task_id, message)`。
5. 工具通过隐藏运行时身份校验 Task ID，不依赖模型声明自己的身份。
6. 错误结果告诉 Agent 为什么失败、哪些 ID 可用以及如何修正。
7. 保持按需注入，避免给所有 Agent 暴露无关工具。

## 非目标

- 不开放兄弟 Task 或任意 Task 间通信。
- 不实现 Workflow 授权图或 peer-to-peer 通信。
- 不增加 `send_message`、`send_to_parent` 或 `target="parent"`。
- 不修改 Gateway Task、Run 或 Task Mailbox 的生命周期模型。
- 不重新设计第三方 MCP 配置。

## 配置模型

每个 Local Agent 新增两个可选字段：

```toml
[agents.architect]
workers = ["executor"]
system_tools = ["artifacts"]
disabled_system_tools = ["cancel_agent"]
```

最终集合按以下顺序计算：

```text
自动推导工具 ∪ system_tools − disabled_system_tools
```

规则：

- 两个字段缺省为空列表。
- `disabled_system_tools` 优先级最高。
- 重复项去重。
- 未知名称导致配置加载失败，并列出合法名称。
- 显式启用但 Runtime 不具备依赖的工具导致配置加载或装配失败，不静默忽略。
- 启动日志输出每个 Agent 的最终系统工具清单及来源（自动、显式、禁用）。

第一版直接使用稳定工具名，不额外引入能力组别名，避免同时维护“能力组”和“工具名”两套词汇。未来工具数量明显增长后再增加能力组。

## 系统工具目录

系统维护唯一的 System Tool Catalog。每一项声明：

- 工具名称。
- 构造工具所需依赖。
- 自动推导条件。
- 是否带模型提示词。
- 与其他工具的组合约束。

第一版目录至少覆盖：

| 工具 | 自动推导条件 |
|---|---|
| `spawn_agent` | Agent 配置了至少一个 Worker |
| `wait_agent` | Agent 配置了至少一个 Worker |
| `check_agent` | Agent 配置了至少一个 Worker |
| `send_input` | Agent 配置了 Worker，或该 Agent 被其他 Agent 用作 Worker |
| `cancel_agent` | Agent 配置了至少一个 Worker |
| `list_agents` | Agent 配置了至少一个 Worker，或被其他 Agent 用作 Worker |
| `publish_artifact` | Runtime 提供 Artifact 注册与 Backend 下载能力 |
| `tool_search` | Agent 配置 `tool_search = true` |
| `call_tool` | Agent 配置 `tool_search = true` |

### Backend、Sandbox 与 Filesystem

当前所有 Runtime Agent 都会获得 Backend，并无条件装配 `FilesystemMiddleware`；Local Backend 与 Daytona Sandbox 只是同一 Backend Interface 的不同 Implementation。因此第一版保持这一既有语义：

- 配置 Local Backend 或 Daytona Sandbox 都自动提供 Filesystem/Execute 能力。
- Backend 能力属于 Runtime 基础能力，记录进最终系统工具清单。
- 若未来出现无 Filesystem 的 Backend，再由 Catalog 根据 Backend capability 判断，而不是根据 `sandbox=true/false` 硬编码。

Skills、Memory、Mailbox、Summarization、Permission Review 属于 Middleware 能力，不全部表现为模型可调用 Tool；它们也进入统一解析结果，但启动日志应区分 `tools` 与 `middleware`，避免把 Middleware 错称为 Tool。

## 解析 Module

新增一个集中、无运行副作用的 System Tool Resolver，输入为：

- 全量 Agent 配置与 Worker 拓扑。
- 当前 Agent 名称。
- Runtime capability snapshot。
- 显式 `system_tools`。
- 显式 `disabled_system_tools`。

输出为不可变的 resolved system capabilities，其中分别保存：

- 最终 Tool 名称。
- 最终 Middleware 名称。
- 每项能力的来源。
- 配置诊断信息。

`bootstrap` 负责构造 capability snapshot；`agent_factory` 和 Middleware stack 只消费解析结果，不再分别重复推导规则。

## Task 身份注入

每次本地 Task Run 已经在 `RunnableConfig.configurable` 中携带：

```text
task_id
parent_task_id
root_task_id
thread_id
agent_name
```

Task 工具改用 LangChain `ToolRuntime` 自动读取这些字段。`ToolRuntime` 参数由 LangChain 注入且不出现在模型看到的工具 schema 中。

模型仍然调用：

```text
send_input(task_id, message)
```

模型提供目标 Task ID；Runtime 自动确定调用者 Task ID、父 Task ID与任务树关系。模型不能提供或覆盖调用者身份。

根 Task 没有 `parent_task_id`，因此只能向自己的直接子 Task 发送输入。

## Task 工具授权

授权按工具分别判断，不能复用一个宽泛的“Task 可见”布尔值。

| 工具 | 直接父 Task | 直接子 Task | 其他 Task |
|---|---:|---:|---:|
| `send_input` | 允许 | 允许 | 拒绝 |
| `wait_agent` | 拒绝 | 允许 | 拒绝 |
| `check_agent` | 拒绝 | 允许 | 拒绝 |
| `cancel_agent` | 拒绝 | 允许 | 拒绝 |

`spawn_agent` 不接收 Task ID，只能选择当前 Agent 配置允许的 Worker 名称。

Task 关系必须从 TaskStore/TaskManager 中的记录验证，不能只相信调用配置中的 `parent_task_id`。校验步骤：

1. 从隐藏的 ToolRuntime 获取调用者 Task。
2. 加载调用者和目标 Task 的持久记录。
3. 判断目标是否为调用者的直接父级或直接子级。
4. 应用当前工具的方向规则。
5. 通过后再执行操作。

Gateway 或非 Task 上下文调用系统工具时，没有调用者 Task 身份，应明确拒绝 scoped 工具；Gateway Task Module 继续使用内部结构化 Interface，不走模型工具授权。

## 错误反馈

所有 scoped Task 工具使用统一错误渲染器，错误必须包含：

- 失败原因。
- 当前工具允许的方向。
- 可用的精确 Task ID。
- 下一步修正动作。

示例：

```text
Cannot send input to task 'task_x'.
send_input may target only your direct parent or direct children.
Allowed task IDs:
- parent: task_parent
- child: task_a
- child: task_b
Use an exact ID above, or call list_agents to refresh the task list.
```

不存在的 ID：

```text
Unknown task_id 'task_x'. Call list_agents and use an exact task_id from its result.
```

方向错误：

```text
Task 'task_parent' is your direct parent. send_input is allowed for a parent task, but cancel_agent is not.
```

错误不得泄露其他任务树、兄弟 Task 或不可见 Agent 的 ID。

## `list_agents` 结果

拥有 `send_input` 的子 Agent 必须能够发现父 Task ID。因此 scoped `list_agents` 返回：

- 当前 Task 身份。
- 直接父 Task（若存在）。
- 直接子 Task。
- 当前 Agent 可创建的 Worker 类型。

不得返回兄弟 Task 或其他 Task Tree。父 Task 出现在列表中不代表可以对其执行 `wait_agent`、`check_agent` 或 `cancel_agent`；每个工具仍独立校验方向。

## 提示词与工具描述

委派提示词按最终工具集合生成，不能描述未注入的工具。

更新 `send_input` 描述：

```text
Send additional input to your direct parent task or one of your direct child tasks. If the target is running, the input is delivered at its next safe model boundary; otherwise the task is awakened.
```

删除“必须等当前 Run 结束后才能 send_input”的旧规则。

只拥有父级通信能力的 Worker 不显示 spawn/wait/cancel 工作流，只提示它可以在需要澄清、升级风险或报告关键进展时向直接父 Task 发送输入。

## 主要代码改动范围

- `config/loader.py`
  - 解析和校验两个新字段。
  - 建立 Agent 被谁引用为 Worker 的反向拓扑。
- 新增 System Tool Catalog/Resolver Module。
- `runtime/bootstrap.py`
  - 构造 Runtime capability snapshot。
  - 将解析结果附加到 Local Agent Spec。
- `runtime/agent_factory.py` 与 `runtime/middleware/stack.py`
  - 按解析结果装配 Tool 与 Middleware。
- `runtime/delegation/async_runtime.py`
  - Task 工具通过 LangChain 隐藏的运行配置取得调用方身份。
  - 将通用可见性判断替换为按操作授权。
  - 改进 `list_agents` 与错误反馈。
- `runtime/middleware/worker_delegation.py`
  - 根据最终工具集合生成提示词。
  - 更新 Active Run `send_input` 语义。
- 配置模板、配置文档和架构文档。

## 兼容与迁移

- 未配置新字段时保持自动推导，因此现有 `workers` 配置无需修改。
- 作为 Worker 但没有自己的 Worker 的 Agent 会新增 `send_input` 和 `list_agents`。
- 旧配置中的 `tool_search` 继续有效，并参与统一解析。
- `send_input(task_id, message)` schema 保持不变。
- 现有父 Agent 操作直接子 Task 的行为保持不变。
- 旧的“运行中不能 send_input”提示和错误语义被移除。

## 测试要求

### 配置与解析

- 显式工具与自动推导合并。
- 禁用列表优先。
- 未知名称和缺失 Runtime capability 报错。
- Worker 反向拓扑自动获得父级通信工具。
- 无关 Agent 不获得委派工具。
- Backend、Artifact 和 Tool Search 推导结果正确。

### 授权

- 父 Task 向直接子 Task `send_input` 成功。
- 子 Task 向直接父 Task `send_input` 成功。
- 兄弟 Task、其他树和未知 Task 被拒绝。
- 子 Task 不能 wait/check/cancel 父 Task。
- 错误只列出允许范围内的 ID。
- ToolRuntime 身份字段不出现在模型工具 schema 中。

### 端到端

- 子 Agent 在 Run 中向父 Task 发送问题，父 Task 正在运行时在安全模型边界收到。
- 父 Task 空闲时被该消息唤醒。
- 无 Worker 的叶子 Agent 仍能联系父 Task。
- 重启恢复后父子授权与消息投递保持正确。

## 验收标准

1. 每个 Agent 的最终系统 Tool/Middleware 集合可预测、可配置、可观察。
2. 没有 Worker 的叶子 Agent 默认只获得必要的父级通信工具，不获得整套委派工具。
3. 子 Agent 无需伪造调用者身份，可以使用父 Task ID 联系直接父级。
4. 所有 Task ID 操作都按隐藏运行时身份和持久 Task 关系校验。
5. 错误信息能让 Agent 在下一次工具调用中改正参数，同时不泄露不可见 Task。
6. 现有配置在不修改的情况下继续工作。
7. 全量测试、目标静态检查与新增端到端测试通过。
