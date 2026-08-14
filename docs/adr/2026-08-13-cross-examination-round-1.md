# 交叉质询第 1 轮：工作流编排架构分歧

**角色:** 独立架构分析师  
**日期:** 2026-08-13  
**参考:** 另一方案（未指名），`2026-08-12-minimal-durable-workflow-layer.md` (Proposed ADR)，`2026-08-13-workflow-orchestration-architecture-analysis.md`

---

## 议题 1：权威运行态存储——单行 JSON vs 规范化表

### 另一方案立场

V1 ≤50 节点且单进程时，`workflow_runs.node_runs_json` 单行 JSON 列足够。

### 我的裁决：**ACCEPT 对方立场，但有严格条件**

**证据分析：**

当前代码库实际展示了两种并行更新模式：

1. **TaskStore.save_task**：全行 INSERT OR REPLACE。整个 TaskRecord 序列化为一行。多列更新（state、result、error、run_count、pending_review）通过同一把 `threading.RLock` 串行化。不存在列级并发冲突，因为每次 `save_task` 写整行。

2. **MailboxStore.claim**：`BEGIN IMMEDIATE` + batch UPDATE + COMMIT。使用 `claimed_by` + `claim_token` 作为乐观锁/owner guard。关键保护是 `WHERE status = 'pending'` 条件——两个并发 claim 不会抢到同一条消息。

对于工作流 NodeRun，并发写入场景是：
- 引擎收到 mailbox settled 通知 → 更新 NodeA 状态为 completed
- 同时，验证步骤完成 → 更新 NodeA 状态为 verified

**但是**，在单进程 `asyncio` 模型下，这两种写入不会真正并发——它们运行在同一个 event loop 中。`asyncio.Lock` 足以保证 NodeRun 级别的互斥。这就是为何单行 JSON 在 ≤50 节点时可接受的核心原因。

**Crash window 分析：**

单行 JSON 的 write hole：
```text
1. 读 workflow_runs.node_runs_json  → {"A": {...}, "B": {...}}
2. 修改 NodeA.state = "completed"
3. 写入全行 JSON
```

如果步骤 2 和 3 之间 crash：零损失（写入未发生）。如果步骤 3 中途 crash：SQLite WAL 的原子性保证要么全写，要么全不写——没有「部分 JSON」问题。这是 WAL 的核心保证。

但是存在逻辑 write hole：
```text
1. 引擎调度 NodeA spawn → 写入行，NodeA.state = "running"
2. 引擎同时调度 NodeB spawn → 读取行，NodeB.state 的"pending→running"变更被另一个协程的读覆盖丢失
```
这在单进程 asyncio 下受 `asyncio.Lock(run_id)` 保护。没有协程切换点意味着没有 write hole。

**严格条件（必须满足）：**

| 条件 | 理由 |
|------|------|
| 每个 WorkflowRun 必须有独立的 `asyncio.Lock` | 防止同一个 run 内的并发写入 |
| 写入前必须重新读取最新行（read-modify-write 不是 read-cached-write） | 防止 stale 数据覆盖 |
| JSON 列必须包含 `version` 字段（单调递增整数） | 允许恢复时检测旧 schema 迁移 |
| V2 前必须迁移到 `workflow_node_runs` 规范化表 | 超过 50 节点时 JSON 重组开销不再可接受 |

**查询成本评估（≤50 节点）：**
- 读取全行 JSON → 解析 → 查找一个 NodeRun：O(n)，n ≤ 50 → < 1ms
- 全行写入 JSON 序列化 → 50 个 NodeRun → ~5-10KB → < 2ms
- SQLite INSERT OR REPLACE on WAL：~1ms

**结论：ACCEPT**——单行 JSON 在 V1 约束（≤50 节点，单进程 asyncio）下是合理的，条件是在 WorkflowEngine 中做到 lock + re-read-before-write。拒绝在 V1 过早进行规范化，因为查询模式尚不明确。

---

## 议题 2：引擎强制输出 schema vs 格式无关

### 另一方案立场

WorkflowEngine 应强制主持 Agent 输出结构化 schema（ACCEPT/DISAGREE/NEEDS_HUMAN）。

### 我的裁决：**DISAGREE——坚持己方立场**

**核心论点：** 强制输出 schema 将引擎专用化（为一类工作流定制），而非通用化。这违反了 Proposed ADR 的「additive」原则和现有 Gateway Task 的「Agent 自决」模型。

**证据：**

1. **Ruyi 现有架构完全格式无关。** `TaskRecord.result` 是 `str | None`。`AgentControl._run_agent_payload` 中的所有后处理（`normalize_agent_turn`、`review_payloads` 检查）都是在 Task 状态机 **之外** 进行的——它检查 `outcome` 结构来决定状态转换，但从不解构 `result` 字符串来做调度决策。

2. **引擎的调度决策基于状态，而非内容。** 引擎驱动 DAG 的信息是：
   - NodeRun.state ∈ {completed, failed, verified, waiting_for_human, skipped}
   - Edge.condition ∈ {on_completed, on_failed, always}
   
   这两者都不需要检查 NodeRun 输出的语义内容。`ACCEPT/DISAGREE` 是人类或下游 Agent 要消化的——不是引擎。

3. **如果引擎强制输出 schema，则出现边界问题：**
   - 验证失败是否等同于「DISAGREE」？还是「DISAGREE」另有含义？
   - 如果一个分析节点输出纯技术方案（不含 ACCEPT/DISAGREE），但验证通过——引擎应如何处理？
   - 强制 schema 意味着引擎必须拒绝不符合 schema 的输出——这是 Agent 层的责任（`VerificationSpec`），不是引擎层的通用能力。

**应该如何处理「收敛」：**

收敛逻辑属于 DAG 的一个节点（主持者的 fan-in 节点），在其 `input_template` 中包含明确的收敛提示：

```text
你是主持 Agent。以下是并行分析的结果：

{node_codex_architect.result}
{node_deepseek_architect.result}

请识别共识点与分歧点，逐项裁决。如果某个分歧需要人工判断，
使用 submit_review 工具请求审批。
```

引擎只负责将上游 `result` 字符串代入模板，调用 `send_task_input`。主持 Agent 自行解读和裁决。**收敛发生在 Agent 语义层，不在引擎的图调度层。**

**对于「ACCEPT/DISAGREE/NEEDS_HUMAN」枚举值：** 这些是特定工作流（架构审查工作流）的契约，应作为 `VerificationSpec.schema` 或 `input_template` 提示词中的约束存在——是可选的、按工作流配置的，不是引擎强制执行的。

**结论：DISAGREE**——引擎保持格式无关，只做字符串模板替换和状态驱动调度。特定工作流的输出约束通过 `VerificationSpec` 和 `input_template` 配置，不进入引擎核心。

---

## 议题 3：持久化 attempt 级记录和 Gateway run 关联

### 另一方案立场

需要持久化 attempt 级记录（独立的 `workflow_node_attempts` 表），以及显式的 workflow→Gateway run 关联。

### 我的裁决：**ACCEPT 部分，拒绝部分**

**逐层分析：**

### 3a. `NodeRun.current_run_count + (task_id, run_count)` 是否足够？

**足够用于正常运行的 fan-in 检测。** 当引擎收到 mailbox 的 settled 通知时，`InterAgentMessage` 携带 `(child_task_id, child_run_count)`。引擎将 `child_task_id` 匹配到 `NodeRun.task_id`，将 `child_run_count` 与 `NodeRun.attempt` 进行比对：

```python
if msg.child_task_id == node_run.task_id and msg.child_run_count >= node_run.last_observed_run_count:
    node_run.last_observed_run_count = msg.child_run_count
```

这样即使同一个 settled 消息被投递多次（mailbox 重放），去重逻辑也仍然有效。

**但有一个 crash window 需要关注：**

```
1. AgentControl.mark_completed(task_id) → TaskRecord 写入 SQLite，state="completed"
2. AgentControl._maybe_publish_settled_message(task_id) → mailbox 发布
   ——CRASH 发生在这里——
```

**恢复时：** 引擎通过 `TaskStore.get_task(task_id)` 读取 TaskRecord，看到 `state="completed"` 但 mailbox 中没有对应消息。引擎 **不能** 依赖「无消息 = 未完成」，因为 TaskRecord 已明确显示完成。

**解决方案：** 恢复时，引擎通过 `TaskStore` 直接获取所有相关 Gateway Task 的最新状态，而非仅依赖 mailbox 消息。伪代码：

```python
async def recover(self, run_id: str) -> None:
    run = self.store.load_run(run_id)
    for node in run.node_runs.values():
        if node.task_id is not None:
            task = await self.control.refresh_task(node.task_id)  # 走 TaskStore 查询
            self._reconcile_node_state(node, task)
    self._resume_scheduling(run)
```

不依赖 mailbox 作为唯一事件源。恢复时以 TaskRecord（权威）为准，mailbox 只是运行时通知的加速通道。

### 3b. 是否需要独立的 `workflow_node_attempts` 表？

**对于 V1：否。** `NodeRun.attempt`（整数）+ `NodeRun.task_id`（稳定）+ `NodeRun.output_snapshot`（最新）已经足够：
- 重试历史：`attempt` 计数告知引擎这是第几次尝试。每次 `send_task_input` 触发新 run 时递增。
- 输出关联：`output_snapshot` 始终是最新一次尝试的结果。之前尝试的结果不保留——这与 Gateway Task 自身的语义一致（`TaskRecord.result` 只存储最新 run）。
- 故障恢复：恢复后，`attempt` 和 `task_id` 不变。引擎检查 TaskRecord 的 `state` 和 `run_count` 来推断当前尝试的状态。

**V2 考虑：** 如果需求出现「对比第 1 次和第 3 次尝试的输出差异」，则需要 `workflow_node_attempts` 表。目前跳过。

### 3c. 是否需要显式的 workflow→Gateway run 关联键？

**需要，但不是额外的表。** 提议在 `NodeRun` JSON 中加入：

```json
{
  "node_id": "node_1",
  "task_id": "abc-123",
  "attempt": 2,
  "last_observed_run_count": 5,
  "state": "running",
  ...
}
```

`(task_id, last_observed_run_count)` 这个 pair 作为关联键已足够：
- `task_id` 唯一绑定到 Gateway Task Session
- `last_observed_run_count` 指向该 task 中最新观察到的 run
- 恢复时，比较 `last_observed_run_count` 与 `TaskRecord.run_count` 来判断是否有新的 settled run

**不需要** 在 `agent_tasks` 或 `workflow_runs` 表中新增外键列。关联通过 member 字段完成，与现有代码库中 `agent_tasks.parent_thread_id` 的模式一致（也是通过 member 字段关联，不是通过单独的关系表）。

**结论：**
- ACCEPT：恢复时应以 TaskRecord（数据库）为权威源，mailbox 仅作运行加速通道
- DISAGREE：V1 不需要 `workflow_node_attempts` 表，`NodeRun.attempt` + `task_id` + `run_count` 足够
- 具体方案：`(task_id, last_observed_run_count)` 作为隐式关联键，不需要额外关联表

---

## 议题 4：`waiting_for_fan_in` 是否应成为持久状态？

### 另一方案立场

未满足的依赖应解释为 BLOCKED/WAITING，不成为独立持久状态。若所有上游终态但 join 永不满足，应立即 skipped/failed 而非等待。

### 我的裁决：**DISAGREE——坚持己方立场，但收敛为一个更精确的状态**

**逐项反驳：**

### 4a. "未满足的依赖可解释为 BLOCKED/WAITING"

这是运行时推理，不是持久状态。引擎重启后，必须从存储中恢复并重新计算「哪些节点已 ready」。如果节点的上游节点已 settled（completed/verified/failed）但边条件不满足（例如 `on_completed` 边要求上游节点 completed，但上游节点 failed），这个节点 **永远不会** 被调度。它卡在一个稳定的阻塞状态中。

**这个状态必须被存储下来**，否则恢复时需要重新计算全部上游节点的状态→边条件→节点状态——这使得「恢复」变成了「重放 DAG」，违背了 proposed ADR 的恢复原则。

### 4b. "若上游终态但 join 永不满足，应立即 skipped/failed"

**同意「不应永远等待」**，但不同意「应立即」。时序是：

```
T1: NodeA 完成，NodeB 仍在运行。
    NodeC 的 fan-in 条件：A 完成 ∧ B 完成 → 未满足。
    引擎：什么也不做。NodeC 保持 pending。
    
T2: NodeB 失败。边 B→C 条件是 on_completed。
    NodeC 的 fan-in 条件：A 完成 ∧ B 完成 → 永不可能满足。
```

在 T2，引擎检测到「所有上游节点已 settled（completed 或 failed）但 fan-in 永远无法满足」。这时 NodeC 应转换为 **`skipped`**——而非继续等待。

**命名和语义：**

| 状态 | 含义 | 引擎行为 |
|------|------|----------|
| `pending` | 尚未被调度，上游仍有活跃节点 | 等待 mailbox 通知 |
| `blocked` | 上游全部 settled 但 fan-in 未满足，**可能是暂时的**（某个上游是 `waiting_for_human`，审查后会变化） | 等待上游状态变更 |
| `skipped` | 上游全部 settled 且 fan-in **永久无法满足**（所有上游都是终态 completed/failed/cancelled/skipped，但边条件不满足） | 转换为终态，触发 on_failed/always 后继边 |
| `waiting_for_fan_in` | Propose 收回此名称 | — |

**关键区分：`blocked` vs `skipped`**

`blocked` 是暂时的——上游有节点在 `waiting_for_human` 状态，审查结果可能打开新的边条件。例如：
```
NodeA →(on_completed)→ NodeC
NodeB →(on_failed)→ NodeC

NodeA 完成，NodeB waiting_for_human。
→ NodeC = blocked（等 B 的审查结果）
→ B 审查拒绝 → B failed → B→C 的 on_failed 边满足 → NodeC 进入 running
```

`skipped` 是永久的——所有上游都已终态终态（completed/failed/cancelled/skipped），且没有 active/in-flight 节点，边条件无法满足。

**结论：**
- DISAGREE「不需要持久状态」：`blocked` 和 `skipped` 都是持久状态，但语义不同
- DISAGREE「应立即 skipped」：应区分 `blocked`（等待上游审查或重试）和 `skipped`（永久不可满足）
- 修正后的 NodeRun 终态集合：`{completed, verified, failed, skipped, cancelled}`
- `blocked` 不是终态——上游状态变更可以解除阻塞

---

## 汇总

| 议题 | 裁决 | 核心理由 |
|------|------|---------|
| 1. 单行 JSON vs 规范化表 | **ACCEPT 对方** | 单进程 asyncio + ≤50 节点 + asyncio.Lock 足够；V2 再迁移 |
| 2. 引擎强制 schema vs 格式无关 | **DISAGREE** | 引擎应是通用调度器，收敛是 Agent 层语义，通过 input_template 和 VerificationSpec 配置 |
| 3. attempt 级记录和关联 | **部分 ACCEPT** | 恢复时以 TaskRecord 为权威源，不需要独立的 attempt 表，`(task_id, run_count)` pair 作为隐式关联键 |
| 4. waiting_for_fan_in 持久状态 | **DISAGREE（修正）** | 改为 `blocked`（暂时）+ `skipped`（永久）两种状态，均为持久状态 |