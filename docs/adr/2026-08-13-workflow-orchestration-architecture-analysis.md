# 独立架构分析：Ruyi 动态工作流编排层

**角色:** 独立软件架构分析师
**日期:** 2026-08-13
**决策状态:** 分析（等待人工审批）
**参考:** `docs/adr/2026-08-12-minimal-durable-workflow-layer.md` (proposed ADR)

---

## 假设

1. `WorkflowEngine` 在 `AgentControl` 同一进程中运行（单进程调度，proposed ADR 约束 #2 已确认）。
2. 工作流节点仅生成 Gateway Task（不直接调用模型或执行代码），所有执行通过现有 Task 生命周期。
3. 现有 Task Mailbox 的 claim/acknowledge lease 机制足以作为工作流状态变更的事件总线——不需要新的消息队列。
4. `parent_task_id` 委托树保持严格树形（深度限制、预算），不引入任何 DAG 语义——这是 proposed ADR 的核心见解，也是本分析的基础约束。
5. 第一版工作流定义是创建时静态的（不动态插入节点），与 proposed ADR 约束 #3 一致。

---

## 约束

1. **Gateway Task 不可变**：Task 是稳定的 `task_id`/`thread_id`，一个 run 结束后 Task 保持可继续输入。不能为工作流改变 `TaskRecord` 模式或 Task 状态机。
2. **Mailbox 不可变**：`MailboxStore`（SQLite）和 `AgentMailbox`（进程内）的 claim/ack/lease 语义保持原样。工作流层只能作为额外的消费者。
3. **委托树不可变**：`parent_task_id` 严格树形，预算由 `root_task_id` 分组执行。不能添加多父级或 DAG 边到此字段。
4. **单进程调度**：`WorkflowEngine` 在同一进程中运作，使用 `asyncio`，不分布式。
5. **节点间数据为字符串**：`input_content`/`result`/`error` 均按字符串传递（与现有 Gateway Task 语义一致）。
6. **V1 工作流上限 50 节点**（proposed ADR 缓解措施）。
7. **验证步骤同步于节点完成**——节点在通过验证前不会触发下游（proposed ADR 约束 #5）。
8. **WorkflowEngine 是基础设施，不是 Gateway Task**（proposed ADR 已拒绝选项 D）。

---

## Proposed ADR 评估

我仔细审查了 `2026-08-12-minimal-durable-workflow-layer.md`。其核心架构判断是正确的：

- **赞同：DAG 边独立于 `parent_task_id`**。钻石依赖（A→C, B→C）有根本性冲突。`parent_task_id` 单值列无法编码多个入站依赖。委托树是控制结构，工作流 DAG 是数据流结构——它们实质上是正交的。

- **赞同：WorkflowEngine 作为独立图调度器**。将其作为 Gateway Task 将产生引导问题并使重启恢复复杂化（见选项 D）。

- **赞同：基于 Mailbox 的 fan-in 监视器**。复用现有 `parent_thread_id` 路由和相关模式，干净的增量设计。

以下是我与 proposed ADR 存在分歧的地方，以及需要进一步细化的点：

---

## 分歧与细化

### 1. 主持 Agent 应是普通 Gateway Task，还是基础设施调度器？

**Proposed ADR：** 未明确界定。暗示工作流节点就是主持 Agent。主持是一个节点，与其他节点一样执行 `agent_name`。

**DISAGREE（部分）：存在概念歧义需要解决。** "主持 Agent" 扮演两个不同的角色，必须在架构中区分：

- **角色 A：作为工作流节点的 Agent**。其行为类似其他节点——接收输入，执行工作，产生输出。这确实是一个普通 Gateway Task。当工作流 DAG 的拓扑排序需要时由引擎调度。例如："分析需求并产生架构方案"。

- **角色 B：作为工作流调度器的 Agent**。工作流实例启动时，创建者可能是主持 Agent，其输出（"并行分析此需求，然后我将收敛分歧"）驱动工作流激活。但工作流一旦激活，**WorkflowEngine 接管调度**——哪个节点何时启动、fan-in 何时满足、验证是否通过——这些都由引擎执行，不由任何 Agent。主持 Agent 的"收敛分歧"步骤不是图调度逻辑；而是**对一个节点（与任何其他节点一样）的输入**，引擎在 fan-in 满足后传递。

**推荐：**
- 主持 Agent 是发起工作流的普通 Gateway Task。它运行至完成（生成图激活指令），后续可通过向自身节点发送 `send_input` 重新进入工作流状态——但那是数据流，不是调度。
- "收敛与裁决" 节点是 DAG 中的一个节点，引擎在 fan-in 条件下调度。没有特殊的"主持会议"权限。
- 收敛输入由引擎通过 `send_task_input` 写入该节点，聚合上游输出和一个收敛提示。
- 引擎永远不解释 Agent 输出。它只跟踪 `state ∈ {completed, failed, waiting_for_human, …}` 和 `result` 字符串。收敛逻辑由 Agent 自行处理。

### 2. Workflow Pause：全局还是仅阻断依赖分支？

**Proposed ADR：** "工作流级暂停"——当任何节点进入 `waiting_for_human` 时，整个工作流转换为 `paused_for_review`。"它防止在中图审查待处理时生成下游节点。"

**DISAGREE（严重）：** 全局暂停对于并行工作流过于粗暴。考虑：

```
Node A (running) ──→ Node C (pending, depends on A)
Node B (waiting_for_human) ──→ Node D (pending, depends on B)
```

在全局暂停下，A 将完成，但 C 不会生成——尽管 C 不依赖于 B。这违反了并行的预期收益。

**推荐：仅阻断依赖分支。** 暂停语义应为：
- 当 Node B 进入 `waiting_for_human` 时，只有 B 的后继节点（D）在 B 被审查解决之前不能被评估为就绪。
- 不通过 B 的依赖链上的节点（A → C）继续执行。
- 工作流运行状态反映最弱进度：`running`（如果有活跃节点）> `paused_for_review`（如果有等待人工的节点且没有活跃节点）> `waiting_for_fan_in`（所有存活节点已 settled 但某些 fan-in 不完整）。
- `paused_for_review` 现在是工作流运行的**合取状态**，不是全局锁。这样命名也消除了歧义：它意味着"此工作流有活动审查"，不是"所有执行已停止"。

### 3. Workflow Run 状态：全局 Gating

鉴于上述情况，实际工作流运行状态机应为：

```text
running ──→ paused_for_review ──→ running ──→ ... (循环)
  │              │
  │              ├──→ cancelled
  │              └──→ failed (不可恢复的节点故障，且没有成功的 on_failed 边)
  │
  ├──→ waiting_for_fan_in (所有存活节点已 settled，存在未满足的依赖)
  │       └──→ running (fan-in 满足或恢复)
  │
  └──→ completed (所有终止节点已 settled 且通过验证)
```

"暂停"不是运行状态的终结状态——它是暂时的，在审查解决后恢复为 `running`。"全局暂停"不存在。

### 4. 重试：复用同一 Task Session，还是新建？

**Proposed ADR：** 未明确。`NodeRun.attempt` 计数器表示重试已设计，但未指定语义。

**立场：取决于重试策略配置。** 必须区分：

| 重试策略 | Task Session | 用例 |
|---|---|---|
| `retry_same_session` | 复用同一 `task_id`，发送新的 `send_task_input`，递增 `run_count` | 瞬态故障（API 超时）、可恢复错误、人工审查后"重试" |
| `retry_new_session` | 创建新的 `TaskRecord`，新的 `task_id` | 确定性故障（配置错误）、模型幻觉重试、不同的 agent_name 重试 |
| `no_retry` | 节点标记为 `failed`，引擎评估 `on_failed` 边 | 非可恢复结果 |

**`retry_same_session` 下的关键细节：**
- `NodeRun.task_id` 保持不变。`NodeRun.attempt` 递增。
- 引擎调用 `AgentControl.send_task_input(task_id, retry_prompt)`，注入关于先前失败原因和重试说明的上下文。
- Task 的 `run_count` 在每次尝试时递增，因此订阅者（mailbox 监视器）可以将 `(child_task_id, child_run_count)` 关联到正确的尝试。
- 这意味着 **NodeRun 不直接绑定到单次 Task run**。它绑定到 Task Session。每个 `run_count` 都是一次不同的尝试。

**推荐：**
- 短期默认重试（最多 3 次）使用 `retry_same_session`。
- 将确定性失败映射到 `on_failed` 边，不重试。
- 将 `retry_new_session` 推迟到 V2，因为需要额外语义来处理多个 `task_id`。

### 5. Node Run 输出格式：JSON 单行 vs 规范化表

**Proposed ADR 没有涉及。这是新问题。**

**立场：规范化表——但定义"规范化"在引擎的通用语义中，而不是特定于域的语义。** 原因：

- 工作流节点间数据流通过字符串传递。如果节点输出是 JSON，它仍作为字符串 `result` 流动。引擎不解析 JSON 来做出图决策。
- 但是，fan-in 聚合需要结构化。如果三个并行分析节点各自产生一个"方案"，主持人需要看到所有三个方案并排比较。这需要**引擎在 fan-in 点整合**，而不仅仅是连接输出。
- 因此，fan-in 节点的 `input_template` 应支持变量替换：`{node_B.result}`、`{node_C.error}` 等。
- 引擎不需要知道 JSON 与纯文本的区别。它只进行字符串替换。
- 如果确实需要 JSON（例如下游解析更简单），那是约定而不是引擎强制执行。引擎是格式无关的。

**推荐：** 引擎不规定格式。Fan-in 输入模板通过 `"{node_id.result}"` 引用进行构建。节点 Agent 以最适合其任务的方式产生输出。如果工作流定义者希望下游获得 JSON，则应在 `input_template` 中指定解析指令。

---

## 提议的架构

### 分层模型

```text
┌──────────────────────────────────────────────────┐
│                   WorkflowEngine                   │
│  (图调度器——不是 Agent，不属于委托树)                 │
│                                                    │
│  activate / resume_from_restart / cancel            │
│  submit_review_decision                            │
│                                                    │
│  读取 WorkflowDefinition + WorkflowRun              │
│  写入 NodeRun 状态变更                              │
│                                                    │
│  ┌────────────┐  ┌──────────────┐                  │
│  │ DAG 编译器  │  │ Fan-out 调度器│                  │
│  │ (拓扑排序,  │  │ (gather 独立  │                  │
│  │  环检测)    │  │  spawn_task) │                  │
│  └────────────┘  └──────────────┘                  │
│  ┌────────────┐  ┌──────────────┐                  │
│  │ Fan-in 监视器│  │ 验证引擎     │                  │
│  │ (mailbox    │  │ (同步/异步   │                  │
│  │  subscriber)│  │  验证检查)   │                  │
│  └────────────┘  └──────────────┘                  │
│  ┌────────────┐  ┌──────────────┐                  │
│  │ 审查协调器  │  │ 恢复管理器    │                  │
│  │ (按节点暂停) │  │ (重启后重新    │                  │
│  │             │  │  驱动调度器)  │                  │
│  └────────────┘  └──────────────┘                  │
└──────────┬──────────────┬──────────────────────────┘
           │              │
    ┌──────▼──────┐  ┌───▼───────────┐
    │ AgentControl │  │  MailboxStore  │
    │ spawn_task   │  │  claim/ack     │
    │ send_task_…  │  │  idempotency   │
    │ cancel_task  │  │                │
    └──────┬──────┘  └───┬───────────┘
           │              │
    ┌──────▼──────┐       │
    │  TaskStore   │       │
    │  (TaskRecord │       │
    │   agent_tasks│       │
    │   表)         │       │
    └─────────────┘       │
                          │
              ┌───────────▼──────────┐
              │  WorkflowRunStore     │
              │  (workflow_definitions │
              │   workflow_runs       │
              │   新表，相同 SQLite db)│
              └──────────────────────┘
```

### 持久化与状态模型

在当前 SQLite 数据库中添加两张新表（同一文件，同一连接模式——WAL、`check_same_thread=False`、`RLock`）：

```sql
-- 工作流定义：以 JSON 嵌入的节点和边文档
CREATE TABLE workflow_definitions (
    workflow_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    definition_json TEXT NOT NULL,  -- {nodes: [...], edges: [...], metadata: {...}}
    state TEXT NOT NULL DEFAULT 'draft',  -- draft|active|archived
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- 工作流运行：一次激活 = 一行
CREATE TABLE workflow_runs (
    run_id TEXT PRIMARY KEY,
    workflow_id TEXT NOT NULL REFERENCES workflow_definitions(workflow_id),
    state TEXT NOT NULL DEFAULT 'running',
      -- running|paused_for_review|cancelled|completed|failed|waiting_for_fan_in
    node_runs_json TEXT NOT NULL DEFAULT '{}',  -- {node_id: NodeRun, ...}
    started_at TEXT NOT NULL,
    settled_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
```

**为什么 `node_runs_json` 放在运行行中而不是单独的表中：** 与 proposed ADR 第 2 节和拒绝的替代方案 C 一致。第一版工作流 ≤50 个节点。JSON 列保持查询面最小。以 `run_id` 为键运行时不进行跨运行查询。如果出现类似于"查找节点 X 曾失败的所有运行"的需求，V2 可增加 `workflow_node_runs` 表。

**NodeRun JSON 模式：**

```json
{
  "node_id": "uuid",
  "task_id": "uuid | null",
  "state": "pending|running|waiting_for_human|completed|failed|verified|skipped",
  "attempt": 1,
  "input_snapshot": "聚合后的输入字符串",
  "current_run_count": 2,
  "output_snapshot": "latest result or error",
  "error": "nullable error message",
  "started_at": "ISO8601|null",
  "settled_at": "ISO8601|null"
}
```

**关键设计决策：`current_run_count`，不是任务历史。** NodeRun 不保留每次尝试的历史。`current_run_count` 跟踪现有 Task 上的第几个 `run_count` 对应于当前尝试。Fan-in 监视器通过 `(child_task_id, run_count)` 关联 mailbox 消息，这与 `MailboxStore.idempotency_key` 模式精确匹配。

### 执行 DAG

```
激活:
  workflow_id + input ──→ load definition ──→ topological sort
    ──→ validate (no cycles, all agent_names resolve)
    ──→ create WorkflowRun row (all NodeRuns = pending)
    ──→ evaluate_ready_nodes()

evaluate_ready_nodes():
  FOR each node WHERE state = pending:
    IF all inbound edges satisfied:
      IF inbound = 0 (source node):
        spawn_node(node, input=input_template)
      ELSE (fan-in):
        IF all upstream NodeRuns.state ∈ {verified, completed (no verifier)}:
          aggregate = apply_data_mapping(upstream_results)
          spawn_node(node, input=aggregate)
        ELIF any upstream is failed AND edge.condition = on_failed:
          spawn_node(node, input=error_context)
        ELSE:
          remain pending

spawn_node(node, input):
  task_id = await AgentControl.spawn_task(
    agent_name=node.agent_name,
    task=input,
    parent_task_id=None,     # 工作流拥有，不讲父级
    parent_thread_id=f"wf-{run_id}"  # mailbox 路由回引擎
  )
  node.state = running
  node.task_id = task_id
  save_run()

fan-in watcher (mailbox 订阅):
  mailbox.claim(recipient_thread_id=f"wf-{run_id}")
  FOR each claimed message:
    node_run = find_node_by(recipient_task_id)
    IF node_run.state = running AND message.child_run_count = node_run.current_run_count:
      node_run.state = message.settled_status  # completed/failed/cancelled
      node_run.output_snapshot = message.content
      node_run.settled_at = now()
      IF verification_configured AND state = completed:
        → verification_step(node_run)
      ELSE:
        → on_node_settled(node_run)

verification_step(node_run):
  result = await verifier(node_run.output_snapshot)
  IF result.passed:
    node_run.state = verified
  ELSE:
    node_run.state = failed
    node_run.error = result.reason
  → on_node_settled(node_run)

on_node_settled(node_run):
  save_run()
  evaluate_ready_nodes()   # 重新驱动调度器
  evaluate_workflow_state()

evaluate_workflow_state():
  IF any terminal node settled → completed/failed:
    run.state = terminal_aggregate_state
  ELIF any node = waiting_for_human:
    run.state = paused_for_review  # 仅在无活跃节点时
  ELIF all alive nodes settled AND unsatisfied fan-ins exist:
    run.state = waiting_for_fan_in
  ELSE:
    run.state = running
```

### 审查恢复流程

```
用户通过 Gateway Task Module 提交审查决定。
  → GatewayTaskModule.submit_review(review_id, decisions)
  → AgentControl.submit_review_decision(review_id, decisions)
  → 节点任务恢复（AgentControl._resume_run）
  → Task 继续执行 → settled → mailbox 发布 → fan-in watcher 拾取
  → WorkflowEngine 接收 settled → 转换节点状态 → evaluate_ready_nodes()

注意：WorkflowEngine 不直接接收审查决定。它通过 mailbox
获知节点已 settled（审查后恢复 + 正常执行完成）。审查是
Task 级别操作；引擎只看到结果。
```

### 重启恢复流程

```
WorkflowEngine.recover():
  FOR each run IN workflow_runs WHERE state IN non-terminal:
    FOR each node IN run.node_runs:
      IF node.task_id IS NOT NULL:
        latest = TaskStore.get_task(node.task_id)  # 始终拉取最新状态
        IF latest.state ∈ SETTLED AND node.state ∈ ACTIVE:
          node.state = latest.state                 # 调和
          node.output_snapshot = latest.result or latest.error
          node.settled_at = latest.updated_at
        ELIF latest.state = waiting_for_human AND node.state != waiting_for_human:
          node.state = waiting_for_human
    save_run()
    evaluate_ready_nodes()  # 重新驱动调度器
```

**为什么这有效：** 恢复不是检查点恢复。重启后没有 in-flight asyncio 任务在运行。但在我们调和 NodeRun 与 TaskStore → 找出哪些节点真正已完成 → 重新计算依赖关系后，调度逻辑会重新启动。如果 Task 已通过其 `run_count` 完成，则其 mailbox 消息被 mailbox 幂等去重，不会重复注入。

### 幂等与并发

1. **Mailbox 幂等性已内置。** `MailboxStore.publish` 使用 `INSERT OR IGNORE`，基于 `idempotency_key = "settled:{thread}:{child_task_id}:{run_count}"`。Fan-in watcher 从不会看到同一个 settled 事件两次。

2. **Fan-in watcher 并发行。** `asyncio.Lock` 保护 `evaluate_ready_nodes` 每个运行。在 mailbox 消息到达、任务完成和恢复步骤之间，重新评估在锁下运行。如果两个上游节点同时 settled，watcher 串行处理它们——第二个重新评估从刚更新的状态读取。

3. **Spawn 竞态条件。** 两个节点同时变为就绪 → `spawn_task` 调用由 `AgentControl._root_budget_locks` 内部的 `asyncio.Lock` 保护（如果它们碰巧共享同一个 `root_task_id`）。工作流拥有的任务使用 `parent_task_id=None`——因此它们不竞争委托预算。这是 proposed ADR 将 DAG 边与委托树分离的另一个好处。

4. **Fan-in 重复计算。** Fan-in 节点在一次评估周期内只计算一次（在锁下）。`evaluate_ready_nodes` 在调用前检查所有入站依赖是否已满足——因此部分 fan-in 永远不会生成节点。

### 接口

```python
class WorkflowEngine:
    """基础设施图调度器——不是 Agent，不是 Gateway Task。"""

    def __init__(
        self,
        *,
        agent_control: AgentControl,
        mailbox_store: MailboxStore,
        db_path: str,  # 用于 WorkflowRunStore 的 SQLite 路径
    ) -> None: ...

    # --- 生命周期 ---

    async def activate(
        self,
        workflow_id: str,
        input: str,
    ) -> WorkflowRun: ...
    """创建新运行。加载定义，验证 DAG，启动源节点。"""

    async def resume_from_restart(self) -> None: ...
    """重新驱动所有非终端运行。在进程启动时调用。"""

    async def cancel(self, run_id: str) -> WorkflowRun: ...
    """取消运行。将 cancel_task 级联到所有活跃节点。"""

    # --- 审查 ---

    async def submit_review_decision(
        self,
        run_id: str,
        node_id: str,
        decisions: list[dict[str, Any]],
        *,
        wait: bool = False,
    ) -> NodeRun: ...
    """通过 AgentControl.submit_review_decision 路由审查。
       工作流引擎无需了解审查——它委托给 Task 生命周期。"""

    # --- 查询 ---

    def get_run(self, run_id: str) -> WorkflowRun: ...
    def list_runs(self, workflow_id: str) -> list[WorkflowRun]: ...
```

**网关公开：** `GatewayTaskModule` 获得可选的传递方法：

```python
class GatewayTaskModule:
    async def create_workflow(self, workflow_id: str, input: str) -> WorkflowRun: ...
    async def cancel_workflow(self, run_id: str) -> WorkflowRun: ...
    def get_workflow_run(self, run_id: str) -> WorkflowRun: ...
```

`submit_review_decision` 的现有路径保持不变（Task 级）。Workflow 上下文通过 `run_id` + `node_id` 添加一个新的审查路径——但最终它调用相同的 `AgentControl.submit_review_decision`，引擎在恢复后接收 Task 完成的 mailbox 通知。

### WorkflowDefinition 创建

第一版无 GUI 工作流构建器。定义通过以下任一方式创建：
1. **JSON 文件** → `WorkflowEngine.register_definition(json_path)`
2. **程序中** → `WorkflowEngine.register_definition(WorkflowDefinition(nodes=[...], edges=[...]))`

主持 Agent 在其输出中生成工作流定义 JSON，该 JSON 由人工或频道逻辑保存/注册。引擎在定义通过 `register_definition` 后仅加载一次并验证。

---

## 提议 ADR 中未解决的具体问题

现给予明确答案：

| 问题 | 答案 |
|---|---|
| 主持 Agent：普通 Task 还是基础设施调度器？ | **普通 Task（作为发起者）+ 自身作为 DAG 节点**。不赋予特殊的调度权限。调度由 WorkflowEngine 完成。 |
| 主持 Agent 如何接收并收敛分歧？ | Fan-in 节点接收由引擎通过 `send_task_input` 聚合的上游输出（使用 `input_template`），加上收敛指令。主持 Agent（在该节点的 Task 会话中）读取、比较、裁决，产生输出。 |
| Workflow pause：全局还是仅阻断依赖分支？ | **仅阻断依赖分支**。`paused_for_review` 是合取运行状态，不是停止全局的信号。仅当暂停节点是 fan-in 依赖时才延迟其后继节点。 |
| 重试是否复用同一 Task Session？ | **默认是（最多 3 次）。** `retry_same_session`：向同一 `task_id` 发送 `send_task_input`。`retry_new_session` 推迟到 V2。`no_retry` 将节点映射到 `failed` → `on_failed` 边。 |
| Node run 应为 JSON 单行还是规范化表？ | **引擎中为纯字符串**。Fan-in 聚合通过 `"{node_id.result}"` 模板变量。数据格式由节点 Agent 决定，不由引擎规定。 |

---

## 被拒绝的替代方案

（这些不重复 proposed ADR 中已拒绝的 A-E 选项。）

### F. 将 WorkflowEngine 构建为 AgentControl 上的包装器

**已拒绝。** 类似中层管理层——它包装 `AgentControl` 但为对等类。这破坏了 proposed ADR 的正确判断，即 WorkflowEngine 位于 `AgentControl` 旁边（两者都位于 `AgentControl._task_manager` 级别以下）。包装器层级说"所有事情都通过我"，但实际上 WorkflowEngine 仅管理图状态转换。它不需要包装 `AgentControl` 的每个方法。

### G. 将 NodeRun 建模为它们自己的 TaskRecord

**已拒绝。** 这会膨胀 `agent_tasks` 表，并在概念上将工作流节点运行（短暂、每个 DAG）与 Gateway Task（长期、独立会话）混为一谈。NodeRun 有根本不同的生命周期——它仅存在于其工作流运行中。将其表示为 TaskRecord 会创建许多"一次性"任务，污染查询并混淆 Task Manager 的关注点。

### H. Fan-in 通过阻塞 `wait_agent` 调用而非 mailbox

**已拒绝。** 如果 WorkflowEngine 对每个上游节点调用 `wait_agent`，则它是串行轮询而不是事件驱动。Mailbox 被设计为用于此目的：订阅者接收通知。阻塞等待将引入不必要的轮询延迟，并使实现依赖于引擎作为调用者保持活跃。

---

## 风险

| 风险 | 可能性 | 影响 | 缓解措施 |
|---|---|---|---|
| Fan-in watcher 在 Task settled 与 mailbox 发布之间遗漏 mailbox 消息（时序问题） | 低 | 高 | Mailbox 在 `_run_agent_payload` 内的 checkpoint 后发布。由于 mailbox 持久化，即使进程在该窗口内崩溃，恢复也能补救。另外，恢复会主动轮询 TaskStore（不仅仅是 mailbox 回放）。 |
| JSON 列 `node_runs_json` 在 >50 个节点时增长过大 | 中 | 中 | V1 上限 50 个节点。V2 在里程碑处标准化为 `workflow_node_runs`。JSON blob 每节点 ~200 字节 → 50 节点 = 10 KB，完全在 SQLite 的舒适范围内。 |
| `parent_task_id=None` 工作流任务与委托树工具范围规则交互不良 | 低 | 高 | `get_scoped_task` 在 AgentControl 中基于 `parent_task_id` 关系检查可见性。工作流任务位于委托树外部。当 Agent 工具（如 `list_agents`）运行时，它们不会看到这些工作流拥有的任务——这是正确的；Agent 不应手动管理工作流调度。但如果工作流节点自身调用 `spawn_agent`，则这些子任务将在树内并可见。这需要明确的规则：**如果工作流节点的 agent 需要委托，它通过现有工具执行，引擎不干预。** |
| `spawn_task` 的 `parent_task_id=None` 绕过预算限制 | 低 | 中 | 第一版中，工作流节点上限为 50。`AgentControl.spawn_task` 基准预算由 `root_task_id` 执行，当 `parent_task_id=None` 时，它就是 task_id。因此每个工作流任务都有自己的预算根。50 个节点 × 默认 20 个任务 = 1000 个可能的总任务，这对调试来说已是上限。可以接受。 |
| 并行节点同时 spawn_task 时 `asyncio.gather` 并发控制不足 | 低 | 低 | `AgentControl._root_budget_locks` 已提供每根串行化。引擎可以另外在 `evaluate_ready_nodes` 中限制 fan-out 为 3-5 个并发 spawn。 |

---

## 验证标准

1. **钻石 DAG 正确性。** `A → B, A → C, B → D, C → D`。验证 D 仅在 B 和 C 都完成（`completed` 或 `verified`）后才接收输入。在此条件之前，D 保持 `pending`。

2. **并行 fan-out。** 两个源节点并发启动。它们的 `created_at` 时间戳间隔 < 100ms。两者都作为独立 Gateway Task 在独立线程中运行。

3. **Fan-in 聚合。** 三个上游节点各自产生输出。Fan-in 节点接收一个单一输入字符串，该字符串包含所有三个输出并通过 `input_template` 格式化。

4. **审查暂停不阻塞无关分支。** `A → C, B → D`（独立分支）。B 进入 `waiting_for_human`。A 完成 → C 正常启动。B 的分支（D）被阻塞。运行状态：`paused_for_review` 如果 B 是唯一的活跃挂起节点；否则 `running`。

5. **重启恢复。** 进程在节点 B 运行期间终止。重启后 `recover()` 扫描运行。B 的 NodeRun 状态与 TaskStore 调和 → `completed`。Fan-in 依赖 B 的节点 C 解锁并启动。

6. **重试。** 节点因瞬态错误失败（`state=failed`）。引擎尝试 `retry_same_session` → `send_task_input` 到同一个 `task_id` → `attempt` 递增。如果成功，节点最终到达 `completed`。如果 3 次尝试后仍失败，节点停留在 `failed`，评估 `on_failed` 边。

7. **取消级联。** `cancel(run_id)` → 每个有活跃 `task_id` 的节点 → `AgentControl.cancel_task`。运行转换到 `cancelled`。Task 会话保留（cancel_task 仅取消当前 run）。

8. **幂等：fan-in watcher 从不触发重复节点。** 在 mailbox 消息重传下的两次连续 `evaluate_ready_nodes` 调用 → 节点生成一次，第二次调用是空操作。

---

## 分阶段计划

### 阶段 0：基础（1-2 天）
- `WorkflowDefinition`、`WorkflowNode`、`WorkflowEdge`、`NodeRun`、`WorkflowRun` 数据类。
- `WorkflowRunStore`：SQLite 的 CRUD，遵循 `TaskStore` 模式。
- 带有环检测的 DAG 拓扑排序工具。

### 阶段 1：核心图调度器（3-5 天）
- `WorkflowEngine.activate`：加载定义 → 创建运行 → 生成源节点。
- `evaluate_ready_nodes`：fan-out 和 fan-in 的图评估。
- 通过 AgentControl 的 `spawn_task` / `send_task_input` 进行节点生成。
- `cancel` 实现。

### 阶段 2：Fan-in Watcher 与 Mailbox 集成（2-3 天）
- 每个运行作为订阅者的 Mailbox watcher（`recipient_thread_id = f"wf-{run_id}"`）。
- NodeRun 通过 mailbox 消息进行状态转换。
- `evaluate_ready_nodes` 中的 fan-in 逻辑。

### 阶段 3：审查与 HITL（2-3 天）
- NodeRun 进入 `waiting_for_human` → 运行状态更新 → 依赖分支暂停。
- 审查决定路由（`GatewayTaskModule` → `AgentControl` → mailbox 通知 → 引擎恢复）。
- 按分支暂停（非全局）。

### 阶段 4：重试与错误处理（1-2 天）
- `retry_same_session` 并带有 attempt 跟踪。
- `on_failed` 边评估。
- 超时处理（节点级超时 → 强制失败）。

### 阶段 5：重启恢复（1-2 天）
- `resume_from_restart`：扫描非终端运行 → 与 TaskStore 调和 → 重新驱动。
- 恢复后保持幂等性（不重复生成节点）。

### 阶段 6：验证集成（1 天）
- `VerificationSpec` 执行：同步调用或轻量级 Agent Task。
- `verified` 状态转换。

### 阶段 7：Gateway 集成与 Web UI（2-3 天）
- `GatewayTaskModule` 的传递方法。
- Web UI 的 WorkflowRun / NodeRun 视图。
- 在工作流 UI 中直接审查（打开子 Task Session）。

### 阶段 8：测试与文档（2-3 天）
- 钻石 DAG 的集成测试。
- 审查暂停/恢复测试。
- 崩溃恢复测试。
- 并行 fan-out 正确性测试。

---

## 总结

核心见解已明确：**WorkflowEngine 是 AgentControl 旁边的图调度器，不更改任何现有基元。DAG 边与委托树正交。Fan-in 通过复用 `parent_thread_id` 路由的 mailbox 订阅处理。暂停是每个依赖分支的，不是全局的。重试默认复用同一个 Task 会话。引擎是格式无关的——节点数据是需要时的纯字符串。主持 Agent 是 DAG 中的普通节点，不赋予特殊调度权限。**

最大的未解决风险（需由人工决定）：**工作流节点 agent 是否应被允许使用 `spawn_agent` 自行委托？** 如果是，这会在工作流内创建委托树，具有单独的预算和可见性。这增加了复杂性，因为工作流监视器需要理解"此节点尚未 settled，因为其子委托尚未完成"——但目前，`AgentControl.wait_agent` 已经语义上处理了这一点。我认为默认允许是正确的，但需在测试中明确覆盖。

第二个决定：**node run 输出中应存储多少历史？** 当前设计仅存储最新的 `output_snapshot`。如果 fan-in 节点需要查看之前尝试的输出，这些信息已存在于 Task 会话历史中（通过 `send_input` 的后续轮次）。引擎不存储输出历史；Task 存储。如果需要，V2 可添加 `output_history: list[str]` 到 NodeRun。
