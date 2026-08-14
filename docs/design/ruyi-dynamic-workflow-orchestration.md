Status: Proposed

# Ruyi 动态工作流编排层设计

## 1. 范围

本设计为 Ruyi 增加持久、动态、可恢复的工作流编排层，支持：

- 多个 Agent 节点并行分析与 fan-out；
- 串行依赖、条件边、fan-in 与确定性 join；
- 主持 Agent 汇总、识别分歧、定向交叉质询并收敛结论；
- 分支级人工审批与人工裁决；
- 节点失败分类、退避、重试和失败分支；
- 运行中对尚未发生的未来图进行受约束修改；
- 进程重启后的状态调和与继续推进；
- 工作流取消及活跃节点 run 的取消传播；
- Local Agent 与 Remote Ref 的一致 Task 控制语义；
- Workflow、Node、Attempt、Gateway Task/run、Review 与 Artifact 的可审计关联。

## 2. 假设与约束

- Gateway Task 仍是 Agent 的唯一独立长 Session：`task_id`、`thread_id` 跨多次 run 稳定；一次 run settled 不关闭 Task。
- `AgentControl` 仍是 Gateway Task 的唯一运行时，负责 Task 状态机、checkpoint、Mailbox 注入、委托树与 Permission Review。
- Gateway Task Module 仍是 Local/Remote 路由、Task 创建、继续、查询、审批和取消的 transport-neutral 边界。
- Task Mailbox 仍只表示发往某个 Gateway Task 的持久输入箱；它不定义工作流依赖、ready queue 或协作策略。
- 初始环境可为单调度进程和 SQLite/WAL，但持久状态使用租约、版本或条件更新防止意外双调度者破坏不变量。
- 节点间主数据为文本和 Artifact 引用；大对象不嵌入 Workflow 可变状态。
- 工作流拥有独立资源预算；节点内部委托仍受现有 delegation root/depth/task budget 约束。

## 3. 显式非目标

- 不替换 Gateway Task、AgentControl、Task Mailbox 或 LangGraph checkpoint。
- 不把 WorkflowRun、NodeInstance 或 NodeAttempt 伪装成 Gateway Task。
- 不把 workflow edge 编码为 `parent_task_id`，也不把 delegation 子树自动吸收到 Workflow DAG。
- 不由调度器直接调用模型、解释任意自然语言或执行 Agent 工具。
- 不把 Task Mailbox 改造成通用事件总线或工作流状态日志。
- 不承诺 Agent 内部非幂等外部副作用的 exactly-once。
- 不支持有环图、任意代码条件表达式或对已发生历史的重写。
- 不定义 UI、实施阶段、施工顺序、任务分配、测试计划、验收标准或发布流程。

## 4. 架构总览

```text
Channel / HTTP / Agent workflow-control tools
                       |
                       v
              Workflow Control Module
                       |
          +------------+-------------+
          |                          |
          v                          v
 Workflow Scheduler/Reconciler   Workflow Store
          |
          v
 Gateway Orchestration Task Port
          |
          v
 Gateway Task Module -> Task Router -> AgentControl
          |                              |
          |                              +-> Gateway Task + checkpoint
          |                              +-> Task Mailbox
          |                              +-> Permission Review
          +-> Local / Remote Ref routing
```

`WorkflowRun` 是一次不可重新打开的图执行；Gateway Task 是可跨多次 run 继续的 Agent Session。两者生命周期不同，通过 NodeAttempt 关联，不共享身份。

## 5. 架构决策

### AD-01：Workflow Scheduler 是基础设施，WorkflowRun 不是 Gateway Task

Workflow Scheduler/Reconciler 是确定性图状态机，不是 Agent，也不是 Gateway Task。WorkflowRun 使用独立持久身份和不可逆终态。

Gateway Task 的 settled 状态仅描述当前或最近一次 run，`send_input` 可唤醒同一 Session；WorkflowRun 的 `completed`、`failed`、`cancelled` 必须不可原地重新打开。Mailbox 输入在安全模型边界注入，而调度事件应由非模型状态机消费。将调度器放入特殊 Gateway Task 会要求特殊 executor、特殊终态或特殊 Mailbox consumer，破坏现有 Task 不变量，并造成恢复自举。

主持 Agent 和所有执行型节点仍是普通 Gateway Task。

### AD-02：主持 Agent 负责语义收敛，Scheduler 负责机械正确性

主持 Agent 可以比较证据、识别共识与分歧、请求指定分析者只回答某个 `decision_id`、建议补充节点、建议重试、请求人工裁决并生成统一结论。

Scheduler 只负责：

- ready、join、edge condition 和 revision 的确定性求值；
- 结构化主持输出的 schema 与引用合法性校验；
- 权限、预算、并发、幂等和状态转换；
- 执行已接受的 graph patch 或控制命令。

`ACCEPT / DISAGREE / NEEDS_HUMAN` 等格式属于具体主持节点的可选契约，不是编排引擎的全局输出格式。上游自然语言永远不能直接修改图或持久状态。

### AD-03：Workflow DAG 与 delegation tree 永久正交

Workflow edge 表示数据流、控制依赖和 join；`parent_task_id` 表示 Agent 主动委托形成的严格树，用于 depth、root budget、scoped tools 和 delegated-run-settled 回投。

工作流节点 Task 默认不因 DAG 获得 `parent_task_id`。节点内部调用 `spawn_agent` 形成正常 delegation 子树；Scheduler 只观察该工作流节点 Task，不自动将其子 Task 变成 WorkflowNode。

### AD-04：人工审批是分支 Gate，不是全局暂停

Task 权限审批继续使用现有 `ReviewSnapshot / ReviewDecision`：Task 进入 `waiting_for_human`，决定恢复同一 `task_id/thread_id`。

业务裁决或高风险改图也通过一个实际 Gateway Task 的受保护操作触发现有 Review 协议。Workflow 保存 `gate_id + task_id + review_id + decision_id/patch_id` 的关联，不创建第二套审批协议。

等待审批只阻塞依赖该 Gate 的路径。无依赖关系的分支继续运行。WorkflowRun 的 `quiescent_waiting_human` 仅表示当前没有可运行工作且至少存在一个未决人工 Gate，不是全局执行锁。

### AD-05：动态图采用不可变 revision 与 `base_revision` CAS

每个 WorkflowRun 从定义快照产生 revision 1。每个已接受 patch 产生新的不可变 revision，并声明 `base_revision`。只有 `base_revision == current_revision` 才可提交；冲突不得静默覆盖或自动投票合并。

允许：

- 增加节点和边；
- 禁用未开始节点或仅影响未来的 edge；
- 修改未开始节点的 retry、timeout、verification、join 或 Agent policy；
- 为未开始节点增加 required predecessor；
- 增加主持轮次、交叉质询、验证或人工 Gate。

禁止：

- 修改已产生 attempt 的节点输入或执行身份；
- 改写 running/settled attempt 的结果和关联；
- 为已开始节点新增 required predecessor；
- 删除已用于生成输入快照的历史因果；
- 复用 node/edge id 表示不同含义；
- 产生环；
- 让终态 WorkflowRun 重新 active。

核心原则是：历史不可变，未来可版本化修改。

### AD-06：动态运行态规范化；不可变图快照可为 JSON 文档

动态可变状态使用独立的 WorkflowRun、GraphRevision、NodeInstance、NodeAttempt、WorkflowCommand 和 PatchProposal 记录。Definition/GraphRevision 的 canonical snapshot 可以是 JSON 文档。

不使用单行 `node_runs_json` 作为权威可变状态，因为动态改图、并行节点 CAS、attempt 结果固化、命令幂等和恢复扫描需要不同聚合边界及唯一约束。即使节点数小，这些仍是数据一致性问题而非容量问题。

### AD-07：节点重试默认复用同一 Gateway Task Session

默认 `same_session` 重试通过 Gateway Task Module 的 routed `send_input` 向同一 `task_id` 发送新输入，形成新的 `run_count`，保留上下文连续性。

仅当会话污染、Agent 切换、安全隔离或副作用不确定策略明确要求时使用 `new_session`；旧 Task 不删除，旧 Attempt 仍保留审计关联。

每次重试都是新的 NodeAttempt。NodeAttempt 与 Gateway run 的强关联是 `(task_id, run_count)`，而非只有 `task_id`。

### AD-08：持久 WorkflowCommand 与端到端 command-id 幂等

所有外部副作用意图在调用 Gateway 前先成为持久 WorkflowCommand：

- `ensure_task`
- `send_input`
- `cancel_run`
- `submit_review`
- `apply_patch`

同一 `command_id` 只能对应同一 payload hash；相同 id、不同内容必须拒绝。

Gateway 编排端口的必要语义是：

- `ensure_task(command_id, ...)` 重复调用返回同一 `task_id`；
- `send_input(command_id, ...)` 将该 id 贯穿到 Mailbox `idempotency_key`，不得产生第二条输入或第二次唤醒；
- cancel 和 review submission 同样按 command id 幂等。

Agent 模型执行和其外部工具副作用不承诺 exactly-once。若 Local 或 Remote Gateway 不支持某项幂等能力，WorkflowRun 必须显式记录降级保证和 `uncertain_side_effect`，不得声称 exactly-once node dispatch，也不得依靠模型回显、agent_name 猜测或孤儿扫描作为正确性机制。

### AD-09：Mailbox 只用于具体 Task 输入；Task 状态查询是恢复事实来源

Scheduler 不直接写 MailboxStore，也不创建虚构 workflow recipient/thread 来消费节点事件。向节点重试、向主持 Agent 发送 fan-in 聚合、向现有 Task 补充信息均通过 Gateway Task Module 的 routed `send_input`。

运行时通知、Remote webhook 或进程内 lifecycle hint 只用于唤醒 Reconciler。恢复与最终调和必须通过 Gateway Task 查询端口读取持久 Task/Route 状态；Mailbox 是否存在 settled 消息不决定 Task 是否完成。

### AD-10：工作流运行期间对节点 Task 建立编排租约

一个被绑定到活跃 NodeAttempt 的 Gateway Task 仍是独立、可查询的长 Session，但改变其 run 序列的外部输入必须遵守 workflow orchestration lease：

- Scheduler 的 command-id 输入允许；
- 对 Task 的普通查询和审批允许；
- 外部直接 `send_input` 默认拒绝并返回该 Task 当前受哪个 WorkflowRun/NodeAttempt 管理；
- 具备 operator override capability 的输入可以进入，但必须形成新的 WorkflowCommand/Attempt，或显式将当前 Attempt 标记为 `superseded/uncertain`。

租约随 NodeAttempt settled 或 WorkflowRun 终态释放。此约束防止 Task 的最新 `result/run_count` 在 Scheduler 固化 Attempt 快照前被无关输入覆盖。

## 6. 组件边界与职责

### 6.1 Workflow Control Module

负责身份认证、owner/tenant 检查、Definition 与 Run 控制接口、主持 Agent 的结构化控制请求、人工 Gate 入口及领域错误。它不计算 ready set，不直接执行 Task，不读写 Mailbox。

### 6.2 Workflow Compiler

负责 schema 校验、Agent target 解析、环检测、拓扑索引、join/edge condition 编译、模板/data mapping 校验及 patch 影响范围判定。输出不可变 CompiledGraphRevision。

### 6.3 Workflow Scheduler

负责 ready set、fan-out、fan-in、预算、并发限制、Node/Attempt 状态转换、重试、失败边、取消及 WorkflowRun 摘要状态。它不解释主持自然语言，不修改 TaskRecord 或 checkpoint。

### 6.4 Workflow Reconciler

消费 lifecycle hint、Remote webhook hint、计时器和启动扫描。它以 Workflow Store 与 Gateway Task 查询结果进行双向调和：Gateway 是 Task 生命周期事实来源；Workflow Store 是“该 Task/run 对哪个 NodeAttempt 意味着什么”的权威来源。

### 6.5 Gateway Orchestration Task Port

Scheduler 使用 Gateway Task Module 提供的稳定应用端口，而非直接使用 TaskStore、GatewayRouteStore、MailboxStore 或 LangGraph checkpointer。端口负责 Local/Remote 路由、附件处理、状态 guard、route 持久化、review、cancel、错误语义和 command-id 幂等。

语义接口：

```text
ensure_task(EnsureTaskCommand) -> TaskView
send_input(SendInputCommand) -> TaskView
get_task(task_id, refresh=true) -> TaskView
cancel_current_run(CancelRunCommand) -> TaskView
get_pending_review(task_id) -> ReviewSnapshot?
submit_review(ReviewCommand) -> TaskView
get_run_result(task_id, run_count) -> RunResult?
```

### 6.6 Review Coordinator

将 Task permission review 与 workflow decision gate 映射到同一现有 Review 协议，验证审批人身份，维护 `gate/review/task` 关联，并保证审批只解锁依赖路径。

### 6.7 Workflow Store

保存不可变图 revision、可变运行投影、attempt 快照、命令 outbox、patch proposal、租约和审计事件。WorkflowEvent 是审计记录，不是唯一调度事实来源。

## 7. 控制接口

以下仅定义领域语义，不限定传输形式。

```text
register_definition(definition, request_id, principal)
  -> WorkflowDefinitionRevision

activate_workflow(definition_id, definition_revision, initial_input,
                  moderator_task_id?, request_id, principal)
  -> WorkflowRun

get_workflow_run(workflow_run_id, include_nodes, include_attempts, principal)
  -> WorkflowRunView

cancel_workflow(workflow_run_id, reason, request_id, principal)
  -> WorkflowRun

propose_graph_patch(workflow_run_id, base_revision, patch_id,
                    operations, reason, proposer_task_id, request_id)
  -> PatchProposal

approve_graph_patch(workflow_run_id, patch_id, review_id,
                    decision, request_id, principal)
  -> PatchProposal

retry_node(workflow_run_id, node_instance_id, retry_mode,
           reason, expected_attempt_no, request_id)
  -> NodeAttempt

submit_node_review(workflow_run_id, node_instance_id,
                   task_id, review_id, decisions, request_id)
  -> ReviewOutcome
```

上述改变持久状态的调用均要求 owner scope 内幂等。

## 8. 数据与状态模型

### 8.1 WorkflowDefinitionRevision

```text
definition_id
revision
owner_scope
schema_version
canonical_definition_snapshot
content_hash
created_by
created_at
```

Definition revision 不可变。

### 8.2 WorkflowRun

```text
workflow_run_id
definition_id
base_definition_revision
current_graph_revision
owner_scope
moderator_task_id?
lifecycle_state
initial_input_snapshot
scheduler_lease_owner?
scheduler_lease_epoch?
version
created_at
updated_at
settled_at?
failure_code?
guarantee_profile
```

状态：

```text
active
quiescent_waiting_human
quiescent_retry_wait
cancelling
completed
failed
cancelled
```

终态不可逆。摘要状态不作为全局锁。

### 8.3 WorkflowGraphRevision

```text
workflow_run_id
revision
base_revision?
patch_id?
canonical_graph_snapshot_or_delta
content_hash
created_by_task_id?
approved_review_id?
created_at
```

每个 revision 自身有效且无环。

### 8.4 WorkflowNodeDefinition

```text
workflow_run_id
node_id
introduced_revision
disabled_revision?
kind: agent | moderator | verifier | human_gate | transform
agent_name?
input_template
join_policy
retry_policy
timeout_policy
verification_policy
permission_profile?
resource_class?
```

### 8.5 WorkflowEdge

```text
workflow_run_id
edge_id
from_node_id
to_node_id
introduced_revision
disabled_revision?
condition: on_success | on_failure | on_any_settled | on_approved | expression
required
data_mapping
priority
```

Expression 使用受限、确定性的 DSL，不执行任意代码。

### 8.6 NodeInstance

```text
node_instance_id
workflow_run_id
node_id
introduced_revision
state
current_attempt_no
active_attempt_id?
ready_revision?
version
created_at
updated_at
settled_at?
```

状态：

```text
pending
blocked
ready
dispatching
running
waiting_for_human
verifying
retry_wait
succeeded
failed
skipped
cancelling
cancelled
```

`blocked` 表示仍可能因上游 retry、review 或 patch 解除；当所有相关前驱永久终结且 join 不可能满足时转为 `skipped`，不得无限等待。

### 8.7 NodeAttempt

```text
attempt_id
node_instance_id
attempt_no
retry_mode
task_id?
task_run_count?
dispatch_command_id
input_snapshot
input_hash
evaluation_revision
source_attempt_ids[]
state
result_snapshot?
error_snapshot?
failure_class?
started_at?
settled_at?
observed_task_updated_at?
```

状态：

```text
planned
dispatching
running
waiting_for_human
verifying
succeeded
failed
interrupted
timed_out
cancelled
superseded
```

一旦 Attempt 进入 `dispatching`，输入快照、evaluation revision 和 source attempts 不可变。

### 8.8 WorkflowCommand

```text
command_id
workflow_run_id
node_instance_id?
attempt_id?
kind: ensure_task | send_input | cancel_run | submit_review | apply_patch
payload_hash
state: pending | executing | acknowledged | failed_retryable | failed_permanent
attempt_count
next_attempt_at?
external_reference?
last_error?
created_at
updated_at
```

### 8.9 PatchProposal

```text
patch_id
workflow_run_id
base_revision
proposer_task_id
operations
reason
risk_class
state: proposed | waiting_for_human | accepted | rejected | conflicted | invalid
review_id?
created_at
resolved_at?
```

### 8.10 WorkflowEvent

```text
event_id
workflow_run_id
aggregate_type
aggregate_id
event_type
causation_id
correlation_id
principal
payload
created_at
```

仅用于审计和观测，不替代当前状态及调和。

## 9. 调度与收敛语义

### 9.1 Ready 与 join

节点在以下条件同时满足时才可 ready：

1. 节点在指定 evaluation revision 有效；
2. 没有活跃或成功 Attempt；
3. required inbound edges 已达到可判定状态；
4. join policy 已满足；
5. 没有未解决的人工 Gate；
6. 预算、并行度和权限允许；
7. 输入能从已固化的 source Attempt 快照确定性生成。

支持的 join：`all_required`、`any`、`quorum(n)`、`all_settled` 以及受限 expression。

### 9.2 Fan-out

同一次 ready 求值可使多个 NodeInstance 独立进入 `ready/dispatching`，每个节点拥有独立 Attempt 和 WorkflowCommand。一个节点的 dispatch 失败不回滚其他已成功分支。

### 9.3 Fan-in

Fan-in 输入从 NodeAttempt 的固化 `result/error snapshot` 生成，而不是临时读取 Gateway Task 的最新 result。输入记录 evaluation revision、source attempt ids、rendered content 和 hash。随后 Task 的其他 run 不得改变该输入的因果依据。

### 9.4 主持收敛

典型图形：

```text
Analyst A --+
Analyst B --+--> Moderator R1
Analyst C --+        |
                      +--> consensus --> Finalizer
                      +--> disagreement --> scoped CrossExam nodes --> Moderator R2
                      +--> needs human --> Human Gate --> Moderator R2
```

主持输入明确区分系统控制 metadata、上游不可信文本、Artifact 引用和主持指令。主持输出可声明：

```text
agreements[]
disagreements[{decision_id, positions, evidence, resolution}]
proposed_patch?
human_decisions?
final_synthesis?
```

Scheduler 只校验 schema、引用、revision、权限与预算，不判断技术立场本身是否正确。

## 10. 失败、重试与恢复语义

### 10.1 失败分类

```text
transient_transport
remote_unavailable
rate_limited
task_interrupted
timeout
verification_failed
agent_failed
invalid_output
permission_denied
configuration_error
cancelled
uncertain_side_effect
```

RetryPolicy 包含 `max_attempts`、`same_session|new_session`、可重试分类、fixed/exponential backoff、上限、jitter 和 retry prompt template。

人工明确拒绝、永久权限拒绝、Agent target 不存在、非法图和副作用状态未知不得默认盲重试。

### 10.2 Task settled 但通知丢失

Reconciler 查询 Task 后以 `(task_id, run_count)` 匹配 Attempt，固化 result/error 并推进 verification 或下游。Mailbox 消息缺失不影响正确性。

### 10.3 Scheduler 崩溃

非终态 WorkflowRun 通过持久 lease 和 version 重新取得调度所有权。Reconciler 加载当前 revision、NodeInstance、Attempt 和未确认 Command，重放相同 command id，查询所有已绑定 Task，调和状态并重新计算 ready/blocked/skipped。恢复是 reconcile + re-drive，不重新解释旧自然语言，也不重建已固化输入。

### 10.4 本地活跃 run 重启后 interrupted

若 Gateway 将活跃本地 run 规范化为 `interrupted`，对应 Attempt 不得假装继续 running。Scheduler 按 RetryPolicy 创建新 Attempt，通常复用同一 Task Session；不可重试则进入失败边。

### 10.5 Remote Ref 状态不确定

远程查询失败不等同任务失败。Attempt 保持 `unknown_remote` 投影；达到确认预算后进入人工裁决、new-session retry 或 `failed_with_uncertain_side_effect`。Remote capability profile 必须显式说明 idempotent create/send、run history 和 review resume 支持程度。

### 10.6 Verification 失败

Gateway Task run 可以真实地 `completed`，而工作流 Attempt 进入 `verifying` 后失败。Verification 失败属于 Workflow 语义，不得回写 TaskRecord 为 failed；它按 RetryPolicy 或 failure edge 处理。

### 10.7 审批期间重启

通过 `(task_id, review_id)` 恢复 Gate 关联；不重复创建 Review。依赖路径继续等待，无关分支继续。

### 10.8 取消

WorkflowRun 先进入 `cancelling`，不再创建新 Attempt；对活跃 Attempt 产生幂等 cancel command，未开始节点转为 cancelled，聚合完成后 WorkflowRun 为 `cancelled`。

Task cancel 只取消当前 run，不删除或关闭长期 Session。以后对该 Task 的独立继续不会自动重新加入已取消 WorkflowRun。

### 10.9 部分失败

有满足的 `on_failure` edge 时，失败是图中可处理结果。无关分支继续。只有所有可达成功路径关闭且不存在 retry/review 可能时，WorkflowRun 才为 failed；除非定义明确选择 fail-fast。

## 11. 不变量

1. Gateway `task_id` 始终标识长期独立 Session，不标识 WorkflowRun 或单次 Attempt。
2. WorkflowRun 终态不可逆；继续讨论应继续相关 Gateway Task 或派生新 WorkflowRun。
3. 一个 NodeAttempt 最多绑定一个 `(task_id, run_count)`。
4. 同一 NodeInstance 同时最多一个 active Attempt，除非显式启用 speculative policy。
5. Workflow edge 永不写入 `parent_task_id`；delegation 子树不被隐式解释为 DAG。
6. 每个 GraphRevision 不可变且自身无环。
7. 每次 ready 判定、输入快照和 Attempt 都绑定明确 revision。
8. 已 dispatch 的 Attempt 输入不可变；settled result/error snapshot 不可被 Task 后续 run 覆盖。
9. 相同 command id 只能对应相同 payload；相同 patch id 只能应用一次。
10. Scheduler 不以模型回显 metadata、agent_name 猜测或 Mailbox 消息存在性建立强 correlation。
11. Scheduler 不直接写 TaskStore、GatewayRouteStore、MailboxStore 或 checkpoint。
12. Workflow cancel 不关闭 Gateway Task Session。
13. Review 决定必须绑定确定的 `task_id + review_id` 和受影响 Gate/patch/decision。
14. 新 revision 不得使已开始节点新增未满足 required dependency。
15. Workflow owner 不自动获得节点 Task 或 Artifact 的越权访问。
16. Scheduler 重启次数不改变 command identity、Attempt 因果或图历史。
17. 工作流租约期间的外部 Task 输入不得静默改变 active Attempt 的 run correlation。

## 12. 安全考虑

### 12.1 身份与授权

Definition、WorkflowRun、Task、Review 和 Artifact 统一携带 owner scope。能力至少区分 creator、operator、moderator、reviewer、observer 和 node-agent capability。拥有 WorkflowRun 查询权不等于拥有节点完整会话历史读取权。

主持 Agent 的改图、增加预算、选择 Remote Ref、取消 Workflow、向既有 Task 输入和跨节点 Artifact 访问需要显式 workflow-control capability；高风险操作通过现有 Permission Review。

### 12.2 Prompt injection

上游 Agent 输出是不可信数据。Scheduler 不解析其中的“调用工具”“修改工作流”等自然语言为控制指令。Fan-in envelope 必须隔离系统 metadata、上游文本、Artifact 和主持指令。

### 12.3 模板、表达式与 Artifact

模板只能引用授权的 node/result/error/artifact 字段；expression 使用受限 DSL；限制递归、大小和计算预算。跨 Agent 或 Remote Gateway 前按 policy 脱敏。Artifact 引用在每次跨边传递时重新进行 owner 和 capability 校验。

### 12.4 审计

审计链必须能够关联 principal、主持 Task/run、proposal、人工 Review、GraphRevision、WorkflowCommand、Gateway Task/run、NodeAttempt 和结果快照。人工决定不得只存在于主持 Agent 自然语言中。

### 12.5 资源滥用

Workflow budget 与 delegation budget 分开，包括最大有效节点数、patch 数、并行节点数、总 Attempt、主持质询轮数、Agent/Remote 配额、聚合大小及 Workflow 总时限。节点内部 delegation 仍受原有预算控制。

## 13. 被拒绝的替代方案

- **WorkflowRun/Engine 是特殊 Gateway Task**：生命周期、Mailbox 消费者和恢复语义冲突。
- **用 `parent_task_id` 表示 DAG**：无法表达 fan-in，并破坏 delegation budget、scope 和回投语义。
- **主持 Agent 自己做调度器**：模型输出不具事务性、幂等性和确定性。
- **Mailbox 作为 Workflow event bus 或虚构 workflow thread**：违反 Task inbox 边界。
- **Scheduler 直写 MailboxStore**：绕过 Local/Remote 路由、附件、状态 guard、route 和错误语义。
- **阻塞 `wait_agent` 实现 fan-in**：把推进依赖于调用者持续活跃，且不能作为恢复事实源。
- **只允许动态追加叶子**：无法显式表达多轮交叉质询、人工 Gate 插入和对尚未执行未来路径的调整。
- **运行中任意改图**：会追溯性破坏已发生因果和恢复确定性。
- **单行 `node_runs_json` 作为动态权威状态**：扩大并发冲突，缺少 Attempt/Command 独立约束。
- **每次 retry 都创建新 Task**：默认破坏长 Session 连续性；仅作为明确隔离策略。
- **永远复用同一 Task**：无法处理上下文污染、Agent 切换和安全隔离。
- **模型输出 envelope 用作幂等/correlation**：模型可遗漏、修改或伪造；强关联必须来自持久记录。
- **孤儿扫描作为 create 幂等机制**：启发式匹配可能误关联或误取消，只能作为防御性观测。
- **任一审批导致全局暂停**：无关分支不应被阻塞。
- **LangGraph 编排跨 Task 工作流**：其 checkpoint 属于单 Task 内部执行现场，不拥有跨长期 Task 的路由、审批和生命周期。
- **外部通用工作流引擎作为本层核心**：会重复或旁路 Ruyi 已有 Task、Review、Mailbox 和 routing 语义。

## 14. 设计风险

- **端到端幂等能力不一致**：Local/Remote Gateway 若缺少持久 command-id，会降级为至少一次并产生副作用不确定状态。
- **Gateway 只暴露最新 run**：若不能按 `run_count` 查询历史，Scheduler 必须在租约下及时固化 Attempt 结果；异常 operator override 会产生不确定状态。
- **非幂等工具副作用**：即使 dispatch 幂等，Agent 内部外部调用仍可能重复，需工具自身支持 idempotency 或人工裁决。
- **动态 patch 复杂度**：影响范围判定错误可能破坏因果；历史不可变、未来 CAS 和 started-node guard 是安全边界。
- **Remote 状态长期不确定**：不可简单映射为 failed，会延长 WorkflowRun 非终态时间。
- **调度者单点或双活**：停机导致工作流停滞；意外双活可能重复 dispatch，必须依赖持久 lease/epoch 和 command id。
- **主持 Agent 无限扩图**：必须依赖预算与审批而非提示词自律。
- **权限边界分裂**：Workflow owner、主持 Task owner、节点 Task owner 和 Remote owner 可能不同，默认继承会导致数据泄露。
- **聚合上下文过大**：fan-in 需要大小限制、摘要和 Artifact 引用，且摘要不能破坏 source Attempt 可追溯性。

## 15. 决策账本

| decision_id | topic | options | participants' positions | evidence | round | status |
|---|---|---|---|---|---:|---|
| WF-001 | WorkflowRun 与 Scheduler 身份 | A 独立基础设施实体；B 特殊 Gateway Task | Codex: A；DeepSeek: 初始 B，挑战后 ACCEPT A | Task 是可继续 Agent Session；Mailbox 在模型边界消费；Workflow 终态不可重开；已有 Proposed ADR 拒绝 Engine=Task | 2 | RESOLVED: A |
| WF-002 | 主持与调度边界 | A 主持做语义、Scheduler 做机械状态；B 主持直接调度 | Codex: A；DeepSeek: A | 调度需要确定性、CAS、幂等和恢复；主持可能失败且输出非事务性 | 1 | RESOLVED: A |
| WF-003 | Workflow edge 与 delegation | A 正交图；B 复用 `parent_task_id` | Codex: A；DeepSeek: A | fan-in 多父无法由单值 parent 表达；parent 已承担 depth/budget/scope/回投 | 1 | RESOLVED: A |
| WF-004 | 人工审批作用域 | A 分支 Gate；B 全局暂停 | Codex: A；DeepSeek: A | 无依赖并行分支不应阻塞；Run pause 是聚合投影而非锁 | 1 | RESOLVED: A |
| WF-005 | 动态改图边界 | A 受约束 revision patch；B 仅追加叶子；C 任意改图 | Codex: A；DeepSeek: 初始 B，挑战后 ACCEPT A | 多轮交叉质询与人工 Gate 需修改未执行未来；CAS、环检测和 started-node guard 保持恢复与审计 | 2 | RESOLVED: A |
| WF-006 | 运行态存储粒度 | A 规范化动态实体 + JSON 不可变快照；B 单行可变 JSON | Codex: A；DeepSeek: 初始 JSON，挑战后 ACCEPT A | Attempt 快照、Command outbox、patch CAS、局部并发更新需要独立约束；节点数小不消除一致性问题 | 2 | RESOLVED: A |
| WF-007 | 节点 retry 与 Task Session | A 默认同 Session，可选新 Session；B 总是新 Task；C 永远同 Task | Codex: A；DeepSeek: 默认同 Session | Task 是长 Session；上下文连续性有价值，但污染、切换和隔离需要显式 new-session escape hatch | 1 | RESOLVED: A |
| WF-008 | create/send 幂等 | A 持久 command + Gateway command-id；B 状态 guard/孤儿扫描；C 模型 envelope | Codex: A；DeepSeek: 承认 Gateway 重启缺口并要求持久 key | create-success/writeback-crash 无法靠 Mailbox key 解决；模型回显不可靠；孤儿匹配非确定 | 1 | RESOLVED: A |
| WF-009 | Mailbox 在工作流中的角色 | A 仅具体 Task 输入，状态查询调和；B workflow event bus/watcher | Codex: A；DeepSeek: 挑战后接受 A | `CONTEXT.md` 明确 Mailbox 不定义 workflow dependency；直接写会绕过路由与 guard | 2 | RESOLVED: A |
| WF-010 | 审批协议 | A 复用现有 Task Review；B 新 Workflow Review 协议 | Codex: A；DeepSeek: 复用既有审批能力 | Review 已具 Task 等待、恢复和审计；双协议会产生不一致 | 1 | RESOLVED: A |
| WF-011 | 工作流期间外部 Task 输入 | A 编排租约/显式 override；B 允许任意输入；C 完全锁死 Task | Moderator: A；架构证据支持 A | 同 Task 多 run 会覆盖最新结果并破坏 correlation；完全锁死又破坏独立 Session 的查询/审批语义 | 1 | RESOLVED: A |
| WF-012 | 恢复事实来源 | A Workflow Store + Gateway Task 查询调和；B 仅 Mailbox 事件；C 从头重放 | Codex: A；DeepSeek: A | Task settled 与通知间存在 crash window；Mailbox 是 hint；旧自然语言和固化输入不得重新解释 | 1 | RESOLVED: A |

所有决策均已 RESOLVED；当前没有 NEEDS_HUMAN 的架构决策。本文只有在获得明确人工批准后才冻结。
