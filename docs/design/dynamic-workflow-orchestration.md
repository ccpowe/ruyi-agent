Status: Proposed

# Ruyi 动态工作流编排层设计

## 1. 范围

本设计为 Ruyi 增加一个持久化的动态工作流编排控制面，支持：

- 多个 Agent 独立并行分析；
- 主持 Agent 基于证据收敛分歧；
- Workflow 级人工审批与既有 Gateway Task Review；
- 串行、并行、条件分支和 fan-in；
- 节点失败重试、超时、暂停与取消；
- 编排进程重启后的状态对账与恢复；
- 复用既有 Gateway Task、Task Mailbox 和 `send_input`；
- 保持 Gateway Task 是独立、持久、可继续交互的长 Session。

## 2. 假设与约束

1. Gateway Task 使用稳定的 `task_id/thread_id` 表示长期 Session；一次 Run settled 不关闭 Task。
2. Task 当前状态描述当前或最近一次 Run；`run_count` 区分同一 Task 的不同 Run。
3. `parent_task_id` 表示委托树，并参与深度、预算、权限继承和父子可见性，不表示 Workflow DAG。
4. Task Mailbox 是投递给真实 Gateway Task 的持久输入箱，在安全模型边界注入输入并唤醒 resumable Task；它不定义 Workflow 依赖关系。
5. Workflow Run 固定保存启动时的 Definition 快照；每次受限 Graph Patch 产生新的不可变运行图 revision，不能回写或覆盖既有快照。
6. 事件、Webhook 和本地 settled hook 都可能重复、乱序或丢失，只能作为调度唤醒提示。
7. 跨 Workflow Store、Gateway 和 Agent 外部工具不存在统一事务，因此不宣称外部副作用 exactly-once。
8. v1 的持久化和调度边界允许单活 Scheduler；模型预留 lease/CAS，以免锁死未来多实例能力。

## 3. 非目标

- 不修改 Gateway Task 的长 Session 生命周期语义。
- 不把 Workflow Engine 实现成 Agent 或 Gateway Task。
- 不把 DAG 边编码进 `parent_task_id`、`TaskRecord` 或 LangGraph checkpoint。
- 不把 Task Mailbox 扩展成通用 Workflow 事件总线。
- 不让 Channel Adapter 拥有 Workflow 调度策略。
- 不承诺任意外部工具副作用 exactly-once。
- 本设计不规定实施阶段、构建顺序、任务分配、测试计划或发布步骤。

## 4. 总体架构

```text
HTTP / Channel / Agent Tool
             |
             v
+-----------------------------------------+
|          Workflow Control Plane         |
|                                         |
|  Definition Registry                    |
|  Workflow Engine / Scheduler            |
|  Convergence Coordinator                |
|  Approval Service                       |
|  Reconciler                             |
|  Workflow Store                         |
+-------------------+---------------------+
                    | GatewayTaskPort
                    v
+-----------------------------------------+
|          Gateway Task Module            |
| create / get / send_input / cancel      |
| review / route / artifact               |
+-------------------+---------------------+
                    v
               Task Router
                    v
               AgentControl
                    v
     Gateway Task + Task Mailbox
                    v
       Local Agent / Remote Agent
```

### 4.1 Workflow Control Plane

Workflow Control Plane 是 transport-neutral 的确定性协调模块，负责：

- Definition 版本化、校验和快照；
- Workflow Run、Node Run 和 Node Attempt 状态；
- DAG ready 判定、fan-out、fan-in 和条件分支；
- 重试、超时、暂停、取消；
- 主持协议和人工升级；
- Gateway Task 状态对账与重启恢复。

它不直接执行 Agent，不直接读写 LangGraph checkpoint，也不绕过选定的 `GatewayTaskPort` 修改 `TaskRecord`。

### 4.2 GatewayTaskPort

Workflow 通过由 Gateway Task Module 提供的窄 `GatewayTaskPort` 使用任务能力；Gateway Task Module 内部继续委托 Task Router 统一处理 Local/Remote Route、远端状态刷新和 routed operations。Workflow 不得绕过该边界直接操作 `AgentControl` 或 Mailbox，见 DEC-009：

```python
class GatewayTaskPort(Protocol):
    async def create_task(
        self,
        *,
        agent_name: str,
        input_content: str,
        metadata: dict,
        idempotency_key: str | None = None,
    ) -> TaskSnapshot: ...

    async def get_task(self, task_id: str) -> TaskSnapshot: ...

    async def get_task_run(
        self,
        task_id: str,
        run_count: int,
    ) -> TaskRunSnapshot: ...

    async def send_input(
        self,
        *,
        task_id: str,
        input_content: str,
        idempotency_key: str | None = None,
    ) -> TaskSnapshot: ...

    async def cancel_task(self, task_id: str) -> TaskSnapshot: ...

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict],
    ) -> TaskSnapshot: ...
```

`idempotency_key` 和按 `run_count` 查询是兼容性增强，不改变 Task 或 Session 生命周期。

### 4.3 Reconciler

Reconciler 接收本地 settled hook、远程 Webhook、审批变化和定时唤醒。任何通知都只表示“状态可能变化”；Reconciler 必须重新读取 Gateway Task 快照并与 Workflow Store 对账。

```text
Gateway Task Snapshot = 执行侧观察事实
Workflow Store        = 编排侧权威事实
Webhook/Event         = 非权威唤醒提示
```

## 5. Workflow 定义模型

Definition 包含不可变 revision、节点、边、输入映射、失败策略、审批策略和主持策略。

```yaml
workflow_id: architecture-review
revision: 1
nodes:
  - id: analyst_a
    kind: agent
    agent_name: architect_codex
  - id: analyst_b
    kind: agent
    agent_name: architect_deepseek
  - id: moderator
    kind: moderator
    agent_name: moderator
    convergence:
      max_rounds: 3
      final_authority: human
      on_unresolved: request_approval
  - id: approval
    kind: approval
    scope: downstream
edges:
  - from: analyst_a
    to: moderator
  - from: analyst_b
    to: moderator
  - from: moderator
    to: approval
```

支持的节点边界：

- `agent`：创建或继续一个 Gateway Task Run；
- `moderator`：执行结构化收敛协议；
- `approval`：Workflow 业务门禁；
- `join`：确定性 fan-in barrier；
- `condition`：确定性条件路由；
- `fan_out`：确定性或受限动态成员展开，动态集合必须显式 seal；
- `transform`：确定性数据映射；
- `verify`：结构、规则或独立 Agent 验证；
- `end`：显式终点。

运行时动态能力包含两类：Definition 预定义的确定性 `condition` 节点/条件边，以及受限 Graph Patch。条件求值只读取结构化输入，使用无循环、无函数调用、无外部 I/O 的受限表达式；求值失败不得激活任何分支。Graph Patch 以 `expected_revision` CAS 原子应用，只能新增或替换未开始区域，不能改写 active/terminal Attempt、已消费 Approval 或历史输出，也不能形成环。动态 fan-out 使用显式 `OPEN → SEALED` 成员集合；Join 在集合 seal 前不得满足。每次 Patch 产生新的不可变运行图 revision，既有 Attempt 继续绑定其创建时的 revision。

## 6. 持久化状态模型

### 6.1 WorkflowDefinition

```text
workflow_id
revision
schema_version
definition_json
definition_hash
status: draft | published | archived
created_at
created_by
PRIMARY KEY(workflow_id, revision)
```

Definition 使用 JSON，因为它是不可变、整体读取且不存在节点级并行写。

### 6.2 WorkflowRun

```text
run_id
workflow_id
workflow_revision
definition_snapshot_json
definition_hash
state
input_json
output_json
error_code
error_detail_json
pause_requested
cancel_requested
version
scheduler_owner
lease_expires_at
next_wakeup_at
created_at
started_at
updated_at
finished_at
```

状态：

```text
created
running
paused
waiting_for_approval
cancelling
completed
completed_with_failures
failed
cancelled
```

`waiting_for_approval` 是聚合视图；局部审批不要求无关分支停止。

### 6.3 WorkflowNodeRun

```text
run_id
node_id
node_kind
state
resolved_input_json
output_json
error_code
error_detail_json
current_attempt_no
version
started_at
finished_at
PRIMARY KEY(run_id, node_id)
```

状态：

```text
blocked
ready
dispatching
running
waiting_for_task_review
waiting_for_workflow_approval
retry_wait
succeeded
failed
skipped
cancelled
unknown
```

`unknown` 表示 Gateway 状态暂时不可确认，不得直接解释为业务失败。

### 6.4 WorkflowNodeAttempt

每行表示一次可独立幂等派发并绑定特定 Gateway Task Run 的节点执行轮次；v1 中它同时承担持久 outbox 意图。

```text
attempt_id
run_id
node_id
attempt_no
operation: create_task | send_input | deterministic
state
idempotency_key
input_json
input_hash
task_id
base_run_count
expected_run_count
observed_run_count
moderation_round
target_node_id
target_task_id
not_before
lease_owner
lease_expires_at
last_error
created_at
updated_at
settled_at
UNIQUE(run_id, node_id, attempt_no)
UNIQUE(idempotency_key)
```

Attempt 状态：

```text
prepared
dispatching
observing
waiting_review
succeeded
failed
retry_wait
ambiguous
cancelled
```

Attempt 独立记录每次 `create_task` 或 `send_input` 的目标、payload、幂等键和预期 `run_count`。这使主持向多个分析 Task 并行提问时，可以分别恢复“已提交、未提交、状态未知”的输入，而不是依赖 NodeRun 上的单个聚合序号。

### 6.5 WorkflowApproval

```text
approval_id
run_id
node_id
attempt_id
kind
scope: node | downstream | run
state: pending | approved | rejected | expired | cancelled
request_json
policy_json
decision_json
version
requested_at
expires_at
decided_at
decided_by
```

Workflow Approval 与 Gateway Task Review 是不同实体，不能伪装成 `TaskRecord.pending_review`。

## 7. Task、Run 与 Attempt 的关系

```text
Workflow Node
    |
    +-- Attempt 1
    |      +-- Gateway Task T1 / run_count=1
    |
    +-- Attempt 2: clean retry
    |      +-- Gateway Task T2 / run_count=1
    |
    +-- Attempt 3: clarification
           +-- Gateway Task T2 / run_count=2
```

不变量：

1. Gateway Task 始终是长期 Session。
2. Workflow Attempt 只观察一个确定的 `task_id + expected_run_count`。
3. Workflow 完成、失败或取消不会删除或关闭 Task。
4. Task 后续被继续时，不得用最新结果覆盖旧 Attempt 的输出。
5. DAG 边不改变 `parent_task_id`、root budget、depth 或权限继承。
6. 同一 Gateway Task 不得同时被多个 Workflow Attempt 作为独占的“下一 Run”目标；冲突必须进入 `ambiguous` 或拒绝派发。

## 8. 调度与 DAG 语义

### 8.1 Ready 判定

节点只有在以下条件全部满足时进入 `ready`：

- Run 未全局 pause/cancel；
- required inbound edges 已有确定结果；
- 至少一个入边激活，或节点是 source；
- 输入映射成功；
- 所需 Approval 已批准；
- 没有 active Attempt；
- retry 的 `not_before` 已到期。

若所有入边均未激活，节点进入 `skipped`，而非永久 `blocked`。

### 8.2 串并行与 fan-in

```text
                         +--------------+
                         | User Request |
                         +------+-------+
                                |
              +-----------------+-----------------+
              v                                   v
      +---------------+                   +---------------+
      | Architect A   |                   | Architect B   |
      | Gateway T1/R1 |                   | Gateway T2/R1 |
      +-------+-------+                   +-------+-------+
              +-----------------+-----------------+
                                v
                         +--------------+
                         | Evidence Join|
                         +------+-------+
                                v
                         +--------------+
                         | Moderator T3 |
                         | Round 1      |
                         +------+-------+
                                |
                         material dispute?
                         +------+------+
                         |             |
                        yes            no
                         |             |
              send_input T1/T2         |
              focused questions        |
                         |             |
              collect next runs        |
                         |             |
                send_input T3          |
                         |             |
                   max 3 rounds        |
                         |             |
                    unresolved?        |
                      +--+--+           |
                     yes   no           |
                      |     +-----------+
                      v                 v
               +--------------+   +-----------+
               |Human Approval|   | Downstream|
               +------+-------+   +-----------+
                      |
                send_input T3
                      |
                      v
               +--------------+
               | Final Result |
               +--------------+
```

Fan-in 正确性来自持久 NodeRun/Attempt 状态，不来自 Mailbox 消息数量。上游输出按 Definition 中稳定顺序聚合并生成输入快照；之后上游 Task 的新 Run 不改变该快照。

### 8.3 状态推进语义

调度决定先持久化 Attempt，再调用外部 Gateway：

```text
事务 A：Attempt = prepared
提交
Gateway create/send_input
事务 B：绑定 task_id/run_count，Attempt = observing
提交
```

外部调用不得持有长数据库事务。Run lease、Attempt lease 和 version CAS 防止重复调度者同时推进同一状态。

## 9. Mailbox 与 send_input

### 9.1 保留的复用路径

```text
Workflow
   -> GatewayTaskModule.send_input
   -> TaskRouter / AgentControl
   -> Task Mailbox
   -> 真实 Gateway Task 的安全模型边界
```

适用于主持追问、格式修订、人工补充约束和同 Session retry。

### 9.2 外部输入冲突语义

Workflow 不能吞掉或静默拒绝发送给独立 Task Session 的合法外部输入。Gateway 正常投递该输入；若它改变活跃绑定 Task 的上下文或产生 Engine 未发起的 Run，关联 Attempt 进入 `conflicted/quarantined`。其后续结果保留审计但不满足依赖、不进入 Join，也不自动驱动 DAG。只有认证人工决定或 Definition 中预先声明、可确定验证的显式策略，才能采纳该结果、丢弃并重试，或将外部输入提升为新的 Workflow 输入快照。

### 9.3 禁止的扩展

不得创建虚拟 `wf-{run_id}` Mailbox recipient 来承载 Workflow fan-in。现有 Mailbox 的接收者、claim/ack 和唤醒语义面向真实 Gateway Task，且 settled 通知可能因 `wait/check` 被抑制或撤回，不能成为 Workflow 唯一事实源。

## 10. 主持 Agent 收敛协议

### 10.1 独立首轮

所有参与者收到相同基础问题，但在首轮提交前互不可见答案，以避免锚定。输出包含稳定 claim ID、假设、证据、取舍、风险和置信度。

### 10.2 决策账本

主持流程维护稳定 `decision_id`。每个条目包含：

```text
decision_id
topic
options
participants_positions
evidence
round
status: OPEN | RESOLVED | NEEDS_HUMAN
minority_report
```

首轮立即提取共识并标记 `RESOLVED`。每个未解决项只向相关参与者发送同一聚焦问题，要求明确 `ACCEPT` 或 `DISAGREE`，并附证据与取舍；参与者不得重写完整方案。

### 10.3 最多三轮

每个 `decision_id` 最多进行三轮挑战：

- 共识形成：`RESOLVED`；
- 仍有实质分歧：默认 `NEEDS_HUMAN`；
- 安全、数据丢失、兼容性和架构不变量必须以证据解决，不能按多数票关闭；
- 少数意见始终进入 minority report。

主持不拥有最终业务裁决权。只要三轮后仍有相关参与者 `DISAGREE`，该 `decision_id` 必须进入 `NEEDS_HUMAN`；安全、数据丢失、兼容性和架构不变量问题也不得由主持或多数票关闭。主持只能给出明确标注为模型建议的推荐，并保留全部证据与 minority report。

人工决定后，通过 `send_input` 继续原主持 Task，由同一 Session 生成采用人工约束的最终结果；有效授权仅来自 Workflow Approval 的认证决定，主持文本不得被解释为人工批准。

## 11. 人工审批语义

### 11.1 Gateway Task Review

工具权限审批继续使用既有 `waiting_for_human`、`pending_review` 和 `submit_review_decision`。Workflow 仅把绑定节点投影为 `waiting_for_task_review`，不自行恢复 LangGraph，也不创建新 Task。

### 11.2 Workflow Approval

Workflow Approval 用于未解决分歧、成本门禁、部分失败选择、动态图修改或不可逆业务动作。

默认 `scope=downstream`：只阻塞依赖该门禁的节点。`scope=run` 才停止整个 Run 的新调度。审批等待与 Workflow cancel 是不同动作；即使 run-scope，审批等待也不自动取消已运行 Task。

审批提交使用 `expected_version` 和幂等键。第一个有效决定胜出；重复相同决定幂等返回；冲突决定返回已解决错误。决策必须记录 actor、时间、理由和请求 hash。

## 12. 失败、重试、暂停与取消

### 12.1 错误分类

```text
transient_transport
rate_limited
remote_unavailable
timeout
process_interrupted
task_failed
task_cancelled
task_review_rejected
workflow_approval_rejected
invalid_output
verification_failed
mapping_failed
ambiguous_dispatch
policy_violation
```

### 12.2 默认失败语义

- 网络超时、限流：重试同一派发，不增加业务 Attempt；
- 远程状态不可用：进入 `unknown` 并退避对账；
- 输出格式错误：优先对同一 Task `send_input` 请求修订；
- 澄清和主持下一轮：同一 Task 新 Run；
- Task failure/interrupted：默认新 Task、新 Attempt；
- Review/Approval rejected：走显式 reject edge 或节点失败；
- Definition、映射或条件错误：配置失败，不盲目重试；
- 派发归属无法确认：进入 `ambiguous`，停止自动推进。

### 12.3 暂停

暂停阻止新节点和新 retry 派发。活跃 Gateway Task 默认继续；是否中断由独立策略决定。

### 12.4 取消

取消必须先设置持久 cancellation fence，停止新派发，并默认 best-effort 取消当前绑定的 active Gateway Task Runs。取消请求不等于底层 Run 已停止；未确认状态继续对账。fence 之后的迟到结果只记录审计，不满足依赖，也不得重新打开 Workflow。取消绝不删除或关闭 Gateway Task Session；Task hard termination 是独立的高权限语义。

## 13. 重启恢复语义

恢复与正常调度使用同一 `reconcile_and_drive` 状态推进函数。对每个 Attempt，以 `task_id + expected_run_count` 对账：

| Gateway 观察 | Workflow 行为 |
|---|---|
| run_count 相等且 running/pending | 继续观察 |
| run_count 相等且 waiting_for_human | 等待 Task Review |
| run_count 相等且 completed | 保存该 Run 输出 |
| run_count 相等且 failed/interrupted | 按 RetryPolicy |
| run_count 小于预期 | 检查 create/send 派发是否提交 |
| run_count 大于预期 | `ambiguous/superseded`，不得误用最新结果 |
| 查询暂时失败 | `unknown` + 退避，不立即重跑 |
| task_not_found | 使用相同幂等键重试或进入人工处理 |

`ping` 可以探测状态；`send_input/resume` 是业务状态转换，不能用作探测。

创建和输入的稳定幂等键分别形如：

```text
wf:{run_id}:{node_id}:{attempt_no}:create
wf:{run_id}:{node_id}:{attempt_no}:send
```

严格恢复要求 Gateway 将相同 key 持久化并在重复调用时返回同一结果。仅在 Workflow Store 中保存 key，不能单方面关闭“远端已提交、本地未记账”的崩溃窗口。

## 14. 安全考虑

1. Workflow actor 必须具有启动指定 Agent 和使用对应权限 profile 的授权。
2. Workflow Approval 与 Task Review 分别鉴权，Agent 不得自行提交人工决定。
3. Definition、revision 和运行快照必须带内容 hash，防止执行中被静默替换。
4. 主持输出必须引用原始 claim ID、Task ID、run count 和内容 hash，防止伪造共识或遗漏异议。
5. 输入映射和条件表达式必须使用受限、确定性的表达语言，不能执行任意代码。
6. Artifact 和大输出保存引用、摘要和 hash；敏感内容遵循 Gateway 既有访问控制。
7. 所有 pause、cancel、approval、retry 和 moderator ruling 都应进入不可变审计记录。
8. 非幂等外部副作用必须由节点提供业务幂等键，或由 Workflow Approval 门禁保护。
9. Workflow metadata 不得成为绕过 Gateway Task 可见性和 delegation scope 的后门。

## 15. 接口设计

### 15.1 Definition

```http
POST /workflows
POST /workflows/validate
POST /workflows/{workflow_id}/revisions
GET  /workflows/{workflow_id}
GET  /workflows/{workflow_id}/revisions/{revision}
POST /workflows/{workflow_id}/revisions/{revision}/publish
```

### 15.2 Run

```http
POST /workflow-runs
GET  /workflow-runs/{run_id}
GET  /workflow-runs/{run_id}/nodes
GET  /workflow-runs/{run_id}/attempts
GET  /workflow-runs/{run_id}/events
POST /workflow-runs/{run_id}/pause
POST /workflow-runs/{run_id}/resume
POST /workflow-runs/{run_id}/cancel
```

### 15.3 Approval

```http
GET  /workflow-approvals?state=pending
GET  /workflow-approvals/{approval_id}
POST /workflow-approvals/{approval_id}/decision
GET  /pending-actions
```

`/pending-actions` 可以统一展示 `task_review` 与 `workflow_approval`，但后端实体和决定接口保持区分。

## 16. 架构不变量

1. DAG 边绝不修改 `parent_task_id`。
2. Workflow Engine 不成为 Gateway Task。
3. Mailbox 不成为 Workflow 依赖或 fan-in 的权威来源。
4. 每个 Agent 和主持者保持独立 Gateway Task Session。
5. 每个 Workflow Attempt 绑定一个确定 `task_id + run_count`。
6. Workflow terminal 不导致 Gateway Task terminal 或删除。
7. 恢复先对账，后重试；暂时未知不能直接触发重跑。
8. 外部调用前必须持久化稳定的派发意图和幂等键。
9. 三轮后仍有 `DISAGREE` 的决定必须转人工；主持无权覆盖。
10. Workflow Approval 默认局部阻塞，显式 run-scope 才全局暂停。
11. 每个 Run 保留启动 Definition 和后续 Graph revision 的不可变快照；受限 Patch 只能影响未开始区域，既有 Attempt 永远绑定创建时 revision。
12. 外部 `send_input` 不得因 Workflow 被吞掉；若改变活跃绑定 Task 的上下文或 Run 序列，关联 Attempt 必须隔离，未经显式采纳不得驱动 DAG。
13. Workflow cancellation fence 先于任何取消后状态推进；迟到结果不得重新打开 Workflow。

## 17. 被拒绝的替代方案

### 17.1 用 `parent_task_id` 表示 DAG

拒绝。Fan-in 无法表示多个父节点，并会污染委托预算、权限和 Mailbox 回投语义。

### 17.2 把 Workflow Engine 做成 Gateway Task

拒绝。确定性调度、lease、CAS、幂等和恢复不能依赖 LLM Session 自行维护。主持可以是 Task，Scheduler 不可以。

### 17.3 把 Task Mailbox 当 Workflow 事件总线

拒绝。Mailbox 面向真实 Gateway Task 的上下文注入和唤醒，不提供类型化 DAG 依赖、fan-in 或 Workflow 消费者语义。

### 17.4 把所有 Node 状态放在 WorkflowRun 单行 JSON

拒绝。并行节点完成会竞争整行；Attempt 唯一性、派发 lease、retry 查询和崩溃窗口难以表达。Definition 可用 JSON，运行态采用 NodeRun/Attempt 行。

### 17.5 仅用 WorkflowRun + NodeRun 两表

拒绝作为可靠基线。主持多轮需要向多个目标分别发送输入，并在部分成功后恢复每条输入的 key、payload、目标和状态；把这些内容塞进 NodeRun JSON 等同于隐藏的 Attempt 表。

### 17.6 任一审批默认全局暂停

拒绝。它破坏 DAG 局部依赖和并行性。全局暂停必须显式选择 run scope。

### 17.7 三轮后由主持强制裁决

拒绝。主持被授权收敛和推荐，不拥有业务风险接受权；任何持续 `DISAGREE` 都必须转人工。

### 17.8 声称 exactly-once

拒绝。跨数据库、Gateway、远程 Agent 和外部工具没有统一事务，只能通过持久意图、幂等键和业务副作用保护实现 effectively-once。

## 18. 设计风险

| 风险 | 设计缓解 |
|---|---|
| Gateway create 不支持幂等 | 要求兼容性 `Idempotency-Key`；否则明确降级为 at-least-once |
| `send_input` key 未端到端透传 | 从 Gateway Task Module 透传至 MailboxStore，并要求远程 Gateway 等价支持 |
| 只能查询 Task 最新 Run | 要求按 `task_id + run_count` 读取 Run snapshot，或对活跃绑定实施冲突保护 |
| 用户手动继续被 Workflow 观察的 Task | 输入仍投递，但关联 Attempt 进入 `conflicted/quarantined`；显式人工或策略决定后才能采纳 |
| Webhook 丢失或乱序 | 仅作为唤醒；周期 `get_task` 对账 |
| SQLite 写竞争 | NodeRun/Attempt 分行、短事务、WAL、并发上限和 lease |
| 主持伪造共识 | 决策账本、claim 引用、内容 hash、minority report、人工升级 |
| 无限讨论或重试 | 每 decision 最多三轮，并限制 Attempt、时间和成本预算 |
| Workflow 取消后 Task 迟到完成 | cancellation fence，迟到结果只审计 |
| 非幂等外部副作用重复 | 业务幂等键或人工门禁 |
| Definition schema 演进 | schema version、启动快照和不可变 Graph revisions |
| 受限 Patch 绕过审批或改变已执行因果 | revision CAS、未开始区域限制、Approval subject hash、无环校验和动态成员 seal |
| 单一 Task 被多个 Workflow 并发继续 | 拒绝派发或标记冲突，不允许隐式争抢下一 Run |

## 19. 决策账本

### DEC-001 — Scheduler 与 Gateway Task 边界

- **decision_id:** DEC-001
- **topic:** Workflow Engine/Scheduler 是否作为 Gateway Task 运行
- **options:** A. Scheduler 是 Gateway Task；B. Scheduler 是独立确定性控制面，主持者仍是普通 Gateway Task
- **participants' positions:** architect_codex=B；architect_deepseek 初始 A、挑战轮 2 接受 B；moderator=B
- **evidence:** Mailbox 注入发生在模型调用边界；lease/CAS、deadline、fan-in、审批等待和恢复对账不需要模型；Task checkpoint 与 Workflow Store 的权威状态及故障域不同
- **round:** 2
- **status:** RESOLVED

### DEC-002 — DAG、委托树与 Mailbox

- **decision_id:** DEC-002
- **topic:** 是否用 `parent_task_id` 或 synthetic Mailbox 表达 Workflow 依赖
- **options:** A. 复用委托树/Mailbox 作为 DAG；B. 独立 DAG 和 Task binding，Mailbox 只投递真实 Task
- **participants' positions:** architect_codex=B；architect_deepseek=B；moderator=B
- **evidence:** `parent_task_id` 是单父委托关系并参与预算、depth 与权限；fan-in 需要多上游；`CONTEXT.md` 明确 Mailbox 不定义 workflow dependencies
- **round:** 1
- **status:** RESOLVED

### DEC-003 — 可靠运行态与恢复保证

- **decision_id:** DEC-003
- **topic:** 最新 Task 快照是否足以支持重启恢复
- **options:** A. Node 状态加最新 Task 快照；B. Run+NodeRun+Attempt/Command intent，并由 Gateway 持久化幂等关联和 Run snapshot
- **participants' positions:** architect_codex=B；architect_deepseek 初始 A、挑战轮 1 接受 B；moderator=B
- **evidence:** Gateway 调用成功但 Workflow 未保存 receipt 的崩溃窗口无法由最新状态消除；多 Run Task 会覆盖旧 Attempt 的可见结果；MailboxStore 已有输入幂等基础但公开 create/send_input 尚未端到端透传
- **round:** 2
- **status:** RESOLVED

### DEC-004 — 活跃绑定期间的外部 `send_input`

- **decision_id:** DEC-004
- **topic:** 外部输入改变 Workflow 正在观察的 Task 时如何归因
- **options:** A. 拒绝输入；B. 正常投递且自动采纳结果；C. 正常投递，但 Attempt 进入 `conflicted/quarantined`，显式决定后才采纳
- **participants' positions:** architect_codex=C；architect_deepseek 初始 B、挑战轮 2 接受 C；moderator=C
- **evidence:** Task 独立 Session 要求输入不被静默吞掉；Engine 无法可靠判定自然语言是补充还是矛盾；把归因安全交给 Moderator 会依赖不可信模型；当前 Task 主要暴露最新 Run
- **round:** 2
- **status:** RESOLVED

### DEC-005 — Workflow Cancel 语义

- **decision_id:** DEC-005
- **topic:** 取消是否中断当前 Run、是否关闭 Task Session
- **options:** A. 仅停止新派发并 drain；B. cancellation fence + 默认 best-effort 取消 active Runs，但保留 Task Session；C. 递归关闭 Task
- **participants' positions:** architect_codex=B；architect_deepseek=B；moderator=B
- **evidence:** fence 是跨重启和派发竞态的线性化边界；Gateway cancel 只取消当前 Run；迟到结果无法避免但可隔离；Workflow 不拥有 Task Session 生命周期
- **round:** 1
- **status:** RESOLVED

### DEC-006 — 动态条件与 Graph Patch

- **decision_id:** DEC-006
- **topic:** 动态工作流是否支持预定义条件与运行中拓扑变更
- **options:** A. 仅 Moderator 临时 Patch；B. 确定性 condition + 受限、版本化 Patch；C. 任意动态图
- **participants' positions:** architect_codex=B；architect_deepseek 初始 A、挑战轮 2 接受 B；moderator=B
- **evidence:** 预定义受限条件更确定、可审计且无需模型；Patch 需 revision/CAS、无环和历史不可变；动态 fan-out 若无显式 seal 会过早满足 Join
- **round:** 2
- **status:** RESOLVED

### DEC-007 — 主持收敛与最终权威

- **decision_id:** DEC-007
- **topic:** 如何判定共识及三轮后由谁决定
- **options:** A. 多数或主持裁决；B. 所有相关参与者明确 ACCEPT 才 RESOLVED，持续 DISAGREE 三轮后转人工
- **participants' positions:** architect_codex=B；architect_deepseek 初始允许主持判断、挑战轮 2 接受 B；moderator=B
- **evidence:** 多数不是共识；主持是不可信 Task、不能授予安全或业务风险；稳定 decision ledger、同题质询和 minority report 防止命题漂移与虚假共识
- **round:** 2
- **status:** RESOLVED

### DEC-008 — 审批实体与阻塞范围

- **decision_id:** DEC-008
- **topic:** Workflow Approval 是否复用 Task Review，默认阻塞局部还是全局
- **options:** A. 统一为 Task Review 且全局暂停；B. 两类审批分离，Workflow Approval 默认 downstream、显式 run scope 才全局暂停
- **participants' positions:** architect_codex=B；architect_deepseek=B；moderator=B
- **evidence:** Task Review 绑定具体 Task 工具调用；Workflow Approval 绑定 DAG 决策快照；无依赖分支不应被无关门禁阻塞
- **round:** 1
- **status:** RESOLVED

### DEC-009 — Gateway 调用边界

- **decision_id:** DEC-009
- **topic:** Workflow 是否可绕过 Gateway Task Module 直接操作 AgentControl/Mailbox
- **options:** A. 直接操作底层运行时或 Mailbox；B. 通过 GatewayTaskPort/Gateway Task Module，保持 Local/Remote 路由与领域错误一致
- **participants' positions:** architect_codex=B；architect_deepseek=B；moderator=B
- **evidence:** 现有架构将 TaskRouter 定义为 local/remote route、远端刷新和 routed operations 的唯一内部边界；直写 Mailbox 会丢失 route、attachment、review 和唤醒语义
- **round:** 1
- **status:** RESOLVED

### DEC-010 — Exactly-once 边界

- **decision_id:** DEC-010
- **topic:** Workflow 是否承诺跨 Gateway 与外部工具 exactly-once
- **options:** A. exactly-once；B. 持久 intent + 端到端幂等实现 effectively-once，无法确认时显式 ambiguous
- **participants' positions:** architect_codex=B；architect_deepseek=B；moderator=B
- **evidence:** Workflow Store、Gateway、远程 Agent 与外部工具之间不存在统一事务；网络超时不能区分未提交与已提交但响应丢失
- **round:** 1
- **status:** RESOLVED

## 20. 待人工决定

无。所有架构决定均已 `RESOLVED`。本设计仍保持 `Status: Proposed`，只有用户显式批准后才冻结。
