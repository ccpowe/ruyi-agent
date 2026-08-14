# 交叉质询第 3 轮（最终）：correlation / 幂等 / 模块边界

**角色:** 独立架构分析师
**日期:** 2026-08-13
**参考:** 上轮 proposal（envelope + triple 幂等键 + 单活跃 attempt guard + V1.1 补 get_run_history），对方三问

---

## 问题 1：直写 MailboxStore 是否破坏模块边界？

### 对方质疑

> 直接绕过 GatewayTaskModule/TaskRouter 是否破坏 local/remote_ref 一致路由、attachment/唤醒/TaskAlreadyRunning 语义与模块边界？

### 裁决：ACCEPT 对方立场——必须走 GatewayTaskModule

经逐条追踪调用链后确认，直接绕过的代价过高：

**调用链 A：create task（走 GatewayTaskModule）**

```
GatewayTaskModule.create_task
  → _router.create_task              # 错误转换 + delegation context 验证
    → _control.spawn_task            # generate task_id, enforce budget, save TaskRecord
      → _task_manager.create_task_record  # SQLite INSERT
      → _start_run(task_id, task)         # asyncio Task + mark_running
    → _route_store.asave_route(route)     # GatewayRouteStore INSERT (local/remote_ref)
```

**调用链 B：send_input（走 GatewayTaskModule）**

```
GatewayTaskModule.send_input
  → _router.get_route(task_id)       # route_kind: local vs remote_ref resolution
  → _router.send_input(route, content)
    → _control.send_task_input(task_id, message)
      → _mailbox.publish_input(recipient_task_id, ..., trigger_run=True)
      → _ensure_task_awake(task_id)
        → _start_mailbox_run(task_id)     # MailboxMiddleware provides input
    ← TaskRecord (after wakeup completes)
```

**如果 WorkflowEngine 直写 MailboxStore.publish_input：**

| 能力 | 走 GatewayTaskModule | 直写 MailboxStore | 丢失什么 |
|------|---------------------|-------------------|---------|
| local vs remote_ref 路由 | `TaskRouter.get_route`→`route_kind` | 没有 Route 信息 | `send_input` for remote_ref 必须走 A2AClient 而非 mailbox |
| attachment 处理 | `_prepare_input_with_attachments` 将 attachment 写入文件并更新 content + metadata | 无 | 工作流节点无法传递文件附件 |
| TaskAlreadyRunning guard | `send_input`→`ensure_record`→检查 state (`waiting_for_human` 拒绝) | `publish_input` 是 fire-and-forget | 向 waiting_for_human 的 task 发 input 未被拒绝，可能注入错误上下文 |
| route metadata 持久化 | `save_route` 写 GatewayRouteStore | 无 | 重启后 `get_route` 失败 → Gateway 路由丢失 |
| 错误转换 | `TaskAlreadyRunningError`→HTTP 409, `UnknownWorkerTaskError`→404 | 原始异常透传 | 非 Gateway 路径调用时异常语义混乱 |

**结论：V1 不可绕行。** WorkflowEngine 必须通过 `AgentControl.spawn_task` 和 `AgentControl.send_task_input`。但是否需要通过 `TaskRouter`？——不需要，因为工作流节点 task 不需要 GatewayRouteStore 的路由（不需要通过 HTTP Gateway 暴露给外部调用者）。引擎可以直接调用 `AgentControl.spawn_task` / `send_task_input`——这两者是 `AgentControl` 的公共接口，是比 `TaskRouter` 更底层但仍然是合法的 API 层。

---

## 问题 2：envelope 是否真可由引擎包裹，而非依赖模型回传？

### 对方质疑

> 模型输出 envelope 是否真的能由"引擎在输出端包裹"，还是只能依赖模型回传，因而不是强 correlation？

### 裁决：DISAGREE 对方——引擎侧包裹是可行的，且是更强 correlation

**分析两个方向：**

**输入端（引擎 → Agent）：** 引擎在构造 input_content 时包裹 envelope，例如：

```
{"wf_run_id":"abc","node_id":"architect","attempt":1,"correlation":"abc.architect.1"}:
请分析以下架构需求...（原始输入）
```

引擎调用 `AgentControl.send_task_input(task_id, enveloped_content)`。这是引擎**主动构造**的字符串，不依赖 Agent 回传。envelope 前缀保证该 input 被发送到正确的 node task。

**输出端（Agent → 引擎 mailbox watcher）：**

Agent 输出落地为 `TaskRecord.result`（字符串）。引擎接收到 `_maybe_publish_settled_message` 产生的 mailbox notification 时：

1. Mailbox message 带有 `child_task_id` 和 `child_run_count`
2. 引擎交叉引用 `workflow_node_runs` 表：`SELECT node_id, attempt FROM workflow_node_runs WHERE run_id=? AND task_id=?`
3. 引擎此时已经知道 `(wf_run_id, node_id, attempt)` 三元组——不需要从 Agent 输出**解析**它

但是，为了**验证**输出确实属于本次 attempt，引擎应该在读取 `TaskRecord.result` 后检查第一行是否匹配预期的 envelope prefix。如果不匹配（模型"忘记"了 envelope），这是 **correlation mismatch**，应记录为 `verified_with_correlation_mismatch` 并记录 warming。

**更精确的 correlation 方案：** 输入 envelope 仅在输入侧。输出侧引擎通过 `(task_id, run_count)` → `workflow_node_runs.get(task_id)` → `node_id, attempt` 建立关联。这是**数据库 join**，不是字符串解析。任务创建时 task_id 就绑定到 node_run 行，无需模型配合。

**结论：引擎侧 correlation 比模型回传更可靠。** 不依赖 Agent 输出中包含任何 metadata。数据库 join：`task_id` → `workflow_node_runs.node_id/attempt` → 确认身份。

---

## 问题 3：create_task 成功后崩溃的重复 Task 处理

### 对方质疑

> create_task 成功后崩溃的重复 Task 如何处理？`spawn_task` 每次调用生成新 `task_id`，无法幂等。

### 裁决：这是硬问题。当前接口确实缺乏幂等 create 能力。

**证据：**

```python
# AgentControl.spawn_task (line 2759)
task_id = str(uuid.uuid4())  # 每次调用生成新 UUID
# ...
self._task_manager.create_task_record(task_id, ...)  # SQLite INSERT
# ...
self._start_run(task_id, task)  # 启动 asyncio task
```

崩溃窗口：

```
1. spawn_task("architect", "分析需求") → task_id=a1, TaskStore 写入 ✓, run 启动 ✓
2. Engine crash (在写入 workflow_node_runs 之前)
3. 重启恢复：workflow_node_runs 中无 a1 → 重新调度 "architect" node
4. spawn_task("architect", "分析需求") → task_id=a2, 创建了新重复 task
5. 结果：a1 仍在运行但引擎不知道，a2 也在运行 → 重复工作
```

**为什么 MailboxStore 的 `idempotency_key` 不能解决 create 幂等：**
- `MailboxStore.idempotency_key` 只适用于 mailbox 消息（INSERT OR IGNORE），不适用于 TaskRecord 创建
- `TaskStore.save_task` 使用 `INSERT OR REPLACE`——需要先有一个 task_id，但 `task_id` 本身就是由 `uuid.uuid4()` 生成的

**两种修复路径：**

| 路径 | 机制 | 成本 |
|------|------|------|
| 让 WorkflowEngine 预分配 task_id | `task_id = f"wf-{run_id}-{node_id}"`，确定性生成 | 低——但破坏了 task_id 的随机性约定 |
| AgentControl.spawn_task 增加 `idempotency_key` | 新增 `idempotency_key` 参数，在 `TaskStore` 查询 `SELECT task_id WHERE idempotency_key=?`，存在则返回已有 TaskRecord | 中——需要改 `agent_tasks` 表加列 + `TaskStore` 加查方法 |
| WorkflowEngine 恢复时检测并收回孤儿 task | 恢复时扫描所有 `task_id` 在 `workflow_node_runs` 之外但 `parent_thread_id == f"wf-{run_id}"` 的 TaskRecord | 低——需要 WorkflowEngine 有权调用 `TaskManager.list_by_parent_thread_id` 或等价的 `TaskStore` 方法 |

---

## 最终选择

### 我方撤回 Y 方案（直写 mailbox）——对方质疑成立。

### 选择 Z：V1 最小侵入性 additive 方案

**不修改 AgentControl 接口。** 利用 WorkflowEngine 恢复能力处理崩溃场景，而非要求接口幂等。

**Z 方案核心设计：**

**1. WorkflowEngine 仅通过 AgentControl 公共接口操作：**
   - `spawn_task(agent_name, task, parent_task_id=None, parent_thread_id=f"wf-{run_id}")`
   - `send_task_input(task_id, enveloped_content)`
   - `cancel_task(task_id)`
   - `submit_review_decision(review_id, decisions)`

**2. Crash-恢复：引擎恢复时扫描 orphan tasks：**
   ```
   recover():
     1. 加载所有 non-terminal WorkflowRun
     2. 对每个 run，从 workflow_node_runs 读已映射的 (node_id, task_id)
     3. 扫描 agent_tasks WHERE parent_thread_id = f"wf-{run_id}" 
        且 task_id 不在 workflow_node_runs.task_id 集合中
        → 这些是 orphan tasks（已创建但未映射到 node）
     4. 孤儿 task 处理：
        a. 通过 task_id 匹配 workflow_node_runs 中 state='pending' 的 node
           （按 agent_name 或 template 推断）
        b. 无法匹配的 → cancel_task(orphan_task_id)
        c. 无法匹配 + node 已 running 的 orphan → cancel_task
   ```

**3. 输入 idempotency（send_input 方向）：**
   - 引擎不直接调用 `publish_input`，而是通过 `send_task_input`
   - 引擎在 recovery 时使用 envelope 前缀检测：如果 task 的 `result` 已包含对应 `correlation` 前缀，则跳过重发
   - 单活跃 attempt guard：`NodeRun.state == 'running'` → 不重发 input

**4. 输出 correlation（settled→引擎方向）：**
   - Mailbox 通知 → `child_task_id` + `child_run_count`
   - 引擎 `SELECT node_id, attempt FROM workflow_node_runs WHERE run_id=? AND task_id=?`
   - TaskRecord.result 直接作为 NodeRun.output_snapshot（通过 TaskStore 读取）
   - 不需要 Agent 输出中包含 correlation metadata

**5. 保证等级：**
   - **At-most-once**：send_input 通过 node state guard + recovery dedup（可检测的重复）
   - **At-least-once**：recovery 重发 pending 节点的 send_input（幂等由 agent 端 tolerate）
   - **Exactly-once node execution**：不保证。重复 task 创建是可能的（已在 recovery 中清理孤儿）
   - **Graceful degradation**：重复 node execution 被日志记录，但不会被静默忽略

**V1.1 补强的接口（排序优先级）：**

| 优先级 | 接口 | 解决 |
|--------|------|------|
| 1 | `AgentControl.spawn_task(idempotency_key=...)` | 完全消除重复 task 创建 |
| 2 | `TaskStore.get_task_by_idempotency_key(key)` | 支撑 #1 |
| 3 | `AgentControl.get_run_history(task_id, since_run_count)` | 引擎获取历史 attempt 输出 |

**Z 方案的优势：**
- 零 API 变更（V1 内）
- 恢复逻辑明确可测试（scan + match + clean orphan）
- 保持 GatewayTaskModule ↔ AgentControl 模块边界完全不变
- V1.1 的 idempotency_key 是自然增量

**Z 方案的劣势：**
- 孤儿 task 清理是 best-effort（依赖恢复时的 agent_tasks 扫描）
- 重复 task 可能在 crash 和 recovery 之间短暂存在
- 恢复匹配（orphan→node）是启发式的，不是确定性的

---

## 总结

| 议题 | 裁决 |
|------|------|
| 问题 1（边界） | **ACCEPT 对方**：不可绕行 GatewayTaskModule/AgentControl |
| 问题 2（envelope） | **DISAGREE 对方**：引擎侧由 `(task_id, run_count)→workflow_node_runs` join 做 correlation，比模型回传更可靠 |
| 问题 3（幂等 create） | **ACCEPT 问题存在**，选择 **Z 方案**：V1 不修改 AgentControl，通过 recovery 孤儿扫描 + V1.1 补 idempotency_key |
