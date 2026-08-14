# ADR: Minimal Durable Dynamic-Workflow Layer

**Status:** proposed
**Date:** 2026-08-12

## Context

Ruyi currently has:

- **Gateway Tasks** — long-lived independent agent sessions with stable `task_id`/`thread_id`, full lifecycle (pending → running → waiting_for_human → settled), and multi-run resumption via `send_input`. Stored in `TaskStore` (SQLite, `agent_tasks` table).
- **Task Mailbox** — durable SQLite-backed inbox (`MailboxStore`, `agent_mailbox_messages` table). Supports active-run safe insertion (claim/acknowledge with leases), `trigger_run` wake-up, parent-child bidirectional `send_input`, and idempotency keys.
- **Parent-child tree via parent_task_id** — `TaskRecord.parent_task_id` encodes a strict tree for delegation budget enforcement (`root_task_id`, `depth`, `max_tasks_per_root`). This is a *runtime control* relationship, not a workflow dependency DAG.

The ask is a workflow layer that supports a **persisted DAG** with parallel fan-out/fan-in, independent verification of each node, restart recovery, human review pause, and workflow-level cancellation — **without encoding DAG edges as `parent_task_id`**.

## Key Insight: Why parent_task_id Is Insufficient for Workflow Edges

`parent_task_id` today is used for three distinct concerns:

| Concern | Encoded via `parent_task_id` | Workflow DAG needs |
|---|---|---|
| Delegation budget (depth, task count) | Yes | No — budget should be per-workflow, not per-tree |
| Mailbox routing (settled result → parent thread) | Yes | No — fan-out/fan-in needs many-to-one edge semantics |
| Skill/permission inheritance | Yes | Should be per-node config, not parent-derived |

Mixing workflow DAG edges into `parent_task_id` would break the delegation budget model (fan-in creates a diamond, not a tree), confuse mailbox routing (which parent gets the notification?), and couple two orthogonal concerns.

## Design

### 1. Core Entities

Three new entities, minimal and discrete:

```
WorkflowDefinition
    workflow_id: str (UUID)
    name: str
    nodes: list[WorkflowNode]
    edges: list[WorkflowEdge]
    state: "draft" | "active" | "paused" | "cancelled" | "completed" | "failed"
    created_at, updated_at: datetime
    metadata: dict

WorkflowNode
    node_id: str  (UUID, stable within the workflow)
    agent_name: str  (maps to Gateway Task agent target)
    input_template: str | None  (optional templated input, resolved at runtime)
    input_mode: "static" | "fan_in_aggregate"
    verification: VerificationSpec | None
    review_policy: "never" | "always" | "on_risk"
    retry_policy: RetryPolicy
    timeout_seconds: int | None

WorkflowEdge
    edge_id: str (UUID)
    from_node_id: str
    to_node_id: str
    condition: "on_completed" | "on_failed" | "always"
    data_mapping: DataMappingSpec | None  (how to transform output → input)

WorkflowRun
    run_id: str (UUID)
    workflow_id: str (FK)
    state: "running" | "paused_for_review" | "cancelled" | "completed" | "failed"
    node_runs: dict[str, NodeRun]  (keyed by node_id)
    started_at, completed_at: datetime | None

NodeRun
    node_run_id: str (UUID)
    node_id: str
    task_id: str | None  (the Gateway Task that executes this node)
    state: "pending" | "running" | "waiting_for_human" | "completed" | "failed" | "verified" | "skipped"
    attempt: int
    input_snapshot: str
    output_snapshot: str | None
    error: str | None
    started_at, settled_at: datetime | None
```

### 2. Persistence

New SQLite table `workflow_definitions` (one row per workflow). Nodes and edges stored as JSON columns (workflows are read in full on activation; partial mutations are rare). This avoids join complexity for what is fundamentally a document.

New table `workflow_runs` with `node_runs_json` column — a single row per active run, atomically updatable. Node-run-level granularity via JSON patch within the row, or a separate `workflow_node_runs` table if per-node query becomes a requirement (start with the single-row design).

Both tables follow the existing `TaskStore` / `MailboxStore` pattern: `sqlite3.connect`, `check_same_thread=False`, `WAL`, thread-safe RLock.

### 3. Execution Model — WorkflowEngine

A new class `WorkflowEngine` that sits beside `AgentControl` in the runtime layer. It does NOT extend `AgentControl` or share its delegation tree. It is a **graph scheduler**, not an agent.

```
WorkflowEngine
    activate(workflow_id, input) → WorkflowRun
    resume_from_restart(run_id) → WorkflowRun
    cancel(run_id) → WorkflowRun
    submit_review_decision(run_id, node_id, decision) → NodeRun
```

**Activation flow:**

1. Load `WorkflowDefinition` from store.
2. Topological sort. Validate no cycles, all `agent_name` references resolve.
3. Create `WorkflowRun` with all `NodeRun` entries in `pending` state.
4. **Fan-out phase:** For every node whose inbound edges are all satisfied (initially, the source nodes), create a Gateway Task via `AgentControl.spawn_task` with a **workflow-owned `parent_task_id = None`**. The `task_id` is recorded in `NodeRun.task_id`.
   - The workflow engine creates tasks *independently*, not as children of any other task.
   - The engine stores the `run_id → task_id` mapping so it can correlate mailbox messages.
5. **Fan-in phase:** When a node's inbound nodes are all settled, the workflow engine checks the edge conditions. If all required incoming edges report `on_completed` and all source nodes completed, fan-in is triggered.
   - The engine **sends input** to the waiting node's task via `AgentControl.send_task_input`, aggregating outputs from all upstream nodes per the `input_mode` and `data_mapping` specs.
   - Alternatively, if the node was not yet spawned, the engine spawns it now with aggregated input.
6. **Verification step:** After a node settles `completed`, if `verification` is configured, the engine runs the verifier (a separate callable or a lightweight Gateway Task). Only on `verified` does the node transition trigger downstream fan-out. A failed verification transitions the node to `failed` and triggers `on_failed` edges.
7. **Completion:** When all terminal nodes (nodes with no outbound edges) are in a settled state, the run transitions to `completed` or `failed`.

### 4. Handling Each Requirement

#### Persisted DAG
- `workflow_definitions` table with DAG serialized as JSON. Loaded atomically on activation.
- DAG structure is immutable during a run (definition snapshot stored in the run).

#### Parallel Fan-Out
- The engine spawns independent Gateway Tasks for each ready node. No `parent_task_id` coupling.
- Uses `asyncio.gather` on `AgentControl.spawn_task` calls. Tasks run concurrently as independent sessions.

#### Parallel Fan-In
- The engine waits for all upstream nodes to settle. Implemented via a **mailbox watcher**: the engine subscribes to mailbox `settled` messages addressed to a synthetic `workflow_run_thread_id`.
- Each Gateway Task is configured with `parent_thread_id = f"wf-{run_id}"` so settled notifications route to the workflow engine's mailbox reader, not to a parent task.
- When all required upstream NodeRuns reach settled state, the engine evaluates edge conditions and proceeds.

#### Independent Verification
- `VerificationSpec` defines a verifier (could be a lightweight agent, a Pydantic schema check, or a function reference).
- Verification runs as a separate step after the node task settles `completed`. Node state transitions `completed → verified` (or `completed → failed` on verification failure).
- Verification failures are surfaced as node errors and can trigger retry or `on_failed` edges.

#### Restart Recovery
- On engine startup, `WorkflowEngine.recover()` scans `workflow_runs` for runs in non-terminal states.
- For each run, reconcile each `NodeRun` against `TaskStore`: if `task_id` exists but NodeRun state is stale, pull the current Gateway Task state.
- Re-evaluate which nodes are ready (fan-out) or waiting (fan-in) and resume scheduling.
- This is the same code path as normal execution — recovery is just re-driving the scheduler with persisted state.

#### Human Review Pause
- When a NodeRun's Gateway Task enters `waiting_for_human`, the engine detects this via mailbox/watch and transitions the **workflow run** to `paused_for_review` state.
- The review is surfaced through the existing `ReviewSnapshot` / `ReviewDecision` control plane contracts, with `task_id` pointing to the Gateway Task.
- On review resolution, the engine receives the decision, resumes that node's task via `send_input` or `AgentControl.resume` semantics, and the run continues.
- The workflow-level pause is critical: it prevents downstream nodes from being spawned while a mid-graph review is pending, since fan-in edges are not yet satisfied.

#### Workflow Cancellation
- `WorkflowEngine.cancel(run_id)` transitions the run to `cancelled`.
- For every NodeRun with an active Gateway Task (`state in ACTIVE_TASK_STATES`), call `AgentControl.cancel_task(task_id)`.
- The cancellation is cascading but fire-and-forget per node — individual task cancellations are best-effort; the run is marked cancelled regardless.
- Cancel propagation respects the existing `TaskRecord.cancel_requested` and `asyncio.Task.cancel()` patterns.

### 5. Why Not Encode DAG Edges as parent_task_id

| DAG scenario | parent_task_id behavior | Correct behavior |
|---|---|---|
| Fan-out (A→B, A→C) | B and C share same parent A. Works superficially. | Both B and C are independent, no parent semantic needed. |
| Fan-in (A→C, B→C) | C can only have ONE parent_task_id. This is the dealbreaker. | C must wait for both A and B, independent of any parent relationship. |
| Diamond (A→B, A→C, B→D, C→D) | D's parent_task_id must be either B or C, not both. Mailbox routing breaks. | D's edge set is {B, C}; no parent relationship. |
| Workflow node → multiple successors | parent_task_id is a single value, can't encode multiple children edges. | Edge table encodes all successor relationships. |

The existing `parent_task_id` tree is a *delegation control structure* (depth limits, budget enforcement, permission inheritance). The workflow DAG is a *dataflow dependency structure*. Conflating them creates a brittle single-inheritance model that cannot express fan-in.

## Constraints

1. **No changes to existing Gateway Task, AgentControl, or mailbox primitives.** The workflow layer is additive and consumes the existing public APIs.
2. **Single-process scheduling.** The `WorkflowEngine` runs within the same process as `AgentControl`. Cross-process workflow orchestration is deferred.
3. **Workflow definitions are static DAGs at run time.** Dynamic node insertion (a node spawning new nodes based on output) is excluded from v1. A node's agent can still delegate internally via `spawn_agent`, but the workflow graph does not change shape mid-run.
4. **Input/output data between nodes is passed as strings** (consistent with Gateway Task `input_content`/`result`). No binary artifact routing in v1.
5. **Verification is synchronous to the node's completion** — a node is not considered "done" for fan-in purposes until verified. This simplifies the fan-in condition to "all upstream nodes are `verified` or `completed` (if no verifier) or `failed` (if on_failed edge)".

## Rejected Alternatives

### A. Encode DAG edges in a new `depends_on` field on TaskRecord

Rejected because it ties the workflow scheduler to the task lifecycle. TaskRecords are managed by `TaskManager` which owns the state machine; adding graph evaluation logic there creates a circular dependency and makes task lifecycle transitions visible to the scheduler in ways that are hard to test independently.

### B. Use LangGraph for workflow orchestration

Rejected because LangGraph's StateGraph is designed for agent-internal reasoning loops, not for orchestrating independent long-lived Gateway Tasks across multiple process lifetimes. The checkpoint model assumes a single graph execution, not a set of independently recoverable tasks with their own lifecycles. Overloading LangGraph for this purpose would require reimplementing most of what it provides on top of its own abstractions.

### C. Store node_runs as separate rows (fully normalized)

Rejected for v1 simplicity. A single JSON column keeps the query surface minimal. Normalization can be introduced later if per-node query patterns emerge (e.g., "find all runs where node X failed in the last hour"). The JSON approach is consistent with how `pending_review_json`, `webhook_json`, and `artifacts_json` are stored in the existing schema.

### D. Make the workflow engine a Gateway Task itself

Rejected because this creates a bootstrapping problem: who spawns the workflow task? The workflow engine is infrastructure, not an agent. Making it a task would also couple its lifecycle to the task state machine, making restart recovery harder (you'd need to resume the workflow-task to resume the workflow).

### E. Use parent_task_id for edges, but allow multiple parents

Rejected because `parent_task_id` is a single-value column used by delegation budget enforcement (depth calculation, `root_task_id` propagation). Making it a list breaks the existing budget model and the mailbox routing (`parent_thread_id` derivation from `parent_task_id`). The delegation tree and the workflow DAG are different graphs that happen to sometimes overlap.

## Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Node-run JSON column grows too large for complex workflows (100+ nodes) | Medium | Medium | Cap workflow size at 50 nodes in v1. Add `workflow_node_runs` table in v2 if needed. |
| Mailbox-based fan-in watcher is racy under high concurrency | Low | High | Use `asyncio.Lock` per run_id. Claim mailbox messages atomically. Re-evaluate ready nodes after every state transition, not just on message receipt. |
| Verification step adds latency to fan-in chains | High | Low | Verification is bounded by a timeout per node. Default timeout: 30s. Verification failures are rare in practice; most nodes skip verification. |
| Restart recovery misses in-flight task state changes | Medium | Medium | On recovery, always pull latest TaskRecord from TaskStore before making scheduling decisions. Use `updated_at` timestamps for staleness detection. |
| Workflow definition schema evolution breaks persisted runs | Low | High | Version the definition and snapshot it into the run at activation time. Restart always uses the snapshot. |
| Two workflow runs of same definition interfering | Low | Low | Each run is independent. Definition is read-only during execution. |

## Migration Steps

1. **Add `workflow_definitions` table** to TaskStore's schema initialization. Follow existing `_ensure_column` pattern. No data migration needed — new table.
2. **Add `workflow_runs` table** with `node_runs_json` column. Same pattern.
3. **Implement `WorkflowEngine`** in `src/ruyi_agent/runtime/workflow/`:
   - `engine.py` — activation, scheduling loop, cancellation, recovery
   - `models.py` — dataclasses for WorkflowDefinition, WorkflowRun, NodeRun, etc.
   - `verification.py` — verification runner
4. **Wire `WorkflowEngine` into `AgentControl.__init__`** as an optional dependency, similar to how `mailbox` and `review_audit_store` are injected.
5. **Add workflow tools** to `AgentControl.build_tools()`: `activate_workflow`, `cancel_workflow`, `check_workflow`. These are scoped by permission policy.
6. **Add restart recovery** hook in the runtime bootstrap: after `AgentControl` is constructed and before the first turn is processed, call `engine.recover()`.
7. **Add integration tests** for the full lifecycle: define → activate → fan-out → fan-in → verify → complete, plus cancel mid-flight and restart recovery.

## Validation Criteria

1. **Fan-out correctness:** A 2-node parallel fan-out workflow produces two independent Gateway Tasks, both with `parent_task_id = None`, and both execute concurrently.
2. **Fan-in correctness:** A diamond workflow (A→B, A→C, B→D, C→D) only spawns D after both B and C settle.
3. **Verification:** A node with verification enabled transitions `completed → verified` only after the verifier passes. A failed verification leaves the node in `failed` state.
4. **Restart recovery:** Kill the process mid-workflow. On restart, `WorkflowEngine.recover()` restores the run to the correct state and continues scheduling from where it left off.
5. **Human review pause:** A node entering `waiting_for_human` pauses the workflow run. Downstream nodes are not spawned until the review is resolved.
6. **Cancellation:** `cancel(run_id)` cancels all active Gateway Tasks in the run and marks the run `cancelled`.
7. **No parent_task_id contamination:** Throughout the lifecycle, no workflow-induced `parent_task_id` is ever set on any Gateway Task. Delegation trees remain independent of workflow edges.
8. **Idempotent activation:** Activating the same workflow definition twice produces two independent runs with no shared state.
