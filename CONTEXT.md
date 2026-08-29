# Ruyi Agent Context

Ruyi Agent is an agent runtime for multi-channel task execution, delegation, review, and recovery. This context records the project language used to discuss runtime behavior and channel architecture.

## Language

**Channel Turn**:
One inbound user interaction from a channel, interpreted against the current channel session and routed to a gateway task or review decision.
_Avoid_: message handler, adapter flow, chat loop

**Channel Adapter**:
A platform-specific adapter that receives channel messages and sends channel responses without owning cross-platform turn policy.
_Avoid_: bot service, platform service

**Channel Session**:
The persisted relationship between a channel identity and the active agent or current gateway task.
_Avoid_: chat state, conversation cache

**Channel Turn Receipt**:
A durable record that binds a platform event idempotency key to the Gateway Task
request fingerprint, the mutation selected for that Channel Turn, and its
response. It lets a retried platform event replay the original outcome without
selecting create versus continue again from mutable Channel Session or Task
state, while rejecting reuse for different input.
_Avoid_: event cache, exactly-once message

**Gateway Task**:
A long-lived agent session created through the Gateway control plane. It keeps a stable task/thread identity across multiple runs and can later be continued, reviewed, interrupted, or queried by channel turns. Its state describes the current or latest run; a settled run does not close the task.
_Avoid_: job, request

**Gateway Task Module**:
The transport-neutral module that exposes public Agent discovery and Gateway Task creation, continuation, review, cancellation, querying, artifact retrieval, and remote routing. HTTP and Channel code reach Gateway Tasks through adapters and do not own task policy.
_Avoid_: HTTP service, gateway controller

**Gateway Command**:
A durable, idempotent mutation intent accepted by the Gateway Task Module. It
binds one authenticated principal and external idempotency key to one canonical
create-or-continue request, reserves stable effect identities, and stores the
successful Gateway Task response for replay.
_Avoid_: request cache, exactly-once run

**Task Message Transcript**:
An oldest-first, snapshot-consistent public textual projection of the canonical
user, assistant, and tool messages in one durable Gateway Task checkpoint. It
preserves tool-call linkage while excluding system prompts, hidden reasoning,
media blocks, and provider metadata. It is conversation state, not an immutable
request or audit log.
_Avoid_: raw message dump, event log, run history

**Task Event Stream**:
An authenticated, fixed-run SSE view of one Gateway Task. It starts from a
current snapshot or resumes from an opaque durable lifecycle cursor, may carry
best-effort public assistant text deltas while the run is live, and ends when
that run settles, requests review, or is superseded.
_Avoid_: raw model stream, complete message history, workflow event bus

**Task Router**:
The internal Gateway Task module that owns local/remote route persistence, runtime-record recovery, remote refresh, routed task operations, and routing error translation. It does not build transport responses or process attachment contents.
_Avoid_: route helper, remote task utility

**Review Command**:
A channel command that resolves one Pending Review by `review_id`. It may be
addressed through the owning Gateway Task or its delegation root.
_Avoid_: approval message, HITL reply

**Pending Review**:
An independently identifiable, durable request for human review owned by one
Gateway Task and grouped under its delegation root. It is authoritative review
state; a root Task's single `pending_review` field is only the earliest-item
compatibility projection.
_Avoid_: root review mirror, approval slot

**Task Watch**:
The shared policy for observing one gateway task run after a channel turn until it reaches a pending review, is superseded by a newer run, or settles.
_Avoid_: polling loop, watcher task

**Task Mailbox**:
The durable inbox for inputs addressed to a Gateway Task. It accepts user,
agent, workflow, and delegated-run-settled inputs, injects them before a safe
model call, and wakes a resumable task when a triggering input would otherwise
remain unread.
_Avoid_: event bus, workflow queue

**Active Agent**:
The agent selected for future channel turns within a channel identity.
_Avoid_: default bot, current worker

## Relationships

- A **Channel Adapter** produces **Channel Turns**.
- A **Channel Turn** reads and updates one **Channel Session**.
- A successful idempotent **Channel Turn** records a **Channel Turn Receipt**
  atomically with its **Channel Session** update.
- A **Channel Session** may point to one current **Gateway Task**.
- A **Channel Adapter** reaches a **Gateway Task** through the **Gateway Task Module**.
- The **Gateway Task Module** delegates execution and task state transitions to `AgentControl`.
- A **Gateway Command** makes Gateway Task creation or continuation safely
  retryable without claiming exactly-once model or tool side effects.
- A **Task Message Transcript** projects one durable checkpoint of a **Gateway
  Task** and is independently paged from the latest Task status.
- The **Gateway Task Module** delegates local and remote routing policy to the **Task Router**.
- A **Task Event Stream** observes one run without owning or cancelling its
  execution; durable lifecycle recovery and complete conversation recovery are
  separate from transient text deltas.
- A **Gateway Task** owns at most one **Pending Review**, while its delegation
  root may group Pending Reviews from multiple descendant Tasks.
- A **Review Command** resolves one **Pending Review** through its owner or
  delegation root without clearing unrelated Pending Reviews.
- A **Task Watch** observes one run of one **Gateway Task**.
- A **Task Mailbox** delivers input to one **Gateway Task** without defining
  workflow dependencies or collaboration policy.
- An **Active Agent** determines which agent receives a new **Gateway Task** when a **Channel Turn** starts a fresh task.

## Example Dialogue

> **Dev:** "Should Telegram own `/resume` differently from Feishu?"
> **Domain expert:** "No. `/resume` is Channel Turn policy; each Channel Adapter should only expose it through its platform message format."

## Flagged Ambiguities

- "message handler" has been used for both platform receive/send code and cross-platform turn policy. Resolved: use **Channel Adapter** for platform code and **Channel Turn** for shared policy.
