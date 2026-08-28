# ADR: Snapshot-Consistent Gateway Task Message Transcript

**Status:** accepted
**Date:** 2026-08-28

## Context

Gateway clients could query only a Task's latest status and `last_result`. A
long-lived Task may span many inputs, assistant replies, and tool calls, but no
public interface exposed that conversation. Adding all messages to
`TaskResponse` would enlarge every status poll and remote status refresh.

Local message state already exists in `RuyiAgentState.messages` and is persisted
by the LangGraph checkpointer. `TaskStore` contains lifecycle state rather than
the transcript, while Task Mailbox is an input-delivery mechanism rather than a
history store. Remote-ref Tasks do not mirror downstream checkpoints locally.

## Decision

Add the authenticated endpoint:

```http
GET /tasks/{task_id}/messages?limit=20&cursor=<opaque>
```

It returns an oldest-first **Task Message Transcript** as
`{task_id, items, next_cursor}`. The transcript is a stable public textual
projection of canonical user, assistant, and tool messages. Public fields are:

- `sequence`, `message_id`, `role`, and textual `content`;
- assistant `tool_calls` with ID, name, and JSON arguments;
- tool result `tool_call_id`, optional name, and success/error status.

The projection accepts only explicit `text` and `output_text` content blocks.
System prompts, reasoning/thinking blocks, media blocks, response metadata, and
arbitrary additional metadata are excluded. A non-text assistant tool-call
message remains present with empty `content`. Tool arguments and tool-result
text are authorized task data and are not automatically redacted.

The API does not invent per-message timestamps or run numbers. Its `sequence`
is a zero-based position within a selected snapshot and may change in a later
snapshot. Native message IDs are retained; a missing ID receives a deterministic
fallback derived from the Task, normalized message fingerprint, and occurrence.

## Checkpoint and Cursor Semantics

Raw latest checkpoint channel values cannot reconstruct a `DeltaChannel`
history. Reading through a graph compiled with `RuyiAgentState` applies the
channel reducer correctly and does not require compiling the configured model,
tools, or middleware.

LangGraph's unpinned `aget_state` may apply pending writes. Therefore the first
request uses it only to locate the latest checkpoint ID, then reads that exact
checkpoint again. A local opaque base64url cursor binds version, Task ID,
checkpoint ID, and offset. Every later page reads the pinned checkpoint, so a
concurrent or later run cannot shift page boundaries.

A Task with no checkpoint returns an empty page. An invalid, cross-Task, stale,
or missing-checkpoint cursor returns `400 invalid_request`. Checkpointer or
state-reconstruction failure returns `503 task_history_unavailable`.

The cursor is opaque but not a signed capability. All decoded fields are
untrusted and validated; the fixed Task thread remains the checkpoint lookup
boundary.

## Remote Routing

For a remote-ref Task, Task Router requests the same endpoint from the
downstream Gateway using `upstream_task_id`. Downstream owns the cursor, and
every upstream layer forwards it byte-for-byte without decoding or rewriting
it. A valid downstream response is schema-checked, must name the expected
upstream Task, and is then returned with the local proxy Task ID.

A downstream `400 invalid_request` remains a client error. Network failures,
missing remote Tasks, old Gateways without this endpoint, and malformed payloads
are upstream failures (`502`). No layer substitutes `last_result` or an empty
page, because that would falsely claim the history is complete.

## Boundary and Consequences

This is the complete ordered **public textual transcript** in one canonical
conversation checkpoint. It is not a raw/full-fidelity LangChain message dump,
an immutable audit log, the original sequence of Gateway requests, or a stream
of token events. Middleware may repair or overwrite canonical conversation
state, and Mailbox delivery may aggregate inputs into one model-visible human
message.

`TaskResponse` and Channel client protocols remain unchanged. The concrete
Gateway HTTP client gains an opt-in page method. Supporting raw request audit,
per-message time, run attribution, or full media blocks would require a
separate append-only event/message design.

## Verification

Tests cover strict projection and hidden-data exclusion, deterministic IDs,
untrusted cursors, exact-checkpoint rereads, missing and unavailable states,
HTTP authentication/errors, real SQLite checkpoint recovery after restart,
tool-call/result linkage, remote payload validation, and a two-Gateway flow in
which a new run is appended between pages while the old cursor remains frozen.
