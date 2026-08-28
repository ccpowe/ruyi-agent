# ADR: Durable Fixed-Run Gateway Task Event Stream

**Status:** accepted
**Date:** 2026-08-28

## Context

Gateway clients could poll `TaskResponse.last_result`, but could not observe a
running Task incrementally. Polling loses intermediate text, adds latency, and
cannot distinguish a missed transition from an unchanged Task. The existing
Task Message Transcript provides complete checkpoint recovery after a run, but
it is not a live event channel.

A Gateway Task is a long-lived session whose `run_count` advances on each
input. A reconnectable stream therefore cannot silently switch from one run to
another. It also must not own graph execution: disconnecting an HTTP client
must never cancel a LangGraph run or leave its checkpoint incomplete.

## Decision

Add the Bearer-authenticated endpoint:

```http
GET /tasks/{task_id}/events?run_count=<non-negative integer>
Last-Event-ID: <opaque cursor>  # optional reconnect
Accept: text/event-stream
```

The required `run_count` fixes the stream to one Task run. A fresh request is
accepted only for the currently persisted run and begins with `task.snapshot`.
A cursor reconnect may replay a prior run's remaining durable events and then
ends as `superseded`. A future or cursorless non-current run returns
`409 task_run_mismatch`.

The stream sends a comment heartbeat every 15 seconds and uses
`Cache-Control: no-cache, no-transform` plus `X-Accel-Buffering: no`. It ends
with an unnumbered `stream.end`. Review takes precedence over terminal status
when selecting the end reason. An error after response headers have been sent
is represented by an unnumbered `stream.error`, followed by
`stream.end(reason=error)`.

The server owns and closes the opened Task stream context from the response's
outer ASGI lifetime. Cleanup therefore also runs if sending response headers
fails, an older ASGI client disconnects before the body is first iterated, or a
body send fails. Closing an observer never cancels the Agent run.

## Event Model and Recovery

Durable public lifecycle types are:

- `task.created`, `task.running`, and `task.review_requested`;
- `task.completed`, `task.failed`, `task.cancelled`, and `task.interrupted`;
- `task.artifact_published`.

`TaskStore` keeps these events in an ordered SQLite ledger. A Task state write
and its lifecycle event commit in one transaction; notification happens only
after commit. Notifications are wakeups, not truth: subscribers also tail the
ledger to repair a lost same-process wakeup. Internal mailbox delivery flags do
not produce public events. Repeated remote status refreshes produce no event
unless the public status, result, error, run, review, or artifact projection
changes.

The opaque base64url cursor binds a version, Task ID, run count, and real ledger
event ID. It is validated for size, schema, binding, and existence. A fresh
snapshot uses the current run's durable high-water cursor, so a transition that
races subscription registration is either represented in the snapshot or
replayed afterward. A pre-ledger Task receives one idempotent reconciliation
anchor marked `reconciled: true` with `observed_at`; no historical transition
time is invented.

Public durable projections are bounded by their actual UTF-8 JSON byte size
before persistence, leaving transport headroom below the SSE record limit.
Oversized result, error, review, and artifact projections are reduced
deterministically and advertise the corresponding `*_truncated: true` flag.
This prevents a durable event that can neither be delivered nor passed during
cursor replay.

V1 has no event pruning or retention window. It assumes one runtime owner for a
Task database; cross-process notification and multi-writer ownership require a
separate coordination design.

## Live Assistant Text

Local graph execution consumes
`astream(stream_mode=["messages", "values"], version="v2")` to completion.
This retains the same checkpoint completion behavior as `ainvoke` while
allowing public `AIMessageChunk` text to be projected as `assistant.delta`.
Only string content and explicit `text`/`output_text` blocks are accepted.
Reasoning, tool-call chunks, tool results, provider metadata, and arbitrary
content blocks are excluded.

Projection additionally requires the complete LangGraph v2 top-level model
provenance tuple: `ns == ()`, `langgraph_node == "model"`, and
`langgraph_path == ("__pregel_pull", "model")`. Tool code can override public
metadata such as the node name, while nested Agent/subgraph calls use a
different namespace or push path; neither satisfies the complete tuple. This
boundary deliberately fails closed if LangGraph changes its provenance shape.

`assistant.delta` is transient, has no SSE ID, is not stored, and each projected
chunk has a bounded text size. A slow client may lose deltas without blocking
the Agent run. Durable terminal state and the Task Message Transcript remain
the recovery mechanisms. If an Agent has no callable `astream`, runtime
compatibility falls back to `ainvoke`; once a stream starts, failure never
invokes the graph a second time. If a completed stream contains no values part,
the persisted graph state is read instead.

The first release intentionally omits tool-call/result events, full state
updates, and a separate `assistant.message` event. Clients needing the complete
conversation use `/tasks/{task_id}/messages` after or during recovery.

## Remote Routing

A remote-ref stream is opened through a dedicated streaming A2A client
boundary. The downstream `Last-Event-ID` is forwarded byte-for-byte and remains
owned by the downstream Gateway. Every downstream event is size-bounded and
validated against a strict public field schema; the top-level downstream Task
ID must match the expected `upstream_task_id` and is rewritten to the local
proxy Task ID. Paths, reasoning, provider metadata, and unknown fields are not
forwarded.

The proxy also validates sequence, not only individual records. A fresh stream
has exactly one leading snapshot; a cursor resume has none. A downstream
`stream.error` is buffered until an immediately following
`stream.end(reason=error)` validates the pair. An orphaned or contradictory
pair is not partially exposed; the outer Gateway emits its single generic
in-band error/end pair instead.

Readers implement the WHATWG UTF-8-only CR/LF/CRLF framing directly, including
the optional initial BOM, so invalid UTF-8 and an oversized unterminated line
fail before an HTTP library can replacement-decode or buffer them without a
limit. A record is dispatched only after its blank-line delimiter, and a clean
HTTP EOF is successful only after `stream.end`. JSON is finite-number-only and
depth-bounded. Encoders apply the same UTF-8 line and complete-record budgets as
decoders, including SSE field prefixes. Provider APIs commonly allow future
unknown event types, but this Gateway-to-Gateway boundary intentionally fails
closed because it is also a public-data sanitizer; adding an event type
requires updating the shared schema.

Streaming clients request `Accept-Encoding: identity`, reject transformed
responses, accept exactly HTTP 200 as a stream, and leave the established read
timeout open for legitimate long-running Tasks. The pre-header handshake still
has a wall-clock deadline. Non-200 bodies retain a finite read deadline and a
64 KiB raw-byte cap before strict UTF-8/JSON decoding, so an error peer cannot
turn the handshake into an unbounded stream.

Lifecycle event names are bound to their unambiguous statuses. A
`task.review_requested` instead requires a non-empty public review projection:
a child review may be mirrored onto a root Task that remains `running` (or has
already settled), so forcing that event to `waiting_for_human` would reject a
valid two-Gateway stream.

Downstream `400 invalid_request` and `409 task_run_mismatch` remain client
errors. Network/auth failures, missing or old downstream endpoints, wrong
content types, malformed SSE, wrong Task/run bindings, and invalid payloads are
`502 upstream_gateway_error`. Once the outer response has started, these become
the in-band error/end pair described above.

## Boundaries and Consequences

The stream is additive. `TaskResponse`, `GatewayTaskClient`, Task Mailbox, and
Task Watch are unchanged. Task Watch retains its existing polling and review
grace semantics; migrating it is a separate task.

Native browser `EventSource` cannot attach the required Authorization header.
Browser integrations must use `fetch` with a readable response body (or an
authenticated same-origin backend); the Bearer token is never accepted in the
query string.

## Verification

Tests cover atomic ledger writes, cursor binding, snapshot/replay races,
reconciliation, restart interruption, transient-delta backpressure, hidden data
exclusion, complete `astream` consumption, authentication and pre-header
errors, direct-ASGI disconnect cleanup, bounded and malformed peer responses,
real nested/tool model token exclusion, external client parsing, downstream
schema enforcement, cursor replay, real loopback streaming before completion,
and a two-Gateway proxy flow.

## Protocol References

- [WHATWG Server-sent events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
  defines UTF-8 framing, named events, comments, and `Last-Event-ID` reconnects.
- [OpenAI streaming events](https://platform.openai.com/docs/api-reference/responses-streaming)
  demonstrate typed lifecycle and delta events for AI responses.
- [Claude streaming messages](https://platform.claude.com/docs/en/build-with-claude/streaming)
  demonstrate named delta, ping, error, and terminal events.
- [NGINX proxy buffering](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_buffering)
  defines response-controlled buffering through `X-Accel-Buffering`.
