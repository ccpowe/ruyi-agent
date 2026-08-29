# ADR: Durable Channel Task Watch and Delivery

**Status:** accepted
**Date:** 2026-08-30

## Context

Channel Task Watches used to be process-local background tasks. A transient
Gateway query error stopped a watch, adapter restart forgot every active watch,
and terminal delivery deduplication lived only in memory. Feishu also treated a
query failure like a failed Gateway Task. The public media limits did not bound
the HTTP response bodies that the adapters buffered.

## Decision

Store one durable delivery intent per `(platform, Channel Session, Gateway
Task, run_count)` in the Channel Session SQLite database. An intent records its
platform/chat destination, delivery kind and state, retry cursor, last error,
and a fenced lease. Adapter startup enumerates recoverable intents, reconciles
each Task through the Gateway, and resumes the shared Task Watch state machine.

The state progression is:

```text
watching -> retry_wait -> watching
watching -> delivering(review|terminal) -> delivered
watching -> error                         (recoverable on restart)
watching -> superseded                    (final)
```

Only one owner may claim a live intent. Every state and delivery-step write is
conditioned on its lease token, so an expired owner cannot acknowledge work
after a newer owner takes over. Shutdown stops new watches, cancels and gathers
all owned watcher tasks, releases leases, and only then lets runners close the
SQLite stores and platform clients.

Gateway query failures are distinct from Gateway Task terminal failures.
Network errors, timeouts, HTTP 408/429 and 5xx responses use bounded exponential
backoff with jitter. A query failure never emits a terminal Task message or a
failure reaction. Non-retryable errors and exhausted retries leave a durable
recoverable `error` intent instead of an unobserved task exception.

Review messages, terminal messages, and each terminal artifact have stable
step keys. A retry skips every step already acknowledged in the ledger, so a
partial artifact failure does not resend the terminal message or earlier
artifacts. A new `run_count` and a new review identity get independent keys.

Telegram and Feishu APIs do not expose an idempotency key for these send
operations. Consequently, a process crash after a platform accepts a send but
before its step commit can still produce a duplicate on recovery. The visible
`task_id`, `run_count`/review identity, stable artifact identity, fenced claim,
and per-step ledger are the explicit deduplication boundary; this ADR does not
claim impossible platform exactly-once delivery.

Both Channel runners pass their configured `media_max_bytes` to inbound
platform downloads and Gateway artifact downloads. Downloads validate the
single `Content-Length` value when present and always enforce the limit while
streaming, buffering at most `limit + 1` bytes. Missing or forged-short lengths
therefore cannot bypass the hard cap. The unused `media_root` setting is
removed.

## Consequences

- Adapter restart restores active Telegram and Feishu watches before receiving
  new events.
- A temporary Gateway outage no longer converts a healthy Task into a failed
  Channel presentation.
- Platform delivery is durable and step-idempotent within the documented API
  boundary, but remains at-least-once across the unavoidable send/commit crash
  window.
- Multiple processes may share the Channel database for claims, although
  SQLite remains the deployment and throughput boundary.
- Platform adapters retain rendering and transport hooks; retry, watch,
  recovery, lease, and delivery progression are shared policy.

## Verification

Tests cover transient retry and exhaustion, restart recovery for both adapters,
concurrent fenced claims, duplicate review/terminal observations, partial
artifact failure, idempotent close, and missing, invalid, repeated,
forged-short, and oversized streamed media bodies.
