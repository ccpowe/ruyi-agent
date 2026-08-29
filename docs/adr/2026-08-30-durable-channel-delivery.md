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
watching -> delivering(review)   -> review_waiting -> watching
watching -> delivering(terminal) -> terminal_grace -> delivered
watching -> error                         (recoverable on restart)
watching -> superseded                    (final)
```

A review step is keyed by `review_id`; acknowledging one review leaves the
intent recoverable so another mirrored descendant review for the same Task run
can reopen observation. Terminal message/artifact steps move the intent only to
`terminal_grace`. The watcher alone marks it final after the explicit grace
window, so a restart skips acknowledged terminal steps but can still deliver a
late review.

Only one owner may claim a live intent. Every state and delivery-step write is
conditioned on its lease token, so an expired owner cannot acknowledge work
after a newer owner takes over. External platform sends and uploads run under a
lease heartbeat; lease loss cancels the local await task and prevents that
owner from starting subsequent steps. It cannot retract an effect already
accepted by a remote platform. Shutdown stops new watches, cancels and gathers
all owned watcher tasks, releases leases, and only then lets runners close the
SQLite stores and platform clients. Startup recovery compensates a partially
started batch by cancelling its new watches and releasing their leases before
it may be retried.

Adapter startup is single-flight: concurrent `start()` callers await the same
recovery task and observe the same result. A failed recovery remains the sole
in-flight startup until its compensation completes. Once `close()` begins it
wins the lifecycle race, rejects new starts, cancels and awaits in-flight
startup, and then performs the ordered coordinator/store shutdown. Cancelling
one start caller does not cancel the shared recovery; close is the only owner
that does so.

Gateway query failures are distinct from Gateway Task terminal failures.
Network errors, timeouts, HTTP 408/429 and 5xx responses use bounded exponential
backoff with jitter. A query failure never emits a terminal Task message or a
failure reaction. Non-retryable errors and exhausted retries leave a durable
recoverable `error` intent instead of an unobserved task exception.
Snapshots whose `run_count` is lower than the watch are treated as stale and
polled again; a higher run supersedes the watch.

Review messages, terminal messages, and each terminal artifact have stable
step keys. A retry skips every step already acknowledged in the ledger, so a
partial artifact failure does not resend the terminal message or earlier
artifacts. A new `run_count` and a new review identity get independent keys.

Telegram and Feishu APIs do not expose an idempotency key for these send
operations. Every ambiguous remote-acceptance window is therefore
at-least-once: the process may crash after acceptance, the request may time
out, its caller may be cancelled, or lease loss may cancel the local await
after the platform has already committed the effect. None of those events can
retract the remote effect. Without a committed local step, a new fenced owner
must retry and may visibly duplicate the message or artifact. The visible
`task_id`, `run_count`/review identity, stable artifact identity, fenced claim,
and per-step ledger are the explicit deduplication boundary; this ADR does not
claim platform exactly-once delivery.

Both Channel runners pass their configured `media_max_bytes` to inbound
platform downloads and Gateway artifact downloads. Downloads validate the
single `Content-Length` value when present and always enforce the limit while
streaming, buffering at most `limit + 1` bytes. Missing or forged-short lengths
therefore cannot bypass the hard cap. The unused `media_root` setting is
removed from configuration and runtime wiring. Adapter constructors continue
to accept it as a deprecated, ignored keyword for source compatibility and
emit a warning when it is supplied.

## Consequences

- Adapter restart restores active Telegram and Feishu watches before receiving
  new events.
- Continuing a settled session waits for the same durable terminal ledger used
  by background watches; adapters do not race it with a direct presentation.
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

Tests cover transient retry and exhaustion, stale run projections, restart
during terminal grace, sequential and concurrent review reopens, atomic startup
recovery for both adapters, heartbeat/fence interleavings, settled-input races,
partial artifact failure, concurrent startup and start/close races, an
accepted-then-blocked send that is intentionally retried after lease loss,
idempotent close, and missing, invalid, repeated, forged-short, and oversized
streamed media bodies.
