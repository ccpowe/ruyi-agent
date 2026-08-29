# ADR: Upgrade legacy remote Task public projections before notification delivery

- Status: Accepted
- Date: 2026-08-30

## Context

Before the remote trust boundary was enforced at ingestion, a persisted
`remote_ref` Task could retain a downstream Task ID in `thread_id`, a downstream
error message in `error`, or a downstream `source_task_id` in a Pending Review.
The same error could already have been copied into a durable lifecycle event,
settled outbox intent, or Task Mailbox message.

Sanitizing only new refreshes is insufficient. A restart can load the old
`TaskRecord`, and the settled notifier can claim an old outbox row before any
remote refresh occurs. Merely fixing the Task row also leaves the raw outbox
content inconsistent with the deterministic public intent, so reconciliation
either rejects it as an identity conflict or delivers the private content.

## Decision

Opening the Task database runs the idempotent
`remote_public_projection_v1` repair before repositories or the settled
notifier are made available. If the Task Mailbox table is created or opened
later, its initialization repeats the same repair before committing startup.

For every persisted `remote_ref` Task:

- `task_id` is the public Task and thread identity; `upstream_task_id` remains
  only the private routing binding.
- A non-null stored error becomes the static `Remote Gateway Task failed`.
- Pending Review `source_task_id` values in Task mirrors, authoritative review
  rows, and durable lifecycle events become the public Task ID.
- Public identity and error fields in durable lifecycle events are reprojected.
- Every not-yet-delivered settled outbox/mailbox projection is brought into
  agreement with the sanitized Task projection.

Claimed affected rows have their lease tokens cleared and return to `pending`.
This fences an intent object claimed before the upgrade: its stale token can no
longer commit a raw mailbox message. Suppressed/retracted rows remain
non-deliverable while their stored public projection is sanitized.

The repair deliberately runs on every open as well as recording a completed
migration row. This makes a partially upgraded database and differing
TaskStore/MailboxStore construction order converge to the same state.

## Consequences

- A baseline database is safe before notifier reconciliation or remote refresh.
- Reconciliation can adopt an existing mailbox row without public-intent
  identity conflicts.
- Previously delivered messages are historical delivery state and are not
  replayed or rewritten by this migration.
- Downstream errors remain available only to private diagnostics outside these
  public durable projections; they cannot reach Task responses, Task Event
  Streams, Task Mailboxes, settled outbox delivery, or caller webhooks after
  restart.
