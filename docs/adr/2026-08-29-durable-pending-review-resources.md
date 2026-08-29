# ADR: Durable Pending Review Resources

**Status:** accepted
**Date:** 2026-08-29

## Context

Human review state was previously represented only by
`TaskRecord.pending_review`. A child Task copied that payload onto its root Task
so a Channel Session bound to the root could expose the review. Because the root
field held only one payload, a second child review overwrote the first. Clearing
either mirror could then hide another child that was still
`waiting_for_human`. Enumeration and restart recovery depended on whichever
payload happened to be mirrored last.

The review state also had to change with its owner Task. Persisting those facts
through separate calls would leave a crash window in which a Task and its
review disagreed.

## Decision

Introduce **Pending Review** as an authoritative durable resource in
`TaskStore`. The `agent_task_pending_reviews` table stores:

- the globally unique `review_id`;
- the owning `task_id`;
- the delegation `root_task_id`;
- the review payload and creation/update timestamps; and
- a locally assigned, monotonically increasing `ingest_sequence` used only for
  stable public enumeration.

`task_id` is unique in this table: one Gateway Task can own at most one Pending
Review at a time. A delegation root can contain many Pending Reviews owned by
different Tasks. Public review list/get operations and Review Command ownership
checks read this resource collection rather than scanning
`TaskRecord.pending_review`.

`TaskRecord.pending_review` remains only as a compatibility projection:

- an owner Task keeps the payload expected by existing single-review Task
  clients;
- a root Task projects the earliest Pending Review in stable
  `(created_at, review_id)` order;
- a child projection adds `source_task_id`;
- resolving one review advances the projection to the next item, if any.

The projection is not authoritative. Review discovery and decisions remain
correct even if a legacy projection must be rebuilt.

## Atomicity and Process-Local State

Creating, replacing, or resolving a Pending Review uses the same `TaskStore`
SQLite transaction for:

- the owner `TaskRecord` lifecycle transition;
- insertion or deletion of the Pending Review row;
- a changed root compatibility projection; and
- the corresponding durable Task lifecycle events.

No second database participates in this correctness boundary. Before the
transaction, `TaskManager` snapshots the mutable owner/root records, its
Pending Review cache, and relevant live-run registry entries. If persistence
fails, those objects are restored in place. A failed Review Command therefore
leaves the owner `waiting_for_human`, preserves the root projection and Pending
Review, exposes no new live run, and can be retried with the same `review_id`.

`ReviewAuditStore` remains a separate, non-authoritative audit log. An extreme
crash may omit an audit event, but audit persistence cannot create, hide, or
resolve a Pending Review and is not part of the atomic state transition.

The same `BEGIN IMMEDIATE` transaction increments a singleton ingest high-water
mark and inserts a new Pending Review. Replacing the same `review_id` preserves
its sequence. Deleting the newest review does not lower the high-water mark, so
a later review cannot enter an older pagination snapshot by reusing a sequence.

## Recovery and Upgrade

Pending Review rows are loaded independently from Task mirror fields on process
restart. When opening an older Task database, `TaskStore` backfills every
`waiting_for_human` Task with a valid legacy `pending_review_json`, then rebuilds
each root projection from the complete durable set. Stale child projections on
settled roots are cleared when no authoritative resource remains.

The upgrade also assigns deterministic ingest sequences to legacy rows in
`(created_at, review_id)` order and persists the maximum in the singleton
high-water record. Opening a partially migrated database repeats that repair
idempotently before installing the unique sequence index.

New review-list cursors use the local ingest high-water as an immutable snapshot
frontier and the first unscanned review's identity/sequence as the resume point.
They therefore do not depend on mutable remote timestamps. Legacy cursor
versions remain readable; after locating their exact resume item (or applying
their strict timestamp fallback when it disappeared), continuation switches to
the ingest-sequence snapshot format.

Local and `remote_ref` Tasks use the same resource model. Remote refresh and
Review Command responses reconcile the local proxy Task and its Pending Review
in the same local transaction; the downstream HTTP request is still outside
that transaction.

## Consequences

- Sibling Tasks can wait for human review concurrently and be decided in any
  order through their root Gateway Task.
- Review list/get/task-list operations survive restart without relying on the
  last mirrored payload.
- Existing Task and Channel clients retain a single `pending_review` view, but
  must use the review collection to display every concurrent item.
- The current SQLite store remains a single-process ownership boundary. A
  future multi-process writer design requires transaction and cache ownership
  to be revisited.

## Verification

Tests cover two sibling reviews in both decision orders, root-scoped public
list/get/decision operations, process restart, legacy database backfill,
single-review and remote compatibility, root projection advancement, injected
failures during review creation and decision, in-memory rollback, absence of a
live run after failed decision, and successful retry with the same review. They
also cover idempotent sequence migration, high-water persistence after deletion
and restart, backdated reviews discovered after a snapshot, transient owner
failures, and legacy-cursor recovery without pagination gaps or duplicates.
