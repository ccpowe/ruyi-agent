# ADR: Durable Idempotency for Gateway Task Mutations

**Status:** accepted
**Date:** 2026-08-28

## Context

`POST /agents/{agent_name}/tasks` and `POST /tasks/{task_id}/input` previously
had no public idempotency contract. A client that lost a response could not tell
whether the Gateway had committed the request. Retrying create could allocate a
second Gateway Task, and retrying input could start a second run.

The affected records do not share one transaction boundary. Gateway Tasks and
Task Mailbox messages use the task SQLite database through separate
connections, Gateway routes use a separate database, and a `remote_ref` effect
is an HTTP request to another Gateway. Therefore a cross-store atomic
"exactly-once" transaction is not available.

## Decision

Introduce a durable **Gateway Command** ledger and an optional public
`Idempotency-Key` header for the two mutation endpoints.

The key contract is:

- A key contains 1–255 visible ASCII characters with no whitespace.
- Key uniqueness is scoped to the authenticated principal. The current Gateway
  has one global Bearer principal, so keys are effectively Gateway-wide and are
  not partitioned by operation, agent, or task.
- The canonical request hash includes the operation, target, input,
  attachments, metadata, and webhook data that apply to that operation.
- Reusing a key for the same canonical request returns the saved status contract
  (`201` for create, `202` for input) and response body, with
  `Idempotency-Replayed: true`.
- Reusing a key for a different operation, target, or body returns
  `409 idempotency_key_reused`.
- Omitting the header preserves the existing non-idempotent API behavior.
- Idempotent local input requires a `MailboxStore`-backed Task Mailbox. A custom
  embedding without one returns `503 idempotency_unavailable` instead of
  claiming a guarantee it cannot recover across crashes. The standard runtime
  bootstrap always configures this store.

`GatewayCommandStore` reserves stable effect identities before execution:

- create reserves a random `task_id`;
- local input reserves a command-specific Task Mailbox `message_id` and
  `idempotency_key`;
- remote create/input forwards a stable key to the downstream Ruyi Gateway.
  Create replay is enabled only when the typed `remote_ref` declaration sets
  `create_idempotency = "ruyi_gateway_v1"`; omission means `"none"` and is
  deliberately conservative. The A2A client verifies that a declared-capable
  create actually carries a key.

Execution is a saga rather than a distributed transaction. If a process fails
after an effect but before saving the command response, local effects and
declared-capable remote creates can drive the same stable identity again. Task
creation rejects duplicate identity inserts, route persistence rejects identity
rebinding, Task Mailbox publication deduplicates the reserved message, and an
official Ruyi Gateway replays the forwarded key. An unknown or explicitly
non-capable remote create is never replayed after its effect boundary: request
cancellation and process recovery durably terminalize the command as
`idempotency_outcome_uncertain`, while retaining only the public Gateway Task
identity and route state. A command release cannot turn such an effect-started
claim back into executable `pending` state.

The official Gateway HTTP client accepts idempotency keys. Telegram uses its
`update_id`, and Feishu uses its `event_id` (falling back to `message_id`) as the
stable Channel Turn key. A successful ordinary Channel Turn stores a durable
receipt containing the selected operation, target Task, and Gateway response in
the Channel Session database, together with a canonical hash of the original
Inbound Turn fields. The receipt and Channel Session binding commit in one
transaction. A platform-event retry first verifies that hash and then consults
the receipt before mutable Task state, so a previously successful create cannot
drift into a continuation after the Task settles but before the platform event
is marked processed. Reusing one event key for different Channel input is
rejected instead of replaying a stale result.

## Guarantee Boundary

For a valid key and canonical request, the Gateway provides:

- at most one Gateway Task identity for create;
- at most one local Task Mailbox input, or one idempotent downstream Gateway
  command, for send input;
- a stable saved HTTP task response after the command succeeds.

This does **not** claim exactly-once model calls, tool calls, webhook delivery,
or arbitrary effects performed inside an Agent run. A process crash can leave a
Task interrupted even though its identity was created only once. Remote
at-most-once behavior requires the downstream service to honor the forwarded
idempotency contract and the operator to declare the verified
`ruyi_gateway_v1` capability. Unknown remotes default to no replay. Public error
metadata never exposes the downstream Task identity.

## Recovery and Deployment Constraint

On startup, the command store changes replay-safe claims left in `processing`
back to `pending`. A non-replay-safe claim whose durable effect marker was set
becomes terminal uncertain instead. This is correct for the current
single-process owner of a Gateway database. Multiple live Gateway processes
must not share that SQLite command database; a future multi-replica design needs
leased claims or an external coordinator.

Successful command records are retained with the task database and have no
automatic expiration in this version. Clients must treat keys as non-reusable.

## Consequences

- The public mutation API is safely retryable across lost responses and process
  restarts while retaining backward compatibility.
- `TaskStore` creation and update are separate operations; creation no longer
  uses SQLite `INSERT OR REPLACE`.
- `GatewayRouteStore` can update metadata for an existing binding but cannot
  silently replace the task-to-agent/route/upstream binding.
- The command ledger adds durable storage growth and requires an explicit
  retention policy before automatic pruning is introduced.
- Channel Turn receipts are likewise retained without automatic expiration in
  this version; platform event idempotency keys must not be reused.
- There is still no cross-database atomic commit; correctness depends on stable,
  independently deduplicated effect identities.

## Verification

Tests cover concurrent identical requests, conflicting key reuse, restart claim
recovery, cancellation and failure after the effect boundary, durable Mailbox
deduplication, stable Task/route identity, capability validation and client
header propagation, conservative unknown-remotes, and a declared-capable
two-Gateway end-to-end flow that drops committed create and input responses.
They also cover replay after a Channel Session binding survives while the
platform processed marker does not, and compatibility with Channel clients that
do not accept the new optional keyword when no key is supplied.
