# ADR: Serialize SQLite store initialization before schema migration

- Status: Accepted
- Date: 2026-08-30

## Context

`TaskStore` and `GatewayCommandStore` enable WAL before running their idempotent
schema migrations. Separate constructors use separate SQLite connections, so a
burst of constructors can race while changing the journal mode. SQLite may
return `database is locked` from the WAL pragma before any constructor reaches
the `BEGIN IMMEDIATE` transaction that already serializes schema changes.

Repeated-test barriers can hide that first database exception: one worker exits
while its peers wait at the next barrier and eventually report
`BrokenBarrierError` instead of the storage failure.

## Decision

Store startup is one operation per canonical database identity:

1. acquire a process-local initialization lock;
2. configure the connection busy timeout, foreign keys where applicable, and
   WAL journal mode;
3. retry only SQLite `locked` failures from WAL negotiation within the existing
   30-second busy-timeout deadline; and
4. retain `BEGIN IMMEDIATE` for the complete idempotent schema migration.

`GatewayCommandStore` keeps interrupted-command recovery under the same startup
lock. Constructor failures close their connection while preserving the original
exception, even if cleanup itself fails.

The process-local lock matches the stores' existing single-process ownership
contract. The bounded WAL retry also tolerates a temporary lock held by a
different connection or process, but it is not a multi-process ownership
protocol.

## Consequences

- Concurrent constructors no longer race at the journal-mode transition.
- Schema rollback and retry retain their existing transaction boundary.
- Non-lock SQLite errors and application migration errors are not retried or
  replaced by coordination errors.
- Initialization locks are weakly retained by database identity, so opening
  many temporary database paths does not create an unbounded registry.

## Verification

Concurrency tests create a fresh exact legacy Task database and Gateway Command
database for each of 60 rounds, then run 12 complete constructors against that
database. They verify the migrated schema, backfills, recovery, uniqueness, and
integrity. Separate injected SQLite failures assert that the first worker's
real error is re-raised rather than a barrier error and that all 12 constructor
connections are closed.
