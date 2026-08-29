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

The identity follows SQLite URI filename semantics rather than the caller's raw
spelling. Ordinary and `file:` disk paths are resolved to the same absolute
filesystem path after URI percent decoding; empty and `localhost` authorities,
relative paths, dot segments, and ignored fragments therefore do not split the
lock. URI query pairs remain part of the identity because they can change the
target VFS or connection behavior. Distinct decoded keys are sorted, while the
final value of each repeated decoded key replaces its earlier values, matching
SQLite's effective URI parameter lookup. Thus an overridden value cannot split
the startup lock from an alias that supplies only the final value. This parsing
does not use HTML form rules: a literal `+` stays distinct from `%20`.

SQLite applies ordinary last-value selection to repeated `cache` and `vfs`
control parameters. `mode` has an additional connection-open validation step:
the sequence may only move toward equal or more restrictive access. For
example, `rwc` followed by `rw`, and `rw` followed by `ro`, open successfully
and expose the final value; the reverse sequences fail with `access mode not
allowed`. Both stores call `sqlite3.connect` before requesting the startup lock,
so an invalid `mode` sequence never participates in the identity critical
section. A successful sequence is folded to its final effective value like the
other parameters.

Named `mode=memory` databases retain their decoded URI path instead of being
resolved as filesystem paths. A non-empty named memory URI with
`cache=shared` uses the shared initialization lock; private or temporary memory
connections bypass the registry because each connection owns a different
database.

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
- Equivalent URI aliases cannot race at WAL, migration, or interrupted-command
  recovery boundaries, while distinct disk and named-memory databases can
  still initialize in parallel.

## Verification

Concurrency tests create a fresh exact legacy Task database and Gateway Command
database for each of 60 rounds, then run 12 complete constructors against that
database. They verify the migrated schema, backfills, recovery, uniqueness, and
integrity. Separate injected SQLite failures assert that the first worker's
real error is re-raised rather than a barrier error and that all 12 constructor
connections are closed.

URI alias tests run complete Task and Gateway Command store constructors through
ordinary, percent-encoded, relative, `localhost`, and reordered-query spellings.
They observe a peak of one inside the complete startup boundary for equivalent
aliases, a peak of at least two for different databases, and the same
serialization/isolation behavior for shared named-memory aliases and distinct
memory names. Twelve-thread rounds mix aliases containing overridden `cache`
and percent-decoded application parameters with aliases containing only their
final values. Repeated query keys, literal plus signs, and percent-encoded paths
have explicit identity assertions; shared/private final memory-cache values
remain separate database boundaries.

Small real-SQLite control tests demonstrate shared/private `cache` behavior,
successful/failing `vfs` order, successful restrictive `mode` transitions, and
rejected access escalation. Full Task and Gateway Command constructors assert
that rejected `mode` aliases make zero initialization-lock calls.
