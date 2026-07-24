# ADR 0007: Durable Collection Checkpoints and Source Health

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The bounded historical workflow commits accepted market-data pages independently and returns an
in-memory resume boundary. That protects accepted evidence when a later page fails, but a process
restart loses the boundary, two workers can attempt the same job, and retry outcomes are not yet
durable health evidence.

Recovery must remain safe when a process stops after committing market data but before recording
progress. The design must not assume that a network request and two SQLite databases can share one
atomic transaction.

## Decision

Add an operator-invoked, provider-independent collection-control workflow with:

- An immutable bounded request and a versioned current checkpoint.
- Pending, running, paused, completed, and failed lifecycle states.
- A bounded worker lease and optimistic compare-and-swap version on every transition.
- One durable cursor update after each accepted page.
- Append-only transition history.
- One append-only source-health observation per terminal page attempt.
- SQL aggregation for job-level healthy, degraded, unavailable, accepted, and attempt counts.
- Read-time contract validation and checks that indexed SQLite values match canonical JSON.
- Streaming page iteration so a 100,000-page bounded request does not allocate every request object
  before work begins.

Market evidence and collection-control state use separate SQLite files. SQLite schema version is
database-wide, and separate files avoid coupling market-evidence migrations or failures to
job-control migrations or failures. Each file retains WAL mode, full synchronous writes, foreign
keys, and a bounded busy timeout.

The workflow writes market evidence before advancing the checkpoint. If the process stops between
those writes, the durable cursor intentionally remains on the prior page. A later worker waits for
the lease to expire, fetches that page again, and relies on the market store's existing idempotent
duplicate handling before advancing. Conflicting provider revisions remain quarantined and fail the
job.

Checkpoint advancement and its page-health observation are committed in one collection-state
transaction. A stale worker receives an explicit checkpoint conflict and cannot silently replace a
newer cursor.

## Safety Boundary

This decision authorizes only explicit, bounded runs over public historical market data. It does
not authorize:

- Background scheduling, a daemon, continuous collection, or live streaming.
- API keys, private account data, balances, positions, orders, or withdrawals.
- Automatic retry of quality failures or storage conflicts.
- Parallel page downloads or unbounded work.
- Treating a held or lost lease as permission to continue.

No current entry point starts a job automatically. A caller must explicitly create and run it.

## Consequences

### Positive

- Completed progress survives restart and is auditable page by page.
- An active lease plus compare-and-swap prevents two workers from advancing the same job.
- Crash recovery is safe without a distributed transaction.
- Provider health and retry pressure can be queried without replaying application logs.
- Large page plans use constant planner memory during execution.
- Existing source, quality, and storage contracts remain unchanged.

### Negative

- A worker that stops must wait for its bounded lease to expire before another worker can recover.
- The current explicit runner has no heartbeat or background scheduler.
- Cross-file atomicity is intentionally unavailable, so a crash can cause one safe duplicate fetch.
- SQLite is the local Phase 2 baseline, not the final multi-host coordination store.

## Alternatives Considered

### Store the cursor in the market-data database

Rejected for this slice. It couples schema versions and failure domains for immutable evidence and
mutable operational control state.

### Advance the checkpoint before writing market data

Rejected. A crash could then skip evidence permanently while the cursor falsely reports progress.

### Use one distributed transaction

Rejected. It adds infrastructure and operational complexity that the local Phase 2 slice does not
need. Idempotent refetch is simpler and safer.

### Run pages in parallel

Deferred. It complicates rate budgeting, cursor ordering, leases, and deterministic recovery.

## Review Triggers

Revisit this decision when adding continuous scheduling, lease heartbeats, multiple hosts, shared
rate budgets, live streams, a second provider, or migration beyond local SQLite.
