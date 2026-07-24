# ADR 0012: Supervised Continuous Candle Polling and Reconnect

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The bounded historical collector can persist page progress, survive restart, enforce worker leases,
and record provider health. It still requires an operator to define one fixed end time. A
continuously updated candle stream needs to discover newly closed intervals, catch up in bounded
chunks, and remember its cursor across invocations.

The control path must not request an unfinished candle, skip evidence after a crash, create an
unbounded retry loop, or let competing workers advance one stream inconsistently. Continuous
operation also needs a deliberate stop mechanism and a safe response to repeated or
non-reconnectable provider failures.

## Decision

Add a provider-independent supervised polling workflow on top of the accepted bounded historical
collector:

- One versioned continuous checkpoint fixes source, venue, canonical instrument, provider symbol,
  instrument type, timeframe, initial window, and a durable next-window cursor.
- The workflow calculates the latest eligible end on the timeframe's UTC grid using an explicit
  settlement delay. It never requests the currently open candle.
- Each polling cycle is capped at a configured candle count no larger than the historical
  collector's existing 100,000-candle hard bound.
- Each invocation has an explicit limit of at most 100 polling cycles.
- Before network work, the continuous checkpoint uses compare-and-swap to attach one UUID and exact
  end time for a bounded historical job.
- The bounded job UUID and creation time are deterministic durable inputs after attachment. If a
  process stops before creating the job, a later process reconstructs it exactly. If it stops after
  market evidence commits, the completed job is reused without refetching.
- The continuous cursor advances only after the attached bounded job reports complete, and only to
  that job's exact end. The active job then clears atomically with the cursor update.
- Competing workers may observe the same active job, but its existing lease and the continuous
  cursor's compare-and-swap prevent duplicate advancement.
- Transport failures and provider unavailability may reconnect only after the bounded page retry
  policy reaches `attempts_exhausted`.
- Rate limits and local request-budget exhaustion pause after their bounded page attempts. The
  current job checkpoint does not retain the final response's exact remaining wait, so the
  supervisor does not guess a shorter reconnect delay.
- Reconnect uses deterministic exponential delay with explicit base, maximum, and consecutive
  failure limits.
- A non-retryable failure, an excessive or unsafe provider wait, a quality or storage rejection, or
  the consecutive-failure limit pauses the stream without moving its cursor.
- Operators can pause before new network work and explicitly resume while retaining the active job
  for safe recovery.
- Continuous checkpoints and append-only transitions use a dedicated SQLite control file with
  optimistic versions, read-time projection validation, WAL mode, full synchronous durability,
  foreign keys, and a bounded busy timeout.

This workflow is supervised and bounded. A caller must explicitly create a stream and invoke one or
more cycles. It is ready to sit behind a future process manager, but this decision does not create
or automatically start that service.

## Failure and Recovery Order

For each catch-up window:

1. Persist the active bounded job identity and exact window.
2. Create or reload that idempotent bounded job.
3. Fetch, validate, and persist market evidence through existing controls.
4. Complete the bounded job and its health evidence.
5. Advance the continuous cursor and clear the active job.

A crash between any two steps can cause safe replay of an idempotent step but cannot advance the
cursor past missing market evidence.

## Safety Boundary

This decision authorizes supervised polling of already closed public candles. It does not
authorize:

- An automatically started daemon, operating-system service, deployment, or external scheduler.
- Live WebSocket ingestion or use of incomplete candles.
- Unbounded cycles, pages, records, reconnect attempts, or waits.
- Automatic retry of malformed data, quality failures, storage conflicts, or unsafe
  `Retry-After` values.
- Multiple hosts that do not share the accepted local control and rate-budget state.
- Private exchange access, credentials, account data, balances, positions, orders, or execution.
- Trading signals, provider ranking, data repair, or investment decisions.

## Consequences

### Positive

- A closed-candle stream can make bounded progress across repeated invocations.
- Planned disconnect and process-restart tests preserve exact cursor continuity.
- An attached job makes crash recovery explicit and prevents a completed window from being skipped.
- Existing quality, raw evidence, idempotency, leases, source-health, and rate-budget controls are
  reused rather than duplicated.
- Repeated failures become a visible paused state instead of an infinite reconnect loop.
- Operators can stop network work without deleting progress.

### Negative

- This is polling, so it has higher latency and request overhead than a future WebSocket stream.
- A future process manager is still required for unattended 24/7 operation.
- Continuous and bounded-job checkpoints use separate SQLite files, so recovery relies on ordered
  idempotent steps instead of a cross-database transaction.
- A crash before bounded-job creation can leave only the attached job identity; recovery must
  reconstruct that job before proceeding.
- SQLite remains a single-host baseline.

## Alternatives Considered

### Extend one historical job's end time indefinitely

Rejected. The bounded job window is immutable audit evidence, and an expanding end would weaken
work limits and reproduction.

### Infer progress from the latest stored candle

Rejected. Stored records alone cannot distinguish a complete controlled cycle from a partial,
quarantined, or externally written stream.

### Advance the continuous cursor before collection

Rejected. A crash or provider failure could permanently skip missing evidence.

### Retry every failure forever

Rejected. Malformed content, quality failures, storage conflicts, and unsafe rate instructions
require operator investigation rather than automated pressure.

### Start a background daemon in this change

Deferred. First prove deterministic cycle, reconnect, pause, and restart behavior behind injectable
clock and sleep boundaries. Process lifecycle, deployment, monitoring, and shutdown signals require
a separate operational task.

## Review Triggers

Revisit this decision when adding a process manager, graceful shutdown protocol, live WebSockets,
provider failover, multi-host coordination, adaptive reconnect jitter, retention policy, or
operational dashboards and alerts.
