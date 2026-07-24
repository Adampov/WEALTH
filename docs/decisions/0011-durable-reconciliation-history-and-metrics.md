# ADR 0011: Durable Reconciliation History and Indexed Quality Metrics

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The deterministic cross-source reconciler produces reproducible evidence for one selected candle
window, but an in-memory report disappears after the caller exits. Without durable history, the
platform cannot inspect past disagreements, distinguish recurring source-quality failures from
isolated events, or provide trustworthy per-series operational metrics.

The canonical report remains the evidence of record. Query-efficient projections are useful, but
they must not silently replace or contradict that evidence. Storage also needs explicit bounds so a
diagnostic query cannot accidentally become an unbounded production scan.

## Decision

Add a dedicated SQLite reconciliation-history boundary with immutable, versioned observation
contracts:

- Each observation has a UUID, an aware recording time, the complete canonical reconciliation
  report, an exact SHA-256 digest of that report's JSON representation, and non-empty lineage.
- The recording time cannot precede the end of the reconciled market-data window.
- One embedded report is capped at 64 MiB before persistence.
- Writes are append-only. Repeating an identical observation is idempotent.
- Reusing an observation UUID for different content returns an explicit identity conflict and never
  overwrites accepted evidence.
- A comparison key is permanently associated with its exact primary and reference streams.
  Reusing that key with different streams returns an explicit series conflict.
- The dedicated database stores the immutable observation JSON and indexed projections for status,
  source-quality failures, compared intervals, and machine-readable issue counts.
- Reads reconstruct and validate the observation contract, report digest, indexed fields, and issue
  projections before returning evidence.
- History queries are restricted to one comparison key, a maximum 366-day half-open time window,
  deterministic recorded-time ordering, and at most 1,000 observations.
- Summary queries use the same bounded window and return counts for `pass`, `divergent`, and
  `blocked` outcomes; primary and reference quality failures; compared intervals; and each issue
  code.
- An unknown comparison key returns no summary. A known key with no observations in the requested
  window returns an explicit zero-valued summary.
- SQLite enables foreign keys, write-ahead logging, full synchronous durability, a bounded busy
  timeout, and a versioned schema.

The first implementation is a local single-host evidence and metrics store. It does not schedule
reconciliation runs, publish a dashboard, send alerts, rank providers, or alter canonical market
records.

## Safety Boundary

This decision authorizes durable local storage and bounded aggregation of reconciliation evidence.
It does not authorize:

- Choosing a source of truth or assigning provider trust scores.
- Blending, repairing, replacing, or deleting market data.
- Automatically changing reconciliation tolerances.
- Converting reconciliation metrics into investment signals or Risk approval.
- Continuous scheduling, live monitoring, alerts, or automated remediation.
- Shared or multi-host database operation.
- Private exchange access, credentials, account data, orders, or execution.

## Consequences

### Positive

- Reconciliation outcomes survive process restarts with their original evidence and provenance.
- Append-only and idempotent behavior protects historical meaning during retries.
- Identity and series-key conflicts are visible rather than silently overwritten.
- Indexed aggregates make recurring disagreement and source-quality patterns queryable without
  parsing every report.
- Bounded reads provide predictable local operational cost.
- Read-time validation detects accidental corruption between canonical JSON and its projections.

### Negative

- The local SQLite database is not a multi-host observability platform.
- The 366-day query bound requires callers to compose longer analyses from multiple windows.
- Indexed projections duplicate values already present in canonical report JSON.
- Summary queries validate aggregate contracts but do not reload every canonical report; callers
  requiring evidence-level verification must use the bounded history read.
- SHA-256 detects inconsistent content but is not a digital signature or protection from an
  attacker with database and application access.

## Alternatives Considered

### Keep reports only in process logs

Rejected. Logs do not provide a stable versioned contract, idempotent identity, or dependable
per-series queries.

### Store only aggregate counters

Rejected. Counters without the original report cannot explain a divergence, reproduce a finding, or
verify lineage.

### Recompute every metric by parsing all report JSON

Rejected for routine summaries. It preserves one representation but creates unnecessary and
unbounded query cost. Canonical JSON remains authoritative while indexed projections are validated
on evidence reads.

### Add reconciliation tables to the market-data database

Rejected for this slice. A dedicated file keeps control and diagnostic evidence isolated from raw
and canonical market records and lets each schema evolve independently.

### Start with a network database and dashboard

Deferred. Local SQLite is sufficient to prove contracts, idempotency, corruption handling, and
metrics before introducing deployment and operational complexity.

## Review Triggers

Revisit this decision when adding continuous reconciliation jobs, retention or archival policy,
dashboard and alert integrations, multi-host collection, a network database, signed evidence,
provider scoring, automatic remediation, or a governed source-selection policy.
