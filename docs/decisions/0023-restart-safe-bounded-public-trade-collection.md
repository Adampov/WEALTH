# ADR 0023: Restart-Safe Bounded Public-Trade Collection State

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit Department

## Context

The adaptive public-trade range ingestor exposes an exact safe resume cursor and typed evidence for
every completed, split, retried, or rejected window. That evidence currently exists only for one
explicit invocation. A process restart loses the cursor, a changed range policy can silently alter
the recovery plan, and two workers have no durable compare-and-swap boundary.

The candle collection checkpoint cannot be reused directly. Candle progress advances on a fixed
timeframe grid, while aggregate-trade density can recursively split one planned window into smaller
millisecond-aligned leaves. Recovery must retain both the first unadmitted boundary and the exact
end of the pending leaf, whether collection paused at a clean local bound or failed terminally.

Market evidence and mutable collection control state also live in separate SQLite databases. They
cannot participate in one atomic transaction.

## Decision

Add dedicated provider-independent contracts for one immutable bounded public-trade collection
job:

- `PublicTradeCollectionCheckpoint` stores the bounded source identity, range, durable cursor,
  exact pending-leaf end, lifecycle, versioned worker lease, and lifetime cumulative committed-work
  totals.
- `PublicTradeSourceHealthObservation` stores append-only evidence for one bounded range
  invocation and the exact checkpoint version that committed it, including the provider symbol,
  accepted progress, requests, completed windows and records, adaptive splits, retry waits,
  terminal classification, and the pending leaf when the invocation does not finish its requested
  range.
- `PublicTradeCollectionHealthSummary` exposes query-efficient aggregate health and work totals.
- `PublicTradeCollectionCheckpointStore` defines create, load, compare-and-swap transition,
  health-history, and health-summary operations.

The checkpoint includes an immutable `policy_fingerprint`. The future application orchestrator
must derive it from the effective versioned range, split, retry, pacing, and request-budget policy.
A recovery attempt cannot silently continue the same job under different collection semantics.

`next_window_start` is the first event-time boundary not yet durably admitted.
`pending_window_end_exclusive`, when present, is the exact end of the adaptive leaf that stopped.
Together they preserve the precise retry window instead of reconstructing it from a possibly
changed planner.

Request, completed-window, record, and split counters are lifetime cumulative only for outcomes
that reach a successful checkpoint compare-and-swap transaction. They are an audit of committed
control outcomes, not a crash-durable hard limit on every network attempt. A process can stop
after a request but before the compare-and-swap, so recovery can repeat that request without it
appearing in these counters.

Each committed observation also records its window-trace count and retry-attempt count. Contracts
require `source_requests = window_traces + retry_attempts`, and require completed windows plus
adaptive split traces not to exceed the total trace count. This makes committed invocation
accounting exact without claiming knowledge of work lost before compare-and-swap.

The existing durable shared provider-rate budget remains the pre-request protection for provider
capacity. Reserving every job attempt durably before its network request, so an individual job
limit also survives a crash before checkpoint commit, belongs to the future application
orchestrator and is not claimed by this control-state slice.

A bounded invocation that stops cleanly at its local outer request or record limit pauses the job
and retains the exact pending leaf. It is not a terminal collection failure. Its source-health
status remains truthful to the observed provider path: `HEALTHY` when no retry or split degraded
the invocation, otherwise `DEGRADED`. A terminal source or admission failure fails the job.
Classification must inspect the typed terminal trace and admission outcome; a stop-reason string
alone is not sufficient to decide between `PAUSED` and `FAILED`. The future application mapper
must translate typed upstream failures into bounded canonical control codes rather than copying
arbitrary provider machine-code strings into durable state.

Add a dedicated file-backed SQLite control store with:

- A database-type marker and explicit schema version.
- One canonical current checkpoint plus append-only transition history.
- Optimistic compare-and-swap on the exact previous version.
- A bounded worker lease with a UUID fencing token that only a running checkpoint may hold. The
  token is checked again at the transition boundary, TTL is limited to one hour, it is retained in
  append-only history for audit, and it is recorded in a per-job acquisition ledger that forbids
  token reuse on a later claim.
- An atomic control-state transaction for a checkpoint transition and its matching source-health
  observation.
- Indexed job identity and progress fields, including normalized UTC timestamps, checked in full
  against canonical serialized records.
- Full read-time contract, indexed-projection, health-summary, and health-history validation with
  explicit corruption failures. Health pages are bounded to 100 observations by default and 1,000
  maximum, use checkpoint version as their exclusive cursor, and never infer causality from
  timestamps or observation IDs.
- Transactional schema installation plus canonical DDL validation, so a failed initialization or
  a weakened same-version schema cannot be used as a control database.

This slice establishes control-state contracts and persistence only. It does not compose or start
the existing range ingestor.

### Recovery ordering

The future orchestrator must commit accepted order-flow evidence before advancing this control
checkpoint. If the process stops after evidence commits but before control state advances, restart
must refetch the retained pending leaf and rely on the existing idempotent order-flow store.

Advancing control state first is forbidden because a crash could permanently skip evidence.
Cross-database atomicity is intentionally unavailable, so evidence-first ordering and idempotent
replay are required rather than claimed atomicity.

## Safety Boundary

This decision does not authorize:

- Starting a collector, process, service, scheduler, or background task.
- Continuous polling, live WebSocket ingestion, or automatic gap recovery.
- Unbounded ranges, requests, records, splits, retries, waits, or memory growth.
- Treating committed-work counters as proof that no uncommitted request was made, or changing a
  job's policy fingerprint.
- Skipping the retained pending leaf or advancing control state before evidence is durable.
- Deciding `PAUSED` versus `FAILED` from an untyped stop-reason string alone.
- Multi-host lease or rate-budget coordination.
- API keys, private data, account access, portfolio state, signals, orders, or trading.

## Consequences

### Positive

- Bounded aggregate-trade progress and the precise pending adaptive leaf survive restart.
- An immutable policy identity makes configuration drift explicit.
- Lifetime counters provide a durable audit of committed checkpoint outcomes.
- UUID-fenced leases and compare-and-swap reject stale checkpoint advancement and retain the
  responsible fencing token in transition history. The acquisition ledger also prevents an old
  token from regaining authority after release or expiry.
- Clean local work-boundary stops remain resumable pauses without falsely reporting provider
  unavailability.
- Health evidence and its matching control transition are atomic inside the control database.
- Transactional schema installation, normalized UTC projections, complete projection checks, and
  summary revalidation fail closed on partial, wrong, or corrupt storage.

### Negative

- The control store does not itself collect, schedule, or recover public trades.
- A crash between evidence and checkpoint commits can repeat one safe bounded leaf.
- A crash after a network request but before checkpoint compare-and-swap can also leave committed
  work counters below actual request attempts; the durable shared rate budget, not those counters,
  protects provider capacity.
- Correct policy fingerprint construction and pre-request per-job attempt reservation belong to
  the future orchestrator.
- A typed reader for actor transition history remains future audit tooling; this slice retains its
  canonical rows and actor fencing tokens.
- Lease ordering trusts the transition timestamps supplied through the internal port. The future
  orchestrator must obtain them from its injected trusted clock; the store still enforces
  monotonicity and a one-hour maximum TTL.
- SQLite is a local single-host baseline, not a distributed coordination service.
- Mutable control state and immutable market evidence require separate operational backup and
  migration procedures.

## Alternatives Considered

### Reuse the candle collection checkpoint

Rejected. Fixed-grid candle pages cannot faithfully retain adaptive trade-leaf boundaries or
event-density split evidence.

### Persist only `next_window_start`

Rejected. The start is gap-safe, but it cannot reproduce the exact stopped leaf after a recursive
split or prevent policy drift from changing that leaf.

### Treat checkpoint counters as a crash-durable request limit

Rejected. The checkpoint and network request cannot commit atomically, so such a claim would be
false. Counters remain lifetime cumulative for committed outcomes. The shared durable rate budget
protects provider capacity now; durable per-job pre-request reservations are deferred to the
future orchestrator.

### Put checkpoint state in the order-flow evidence database

Rejected for this slice. Mutable operational control and immutable provider evidence have
different failure domains and schema lifecycles. Evidence-first idempotent replay provides a safer
local recovery boundary without pretending cross-database atomicity.

## Review Triggers

Revisit this decision when adding the bounded application orchestrator, continuous polling,
WebSocket gap recovery, a scheduler or operating-system service, multi-host coordination,
provider-ID continuation, private data, or any order-capable workflow.
