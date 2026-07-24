# ADR 0008: Shared Provider Request Budget

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

Per-worker pacing and retry delays do not protect a provider when several collection workers share
the same outbound IP address. Each process can remain inside its own configured limit while their
combined traffic exceeds the actual budget. That creates avoidable rate-limit responses, IP bans,
retry storms, and incomplete market-data jobs.

The coordination path must remain provider-independent, work before network access, support
weighted requests, preserve exact reservation evidence, and avoid scanning an ever-growing request
history on every decision.

## Decision

Add a provider-independent shared request-budget boundary with:

- A versioned policy identified by one explicit `budget_key`.
- Capacity units over a bounded period of at most one hour.
- Weighted reservation cost.
- A UUID idempotency key for every reservation.
- Durable grant or denial decisions with available capacity and bounded retry time.
- Aggregate granted, denied, cost, total-wait, and maximum-wait metrics.
- A source decorator that reserves capacity before delegating to a public market-data adapter.
- Existing bounded retry behavior for a local budget denial.

The first coordinator uses the Generic Cell Rate Algorithm (GCRA) with integer microseconds. It
stores one theoretical-arrival timestamp per budget, so each new decision requires constant state
rather than a scan of prior reservations. The emission interval rounds upward to remain
conservative when the configured period does not divide evenly by capacity.

SQLite uses `BEGIN IMMEDIATE`, WAL mode, full synchronous writes, foreign keys, and a bounded busy
timeout. Separate processes pointing at the same file serialize their reservations before network
access. The policy for an existing key is immutable; a conflicting capacity or period fails
closed. A timestamp earlier than the last coordinated observation also fails closed.

Reservations are append-only evidence. Repeating the same reservation identifier with identical
content returns its existing decision without consuming capacity again. Reusing it with different
content is a conflict.

The rate-budget database remains separate from market evidence and collection checkpoints because
each state has a different schema lifecycle, retention policy, and failure domain.

## Safety Boundary

This decision authorizes only local coordination before public market-data requests. It does not
authorize:

- API keys, account data, balances, positions, orders, or withdrawals.
- Background scheduling, continuous collection, or live streaming.
- Guessing a provider's limits or request weights.
- Ignoring a provider response such as HTTP 429 or `Retry-After`.
- Coordination across hosts that do not share this SQLite file and a reliable clock.
- Unlimited capacity, period, cost, retries, or wait time.

Provider-specific policy values remain explicit configuration. A local grant is only permission to
attempt a public request; the provider response and existing retry rules remain authoritative.

## Consequences

### Positive

- Cooperating workers cannot exceed their configured combined request budget.
- Decisions are constant-state, deterministic, weighted, and auditable.
- Duplicate reservation delivery cannot double-charge the budget.
- Local pressure becomes visible through durable denial and wait metrics.
- Budget exhaustion happens before network access and feeds existing collection-health evidence.
- No dependency or provider-specific behavior enters the application contract.

### Negative

- SQLite coordinates one shared filesystem, not multiple independent hosts.
- Clock regression blocks requests until operator investigation or time recovery.
- GCRA permits the explicitly configured burst and requires correct provider-specific cost values.
- The reservation history grows until a future retention policy is approved.
- The current source decorator creates one reservation for each attempt; there is no global
  scheduler or adaptive policy manager.

## Alternatives Considered

### Per-process sleep

Rejected. Independent workers cannot see one another and can exceed a shared IP limit.

### Sliding-window history scan

Rejected for the hot path. It offers intuitive accounting but makes every decision depend on a
growing reservation set.

### Fixed windows

Rejected. Requests can burst at both sides of a boundary and materially exceed the intended rate.

### Provider logic inside the Binance adapter

Rejected. Coordination, idempotency, and pacing are application-control concerns and must remain
usable by future providers.

### Redis or a distributed rate-limit service

Deferred. It is appropriate for multiple hosts but adds infrastructure before Phase 2 requires it.

## Review Triggers

Revisit this decision when adding multiple hosts, live streams, adaptive provider-header budgets,
reservation retention, another provider, or automatic scheduling.
