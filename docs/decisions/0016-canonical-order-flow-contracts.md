# ADR 0016: Canonical Trade, Ticker, and Best-Bid-Ask Contracts

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, and Audit Department

## Context

The accepted market-data path models final candles end to end. Professional market-structure,
liquidity, and derivatives analysis also needs event-level trades, last-price snapshots, and
top-of-book evidence. Provider payloads differ in identifiers, timestamps, side semantics, optional
statistics, precision, and sequence fields, so downstream agents must not consume provider-specific
objects directly.

These contracts need to establish honest point-in-time and lineage boundaries before selecting
WebSocket adapters, storage layout, quality gates, or order-flow features.

## Decision

Add three strict, immutable, provider-independent schema-version `1.0` contracts:

### Shared evidence

Every record contains:

- A UUID record identity.
- Source, venue, canonical instrument, and instrument type.
- Exchange event time, local observation time, and processing time.
- An optional non-negative provider sequence.
- At least one lineage reference.
- Exact decimal market values.

Event time cannot follow observation time, and observation time cannot follow processing time.
Identifiers cannot require whitespace normalization. Unknown fields and implicit string-to-decimal
coercion are rejected.

### Canonical trade

- Require a provider trade identity, positive price, and positive base quantity.
- Retain optional positive provider quote quantity separately from the exact locally calculated
  `price * base_quantity`.
- Represent the aggressor as `buy`, `sell`, or `unknown`.
- Never infer an aggressor side when the provider does not prove it.
- Use source, venue, instrument, instrument type, and provider trade identity as the natural key.

### Canonical ticker

- Require a positive last price.
- Permit optional rolling-window open, high, low, base volume, and quote volume only with explicit
  window start and end times.
- Require window start before window end and window end no later than event time.
- Require high and low together, low no greater than high, and any supplied open and last prices
  inside the supplied range.
- Permit a last-price-only snapshot with no rolling-window claim.

### Canonical best bid and ask

- Require positive bid and ask prices and positive displayed quantities.
- Require best bid strictly below best ask.
- Reject locked or crossed snapshots at the canonical boundary.
- Expose exact derived absolute spread, arithmetic midpoint, and midpoint-relative spread in basis
  points.

Ticker and best-bid-ask snapshot natural keys contain source, venue, instrument, instrument type,
event time, and optional provider sequence. Each contract exposes provider-neutral market values
for future idempotency and conflict handling.

## Safety Boundary

This decision authorizes schema and validation work only. It does not authorize:

- A public REST or WebSocket adapter for these records.
- Persistent storage, replay, aggregation, correction, or gap-filling for these records.
- A full depth order book or reconstruction from deltas.
- Private exchange access, credentials, balances, positions, orders, or execution.
- Treating a trade, ticker, spread, or aggressor side as an investment recommendation.
- Using top-of-book data as permission to place or price an order.

## Consequences

### Positive

- Future provider adapters have a stable target independent of exchange payload shape.
- Event, observation, and processing time preserve point-in-time truth.
- Unknown aggressor evidence remains explicit instead of becoming fabricated directional flow.
- Exact decimal quantities and derived BBO metrics avoid binary floating-point drift.
- Windowed ticker statistics cannot silently lose their time context.
- Natural keys and market values prepare deterministic idempotency and conflict tests.

### Negative

- No live or historical order-flow data is collected by this change.
- A locked or crossed provider snapshot is rejected even if it reflects a transient venue state;
  future raw evidence must retain it for investigation.
- Provider quote quantity may differ from the local multiplication because of provider rounding;
  this contract preserves both rather than declaring one silently correct.
- A best-bid-ask snapshot is not enough to calculate depth, queue position, or realistic execution
  impact.
- Snapshot identity without a provider sequence relies on provider event-time precision.

## Alternatives Considered

### Reuse candle contracts for trades and quotes

Rejected. Event-level records have different identities, timing, side evidence, and quality
failure modes.

### Infer aggressor side from price movement

Rejected. Tick-rule inference is an analytical estimate, not provider evidence, and belongs in a
separate versioned feature.

### Allow bid equal to or above ask

Rejected at the canonical boundary. Locked and crossed observations require explicit raw evidence
and a governed quality decision rather than silent acceptance as ordinary BBO state.

### Define full order-book deltas now

Deferred. Sequence continuity, snapshots, delta recovery, checksum rules, and provider-specific
resynchronization need a separate design and fault-test suite.

## Review Triggers

Revisit this decision when adding the first trade or BBO adapter, durable storage, point-in-time
replay, full order-book deltas, provider checksum validation, ticker-window normalization, or
aggressor-side inference features.
