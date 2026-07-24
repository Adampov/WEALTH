# ADR 0017: Order-Flow Quality and Idempotency Boundary

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, and Audit Department

## Context

The canonical trade, ticker, and best-bid-ask contracts provide strict provider-independent
records, but contracts alone do not prove that a bounded stream is complete, correctly ordered, or
safe to store. Event streams may contain records from the wrong market, records outside a requested
window, repeated identities, conflicting revisions, time regressions, or provider-sequence
failures.

Provider sequence semantics are not universal. Some feeds guarantee only a monotonic identifier,
some guarantee contiguous increments, and others expose a number with no continuity promise. The
quality layer must not report a gap unless the selected provider contract makes that conclusion
valid.

## Decision

Add a bounded, deterministic quality boundary for one exact canonical record family and an
idempotent in-memory storage proof.

### Exact stream identity

`OrderFlowStream` identifies source, venue, canonical instrument, instrument type, and exactly one
record family: trade, ticker, or best bid and ask. Ticker and best-bid-ask snapshots cannot be mixed
with trades or with each other during one audit.

The stream also declares one sequence policy:

- `unspecified`: make no ordering or continuity claim from provider sequences.
- `monotonic`: require every accepted record to carry a sequence and require it to increase.
- `contiguous`: apply the monotonic rules and report exact absent integer ranges.

The default is `unspecified`. Sequence policy must come from documented adapter knowledge; it is
never inferred from a sample of records.

### Bounded quality audit

`OrderFlowSequenceAuditor` accepts one timezone-aware half-open event-time window and at most
100,000 input records. It:

- Detects records from another exact stream.
- Detects event times outside the declared window.
- Detects input that regresses in event time.
- Groups provider-scoped natural keys.
- Treats equivalent canonical market values as duplicates.
- Treats different values for one natural key as a conflict and does not count that identity as
  usable.
- Enforces missing, reused, or regressing provider sequences only when the stream declares a
  sequence promise.
- Reports exact missing ranges only under the explicit contiguous policy.

The report retains input, usable, and sequenced counts plus machine-readable findings. Any finding
or proven sequence gap fails the report.

### Idempotent temporary storage

`OrderFlowStore` defines append and exact-stream query operations.
`InMemoryOrderFlowStore` is the first replaceable proof:

- Natural keys are namespaced by canonical record family.
- The first record is retained.
- An equivalent repeat returns `DUPLICATE`.
- A different record for the same identity returns `CONFLICT`.
- Neither duplicate nor conflict replaces the first record.
- Queries are isolated by exact stream and ordered deterministically by event time, provider
  sequence availability, provider sequence, and record identity.

The application boundary is responsible for admitting only quality-approved batches. Direct
storage access does not turn an unaudited record into trusted evidence.

## Safety Boundary

This decision does not authorize:

- A REST or WebSocket order-flow adapter.
- Persistent order-flow storage, replay, retention, correction, or gap filling.
- Guessing undocumented provider sequence semantics.
- Reconstructing a full order book from snapshots or deltas.
- Aggregating order flow into a signal, recommendation, risk limit, or order.
- Private exchange access, credentials, balances, positions, execution, or live trading.

## Consequences

### Positive

- Downstream work receives deterministic evidence about stream integrity.
- Mixed records, silent overwrites, and conflicting provider revisions fail visibly.
- Exact provider gaps can be represented without inventing missing market values.
- Providers without a continuity guarantee are not falsely accused of data loss.
- The storage protocol can later receive an independently tested durable adapter.

### Negative

- The in-memory adapter does not survive restart and is not an operational data store.
- A quality finding rejects the bounded batch even when some records remain individually usable.
- Sequence guarantees must be researched and encoded per provider and endpoint.
- No rejected raw provider payload is retained by this slice.

## Alternatives Considered

### Treat every integer sequence as contiguous

Rejected. Many provider counters skip for reasons unrelated to missing records or cover a broader
event domain than one filtered stream.

### Sort records before auditing

Rejected. Sorting would hide arrival-order regressions that matter for replay and live recovery.
Storage queries may sort accepted evidence, but the quality gate evaluates supplied order.

### Last write wins

Rejected. A changed value for one provider identity is evidence of a conflict, not permission to
rewrite history.

### Add durable SQLite order-flow storage now

Deferred. The quality and idempotency semantics should be stable before choosing tables, raw
payload linkage, indexes, retention, and migration behavior.

## Review Triggers

Revisit this decision when adding a provider adapter, durable raw and canonical storage,
point-in-time order-flow replay, provider-specific gap recovery, full order-book deltas, or
operational retention and capacity limits.
