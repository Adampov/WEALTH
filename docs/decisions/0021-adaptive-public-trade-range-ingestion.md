# ADR 0021: Adaptive Public-Trade Range Ingestion

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit Department

## Context

The Binance aggregate-trade adapter rejects a response at the 1,000-row cap because that response
cannot prove that its requested event-time window is complete. A caller could manually request
smaller windows, but an ungoverned loop could create gaps, overlaps, unbounded requests, excessive
retries, or an incorrect resume boundary.

The application already uses bounded page orchestration for historical candles. Public trades need
the same separation of responsibilities while accounting for event-driven density: a fixed time
window may contain zero observations or reach the provider cap.

## Decision

Add `AdaptivePublicTradeRangeIngestor` as the provider-independent application boundary for one
bounded `PublicTradeWindowRequest`.

### Chronological planning and adaptive splitting

The ingestor:

1. Splits the requested range into deterministic contiguous initial windows.
2. Processes windows in chronological order.
3. When a source explicitly marks a response as requiring a smaller window, divides that window
   into two exact millisecond-aligned children.
4. Processes the left child before the right child and repeats until the source proves completion
   or the configured minimum duration is reached.
5. Sends every complete batch through `OrderFlowBatchIngestor` before continuing.

The default initial duration is 30 minutes and the minimum is one millisecond. Empty complete
windows are admitted as raw evidence and advance range coverage.

`PublicTradeSourceError.requires_smaller_window` is a provider-neutral structured classification.
It cannot also be retryable unchanged: the next safe action must alter the request.

### Bounded work and retries

One run has explicit policy limits:

- Maximum range duration: 24 hours by default and seven days as a hard configurable ceiling.
- Maximum source requests: 256 by default and 1,024 as a hard ceiling.
- Maximum admitted canonical records: 100,000.
- Finite inter-request pacing: 250 milliseconds by default.
- Transient retries: three attempts by default and five as a hard ceiling.
- Exponential retry delays and provider `Retry-After` values are bounded.

Every network attempt counts against the source-request limit, including retries and probes that
trigger a split. A retry is never started or slept for after the request budget is exhausted.
Malformed data, provider rejection, quality failure, and storage conflict are not retried.

The total record limit is checked after a bounded fetch but before admission. The fetched batch
remains in the in-memory result evidence, but its raw or canonical content is not written.

### Explicit progress evidence

Every attempted window emits a typed trace with:

- Exact request window.
- Attempt count and applied retry delays.
- Outcome: ingested, split, source failure, ingestion rejection, density limit, or record limit.
- Safe structured source-failure evidence when applicable.
- Exact split children or ingestion result when applicable.

The range result has a typed stop reason and `next_window_start`, the first event-time boundary not
durably admitted. Completed windows remain durable if a later window stops. A resumed run is
idempotent through the existing storage contract.

Trace contracts verify that split children exactly partition their parent and that a completed
result covers the entire requested range. A stopped result must point its pending window at the
same exact safe resume boundary.

## Safety Boundary

This decision does not authorize:

- Automatic startup, scheduling, continuous polling, or live WebSocket collection.
- Unbounded ranges, requests, records, retries, provider waits, or memory growth.
- Accepting a capped response, synthesizing missing trades, or skipping a dense millisecond.
- Persisting a fetched batch after the total record bound is reached.
- API keys, account access, private data, portfolio state, signals, or trading.

## Consequences

### Positive

- Dense markets can be collected without silently accepting partial provider responses.
- Initial pages and recursive splits remain gap-free, overlap-free, and chronological.
- Empty and active windows share one honest completeness boundary.
- Retry, pacing, request, record, and minimum-duration behavior is deterministic and testable.
- Partial durable progress has an exact idempotent resume boundary.
- Every adaptive choice remains inspectable rather than hidden inside the provider adapter.

### Negative

- A millisecond containing 1,000 aggregate rows still stops explicitly because time-based
  subdivision cannot prove completeness.
- Recursive splitting increases request weight in dense periods.
- Completed windows are separate durable transactions, not one all-or-nothing range transaction.
- Record-limit evidence is returned in memory but not retained in a rejected-evidence store.
- The candle and trade retry policies remain separate until measured reuse justifies a shared
  abstraction.

## Alternatives Considered

### Accept the capped response and continue from its final ID

Rejected. Completeness and boundary behavior would remain ambiguous, especially when event times
collide.

### Put splitting and retries inside the Binance adapter

Rejected. One adapter fetch must remain one observable network operation. Application policy owns
work bounds, pacing, retries, admission, and progress.

### Use fixed one-millisecond windows for every request

Rejected. It would waste request budget during ordinary market conditions and still would not
solve a provider cap inside one millisecond.

### Make the whole range one storage transaction

Rejected. Large transaction duration and loss of safe partial progress would make recovery worse.
Each admitted window is independently atomic and replay-safe.

## Review Triggers

Revisit this decision when adding ID-based continuation, rejected-evidence persistence, shared
provider request budgets, continuous order-flow collection, live WebSocket gap recovery, another
trade provider, or measured density that frequently reaches the minimum window.
