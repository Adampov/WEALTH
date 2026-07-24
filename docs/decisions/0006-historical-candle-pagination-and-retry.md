# ADR 0006: Historical Candle Pagination and Retry

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The first Binance adapter deliberately accepts one already-closed window of at most 1,000 candles.
Phase 2 also needs bounded historical ranges that can survive ordinary transport and provider
failures without hiding retries, skipping intervals, overwriting evidence, or creating an
unattended downloader.

Binance's public Spot kline endpoint allows at most 1,000 records and consumes IP-based request
weight. Binance also requires clients to back off after HTTP 429, returns `Retry-After` for HTTP 418
and 429, and may ban an IP when a client continues sending requests instead of backing off.

Pagination, retry, and pacing are application workflow concerns. They must not be hidden inside the
provider adapter because downstream operators and Audit need exact attempts, delays, failure
classification, and partial progress.

## Decision

Add a provider-independent, operator-invoked historical range workflow:

- `HistoricalCandlePagePlanner` splits one aligned range into contiguous, non-overlapping pages.
- A page contains at most 1,000 candles.
- One invocation contains at most 100,000 candles and is rejected before network access when it
  exceeds that bound.
- The default application delay between successful pages is 0.25 seconds.
- The range ingestor calls the existing single-page source, quality gate, and transactional store
  path for every page.
- The provider adapter still performs one HTTP request and contains no hidden retry loop.
- Provider errors cross the source port with a safe machine code, retry classification, and
  optional `Retry-After`.
- Transport failures and provider unavailability are retryable. A rate-limit response is retryable
  only when it contains a valid bounded `Retry-After`.
- Invalid requests, unsupported instruments, provider rejection, and malformed content are not
  retryable.
- The default retry policy permits three total attempts, uses deterministic exponential delays
  starting at one second, caps ordinary delay at 30 seconds, and accepts `Retry-After` up to 120
  seconds.
- Hard policy bounds allow at most five attempts, 60 seconds for ordinary retry or page pacing,
  and 300 seconds for `Retry-After`.
- A larger provider-mandated wait is not shortened or ignored. The workflow stops and reports
  `retry_after_exceeds_policy` so an operator or future scheduler can resume later.
- A missing, malformed, non-ASCII, or excessively large `Retry-After` value stops the workflow
  without guessing a replacement delay.
- Page results retain attempt count, exact retry delays, machine code, and terminal stop reason.
- A quality failure or storage conflict is never retried as a network problem.
- The first unaccepted page stops the range. Its start time is the resume boundary.

Every passing page is committed independently. Earlier pages remain durable when a later page
fails. Repeating an earlier page is safe because raw and canonical storage already provides
idempotent outcomes and explicit conflict quarantine.

## Safety Boundary

This decision authorizes only bounded, unauthenticated historical public-market-data reads. It does
not authorize:

- API keys, private account data, balances, positions, orders, or withdrawals.
- Continuous or unattended collection.
- Background scheduling, a service daemon, or a live WebSocket connection.
- Treating a partial range as complete.
- Retrying malformed data, quality failures, or storage conflicts.
- Unlimited waits, attempts, pages, records, or response sizes.

No current application entry point contacts Binance automatically. A caller must explicitly
construct the paginated ingestor, source, store, policies, and sleeper.

## Consequences

### Positive

- Large historical ranges have deterministic page boundaries and complete interval coverage.
- Transient failures are handled without retry storms or hidden sleeps.
- Rate-limit instructions are honored when safely bounded.
- Partial progress is explicit, durable, idempotent, and resumable.
- Retry evidence is available to future logs, metrics, schedulers, and Audit.
- The workflow remains provider-independent and adds no dependency.

### Negative

- A whole range is not atomic; callers must check the range result before treating it as complete.
- Fixed pacing does not know about traffic from other processes sharing the same IP.
- Deterministic backoff currently has no jitter.
- Resume position is returned but not yet stored as a durable collection job.
- An operator or future scheduler must decide when to resume after a wait exceeds policy.

## Alternatives Considered

### Retry inside the Binance adapter

Rejected. Hidden provider retries would make attempts and delays less observable and would mix
application policy with payload normalization.

### One transaction for the complete range

Rejected. It would hold a database transaction across network waits and discard useful accepted
progress after a late-page failure.

### Unlimited `Retry-After`

Rejected. An untrusted or unexpectedly large header must not block a worker for hours or days.
Stopping with an explicit reason preserves Binance's instruction without creating unbounded sleep.

### Parallel page downloads

Deferred. Concurrency complicates IP-rate budgeting, ordering, failure recovery, and provider load.
Sequential pages are easier to audit and sufficient for the current bounded slice.

## Review Triggers

Revisit this decision when adding continuous collection, durable job checkpoints, shared IP-rate
budgeting, adaptive pacing, retry jitter, parallel downloads, another provider, or a live stream.

## References

- [Binance Spot kline endpoint](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)
- [Binance Spot REST limits](https://developers.binance.com/en/docs/products/spot/rest-api#limits)
- [Binance USD-M Futures kline endpoint](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/market-data/rest-api/Kline-Candlestick-Data)
