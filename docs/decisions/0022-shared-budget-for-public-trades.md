# ADR 0022: Shared Request Budget for Public Trades

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit Department

## Context

ADR 0008 introduced durable weighted request-budget coordination and a source decorator for
historical candles. Public aggregate-trade range ingestion now performs multiple requests and
retries, but without the same decorator it could compete with candle workers sharing one outbound
IP and exceed their combined configured capacity.

Binance documents different request weights for the public aggregate-trade endpoints: 4 for Spot
and 20 for USD-M Futures. The application must preserve those explicit provider values without
embedding provider policy in its generic range orchestration.

## Decision

Add `RateBudgetedPublicTradeSource`, a provider-independent decorator around
`PublicTradeWindowSource`.

Before every delegated fetch it creates a unique durable weighted reservation through the existing
`RateBudgetCoordinator`. The request cost is mandatory configuration, must be positive, and cannot
exceed the selected policy capacity.

If the reservation is granted, the decorator performs exactly one delegated source fetch. If it is
denied, the decorator performs no network access and raises a provider-neutral
`PublicTradeSourceError` with:

- Machine code `local_rate_budget_exhausted`.
- Explicit retryable classification.
- The coordinator's bounded `retry_after_seconds`.
- No smaller-window instruction.

`AdaptivePublicTradeRangeIngestor` therefore treats a local denial through its existing bounded
retry policy. The denial and subsequent attempt both consume the range's source-request limit, but
only a granted reservation reaches Binance.

Expose documented Binance request-weight constants:

- Spot aggregate trades: 4.
- USD-M aggregate trades: 20.

Callers configure wrappers for the instrument endpoint they use. Workloads share capacity only
when they intentionally use the same coordinator database and `budget_key`.

The candle decorator is refactored to use the same private reservation operation without changing
its public behavior.

## Safety Boundary

This decision does not authorize:

- Guessing a provider limit, capacity, period, request cost, or shared budget key.
- Treating a local grant as permission to ignore HTTP 429, `Retry-After`, or any provider response.
- Retrying outside the range policy's attempt, delay, and request limits.
- Coordination across hosts that do not share the database and a reliable clock.
- API keys, private data, background scheduling, live streams, or trading.

## Consequences

### Positive

- Candle and aggregate-trade workers can consume one durable weighted capacity envelope.
- Local exhaustion is visible, retryable, and stops network access before provider pressure grows.
- Spot and USD-M weights are explicit and testable rather than silently assumed equal.
- Existing range traces preserve attempts and wait evidence without a second retry mechanism.
- The coordinator's durable summaries include aggregate-trade grants, denials, requested cost, and
  wait pressure.

### Negative

- Correct capacity, period, budget key, and endpoint cost remain operator configuration.
- A conservative USD-M cost consumes five times the Spot cost.
- SQLite remains a single-host coordination baseline.
- Budget reservations are durable even when the later provider request fails.
- Provider header feedback does not yet adapt the configured local policy.

## Alternatives Considered

### Rate-limit inside the Binance adapter

Rejected. The adapter cannot see candle workers or other processes and should still represent one
network operation.

### Use cost one for every public request

Rejected. It would contradict documented endpoint weights and undercount expensive futures
requests.

### Give order flow a separate budget key by default

Rejected. Separate keys would not protect the combined shared-IP capacity. Separation remains an
explicit operator choice when provider limits genuinely differ.

## Review Triggers

Revisit this decision when adding provider-header adaptation, multi-host coordination, automatic
policy configuration, another trade provider, continuous order-flow collection, or retention for
reservation history.

## References

- [Binance Spot aggregate trades](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#compressedaggregate-trades-list)
- [Binance USD-M Futures aggregate trades](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#compressed-aggregate-trades-list)
