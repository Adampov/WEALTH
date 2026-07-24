# ADR 0020: Binance Public Aggregate-Trade Adapter

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit Department

## Context

The project has strict canonical order-flow contracts, deterministic quality auditing,
fail-closed admission, and durable raw-to-canonical storage, but no real provider has crossed that
boundary. The first provider slice must remain public, bounded, reproducible, and incapable of
account or order access.

Binance exposes public aggregate-trade REST endpoints for Spot and USD-M Futures. Each response row
can combine multiple underlying trades, so representing it as an ordinary individual trade would
discard material provider semantics. The endpoints also cap one response at 1,000 rows. A full
response therefore cannot prove that the requested time window is complete.

## Decision

Add `BinancePublicAggregateTradeSource` as a bounded implementation of
`PublicTradeWindowSource`.

### Public and bounded transport

- Spot uses the market-data-only `https://data-api.binance.vision/api/v3/aggTrades` endpoint.
- USD-M perpetual and dated futures use
  `https://fapi.binance.com/fapi/v1/aggTrades`.
- No API key, account endpoint, private state, WebSocket, or trading capability is present.
- Requests use finite timeouts, HTTPS, explicit uppercase provider symbols, millisecond-aligned
  half-open event-time windows, and a 1,000-row limit.
- One request must be shorter than one hour. USD-M requests must also remain inside the provider's
  documented latest-24-hour history boundary.

The adapter converts the canonical half-open window into Binance's inclusive millisecond
`startTime` and `endTime` parameters by subtracting one millisecond from the exclusive end.

### Honest aggregate semantics

`CanonicalTrade` gains explicit aggregation evidence:

- `aggregation_kind` distinguishes an individual observation from a provider-defined aggregate.
- `provider_first_trade_id` and `provider_last_trade_id` are required together for a
  provider-defined aggregate and forbidden for an individual observation.

For Binance rows, the aggregate trade ID is both the provider identity and monotonic sequence. The
underlying first and last trade IDs are retained. `buyer_is_maker=true` maps to a sell aggressor;
`false` maps to a buy aggressor. The adapter does not invent quote quantity from a local
calculation.

### Completeness and evidence

- Any malformed, unknown, contradictory, out-of-window, or canonically invalid row fails the
  entire fetch.
- A response containing 1,000 rows fails with `possibly_truncated`; callers must request a smaller
  window before claiming complete evidence.
- An empty array is a valid complete observation. `OrderFlowFetchBatch` therefore permits zero
  canonical records so the exact raw response can still pass through the quality gate and be
  persisted.
- Exact raw bytes, SHA-256 digest, request lineage, and deterministic canonical identities are
  retained.
- Provider errors have safe machine-readable classifications; untrusted response text is not
  copied into application errors.

The adapter only returns a canonical batch. Provider data still enters storage through
`OrderFlowBatchIngestor`, which applies the accepted quality gate before any write.

## Safety Boundary

This decision does not authorize:

- API keys, account access, balances, positions, private streams, or order submission.
- Continuous polling, automatic startup, live WebSocket collection, or provider gap recovery.
- Silent pagination, truncation acceptance, missing-trade synthesis, sorting, or correction.
- Treating aggregate trades as individual executions.
- Features, signals, recommendations, portfolio actions, or trading actions.

## Consequences

### Positive

- A real public trade provider now crosses the complete raw, normalization, quality, and durable
  admission path.
- Provider aggregation is visible and cannot be mistaken for an individual market execution.
- A full provider page cannot silently enter research as a complete time window.
- Empty markets retain positive raw evidence rather than becoming indistinguishable from a failed
  fetch.
- Spot and USD-M share one provider-independent request and canonical contract.

### Negative

- Exact 1,000-row windows are rejected even when they may happen to be complete.
- Callers must shrink dense windows and must respect the short USD-M history boundary.
- Aggregate trades cannot reconstruct every underlying execution.
- No bounded range planner, adaptive window splitter, scheduler, or live recovery exists yet.
- Strict field-set validation may require an explicit adapter update if Binance evolves the
  response schema.

## Alternatives Considered

### Treat each aggregate as an individual trade

Rejected. It would erase the provider's first/last underlying trade identities and misstate the
evidence.

### Accept a 1,000-row response and continue from the last ID

Rejected for this slice. It would add provider-specific pagination and boundary semantics before
the single-window adapter has a proven completeness contract.

### Fetch underlying individual trades

Deferred. Binance's bounded public aggregate endpoint is sufficient to prove the first provider
path, while individual-trade history and licensing or retention requirements need a separate
decision.

### Store no record for an empty response

Rejected. The raw response is evidence that the provider returned no aggregate trade in the
requested window.

## Review Triggers

Revisit this decision when adding adaptive window splitting, ID-based pagination, live WebSocket
collection, provider-specific gap recovery, another trade provider, point-in-time replay, or
measured throughput and retention requirements.

## References

- [Binance Spot aggregate trades](https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#compressedaggregate-trades-list)
- [Binance USD-M Futures aggregate trades](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data#compressed-aggregate-trades-list)
