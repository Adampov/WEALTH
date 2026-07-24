# ADR 0009: Coinbase Exchange Public Candles

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The first historical market-data path uses Binance. Phase 2 also requires evidence that canonical
contracts, quality gates, raw lineage, storage, replay, retries, and collection control are
provider-independent rather than Binance-specific.

Coinbase Exchange exposes unauthenticated historic Spot candles. Its response order and schema
differ from Binance, intervals without trades may be absent, and a response may include candles
before the requested start. The official endpoint rejects ranges above 300 data points and accepts
only a defined set of granularities.

## Decision

Add a second `HistoricalCandleSource` adapter for the public Coinbase Exchange product-candles
endpoint:

- Source identity is `coinbase.exchange-public-rest`.
- Venue identity is `COINBASE`.
- The default endpoint is
  `https://api.exchange.coinbase.com/products/{product_id}/candles`.
- Requests are unauthenticated HTTPS GETs and contain only `start`, `end`, and `granularity`.
- The adapter supports Spot instruments only.
- Supported canonical timeframes are one minute, five minutes, fifteen minutes, one hour, and one
  day.
- Four-hour candles fail before network access because Coinbase Exchange supports six hours rather
  than the canonical four-hour interval.
- One provider request contains at most 300 expected candles.
- Product IDs must be explicit uppercase `BASE-QUOTE` identifiers and cannot alter the URL path.
- The exact response bytes, digest, endpoint, request parameters, observation time, processing
  time, and lineage are retained.
- JSON decimal tokens are parsed without binary floating-point conversion.
- Each six-value provider row is structurally validated and converted to the existing canonical
  candle contract.
- Documented candles before `start` are retained in raw evidence but excluded from the canonical
  requested window.
- Candles at or after the exclusive end are rejected as an unexpected payload.
- In-window records are sorted by market time before the quality gate.
- Missing buckets are never filled. The existing quality gate reports the gap and prevents
  persistence of an incomplete requested batch.
- HTTP 429, transport failures, provider unavailability, provider rejection, malformed JSON,
  invalid decimals, clock regression, and invalid canonical values retain explicit safe
  classifications.

The adapter performs one bounded request and contains no hidden retries. Application retry,
pagination, pacing, and durable collection behavior remain outside the provider boundary.

## Safety Boundary

This decision authorizes only bounded, historical, public Coinbase Exchange Spot candle reads. It
does not authorize:

- API keys, authentication headers, account data, balances, positions, orders, or withdrawals.
- Coinbase Advanced Trade, International Exchange, Prime, or derivatives access.
- Automatic scheduling, continuous polling, or live WebSocket collection.
- Treating absent intervals as zero-volume candles.
- Accepting undocumented future or post-window rows.
- Bypassing the canonical quality or storage gates.
- Assuming one venue's prices or volumes are interchangeable with another venue.

## Consequences

### Positive

- The same application and domain ports now accept two materially different providers.
- Provider response ordering and extra pre-window rows cannot leak outside the requested window.
- Decimal values retain provider precision until canonical validation.
- Empty trading intervals remain explicit data-quality evidence.
- Existing durable storage and replay require no provider-specific change.
- Coinbase response bodies and error text remain untrusted and do not become instructions or logs.

### Negative

- Coinbase Exchange supports fewer canonical timeframes than the current domain.
- The 300-candle provider maximum requires smaller pagination policy than Binance.
- Sparse products can fail a complete-window quality gate because Coinbase publishes no bucket
  without ticks.
- Only Spot data is added; Coinbase futures require a different product and authorization review.
- A live stream and cross-venue comparison remain future work.

## Alternatives Considered

### Coinbase Advanced Trade public candles

Rejected for this slice because the documented endpoint requires bearer-token authorization. The
Exchange endpoint provides the required public Spot proof without credentials.

### Fill missing intervals with zero volume

Rejected. Coinbase explicitly documents that intervals without ticks may be absent. Inventing OHLC
values would hide missing source data and create false evidence.

### Accept Coinbase six-hour candles as canonical four-hour candles

Rejected. Different interval semantics cannot be relabeled safely.

### Normalize inside a shared Coinbase/Binance base class

Deferred. The providers have different endpoint, row, range, timeframe, and status behavior.
Keeping those rules in separate adapters avoids a premature abstraction.

## Review Triggers

Revisit this decision when adding Coinbase live streams, derivatives, instrument metadata,
cross-venue comparison, provider-specific rate policy, or additional canonical timeframes.

## References

- [Coinbase Exchange product candles](https://docs.cdp.coinbase.com/api-reference/exchange-api/rest-api/products/get-product-candles)
- [Coinbase Exchange REST requests](https://docs.cdp.coinbase.com/exchange/rest-api/requests)
- [Coinbase Exchange REST rate limits](https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits)
