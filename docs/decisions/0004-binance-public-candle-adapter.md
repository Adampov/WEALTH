# ADR 0004: Binance Public Candle Adapter

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, and Engineering Department

## Context

Phase 2 requires a first exchange adapter that proves provider data can cross an untrusted network
boundary, become canonical records, pass deterministic quality checks, and remain replaceable by a
second provider.

The first slice must support both spot and cryptocurrency futures without introducing account
access, credentials, trading permissions, order endpoints, a broad exchange framework, or
unbounded historical downloads.

Binance documents public kline endpoints for Spot and USD-M Futures. The Spot API also provides a
market-data-only base endpoint. Both kline responses use positional arrays and provider-specific
inclusive close timestamps, so strict normalization is required before the data can enter the
domain.

## Decision

Use Binance as the first public market-data provider for one bounded historical candle slice:

- Spot candles use `https://data-api.binance.vision/api/v3/klines`.
- USD-M futures candles use `https://fapi.binance.com/fapi/v1/klines`.
- Requests are unauthenticated HTTP GET operations and never include an API key or signature.
- One request must describe an explicit UTC-aligned, already-closed window of at most 1,000
  candles.
- Canonical and provider symbols are supplied separately so Binance naming does not become the
  downstream instrument contract.
- Spot, perpetual-future, and dated-future records share the existing `CanonicalCandle` contract.
- Binance's inclusive close timestamp is validated and converted to the canonical exclusive
  interval boundary.
- Observation and processing times are injected through the existing clock port.
- Record IDs are deterministic for exact provider content; a changed provider row therefore
  becomes a visible conflict instead of an invisible overwrite.
- Provider payloads, symbols, timestamps, numbers, response sizes, and HTTP statuses are validated
  at the adapter boundary.
- Rate limits, provider unavailability, provider rejection, malformed payloads, and transport
  failures use explicit machine-readable error codes.
- A complete fetched window must pass the existing sequence-quality gate before any records are
  offered to storage.
- Unit and integration tests use injected responses and make no network calls.

The HTTP implementation uses the Python standard library for this narrow slice. Network access
remains behind a typed port, has a finite timeout and response-size limit, and performs no implicit
retry.

## Safety Boundary

This decision authorizes public market-data reads only. It does not authorize:

- API keys, account data, balances, positions, or private endpoints.
- Order creation, cancellation, modification, or any other financial action.
- Withdrawal permission or credential storage.
- Continuous streaming or unattended runtime operation.
- Treating fetched data as a recommendation or permission to trade.

No current application entry point automatically contacts Binance.

## Consequences

### Positive

- The first real provider is isolated behind stable request and source ports.
- The quality gate is exercised with the same canonical records used by replay and research.
- Repeated fetches are idempotent while provider revisions remain observable.
- A future second exchange can implement the source port without changing downstream candle logic.
- The slice adds no runtime dependency and no secret-management surface.

### Negative

- A single request is limited to 1,000 candles.
- There is no pagination, retry, rate-limit scheduler, reconnect policy, or live WebSocket stream.
- Provider-symbol mapping is supplied by the caller until the instrument catalog is implemented.
- The standard-library HTTP adapter is intentionally small and may later be replaced behind the
  same port if measured operational needs justify it.

## Alternatives Considered

### CCXT as the first adapter

Deferred. CCXT may be useful for broader exchange coverage, but introducing a broad abstraction
before the canonical boundary is proven would make it harder to see which validation and behavior
belong to WEALTH versus the library.

### Official Binance SDK

Deferred. The first slice needs two public GET endpoints only. An SDK adds dependency and upgrade
surface without yet providing required value.

### Provider-specific records downstream

Rejected. Positional Binance payloads must not escape the adapter or define replay, research, or
future strategy contracts.

### Automatic retries in the adapter

Deferred. Retry and backoff require an explicit policy, attempt observability, rate-limit handling,
and deterministic tests. The current adapter surfaces enough structured information for that
future task without retrying invisibly.

## Review Triggers

Revisit this decision when implementing pagination, live WebSocket ingestion, an instrument
catalog, a second exchange adapter, measured retry policy, or provider-specific operational
requirements.

## References

- [Binance Spot market REST API](https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/rest-api/market)
- [Binance USD-M Futures kline documentation](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/market-data/rest-api/Kline-Candlestick-Data)
