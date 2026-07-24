# Market Data Contract — First Slice

## Purpose

This contract establishes the first provider-independent market-data record and the point-in-time
boundary required for honest research, replay, and backtesting.

It does not define a trading strategy and does not connect to an exchange.

## Canonical Candle

`CanonicalCandle` represents one final OHLCV interval. Every record includes:

- Schema version and unique record ID.
- Source, venue, instrument, and instrument type.
- Timeframe, open time, and close time.
- Observation and processing times.
- Exact decimal OHLCV values.
- Provider sequence when available.
- One or more lineage references.

The contract rejects:

- Unknown fields or mutable records.
- Non-positive prices or negative volume.
- OHLC values that contradict one another.
- Intervals that do not match their declared timeframe.
- Candles reported as observed before they closed.
- Processing timestamps earlier than observation timestamps.
- Partial candles.

## Point-in-Time Replay

`MarketReplay.slice_at(evaluation_time)` returns only records whose `observed_at` is less than or
equal to the evaluation time. A candle that closed earlier but arrived late remains unavailable
until its actual observation time.

Replay input is sorted deterministically. Duplicate natural keys and conflicting revisions fail
closed with machine-readable reason codes.

This boundary is mandatory for future features, signals, strategies, backtests, evaluation, and
learning. No analytical component may receive the replay object's complete future record set.

## Candle Quality Gate

`CandleSequenceAuditor` evaluates one explicit stream and expected time window. It:

- Requires the window and every candle to align to the timeframe's UTC grid.
- Detects input that regresses in market time.
- Detects records from another source, venue, instrument, instrument type, or timeframe.
- Detects records outside the expected window.
- Distinguishes identical duplicates from conflicting values.
- Collapses absent or unusable intervals into explicit contiguous missing ranges.
- Caps audit-window size to prevent accidental unbounded memory use.

A conflict is not selected arbitrarily. Its interval remains unusable and is represented as
missing until a future governed correction mechanism resolves it.

## Idempotent Temporary Storage

`InMemoryCandleStore` proves the persistence contract before a durable storage technology is
selected. The first record for a natural key is inserted. A repeated equivalent record returns
`DUPLICATE`; a different record for the same key returns `CONFLICT`. Neither outcome overwrites the
stored record.

## Raw Evidence and Durable Storage

Every successful `CandleFetchBatch` includes one `RawMarketPayload` containing the exact bounded
provider-response bytes, a SHA-256 digest, observation and processing times, source identity, and
provenance. Every canonical candle in the batch must reference that raw payload ID in its lineage.

`SQLiteCandleStore` is the first durable implementation of the storage port. It:

- Stores raw response bytes separately from canonical candles.
- Commits one accepted batch transactionally.
- Revalidates raw content hashes and canonical schemas when records are read.
- Preserves exact decimal values through the canonical serialized record.
- Enforces one canonical record per provider-scoped natural key.
- Treats equivalent canonical values as idempotent duplicates while retaining each raw capture.
- Links every equivalent raw capture to the accepted canonical record.
- Keeps the original canonical record and quarantines a conflicting incoming revision.
- Versions its local schema and rejects unknown versions without an implicit migration.

The SQLite adapter is a replaceable Phase 2 local durability baseline. It is not yet the final
high-volume operational or analytical storage design.

## Public Binance Historical Adapter

`BinancePublicCandleSource` is the first real provider implementation of the historical-candle
source port. It reads bounded, already-closed windows from unauthenticated Binance public REST
endpoints:

- Spot through Binance's market-data-only host.
- USD-M perpetual and dated futures through the public futures host.

The adapter accepts separate canonical and provider symbols, forces Spot intervals onto the UTC
grid, uses a finite timeout and response-size limit, and validates every positional response field.
It converts Binance's inclusive final-millisecond close timestamp to the canonical exclusive
interval boundary.

Provider rows receive deterministic content-derived record IDs and explicit lineage. Exact repeated
content is therefore idempotent, while a changed row for the same natural key remains a visible
conflict.

Rate limits, provider rejection, provider unavailability, transport failure, malformed JSON, and
canonical-contract violations fail with machine-readable reason codes. Untrusted provider error
text is not copied into application errors.

`HistoricalCandleIngestor` sends the complete fetched batch through `CandleSequenceAuditor`. A
batch with a gap, duplicate, conflict, mixed stream, out-of-order record, or out-of-window record is
reported and not written to storage. A passing batch persists its exact raw response and canonical
records together. A storage conflict makes the ingestion result unaccepted and remains explicit in
the write outcomes and conflict quarantine.

## Current Limitations

- Only final candles are modeled.
- Binance reads are bounded to one already-closed window of at most 1,000 candles.
- No pagination, automatic retry/backoff, scheduled collection, or live WebSocket stream exists.
- No instrument catalog or governed provider-symbol mapping exists yet.
- No governed correction stream or cross-source reconciliation exists yet.
- Durable storage is local SQLite only; backup, retention, compaction, distributed operation, and
  large-scale analytical storage remain future work.
- Malformed or rejected provider responses are not yet retained under a governed failure-evidence
  policy.
- Trades, ticker, order book, funding, open interest, and liquidation schemas remain future work.
