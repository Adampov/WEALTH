# Market Data Contract — Phase 2 Foundation

## Purpose

This contract establishes provider-independent market-data records and the point-in-time boundary
required for honest research, replay, and backtesting.

It does not define a trading strategy and does not connect to an exchange.

## Canonical Order-Flow Foundation

`CanonicalTrade`, `CanonicalTicker`, and `CanonicalBestBidAsk` establish the provider-independent
target for future public trade and market-structure adapters.

All three retain source, venue, canonical instrument, instrument type, exchange event time, local
observation time, processing time, optional provider sequence, exact decimal values, and lineage.
Event time cannot follow observation, and observation cannot follow processing.

Canonical trades require provider identity, positive price and quantity, and an explicit aggressor
side of buy, sell, or unknown. Optional provider quote quantity remains separate from the exact
locally calculated notional.

Canonical tickers always contain a positive last price. Optional rolling-window statistics are
accepted only with an explicit valid window; supplied high and low must contain last price and any
supplied window open.

Canonical best-bid-ask snapshots require positive displayed quantities and a best bid strictly
below best ask. Exact spread, midpoint, and spread basis points are derived from the accepted
decimal prices.

These contracts do not yet have provider adapters, durable storage, replay, or live-stream
orchestration.

## Order-Flow Quality Gate

`OrderFlowSequenceAuditor` evaluates one exact trade, ticker, or best-bid-ask stream inside a
timezone-aware, half-open event-time window. One audit is capped at 100,000 input records and
detects mixed streams, out-of-window records, event-time regressions, equivalent duplicates, and
conflicting values for one natural key.

Provider-sequence guarantees are explicit rather than guessed. The default policy makes no
sequence claim. A documented monotonic policy requires sequences to be present and increasing. A
documented contiguous policy additionally reports exact absent integer ranges. Missing ranges are
therefore evidence-backed; the auditor never manufactures missing market values or assumes every
provider counter is contiguous.

`InMemoryOrderFlowStore` proves the replaceable persistence port. It namespaces identities by
record family, inserts the first canonical record, reports equivalent repeats as duplicates, and
reports changed values for one identity as conflicts. A duplicate or conflict never overwrites the
accepted record. Exact-stream queries are returned in deterministic market-time order.

`OrderFlowFetchBatch` binds one exact raw response to one record family. Source, venue, timestamps,
and raw lineage must agree across the batch, and one batch is capped at 100,000 records.

`SQLiteOrderFlowStore` adds a dedicated versioned file-backed implementation. Raw bytes and
canonical records are written atomically, equivalent new captures add lineage to the first record,
and changed values are quarantined without replacement. A database-type marker prevents another
SQLite store with the same integer version from being opened accidentally. Raw hashes, canonical
schemas, natural keys, record types, and stream indexes are revalidated when evidence is read.

`OrderFlowBatchIngestor` is the fail-closed admission path. It audits the complete bounded batch
before storage. A quality failure causes no raw or canonical write. A passing report permits the
atomic batch write, but a raw or canonical storage conflict keeps the overall result unaccepted.
Exact repeats remain accepted idempotent outcomes.

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

## Bounded Historical Pagination and Retry

`PaginatedHistoricalCandleIngestor` extends the single-window flow without changing the provider
or canonical candle contracts. It:

- Plans deterministic, contiguous pages with no overlap or gap.
- Keeps every provider request at or below 1,000 candles.
- Rejects one invocation above 100,000 candles before making a source request.
- Applies an explicit delay between successful pages.
- Retries only source failures classified as transient.
- Uses bounded exponential delays when the provider does not supply `Retry-After`.
- Honors `Retry-After` only when it is within the configured and hard safety bounds.
- Stops when a rate-limit response omits a usable `Retry-After`, rather than guessing a wait.
- Records attempts, retry delays, and the terminal retry stop reason in the page result.
- Never retries malformed payloads, invalid requests, unsupported instruments, quality failures, or
  storage conflicts.
- Stops at the first unaccepted page and returns its start time as the exact resume boundary.

Each passing page is quality-gated and stored transactionally before the next page begins. The
whole range is intentionally not one database transaction: completed pages remain durable after a
later source failure, and replaying them is idempotent. No current entry point starts this flow
automatically.

## Current Limitations

- Final candles are implemented end to end. Trade, ticker, and best-bid-ask records now have strict
  contracts, bounded quality auditing, fail-closed ingestion, and idempotent raw/canonical SQLite
  storage, but no provider adapter, live collection, or replay path.
- Each Binance provider request remains bounded to one already-closed window of at most 1,000
  candles; the application composes multiple requests into a bounded range.
- No operating-system-managed scheduling, deployment, adaptive pacing, retry jitter, or live
  WebSocket stream exists. Durable bounded and continuous checkpoints, a local interruptible
  service lifecycle, and shared single-host request-budget coordination are available.
- Collector lifecycle health and internal alert codes are queryable, but external delivery,
  acknowledgement, escalation, and automatic remediation are not implemented.
- A local JSON command exposes collector health from an existing database in enforced read-only
  mode; it does not control the service or create missing storage.
- No instrument catalog or governed provider-symbol mapping exists yet.
- Selected-window cross-source reconciliation and durable audit history exist, but no governed
  correction stream, automatic source ranking, or remediation workflow exists yet.
- Durable storage is local SQLite only; backup, retention, compaction, distributed operation, and
  large-scale analytical storage remain future work.
- Malformed or rejected provider responses are not yet retained under a governed failure-evidence
  policy.
- Full-depth order books, funding, open interest, and liquidation schemas remain future work.
