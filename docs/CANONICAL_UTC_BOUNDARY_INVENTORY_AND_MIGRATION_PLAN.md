# Canonical UTC Boundary Inventory and Migration Plan

- **Inventory date:** 2026-07-25
- **Task:** TASK-026 — `phase2.canonical_utc_boundary_inventory_and_migration_plan`
- **Decision:** [ADR 0027](decisions/0027-canonical-utc-boundary-and-migration-plan.md)
- **Risk:** `RISK-005`
- **Scope:** Repository evidence, target contract, and staged migration plan only

## Outcome

The repository does not yet have one global UTC contract.

Three boundaries are explicitly zero-offset-only today: `DomainEvent`, `ProjectState`, and the
typed public-trade transition-history envelope. The bounded public-trade orchestrator also rejects
nonzero-offset request, clock, creation, and loaded-checkpoint values before it proceeds. Those
checks do not yet prove fixed `datetime.UTC` zone identity. Most other timestamp-bearing models
use Pydantic `AwareDatetime`, which rejects naive values but accepts and retains any numeric
offset.

That distinction matters because Python compares aware datetimes as instants, while several
SQLite stores persist and order their original ISO 8601 text. Two values that denote the same
instant can therefore:

- compare equal or share a dictionary key in memory;
- serialize to different JSON bytes;
- produce different digests or natural-key text;
- occupy different SQLite keys; or
- sort in the wrong chronological order when their offsets differ.

The newer public-trade control and reconciliation stores normalize selected indexed projections
to UTC, but their canonical JSON can still retain the caller's offset. A normalized index
therefore does not prove that the canonical record is UTC.

Completing TASK-026 accepts the target and migration sequence below. It does **not** change a
runtime contract, database schema, stored record, provider interaction, or application behavior.

## Classification

| Mark | Meaning |
| --- | --- |
| **Strict UTC** | The current boundary rejects naive values and any value whose UTC offset is not zero. This does not yet prove the target fixed `datetime.UTC` timezone identity. |
| **Aware only** | Naive values are rejected, but a nonzero offset is accepted and retained. |
| **Edge normalized** | An adapter converts an aware value to UTC for one provider or projection boundary; the source model may still retain its original offset. |
| **Causal order** | Version or sequence, rather than timestamp text, defines durable order. |
| **High** | Can change canonical evidence, identity, digest, durable control, query membership, or chronology. |
| **Medium** | Primarily affects derived/in-memory evidence or an external text view, but can propagate into a high-impact store. |
| **Low** | Already UTC-strict or isolated, with a remaining representation-consistency gap. |

## Target Canonical Contract

The target contract has one logical instant and explicit physical encodings:

1. **Canonical Python value:** a timezone-aware `datetime` whose `tzinfo` is the fixed
   `datetime.UTC` singleton. Checking only `utcoffset() == timedelta(0)` is insufficient because a
   regional or rule-based timezone can be zero-offset for one date and change after arithmetic.
   Internal domain and application boundaries reject naive values, nonzero offsets, and
   zero-offset non-UTC zones. A provider adapter, canonical-text decoder, or versioned
   legacy-reader edge may explicitly normalize an aware input with `astimezone(UTC)`; canonical
   domain code must not normalize silently.
2. **Canonical JSON/text value:** RFC 3339 UTC with a literal `Z` and exactly six fractional
   digits, for example `2026-07-25T09:00:00.123000Z`. The decoder produces a `datetime.UTC`
   instance. Fixed microsecond precision preserves the precision already supported by Python and
   prevents multiple text encodings of one value.
3. **Sortable SQLite projection:** signed integer microseconds since the Unix epoch. It is a
   derived, checked projection, not a second canonical truth source. Every query adds an explicit
   deterministic tie-break such as record ID, observation ID, version, or sequence.
4. **Provider input:** documented epoch seconds, epoch milliseconds, or provider RFC 3339 remains
   an untrusted edge encoding. The adapter range-checks it, converts it to UTC, and retains the
   original provider bytes as raw evidence.
5. **Presentation time:** `Asia/Jerusalem` conversion is permitted only in a presentation layer.
   It is not stored in canonical records.

Existing `TEXT` projections and version-1 JSON remain legacy encodings until a separately
approved migration verifies and replaces them. Raw provider response bytes are immutable evidence
and are never rewritten merely to normalize a timestamp inside the provider payload.

## Timestamp-Bearing Model Inventory

Every class with a direct datetime annotation discoverable in `src/wealth` is listed below. The
range-result and CLI-output wrappers that derive or serialize timestamps are also called out
explicitly. Other nested result types that only carry one of these models inherit its
classification.

| Owner and evidence | Models and timestamp fields | Current guarantee and behavior | Impact and proposed treatment |
| --- | --- | --- | --- |
| Foundation domain — [`events.py`](../src/wealth/domain/events.py#L27-L59) | `DomainEvent`: `event_time`, `observed_at`, `processed_at`; `timestamp` returns `event_time` | **Strict UTC** and `event_time <= observed_at <= processed_at` | **Low.** Move to the shared codec without weakening rejection; lock fixed text serialization. |
| Repository governance — [`project_state.py`](../src/wealth/domain/project_state.py#L81-L117) | `ProjectState.last_updated_utc` | **Strict UTC**; `PROJECT_STATE.json` is validated on load | **Low.** Retain strict validation and adopt the fixed serializer only through a versioned state-contract change. |
| Market Data domain — [`market.py`](../src/wealth/domain/market.py#L51-L195) | `RawMarketPayload`: `observed_at`, `processed_at`; `CanonicalCandle`: `open_time`, `close_time`, `observed_at`, `processed_at` | **Aware only.** UTC conversion is used only for candle-grid arithmetic. `open_time` remains in the natural key with its caller representation. | **High.** Make all fields strict UTC in the core-evidence wave; normalize only provider and legacy ingress before construction. |
| Market Data domain — [`order_flow.py`](../src/wealth/domain/order_flow.py#L29-L255) | `_CanonicalTimedMarketRecord` defines `event_time`, `observed_at`, `processed_at`; `CanonicalTrade`, `CanonicalTicker`, and `CanonicalBestBidAsk` inherit them; ticker adds `window_start`, `window_end` | **Aware only.** Python causal comparison is instant-aware. Ticker and best-bid/ask natural keys include the original `event_time`. | **High.** Move inherited and ticker-window fields to strict UTC before migrating natural keys; detect canonical-identity collisions. |
| Candle quality — [`quality.py`](../src/wealth/domain/quality.py#L100-L198) | `CandleQualityIssue.open_time`; `MissingCandleRange.start_open_time`, `end_open_time_exclusive`; `CandleSequenceReport.window_start`, `window_end_exclusive`; `CandleConflictRecord.open_time`, `detected_at` | **Aware only.** Ranges and conflicts compare instants; returned models preserve offsets. | **Medium/High.** Convert after core candle contracts; version any persisted derived representation. |
| Order-flow quality — [`order_flow_quality.py`](../src/wealth/domain/order_flow_quality.py#L114-L251) | `OrderFlowQualityIssue.event_time`; `OrderFlowSequenceReport.window_start`, `window_end_exclusive`; `OrderFlowConflictRecord.detected_at`; `order_flow_sort_key` uses `event_time` | **Aware only.** Python ordering is chronological, but JSON remains offset-sensitive. | **Medium/High.** Convert after core order-flow contracts and preserve the ID/sequence tie-break. |
| Reconciliation domain — [`reconciliation.py`](../src/wealth/domain/reconciliation.py#L46-L193) | `CandleIntervalComparison.open_time`; `CandleReconciliationIssue.open_time`; `CandleReconciliationReport.window_start`, `window_end_exclusive` | **Aware only.** Comparisons are sorted by Python datetime; reports retain original offsets. | **High.** Strict UTC must precede history migration because report JSON is digest input. |
| Reconciliation history — [`reconciliation_history.py`](../src/wealth/domain/reconciliation_history.py#L20-L184) | `ReconciliationObservation.recorded_at`; `ReconciliationObservationQuery` and `ReconciliationSummaryQuery`: `recorded_start`, `recorded_end_exclusive`; `ReconciliationHistorySummary`: those bounds plus `first_recorded_at`, `last_recorded_at` | **Aware only.** `report_sha256` covers exact offset-preserving report JSON. | **High.** Introduce an explicit serialization/digest version; never recompute or overwrite a version-1 digest in place. |
| Shared rate control — [`rate_budget.py`](../src/wealth/domain/rate_budget.py#L54-L108) | `RateBudgetRequest.requested_at`; `RateBudgetDecision.requested_at`, `theoretical_arrival_at` | **Aware only.** Decision causality uses Python datetime comparison. | **High/Medium.** Make clock/request strict after the compatibility reader can preserve old decision JSON. |
| Historical collection domain — [`collection.py`](../src/wealth/domain/collection.py#L38-L180) | `HistoricalCollectionJob`: window, cursor, creation/update, lease expiry; `SourceHealthObservation`: page bounds and observation time | **Aware only.** Range, lease, progress, and health comparisons use Python instants. | **High.** Durable control JSON and text projections require a versioned reader and migration before validation tightens. |
| Continuous collection domain — [`continuous_collection.py`](../src/wealth/domain/continuous_collection.py#L29-L150) | `ContinuousCollectionRequest.window_start`; `ContinuousCollectionCheckpoint`: start/cursor, creation/update, active end, retry time | **Aware only.** Start is converted to UTC only for grid arithmetic. | **High.** Migrate checkpoint and retry projections together; do not strand a recoverable active job. |
| Collector-service domain — [`collector_service.py`](../src/wealth/domain/collector_service.py#L45-L275) | `CollectorServiceHeartbeat`: `observed_at`, `next_window_start`; `CollectorServiceHealthAssessment.evaluated_at`; `CollectorServiceHealthReport.evaluated_at` | **Aware only.** Sequence orders history; Python subtraction computes age; report checks newest-first by datetime. | **High/Medium.** Keep sequence as causal order, migrate heartbeat text/JSON, and make service/health clocks strict. |
| Public-trade control domain — [`order_flow_collection.py`](../src/wealth/domain/order_flow_collection.py#L27-L340) | `PublicTradeCollectionCheckpoint`: range/cursor/pending, creation/update, lease expiry; `PublicTradeSourceHealthObservation`: range/cursor/pending and observation time | **Aware only.** Millisecond alignment and causal comparisons do not require zero offset. Direct model/store use can retain offsets. | **High.** Treat as the control-store pilot because projections and version cursors already reduce ordering risk; preserve legacy health evidence until migration. |
| Public-trade audit domain — [`order_flow_collection.py`](../src/wealth/domain/order_flow_collection.py#L166-L195) | `PublicTradeCollectionTransition` wraps every checkpoint timestamp | **Strict UTC** at the typed history-reader boundary | **Low locally / High ecosystem inconsistency.** Retain this defense while underlying checkpoint and health contracts migrate. |
| Market source port — [`ports/market.py`](../src/wealth/ports/market.py#L46-L137) | `HistoricalCandleRequest.window_start`, `window_end_exclusive`; `CandleFetchBatch.observed_at`, `processed_at` | **Aware only.** Request start is converted to UTC only for grid validation; batch equality preserves offsets. | **High.** Make requests/batches strict in the core-evidence wave; provider adapters normalize before constructing them. |
| Order-flow source port — [`ports/order_flow.py`](../src/wealth/ports/order_flow.py#L28-L126) | `PublicTradeWindowRequest.window_start`, `window_end_exclusive`; `OrderFlowFetchBatch.observed_at`, `processed_at` | **Aware only.** Millisecond alignment and causal checks retain offsets. | **High.** Make the source/batch contract strict after legacy callers are inventoried. |
| Foundation clock — [`ports/foundation.py`](../src/wealth/ports/foundation.py#L1-L54), [`adapters/foundation.py`](../src/wealth/adapters/foundation.py#L30-L38) | `Clock.now()` returns plain `datetime`; concrete `SystemClock.now()` calls `datetime.now(UTC)` | **Strict fixed UTC at use boundaries.** The protocol documents `tzinfo is datetime.UTC`; `require_utc_clock` checks identity and returns the accepted value unchanged. `SystemClock` conforms. | **Controlled for new clock output / High ecosystem migration remains.** Keep conformance coverage and do not use the clock-only assertion to tighten legacy request or persisted-model inputs. |
| Replay application — [`replay.py`](../src/wealth/application/replay.py#L27-L103) | `ReplaySlice.evaluation_time`, `next_observation_time`; `MarketReplay.slice_at` input | Plain dataclass; rejects naive evaluation time only. Python filtering/sorting is instant-correct and returns original offsets. No serializer is defined. | **High for honest replay.** Make evaluation strict UTC and define serialization only if the dataclass becomes an external boundary. |
| Range-result wrappers — [`pagination.py`](../src/wealth/application/pagination.py#L240-L269), [`order_flow_range.py`](../src/wealth/application/order_flow_range.py#L310-L377) | `HistoricalCandleRangeIngestionResult.next_window_start` and `PublicTradeRangeIngestionResult.next_window_start` derive a resume timestamp from nested requests/traces | They preserve the selected nested request's aware offset and define no serializer. | **Medium.** Their derived cursor must become fixed UTC when the underlying request contracts migrate; retain explicit causal prefix logic. |
| Collector-health command envelope — [`collector_health_cli.py`](../src/wealth/collector_health_cli.py#L46-L78) | `CollectorHealthCommandOutput` serializes a timestamp-bearing `CollectorServiceHealthReport` and its assessments | Nested `evaluated_at` values are aware-only and can expose caller offsets in external JSON. | **High/Medium external compatibility.** Version the command output before adopting the fixed serializer and inventory exact-byte consumers. |

## Application Comparison and Clock Inventory

| Application boundary | Comparison, normalization, and output behavior | Gap and staged treatment |
| --- | --- | --- |
| Candle quality and ingestion — [`quality.py`](../src/wealth/application/quality.py#L39-L237), [`ingestion.py`](../src/wealth/application/ingestion.py#L44-L77) | Windows are aware-only; UTC is used only for grid arithmetic. Records are keyed and sorted by Python `open_time`, and reports preserve input offsets. | Equivalent instants behave chronologically in memory but may produce different report JSON or storage identity. Convert after the shared codec and core candle model. |
| Order-flow quality and ingestion — [`order_flow_quality.py`](../src/wealth/application/order_flow_quality.py#L39-L238), [`order_flow_ingestion.py`](../src/wealth/application/order_flow_ingestion.py#L42-L72) | Windows are aware-only; event comparisons and issue order use Python datetimes. | Make request and record contracts strict before changing the persisted conflict or natural-key representation. |
| Cross-source reconciliation — [`reconciliation.py`](../src/wealth/application/reconciliation.py#L49-L233) | Window offsets are preserved; dictionaries and comparison order use Python instants. | A representation-only change alters report digest bytes. Introduce digest versioning first. |
| Candle and trade page/range planning — [`pagination.py`](../src/wealth/application/pagination.py#L148-L266), [`order_flow_range.py`](../src/wealth/application/order_flow_range.py#L245-L376) | Timed request objects are split, compared, and resumed without normalizing their original offsets. | Tighten the port contract after callers have a normalization edge; keep causal resume cursors explicit. |
| Recoverable historical collection — [`collection.py`](../src/wealth/application/collection.py#L83-L389) | Every direct clock result is checked for fixed `datetime.UTC` before ID or checkpoint/health mutation. Optional caller `created_at` and loaded durable state retain their current aware-only contract. | New clock drift is controlled. Add legacy checkpoint reading later, before tightening the persisted model itself. |
| Continuous candle collection — [`continuous_collection.py`](../src/wealth/application/continuous_collection.py#L65-L129), [`continuous_collection.py`](../src/wealth/application/continuous_collection.py#L186-L478) | Every creation, cycle, pause/resume, success, and failure clock result is fixed-UTC checked before the next cursor mutation. Settlement policy inputs and persisted checkpoint fields retain current acceptance. | New clock drift is controlled; later migrate checkpoint and retry representations as one atomic unit. |
| Collector service and health — [`collector_service.py`](../src/wealth/application/collector_service.py#L67-L217), [`collector_health.py`](../src/wealth/application/collector_health.py#L50-L135) | Service and evaluation clocks require fixed `datetime.UTC` before heartbeat IDs, writes, or health-store reads. Heartbeat age and freshness still use Python instants. | Clock output is controlled; retain sequence as durable history order and use ID as equal-time tie-break during later representation migration. |
| Shared rate budget — [`rate_budget.py`](../src/wealth/application/rate_budget.py#L105-L128) | The clock is fixed-UTC checked before reservation ID generation or coordinator access; the accepted value becomes `RateBudgetRequest.requested_at`. | New clock drift is controlled; retain version-1 decision compatibility until the later storage wave. |
| Foundation health event — [`health.py`](../src/wealth/application/health.py#L18-L35) | The clock is fixed-UTC checked before either event ID, storage, or logging; the accepted value becomes all three `DomainEvent` times. | Enforced without changing the strict event contract or its representation. |
| Public-trade orchestration — [`public_trade_collection.py`](../src/wealth/application/public_trade_collection.py#L241-L616), [`public_trade_collection.py`](../src/wealth/application/public_trade_collection.py#L719-L729) | Direct trusted-clock and shared-budget clock reads require fixed `datetime.UTC` and preserve `PublicTradeCollectionClockError`. Request bounds, explicit creation, loaded checkpoints, and lease values retain their existing zero-offset acceptance. | New clock drift is controlled without tightening compatibility boundaries; replace broader local validation only in a later approved wave. |

For the fixed numeric-offset values produced by current serializers and tests, Python comparison,
subtraction, hashing, and equality are generally instant-based. This is not universal for
regional timezones: same-`tzinfo` and `fold` rules can produce different equality or arithmetic
behavior. That is why regional and rule-based zones are not canonical even when their offset is
zero at one instant. For ordinary fixed-offset inputs, an equal instant can still collapse into
one dictionary identity while its original JSON/text remains different. Migration tests must
cover fixed offsets, regional/fold cases, and representation identity together.

## Provider and External Text Inventory

| Boundary and evidence | Input/output representation | Current UTC behavior | Treatment |
| --- | --- | --- | --- |
| Binance candles — [`binance.py`](../src/wealth/adapters/binance.py#L123-L212), [`binance.py`](../src/wealth/adapters/binance.py#L307-L442) | Request bounds become epoch milliseconds; Spot requests specify UTC; provider epoch milliseconds become UTC datetimes. Capture times come from an injected clock. | Provider market time is **edge normalized**. Every capture clock read is fixed-UTC checked and maps failures to `BinanceCandleErrorCode.INVALID_REQUEST`. | Range-check provider integers before conversion and construct strict UTC models in the later core-evidence wave; keep raw bytes unchanged. |
| Coinbase candles — [`coinbase.py`](../src/wealth/adapters/coinbase.py#L121-L229), [`coinbase.py`](../src/wealth/adapters/coinbase.py#L316-L446) | Request bounds become UTC `Z` seconds; provider epoch seconds become UTC datetimes. | Provider market time is **edge normalized**. Every capture clock read is fixed-UTC checked and maps failures to `CoinbaseCandleErrorCode.INVALID_REQUEST`. | Preserve documented second precision and range-check provider input in the later core-evidence wave. |
| Binance aggregate trades — [`binance_order_flow.py`](../src/wealth/adapters/binance_order_flow.py#L133-L239), [`binance_order_flow.py`](../src/wealth/adapters/binance_order_flow.py#L340-L514) | Request bounds become epoch milliseconds; provider event milliseconds become UTC. | Provider event time is **edge normalized**. Every capture clock read is fixed-UTC checked and maps failures to `BinanceAggregateTradeErrorCode.INVALID_REQUEST`. | Keep provider raw bytes and aggregation evidence unchanged; add provider-range hardening in the later adapter wave. |
| HTTP `Retry-After` — [`binance.py`](../src/wealth/adapters/binance.py#L436-L440), [`coinbase.py`](../src/wealth/adapters/coinbase.py#L440-L444), [`binance_order_flow.py`](../src/wealth/adapters/binance_order_flow.py#L509-L513) | At most ten ASCII digits interpreted as delay seconds; HTTP-date is not supported. | This is a duration/text boundary, not an internal instant. Malformed or absolute-date values are not converted. | Keep duration behavior explicit. Any future HTTP-date support must parse at the adapter edge and immediately normalize to the canonical instant. |
| Structured logs — [`logging.py`](../src/wealth/observability/logging.py#L14-L44) | Log-record epoch float becomes UTC ISO text with `+00:00`; strict domain events are JSON-dumped, normally with `Z`. | UTC instant is guaranteed, but two textual conventions and variable fractional precision remain. | Use the canonical serializer in a versioned log-envelope change; do not reinterpret historical log bytes. |
| Collector-health CLI — [`collector_health_cli.py`](../src/wealth/collector_health_cli.py#L253-L267) | Pydantic JSON-mode output with no extra datetime normalization | Stored heartbeat/report offsets can become externally visible. | Version the CLI output before fixed UTC serialization if any consumer depends on exact bytes. |

All three provider adapters currently convert a provider integer to `datetime` before their
model-construction error wrapper. An out-of-range integer can therefore escape as `OverflowError`
instead of the adapter's bounded invalid-payload error. That is adjacent provider hardening to
perform with the adapter cutover, not part of TASK-026.

## Persistence and Serialized Representation Inventory

| Persistence path and evidence | Canonical JSON or text | Stored timestamp projections | Time-bearing keys and indexes | Read/order/cursor behavior | Risk and treatment |
| --- | --- | --- | --- | --- | --- |
| `PROJECT_STATE.json` — [`project_state.py`](../src/wealth/domain/project_state.py#L108-L180) | Version-controlled JSON; `last_updated_utc` must have zero offset | None | None | No time query | **Low.** Keep strict; version any change to fixed fractional precision. |
| In-memory event store — [`foundation.py`](../src/wealth/adapters/foundation.py#L14-L28) | Stores a strict `DomainEvent` object; no persistence serializer | Python object fields only | None | Append order | **Low.** No data migration. |
| In-memory candle store — [`market.py`](../src/wealth/adapters/market.py#L20-L140) | Stores offset-preserving objects | Python object fields only | Python `open_time` participates in natural keys | Python instant ordering, record-ID tie-break | **High semantic mismatch** with SQLite. Strict core models align both stores. |
| In-memory order-flow store — [`order_flow.py`](../src/wealth/adapters/order_flow.py#L20-L160) | Stores offset-preserving objects | Python object fields only | Python event time participates in ticker/best-bid-ask natural keys | Python instant ordering, sequence/ID tie-break | **High semantic mismatch** with SQLite. Strict core models align both stores. |
| SQLite market evidence — [`sqlite_market.py`](../src/wealth/adapters/sqlite_market.py#L97-L214), [`sqlite_market.py`](../src/wealth/adapters/sqlite_market.py#L218-L290), [`sqlite_market.py`](../src/wealth/adapters/sqlite_market.py#L330-L520) | Raw timing text uses direct `isoformat`; candle and conflict JSON preserves offsets | `open_time`, `observed_at`, `processed_at`, and `detected_at` are offset-preserving `TEXT` | Candle primary key and conflict stream index include `open_time`; capture and detection times are not otherwise indexed | `ORDER BY open_time, record_id`; conflict order also uses `open_time` | **High.** Mixed offsets can misorder and equal instants can have different keys. Build version-2 epoch projections and collision quarantine before cutover. |
| SQLite order-flow evidence — [`sqlite_order_flow.py`](../src/wealth/adapters/sqlite_order_flow.py#L102-L237), [`sqlite_order_flow.py`](../src/wealth/adapters/sqlite_order_flow.py#L250-L340), [`sqlite_order_flow.py`](../src/wealth/adapters/sqlite_order_flow.py#L396-L648) | Raw and canonical/conflict JSON preserves offsets; ticker/BBA natural-key JSON includes direct `event_time.isoformat()` | `event_time`, capture times, and `detected_at` are offset-preserving `TEXT` | `natural_key_json` is the canonical primary key; canonical-stream and conflict-stream indexes include `event_time`; capture and detection times are not otherwise indexed | Canonical rows are selected with lexical `ORDER BY` but re-sorted by parsed Python time before return; conflict results retain lexical `event_time` order | **High.** Returned canonical order is repaired in memory, but stored identity, natural keys, conflict order, and any future SQL page/limit remain unsafe. Migrate by record family with explicit collision review. |
| Historical collection SQLite — [`sqlite_collection.py`](../src/wealth/adapters/sqlite_collection.py#L43-L225), [`sqlite_collection.py`](../src/wealth/adapters/sqlite_collection.py#L276-L445) | Checkpoint, transition, and health JSON preserves offsets | `collection_jobs.next_window_start`, transition `recorded_at`, and health `observed_at` use direct `isoformat()` `TEXT`; other window values remain only in JSON | Health index is `(job_id, observed_at, observation_id)`; transition identity/order is `(job_id, version)`; `next_window_start` is stored but not indexed | Health uses lexical `ORDER BY observed_at, observation_id`; checkpoint progress is versioned and the direct cursor projection is cross-checked on read | **High.** Health can misorder and cursor text can diverge; tightening the model first would make legacy JSON unreadable. Add legacy decode, epoch projection, then strict contract. |
| Continuous collection SQLite — [`sqlite_continuous_collection.py`](../src/wealth/adapters/sqlite_continuous_collection.py#L45-L220), [`sqlite_continuous_collection.py`](../src/wealth/adapters/sqlite_continuous_collection.py#L240-L412) | Checkpoint/transition JSON preserves offsets | Cursor, active end, retry, and transition time are direct `TEXT` | Retry index is `(status, next_retry_at)`; transition identity/order is `(collection_id, version)` | Current reads use identity/version; no current time-range query uses the retry index | **High latent.** A future scheduler could trust unsafe text order. Migrate the full checkpoint atomically before scheduling is added. |
| Collector-service SQLite — [`sqlite_collector_service.py`](../src/wealth/adapters/sqlite_collector_service.py#L130-L283), [`sqlite_collector_service.py`](../src/wealth/adapters/sqlite_collector_service.py#L285-L475) | Heartbeat JSON preserves offsets | Run and heartbeat `observed_at` use direct `isoformat()` `TEXT` | Run and heartbeat indexes include `observed_at`; heartbeat uniqueness is causal `(run_id, sequence)` | History uses causal `sequence`; recent runs use `julianday(observed_at) DESC, rowid DESC` | **High/Medium.** `julianday` interprets offsets but loses the desired exact integer/tie contract. Migrate text/JSON; retain sequence and add explicit run-ID tie-break. |
| Shared rate-budget SQLite — [`sqlite_rate_budget.py`](../src/wealth/adapters/sqlite_rate_budget.py#L80-L190), [`sqlite_rate_budget.py`](../src/wealth/adapters/sqlite_rate_budget.py#L229-L272), [`sqlite_rate_budget.py`](../src/wealth/adapters/sqlite_rate_budget.py#L401-L478) | Request and decision JSON plus `requested_at` text preserve offsets | Internal token-bucket state uses exact epoch-microsecond integers; history `requested_at` remains direct `TEXT` | History index is `(budget_key, requested_at, reservation_id)` | Budget arithmetic is instant-safe; history uses lexical `ORDER BY requested_at, reservation_id` | **High/Medium.** Keep integer control state, migrate history projection/JSON, and add exact epoch validation. |
| Reconciliation SQLite — [`sqlite_reconciliation.py`](../src/wealth/adapters/sqlite_reconciliation.py#L121-L260), [`sqlite_reconciliation.py`](../src/wealth/adapters/sqlite_reconciliation.py#L262-L320), [`sqlite_reconciliation.py`](../src/wealth/adapters/sqlite_reconciliation.py#L360-L520) | Observation/report JSON preserves offsets; report digest covers offset-sensitive bytes | `recorded_at` projection is normalized to UTC `TEXT` | Series-time and status-time indexes include normalized `recorded_at` | Range filters, `MIN`/`MAX`, and order use the normalized projection plus observation ID | **High dual representation.** Add digest/serialization version, preserve old bytes, and migrate to epoch projection without silently recomputing evidence. |
| Public-trade control SQLite — [`sqlite_order_flow_collection.py`](../src/wealth/adapters/sqlite_order_flow_collection.py#L290-L620), [`sqlite_order_flow_collection.py`](../src/wealth/adapters/sqlite_order_flow_collection.py#L715-L1011), [`sqlite_order_flow_collection.py`](../src/wealth/adapters/sqlite_order_flow_collection.py#L1013-L1183) | Checkpoint and health JSON can preserve offsets | Every timestamp projection is normalized with `_timestamp`; lease acquisition reads require canonical UTC text | No temporal secondary index; transition primary key and health uniqueness use `(job_id, checkpoint_version)`, while the only secondary index is status/stream | Transition and health pagination use checkpoint version; the typed transition reader additionally rejects non-UTC canonical content | **High dual representation, lower order risk.** Retain version causality and consider this store as a pilot only after connected-family quarantine rules are accepted. |

### SQL order and cursor conclusion

- Unsafe lexical time ordering affects SQLite market, order-flow conflict reads, historical
  collection health, and rate-budget history. Canonical order-flow rows are selected lexically but
  currently re-sorted by parsed Python time before return; their stored identities and future SQL
  page/limit behavior remain unsafe.
- Continuous-collection retry text is indexed but not yet queried by time; it is a latent defect,
  not evidence of a current scheduler error.
- Reconciliation uses UTC-normalized text, which makes its current range/order operations
  chronological, but its canonical JSON and digest can still disagree in representation.
- Collector-service history is causally ordered by sequence. Its recent-run query asks SQLite to
  interpret timestamp text through `julianday`, so it is offset-aware but not the target exact
  microsecond projection.
- Public-trade transitions and health use checkpoint versions as cursors. That is the correct
  durable order and must remain authoritative after migration.

### Read validation and quarantine conclusion

- Existing candle and order-flow conflict tables quarantine competing semantic market revisions;
  they are not a quarantine for malformed, naive, offset-ambiguous, or projection-corrupt rows.
- SQLite market stream reads do not select and compare the indexed `open_time` against canonical
  JSON. Order-flow stream reads validate natural-key JSON but not every time projection, and
  conflicts can rely on unchecked indexed time for SQL order.
- Historical collection health order and summaries use indexed columns without revalidating every
  selected projection against canonical JSON.
- Collector-service `ORDER BY`/`LIMIT` and reconciliation range/aggregate SQL execute before
  per-record validation. A malformed indexed time can therefore fall outside a returned page or
  affect an aggregate without the normal record reader seeing it.
- Public-trade control validates complete normalized projections, but direct create/transition
  APIs can commit an aware nonzero-offset checkpoint. A normal `get` can return it while the
  stricter typed transition-history reader later reports `CORRUPT_RECORD`.

For these reasons, migration preflight must scan raw tables directly and validate every row. It
must not treat a clean result from an existing paginated, ranged, or aggregate API as proof that
the whole database is migration-safe.

## Current Test Evidence and Missing Regression Coverage

Current explicit UTC defenses are exercised by:

- [`test_domain_events.py`](../tests/unit/test_domain_events.py#L46-L86), which rejects naive and
  nonzero-offset event fields;
- [`test_project_state.py`](../tests/unit/test_project_state.py#L91-L96), which rejects a non-UTC
  repository-state timestamp;
- [`test_clock_contract.py`](../tests/unit/test_clock_contract.py), which verifies exact fixed-UTC
  clock identity and fail-closed handling of naive, offset, named/rule-based, and hostile values;
- [`test_canonical_utc.py`](../tests/unit/test_canonical_utc.py), which verifies the isolated
  validator, explicit normalizer, exact text codec, calendar boundaries, folds, hostile
  subclasses, malformed inputs, property-style instant preservation, canonical text round trips,
  exact signed epoch bounds, invalid integer/range handling, full-calendar projection round trips,
  one-microsecond distinction, and monotonic ordering;
- [`test_public_trade_collection_orchestrator.py`](../tests/unit/test_public_trade_collection_orchestrator.py#L451-L508),
  which rejects non-UTC request, clock, and creation boundaries before storage;
- [`test_order_flow_collection_contracts.py`](../tests/unit/test_order_flow_collection_contracts.py#L115-L123)
  and
  [`test_public_trade_transition_history.py`](../tests/integration/test_public_trade_transition_history.py#L910-L963),
  which reject or report corrupt non-UTC transition content; and
- [`test_sqlite_order_flow_collection.py`](../tests/integration/test_sqlite_order_flow_collection.py#L748-L809),
  which deliberately demonstrates that direct public-trade health/checkpoint storage can accept
  different offsets while UTC-normalized projections and version ordering preserve chronology.

Missing coverage is material:

- no field-complete nonzero-offset rejection or normalization matrix exists for the aware-only
  domain and port models;
- the fixed-`Z`, fixed-microsecond codec and exact epoch projection have no runtime consumer yet,
  so existing exact model, storage, query, log, and CLI representations remain intentionally
  unconverted and unverified against the isolated primitives;
- no regression compares equivalent instants across Python equality/hash, JSON bytes, natural
  keys, conflict identity, and SQL keys;
- no mixed-offset SQL-order test covers the legacy market, order-flow, historical collection,
  collector-service, or rate-budget stores;
- reconciliation tests do not version the digest when only timestamp representation changes; and
- no preflight, quarantine, migration, restore, or rollback test suite exists.

The implementation program must add table-driven and property-based tests for every listed field:
naive rejection, positive and negative offset handling, fixed-UTC acceptance, named/rule-based
zero-offset rejection including fold cases, fixed serialized bytes, exact round trip, provider
range overflow, equal-instant collisions, chronological order, tie-break order, digest versioning,
quarantine reasons, backup verification, and restored-state causality.

## Explicit Unknowns

Unknown behavior is not treated as safe:

| Unknown | Evidence | Required resolution before migration |
| --- | --- | --- |
| Actual local database population and offset distribution | No SQLite/database artifact is committed; catalog paths are chosen by each local deployment. | Run a read-only preflight against every selected database and record schema, row counts, timestamp encodings, parse failures, offset distribution, min/max instants, and collision groups. |
| External consumers of exact JSON, log, or CLI bytes | The repository contains serializers but no complete consumer registry. | Inventory consumers and snapshots; require owner sign-off or a versioned compatibility period. |
| Manually inserted or older rows that bypass current adapters | Local SQLite is mutable outside the process and no deployment inventory is committed. | Treat every row as untrusted during preflight; never infer validity from current writer code. |
| Plain application dataclass serialization | `ReplaySlice` and range/page result dataclasses define no canonical serializer. | Keep them internal, or define and version a serializer before external use. |
| Collision semantics for equal instants with different text | Current in-memory and SQLite identity behavior differs. | Report every collision. Merge only under an approved, record-family-specific equivalence rule; otherwise quarantine. |
| Safe rollback after version-2-only writes | No reverse converter or migration runner exists. | Define the cutover flag, write freeze, reverse proof, and point of no return before writes begin. |

## Staged Implementation and Migration Plan

Each stage has a separate acceptance gate. A later stage cannot use completion of TASK-026,
TASK-027, TASK-028, or TASK-029 as authorization for incompatible wiring or migration.

### Stage 1 — Prevent new clock drift — COMPLETE

TASK-027 added one reusable exact fixed-UTC assertion, strengthened the `Clock` contract, and
checks every scoped direct injected-clock result before it can flow into the next ID, provider
call, persistence write, reservation, wait, log, or canonical record. Historical and continuous
collection, collector service and health, the foundation `HealthCheckService`, rate budget, all
three public provider adapters, and public-trade orchestration are covered. Persisted models,
request acceptance, JSON, digests, keys, schemas, projections, and stored bytes were not changed.

Completed exit evidence:

- the helper rejects naive, nonzero-offset, and zero-offset non-`datetime.UTC` clock values and
  returns an existing fixed-UTC value unchanged;
- every direct clock boundary has table-driven tests for valid fixed UTC, naive, positive-offset,
  negative-offset, and named or fold-capable zero-offset results;
- an invalid initial clock causes zero ID generation, HTTP, storage, reservation, wait, or other
  downstream side-effect calls, and an invalid later clock fails before the next mutation;
- each scoped provider/application keeps its existing typed error and error-code mapping; and
- existing persisted models, JSON, digests, keys, schemas, and stored data remain unchanged.

### Stage 2 — Additive codec and conformance foundation — COMPLETE

TASK-028 added the shared strict validator, explicit edge normalizer, and fixed RFC 3339
serializer/parser as one isolated pure module. TASK-029 completed that unused foundation with
exact signed epoch-microsecond bounds plus integer-only projection and inverse decoding across
Python's full calendar range. Exhaustive deterministic, property-style, and hostile-subclass tests
cover type/range rejection, negative/zero/positive landmarks, one-microsecond distinction, exact
round trips, and monotonic order. No existing runtime path imports or calls these helpers. They
may not be wired into an existing persisted model until its compatibility reader and version plan
are accepted.

Exit evidence:

- the fixed-UTC validator returns the accepted object unchanged, and the explicit edge normalizer
  rejects naive or inconsistent values and returns a `datetime.UTC` instance;
- the serializer emits exactly six digits plus `Z`, and the strict parser rejects noncanonical
  variants and returns `datetime.UTC`;
- epoch conversion is exact, integer-only, range-safe, and order-preserving; and
- current stored representations and external output remain byte-for-byte unchanged.

### Stage 3 — Read-only compatibility preflight

Build a bounded, read-only scanner only for an explicitly authorized database/path list, with an
approved report destination and retention/disposal boundary. It opens SQLite directly through a
read-only URI (`mode=ro`), never instantiates a normal adapter that might create directories,
install a schema, enable WAL, or otherwise mutate state, and never identifies a database from
`user_version` alone. Before reading rows it verifies an exact store fingerprint: database
encoding; application/storage marker; table, column, storage-class, index, and trigger inventory;
normalized DDL; and expected version. Ambiguous or unknown layouts fail closed.

TASK-030 completed only the synthetic prerequisite for this stage. Strict frozen version-1
contracts and a direct `mode=ro&immutable=1` connection now identify all eight current generated
fixture layouts from pinned logical fingerprints while preserving separate whole-file and
directory evidence. The inspector rejects sidecars and mutations and reads only schema metadata
plus the two approved storage-marker tables. It has not read a timestamp row or operator
database, created a report or manifest, or satisfied any Stage 3 exit evidence.

Every scan works from a writer-fenced, SQLite-safe immutable source snapshot. For every
timestamp-bearing column it records SQLite `typeof`, database encoding, exact stored bytes using
`CAST(column AS BLOB)`/hex rather than a decoded Python `str` alone, declared version, parse
result, normalized instant, offset, precision, projection agreement, identity collision group,
and quarantine reason. It performs no repair.

Exit evidence:

- a manifest tied to the source snapshot identity, generation/watermark, whole-file hash, exact
  schema fingerprint, and extracted-byte hashes; its hash is signed or anchored in a separate
  append-only/read-only evidence location rather than stored only beside the mutable manifest;
- counts reconcile by table and record family;
- all unknown encodings and collision groups are explicit;
- no source database file, schema, journal, or application state changes during the scan; only
  the separately approved evidence destination may receive the report and manifest.

### Stage 4 — Versioned compatibility readers and contract waves

Add a legacy decode layer before tightening a persisted model. It retains original bytes and
schema/serialization version, normalizes only at that edge, and emits a strict canonical value or
a typed quarantine result.

Apply contract waves in dependency order:

1. provider/input adapters, checked clocks, candle/order-flow request and batch ports, and core raw
   market/canonical market models;
2. quality, replay, reconciliation, and reconciliation-history models, with explicit digest
   versioning;
3. historical collection, continuous collection, collector service, and rate-budget models and
   applications; and
4. public-trade checkpoint/health models and direct store APIs, retaining the already strict
   orchestrator and transition-reader defenses throughout.

Every wave requires field-complete regression tests and a compatibility reader for any durable
model before strict validation is enabled.

Reconciliation version 2 stores explicit `serialization_version` and `digest_algorithm` values
and includes both in a domain-separated digest input. Version 1 did not store the standalone
report JSON bytes independently: it stored the exact enclosing observation `record_json` plus
`report_sha256`. The compatibility reader preserves those two values, assigns only historically
proven constants from the version-1 store context, and uses a frozen version-1 parser/serializer
with fixtures to reproduce and verify the digest. It does not claim that reserialization proves
an independently stored report byte blob, and it quarantines any digest it cannot reproduce. It
does not infer digest semantics from a model `schema_version` or whichever Pydantic release is
installed.

### Stage 5 — Version-2 storage and shadow verification

After required approval, introduce version-2 storage in a separate physical database. Do not add
tables, indexes, triggers, or metadata to a strict version-1 database: some current adapters reject
even an extra schema object. An in-place path is allowed only if a later store-specific ADR proves
exact-schema compatibility, migration atomicity, and rollback. Canonical JSON uses the fixed
target text; sortable/queryable projections use epoch microseconds. Legacy readers stay available.
New writers are single-version and do not write ambiguous mixtures.

Default store migration order, unless the preflight evidence records a safer dependency order:

1. public-trade control as the candidate pilot because it already uses UTC projections and causal
   version cursors, but only after its connected-family quarantine and operational halt/resume
   rules are accepted;
2. canonical candle and order-flow evidence, resolving natural-key collisions before downstream
   history;
3. reconciliation history with versioned digest semantics;
4. historical collection control;
5. continuous collection control;
6. collector-service lifecycle and health; and
7. rate-budget history while preserving its existing epoch-microsecond control state.

For each store, establish one comparable generation: fence writers, create a consistent source
snapshot, and record a snapshot ID plus the store's maximum causal sequence/version and any
approved migration watermark. Build version 2 only from that immutable generation, then
shadow-read both copies and compare normalized records, counts, identity, chronology, and
projections. A static version-2 copy is never compared with a changing version-1 source.

Before cutover, fence writers again and either migrate a formally captured delta through a shared
generation watermark or take and migrate a fresh final snapshot. If no accepted lossless
change-capture protocol exists, writers remain stopped from the final snapshot through the
atomic read/write routing switch. Record both the cutover generation and the last generation for
which switching back to untouched version 1 is lossless.

### Stage 6 — Data migration, quarantine, and cutover

Create a verified SQLite backup and manifest before any write. Migrate into the separate physical
version-2 database, using transactions within that target; never partially rewrite the only
version-1 copy.

Quarantine, without silent coercion, any row that:

- cannot be parsed or is naive;
- overflows during UTC conversion;
- violates range, grid, causal, lease, cursor, or digest rules after conversion;
- disagrees with a checked projection;
- collides with another legacy row under the canonical identity; or
- lacks the original bytes/version needed to prove its meaning.

An equal-instant collision may be deduplicated only when the affected contract owner approves an
exact equivalence rule and all non-time evidence agrees. Otherwise both source rows remain
preserved and excluded from canonical promotion.

Quarantine is record-family-aware, not an arbitrary row filter. Before choosing a pilot, its
implementation ADR defines dependency closure, typed quarantine records and tombstone/references,
an operational halt, and owner-approved resume criteria. At minimum, a collection job keeps its
checkpoint, transitions, health, pending leaves, and lease/fencing evidence together; rate-budget
state keeps reservations and decisions together; and raw/canonical/conflict lineage remains
traceable. If one required member is unsafe, no dependent connected component is partially
promoted or silently orphaned.

### Stage 7 — Enforce and retire legacy behavior

Reject legacy writes first. Remove the legacy reader only after the compatibility window,
consumer inventory, rollback window, and repeated clean verification are complete. Close
`RISK-005` only after every inventory row is strict, normalized at an approved edge, or explicitly
retained as immutable raw provider evidence.

## Migration Verification

Each store migration must prove:

1. source snapshot identity, writer generation/watermark, database encoding, whole-file hash,
   schema fingerprint, SQLite storage classes, and exact legacy BLOB/hex bytes match the anchored
   manifest;
2. schema and database-type markers match the approved version;
3. table and record-family counts reconcile, including explicit quarantine counts;
4. every original row is represented by a migrated row, preserved raw record, or typed quarantine
   entry, and every connected control/evidence family satisfies its dependency-closure rule;
5. version-1 enclosing `record_json`, other stored bytes, and stored digests remain preserved; the
   frozen legacy reader reproduces each v1 digest without claiming an independently stored report
   blob. Separately, version-2 canonical JSON deterministically round-trips byte-for-byte under
   its declared serialization version;
6. epoch projections equal the canonical instant exactly;
7. min/max instants and chronological order match Python fixed-UTC normalization with the declared
   tie-break;
8. natural keys, raw lineage, record IDs, conflict relationships, and typed quarantine references
   reconcile;
9. reconciliation digests verify with explicit serialization-version and algorithm domain
   separation;
10. checkpoint versions, lease authority, retry state, health cursors, transition causality,
    rate-budget reservations, and operational halt/resume state remain valid;
11. shadow comparisons refer to the same recorded source generation and the atomic routing marker
    names the verified cutover generation;
12. idempotent replay produces the same outcome on a second migration run; and
13. the backup restores to a separately opened database with matching integrity checks, schema,
    counts, hashes, order, and causal state.

## Rollback Boundary

Before migration:

- fence and stop writers for the selected store and record the final causal generation/watermark;
- use the SQLite Online Backup API or an equivalently proven transactionally consistent snapshot
  procedure with an explicit WAL/checkpoint policy; never treat separate copies of database, WAL,
  and SHM files as a sufficient backup by themselves;
- run `PRAGMA integrity_check` and `PRAGMA foreign_key_check` as applicable on the source snapshot
  and restored copy;
- record whole-file hashes, exact schema SQL/fingerprint, encoding, `user_version`, database-type
  marker, row counts, generation, and verification queries in the externally anchored manifest;
  and
- restore and open the backup independently at a different path before migration.

During same-generation dual-read/shadow operation, rollback discards the unpromoted version-2
database; version 1 remains untouched. An atomic routing marker identifies which physical database
owns reads and writes. Switching that marker back is lossless only before any version-2-only
generation exists. After version-2-only writes begin, rollback is allowed only if a tested reverse
converter preserves every new record, dependency family, and declared serialization/digest
version. Otherwise that cutover generation is a recorded point of no return and recovery uses the
forward version-2 path.

## Decisions and Approvals Required

ADR 0027 accepts the target and staged plan, but not an incompatible cutover. TASK-027 through
TASK-030 are complete. The canonical next action is the bounded RISK-1 TASK-031; it may add only
unused, deterministic timestamp storage-class and byte evidence from generated fixtures after an
exact TASK-030 fingerprint match. Department and agent reviews are validation evidence, not human
approval.

Before Stage 3 accesses any operator database, the project owner must approve the exact read-only
database/path list, snapshot method, report destination, and evidence retention/disposal boundary.

Before changing a canonical truth source, the project owner and every affected contract owner
must approve the exact compatibility, digest, identity-collision, and reconciliation evidence
required by [Policies](POLICIES.md#explicit-human-approval-matrix). Before a database, schema, or
stored-state migration, the project owner must also approve a dedicated implementation ADR,
backup, validation, rollback, and affected control-owner review as required by
[`POLICIES.md`](POLICIES.md#explicit-human-approval-matrix).

Completed TASK-027 needed no migration approval because it enforced the existing internal UTC
clock policy without changing a persisted model or representation. TASK-028 and TASK-029 likewise
added only unused pure codec and projection primitives. TASK-030 added only unused synthetic
fixture contracts and immutable schema/marker fingerprint evidence; it did not scan timestamp
rows or operator data. Wiring any of these foundations into an existing runtime or advancing
Stages 3 through 7 requires the stage-specific authorization and applicable approvals described
here.

## TASK-031 Handoff

The next bounded action is
`phase2.canonical_utc_preflight_timestamp_evidence_foundation`.

It is limited to unused strict frozen versioned extraction plans and deterministic, bounded
evidence for explicitly declared timestamp columns in generated temporary SQLite fixtures. Row
access is allowed only after one exact TASK-030 family fingerprint matches. Evidence records the
stable row key, SQLite `typeof`, exact `hex(CAST(column AS BLOB))`, byte length, deterministic
order, and unchanged source identity. It may not parse or normalize a timestamp, analyze
collisions, quarantine data, inspect an operator, user-selected, deployment, or discovered
database, write a report or manifest, invoke a schema-installing adapter, create a journal, WAL,
or SHM file, wire into an active runtime, migrate or repair data, or claim Stage 3 completion.
Before any later operator database scan, the project owner must still approve the exact path
list, immutable snapshot method, report destination, and evidence retention/disposal boundary.
