# Market Data Contract — Phase 2 Foundation

## Purpose

This contract establishes provider-independent market-data records and the point-in-time boundary
required for honest research, replay, and backtesting.

It does not define a trading strategy and does not connect to an exchange.

## Public HTTP Finite-Work Boundary

`UrllibPublicHttpClient` accepts only a finite, positive request timeout. The Binance candle,
Coinbase candle, and Binance aggregate-trade sources enforce the same rule when configured.
`NaN`, positive or negative infinity, zero, and negative values fail with one explicit error before
URL/request construction or provider HTTP work. Finite positive integer and fractional values are
accepted only through one shared maximum of 120 seconds. A larger finite-positive value fails with
exact `ValueError("timeout_seconds must be at most 120")` at the transport and every active
provider construction boundary before request or provider work. Exact integer and float 120 and
smaller accepted objects are forwarded unchanged; a float subclass at exactly 120 also retains its
identity because no exact numeric-type policy was added. All active defaults remain 10.0 seconds.
No boundary coerces an invalid value, substitutes a fallback, or adds a retry. The timeout retains
standard transport semantics rather than claiming a total wall-clock deadline; the maximum does
not separately bound DNS, multiple operations, or caller/provider work and changes no retries,
waits, pacing, or rate budgets.

The shared transport also accepts only a built-in integer response limit from 1 through 2,000,000
bytes. Booleans, integer subclasses, floats, non-finite values, non-positive values, and larger
integers fail during client construction. Both successful and HTTP-error response paths request
exactly one byte beyond the configured limit and fail explicitly when that sentinel proves the
body is oversized; no truncated body is returned as evidence. Valid smaller limits and the
2,000,000-byte default and maximum remain exact configuration values.

After either bounded body read and its body-size decision, the shared client takes one bounded
response-header snapshot before constructing `HttpResponse`. Both the successful and `HTTPError`
paths call `headers.items()` once, start its iterator once, perform no direct message iteration or
second pass, request no length hint, and pull at most 101 times. Zero through 100 yielded pairs are
accepted only while cumulative `len(name) + len(value)` is at most 65,536 Python characters. A
yielded 101st pair fails before unpacking or inspecting it, and a 65,537th cumulative character
fails immediately with exact sanitized
`HttpTransportError("public HTTP response headers exceeded the configured limit")`.

Accepted pair order, duplicate names, original casing, empty strings, and all name and value
content remain exact, including existing `Retry-After` behavior. Body-read failures and body
oversize retain precedence without header enumeration. On a successful response, a header-limit
failure has no direct cause or hidden context and exits the response context once. On an
`HTTPError`, the originating provider error is the header-limit failure's direct cause and active
context, followed by exactly one cleanup attempt whose failure cannot replace that primary
outcome. Exceptions raised by `headers.items()`, iterator creation, or a consumed pull remain the
same raw objects; on the `HTTPError` path they retain only their natural implicit provider-error
context.

This is an adapter-controlled projection bound after standard-library parsing and prior
allocation. It does not bound wire-header bytes, parser work or memory, total response or process
memory, response time, or provider work and makes no privacy, redaction,
content-type/length/encoding, hostname/provider-allowlist, DNS/IP-routability, or SSRF guarantee.

If reading an HTTP-error response body raises `URLError`, `TimeoutError`, or `OSError`, the shared
transport returns no partial response and raises the same sanitized typed transport failure used
for supported successful-body read failures. The body is attempted once with the configured
one-byte sentinel, no retry is added, the read failure remains the direct cause, and its untrusted
detail is absent from the public message.

A real `IncompleteRead` raised specifically by either response-body read follows that same typed
failure boundary. Its partial provider bytes and expected-byte count are never accepted or
returned, the body is attempted once with the configured sentinel, and the original
`IncompleteRead` remains the direct cause. An `IncompleteRead` raised by response entry or exit is
outside this body-read mapping.

A real `IncompleteRead` raised directly by `urlopen` before a response handle reaches the adapter
follows the same sanitized typed boundary. The acquisition is attempted once, no adapter body read
or cleanup occurs without a handle, and no partial bytes or expected-byte count enters the public
message. Response entry and exit remain outside this acquisition mapping.

Only `BadStatusLine`, `LineTooLong`, and `UnknownProtocol` raised directly by `urlopen` or by either
response-body read follow the same sanitized typed failure boundary. The original protocol
exception remains the direct cause, provider status/header-line detail is absent from the public
message, acquisition is attempted once, and each body seam retains its single configured
one-byte-sentinel read. A direct base `HTTPException`, `InvalidURL`, or one of those three failures
raised by response entry, response exit, or HTTP-error cleanup remains outside this mapping.
`RemoteDisconnected` retains its pre-existing `OSError`-family typed outcome; the explicit protocol
tuple does not create a broad `HTTPException` catch.

The shared transport uses one private urllib opener with a no-follow redirect handler. Original
301, 302, 303, 307, and 308 responses are never followed. Before parsing `Location` or `URI`, the
handler rejects absent, empty, relative, same-origin, cross-origin, HTTPS-to-HTTP downgrade, FTP,
unsupported-scheme, and malformed targets. It creates no follow-up `Request` and performs no body
read or cleanup. The original 3xx instead enters the existing bounded `HTTPError` path: the adapter
performs one configured one-byte-sentinel read and one cleanup attempt, retains the original status
and headers, returns only a complete in-limit body, and preserves all existing read, protocol,
cleanup, direct-cause, and primary-failure mappings. No process-global opener is installed or
mutated.

After finite-positive timeout validation and before any query-mapping operation, the shared client
validates the original initial target as an absolute credential-free HTTPS URL with a non-empty
CPython-parser hostname. A literal query or fragment delimiter, backslash, C0 or DEL control,
Unicode whitespace, lone surrogate code point, relative or non-HTTPS form, absent or malformed
authority, any userinfo, and an empty, non-numeric, signed, Unicode-digit, zero, or
greater-than-65,535 explicit port fails with the exact context-suppressed
`ValueError("url must be an absolute credential-free HTTPS endpoint without query or fragment")`.
Every percent sign in the authority also fails, covering encoded host characters, ports, userinfo
delimiters, slashes, backslashes, and controls, as well as malformed escapes and IPv6 zones, before
urllib can reinterpret the authority. The authority is inspected under NFKC only to reject
compatibility forms that IDNA could emit as a percent sign, backslash, whitespace, C0, or DEL; the
accepted URL itself is never normalized, reconstructed, or repaired. A rejected target performs
no query iteration or serialization, `Request` construction, opener or handler work, DNS lookup,
network access, or filesystem access. An accepted target retains its exact text before the
separately supplied sorted query is appended once, and GET, `Accept`, `User-Agent`, and timeout
behavior remain unchanged.

After that structural validation and still before any query-mapping operation, the caller target
port must be omitted or parse as numeric 443. A structurally valid explicit nonstandard port fails
with exact `ValueError("url must use the standard HTTPS target port")`, with no direct cause or
hidden context and without query access, serialization, request construction, opener or handler
work, DNS lookup, network access, or filesystem access. Malformed, percent-encoded, empty,
non-numeric, signed, Unicode-digit, zero, and greater-than-65,535 ports retain the earlier exact
structural error and precedence. An accepted implicit, explicit, or zero-padded 443 target retains
its exact original text before the separately supplied sorted query is appended.

These initial-target controls are structural and caller-authority policies, not hostname or SSRF
policies. They still permit localhost, IPv4, IPv6, CPython-parser-accepted IPvFuture and DNS-label
forms, Unicode hostnames, and trailing-dot hostnames. They perform no DNS resolution,
IP/public-routability check, provider or hostname allowlist, or SSRF guarantee. A configured proxy
peer may use a non-443 port, and standard TLS and proxy behavior remains unchanged.

After timeout, structural-target, and target-port validation and before `urlencode`, the shared
client takes one bounded query snapshot. It calls `items()` and starts its iterator once; it does
not call `len(query)`, directly iterate the mapping, start a second item pass, or request an
iterator length hint, and it pulls at most 33 yielded items. Zero through 32 exact built-in tuple
pairs are accepted only when both components are exact built-in strings and their cumulative
key-plus-value length is at most 8,192 Python characters. A 33rd item, invalid pair shape or tuple
subclass, non-string or string-subclass component, or 8,193rd character fails with exact
`ValueError("query must contain at most 32 built-in string pairs totaling at most 8192 characters")`
and no direct cause or hidden context. Rejection occurs before encoding, request construction,
opener or handler work, DNS lookup, network access, or filesystem access. Mapping-originated
exceptions, including `ValueError`, remain the same raw objects rather than becoming the boundary
error. Accepted pairs retain their existing single sorted standard-library encoding, including
duplicates and empty or Unicode content. This is an adapter-controlled enumeration and raw-string
volume bound, not a total wall-clock bound on caller mapping code or a new query-content,
normalization, or multi-value policy.

After finite-positive timeout validation and before any content-dependent URL work, the shared
client measures the original target with non-polymorphic `str.__len__`. More than 8,192 Python
characters fails with exact `ValueError("url must contain at most 8192 characters")` and no direct
cause or hidden context. Rejection occurs before literal membership or character scanning,
`urlsplit`, hostname, username, port, or NFKC inspection, query access or serialization, request
construction, opener or handler work, DNS lookup, network access, or filesystem access. Caller
length and content overrides are not dispatched. Length intentionally precedes structural and port
errors for an oversized target; at or below the limit, every existing structural, parser-context,
port, and query rule retains its order and behavior. Exact-limit ASCII and multi-byte Unicode
targets retain every original character. The limit counts Python characters, not encoded bytes,
and is not a provider request-line compatibility or total-wall-clock guarantee.

After preserving `max_response_bytes` validation and its first precedence, client construction
requires `user_agent` to be an exact built-in `str` of 1 through 256 Python characters, all in the
inclusive visible-ASCII range U+0020 through U+007E. A wrong type, empty or oversized value, C0 or
DEL control, non-ASCII code point, or lone surrogate fails with exact context- and cause-free
`ValueError("user_agent must be a built-in string of 1 to 256 visible ASCII characters")`.
Exact-type validation precedes length and character inspection, and an oversized exact string
fails before scanning its characters. Every rejection occurs during construction before URL,
query, encoding, request, opener, handler, DNS, network, or filesystem work. Accepted text,
including leading or trailing spaces and punctuation, is forwarded unchanged exactly once as the
sole `User-Agent` header; the exact default remains the 29-character
`"WEALTH/0.1 public-market-data"`. The boundary performs no normalization, trimming, truncation,
repair, fallback, replacement, redaction, or synthesis. It makes no privacy or
total-header-block guarantee and adds no provider or hostname allowlist, DNS or IP policy, or SSRF
guarantee.

Every `HTTPError` path makes one explicit cleanup attempt after at most one bounded body read.
When cleanup succeeds, the error-response resource is closed before a response or primary failure
leaves the transport boundary. An `OSError`-family or `IncompleteRead` cleanup failure after
otherwise complete processing becomes the same sanitized typed transport failure with that
cleanup error as direct cause. If any primary read, oversize, header, or other processing failure
already exists, cleanup is still attempted and no cleanup failure replaces the primary outcome.
Unsupported cleanup-only failures retain their original type, and a failed cleanup attempt is not
treated as proof that the resource closed.

## Canonical Order-Flow Foundation

`CanonicalTrade`, `CanonicalTicker`, and `CanonicalBestBidAsk` establish the provider-independent
target for future public trade and market-structure adapters.

All three retain source, venue, canonical instrument, instrument type, exchange event time, local
observation time, processing time, optional provider sequence, exact decimal values, and lineage.
Event time cannot follow observation, and observation cannot follow processing.

Canonical trades require provider identity, positive price and quantity, and an explicit aggressor
side of buy, sell, or unknown. Optional provider quote quantity remains separate from the exact
locally calculated notional. A provider-defined aggregate must retain both its first and last
underlying provider trade identities; an individual observation cannot declare that range.

Canonical tickers always contain a positive last price. Optional rolling-window statistics are
accepted only with an explicit valid window; supplied high and low must contain last price and any
supplied window open.

Canonical best-bid-ask snapshots require positive displayed quantities and a best bid strictly
below best ask. Exact spread, midpoint, and spread basis points are derived from the accepted
decimal prices.

Trade records have one bounded Binance public REST adapter and durable storage. Ticker and
best-bid-ask records still have no provider adapters. No order-flow record has point-in-time replay
or live-stream orchestration.

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
and raw lineage must agree across the batch, and one batch is capped at 100,000 records. A valid
empty provider window may contain zero canonical records while preserving its exact raw response.

`SQLiteOrderFlowStore` adds a dedicated versioned file-backed implementation. Raw bytes and
canonical records are written atomically, equivalent new captures add lineage to the first record,
and changed values are quarantined without replacement. A database-type marker prevents another
SQLite store with the same integer version from being opened accidentally. Raw hashes, canonical
schemas, natural keys, record types, and stream indexes are revalidated when evidence is read.

`OrderFlowBatchIngestor` is the fail-closed admission path. It audits the complete bounded batch
before storage. A quality failure causes no raw or canonical write. A passing report permits the
atomic batch write. Admission then requires the returned raw outcome to identify the exact batch
payload with a coherent inserted or duplicate status, plus exactly one ordered outcome for every
canonical record. Each canonical outcome must bind the incoming record ID and record family, use a
coherent inserted or duplicate status, and preserve input order. Missing, extra, duplicated,
reordered, misidentified, wrong-family, conflicting, or status-incoherent outcomes keep the result
unaccepted. A valid zero-record batch remains accepted when its raw outcome is coherent and its
canonical outcome tuple is empty. Exact repeats remain accepted idempotent outcomes.

## Public Binance Aggregate-Trade Adapter

`BinancePublicAggregateTradeSource` reads one bounded event-time window from Binance's
unauthenticated aggregate-trade REST endpoints:

- Spot through the market-data-only host.
- USD-M perpetual and dated futures through the public futures host.

The provider symbol is explicit and separate from the canonical instrument. Requests must be
millisecond-aligned, already closed, and shorter than one hour; USD-M requests must remain within
the latest 24 hours. The canonical half-open end is converted to Binance's inclusive final
millisecond.

Each row retains the aggregate trade ID, first and last underlying trade IDs, exact price and base
quantity, exchange event time, and maker evidence. Binance's buyer-maker flag maps to the opposite
aggressor side. The source declares only a monotonic aggregate-ID promise, not a contiguous one.

A response at the 1,000-row provider cap is rejected as possibly truncated. Callers must shrink
the window rather than treating potentially partial data as complete. Empty arrays remain valid
raw evidence. Invalid field sets, values, ordering, timing, transport, and provider responses fail
with machine-readable errors before storage. The adapter does not paginate, retry, poll, use an API
key, or expose any account or order capability.

## Adaptive Public-Trade Range Ingestion

`AdaptivePublicTradeRangeIngestor` composes the single-window source and fail-closed admission path
without hiding network work inside the adapter. It plans contiguous chronological initial windows
and bisects only a window whose source error explicitly requires a smaller request. Split children
must exactly partition their parent on the millisecond grid and the left child is always processed
first.

The range, source requests, records, minimum window, inter-request pacing, attempts, exponential
delays, and accepted `Retry-After` values all have explicit finite policy bounds. Every network
attempt consumes the request budget. Retry is limited to failures explicitly classified as
transient and is never started after that budget is exhausted.

`RateBudgetedPublicTradeSource` can additionally reserve durable weighted capacity before every
delegated request. A local denial returns bounded retry evidence without network access and enters
the same range retry and trace path. Binance Spot aggregate trades use documented request cost 4;
USD-M aggregate trades use cost 20. Capacity, period, cost, database, and shared budget key remain
explicit deployment configuration.

Every complete window passes through the order-flow quality and storage gate before the next
window. Successful earlier windows remain durable if a later source, density, record, quality, or
storage boundary stops the run. The result retains typed traces for splits, retries, ingestion, and
failure, plus the exact first unadmitted event-time boundary for safe idempotent resumption.

An empty complete window advances coverage and stores its raw response. A fetched window that would
exceed the total record limit is returned as in-memory evidence but is not admitted. A provider cap
at the configured minimum one-millisecond window stops explicitly; it is never treated as complete.

## Restart-Safe Bounded Public-Trade Control and Orchestration

`PublicTradeCollectionCheckpoint` records one immutable bounded source and event-time range in a
dedicated control boundary. Its durable cursor is the first unadmitted event-time boundary. A
paused or failed job also retains the exact exclusive end of the adaptive leaf that stopped, so
recovery does not reconstruct a different request from a changed planner.

The checkpoint's immutable `policy_fingerprint` identifies the effective versioned range, split,
retry, pacing, and request-budget policy. Request, completed-window, record, and split totals are
lifetime cumulative only for outcomes committed by checkpoint compare-and-swap. They are durable
audit totals, not proof that a crashed process made no additional request: a request completed
before an uncommitted transition can be repeated after recovery. The durable shared provider-rate
budget is the current pre-request protection. Crash-durable per-job attempt reservations are
not provided by the bounded orchestrator and require a separate future reservation design.

Committed health and checkpoint counters separately retain window traces and retries. The contract
requires `source_requests = window_traces + retry_attempts`, so every committed provider attempt is
accounted for exactly while still acknowledging that a pre-commit crash can lose control-state
telemetry.

`PublicTradeSourceHealthObservation` records append-only accepted or rejected invocation evidence,
including the provider symbol, adaptive work, retry waits, the safe resume cursor, and exact
pending leaf. Each observation identifies the exact checkpoint transition version that committed
it. History reads use that causal version as an exclusive cursor and return at most 100 observations
by default or 1,000 when explicitly requested.
`PublicTradeCollectionHealthSummary` streams and validates the complete evidence ledger without
materializing it in memory.

Clean local stops at an invocation's outer request or record bound transition the job to `PAUSED`
with the exact pending leaf retained. Their provider health remains `HEALTHY` when the admitted
path needed neither retry nor split, or `DEGRADED` when it did; exhausting a local work bound does
not invent source unavailability. A real terminal source or admission failure transitions to
`FAILED`. The distinction is derived from the typed terminal trace and admission outcome, never
from the stop-reason text alone. The bounded application mapper translates a typed upstream
failure into a canonical whitespace-free control code of at most 128 characters; it does not copy
an arbitrary upstream machine-code string directly into durable control state.

A dedicated file-backed SQLite control store persists canonical checkpoints, append-only
transitions, and health observations. It uses an explicit database marker, schema version,
versioned worker leases with UUID fencing tokens, optimistic compare-and-swap, indexed-projection
checks, and read-time contract validation. The fencing token is checked at the transition boundary
and retained in append-only history. Lease TTL cannot exceed one hour. A per-job acquisition ledger
rejects reuse of a previously claimed UUID, including after pause, failure, or lease expiry.
Indexed timestamps are normalized to UTC, all persisted projections and computed summaries are
validated against canonical records, health is ordered by checkpoint version rather than timestamp
or observation ID, and the complete versioned DDL is checked before use. Schema installation is
transactional. One checkpoint transition and its matching health observation commit atomically
inside this control database.

`PublicTradeCollectionTransition` is the immutable typed view of one retained transition. It
contains the full canonical checkpoint and the optional actor fencing token stored for that
transition; it does not copy mutable control projections or source-health evidence.
`PublicTradeCollectionTransitionReader.transitions_for_job` returns ascending contiguous
checkpoint versions, uses a previously returned version as an exclusive cursor, defaults to 100
records, and rejects limits above 1,000. A missing job without a cursor and a cursor at the
validated tail return an empty tuple; an invalid or missing stored cursor is rejected.

The SQLite reader revalidates canonical JSON, exact SQLite storage types and indexed projections,
immutable identity, UTC content, pristine creation, lifecycle causality, actor authority against
the durable lease-acquisition ledger, cursor and page continuity, the ledger tail, and equality
between the latest transition and current checkpoint. Malformed, noncanonical, orphaned, gapped,
unauthorized, reused-authority, or otherwise inconsistent history fails closed as a control-storage
`CORRUPT_RECORD`. Reads use the existing schema, do not mutate any table, and do not infer causal
order from timestamps. The store separately enforces monotonic transition time and bounded TTL;
the bounded orchestrator sources transition time from its injected trusted clock.

The control store itself does not compose or start a collector, service, scheduler, or network
request. A separate explicitly invoked bounded application orchestrator validates the immutable
policy fingerprint, claims the job with a fresh UUID fencing token, resumes the exact retained
leaf, and invokes finite range collection. When that retained leaf ends before the immutable job,
the orchestrator commits it first and may process the remaining range once; an operator invocation
therefore contains at most two bounded segments. It advances the checkpoint only after accepted
order-flow evidence is durable, except for the preceding control-only lease claim. The work
transition and matching health observation commit atomically inside the control database when
lease authority remains current and compare-and-swap succeeds. Lost authority or a version
conflict returns an explicit non-progress result and leaves the durable cursor unchanged for safe
refetch.

Because the evidence and control databases cannot commit atomically together, a crash after
evidence and before control advancement causes the retained leaf to be fetched again. The existing
order-flow store accepts that replay idempotently. Advancing the checkpoint before evidence is
durable remains forbidden. Malformed non-conflicting persistence evidence follows the existing
ingestion-rejected path: the checkpoint records degraded health and failure diagnostics without
advancing its cursor or completion counters and without requesting a later window. Typed write
outcomes establish internally coherent admission evidence; they do not prove physical durability,
readback, `fsync`, rollback, or atomicity between the evidence and control databases. The
orchestrator is not a scheduler, daemon, continuous poller, or live stream.

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

`CandleStore.append_batch` returns typed outcomes bound to the attempted batch. The raw outcome's
`incoming_record_id` must equal the batch raw-payload ID. `INSERTED` identifies no existing raw
record; `DUPLICATE` identifies that same prior raw identity; and `CONFLICT` rejects admission.
After a non-conflicting raw outcome, the candle outcomes must have exactly the batch-record count
and order, with each `incoming_record_id` equal to the corresponding candle ID. An inserted candle
identifies no existing record, while a duplicate or conflict identifies the previously retained
canonical record.

`HistoricalCandleIngestionResult.accepted` requires a passing quality report, coherent raw-write
evidence, one exact ordered candle outcome per batch record, and only inserted or duplicate candle
outcomes. Missing, extra, reordered, duplicated, misidentified, conflicting, or internally
contradictory outcomes fail closed. The returned outcomes are preserved unchanged for diagnosis;
the ingestion boundary does not sort, repair, or invent evidence.

This validation establishes only the completeness and internal coherence of the typed outcomes
returned by the configured store. It performs no post-write readback, does not independently
verify filesystem synchronization or physical durability, cannot roll back mutations made by a
nonconforming store, and does not make the market-evidence and checkpoint databases atomic
together.

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

`RecoverableHistoricalCandleCollector` advances page and candle progress only after
`HistoricalCandleIngestionResult.accepted` is true. Malformed persistence evidence follows the
existing `page_rejected` / `quality_or_storage_gate` path: the collector records a rejected health
observation and a failed control transition, retains the prior `next_window_start`,
`pages_completed`, and `candles_completed`, and does not request a later page. The failure
transition may still update status, version, attempts, and diagnostic fields; no progress does not
mean no control-state write.

## Current Limitations

- Final candles are implemented end to end. Trade, ticker, and best-bid-ask records have strict
  contracts, bounded quality auditing, fail-closed ingestion, and idempotent raw/canonical SQLite
  storage. Aggregate trades have one bounded Binance provider adapter; ticker and best-bid-ask
  records do not. No order-flow record has live collection or replay.
- Binance aggregate-trade requests are single windows shorter than one hour. A response at the
  1,000-row cap fails closed and USD-M history is limited to the latest 24 hours. Bounded range
  ingestion can split dense windows down to one millisecond, but it stops if that minimum still
  reaches the cap.
- Public-trade range ingestion remains explicitly invoked. Durable bounded checkpoint and health
  control state and explicitly invoked bounded checkpoint orchestration are active. Automatic
  scheduling, continuous polling, live WebSockets, and gap recovery remain absent. Every
  orchestrated provider fetch passes through the required shared durable single-host
  request-budget wrapper. Committed checkpoint counters do not durably reserve per-job attempts
  made before a crash; that reservation design remains future work. Health history is available
  only through bounded checkpoint-version pages. Actor transition history now has a separate
  typed bounded read port, but neither history has an operator CLI, dashboard, repair endpoint, or
  external audit export. One composed generated-fixture drill now exercises an exhausted
  disconnect, sparse one-millisecond windows, newly constructed evidence, checkpoint, and shared
  rate-budget SQLite adapters, fresh fencing authority, and the typed audit chain. It proves exact
  pending-leaf recovery from failed checkpoint version 3 through completed version 6, five
  budgeted requests, one retry, two pacing waits, three raw captures, one canonical trade, zero
  conflicts, and a no-work completed rerun. It does not prove cross-database atomicity, physical
  durability, continuous operation, or automatic recovery. Versioned synthetic fixtures for the
  five active provider payload variants and an operator-visible schema-drift response runbook
  remain pending under TASK-057.
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
