# WEALTH

WEALTH is a governed multi-agent cryptocurrency research and trading platform. The project is
currently building its reliable market-data platform. It can read bounded historical candle ranges
from Binance and Coinbase public endpoints and supervise restart-safe polling of closed candles,
but it has no account access and cannot execute trades.

## Read First

- `PROJECT_STATE.json` — validated current state and canonical next action
- `docs/PROJECT_CHARTER.md`
- `docs/AI_DEPARTMENTS.md`
- `docs/ORGANIZATION.md`
- `docs/ARCHITECTURE.md`
- `docs/POLICIES.md`
- `docs/SECURITY_POLICY.md`
- `docs/RISK_POLICY.md`
- `docs/EXECUTION_POLICY.md`
- `docs/DATA_CONTRACTS.md`
- `docs/DATA_CATALOG.md`
- `RISK_REGISTER.md`
- `BACKLOG.md`
- `docs/ROADMAP.md`
- `AGENTS.md`

Accepted Architecture Decision Records live in `docs/decisions/`. The state file names one next
action; the backlog supplies its bounded acceptance contract.

## Prerequisites

- Git
- `uv`

The repository pins Python 3.13 in `.python-version`. `uv` can install a compatible Python automatically.

## Setup

```text
uv sync --all-groups
```

Do not create or commit a real `.env` containing credentials during foundation work. `.env.example` contains safe, non-secret local defaults.

## Run the Foundation Health Slice

```text
uv run wealth-health
```

The command validates, stores, and emits one synthetic health event. It does not access the network, an exchange, or financial credentials.

## Quality Checks

```text
uv lock --check
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv --preview-features audit-command audit --locked
```

Run all checks before requesting review. Report any unavailable or failed check explicitly.

## Safe Runtime Defaults

Runtime identity is loaded from environment variables and fails closed to:

```text
WEALTH_ENVIRONMENT=development
WEALTH_OPERATING_MODE=research
WEALTH_PRIMARY_MARKET=crypto
WEALTH_TRADING_TYPE=spot
WEALTH_SYSTEM_TIMEZONE=UTC
WEALTH_USER_TIMEZONE=Asia/Jerusalem
WEALTH_BASE_CURRENCY=USD
WEALTH_ARCHITECTURE_STYLE=modular_monolith
WEALTH_LIVE_TRADING_ENABLED=false
WEALTH_LEVERAGE_ENABLED=false
WEALTH_WITHDRAWALS_ENABLED=false
WEALTH_EXTERNAL_NOTIFICATIONS_ENABLED=false
WEALTH_AUTONOMOUS_LIVE_EXECUTION_ENABLED=false
WEALTH_LOG_LEVEL=INFO
```

The system remains in research mode as an approved safer deviation until a real paper exchange or
simulator exists. Paper is still the target after the Phase 5 gates; calling the current runtime
paper would be misleading. Any attempt to enable a sensitive control or use the live environment
fails validation. Configuration never grants execution authority, and no current code path can
submit an order.

## Professional Algorithm Foundation

The first algorithmic capability is deterministic market-data replay, not a trading signal.
Canonical candles preserve source lineage and event, observation, and processing times. A replay
slice exposes only records observed by its evaluation time, preventing a strategy from seeing
late-arriving or future data.

## Public Binance Candle Slice

The first real provider adapter can read already-closed Spot and USD-M Futures candles from
Binance's public REST endpoints. It requires no API key and exposes no account or order capability.

## Public Coinbase Candle Slice

A second provider adapter can read already-closed Spot candles from Coinbase Exchange's public REST
endpoint. It supports the canonical one-minute, five-minute, fifteen-minute, one-hour, and one-day
timeframes with at most 300 candles per provider request. Documented pre-window rows are excluded
from the requested canonical batch, while missing in-window buckets remain explicit quality
failures rather than invented candles.

## Cross-Source Candle Reconciliation

Selected candle windows from two distinct sources can be compared after each source passes the
existing sequence-quality gate. The report retains both record identifiers, missing-source
findings, and symmetric basis-point differences for OHLC and base volume. Price tolerance is
explicit and versioned; a volume limit is optional because volume is venue-specific.

Reconciliation requires the same exact canonical instrument, instrument type, and timeframe.
`BTC-USD` is therefore not compared with `BTC-USDT` through an implicit conversion. Outcomes are
`pass`, `divergent`, or `blocked`; the report does not choose a true provider, blend prices, repair
records, or authorize a trade.

Reconciliation reports can be wrapped as immutable observations and appended to a dedicated local
SQLite history database. Exact repeats are idempotent, while reused observation identities or
comparison keys with different source streams fail explicitly. Bounded queries reload and
revalidate the original evidence. Indexed summaries expose pass, divergence, blocked,
source-quality-failure, compared-interval, and issue-code counts for one comparison series and time
window. This evidence layer does not rank providers, trigger alerts, or make trading decisions.

Every response is normalized into the provider-independent candle contract and sent through the
deterministic sequence-quality gate. Incomplete, conflicting, malformed, or time-inconsistent
batches are reported and are not written to storage.

Accepted batches can now be stored in a file-backed SQLite adapter. The adapter keeps exact raw
provider bytes separately from canonical candles, verifies them again when reading, survives
restart, treats repeats idempotently, and quarantines conflicting revisions without overwriting the
accepted record.

Provider adapters remain bounded to one window: at most 1,000 Binance candles or 300 Coinbase
candles. An explicit application flow can split a larger range into contiguous pages, pace
requests, retry only classified transient failures, honor a bounded `Retry-After`, and stop at the
first failed page with an exact resume boundary. One invocation is capped at 100,000 candles.

Operator-invoked collection jobs can persist their cursor in a dedicated SQLite control database.
Versioned worker leases prevent duplicate advancement, accepted pages checkpoint independently,
and page attempts produce append-only source-health evidence. Recovery deliberately refetches a
page when a process stops after storing market evidence but before advancing its checkpoint; the
market store handles that repeat idempotently.

A supervised polling layer can now maintain a separate durable cursor for one continuous candle
stream. It requests only timeframe boundaries that are closed beyond an explicit settlement delay,
limits every catch-up window and invocation, and attaches one exact bounded collection job before
network work begins. Restart resumes that same job. A disconnect uses bounded reconnect backoff;
unsupported failures or a configured consecutive-failure limit pause the stream until operator
review. Manual pause also blocks new network work until explicit resume. This is an application
workflow, not an automatically started daemon.

An interruptible local service runner can now invoke those cycles repeatedly under an explicit
run limit. A stop request wakes polling waits immediately, and each invocation stores validated
startup, cycle, pause, failure, stop, or run-limit evidence in append-only SQLite history. Restart
creates a new service run audit trail while retaining the existing continuous market cursor. This
is a process-lifecycle boundary, not an installed operating-system service or 24/7 deployment.

Read-only operational health reports can classify the latest or a bounded set of recent service
runs as active, stale, stopped, paused, failed, or completed. Stale and failed runs produce
structured critical alerts; paused runs produce a warning. The default ten-minute stale threshold
is longer than the collector's maximum bounded wait. These alerts remain internal data contracts:
no message is sent and no process is restarted automatically.

The next market-structure boundary defines strict canonical trades, last-price tickers, and
best-bid-ask snapshots. It preserves provider identity, event/observation/processing time, exact
decimal values, sequences, and lineage. Aggressor side can remain explicitly unknown, optional
ticker statistics require a declared window, and locked or crossed top-of-book snapshots fail
closed.

A bounded quality gate now checks one exact record family for mixed streams, event-time ordering,
window membership, duplicate identities, conflicting values, and documented provider-sequence
promises. Sequence gaps are reported only when an adapter explicitly declares a contiguous
provider contract. An idempotent in-memory store proves that equivalent repeats and conflicting
revisions never overwrite the first accepted record.

A dedicated SQLite adapter now persists exact raw captures and all three canonical order-flow
families atomically. It survives restart, retains every equivalent raw lineage link, quarantines
conflicting revisions, rejects a reused canonical identity, validates its database type, and
revalidates raw hashes and canonical records on reads.

The application admission path sends the complete bounded batch through the deterministic quality
gate before storage. Quality failure leaves raw and canonical storage untouched; raw or canonical
storage conflict makes the result explicitly unaccepted.

A bounded public Binance REST adapter now normalizes Spot and USD-M aggregate trades through that
path without an API key. It preserves the provider's aggregate ID and underlying first/last trade
IDs, maps maker evidence to aggressor side, retains exact raw bytes, and admits valid empty
windows. A response at Binance's 1,000-row cap fails as possibly truncated so partial evidence
cannot be stored as a complete window.

A bounded application flow can collect a longer public-trade range using chronological initial
windows. Dense windows split into exact left and right halves until each is complete or the
one-millisecond minimum is reached. Request count, total records, pacing, attempts, exponential
delay, and `Retry-After` are bounded; every adaptive decision is traced and every stop exposes the
exact first unadmitted event-time boundary. No live order-flow collection, replay path, trading
signal, or trading action is present.

Dedicated public-trade control contracts can now retain that bounded progress across process
restart. A checkpoint stores the durable cursor, exact end of a pending adaptive leaf, immutable
policy fingerprint, committed-outcome lifetime work counters, and a UUID-fenced worker lease.
Health evidence retains the provider symbol. A separate SQLite control store installs its schema
transactionally, normalizes indexed time to UTC, validates complete projections and summaries,
and atomically persists compare-and-swap transitions with matching source-health evidence. The
lease token is checked at the transition boundary, lease TTL is capped at one hour, every new
claim must use a UUID not previously acquired by that job, and acquisitions are retained in a
durable fencing ledger. Health evidence identifies the exact checkpoint version that committed it
and is read in bounded, causally ordered pages (100 rows by default, 1,000 maximum).

These counters do not claim to be crash-durable hard job limits. A request made before an
uncommitted checkpoint transition can be repeated after restart; the shared durable provider-rate
budget remains the pre-request capacity protection. Crash-durable per-job attempt reservation
before network access remains future work.

An explicitly invoked bounded orchestrator now validates the immutable policy fingerprint, claims
the job with a fresh UUID fencing token, and invokes finite range collection from the durable
cursor or exact pending leaf. Recovery finishes that exact leaf first and may then process the
remaining range once, so one operator invocation contains at most two bounded segments. It
classifies clean outer request- or record-limit stops as resumable `PAUSED` outcomes with truthful
`HEALTHY` or `DEGRADED` source health. Typed terminal source or admission failures become `FAILED`
with a bounded canonical control code; stop-reason text and arbitrary upstream machine-code
strings are not trusted as classifications.

The lease claim is a control-only transition; the orchestrator advances the durable work cursor
and counters only after market evidence is durable. A crash between the evidence and work-progress
commits therefore causes an idempotent refetch rather than a permanent gap. The work transition
and matching health observation commit atomically in the control database when lease authority
remains current and compare-and-swap succeeds. Lost authority or a version conflict returns an
explicit non-progress result and leaves the durable cursor unchanged for safe refetch. Append-only
transitions are now available through a typed read-only port. Each immutable result contains the
validated canonical checkpoint and the actor fencing token retained for that transition. Pages
are ascending and contiguous by checkpoint version, use a returned version as an exclusive cursor,
default to 100 records, and reject a requested limit above 1,000. The SQLite reader revalidates
canonical JSON, indexed projections, immutable job identity, UTC content, lifecycle causality,
actor authority against the durable lease-acquisition ledger, exact SQLite storage types, page
continuity, and agreement between the ledger tail and current checkpoint. Corruption fails closed
through the existing control-storage error boundary. Reads do not change the database or schema
and remain separate from source-health history. Transition time comes from an injected trusted UTC
clock. This flow is not a scheduler, daemon, continuous poller, or live stream.

A deterministic generated-fixture recovery drill now composes the evidence, checkpoint, and
shared rate-budget SQLite boundaries across a process-style reopen. One worker exhausts two
retryable disconnect outcomes and retains the exact pending one-millisecond leaf at failed
checkpoint version 3; a newly constructed worker with a fresh UUID fence then consumes exact
empty, one-trade, and empty windows and completes at version 6. The audit chain retains all six
causal transitions, health at versions 3, 5, and 6, five budgeted requests, one retry, two pacing
waits, three raw captures, one canonical trade, and zero conflicts. Reinvocation after completion
does no work and changes no observation. This generated test evidence does not establish
cross-database atomicity, physical durability, continuous operation, or automatic recovery.

Operators and monitoring tools can read the separate candle collector-service state through a
dedicated JSON command:

```text
uv run wealth-collector-health --database storage/collector-service.sqlite3 --collection-id 00000000-0000-0000-0000-000000000001 --pretty
```

The database must already exist and is opened read-only. The command returns `0` for OK, `1` for a
warning, `2` for a confirmed critical alert, and `3` when the result is unknown or cannot be
trusted. It never starts, stops, or repairs the collector.

Shared request-budget coordination can prevent cooperating candle and aggregate-trade processes on
one host from exceeding an explicit combined weighted budget before network access. Binance Spot
aggregate trades use documented cost 4 and USD-M aggregate trades cost 20; capacity, period, and
shared key remain explicit configuration. Multi-host coordination, automatic scheduling, and live
WebSocket ingestion remain future tasks.

## Current Scope

Included:

- Approved foundation documents.
- A strict current-state snapshot, governed backlog, risk register, and explicit approval matrix.
- Versioned security, risk, and execution policies with fail-closed sensitive controls.
- Architecture Decision Records.
- Python package and quality-tool configuration.
- A minimal deterministic event pipeline used to prove validation, storage, logging, and testing boundaries.
- Safe runtime identity with explicit environment and operating mode.
- A first canonical candle contract and point-in-time replay boundary.
- Deterministic candle-sequence quality reports and idempotent in-memory storage.
- A bounded, public Binance REST adapter for closed Spot and USD-M Futures candles.
- A bounded, public Coinbase Exchange adapter for closed Spot candles.
- Fail-closed historical ingestion from provider response through the quality gate.
- Bounded historical pagination, pacing, retry evidence, and safe resume boundaries.
- Durable local SQLite storage for raw evidence, canonical candles, and conflict quarantine.
- Durable collection checkpoints, worker leases, crash recovery, and source-health summaries.
- Weighted shared request budgets with idempotent reservations and observable local backpressure.
- Deterministic cross-source candle reconciliation with explicit tolerances and missing evidence.
- Append-only reconciliation history with bounded source-quality and issue-code summaries.
- Supervised closed-candle polling with durable cursors, bounded reconnects, and restart recovery.
- Interruptible local collector runs with durable lifecycle heartbeats and explicit terminal
  reasons.
- Queryable collector health reports with stale-run detection and structured internal alerts.
- A read-only JSON collector-health command with monitoring-compatible exit codes.
- Strict canonical trade, ticker, and best-bid-ask contracts with point-in-time lineage.
- Bounded order-flow quality auditing and idempotent temporary storage without silent overwrite.
- Durable raw and canonical SQLite order-flow evidence with conflict quarantine.
- Fail-closed order-flow admission through the quality gate before durable storage.
- A bounded, public Binance REST adapter for Spot and USD-M aggregate trades with no credentials.
- Bounded adaptive public-trade range ingestion with retries, pacing, and safe resume evidence.
- Shared durable weighted request-budget gating for candle and public-trade sources.
- Restart-safe bounded public-trade checkpoint and health contracts with a dedicated local SQLite
  control store, UUID fencing, provider-symbol identity, and validated UTC projections.
- Explicitly invoked bounded public-trade checkpoint orchestration with policy validation,
  evidence-first progress, typed pause/failure outcomes, and restart recovery.
- Typed read-only public-trade transition history with bounded checkpoint-version pagination,
  actor-authority validation, restart replay, and fail-closed corruption detection.
- Generated-fixture public-trade disconnect, sparse-window, and process-style reopen recovery evidence
  across the local evidence, control, and shared rate-budget SQLite boundaries.
- Continuous integration and dependency vulnerability auditing.

Not included:

- Private exchange or account access.
- An automatically started operating-system service, deployment, or live WebSocket ingestion.
- Automatic aggregate-trade scheduling, continuous collection, live WebSockets, provider gap
  recovery, an operator transition-history CLI or dashboard, or crash-durable per-job pre-request
  attempt reservations.
- A versioned synthetic public-provider schema-fixture corpus, automatic schema-drift detection,
  and an operator-visible provider schema-drift response runbook.
- Automatic historical collection scheduling.
- Multi-host request-budget coordination.
- Automatic source ranking, price blending, or cross-quote conversion.
- Reconciliation and operational dashboards or automatic remediation.
- External alert delivery, acknowledgement, escalation, or automatic service restart.
- Trading strategies.
- AI model integration.
- Portfolio, risk, or order execution.
- Real credentials or financial actions.
