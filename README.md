# WEALTH

WEALTH is a governed multi-agent cryptocurrency research and trading platform. The project is
currently building its reliable market-data platform. It can read bounded historical candle ranges
from Binance and Coinbase public endpoints and supervise restart-safe polling of closed candles,
but it has no account access and cannot execute trades.

## Read First

- `docs/PROJECT_CHARTER.md`
- `docs/AI_DEPARTMENTS.md`
- `docs/ARCHITECTURE.md`
- `docs/ROADMAP.md`
- `AGENTS.md`

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
WEALTH_LOG_LEVEL=INFO
```

These values identify the environment and operating mode. They do not grant execution
authority. No current code path can submit an order.

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

Shared request-budget coordination can prevent cooperating processes on one host from exceeding an
explicit combined budget before network access. Multi-host coordination, automatic scheduling, and
live WebSocket ingestion remain future tasks.

## Current Scope

Included:

- Approved foundation documents.
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
- Continuous integration and dependency vulnerability auditing.

Not included:

- Private exchange or account access.
- An automatically started operating-system service, deployment, or live WebSocket ingestion.
- Automatic historical collection scheduling.
- Multi-host request-budget coordination.
- Automatic source ranking, price blending, or cross-quote conversion.
- Reconciliation and operational dashboards or automatic remediation.
- External alert delivery, acknowledgement, escalation, or automatic service restart.
- Trading strategies.
- AI model integration.
- Portfolio, risk, or order execution.
- Real credentials or financial actions.
