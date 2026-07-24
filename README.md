# WEALTH

WEALTH is a governed multi-agent cryptocurrency research and trading platform. The project is
currently building its reliable market-data platform. It can read bounded public candle windows
from Binance, but it has no account access and cannot execute trades.

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

Every response is normalized into the provider-independent candle contract and sent through the
deterministic sequence-quality gate. Incomplete, conflicting, malformed, or time-inconsistent
batches are reported and are not written to storage.

Accepted batches can now be stored in a file-backed SQLite adapter. The adapter keeps exact raw
provider bytes separately from canonical candles, verifies them again when reading, survives
restart, treats repeats idempotently, and quarantines conflicting revisions without overwriting the
accepted record.

The current adapter is deliberately bounded to one window of at most 1,000 candles. Pagination,
continuous scheduling, retry policy, and live WebSocket ingestion remain separate future tasks.

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
- Fail-closed historical ingestion from provider response through the quality gate.
- Durable local SQLite storage for raw evidence, canonical candles, and conflict quarantine.
- Continuous integration and dependency vulnerability auditing.

Not included:

- Private exchange or account access.
- Continuous or live-streaming market-data collection.
- Historical pagination and automatic retry scheduling.
- Trading strategies.
- AI model integration.
- Portfolio, risk, or order execution.
- Real credentials or financial actions.
