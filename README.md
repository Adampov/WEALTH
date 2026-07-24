# WEALTH

WEALTH is a governed multi-agent cryptocurrency research and trading platform. The project is currently building its engineering foundation; it does not connect to an exchange or execute trades.

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

## Current Scope

Included:

- Approved foundation documents.
- Architecture Decision Records.
- Python package and quality-tool configuration.
- A minimal deterministic event pipeline used to prove validation, storage, logging, and testing boundaries.
- Safe runtime identity with explicit environment and operating mode.
- A first canonical candle contract and point-in-time replay boundary.
- Continuous integration and dependency vulnerability auditing.

Not included:

- Exchange adapters.
- Live market data.
- Trading strategies.
- AI model integration.
- Portfolio, risk, or order execution.
- Real credentials or financial actions.
