# Data Catalog

## Scope

This catalog lists data that the current Phase 2 repository can acquire or retain. It is an
inventory of capabilities, not a claim that a continuously running production dataset exists.

## Approved External Sources

| Catalog ID | Provider and dataset | Access | Canonical output | Current limits |
| --- | --- | --- | --- | --- |
| `binance.public.candles` | Binance public closed OHLCV candles | Unauthenticated, read-only REST | `RawMarketPayload`, `CanonicalCandle` | Explicit bounded windows; provider limits and rate budget apply |
| `coinbase.exchange.public.candles` | Coinbase Exchange public closed OHLCV candles | Unauthenticated, read-only REST | `RawMarketPayload`, `CanonicalCandle` | Explicit bounded windows; provider limits and rate budget apply |
| `binance.public.aggregate_trades` | Binance public spot and USD-M aggregate trades | Unauthenticated, read-only REST | `RawMarketPayload`, `CanonicalTrade` | Closed millisecond windows; density cap, adaptive split, retry, and rate budget apply |

No source uses a private account endpoint. No API key, balance, position, order, withdrawal,
personal, news, social, macro, on-chain, or model-training dataset is approved or active.

## Internal Stores

| Catalog ID | Content | Storage and retention behavior |
| --- | --- | --- |
| `local.raw_market_evidence` | Exact bounded response bytes, digest, timing, and lineage | Versioned SQLite adapter; local deployment chooses the database path |
| `local.canonical_candles` | Validated provider-scoped candles | Versioned SQLite; idempotent duplicates, quarantined conflicts |
| `local.canonical_order_flow` | Validated provider-scoped public trades | Versioned SQLite; idempotent duplicates, quarantined conflicts |
| `local.collection_control` | Checkpoints, transitions, leases, and health observations | Dedicated versioned SQLite control stores |
| `local.reconciliation_history` | Cross-source comparison evidence and metrics | Versioned SQLite |
| `repository.project_state` | Compact governed snapshot of phase, controls, risks, and next work | Version-controlled `PROJECT_STATE.json`; validated before acceptance |

## Quality and Provenance

Every admitted external batch retains its raw evidence and source lineage. Quality gates run before
canonical admission. Cross-source agreement is measured rather than assumed. Unknown schema
versions, corrupted digests, conflicting natural keys, unsafe time windows, and incomplete bounded
responses are rejected.

Dataset ownership belongs to the Market Data function; assurance may quarantine or reject evidence.
Any new source, authentication mode, retention class, or material schema requires an approved task,
catalog update, security review, and the applicable decision record.
