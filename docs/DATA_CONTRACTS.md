# Data Contracts

## Purpose

This index identifies the canonical Phase 2 data boundaries. The detailed behavioral contract is
[`contracts/MARKET_DATA.md`](contracts/MARKET_DATA.md); executable truth is the strict Pydantic
model and its tests. The cross-boundary timestamp evidence and staged target are recorded in the
[`Canonical UTC boundary inventory and migration plan`](CANONICAL_UTC_BOUNDARY_INVENTORY_AND_MIGRATION_PLAN.md).

## Active Contracts

| Boundary | Canonical implementation | Contract status |
| --- | --- | --- |
| Domain event envelope | `wealth.domain.events.DomainEvent` | Active foundation; immutable, versioned, UTC-only |
| Raw market evidence and candles | `wealth.domain.market` | Active |
| Public trades, ticker, and best bid/ask | `wealth.domain.order_flow` | Trade path active; ticker and best bid/ask are contracts only |
| Candle quality | `wealth.domain.quality` | Active, fail closed |
| Order-flow quality | `wealth.domain.order_flow_quality` | Active, fail closed |
| Bounded collection state | `wealth.domain.collection` | Active |
| Continuous candle collection state | `wealth.domain.continuous_collection` | Active |
| Public-trade collection state | `wealth.domain.order_flow_collection` and `wealth.ports.order_flow_collection` | Active checkpoint, health, and immutable transition contracts; bounded orchestration and causally validated transition-history reader active |
| Public-trade collection application | `wealth.application.public_trade_collection` | Active, explicitly invoked, bounded, evidence-first |
| Shared provider-rate budget | `wealth.domain.rate_budget` | Active |
| Foundation clock | `wealth.ports.foundation.Clock` and `require_utc_clock` | Active; injected results require `tzinfo is datetime.UTC` before downstream side effects |
| Cross-source reconciliation | `wealth.domain.reconciliation` and `wealth.domain.reconciliation_history` | Active |
| Repository operating state | `wealth.domain.project_state.ProjectState` | Active; validates `PROJECT_STATE.json` |
| Canonical UTC primitives | `wealth.domain.canonical_utc` | Pure and intentionally unused; strict fixed-UTC validation, explicit edge normalization, exact six-digit RFC 3339 `Z` serialization/parsing, and exact signed epoch-microsecond projection/decoding |
| SQLite preflight fingerprint and timestamp-byte contracts | `wealth.domain.sqlite_preflight` and `wealth.adapters.sqlite_preflight` | Intentionally unused and generated-fixture-only; strict expected identity remains separate from the immutable fingerprint, exact snapshot, and bounded raw evidence for all 37 direct timestamp columns |
| Canonical UTC target | ADR 0027 and the UTC boundary inventory | Clock enforcement plus the isolated codec, projection, synthetic fingerprint, and synthetic raw timestamp-byte foundations are complete; parse evidence, operator preflight, persisted-contract, query, schema, and migration work remain open under `RISK-005` |

## Contract Rules

- Unknown fields and mutable records are rejected by the strict canonical models. `AwareDatetime`
  rejects naive timestamps. Non-UTC aware timestamps are currently rejected only at the explicit
  strict boundaries identified in the UTC inventory; most other timestamp-bearing models remain
  aware-only while `RISK-005` is open.
- `Clock.now()` must return a value whose `tzinfo is datetime.UTC`. The shared clock assertion
  rejects naive, nonzero-offset, and named/rule-based zero-offset results without normalizing
  them. This rule applies only to injected clock output; it does not tighten existing persisted
  models, request windows, or explicit legacy timestamp inputs.
- The accepted target is a Python datetime in the fixed `datetime.UTC` zone, fixed
  microsecond-precision RFC 3339 `Z` text, and checked epoch-microsecond SQL projections.
  Zero offset alone is insufficient because a regional timezone can have offset zero only for
  part of the year. ADR 0027 does not authorize a runtime, schema, or stored-data migration.
- The pure canonical UTC module validates datetimes without copying, normalizes only through an
  explicit edge function, emits exactly `YYYY-MM-DDTHH:MM:SS.ffffffZ`, and accepts only that exact
  text. Its epoch projector uses integer `timedelta` arithmetic; inverse decoding rejects
  booleans, non-integers, and values outside Python's representable calendar, then returns fixed
  `datetime.UTC`. Exact bounds, negative/zero/positive values, full-range round trips, and order
  preservation are tested. No existing runtime, model, serializer, provider, digest, key, query,
  projection, or persistence path imports or calls these primitives yet.
- The SQLite preflight accepts only a request labelled as a generated synthetic fixture and selects
  the trusted identity from a pinned registry rather than caller-supplied DDL or digests. Its
  direct `mode=ro&immutable=1` connection fingerprints exact typed marker bytes and the complete
  logical schema, while source-file and directory identity remain separate observed evidence.
  Only after one exact family match, the same connection may temporarily read the pinned stable
  keys and 37 direct timestamp columns from generated fixtures. Strict bounded evidence records
  `typeof`, exact `hex(CAST(column AS BLOB))`, byte length, deterministic row order, and the
  unchanged snapshot identity without selecting a raw timestamp value. It rejects sidecars,
  mutations, excess rows, oversized cells, and unstable keys and never invokes a
  schema-installing adapter. No parser, runtime consumer, operator path, report, repair, or
  migration exists.
- Source identity, event time, observation time, processing time, schema version, and lineage are
  retained whenever applicable.
- Missing, stale, conflicting, truncated, or unverifiable evidence is represented explicitly and
  fails closed; critical data is never invented.
- Provider details remain behind ports and adapters. Canonical records do not expose account,
  credential, or order capabilities.
- A schema or semantic change requires tests, documentation, and a decision record when it is
  material or incompatible.

Signal, decision, intent, risk-approval, execution, and outcome contracts are intentionally absent
until their roadmap phases begin. Their absence is a safety boundary, not an implicit dictionary
contract.
