# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-027 — Canonical UTC clock-boundary enforcement

- **Key:** `phase2.canonical_utc_clock_boundary_enforcement`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Prevent an injected clock value outside Python's fixed `datetime.UTC` zone from
  creating new offset-preserving evidence or control state while preserving every current stored
  contract and representation.
- **Scope:** Add one reusable strict-UTC clock assertion; strengthen the `Clock` contract to UTC;
  apply the assertion at every direct clock read in historical and continuous collection,
  collector service and health, the foundation `HealthCheckService`, shared rate-budget admission,
  Binance and Coinbase public-data adapters, and public-trade orchestration; add
  fail-before-side-effect tests.
- **Constraints:** Clock output only. Do not tighten a timestamp-bearing domain/request model,
  normalize caller request windows, change JSON/text serialization, digest or natural-key
  behavior, alter a database schema or stored row, scan or repair an operator database, call a
  real provider, schedule new work, access credentials, produce a signal, make a portfolio or
  Risk decision, submit an order, or perform any financial action.

Acceptance gates:

1. One shared helper rejects naive, nonzero-offset, and zero-offset non-`datetime.UTC` clock
   results and returns an existing fixed-UTC value unchanged.
2. `Clock.now()` documents canonical UTC, and `SystemClock` plus every accepted injected UTC fake
   conform without changing their timestamp value.
3. Every direct clock call in the scoped applications and adapters is checked before its value
   can reach the next HTTP, storage, reservation, wait, log, or canonical-record side effect.
4. Table-driven tests cover fixed UTC, naive, positive-offset, negative-offset, and a
   fold-capable or named zero-offset timezone. An invalid initial clock produces zero ID,
   downstream, or external calls; an invalid later clock fails before the next mutation.
5. Existing typed error/code mapping remains equivalent at every scoped provider and application
   boundary after adopting the shared helper; invalid later clock reads fail before persistence
   or canonical-evidence creation.
6. Persisted model validation, request-window acceptance, JSON bytes, digests, keys, schemas,
   database contents, and migration state remain unchanged.
7. Relevant unit, integration, format, lint, type, lockfile, health-slice, dependency-audit, and
   CI gates pass.

## Recently Completed

### TASK-026 — Canonical UTC boundary inventory and migration plan

- **Key:** `phase2.canonical_utc_boundary_inventory_and_migration_plan`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0027-canonical-utc-boundary-and-migration-plan.md`
- **Result:** The repository now has an evidence-backed inventory of every discovered
  timestamp-bearing model, clock, provider edge, JSON/text boundary, SQLite projection, order,
  index, cursor, and test path. It selects Python datetimes in the fixed `datetime.UTC` zone, fixed
  microsecond-precision RFC 3339 `Z` text, and derived epoch-microsecond SQL projections as the
  target, with staged compatibility readers, preflight, quarantine, collision handling, digest
  versioning, backup, rollback, and migration verification. No runtime, schema, or data migration
  was performed.
- **Inventory:** `docs/CANONICAL_UTC_BOUNDARY_INVENTORY_AND_MIGRATION_PLAN.md`

### TASK-025 — Typed public-trade transition-history reader

- **Key:** `phase2.public_trade_transition_history_reader`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0026-typed-public-trade-transition-history.md`
- **Result:** The existing append-only SQLite transition ledger is now exposed through an
  immutable typed record and a read-only port with ascending contiguous checkpoint-version pages,
  an exclusive cursor, actor-authority and lifecycle validation, strict bounds, restart behavior,
  and fail-closed projection, canonical-record, continuity, and corruption checks. The existing
  schema is unchanged.

### TASK-024 — Public-trade checkpoint orchestrator

- **Key:** `phase2.public_trade_checkpoint_orchestrator`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Decision:** `docs/decisions/0025-bounded-public-trade-checkpoint-orchestration.md`
- **Result:** One explicitly invoked bounded application flow now composes the public-trade range
  collector, durable request budget, market-evidence admission, and restart-safe checkpoint
  control with policy validation, UUID fencing, evidence-first progress, typed outcomes, and
  injected UTC time.

## Queued, Not Yet Approved

- Design continuous public-trade collection only after typed transition audit access and
  operational recovery drills are accepted.

## Backlog Rules

- Only one item may be the canonical next action.
- A task must define goal, scope, constraints, acceptance evidence, and excluded work before code.
- Missing approval, policy, state, or critical evidence fails closed.
- Completing a task does not authorize deployment, mode promotion, private access, or trading.
