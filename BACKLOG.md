# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-028 — Additive canonical UTC codec primitives

- **Key:** `phase2.canonical_utc_codec_primitives`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Add pure, unused canonical UTC conversion and text-codec primitives so later
  compatibility work can target one exact representation without changing any active contract.
- **Scope:** Add a reusable fixed-`datetime.UTC` value validator, an explicit aware-to-UTC edge
  normalizer, an exact six-fractional-digit RFC 3339 `Z` serializer, a strict canonical parser,
  and exhaustive deterministic tests.
- **Constraints:** Additive primitives only. Do not wire them into a model, provider adapter,
  request, stored row, JSON/log/CLI output, digest, natural key, SQLite projection, schema, or
  operator database. Do not call a real provider, access credentials, produce a signal, make a
  portfolio or Risk decision, submit an order, or perform any financial action.

Acceptance gates:

1. The fixed-UTC validator accepts only values whose `tzinfo is datetime.UTC` and returns the
   original object unchanged.
2. The edge normalizer rejects naive values and converts every aware positive, negative, named,
   regional, and fold-capable input to the same instant with `tzinfo is datetime.UTC`.
3. The serializer emits exactly `YYYY-MM-DDTHH:MM:SS.ffffffZ`; the parser accepts only that
   canonical form, rejects offset or variable-precision alternatives, and returns fixed UTC.
4. Boundary and property-style tests cover microsecond extremes, calendar limits, malformed
   input, named/rule-based zones, folds, exact round trips, and instant preservation.
5. No existing runtime path imports or calls the new codec primitives, and current request/model
   acceptance, serialized bytes, digests, keys, schemas, projections, and stored data are
   unchanged.
6. Relevant unit, integration, format, lint, type, lockfile, health-slice, dependency-audit, and
   CI gates pass.

## Recently Completed

### TASK-027 — Canonical UTC clock-boundary enforcement

- **Key:** `phase2.canonical_utc_clock_boundary_enforcement`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One shared exact-`datetime.UTC` assertion now guards every direct injected-clock
  read in the scoped foundation, application, rate-budget, provider, and public-trade boundaries.
  Invalid initial values fail before IDs or downstream mutations; invalid later reads fail before
  the next side effect; provider and application error mappings remain typed. Persisted models,
  request acceptance, JSON, digests, keys, schemas, projections, and stored data are unchanged.

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
