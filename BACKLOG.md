# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-031 — Synthetic SQLite timestamp-byte evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_evidence_foundation`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Extend the unused fixture-only preflight so exact timestamp-byte evidence can be
  extracted deterministically from generated SQLite fixtures, but only after TASK-030 identifies
  one exact registered store layout.
- **Scope:** Add strict frozen versioned extraction-plan and observed-evidence contracts for every
  current timestamp-bearing SQLite column. Record the table, stable row key, SQLite `typeof`,
  exact `hex(CAST(column AS BLOB))`, byte length, deterministic bounded order, and linkage to the
  unchanged TASK-030 snapshot identity. Parsing, normalization, collision analysis, quarantine,
  manifests, and actual operator scans remain later tasks.
- **Constraints:** Generated temporary fixtures only. Do not inspect an operator, user-selected,
  deployment, or discovered database path. Do not add CLI, service, adapter, or active runtime
  wiring. Do not write a report or manifest. Row access must reuse the direct immutable
  `mode=ro&immutable=1` boundary and must stop unless the exact TASK-030 fingerprint matches one
  expected family. It must not invoke a normal adapter, create directories, install schemas,
  enable WAL, or write beside the source. Do not migrate or repair data, change canonical truth,
  call a provider, access credentials, produce a signal, make a portfolio or Risk decision,
  submit an order, or perform any financial action. Do not claim Stage 3 completion.

Acceptance gates:

1. Versioned strict frozen plans and evidence reject unknown fields, undeclared tables or columns,
   unstable row keys, unbounded limits, and duplicate extraction targets.
2. No timestamp row is read until the exact TASK-030 fingerprint matches one and only one
   registered expected family; mismatches and ambiguity fail closed.
3. Generated fixtures cover every current timestamp-bearing SQLite column plus hostile NULL,
   INTEGER, REAL, TEXT, and BLOB cells without silently coercing the original storage class or
   bytes.
4. Extraction is bounded and deterministic, preserves explicit stable row-key evidence, and links
   every result to the exact unchanged snapshot identity.
5. Source hash, size, modification time, file identity, and directory entries remain unchanged;
   no journal, WAL, SHM, report, or manifest is created.
6. No existing runtime path imports or calls the foundation, and relevant unit, integration,
   format, lint, type, lockfile, health-slice, dependency-audit, and CI gates pass.

## Recently Completed

### TASK-030 — Synthetic read-only SQLite preflight fingerprint foundation

- **Key:** `phase2.canonical_utc_preflight_fingerprint_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** Strict frozen version-1 contracts now keep the expected family identity separate
  from observed evidence. A direct `mode=ro&immutable=1` inspector fingerprints encoding,
  application and user versions, exact typed marker bytes, normalized DDL, every schema object,
  tables, columns, foreign keys, explicit and implicit indexes, and triggers for all eight
  generated SQLite layouts. Exact pinned digests reject missing, extra, renamed, altered,
  spoofed, combined, wrong-family, or ambiguous layouts before timestamp rows can be read. Source
  hash, size, modification time, file identity, directory entries, and sidecar absence are
  reverified; an authorizer denies writes, temporary objects, `ATTACH`, and write pragmas. The
  foundation remains unused, scans no operator database or timestamp row, writes no report, and
  does not complete Stage 3.

### TASK-029 — Additive exact epoch-microsecond projection primitives

- **Key:** `phase2.canonical_utc_epoch_microsecond_primitives`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** The isolated canonical-UTC module now exposes exact signed bounds plus integer-only
  projection and inverse decoding between strict fixed-`datetime.UTC` values and Unix-epoch
  microseconds. Strict type and range handling rejects booleans, non-integers, and values outside
  Python's calendar; deterministic, property-style, and hostile-subclass tests prove exact
  negative/zero/positive landmarks, full-range round trips, one-microsecond distinction, and
  monotonic order. No runtime consumer, model, serialized byte, digest, identity, schema, query,
  projection, migration, or stored record changed.

### TASK-028 — Additive canonical UTC codec primitives

- **Key:** `phase2.canonical_utc_codec_primitives`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One isolated pure module now provides strict fixed-`datetime.UTC` validation,
  explicit aware-input normalization, exact six-fractional-digit RFC 3339 `Z` serialization, and a
  strict canonical parser. Exhaustive deterministic and property-style tests cover offsets,
  named/rule-based zones, folds, calendar limits, malformed text, exact round trips, and hostile
  datetime subclasses. No existing runtime path imports or calls the helpers, and no model,
  serializer, digest, identity, schema, projection, query, or stored record changed.

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
