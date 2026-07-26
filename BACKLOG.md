# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-035 — Synthetic all-family candidate-census bundle reconciliation evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_candidate_census_bundle_evidence_foundation`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Goal:** Add an unused pure all-family reconciliation layer over the complete reviewed set of
  TASK-034 synthetic candidate censuses while retaining every input result unchanged.
- **Scope:** Consume exactly eight exact TASK-034 results, one per reviewed family in canonical
  family order. Reconcile exactly eight families, 20 tables, and 37 declared timestamp columns,
  and aggregate only the existing total and exhaustive status counts, source-offset and
  fractional-precision frequencies, and projectable canonical epoch extrema.
- **Constraints:** Pure evidence consumption only: no SQLite, filesystem, adapter,
  JSON-container, serialization, report, manifest, operator-data, or runtime access. Do not
  compare stored projections, group rows or instants, assign collision identities, deduplicate,
  merge, quarantine, choose replacement bytes, migrate, repair, alter a schema, or add CLI,
  service, provider, credential, signal, portfolio, Risk, order, financial, or active runtime
  wiring. Do not claim Stage 3 completion.

Acceptance gates:

1. Strict frozen plans and bundle evidence reject altered, missing, duplicate, reordered,
   unsupported, or incorrectly linked TASK-034 inputs and aggregate declarations.
2. Construction accepts exactly eight exact successful TASK-034 results in reviewed family order
   and performs no I/O, adapter, serialization, report, manifest, or runtime operation.
3. The bundle reconciles exactly eight families, 20 tables, and 37 per-column summaries while
   retaining every complete TASK-034 result and all nested TASK-030/031/032/033 evidence.
4. Aggregate totals, exhaustive status counts, bounded sorted offset and precision frequencies,
   and projectable epoch extrema reconcile exactly to the unchanged family censuses.
5. Empty families or columns, mixed outcomes, duplicate instants, normalization overflow, signed
   offsets, precision variants, and epoch bounds remain deterministic without row or instant
   grouping.
6. No operator data, stored-projection comparison, collision identity, deduplication, merge,
   quarantine, replacement, migration, schema change, or Stage 3 claim is introduced, and all
   repository gates pass.

## Recently Completed

### TASK-034 — Synthetic canonical-candidate census evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_candidate_census_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** Eight immutable pure family-scoped census plans now flatten the exact TASK-033
  declarations into one ordered summary per source-family timestamp column, collectively covering
  all eight families, 20 tables, and 37 columns, including genuinely empty columns. Every summary
  exactly reconciles its total, exhaustive candidate and parse status counts, bounded sorted
  source-offset and fractional-precision frequencies, and projectable epoch extrema while
  retaining the complete TASK-033 and nested TASK-030/031/032 evidence unchanged. Deep validation
  rejects forged plans, candidates, declarations, summaries, counts, frequencies, extrema,
  registry replacement, and reordered or missing evidence. Synthetic tests cover all families,
  empty and mixed columns, malformed and nullable inputs, signed and subminute offsets, precision,
  duplicate instants, calendar overflow, epoch bounds, and post-snapshot no-I/O behavior. No
  report, operator scan, grouping, collision identity, replacement, runtime consumer, migration,
  schema change, or Stage 3 completion was added.

### TASK-033 — Synthetic canonical-instant candidate evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_canonical_candidate_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One immutable pure registry now wraps the exact eight TASK-032 plans and freezes the
  complete two-success/eight-nonprojectable status partition. Every ordered parse outcome retains
  its source evidence and receives either an exact built-in `datetime.UTC`, exact 27-character
  six-fractional-digit `Z` text, and exact epoch-microsecond triple; a typed year-boundary
  normalization overflow; or a source-not-projectable disposition. Epoch and text candidates
  round-trip through the TASK-028/029 primitives. Tests cover all eight families and 37 columns,
  positive, negative, and subminute offsets, exact calendar and epoch bounds, equal instants with
  distinct spellings retained separately, every prior failure status, forged nested evidence,
  registry replacement, ordering, and post-snapshot no-I/O behavior. No collision grouping,
  report, operator scan, replacement byte, runtime consumer, migration, or Stage 3 completion was
  added.

### TASK-032 — Synthetic SQLite timestamp parse-evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_parse_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One immutable pure registry now binds the exact TASK-031 plans for all eight store
  families to 20 offset-preserving Python `isoformat` text columns, 15 fixed-UTC `isoformat` text
  columns, two signed epoch-microsecond integer columns, and the exact five nullable declarations.
  Manual component parsing plus exact writer round trips preserve offset spelling and subsecond
  offsets without normalization. Every source cell receives one typed outcome for aware text,
  fixed-UTC policy mismatch, naive text, declared absence, malformed UTF-8/text/epoch bytes,
  calendar-range overflow, or unexpected SQLite storage. Deep validation rejects forged plans,
  snapshots, rows, keys, cells, outcomes, and public-registry replacement before parsing.
  Synthetic end-to-end tests cover all 37 columns and hostile TEXT, NULL, INTEGER, REAL, and BLOB
  evidence while retaining exact bytes, row order, TASK-030 fingerprint, TASK-031 plan, and
  snapshot identity. The module performs no I/O, has no runtime consumer, scans no operator data,
  emits no replacement bytes, and does not complete Stage 3.

### TASK-031 — Synthetic SQLite timestamp-byte evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One pinned strict extraction plan per TASK-030 family now declares all 20 direct
  timestamp-bearing tables and 37 timestamp columns. The unused generated-fixture-only inspector
  fingerprints and extracts through the same immutable connection and exact whole-file snapshot,
  fails before row access unless exactly one expected family matches, and temporarily authorizes
  only each declared stable key and timestamp target. Bounded deterministic evidence preserves
  SQLite `typeof`, exact `hex(CAST(column AS BLOB))`, byte length, row-key bytes, and snapshot
  linkage without materializing a raw timestamp value. Tests cover all layouts, NULL, INTEGER,
  REAL, TEXT, BLOB, malformed text bytes, ordering, bounds, oversized cells, mismatches,
  wrong-family and ambiguity rejection, and unchanged source/directory evidence. No operator
  database, parser, report, manifest, runtime consumer, migration, or repair was added, and Stage
  3 remains incomplete.

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
