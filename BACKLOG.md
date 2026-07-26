# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; later items are directional until promoted through review.

## Next Action

### TASK-037 — Operator-preflight authorization package and project-owner decision

- **Key:** `phase2.canonical_utc_preflight_operator_authorization_package_owner_decision`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 3 — PRODUCTION AFFECTING (authorization decision only)
- **Status:** READY
- **Human approval:** REQUIRED — project owner plus independent Risk and Security review.
- **Preparation artifact:** The repository contains only the non-authorizing placeholder template
  at `docs/governance/TASK-037-operator-preflight-authorization-package.template.md`. It prohibits
  real deployment values and cannot satisfy any acceptance gate or approval requirement.
- **Goal:** Prepare the exact operator-preflight authorization package and obtain an explicit
  project-owner `APPROVE`, `REJECT`, or `REVISE` decision without accessing operator data.
- **Scope:** Record the proposed exact read-only database/path list and its real deployment
  cardinality, the writer-fenced consistent/immutable snapshot procedure, report destination,
  evidence retention/disposal boundary, and the change, environment, evidence, approver, UTC
  decision time, expiry or review trigger, monitoring, and tested rollback evidence required by
  policy.
- **Constraints:** Governance preparation and decision only. Do not inspect, resolve, check, or
  open any proposed operator path or database; access SQLite or operator data; scan rows; invoke
  an adapter; create an operational report or manifest; add serialization or scanner code; wire a
  runtime; migrate or repair data; alter a schema; or perform or claim Stage 3. Approved
  governance-artifact writes are the only filesystem mutation in scope. Do not store sensitive
  path metadata in an unapproved location. Missing, ambiguous, expired, conflicting, rejected, or
  revise-required authority remains denial.

Acceptance gates:

1. The package distinguishes the real proposed path-list cardinality from TASK-036's eight
   synthetic family-coverage slots and makes every exact family/path entry explicit.
2. The exact snapshot procedure is writer-fenced and SQLite-safe and specifies its consistency,
   immutability, and WAL/checkpoint handling without executing it.
3. The exact report destination and evidence retention/disposal boundary are identified through
   an approved handling location, with independent Risk and Security review recorded.
4. The decision identifies the change, scope, environment, evidence, project-owner approver, UTC
   decision time, expiry or review trigger, monitoring, tested rollback evidence, and one explicit
   `APPROVE`, `REJECT`, or `REVISE` outcome. Anything else fails closed.
5. No proposed operator path or database is inspected, resolved, checked, or opened; no SQLite or
   operator-data access, scan, adapter, operational report or manifest, serializer, scanner code,
   runtime action, migration, repair, schema change, or Stage 3 action occurs. Only approved
   governance-artifact writes are allowed.
6. Any approved scanner remains a separately scoped later task with its own risk review; no
   approval outcome automatically runs or authorizes code beyond its exact recorded scope, and
   all repository gates pass.

## Recently Completed

### TASK-036 — Synthetic operator-preflight authorization-request contract foundation

- **Key:** `phase2.canonical_utc_preflight_operator_authorization_request_contract_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One unused strict frozen proposal plan is pinned to the private exact TASK-035
  all-family bundle plan. A zero-argument pure builder emits exactly eight ordered immutable
  literal slots linked to the reviewed families plus fixed unselected snapshot-method,
  report-destination, and retention/disposal placeholders. Fixed states remain `proposal_only`,
  `none_proposal_only`, human approval `not_recorded`, operator-data access `not_authorized`, and
  the Stage 3 gate `not_satisfied`; successful construction, validation, review, or merge grants
  no authority. Deep strict validation rejects altered plans, ordinals, family/slot mappings,
  placeholders, subclasses, bypassed construction, extras, and any authority-state forgery. The
  eight symbolic slots prove synthetic family coverage only and do not assert that a future real
  deployment has one path per family; a later populated package must establish its own reviewed
  cardinality. No real path, path check, filesystem, SQLite, operator data, scan, adapter, report,
  manifest, serialization, runtime consumer, scanner, migration, schema change, or Stage 3 action
  was added.

### TASK-035 — Synthetic all-family candidate-census bundle reconciliation evidence foundation

- **Key:** `phase2.canonical_utc_preflight_timestamp_candidate_census_bundle_evidence_foundation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** One immutable pure bundle plan now pins the complete reviewed TASK-034 plan sequence
  and accepts only an exact built-in tuple of eight deeply valid family census results in
  canonical order. The result retains all eight TASK-034 inputs and their nested evidence
  unchanged while exactly reconciling eight families, 20 tables, 37 columns, total and exhaustive
  status counts, sorted source-offset and precision frequencies, and projectable epoch extrema.
  Deep validation rejects missing, duplicate, reordered, forged, subclassed, or altered sources,
  plans, counts, frequencies, and extrema before aggregation. Synthetic tests cover the exact
  one-row aggregate, all-empty families, mixed outcomes and bounds, hostile source sequences,
  deep forgery before aggregation, and post-TASK-034 no-I/O behavior. No filesystem, SQLite,
  operator scan, stored-projection comparison, row/instant/collision grouping, serialization,
  report, manifest, adapter, replacement, runtime consumer, migration, schema change, or Stage 3
  completion was added.

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
