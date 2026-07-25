# ADR 0027: Canonical UTC Boundary and Migration Plan

- **Status:** Accepted
- **Date:** 2026-07-25
- **Decision owners:** Project owner, Market Data Department, Engineering Department, Security
  Department, and Audit Department

## Task Contract

### Goal

Inventory every repository timestamp boundary still covered by `RISK-005`, choose one canonical
UTC target, and define a staged compatibility, quarantine, verification, and rollback plan without
changing runtime or stored state.

### Context

ADR 0024 established UTC as the internal-system baseline and explicitly left global conformance
open under `RISK-005`. Subsequent Phase 2 slices added provider, quality, reconciliation,
collection-control, service-health, and rate-budget contracts.

Most of those models use `AwareDatetime`. That prevents naive values but accepts and retains
nonzero offsets. Python comparison treats those values as instants, while several SQLite paths
store direct ISO 8601 text in keys and ordered columns. Other stores normalize indexed
projections to UTC while retaining the original offset in canonical JSON. Neither pattern is a
global canonical representation.

The evidence-backed field, application, provider, persistence, ordering, clock, and test inventory
is
[`CANONICAL_UTC_BOUNDARY_INVENTORY_AND_MIGRATION_PLAN.md`](../CANONICAL_UTC_BOUNDARY_INVENTORY_AND_MIGRATION_PLAN.md).

### Scope

- Enumerate every timestamp-bearing domain, port, application, adapter, persistence, JSON, text,
  clock, provider, comparison, ordering, index, and cursor boundary discoverable in the repository.
- Classify current validation, normalization, serialization, impact, compatibility, and test
  coverage.
- Choose one canonical UTC Python and text representation plus one derived sortable SQLite
  projection.
- Define staged drift prevention, compatibility reading, preflight, contract conversion, schema
  migration, quarantine, verification, rollback, and legacy retirement.
- Record the approvals that remain required before an incompatible contract, canonical-truth,
  schema, or stored-data change.

### Constraints

- Do not change a runtime timestamp contract, serializer, application behavior, database schema,
  stored row, migration, provider call, scheduler, or project database.
- Do not repair or quarantine actual data, access credentials, enable external notifications,
  produce a strategy or signal, make a portfolio or Risk decision, submit an order, or perform any
  financial action.
- Unknown local data and consumer behavior must remain explicit rather than inferred from current
  writer code.

### Done When

- Every timestamp-bearing model and persistence path has an owner, evidence link, current
  representation, guarantee or gap, impact, compatibility concern, and proposed treatment.
- Canonical JSON, indexed projections, SQL order, cursors, Python comparisons, clocks, and
  provider inputs are traced separately.
- The target defines strict internal UTC, exact serialized text, sortable projections, and
  deterministic tie-breaks.
- The plan defines migration order, legacy compatibility, collision policy, quarantine, backup,
  rollback, verification, and regression evidence.
- The roadmap, backlog, risk register, data-contract index, decision index, and project state
  identify the result and one bounded next action.
- No implementation or migration is implied by accepting this planning decision.

### Not Included

- A shared datetime type or serializer implementation.
- Tightening an existing Pydantic model, port, clock, or application boundary.
- A database preflight tool, compatibility reader, schema version 2, migration runner, repair
  command, or stored-data rewrite.
- Provider/network access, continuous public-trade scheduling, live streaming, multi-host
  coordination, private or account data, credentials, strategies, signals, portfolio state,
  approvals, orders, execution, or real-money behavior.

## Decision

### Canonical instant

An internal canonical timestamp is a timezone-aware Python `datetime` whose `tzinfo` is the fixed
`datetime.UTC` singleton. Zero offset alone is insufficient: a regional or rule-based zone can
have offset zero for one date and change after arithmetic. Canonical domain and application
boundaries reject naive values, nonzero offsets, and zero-offset non-UTC zones; they do not
silently normalize them.

Normalization is permitted only at an explicit provider/input adapter, canonical-text decoder, or
versioned legacy reader. It returns a `datetime.UTC` instance. That edge must retain the original
provider bytes or legacy serialized bytes and declared version for audit and quarantine.

### Canonical text

Canonical JSON and human/audit text use RFC 3339 UTC with a literal `Z` and exactly six fractional
digits:

`YYYY-MM-DDTHH:MM:SS.ffffffZ`

Microseconds retain the precision already supported by current Python models. Fixed width prevents
multiple canonical spellings of one instant, and the canonical decoder produces `datetime.UTC`.
`Asia/Jerusalem` remains presentation-only.

### Sortable projection and causal order

Queryable SQLite time projections use signed integer microseconds since the Unix epoch and are
checked against canonical JSON. Timestamp text is not used for identity, range membership, or
chronological ordering in version-2 storage.

Every ordered query includes a deterministic non-time tie-break. Existing version and sequence
cursors remain the authority for public-trade transitions, public-trade health, and
collector-service history. Migration must not replace causal order with timestamp order.

### Compatibility and evidence

Version-1 JSON/text remains a legacy representation. A strict version-2 writer cannot be enabled
until a separate legacy reader can preserve original bytes, normalize valid aware values at its
edge, and return typed quarantine evidence for malformed, naive, overflowing, causally invalid,
projection-disagreeing, or identity-colliding rows.

Raw provider response bytes remain immutable. Timestamp normalization applies to canonical
metadata and derived records, not to bytes received from a provider.

Reconciliation version 2 persists explicit `serialization_version` and `digest_algorithm` values
and includes both in a domain-separated digest input. Version 1 stored exact enclosing observation
`record_json` and `report_sha256`, not a separate report-byte blob. Its compatibility reader
preserves those stored values and uses a frozen historical parser/serializer plus fixtures to
reproduce and verify the digest without claiming independent original-report byte preservation.
It does not infer digest behavior from model `schema_version` or the installed Pydantic release,
and a version-1 digest is never silently recomputed over normalized JSON.

### Migration sequence

Implementation proceeds through separately accepted stages:

1. prevent new drift by enforcing the existing UTC clock policy at every direct clock boundary,
   without changing persisted models or serializers;
2. add and test the shared strict validator, explicit edge normalizer, fixed text codec, and exact
   epoch-microsecond projection;
3. after explicit path and retention authorization, run a bounded direct-SQLite `mode=ro`
   preflight over writer-fenced snapshots; verify exact schema/store fingerprints rather than
   trusting `user_version`, and record externally anchored manifests, exact stored-byte evidence,
   parse failures, offset distributions, projection disagreements, and collision groups;
4. add versioned legacy readers before tightening models, then convert provider/core evidence,
   derived quality/reconciliation, operational control, and public-trade contracts in dependency
   order;
5. create each version-2 store as a separate physical database, shadow-read both versions at one
   recorded snapshot generation/watermark, and cut over one store at a time through an atomic
   routing marker;
6. migrate or quarantine every row with counts, hashes, identities, digests, chronology, lineage,
   lease, cursor, and restore verification; and
7. reject legacy writes and retire legacy readers only after the compatibility and rollback
   windows close.

Public-trade control is the candidate storage pilot because its projections already normalize UTC
and its histories use causal versions, but it is not selected until its connected-family
quarantine and operational halt/resume rules are accepted. Canonical candle and order-flow
evidence follow, then reconciliation history, historical collection, continuous collection,
collector-service lifecycle, and rate-budget history. Read-only preflight evidence may change
that order only through a recorded decision.

### Collision, quarantine, and rollback

Equal instants with different legacy text are not merged automatically. An affected contract owner
may approve deduplication only when every non-time value and lineage rule proves equivalence.
Otherwise all source rows are preserved and excluded from canonical promotion.

Quarantine operates on an approved connected record family, not an arbitrary row. Checkpoints,
transitions, health, pending leaves, and lease/fencing evidence remain together; rate-budget state,
reservations, and decisions remain together; and raw/canonical/conflict lineage stays traceable
through typed quarantine references. An unsafe required member halts promotion of its dependent
component until the owner approves resume criteria.

Every migration fences writers and uses the SQLite Online Backup API or an equivalently proven
consistent snapshot procedure with an explicit WAL/checkpoint policy, externally anchored
manifest, `integrity_check`/`foreign_key_check`, and independent restore test. Separately copying
database, WAL, and SHM files is not sufficient by itself. Before version-2-only writes, rollback
switches the atomic routing marker back to the untouched version-1 generation. After
version-2-only writes, rollback requires a proven lossless reverse converter; otherwise the
recorded cutover generation is a point of no return and recovery moves forward on version 2.

## Approval Boundary

This ADR accepts a planning target. It does not approve an incompatible runtime, canonical-truth,
schema, or data migration. The project owner's instruction to continue authorizes this planning
task and the bounded RISK-1 clock task only; department and agent reviews are evidence, not human
approval.

A canonical-truth change requires the project owner and affected contract owners plus
migration/reconciliation evidence. A database, schema, retention, or state migration additionally
requires a dedicated implementation ADR, backup, validation, rollback, and affected control-owner
review under [`POLICIES.md`](../POLICIES.md#explicit-human-approval-matrix).

Any Stage-3 preflight against an operator database first requires explicit approval of the exact
read-only path list, consistent-snapshot method, report destination, and evidence
retention/disposal boundary.

The next task may enforce UTC on injected clock results because the current project policy already
requires internal UTC and the system clock already returns it. That task may not change persisted
model validation, serialized bytes, schema, or stored data. If it reaches any of those boundaries,
it stops and obtains the applicable approval.

## Safety Boundary

This decision changes documentation and governed project state only. It creates no network,
database-write, repair, credential, notification, signal, portfolio, order, execution, or trading
capability. All live, leverage, withdrawal, external-notification, and autonomous-execution flags
remain disabled.

## Consequences

### Positive

- The repository has one evidence-backed map rather than relying on broad `AwareDatetime` or
  selected UTC projections as proof of global conformance.
- JSON truth, indexed projections, SQL order, clocks, provider inputs, and causal cursors have
  distinct, testable responsibilities.
- The target removes mixed-offset lexical ordering and identity ambiguity while retaining raw
  evidence.
- Compatibility, digest versioning, collision handling, quarantine, and rollback are designed
  before strict readers or migrations can strand existing rows.
- `RISK-005` remains truthfully open until implementation and migration evidence is complete.

### Negative

- Version-2 storage needs explicit schema migrations for every current SQLite adapter.
- Fixed canonical JSON changes exact bytes, so downstream snapshots and reconciliation digests
  need versioned compatibility.
- Preflight can discover collisions or corrupt rows that require owner review instead of automatic
  repair.
- Dual readers and shadow verification temporarily increase implementation and test complexity.
- An old binary cannot open a version-2 database merely by changing `user_version`; downgrade
  requires backup restore or a proven reverse export.

## Alternatives Considered

### Keep all aware offsets and compare only in Python

Rejected because canonical bytes, SQLite identity, lexical queries, digests, and external output
remain representation-dependent even when Python comparison is chronological.

### Silently normalize inside every domain model

Rejected because it hides an invalid internal caller, changes identity and digest bytes without an
explicit ingress boundary, and can make legacy corruption look valid.

### Keep UTC text projections as the universal SQL order

Rejected as the long-term target because integer epoch microseconds provide exact range/order
semantics and clearer type validation. Fixed UTC text remains suitable for JSON and audit display.

### Rewrite every database in place in one release

Rejected because current stores have different key, digest, projection, and causal behavior and
no version-1-to-version-2 migration path. A one-shot rewrite cannot safely isolate collisions or
provide a verified rollback.

### Treat existing semantic conflict tables as timestamp quarantine

Rejected because those tables preserve competing market revisions, not malformed or ambiguous
storage records. Migration quarantine needs its own typed evidence and manifest.

## Review Triggers

Review this decision when selecting a different canonical precision or text form, changing epoch
projection units, adding a new timestamp-bearing model or store, changing digest or natural-key
semantics, beginning any schema or data migration, adding an external time-serialized API,
introducing a scheduler that queries time indexes, retiring a legacy reader, or proposing closure
of `RISK-005`.
