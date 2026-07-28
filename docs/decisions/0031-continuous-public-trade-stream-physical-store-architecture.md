# ADR 0031: Continuous Public-Trade Stream Physical Store Architecture

- **Status:** Accepted
- **Date:** 2026-07-28
- **Decision owners:** Project owner, Market Data Department, Engineering Department, Security
  Department, and Audit and Assurance Department

## Task Contract

### Goal

Select one evidence-backed physical single-host architecture for the unused ADR 0030 continuous
public-trade stream-store port and freeze the preimplementation evidence plan without adding a
schema, adapter, database, path, runtime, or authority.

### Context

ADR 0029 defines exact durable state, original canonical bytes, domain-separated digests, rolling
history roots, evidence scopes, crash dispositions, migration constraints, and retention
obligations. TASK-061 implements those pure records and codecs. ADR 0030 and TASK-062 freeze the
unused logical port, atomic ownership, closed outcomes, retry dispositions, constant-size current
views, and audit pages of 1 through 100 new records with at most one predecessor overlap.

The remaining design question is physical: which single-host store can preserve the full accepted
epoch-millisecond range, exact bytes, uniqueness, one-winner compare-and-swap, bounded access, and
deterministic failure classification without becoming a runtime or authority boundary.

The repository already uses SQLite behind other ports, but their schemas, timestamp encodings,
serializers, physical paths, and operational evidence are not inherited. This decision selects a
new dedicated store generation and does not modify or reuse an existing database.

### Scope

This decision freezes:

- a dedicated local SQLite database as the proposed physical technology;
- a non-executable version-one physical descriptor for metadata, current state, immutable history,
  indexes, constraints, and mutation guards;
- exact signed-64-bit integer epoch-millisecond storage with no datetime or precision projection;
- reversible binary UUID and natural-identity keys;
- canonical-BLOB authority and non-authoritative index/projection rules;
- coherent create, current-load, compare-and-swap, duplicate, and bounded-audit transaction plans;
- closed SQLite/store outcome mapping;
- crash and lost-acknowledgement evidence requirements;
- forward migration, rollback, backup/restore, and preserve-all retention rules; and
- finite preimplementation capacity, query-plan, concurrency, corruption, and durability evidence
  gates.

### Constraints

This decision adds no production source or adapter, repository implementation, executable DDL,
database or schema creation, migration execution, path or configuration, filesystem or provider
I/O, benchmark, runtime import or composition, clock, evidence-body or attestation access, fence,
lease, request-budget use, retry or recovery action, child or stream action, credential,
permission, notification, capacity or durability claim, readiness, deployment, or Phase 2 claim.

It does not alter TASK-059 behavior, TASK-061 bytes or digest domains, ADR 0030 values or outcomes,
the existing SQLite stores, ADR 0027, ADR 0028, TASK-037 denial, or Stage 3.
TASK-037 remains blocked and authorization remains denied.

### Done When

- Every ADR 0030 public value and operation has one physical mapping or an explicit
  preimplementation prerequisite.
- Exact TASK-061 bytes remain authoritative and every stored projection is revalidated.
- `0` through `2**63 - 1` epoch milliseconds remain exact SQLite integers.
- Create and compare-and-swap each use one SQLite transaction with exact uniqueness and one-winner
  semantics; no upsert, replacement, repair, or hidden retry exists.
- Current reads remain constant-size and page-range materialization never exceeds 100 new history
  rows plus one overlap.
- Failure, corruption, unsupported-version, identity, anchor, absence, and unavailability
  classifications remain distinct.
- Crash, backup/restore, migration, retention, query-bound, and capacity evidence gates are
  explicit and fail closed.
- No physical capability or readiness claim enters the repository.

### Not Included

- Executable schema text, a schema installer, an adapter, repository code, a fake in production
  source, a physical path, configuration, database creation, or migration.
- A trusted mutation boundary, evidence or attestation service, clock, outer fence, budget,
  scheduler, service, runtime, automatic retry, recovery, repair, deletion, or compaction.
- Operator data, private/account data, credentials, strategies, signals, Risk decisions, orders,
  execution, or any financial action.

## Evidence Basis

### Repository facts

- ADR 0030 requires one atomic current-plus-creation insert and one atomic
  current-replacement-plus-transition append.
- Original TASK-061 canonical bytes, not reconstructed objects or generic JSON, are authority.
- Stream UUID and the six-field natural identity are separately unique.
- Full-range epoch milliseconds and causal versions fit the non-negative signed-64-bit range.
- Current views require the creation entry, current entry, and at most one direct predecessor.
- Audit results expose at most 100 new history entries plus one predecessor overlap and no
  lookahead.
- Exact historical replay is `DUPLICATE`; stale or competing valid state is `CONFLICT`.
- A commit whose acknowledgement is unknown is resolved only by exact reload.
- `PRAGMA user_version` alone is not a storage-format authority.
- No retention deletion, compaction, capacity, backup, durability, or readiness evidence exists.

### Primary SQLite facts

The following official SQLite sources were retrieved on 2026-07-28:

- [Datatypes in SQLite](https://www.sqlite.org/datatype3.html) defines the signed-64-bit `INTEGER`
  storage class and byte-preserving `BLOB`.
- [STRICT tables](https://www.sqlite.org/stricttables.html) require SQLite 3.37.0 or later, restrict
  declared types, preserve normal constraints, and extend integrity checks to stored column
  types. STRICT still performs lossless affinity conversion, so adapter-side exact Python-type
  validation remains mandatory.
- [CREATE TABLE](https://www.sqlite.org/lang_createtable.html) defines `NOT NULL`, `CHECK`,
  `UNIQUE`, and primary-key behavior. Required unique or checked values must also be explicitly
  non-null.
- [Transactions](https://www.sqlite.org/lang_transaction.html) and
  [isolation](https://www.sqlite.org/isolation.html) establish one SQLite writer at a time,
  `BEGIN IMMEDIATE` acquisition behavior, committed-view isolation, and WAL snapshot reads.
- [Write-ahead logging](https://www.sqlite.org/wal.html) permits same-host concurrent readers with
  one writer, requires the WAL to remain with the database, does not support network filesystems,
  and makes checkpoint policy and long-reader starvation explicit.
- [PRAGMA synchronous](https://www.sqlite.org/pragma.html#pragma_synchronous) describes the extra
  WAL sync performed by `FULL`. This is an engine request to the VFS, not proof that a filesystem
  or device truthfully persists writes.
- [The WAL-reset bug](https://www.sqlite.org/wal.html#the_wal_reset_bug) affects unpatched SQLite
  3.7.0 through 3.51.2 under a concurrent writer/checkpointer seam. A future implementation must
  bind a patched exact SQLite source ID, not merely compare an imprecise version string.
- [How to corrupt SQLite](https://www.sqlite.org/howtocorrupt.html) explains why raw copying,
  moving, renaming, or separating a live database from its journal/WAL is unsafe and why locking
  and sync behavior remain environmental assumptions.
- [Online backup](https://www.sqlite.org/backup.html) provides a consistent live snapshot.
  `integrity_check` and `foreign_key_check` remain separate verification steps under the
  [PRAGMA reference](https://www.sqlite.org/pragma.html).
- [SQLite limits](https://www.sqlite.org/limits.html) defines compile-time and connection limits,
  page-count limits, and theoretical file sizes. Those values are not an operational capacity
  claim.
- [WITHOUT ROWID](https://www.sqlite.org/withoutrowid.html) is primarily beneficial for compact
  non-integer or composite-key rows and can be less suitable when rows contain large BLOBs.

These sources support a candidate architecture. They do not prove the target host filesystem,
storage controller, Python binding, workload, capacity, backup destination, or failure behavior.

## Decision

### Technology and isolation boundary

Select one dedicated SQLite database per physical stream-store generation, accessed through
Python's standard-library `sqlite3` binding behind the existing unused
`ContinuousPublicTradeStreamStore` port.

The database is:

- a new stream-only generation and never an added table in an existing child, market, evidence,
  lifecycle, health, budget, or candle database;
- local to one host on a filesystem whose locking and sync semantics have separately passed the
  evidence gates;
- opened through SQLite only, under one canonical path identity, with no network filesystem,
  multiple hard-link names, rename/unlink while open, raw copying, shared cache, or attached
  database;
- single-writer with concurrent bounded snapshot readers; and
- not a cross-database transaction, outer fence, trusted mutation boundary, evidence authority,
  or multi-host coordination mechanism.

SQLite is selected because it can atomically change current state and immutable history in one
local transaction, enforce unique keys, preserve signed-64-bit integers and exact BLOBs, provide
indexed bounded reads, and reuse an already approved runtime capability without a new service or
dependency. This selection grants no adapter or database creation authority.

### Physical generation and connection profile

A future bootstrap creates a new empty physical generation through a separately reviewed
administrative boundary. The port adapter never creates a missing file implicitly. Normal writer
opens require an existing read-write generation; normal reader opens require an existing
read-only or read-write generation. A missing, inaccessible, or unopenable file is
`UNAVAILABLE`, never an empty store or `NOT_FOUND`.

Version-one bootstrap and every connection must verify:

| Setting or identity | Version-one decision |
|---|---|
| SQLite engine | Exact source ID is recorded and must contain the WAL-reset fix; implementation rechecks the current supported release. |
| database encoding | UTF-8, verified before any application row exists. |
| page size | 4,096 bytes, fixed before WAL mode and included in the format marker. |
| journal mode | WAL, with the returned mode required to equal `wal`. |
| synchronous | `FULL`, verified on every writer connection. |
| foreign keys | `ON` on every connection. |
| read isolation | shared cache disabled and `read_uncommitted=OFF`. |
| trusted schema | `OFF` on every connection. |
| busy behavior | zero busy timeout and no busy-handler retry; contention becomes `UNAVAILABLE`. |
| auto vacuum | `NONE`; no logical retention or deletion authority exists. |
| reader behavior | explicit finite read transaction, `query_only=ON`, and all cursors closed before return. |
| writer behavior | explicit `BEGIN IMMEDIATE`; no implicit transaction or transaction upgrade. |
| application identity | exact non-colliding `application_id` remains a named bootstrap prerequisite. |
| physical version | `user_version=1` plus the dedicated metadata row and exact schema fingerprint. |
| checkpoint policy | exact threshold, owner, maximum reader duration, and WAL ceiling remain capacity prerequisites. |
| file ceiling | exact `max_page_count`, host quota, free-space reserve, and alert thresholds remain capacity prerequisites. |

`user_version`, `application_id`, table metadata, and the schema fingerprint must agree. An unknown
recognized generation is `UNSUPPORTED_VERSION`. A database claiming version one whose marker,
objects, types, constraints, indexes, triggers, or fingerprint disagree is `CORRUPT`.

The future implementation records the SQLite source ID, compile options, thread mode, page size,
connection limits, defensive-mode availability, and every verified PRAGMA in its evidence packet.
No connection proceeds when a required setting cannot be established.

### Exact epoch and scalar representation

Every TASK-059 epoch-millisecond coordinate remains exact across the frozen accepted range. The
version-one physical descriptor selects only `stream_start_epoch_ms` as an epoch-valued SQL scalar
projection. That projection and every causal version projection use `INTEGER NOT NULL` with an
explicit range constraint:

- epoch milliseconds: `0` through `9223372036854775807`;
- causal versions: `1` through `9223372036854775807`; and
- optional prior versions: SQL `NULL` only where the version-one creation record requires null.

Current `cursor_epoch_ms` and optional attachment `window_start_epoch_ms` and
`window_end_epoch_ms` are deliberately not SQL scalar projections in version one. They remain
exact inside the authoritative original TASK-061 record and envelope BLOBs and are decoded,
range-checked, and cross-checked whenever those BLOBs are validated. TASK-064 must not invent
cursor or attachment-window columns without a successor architecture decision.

No epoch value is converted to `datetime`, RFC 3339 text, floating point, seconds, epoch
microseconds, unsigned arithmetic, or a generated time column, whether it is projected or embedded.
For each SQL integer projection, the adapter accepts and binds only an exact built-in Python `int`,
rejects `bool` and subclasses before SQLite, and requires the read-back SQLite storage class and
integer value to agree exactly.

Fixed-UTC `recorded_at` values remain inside original TASK-061 record BLOBs. Causal ordering is the
integer successor version, never textual or wall-clock ordering.

UUID keys use the exact 16 bytes of `UUID.bytes` in network byte order. Digests and roots use their
exact 71 visible-ASCII bytes as BLOBs. They are decoded and validated through TASK-061 before use;
SQL collation or case-folding never participates.

### Reversible natural-identity key

SQLite TEXT is not selected as the uniqueness authority because affinity, collation, embedded NUL,
and Python strings containing surrogate code points must not narrow or normalize the accepted
logical contract.

The physical natural-identity key is a reversible BLOB:

1. begin with the fixed bytes
   `b"wealth.continuous_public_trade.natural_identity_key/v1\x00"`;
2. use the fixed field order `source`, `venue`, `instrument`, `provider_symbol`,
   `instrument_type.value`, `request_variant`;
3. encode each exact Python string with UTF-8 and the `surrogatepass` error handler; and
4. prefix each resulting byte string with its unsigned four-byte big-endian length.

The fixed field count, order, and length framing make the projection injective. It is not a hash,
digest, canonical record, authority artifact, or replacement for TASK-061 bytes. On every
operation, the key derived from the command/query must match the stored key. Whenever the creation
entry is loaded, decoding its authoritative bytes and recomputing the key must reproduce the same
BLOB. Any disagreement is `CORRUPT`, not an identity mismatch that could authorize another stream.

Changing this key algorithm requires a new physical generation and migration; it never rewrites a
version-one key in place.

### Non-executable physical descriptor

This ADR intentionally contains no executable DDL. A future schema task must translate the
descriptor into exact reviewed SQL and freeze the full object fingerprint before creating even a
test database.

#### Singleton format metadata

One small STRICT singleton table records:

- singleton key `1`;
- exact storage marker bytes
  `wealth.continuous_public_trade.stream_store/sqlite/v1`;
- physical format version `1`;
- schema generation `1`;
- natural-identity key version `1`;
- page size `4096`; and
- the exact domain-separated schema-object fingerprint.

The schema fingerprint is lowercase `sha256:` over the fixed domain
`b"wealth.continuous_public_trade.stream_store_schema/v1\x00"` followed by canonical JSON for the
ordered expected table, column, declared-type, nullability, primary-key, foreign-key, index,
trigger, and normalized SQL object descriptor. The descriptor profile is separately frozen by the
future schema task. The fingerprint describes schema objects only and never enters a TASK-061
record or history root.

The metadata row cannot be updated or deleted by the normal adapter. A new format uses a separate
physical generation.

#### Stream identity and current state

One ordinary STRICT rowid table has an internal `INTEGER PRIMARY KEY` used only for compact local
foreign keys. It contains:

- exact 16-byte stream UUID key, unique and non-null;
- exact reversible natural-identity key, unique and non-null;
- stream-contract version;
- immutable creation successor version, exactly `1`;
- an immutable exact canonical version-one creation-record witness BLOB, its recomputed record
  digest, and its recomputed initial history root;
- exact policy fingerprint bytes and every immutable effective stream-policy field as
  non-authoritative physical projections;
- exact stream-start epoch milliseconds;
- current causal version;
- an exact canonical current-record witness BLOB and its recomputed record digest;
- original current canonical envelope BLOB;
- current envelope-digest BLOB; and
- current rolling-history-root BLOB.

The complete effective policy projection contains exactly the TASK-061 version-one fields:
`schema_version`, `window_size_ms`, `settlement_lag_ms`, `max_catchup_span_ms`,
`max_jobs_per_invocation`, `max_requests_per_job`, `max_records_per_job`, and
`policy_fingerprint`. Strings use reversible BLOB atoms; integers use exact signed-64-bit
INTEGERs.

The stream row has no scalar column for the current cursor or optional attachment-window
coordinates. Those values are reconstructed only by decoding the exact current-record and current
envelope BLOBs, which must agree byte-for-byte with the immutable current history tail and satisfy
the frozen TASK-059/TASK-061 validators.

Identity, policy, start, creation-witness, and internal-key fields are immutable. A schema mutation
guard permits only the current version, current-record bytes/digest, current-envelope bytes/digest,
and root to change and requires the version to increase by exactly one. No normal delete exists.

The creation witness is an immutable bounded-read copy of the authoritative version-one history
record, not new authority. A deferred composite foreign key binds its stream key, fixed version
one, and digest to the immutable creation row. A schema insertion guard also requires its BLOB,
digest, and initial root to equal that row byte for byte before the create transaction can commit.

A second deferred composite foreign key binds the stream row's current version, current-record
digest, current-envelope digest, and current history root to the corresponding immutable history
tail. Exact schema guards require the current-record and current-envelope BLOBs to equal the tail
row's original record and successor-envelope BLOBs on initial creation and on every current update.
The history row and stream row therefore cannot commit a split tail through the reviewed schema.
These circular bindings are exact future-schema requirements whose insertion order and
deferred-constraint behavior must be proven by TASK-064 before any adapter.

The remaining stream columns are lookup and compare-and-swap projections. They never replace the
creation or current history records. Create, current load, compare-and-swap, and audit decode the
exact creation witness, current-record witness, and current envelope. Those bytes must reproduce
every identity, complete-policy, applicable-child-policy, version, digest, root, and projection
before a result or write. Whenever version one or the current tail is materialized as a
page/history row, its original BLOB must also equal the corresponding stream-row bytes. Audit
output remains store-local structural evidence and never an accepted ADR 0029 history attestation.

#### Immutable history

One ordinary STRICT rowid table has an internal `INTEGER PRIMARY KEY` and a non-cascading foreign
key to the stream row. Each row contains:

- stream foreign key;
- successor version;
- entry kind, exactly creation or transition;
- record-model and serialization versions;
- original canonical creation/transition record BLOB;
- record-digest BLOB;
- original canonical successor-envelope BLOB;
- successor-envelope-digest BLOB;
- optional prior version, prior envelope digest, and prior history root with exact
  creation-versus-transition nullability;
- for transitions only, an exact canonical direct-predecessor-record witness BLOB and its
  recomputed record digest, with exact creation-versus-transition nullability;
- successor rolling-history-root BLOB; and
- no copied external evidence body, accepted attestation, clock value, operator data, or
  permission.

History rows likewise have no scalar cursor or attachment-window columns. Each retained
successor's coordinates are reconstructed only from its exact canonical record and envelope BLOBs
and must pass the same full-range validation before contributing to any result.

The pair `(stream foreign key, successor version)` is unique and indexed in that order. This is the
only range-access path required by audit. A second composite unique key over
`(stream foreign key, successor version, record digest)` is the parent of a self-referential,
non-cascading foreign key from each transition's
`(stream foreign key, prior version, predecessor-record-witness digest)`. The adapter recomputes
the witness digest from its BLOB before use. An immutable history-insert guard additionally
requires the witness BLOB, entry kind, version, and digest to equal the retained predecessor row
byte for byte before insertion. The constraint and guard prove that a committed witness names and
copies the retained predecessor; the original predecessor row remains authoritative. The exact
composite unique key
`(stream foreign key, successor version, record digest, successor-envelope digest, history root)`
is the parent of the deferred stream-current binding. The shorter existing record-digest key is the
parent of the creation and predecessor bindings; byte/root guards close the redundant BLOB
projections. These keys support constraints without a digest-only query path. There is no
digest-only lookup, total-count index, time index, or unbounded iterator.

The predecessor witness is deliberate bounded-read redundancy. A continuation overlap at an
arbitrary later version, including the current tail, cannot satisfy TASK-061 full-link validation
from its own transition record alone: that validation also requires the predecessor's envelope,
history root, and recorded time. The exact predecessor record contains enough canonical material
to reconstruct and revalidate those values without fetching another logical history entry. The
witness is not returned as another page record, does not widen the ADR 0030 protocol, and is never
new authority. When the predecessor is already among the page's materialized rows, the adapter
also requires the witness to equal that row's canonical record bytes.

Normal schema paths cannot later make either side stale: original rows and witnesses are immutable,
updates/deletes are rejected, and schema identity is verified before use. Arbitrary out-of-band
page-file tampering is not universally detectable by one bounded port call; any successful result
is still only bounded store-local structural evidence, and full paginated verification remains a
separate attestation prerequisite.

Normal history rows cannot be updated or deleted. Exact schema triggers reject either operation.
The adapter exposes insert only inside create or compare-and-swap transactions. Migration copies
into a separate generation and never disables these guards on a live generation.

Original record and envelope BLOBs are authoritative. Record digests, envelope digests, versions,
prior bindings, roots, entry kind, and foreign keys are redundant corruption-detection and access
projections. TASK-061 decoders and validators must reproduce every one before a row can contribute
to any non-error result.

Large canonical BLOB rows use ordinary rowid tables. `WITHOUT ROWID` is rejected for current and
history because the official guidance favors it for compact composite-key rows, while these rows
can contain 16 KiB envelope and 64 KiB record values. The small singleton metadata table may use
`WITHOUT ROWID`; that choice is included in the future exact schema fingerprint.

### Logical-to-physical mapping

| ADR 0030 value | Physical representation and reconstruction rule |
|---|---|
| `ContinuousPublicTradeStreamIdentityV1` | UUID BLOB, reversible natural key, stream-contract/start/policy projections in the stream row; exact fields are revalidated from the constraint-bound creation-record and current-record witnesses. |
| `ContinuousPublicTradeStreamExpectationV1` | Invocation-only; never persisted. It is revalidated before opening a transaction and compared with stream projections plus authoritative decoded entries where required. |
| `ContinuousPublicTradeStreamStoredEnvelopeV1` | Original envelope BLOB plus exact digest projection in current and history; decoded value, including cursor and optional attachment-window epochs that have no SQL scalar projections, is reconstructed only through TASK-061 and must equal the envelope embedded in the current-record witness. |
| `ContinuousPublicTradeStreamStoredCreationV1` | Immutable history version one: original creation-record BLOB/digest, original successor-envelope BLOB/digest, initial root; governed-create scope is deterministically rederived and checked, and the stream creation witness must match byte for byte. |
| `ContinuousPublicTradeStreamStoredTransitionV1` | Immutable history version greater than one: original transition BLOB/digest, original successor-envelope BLOB/digest, prior projections, exact constraint-bound predecessor-record witness BLOB/digest, next root; transition/completion scopes are deterministically rederived and checked. |
| create/CAS commands | Invocation-only finalized values. The adapter revalidates them before storage and persists only their exact authoritative entry plus required projections. |
| load/audit queries | Invocation-only. No query, limit, continuation, or expectation becomes durable state. |
| create/CAS receipts | Reconstructed from one fully validated immutable accepted history row after the transaction classification. |
| current view | Reconstructed from one stream row and the distinct creation, current, and direct-predecessor history rows; at most three history rows. |
| audit continuation | Reconstructed from the final validated new row; never separately stored. |
| audit page | Reconstructed from an indexed bounded history range and validated by the public ADR 0030 page validator before return. |
| outcome and retry disposition | Derived from one coherent transaction plus typed SQLite/store classification; never stored as authority or retry state. |
| typed evidence scopes | Recomputed from exact record bytes and the complete effective policy; no separately mutable scope copy is authoritative. |
| external evidence/attestation | Not stored or accessed. References remain only inside original TASK-061 bytes. |

No generic serializer, SQL JSON function, model dump, text collation, reconstructed envelope, or
new physical digest may replace an original TASK-061 BLOB.

## Operation Design

### Common boundary and coherent reads

The adapter first recursively revalidates the exact ADR 0030 command/query before opening or
touching SQLite. It then opens an existing generation and establishes the required connection-local
settings that SQLite requires outside a transaction. It starts the operation's explicit
transaction, acquires its read snapshot or write lock, and only then reads and revalidates the
database `application_id`, `user_version`, metadata row, page/format identity, schema objects, and
schema fingerprint inside that same coherent operation transaction before classifying any row.
Any unavoidable pretransaction bootstrap check is repeated inside the transaction and cannot
authorize a result by itself.

Every result is classified from one coherent transaction. Readers use one finite WAL snapshot.
Writers obtain `BEGIN IMMEDIATE` before their first identity or history read. There is no
read-then-upgrade transaction, busy handler, hidden retry, savepoint loop, automatic reload after a
conflict, or SQL statement assembled from caller text.

UUID and natural-key probes can locate zero rows, the same row, or two distinct rows. Before
classifying any existing row as a coherent conflict, the adapter fully decodes and validates that
row's constraint-bound creation witness, current-record witness, current envelope, immutable
projections, and current-tail bindings. When an operation needs a public current view, it also loads
and validates the exact history rows required by ADR 0030. A recognized unsupported value is
`UNSUPPORTED_VERSION`, and any malformed located material is `CORRUPT`; only completely valid
disagreement can become `CONFLICT`, `IDENTITY_CONFLICT`, or `ANCHOR_CONFLICT`.

SQLite errors are sanitized and never include paths, SQL, record bytes, identity strings, or
evidence references.

### Create

Inside one `BEGIN IMMEDIATE` transaction:

1. read by exact UUID key and exact natural-identity key;
2. if either key exists, validate every located stream row and load the bounded
   stream/creation/current/predecessor material needed to distinguish exact historical creation
   replay, valid disagreement, unsupported version, and corruption;
3. return `DUPLICATE` only when the retained version-one creation entry, original bytes, digest,
   root, identity, full policy, and scope exactly match the command;
4. return `CONFLICT` for a coherent same-UUID disagreement or natural-identity/different-UUID
   row;
5. when both keys are absent, insert exactly one stream row carrying matching creation and current
   record witnesses and one immutable version-one history row in the same transaction, satisfying
   their deferred byte-for-byte bindings;
6. preconstruct and revalidate the accepted receipt, commit once, and return `INSERTED` only after
   commit success.

No `REPLACE`, `INSERT OR IGNORE`, merge, repair, normalization, or upsert is permitted. Unique
constraints are defense in depth. Because the write lock precedes the coherent identity reads, a
uniqueness failure after both were absent indicates an invariant or schema problem and cannot be
silently reclassified as success.

An error or lost connection during commit returns `UNAVAILABLE`; it never returns `INSERTED`.
Later presentation of the exact unchanged command determines old state versus historical
`DUPLICATE`.

### Current load

Inside one explicit read transaction:

1. read the stream row by UUID and natural key in the same snapshot;
2. return `NOT_FOUND` only when both are absent;
3. fully validate each located stream row's creation/current witnesses and immutable projections;
4. load the version-one creation row, the current history row, and exactly one direct predecessor
   for the selected stream when current version is greater than one, deduplicating overlapping
   versions;
5. decode original bytes and validate every identity, full-policy, child-policy, digest, scope,
   link, time-ordering, current-tail, and root projection required by ADR 0029/0030;
6. return `IDENTITY_CONFLICT` only after every located value needed for the classification is
   coherent and one key resolves differently or an immutable expectation disagrees; and
7. construct `FOUND` only after the bounded current view revalidates.

A successful selected-stream view uses one stream row and at most three distinct history rows,
independent of total history length. A two-row identity-conflict path remains constant-size and
returns no current view or audit page.

### Compare-and-swap

Inside one `BEGIN IMMEDIATE` transaction:

1. resolve exact UUID/natural identity and validate the bounded current view;
2. inspect and fully validate the immutable row at the command successor version and its direct
   predecessor when necessary;
3. classify a candidate row ahead of the bound current tail as `CORRUPT`; a current-at-prior row
   with an already present successor is a split history/tail state, never a duplicate;
4. when `current version >= command successor version`, classify a missing candidate row or missing
   required candidate predecessor as a retained history gap and therefore `CORRUPT`;
5. return historical `DUPLICATE`, even when a later current version exists, only when
   `current version >= command successor version`, the candidate belongs to the coherent retained
   chain, and its original transition bytes, digests, scopes, predecessor bindings, successor, and
   root exactly match;
6. return `CONFLICT` for a coherent competing historical row, stale current version/digest/root,
   identity/policy mismatch, missing stream, or different current winner;
7. for a matching current prior, validate the command's sole finalized transition against the
   authoritative current and predecessor material;
8. insert one immutable history row, including the exact constraint-bound predecessor witness;
9. conditionally update only the current version, record witness/digest, envelope/digest, and root
   using exact stream key, expected version, expected digest, and expected root, requiring exactly
   one changed row; and
10. preconstruct the receipt, commit once, and return `UPDATED` only after commit success.

The insert and update roll back together. Zero changed rows is `CONFLICT`; more than one is
`CORRUPT`. There is no automatic retry, alternate successor, resampled time, regenerated UUID,
or reload-and-continue path.

Historical duplicate classification remains constant-size: creation, current, current
predecessor, candidate transition, and candidate predecessor produce at most five distinct
history rows.

### Bounded audit

Audit uses one explicit finite read snapshot and the unique ordered
`(stream foreign key, successor version)` access path.

For a start query:

- decode the constraint-bound creation/current-record witnesses and current envelope, then validate
  exact stream identity, complete policy, current tail/root, and the query's applicable child-policy
  fingerprint before relying on any projection;
- derive `new_count = min(limit, current_version)` from the validated current tail;
- request versions one through `new_count`, ordered by successor version;
- materialize exactly `new_count` history rows; any missing or extra row is `CORRUPT`;
- require the returned version-one creation BLOB to equal the stream creation witness;
- require version one creation followed only by contiguous transitions; and
- run the public ADR 0030 query/page validator before `PAGE`.

For a continuation query:

- resolve the exact stream, decode the constraint-bound creation/current-record witnesses and
  current envelope, and validate identity, complete policy, current tail/root, and the query's
  applicable child-policy fingerprint before relying on any projection;
- require the expectation's child-policy fingerprint to be exact when the decoded current envelope
  is attached and `None` otherwise; a coherent mismatch is `IDENTITY_CONFLICT`;
- reject a continuation version above the validated current version as `ANCHOR_CONFLICT`;
- derive `remaining = current_version - continuation_version`,
  `new_count = min(limit, remaining)`, and
  `high_version = continuation_version + new_count`; this never exceeds the signed-64-bit current
  version and never evaluates the potentially overflowing expression `continuation version +
  limit`;
- request the inclusive interval from continuation version through `high_version`;
- materialize exactly one overlap plus exactly `new_count` new rows, for an absolute maximum of
  101; a missing or extra required retained row is `CORRUPT`;
- fully decode and validate the located overlap and, when it is a transition, recompute its
  constraint-bound predecessor-record witness, reconstruct the predecessor entry, and run the
  complete TASK-061 link validation before comparing the supplied continuation;
- return `ANCHOR_CONFLICT` only when that fully valid overlap's digest/root disagrees with the
  supplied continuation;
- require each in-page transition's witness to equal its already materialized predecessor record;
- return validated `AT_TAIL` only after the exact current-tail overlap and, when applicable, its
  predecessor witness pass those checks, its record/envelope bytes equal the stream current
  witnesses, and `new_count` is zero; and
- otherwise run the public query/page validator before `PAGE`.

The SQL shape has an exact bounded version range and exact expected row count. It does not use
`LIMIT n+1`, `COUNT`, maximum-version discovery, unbounded iteration, total count, lookahead,
offset pagination, or a second history-row query. The creation/current/predecessor witnesses in the
stream or transition row are constraint-bound copies, not separately materialized logical history
entries. Continuation audit output remains structural store-local evidence; a complete or
incremental ADR 0029 attestation is separate.

A future test-only prototype must prove through query-plan inspection, statement/row
instrumentation, hostile rows immediately beyond the boundary, and exact materialization counts
that no history row outside the defined range is fetched or decoded. Failure keeps the adapter
blocked.

## Closed Outcome Mapping

Classification precedence is structural caller rejection before storage, then storage
availability/format, retained corruption, exact duplicate, coherent conflict/absence, and success.
No lower-precedence result hides a higher-precedence inability to establish a coherent view.

| Condition | Port classification |
|---|---|
| invalid or bypass-constructed command/query | existing ADR 0030 contract exception before SQLite |
| recognized but unsupported physical, record, model, or serialization version | `UNSUPPORTED_VERSION` |
| expected v1 marker with schema/type/object/fingerprint disagreement | `CORRUPT` |
| SQLite `CORRUPT`, `NOTADB`, malformed retained bytes/types/projections, broken links/roots, missing required row, tail disagreement, or retained `foreign_key_check` violation | `CORRUPT` |
| statement-time `CHECK`, `UNIQUE`, `NOT NULL`, trigger, or foreign-key constraint after a coherent validated snapshot | `UNAVAILABLE`, unless the same transaction proves a retained v1 contradiction, which is `CORRUPT` |
| `BUSY`, `LOCKED`, `IOERR`, `FULL`, `CANTOPEN`, `READONLY`, `INTERRUPT`, resource exhaustion, configuration establishment failure, or unknown commit outcome | `UNAVAILABLE` |
| coherent UUID/natural identity disagreement | operation-specific `CONFLICT` or `IDENTITY_CONFLICT` |
| coherent missing UUID and natural identity in load/audit | `NOT_FOUND` |
| coherent continuation mismatch | `ANCHOR_CONFLICT` |
| exact validated historical request | `DUPLICATE` |
| exact committed create/CAS | `INSERTED` or `UPDATED` |
| exact coherent current/audit value | `FOUND`, `PAGE`, or `AT_TAIL` |

`SQLITE_FULL` and a read-only or permission failure are never logical conflicts. An unsupported
future format is never absence. A row with a recognizable supported version but invalid content
is `CORRUPT`. Constraint names or generic primary codes are not enough to infer retained
corruption. TASK-064 must freeze the extended SQLite result-code matrix for every statement and
constraint seam. If an SQLite code does not have a reviewed deterministic mapping, the result is
`UNAVAILABLE`.

The exact ADR 0030 retry dispositions remain:

| Operation | `NOT_REQUIRED` | `DO_NOT_RETRY` | `EXACT_REQUEST_ONLY` |
|---|---|---|---|
| create | `INSERTED`, `DUPLICATE` | `CONFLICT`, `UNSUPPORTED_VERSION`, `CORRUPT` | `UNAVAILABLE` |
| current load | `FOUND`, `NOT_FOUND` | `IDENTITY_CONFLICT`, `UNSUPPORTED_VERSION`, `CORRUPT` | `UNAVAILABLE` |
| compare-and-swap | `UPDATED`, `DUPLICATE` | `CONFLICT`, `UNSUPPORTED_VERSION`, `CORRUPT` | `UNAVAILABLE` |
| audit | `PAGE`, `AT_TAIL`, `NOT_FOUND` | `IDENTITY_CONFLICT`, `ANCHOR_CONFLICT`, `UNSUPPORTED_VERSION`, `CORRUPT` | `UNAVAILABLE` |

`NOT_REQUIRED` describes a terminal store-local classification and is not permission for another
action. `DO_NOT_RETRY` is fail-closed. Only `UNAVAILABLE` carries `EXACT_REQUEST_ONLY`, which
describes only the unchanged request shape and grants no retry authority, delay, loop, recovery,
or mutation.

## Crash and Lost-Acknowledgement Plan

Before a production adapter exists, a separately governed test-only prototype must exercise each
seam in a fresh subprocess with generated TASK-061 values and a newly opened connection:

| Injected seam | Required durable result after reopen |
|---|---|
| before transaction or before first write | exact old state |
| after stream/current insert but before creation-history insert | exact old state |
| after creation-history insert but before create commit | exact old state |
| after transition-history insert but before current update | exact old state |
| after current update but before CAS commit | exact old state |
| during commit with an injected connection/process failure | exact old or exact new state; never partial |
| after commit before accepted result reaches the caller | exact new state; the unchanged request is historical `DUPLICATE` |
| two same-natural-identity creates | exactly one inserted stream; the other is exact duplicate or conflict according to bytes/UUID |
| two CAS commands for one prior | exactly one updated successor; the other is duplicate only if identical, otherwise conflict |
| missing, altered, misbound, or non-canonical creation/current-record/current-tail/predecessor witness or binding | no page, tail, load, duplicate, or success claim; recognized v1 state is `CORRUPT` |
| writer/checkpointer concurrency | no unpatched SQLite source ID; no corrupt or skipped committed state |
| disk full, readonly, busy, lock, and injected I/O failure | no success claim; exact unchanged/reloaded classification or `UNAVAILABLE` |

Each successful reopen runs format/schema verification, focused integrity/FK checks, bounded
current validation, and full paginated history/root validation. Tests must assert no orphan
history row, missing version, split current/tail, duplicate natural identity, alternate successor,
new clock sample, automatic retry, or widened request.

Process-kill evidence is not a power-loss durability claim. Before any operational use, a separate
target-filesystem/VFS test must verify locking, sync, power interruption, storage-cache behavior,
and WAL recovery on the exact host/storage class. If that evidence is unavailable or fails, the
continuous store remains undeployed.

## Backup and Restore

A future backup owner uses SQLite's Online Backup API from a normal SQLite source connection to a
fresh destination generation. Raw copying, copying only the main file, moving an open database,
or separating `-wal`/`-shm` files is prohibited.

After backup completion, the owner finalizes the destination through SQLite under a tested
exclusive-destination policy: finish the selected checkpoint, close every destination connection,
verify whether any WAL state remains required, reopen through SQLite, and rerun all checks. The
manifest hashes either one proven standalone closed main database after a successful final
checkpoint or the exact complete closed file set that SQLite requires. A main-file digest alone is
invalid whenever a WAL may still contain required state.

A backup becomes eligible only after the closed destination independently passes:

1. exact application ID, `user_version`, metadata marker, page size, schema objects, constraints,
   triggers, indexes, and schema fingerprint;
2. `integrity_check` with exact `ok` result;
3. an empty `foreign_key_check`;
4. exact stream and history counts under declared finite limits;
5. every UUID and natural-identity uniqueness invariant;
6. complete paginated decoding of original bytes, digests, roots, scopes, links, creation/current
   and predecessor witnesses, and current-tail agreement for every stream;
7. a separately retained manifest binding source generation, destination generation, SQLite
   source ID, page size/count, schema fingerprint, per-stream final version/digest/root, exact
   standalone-file or complete-file-set identities and digests after close, checkpoint/finalization
   outcome, and fixed-UTC evidence time; and
8. an independent restore drill into another isolated generation with the same checks.

Backup API completion alone is not restore proof. A partial, failed, unverified, or
manifest-mismatched destination is unusable and grants no cutover or deletion authority. The
physical backup destination, encryption/access policy, retention, disposal, RPO, and RTO remain
separately governed deployment prerequisites.

## Migration and Rollback

No in-place schema migration or automatic open-time upgrade is allowed.

A future incompatible change:

1. fences writers through a separately approved boundary;
2. takes a verified Online Backup snapshot and externally anchored manifest;
3. creates a separate destination generation with an exact new application/format/schema
   identity;
4. reads old records through the frozen old reader and copies original canonical BLOBs without
   normalization;
5. recomputes and compares every decoded value, digest, root, identity, and current-tail binding;
6. fully paginates both generations under finite bounds;
7. independently restores the candidate backup;
8. shadow-reads old and new generations at one recorded watermark;
9. verifies exact counts, identities, final versions, envelope digests, and roots; and
10. changes an external atomic routing marker only after separate approval.

Before any new-generation-only write, rollback changes routing to the untouched old generation.
After incompatible new writes, rollback requires a proven lossless reverse converter; otherwise
recovery is forward-only and the continuous path remains disabled.

An old binary never opens a new generation by changing `user_version`. A failed migration never
repairs, deletes, rewinds, advances, or normalizes the source.

## Retention and Compaction

Version one is preserve-all:

- retain the current row, immutable creation row, complete contiguous transition history, original
  canonical record/envelope BLOBs, digests, roots, and projections;
- retain every linked external evidence body, accepted attestation, child completion chain,
  lifecycle, market, health, conflict, fence, and budget record under its separately owned
  retention contract;
- reject normal update or delete of history and reject stream deletion;
- perform no logical compaction, downsampling, root-only substitution, payload reconstruction,
  cursor derivation, or retention expiry; and
- do not treat WAL checkpointing, backup, `VACUUM`, or file-level compression as semantic
  permission to remove history.

Any future deletion or compaction rule requires a separate capacity-backed ADR proving that every
identity, attachment reconstruction, child completion, hold/resume authority, evidence scope,
attestation, rolling root, rollback, and audit obligation remains independently verifiable.

## Finite Capacity and Performance Evidence

SQLite's theoretical limits and a 4,096-byte page size are not capacity evidence. Before a
production adapter or runtime is proposed, owners must approve exact finite values for:

- number of streams and natural identities;
- maximum retained transitions per stream and total transitions;
- transition arrival rate and read/write concurrency;
- database, WAL, backup, restore, and side-by-side migration byte budgets;
- host quota, `max_page_count`, minimum free-space reserve, and alert/stop thresholds;
- WAL auto-checkpoint threshold, maximum WAL bytes, checkpoint owner, and maximum read-transaction
  duration;
- maximum create, CAS, current-load, 100-record audit, checkpoint, backup, restore, and full-audit
  latency;
- backup/restore frequency, RPO, RTO, retention, and independent-restore cadence; and
- exact Python/SQLite runtime, compile options, connection limits, filesystem, VFS, and storage
  class.

A separately approved test-only evidence harness uses only generated non-operator records and
measures:

- minimal, typical, and maximum contract-size records;
- maximum physical rows including creation, current, and predecessor witness copies and every
  projection;
- one writer with the approved reader matrix;
- exact create/CAS contention and historical duplicate paths;
- every audit limit, boundary, and query plan;
- long-reader checkpoint starvation and WAL growth;
- disk full, `max_page_count`, busy/locked, readonly, corrupt, and I/O failures;
- repeated process kills at every transaction seam;
- backup while writes occur, independent restore, full audit, and migration cutover; and
- database/WAL bytes, page/freelist counts, latency percentiles, memory, and open-cursor bounds.

The evidence packet records the exact commit, task contract, seed, generated workload, SQLite
source ID, compile options, PRAGMAs, filesystem, hardware/storage class, run count, thresholds,
raw results, failures, and conclusion. Thresholds are frozen before measurement. A failed,
missing, stale, target-mismatched, or capacity-exceeding result blocks the production adapter and
all continuous-operation claims.

## Preimplementation Gate Matrix

| Gate | Owner | Evidence required | Failure disposition |
|---|---|---|---|
| exact schema and fingerprint | Engineering + independent assurance | reviewed non-ambiguous DDL, schema descriptor/golden fingerprint, object/type/index/trigger tests | no schema or adapter |
| SQLite/runtime security | Security + Engineering | patched exact source ID, compile options, connection limits, trusted-schema/defensive settings | no open or bootstrap |
| path/filesystem identity | Security + Operations | one canonical local path policy, permissions, no network FS/link aliases, locking/sync evidence | no deployment |
| atomicity and lost acknowledgement | Engineering + QA | two-writer and every process-kill seam with old/new/duplicate outcomes | no adapter acceptance |
| bounded reads | Engineering + QA | query plan plus exact row-materialization proof at limits 1 and 100 | no audit implementation |
| corruption mapping | Engineering + Security | marker/schema/row/link/root/FK/SQLite-code matrix, sanitized output | fail closed; no success result |
| backup and independent restore | Operations + Audit/Assurance | verified Online Backup, manifest, full audit, isolated restore | backup unusable; no cutover |
| migration and rollback | Engineering + Operations + Audit/Assurance | separate generation, watermark shadow read, exact roots/counts, routing rollback | source remains authoritative |
| retention | Data Governance + Audit/Assurance | preserve-all confirmation across every linked domain | no delete/compaction |
| capacity and checkpoints | Engineering + Operations + Risk | approved workload/space/latency envelope and repeated generated-data evidence | no runtime/readiness claim |
| authority exclusions | Risk + Security | proof of no runtime import, evidence access, fence, budget, clock, permission, or action | task rejected |

The exact non-colliding SQLite `application_id`, schema DDL/fingerprint, target filesystem/path,
capacity envelope, checkpoint policy, backup boundary, and operational thresholds remain named
prerequisites. TASK-063 resolves their required form and owners; it does not invent deployment
values.

## Consequences

### Positive

- One dedicated SQLite transaction can satisfy atomic current/history ownership on one host.
- Full-range epoch milliseconds and causal versions fit exactly without changing ADR 0027.
- Original canonical bytes remain authoritative while compact projections provide uniqueness,
  compare-and-swap predicates, and bounded range access.
- UUID/natural identity, duplicate/conflict, unsupported/corrupt/unavailable, and audit-anchor
  distinctions remain fail closed.
- The physical design adds no new service or dependency and preserves the modular monolith.
- Crash, backup, migration, retention, and capacity claims now have explicit evidence gates.

### Negative

- One writer and long-reader checkpoint starvation constrain concurrency.
- Exact bytes, current/history projections, and one predecessor-record witness per transition
  consume redundant storage.
- Preserve-all history grows without bound until a separately governed retention decision.
- WAL, `FULL`, and passing process-kill tests cannot prove a target filesystem or device is
  durable.
- Several environment- and workload-specific prerequisites remain deliberately unresolved.
- A separate test-only evidence task and later production-adapter task are still required.

## Alternatives Considered

### Reuse an existing SQLite database

Rejected. It couples incompatible ownership, schema, timestamp, retention, and migration domains
and cannot make separate child, evidence, budget, or market state atomic.

### PostgreSQL or another service database

Rejected for this single-host stage. It adds a service, dependency, deployment boundary, and
operational failure modes without measured need. Multi-host requirements trigger a new decision.

### Flat append-only files

Rejected. They do not provide the required atomic UUID/natural uniqueness, current replacement,
immutable append, indexed historical duplicate lookup, and bounded page access without building a
new database engine.

### Store only canonical BLOBs

Rejected. It cannot enforce UUID/natural uniqueness or bounded indexed version access and would
require unbounded decode scans.

### Store only decoded projections

Rejected. Reconstructed models or generic serialization cannot replace exact historical
TASK-061 bytes.

### SQLite TEXT identity and timestamp columns

Rejected. Text collation/affinity and datetime conversion could normalize or narrow accepted
identity and epoch contracts. Reversible BLOB keys and exact INTEGER epochs avoid that ambiguity.

### `WITHOUT ROWID` current and history tables

Rejected for version one because rows carry large BLOB payloads and the official guidance favors
ordinary rowid tables in that shape. A measured future layout change requires a new generation.

### In-place migration or automatic open-time upgrade

Rejected. It weakens rollback, exact-byte preservation, compatibility, and independent restore
evidence.

### `synchronous=NORMAL` or raw file backup

Rejected. `NORMAL` omits per-commit WAL sync, and raw copying can separate committed WAL state.
`FULL` plus Online Backup is selected as a requested engine behavior, not a durability claim.

## Explicit Non-Goals

- Implementing or creating any schema, database, adapter, repository, migration, backup,
  benchmark, repair, retention worker, or physical path.
- Granting create, mutation, child, request, budget, fence, retry, recovery, operator, or
  deployment authority.
- Adding a clock, planner/finalizer, evidence-body access, accepted attestation, service, scheduler,
  CLI, health monitor, runtime, or automatic action.
- Claiming exactly-once provider requests, cross-database atomicity, multi-host correctness,
  capacity, availability, durability, backup readiness, recovery readiness, continuous operation,
  deployment, Phase 2 completion, or risk closure.
- Changing TASK-059, TASK-061, ADR 0030, existing SQLite stores, TASK-037, operating mode, or any
  financial permission.

## Review Triggers

Review this decision before:

- changing any ADR 0030 value, outcome, atomic rule, audit bound, overlap, or validator;
- changing TASK-059 epoch range or TASK-061 bytes, codecs, digest domains, roots, or scopes;
- choosing executable DDL, an application ID, adapter, path, capacity value, checkpoint threshold,
  backup destination, retention duration, or migration implementation;
- changing SQLite version/source ID, page size, journal/synchronous mode, schema objects, natural
  key encoding, error mapping, or physical generation;
- adding a trusted mutation boundary, evidence/attestation integration, clock, fence, budget,
  service, runtime, retry, recovery, repair, deletion, or compaction;
- moving beyond one local host or selecting a network filesystem or service database; or
- changing TASK-037 authority, operating mode, deployment, or any financial capability.
