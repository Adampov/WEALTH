# ADR 0029: Continuous Public-Trade Stream Persistence Contract

- **Status:** Accepted
- **Date:** 2026-07-27
- **Decision owners:** Project owner, Market Data Department, Engineering Department, Security
  Department, and Audit and Assurance Department

## Task Contract

### Goal

Define one conceptual, versioned persistence contract for a possible future single-host continuous
public-trade stream checkpoint before any port, repository, physical store, schema, migration, or
runtime implementation is proposed.

### Context

ADR 0028 selects a bounded single-host operating contract around the existing explicitly invoked
public-trade orchestrator. It requires an immutable child attachment to commit before child
creation, evidence before child progress, exact pending-leaf recovery, fresh outer and child UUID
fences, and child completion before continuous cursor progress.

TASK-059 adds strict frozen policy, attachment, stream-checkpoint, plan, and lifecycle values plus
pure closed-window planning and transition validation. Those values are intentionally unused.
Nothing persists them, and no runtime composition imports their module.

The existing bounded public-trade control store and the continuous candle store provide useful
create, load, compare-and-swap, history, and recovery evidence. They do not define a public-trade
stream store, and their physical layouts or timestamp encodings are not selected here. ADR 0027's
canonical UTC and migration limits remain binding.

### Scope

This decision:

- identifies the exact TASK-059 values that form durable stream state and those that remain
  invocation-local or belong to separate durable domains;
- requires a reconstructable canonical child-creation record in addition to TASK-059's
  non-invertible creation fingerprint;
- defines conceptual create, exact-identity load, and versioned compare-and-swap operations;
- defines canonical record and child-creation serialization requirements without selecting a
  physical store;
- pins immutable identity, policy, cursor, attachment, hold, completion, and transition-evidence
  invariants;
- enumerates crash behavior at every stream attachment, child creation, evidence, child
  completion, cursor advancement, hold, and resume seam; and
- defines compatibility, versioning, migration prerequisites, retention, rollback, and
  deterministic evidence required before implementation.

### Constraints

This is architecture, documentation, and governance only. It does not:

- add or change production source, a port, repository, adapter, database, SQLite, DDL, schema,
  migration, serializer implementation, dependency, or lockfile;
- wire a runtime, network/provider path, scheduler, trigger, daemon, service, CLI, configuration,
  deployment, operator path/data, credential, permission, notification, or automatic action;
- inspect or migrate an existing store, select a host path or retention duration, or authorize a
  repair;
- implement an outer fence, child fence, shared budget, evidence store, health store, service
  lifecycle, drift detector, hold, resume, restart, or recovery; or
- claim cross-database atomicity, exactly-once requests, physical durability, capacity adequacy,
  continuous-operation readiness, multi-host exclusivity, Phase 2 completion, or closure of
  `RISK-002`, `RISK-004`, or `RISK-005`.

### Done When

- Durable and non-durable boundaries are exact and do not redefine existing child, evidence,
  source-health, service-health, budget, or authority contracts.
- Create, exact-identity load, and compare-and-swap semantics are unambiguous and fail closed.
- Canonical bytes, fingerprints, model versions, serialization versions, and future store-schema
  versions cannot be confused.
- Every separate-store crash seam has one deterministic old-state, new-state, or exact-reload
  disposition without invented progress.
- Compatibility, migration, retention, rollback, and evidence requirements precede any physical
  implementation.
- TASK-037 remains blocked and authorization remains denied.

### Not Included

- The pure persistence records or codec selected as the next bounded task.
- A persistence port, transition-history reader, repository, SQLite adapter, DDL, migration, or
  repair tool.
- Outer-claim, service-run, lifecycle-health, monitoring, capacity, trigger, or deployment
  implementation.
- Operator data, private/account data, credentials, strategies, signals, portfolio state, Risk
  decisions, orders, execution, or financial action.

## Boundary Inventory

### Exact durable TASK-059 stream state

A future durable current record contains exactly one
`ContinuousPublicTradeStreamCheckpoint`:

| Field | Durable rule |
|---|---|
| `schema_version` | Exact TASK-059 model version; unknown values fail closed. |
| `stream_id` | Immutable stream identity. |
| `source`, `venue`, `instrument`, `provider_symbol`, `instrument_type` | Immutable market and provider identity. |
| `request_variant` | Immutable endpoint/request-contract identity. |
| `policy_fingerprint` | Immutable continuous-stream policy identity; it is not the bounded-child policy fingerprint. |
| `stream_start_epoch_ms` | Immutable non-negative whole-millisecond UTC grid coordinate. |
| `cursor_epoch_ms` | First millisecond not covered by a verified completed child; never inferred from market rows. |
| `status`, `pause_reason` | Exact `ACTIVE`/`PAUSED` state and bounded reason/null invariant. A reason is not authority evidence. |
| `attachment` | Optional exact immutable child identity described below. |
| `version` | Positive monotonic compare-and-swap version; every accepted transition adds exactly one. |

When present, the TASK-059 attachment durably retains exactly:

- `job_id`;
- `window_start_epoch_ms` and `window_end_epoch_ms`;
- its TASK-059 stream-policy fingerprint; and
- `creation_fingerprint`.

The attachment begins exactly at the cursor, uses the stream fingerprint, has a non-empty
half-open range, remains on the policy grid, and stays inside the finite catch-up span. A pause,
service stop, bounded child pause/failure, uncertain child outcome, or lost process does not alter
or clear it.

### Required durable companion child-creation record

The TASK-059 `creation_fingerprint` is a digest, not a reversible creation record. It cannot by
itself reconstruct the exact pristine `PublicTradeCollectionCheckpoint`, especially its
`created_at`, bounded-child policy fingerprint, and complete identity. ADR 0028's
attach-before-create guarantee therefore requires a future companion child-creation record to
commit atomically with the stream `ATTACH` transition.

That companion record preserves one explicit full projection containing:

- its record type, child-record model version, and canonical serialization version;
- the binding stream ID, request variant, and stream-policy fingerprint;
- the exact canonical pristine version-one bounded-child checkpoint value, including job ID,
  source, venue, instrument, provider symbol, instrument type, bounded-child policy fingerprint,
  exact UTC range, deterministic `created_at`/`updated_at`, pending status, cursor, empty lease,
  zero counters, and empty stop/failure evidence; and
- no undeclared field.

`canonical_creation_bytes` means the canonical bytes of that entire versioned projection, not only
the nested child checkpoint. The stream/request binding, both model/serialization versions, and
the complete pristine child are therefore inside the domain-separated SHA-256 input and cannot be
transplanted independently.

This is a new `child_creation_payload` evidence contract. It is not the bounded-child SQLite
store's existing `record_json`, does not redefine that store's serializer, and is never compared
byte-for-byte with the store's private JSON. It must decode to the exact existing
`PublicTradeCollectionCheckpoint`; idempotent child creation then compares every decoded
immutable/pristine model field and uses the child store's own validation contract.

Its decoded child ID and range must equal the TASK-059 attachment. Its market identity must equal
the stream. Its stream fingerprint must equal the stream checkpoint. Its bounded-child policy
fingerprint must equal the separately supplied effective child policy. The stream and child
policy fingerprints have different meanings and must never be substituted for one another.

Missing creation bytes, a digest disagreement, a child-policy disagreement, or incomplete
creation material is corruption and grants no child creation or recovery authority. `ATTACH` must
construct and validate the complete child value before its stream compare-and-swap; if the exact
epoch-millisecond range cannot be represented by the existing bounded-child datetime contract,
attachment fails before commit. Supporting such a range requires a separately governed contract
tightening or new bounded-child model, not truncation or a partially reconstructable attachment.

### Values outside the stream current record

The following do not become stream-checkpoint fields:

- the full `ContinuousPublicTradePolicy` in the current checkpoint; one immutable canonical
  projection is retained only in stream-creation evidence, and governed configuration supplied to
  each invocation must match it field-for-field plus fingerprint;
- the exact injected `now`, latest eligible end, planner status, and every
  `ContinuousPublicTradePlan` result;
- an uncommitted candidate child UUID or creation fingerprint;
- service-run identity, lifecycle status, work counters, liveness, and service-health evidence;
- outer or child lease owner, fresh UUID token, expiry, and acquisition history;
- transition command kind, actor, manual authority, completion proof, and observation time in the
  current checkpoint;
- the bounded-child current checkpoint, transition history, source-health evidence, accepted raw
  and canonical market evidence, conflicts, and exact pending leaf; and
- shared request-budget policy, reservations, decisions, and counters.

Those values are invocation-local or already belong to separately versioned durable domains.
Transition kind and bounded evidence references are retained in a separate append-only stream
transition record; they do not become mutable current-checkpoint fields. Stream persistence
neither copies external evidence bodies or mutable state nor makes their stores atomic.

## Decision

### Canonical persistence envelope

A future logical record uses an explicit envelope with:

- `record_type`;
- `serialization_version`;
- the exact TASK-059 checkpoint;
- the optional exact companion child-creation record; and
- no undeclared fields.

The canonical envelope digest is not a recursive field inside that envelope. It is lowercase
`sha256:` plus SHA-256 over:

`b"wealth.continuous_public_trade.stream_record/v1\x00" + canonical_envelope_bytes`

Readers recompute it from the exact retained envelope bytes. Compare-and-swap commands supply the
trusted prior digest, and each separate transition record binds both the recomputed prior digest
and successor digest.

Create also appends one separate canonical stream-creation evidence record. It is not a TASK-059
transition and contains exactly:

- its record type, creation-evidence model version, and canonical serialization version;
- the stream ID and complete immutable stream identity plus an exact canonical projection of every
  validated `ContinuousPublicTradePolicy` field, including its distinct caller-supplied
  `policy_fingerprint`;
- explicit `null` prior version and prior digest;
- successor version `1`, lowercase `successor_envelope_hex` containing the exact canonical
  version-one stream-envelope bytes, and their digest;
- the bounded external governed-create evidence reference; and
- one fixed-`datetime.UTC` `recorded_at`.

The version-one stream-policy projection freezes exactly `schema_version`, `window_size_ms`,
`settlement_lag_ms`, `max_catchup_span_ms`, `max_jobs_per_invocation`,
`max_requests_per_job`, `max_records_per_job`, and the caller-supplied `policy_fingerprint`.
Later model fields do not silently enter this projection or alter version-one bytes.

Its digest is lowercase `sha256:` plus SHA-256 over
`b"wealth.continuous_public_trade.stream_creation/v1\x00" + canonical_stream_creation_bytes`.
The causal stream history begins with this record and continues with TASK-059 transition records;
no nonexistent `CREATE` transition kind is invented.

JSON records embed canonical envelope bytes only as an even-length lowercase hexadecimal string.
Strict decoding rejects a prefix, uppercase, odd length, non-hex character, decoded oversize, or
bytes that fail the envelope codec; it then re-encodes the decoded bytes and requires exact hex
equality. The digest input is the decoded canonical envelope bytes, never the hexadecimal text.
This one representation is used by every `successor_envelope_hex` field.

The model's `schema_version`, the envelope's `serialization_version`, the checkpoint's causal
`version`, and a future physical store's schema version are four distinct axes. None may be
derived from another.

Canonical version-one bytes are:

- UTF-8 JSON with no byte-order mark, leading/trailing whitespace, or terminal newline;
- produced from one explicit field projection with lexicographically sorted keys, compact
  `,`/`:` separators, `ensure_ascii=True`, and non-finite numbers forbidden;
- exact lowercase hyphenated UUID strings, exact enum values, JSON integers for integer fields,
  explicit `null` for absent optionals, and no implicit normalization or coercion;
- parsed with duplicate keys, unknown keys, unknown record/model/serialization versions,
  noncanonical spellings, booleans in integer fields, floats, overflow, and trailing bytes
  rejected; and
- reproduced byte-for-byte after strict decode before the value is trusted.

An installed Pydantic version's generic JSON output is not the long-term byte authority. A future
codec task must freeze an explicit projection, golden byte fixtures, and digests. The conceptual
child-creation fingerprint is lowercase
`sha256:` plus SHA-256 over:

`b"wealth.continuous_public_trade.child_creation/v1\x00" + canonical_creation_bytes`

The prefix and bytes are versioned. Original canonical bytes remain evidence; a later reader never
silently recomputes an old fingerprint with a new serializer.

The stream-envelope digest uses its separate domain above. A child-creation fingerprint and a
stream-envelope digest are not interchangeable even when their JSON happens to contain related
fields.

### Required pure-codec safety bounds

TASK-061 must freeze projections that preserve every currently valid model value while enforcing:

- 65,536 raw bytes per outer JSON record before UTF-8 decoding or parser materialization;
- 8,192 canonical bytes per `child_creation_payload`;
- 16,384 decoded canonical bytes per stream envelope;
- 32,768 even lowercase ASCII characters per `successor_envelope_hex`, which decodes to at most
  the 16,384-byte envelope cap;
- 8,192 raw lexical bytes between the quote delimiters of every other escaped JSON string token,
  measured on canonical `ensure_ascii=True` input rather than by re-encoding its decoded value;
- at most 16 JSON nesting levels, 128 total object members, fixed ASCII keys of at most 64
  characters, and 19 decimal digits per integer token excluding an optional leading minus.

A bounded lexical scan enforces byte, depth, member, string-token, and integer-token limits before
the general JSON/Pydantic boundary. Maximal control-character and astral Unicode identifiers,
maximal pristine child, maximal attached envelope, and maximal scalar-only evidence references
must fit the frozen projection; if a chosen key layout exceeds a limit, the projection changes
rather than narrowing TASK-059. Exact-limit and limit-plus-one fixtures pin every byte bound. These
are parser-safety limits, not operational capacity evidence.

### Interaction with ADR 0027

TASK-059 cursor and range coordinates are exact epoch milliseconds and remain exact integers.
They are not silently converted to local time, mixed-offset text, floating point, or a different
precision.

TASK-059 permits values through `2**63 - 1` milliseconds. That full range is not representable as
signed-64-bit epoch microseconds, and most of it is outside Python's `datetime` calendar. A future
physical design must either preserve the accepted integer range exactly or introduce a separately
reviewed contract tightening. It must fail closed on projection overflow and may not truncate,
wrap, clamp, or treat ADR 0027's epoch-microsecond projection as proof that every TASK-059 value is
SQLite-projectable.

Any transition or audit observation time is a separate fixed-`datetime.UTC` instant serialized as
ADR 0027's exact six-fractional-digit RFC 3339 `Z` text. Causal ordering remains checkpoint version
order, never wall-clock order.

Every datetime inside `child_creation_payload`—range boundaries, cursor, `created_at`,
`updated_at`, and any non-null datetime field—uses that same exact fixed-UTC six-fractional-digit
`Z` encoding. Conversion from an attachment's epoch milliseconds is exact integer arithmetic and
must round-trip to the identical millisecond; it never rounds, truncates, or accepts sub-millisecond
drift. These payload rules remain separate from the bounded-child store's existing serializer.

### Pure two-pass attachment finalization

TASK-059's planner accepts a creation fingerprint before it returns a due attachment, while the
fingerprint selected here covers a child payload that includes that returned range. No runtime may
duplicate the planner's range arithmetic or persist a placeholder. A future pure caller therefore:

1. fixes the exact checkpoint, policy, candidate child UUID, effective child policy, and one
   trusted fixed-UTC ATTACH command time used both as the planner's `now` and identically for child
   `created_at`/`updated_at` and transition `recorded_at`;
2. calls the unchanged TASK-059 planner with the fixed in-memory all-zero provisional value
   `sha256:0000000000000000000000000000000000000000000000000000000000000000`;
3. for `HELD`, `WAITING`, or an already attached stream, discards unused candidate values and uses
   the returned existing outcome without creating anything;
4. for a new due attachment, builds and validates the exact `child_creation_payload` from the
   provisional range, fails closed on datetime representability, and computes its real
   domain-separated fingerprint;
5. calls the planner again with every identical input and the real fingerprint; and
6. requires exact equality of status, stream/policy identity, cursor, eligibility boundary, child
   UUID, and range between both plans, with only the creation-fingerprint value permitted to
   change, before constructing the final successor envelope.

The provisional value is never durable evidence, never reaches child creation or compare-and-swap,
and grants no authority. “Provisional” is a workflow label, not a claim that the all-zero value is
outside SHA-256's output space; equality with the real digest remains valid. TASK-061 must prove
this two-pass equivalence across boundary and hostile fingerprint cases while preserving the
existing TASK-059 API and behavior.

## Conceptual Operations

### Create

Create accepts one exact proposed envelope, the complete effective `ContinuousPublicTradePolicy`
whose validated, caller-supplied `policy_fingerprint` exactly matches it, one bounded
governed-create evidence reference. The envelope contains only a pristine version-one `ACTIVE`
checkpoint: cursor equals start, pause reason and attachment are absent, and exact policy/grid
invariants validate.

The boundary first performs exact identity/natural-identity load before sampling a mutation time.
An exact retained envelope and stream-creation record are classified historically from their stored
`recorded_at`; this path writes nothing and samples no new time. Only when both identities are
absent may the future mutation boundary—not the caller—obtain command time exactly once from its
trusted injected UTC clock after structural/policy validation and before authority validation or
write. Caller override and backdating are forbidden. The evidence must authorize this exact
immutable natural identity and proposed digest at the trusted command time. The future store
commits the current envelope and its version-one append-only stream-creation evidence in one
store-local transaction.

Outcomes are explicit:

- absent identity and valid record: `INSERTED`;
- exact retry whose proposed envelope, policy, and evidence reference match a retained envelope and
  stream-creation record whose stored bytes, stored `recorded_at`, external evidence, digest, and
  complete creation/history validation all pass: `DUPLICATE` without new clock sampling or write;
- same stream ID with any different canonical content, or the same natural stream identity under
  a different stream ID: `CONFLICT`;
- invalid model/policy: reject before storage; and
- unsupported, corrupt, or unavailable store: typed failure, never absence.

Create never replaces, merges, normalizes, or upserts existing state. The store-local natural
identity is `(source, venue, instrument, provider_symbol, instrument_type, request_variant)`; it
excludes UUID, start, and policy so a second UUID cannot silently create overlapping collection for
the same feed. A future lifecycle or migration that permits replacement requires a separate
decision. This uniqueness is not a multi-host or cross-store guarantee. `NOT_FOUND` from a later
load is not permission to create unless an explicit governed create command independently exists.

### Exact-identity load

Load requires the stream ID, complete expected immutable stream identity, and the complete
effective `ContinuousPublicTradePolicy`; its validated, caller-supplied fingerprint must exactly
match the checkpoint. When a stream is attached, load also requires the complete effective
bounded-child policy, validated in its existing application contract, whose computed fingerprint
must match the companion creation payload. It does not trust the key or a supplied digest string
alone.

TASK-059 intentionally defines no derivation algorithm for the stream-policy fingerprint. This
decision does not invent one or claim that the fingerprint cryptographically binds every policy
field. Instead, the stream-creation record binds the complete canonical policy projection, and
load/CAS require exact field-for-field equality with it in addition to exact fingerprint equality.
Any future derived stream-policy fingerprint is a separately reviewed contract change. The pure
persistence codec must not import the application-layer bounded-child policy merely to recompute
its fingerprint.

A bounded current load is returned only after:

- exact canonical byte/profile and model validation;
- immutable identity plus complete supplied stream-policy validation and field-for-field agreement
  with the stream-creation record, including exact fingerprint comparison;
- cursor, pause, attachment, grid, range, and version validation;
- companion creation-byte, decoded-child, and supplied child-policy validation when attached; and
- current-envelope agreement with the creation record at version one, or with the latest transition
  and its directly addressed predecessor envelope at later versions.

Current load reads a constant number of stream-store records; it does not replay an ever-growing
history or re-fetch every external evidence body. At version greater than one, the latest
transition's exact successor bytes must equal current, its prior version/digest must match the
direct predecessor's immutable successor bytes, and the TASK-059 validator must accept that one
step. At version one it loads and validates the external governed-create evidence body; at later
versions it loads and validates every external authority/completion evidence body required by the
latest transition, including digest, scope, outcome, and historical validity at `recorded_at`.
It recomputes the creation root at version one or the latest transition root at later versions and
returns that root with the current view. This bounded check is not a full-history attestation and
never implies that the retained prior root or older evidence was rechecked.

A separate audit operation returns immutable creation/transition records in pages of 1 through 100
new records with an exact continuation cursor and preceding rolling-history-root anchor returned by
the immediately prior validated page or held in an accepted attestation. The first page begins with
the creation record. Every later page additionally loads exactly
one immutable predecessor creation/transition record as an overlap, outside the 1-through-100
returned records, and requires its stream ID, successor version, exact successor-envelope bytes and
digest to equal the continuation cursor. A creation-record overlap uses the version-one creation-root
formula; a transition overlap uses its retained prior root. In either case the exact predecessor
bytes must recompute the same anchored successor-history root. The first new transition is then
validated against that predecessor envelope before the page continues. Each page
validates canonical bytes, versions, digests, successor envelopes, TASK-059 transitions, rolling
roots, and historical evidence-reference validity against the referenced external evidence bodies;
no call reads more than 101 stream-history records or an unbounded chain. Migration, compaction, or
a full attestation must consume every page under an explicit finite work limit and preserve the
final root.

A successful complete or incremental audit produces one separately retained, externally anchored
history attestation containing stream ID, audit-profile version, through-version, through-envelope
digest, through-history-root, fixed-UTC completion time, and explicit `ACCEPTED` outcome.
An incremental audit may extend an already accepted attestation by a bounded page after checking
its exact prior anchor. Governed create plus its authority evidence is the sole bootstrap: it
atomically inserts version one and its creation record without a prior attestation, then a bounded
creation audit must produce the accepted version-one attestation. Before any post-create child
create/recovery, outer or child claim, budget reservation, provider request, evidence admission, or
stream mutation, a future runtime requires an accepted attestation whose through-version and
envelope digest/history root exactly match the trusted current state, plus separately validated
latest transition evidence. The only narrow exception preserves ADR 0028: a provider operation
that already crossed every pre-request gate under the exact accepted pre-hold attestation, fence,
and authority may admit only its already-returning evidence and finish or fail that same finite
child lifecycle after a concurrent `MANUAL_HOLD` when the separately validated hold evidence
explicitly classifies the hold as non-integrity operational control and preserves the response and
admission contract. The exception never applies to schema/contract drift, invalid payload, quality
rejection, raw/canonical conflict, evidence-admission failure, corruption, or an absent/ambiguous
classification. Those outcomes stop canonical admission and progress; returned material may enter
only an explicitly governed quarantine/attention-evidence path. The child may start no new attempt
or request, select no new child, and perform no stream advance or other stream mutation. Any later
child invocation, request pipeline, or stream mutation again requires an attestation matching the
then-current stream state and every applicable fresh gate. Current load or planning alone grants no
action. Missing, stale, conflicting, or rejected attestation is otherwise a fail-closed
hold/attention condition. The attestation remains outside the current checkpoint and TASK-061
record/codec scope.

Absence, identity conflict, unsupported version, canonical-byte disagreement, malformed storage
type, projection disagreement, history gap/orphan/tail mismatch, invalid attachment, missing
creation bytes, and storage failure remain distinct fail-closed outcomes. No outcome repairs,
deletes, infers, advances, resumes, or creates work.

### Versioned compare-and-swap transition

A mutation request crossing the trusted boundary supplies:

- complete expected immutable identity and effective stream policy, plus the effective
  bounded-child policy whenever an attachment is present or proposed;
- the exact trusted prior version, prior canonical-record digest, and accepted prior rolling
  history root;
- one explicit TASK-059 transition kind;
- the time-independent transition intent, including the exact candidate child UUID for `ATTACH`,
  bounded reason when applicable, and exact completed-child identity for `CHILD_COMPLETED`;
- exactly one `STREAM_TRANSITION_AUTHORITY` reference and, for `CHILD_COMPLETED`, exactly one
  `CHILD_COMPLETION` reference; and
- any separately validated outer authority required by a future runtime.

The caller does not supply `recorded_at`, an ATTACH child-creation payload, or a preconstructed
successor envelope. Before sampling a new command time, the boundary performs exact reload. A
retained successor and transition are a historical exact duplicate only when the boundary can
reconstruct them from the exact time-independent request inputs plus the retained transition's
stored `recorded_at` and obtain identical canonical bytes and evidence references; that path takes
no new clock sample, authority action, or write. Only when the expected prior is still current does
the mutation boundary sample its trusted injected UTC clock exactly once. It uses that instant as
the planner's `now`; for `ATTACH` it then executes the pure two-pass finalizer, constructs the exact
child-creation payload and successor envelope, and uses the same instant for child
`created_at`/`updated_at` and transition `recorded_at`. For every kind the boundary constructs the
complete successor at exactly `prior.version + 1`, runs the pure TASK-059 transition validator, and
independently validates authority and causal evidence. Only then does it construct the internal
store-level compare-and-swap command carrying the successor canonical bytes/digest and
`recorded_at`.

Inside one future stream-store transaction, one matching current record is replaced and one
immutable transition record is appended. Missing, stale, digest-mismatched, identity-mismatched, or
concurrently changed state yields `CONFLICT` with no write. A committed successor after a lost
acknowledgement is therefore classified only by the historical reload path, never by resampling or
backdating time.

The separate canonical transition record contains exactly:

- record type, transition-model version, and canonical serialization version;
- stream ID, prior version, successor version, and TASK-059 transition kind;
- the exact prior rolling history root;
- the domain-separated prior stream-envelope digest plus lowercase `successor_envelope_hex`
  containing the exact canonical successor-envelope bytes and their recomputed digest;
- a bounded reason code when the transition kind permits one;
- the exact bounded authority-evidence reference and/or child-completion-evidence reference
  required by that transition kind, otherwise explicit `null`; and
- one fixed-`datetime.UTC` `recorded_at` observation serialized in ADR 0027 canonical text.

`reason_code` is required for `RETAIN` and `MANUAL_HOLD`, is exactly equal to the successor
checkpoint's `pause_reason` for `MANUAL_HOLD`, and is explicit `null` for `ATTACH`,
`CHILD_COMPLETED`, and `MANUAL_RESUME`. This matrix is canonical rather than caller-selectable.

Each evidence reference is one version-one scalar-only object with fixed ASCII keys of at most 64
characters and no arbitrary map, array, body, or extension field. It contains exactly:

- reference version `1`;
- `evidence_kind`, one of `STREAM_CREATE_AUTHORITY`, `STREAM_TRANSITION_AUTHORITY`, or
  `CHILD_COMPLETION`;
- an opaque exact built-in `evidence_id` of 1 through 128 visible-ASCII non-whitespace characters;
- an exact lowercase domain-separated `evidence_digest`;
- `scope_digest`;
- `outcome`, exactly `APPROVED` for either authority kind or `ACCEPTED` for child completion;
- fixed-UTC `valid_from`; and
- fixed-UTC `expires_at` or explicit `null`.

`scope_digest` is lowercase `sha256:` plus SHA-256 over
`b"wealth.continuous_public_trade.evidence_scope/v1\x00" + canonical_scope_bytes`. The version-one
scope projection contains exactly evidence kind, stream ID, transition kind or `null`, prior
version/digest/history root or `null`, successor version, successor digest or `null`, child job ID
or `null`, child-policy fingerprint or `null`, child-creation fingerprint or `null`, reason code or
`null`, and the complete stream-policy projection or `null`.

Create requires null transition, prior version/digest/history root, and child fields, successor
version one, the exact successor digest, and the complete policy projection from the stream-creation
record. A
`STREAM_TRANSITION_AUTHORITY` scope for `ATTACH` is intentionally independent of the as-yet-unsampled
trusted time: it binds the exact stream ID, `ATTACH` kind, prior version/digest/history root,
successor version, candidate child UUID, and effective child-policy fingerprint, while successor
digest, child-creation fingerprint, reason code, and stream-policy projection are `null`. It
authorizes only that next policy-determined attachment while valid; it does not select a range,
time, or payload and grants no action by itself. The trusted boundary then samples time once,
finalizes the attachment, and the immutable transition record plus rolling root bind the exact
successor bytes and creation fingerprint. Prior version/digest/history root, successor version,
candidate UUID, and compare-and-swap uniqueness prevent reuse on another state or convergent
alternate history.

Every other `STREAM_TRANSITION_AUTHORITY` scope requires exact kind, prior
version/digest/history root, successor version/digest, null child fields, and a null policy
projection. Its reason code follows the same exact transition-kind matrix above.
`CHILD_COMPLETION` requires exact `CHILD_COMPLETED` kind, prior
version/digest/history root, and successor bindings plus the attached child ID, effective
child-policy fingerprint, and creation fingerprint, with null reason and policy projection.
The reference, external evidence body, stream creation/transition record, and successor envelope
must reproduce every applicable non-null scope value; nullability is kind-specific and exact.

Every stream mutation retains one `STREAM_TRANSITION_AUTHORITY` reference; `CHILD_COMPLETED` also
retains one `CHILD_COMPLETION` reference. Stream creation retains one
`STREAM_CREATE_AUTHORITY` reference. A bounded reason is an exact built-in string of 1 through 128
non-whitespace code points and is not authority. The evidence body and any operator identity,
secret, credential, or operator path remain in the separately approved evidence domain. A
reference is not authority until that external evidence is loaded and its evidence digest,
canonical scope, outcome, and validity are checked under the applicable policy.

For create or a new transition, authority is valid only when
`valid_from <= recorded_at < expires_at`; absent expiry has an open upper bound. The explicit
command time is sampled exactly once by the future mutation boundary's trusted injected UTC clock
and becomes persisted `recorded_at`; the caller cannot choose an in-window historical value.
TASK-061's pure values can carry and validate the timestamp shape but cannot prove its clock
provenance or grant authority. Historical load/audit proves that the evidence was valid at that
recorded time; later expiry does not corrupt an accepted historical transition and never authorizes
a new command. A lost-acknowledgement reopen classifies retained state historically by exact load;
it does not backdate a new mutation.

Every reference separately requires `expires_at is null or expires_at > valid_from`. A
creation/transition record's one trusted `recorded_at` must fall inside the interval of every
reference required by that record, including both transition-authority and child-completion
references on `CHILD_COMPLETED`; equal or reversed bounds fail closed.

Every transition `recorded_at` must be greater than or equal to the directly preceding
creation/transition `recorded_at`; equality is permitted at canonical microsecond precision, while
checkpoint version remains the causal authority. A regressing trusted clock fails before mutation.
For `ATTACH`, the pristine child's `created_at` and `updated_at` both equal that transition's
trusted command time. A future-dated or unequal child time fails before the companion payload,
attachment, or stream write is accepted.

Canonical transition bytes use the same strict JSON profile. Their digest is lowercase `sha256:`
plus SHA-256 over:

`b"wealth.continuous_public_trade.stream_transition/v1\x00" + canonical_transition_bytes`

The rolling history root commits to every exact creation/transition byte, including every authority
and completion reference. Let `raw32(x)` be the exact 32 bytes decoded from a canonical
`sha256:<64-lowercase-hex>` value. The version-one root is:

`sha256:` plus SHA-256 over
`b"wealth.continuous_public_trade.history_root/v1\x00\x01" + canonical_stream_creation_bytes`

For each later transition it is:

`sha256:` plus SHA-256 over
`b"wealth.continuous_public_trade.history_root/v1\x00\x02" + raw32(prior_history_root) + canonical_transition_bytes`

The transition record's retained `prior_history_root` must equal the prior accepted root. Page
audits recompute roots in order, and any byte change in an older record changes every later root.

The transition tail is contiguous only when every stream ID and version pair is exact, every
prior/successor envelope digest matches the immutable versioned envelope bytes, every successor
passes the TASK-059 transition validator against the preceding retained envelope, the current
record equals the final successor bytes, and every transition record digest recomputes from its
original bytes.

Compare-and-swap is optimistic state control. It is not an outer lease, fresh-UUID fence, retry
policy, network authority, or recovery permission. ADR 0028's separate outer fence and acquisition
ledger remain future requirements. A version/digest conflict or lost fence ends that invocation:
a future runtime records a failed service outcome, performs no automatic reload-and-continue or
blind compare-and-swap retry, and requires ADR 0028's operator decision/manual stream-hold boundary
before recovery. Failure to commit the hold is itself fail-closed and grants no continuation.

## Transition Evidence and Preconditions

- `RETAIN` changes only the version and records a bounded reason. It grants no claim or work
  authority.
- `ATTACH` requires an active unattached stream and commits the exact attachment plus complete
  companion child-creation bytes before any bounded-child create call.
- `CHILD_COMPLETED` requires a separately loaded, causally validated completed child record whose
  ID, market identity, exact range, bounded-child policy fingerprint, terminal version/history,
  `next_window_start`, and completion evidence match the attachment and companion creation
  record. It advances the cursor exactly to the attached end and clears the attachment and its
  current-record companion reference in one stream-store transition. Retained transition history
  keeps the creation and completion references.
- `MANUAL_HOLD` preserves cursor and attachment and requires exact actor/governance evidence plus
  a bounded reason. `pause_reason` alone is not authorization evidence.
- `MANUAL_RESUME` preserves cursor and attachment and requires a fresh explicit authority
  reference plus evidence that all applicable failure, reconciliation, rollback, and TASK-057
  drift gates passed.

The TASK-059 validator's `completed_job_id` proves only ID equality; it does not prove durable
child completion. Missing, malformed, stale, expired, conflicting, rejected, or
revise-required authority or evidence never authorizes creation, advancement, restart,
remediation, or resume.

## Crash-Seam Matrix

| Seam | Durable interpretation and required next action |
|---|---|
| Before attachment CAS | The candidate exists only in invocation memory. The stream remains unattached; no child creation is authorized. A later invocation may plan from the unchanged cursor. |
| Attachment CAS result unknown | Exact reload decides. An absent attachment means no child may be created; an attached record requires the exact retained child ID, range, and creation bytes. Never replan or replace it. |
| Attachment committed, child absent | Exact reload only classifies the seam. After an accepted attestation matches current stream state and every applicable fresh fence and authority gate passes, reconstruct the exact pristine child from the companion bytes and effective child policy. An exact duplicate create is acceptable; any disagreement is conflict/corruption. |
| Child create result unknown | Load the exact child ID and compare every immutable/pristine field. Never create an alternate child or use a new creation time. |
| Before provider request | Existing outer/child fences and the shared durable pre-request budget must validate in their own domains. Stream attachment alone grants no request authority. |
| Request or budget reservation committed, market evidence absent | No child or stream cursor progress exists. A later bounded invocation may repeat work under existing finite request and budget semantics; exactly-once requests are not claimed. |
| Market evidence committed, child checkpoint not advanced | Reload the exact durable child checkpoint. Refetch its retained pending leaf when one exists; otherwise resume the exact remaining bounded range from its durable cursor under the existing child policy. Existing idempotent admission resolves duplicate evidence; the stream does not advance. |
| Child checkpoint advanced but not completed | Preserve the same stream attachment and exact durable child checkpoint. Resume its retained pending leaf when present, otherwise its exact remaining bounded range under the existing child policy. No new child is selected. |
| Child completed, stream CAS absent | Keep the attachment; exact reload alone authorizes nothing. After an accepted attestation matches current stream state and fresh transition authority plus completion evidence validate, a later invocation verifies the exact completed child and advances the stream with zero provider requests. |
| Stream completion CAS result unknown | Exact reload decides. Old state repeats only the completion verification; new state has the cursor at the attached end and no attachment and must not refetch or rewind. |
| Stream CAS committed, service evidence absent | The stream state remains authoritative. Missing lifecycle evidence is a fail-closed audit/attention condition, not permission to rewind or replay the child. |
| Hold/resume CAS result unknown | Exact reload decides the state. Neither silence nor an assumed actor result changes authority. |
| Hold while child work is in flight | The hold preserves cursor and attachment. A provider operation already past every pre-request gate may admit only its already-returning evidence and finish or fail that same finite child lifecycle under the exact pre-hold attestation, fence, and authority only when separately validated hold evidence explicitly classifies a non-integrity operational hold and preserves the response/admission contract. Schema/contract drift, invalid payload, quality/evidence failure, corruption, or ambiguous classification stops canonical admission/progress and permits only separately governed quarantine/attention evidence. No path starts a new attempt/request or performs stream advancement/mutation until current attestation and explicit governed resume gates pass. |

Stream current state/history, bounded-child control/history/health, market evidence, lifecycle
evidence, and shared budget remain separate commit domains. No ordering above makes them atomic.
Safe replay may repeat a provider request or idempotent evidence admission; it may not skip a gap
or invent progress.

## Versioning, Compatibility, and Migration

No continuous public-trade stream store exists today. An initial implementation therefore
bootstraps an empty, dedicated store; it never derives a cursor by scanning bounded jobs, market
evidence, or the continuous candle database, and it never adds objects to the current strict
public-trade version-one control store.

Before any writer version changes:

1. freeze the old model, canonical byte profile, domain-separated fingerprint, and hostile/golden
   fixtures;
2. add a strict version-dispatching reader that preserves the exact original bytes;
3. reject unknown newer versions and type incompatible older values rather than ignoring fields;
4. validate the connected stream checkpoint, full transition chain, attachment creation record,
   manual authority, and referenced child completion evidence; and
5. define typed quarantine without silently normalizing or repairing a record.

A version-one reader rejects version-two records, unknown fields, and every unsupported version; it
does not treat an additive field as compatible. A version-two reader dispatches retained
version-one bytes through the frozen version-one codec and preserves those bytes unchanged. An old
writer never writes a newer physical generation. Downgrade reads only an untouched older
generation unless a separately reviewed, proven lossless reverse converter produced and verified
it; otherwise recovery is forward-only.

Selecting SQLite or another repository, DDL, indexes, projections, store markers, backup, or
migration is a separate implementation decision. If SQLite is later selected, `PRAGMA
user_version` alone is insufficient: the design must require a dedicated storage-format marker,
exact schema/object fingerprint, exact storage types, foreign-key/integrity checks, and canonical
record/projection agreement.

A later migration follows ADR 0027: fence writers, take a proven consistent snapshot, retain an
externally anchored manifest, validate connected records and causal history, copy into a separate
physical generation, shadow-read at a recorded watermark, verify counts/hashes/identities and an
independent restore, then cut over through an atomic routing marker. An old binary does not open a
new generation by changing a version integer. After incompatible new writes, rollback requires a
proven lossless reverse converter; otherwise recovery moves forward.

## Retention and Rollback

No retention duration, capacity, compaction algorithm, or deletion authority is selected.
Until separately approved evidence proves otherwise, preserve:

- the current checkpoint, stream-creation record with complete policy and version-one envelope
  bytes, and complete contiguous transition chain with immutable successor-envelope bytes and
  rolling history roots for every version;
- every accepted externally anchored full/incremental history attestation and its exact audit
  profile, version, envelope digest, rolling root, outcome, and completion time;
- cleared and active attachment creation bytes and fingerprints;
- outer-fence acquisition references when later implemented;
- every governed-create, transition-authority, and child-completion reference plus its exact
  external evidence body, canonical scope, digest, outcome, and validity interval under the
  separately approved handling/retention boundary;
- child completion chain material sufficient to revalidate every cursor advance; and
- linked lifecycle, source-health, market-evidence, conflict, and budget records under their
  existing retention contracts.

Compaction may not remove the evidence needed to prove immutable identity, a hold, attachment
reconstruction, child completion, cursor causality, evidence scope, attestation continuity, fence
non-reuse, or rollback. Coordinated retention across separate domains needs a separate
capacity-backed decision.

Rollback disables the continuous trigger/deployment and returns operation to the existing
explicitly invoked bounded flow while preserving the exact stream, attachment, history, child,
market, health, lifecycle, and budget evidence. It never rewinds or advances a cursor, clears a
hold, deletes or replaces a child, resets a budget, reuses a UUID fence, downgrades an unknown
schema, or treats disabled deployment as a stream state. Older code may resume only if it reads
retained state exactly; otherwise the continuous path stays disabled and recovery proceeds through
a separately approved forward change.

## Evidence Required Before Implementation

A later implementation proposal must first provide deterministic tests for:

- golden canonical bytes and fingerprints; cross-domain substitution among all six digest
  contracts; reordered/duplicate/unknown keys; malformed UTF-8; trailing bytes; unknown
  record/model/serialization versions; and exact round trips;
- every exact codec cap and cap-plus-one, including lexical string limits, decoded envelope/hex
  agreement, maximal control/astral identifiers, deep JSON, huge integers, and sanitized raw
  parser/depth failures;
- pristine create, exact duplicate, same-ID conflict, exact-identity load, absent state, immutable
  mismatch, natural-identity/different-UUID conflict, corrupt bytes/types/projections, history
  gaps, and current/tail disagreement;
- complete stream-policy field mismatch under a reused identical caller fingerprint;
- every legal and illegal TASK-059 transition plus missing, stale, conflicting, or mismatched
  transition evidence;
- two competing compare-and-swap writers, stale versions/digests, lost acknowledgements, and one
  winner without automatic retry pressure, including historical duplicate classification before
  any new trusted-clock sample;
- attachment/creation-byte/digest disagreement, stream-versus-child policy confusion, missing
  deterministic `created_at`, and exact same-child reconstruction;
- two-pass provisional/final planning equality across boundaries and hostile fingerprints, using
  one identical fixed-UTC instant as planner `now`, child `created_at`/`updated_at`, and transition
  `recorded_at`, with no durable provisional fingerprint or duplicated range arithmetic;
- child completion ID/range/policy/version/history mismatch and zero-request advancement only
  after exact completion;
- typed evidence kind/outcome/field bounds, evidence-body digest, kind-specific scope digest,
  create-policy scope, exact ATTACH authority binding before time sampling, ATTACH null
  successor/creation digests, exact successor binding for every other transition authority,
  child-policy binding, the exact transition-kind reason matrix, nullability, equal/reversed
  validity intervals, and cross-record mismatch;
- trusted mutation-clock provenance, caller backdating rejection, non-regressing record time,
  ATTACH child-time equality, and historical later-expiry behavior;
- creation-root and chained-transition-root golden values, prior-root substitution, older-record
  mutation, bounded page continuation, and exact cumulative-root recomputation;
- governed-create bootstrap, required version-one attestation, accepted/stale/conflicting/rejected
  attestation outcomes, incremental-anchor extension, and denial of every post-create action while
  an exact current attestation is absent;
- every crash seam in the table, including evidence-first idempotent refetch, old/new state reload
  at transaction boundaries, permitted non-integrity in-flight hold completion, and denial or
  explicit quarantine for drift, invalid-payload, quality, evidence, corruption, and ambiguous
  in-flight hold classifications;
- hold/resume with missing, stale, rejected, or conflicting authority and preservation of cursor
  and attachment;
- epoch-millisecond extremes, Python-calendar limits, epoch-microsecond projection overflow, and
  no truncation or silent precision change;
- unknown schema/profile handling, backup/restore, shadow-read, routing rollback, forward-only
  recovery, and retention-link integrity; and
- unchanged shared pre-request budget, child fresh-UUID fencing, exact pending leaf, causal source
  health, manual TASK-057 drift governance, and disable-to-bounded-flow behavior.

An implementation must also pass lockfile, formatting, lint, strict typing, focused and complete
tests, dependency audit, health, CI, security, scope, and independent assurance review. Passing
these checks does not authorize deployment or continuous operation.

## Safety and Authority Boundary

This decision creates no persisted record and performs no I/O. It does not make TASK-059 active,
implement ADR 0028, access operator data, grant a fence, authorize a provider request, create or
recover a child, write a hold, approve a resume, or schedule another invocation.

Live trading, leverage, withdrawals, external notifications, and autonomous execution remain
disabled. TASK-037 remains blocked; its missing project-owner plus independent Risk and Security
authorization remains denial.

## Consequences

### Positive

- A future store has an exact minimal current state and does not copy separately owned control or
  evidence domains.
- The attach-before-create crash seam becomes reconstructable instead of relying on a
  non-invertible digest.
- Stream policy, child policy, lifecycle evidence, and authority remain distinct and testable.
- Canonical bytes, causal versions, corruption, conflicts, and lost acknowledgements have
  deterministic outcomes before a repository is selected.
- Rollback and retention preserve auditability without claiming cross-store atomicity.

### Negative

- A future implementation needs a second immutable child-creation record and complete transition
  evidence in addition to the TASK-059 checkpoint.
- Separate stores retain replay and reconciliation complexity and cannot provide cross-database
  exactly-once semantics.
- Full causal retention can grow until a separately governed compaction policy is proven.
- The TASK-059 integer range and ADR 0027's SQLite epoch-microsecond target need an explicit
  representability decision before physical time projection.
- Continuous operation remains unavailable.

## Alternatives Considered

### Persist only the TASK-059 checkpoint

Rejected. `creation_fingerprint` cannot reconstruct the exact pristine bounded child, and
`pause_reason`/`completed_job_id` do not prove manual authority or durable completion.

### Recompute missing child inputs from the current clock and configuration

Rejected. A new creation time or changed policy produces a different child and breaks exact
idempotent recovery.

### Put stream, child, market evidence, lifecycle, and budget state in one database

Rejected. It would replace accepted boundaries, require a material migration, and still would not
prove physical durability or exactly-once provider requests. This task does not select storage.

### Derive the cursor from the latest stored trade

Rejected. Sparse windows, rejected/conflicting evidence, an incomplete child, or external writes
could create a permanent gap or skip unverified work.

### Treat compare-and-swap as the outer fence

Rejected. A successful state transition does not provide ADR 0028's fresh non-reused UUID,
bounded lease, acquisition ledger, or authority for later network work.

### Use Pydantic's installed JSON output as the canonical byte contract

Rejected. Dependency-version behavior is not a stable serialization version, and semantically
equivalent bytes must not silently replace exact historical evidence.

## Explicit Non-Goals

- Implementing a stream store, serialization codec, port, adapter, database, schema, migration,
  retention worker, repair command, scheduler, service, or deployment.
- Authorizing automatic create, recovery, restart, pause, resume, drift handling, provider
  failover, or external notification.
- Claiming that an attachment, compare-and-swap, `synchronous=FULL`, one host, or passing tests
  proves exactly-once requests, physical durability, capacity, availability, failover, or
  continuous-operation readiness.
- Changing existing bounded public-trade checkpoints, adapters, evidence stores, health,
  transition history, range/retry/pacing policy, or the shared request budget.
- Changing ADR 0027, ADR 0028, TASK-037 authority, an operating mode, or any financial permission.

## Review Triggers

Review this decision before:

- changing any durable field, creation-record input, canonical byte/fingerprint rule, causal
  version, attachment, hold/resume evidence, or completion proof;
- selecting a port, repository, database, schema, projection, index, retention duration,
  compaction, migration, repair, backup, or physical path;
- tightening or widening TASK-059's epoch range or relying on an epoch-microsecond projection;
- adding an outer fence, service lifecycle, trigger, scheduler, runtime, health monitor,
  notification, deployment, automatic recovery, or multi-host coordination;
- changing the bounded child, evidence-first, pending-leaf, source-health, budget, or TASK-057
  drift contracts;
- accessing operator data, adding credentials/private data, enabling execution or a higher
  operating mode; or
- claiming persistence implementation, recovery, capacity, continuous operation, deployment,
  Phase 2 completion, or risk closure.
