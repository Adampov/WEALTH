# ADR 0030: Continuous Public-Trade Stream Store Port Contract

- **Status:** Accepted
- **Date:** 2026-07-28
- **Decision owners:** Project owner, Market Data Department, Engineering Department, Security
  Department, and Audit and Assurance Department

## Task Contract

### Goal

Freeze one strict, provider-independent logical port for conceptual continuous public-trade
stream creation, exact-identity current load, versioned compare-and-swap, and bounded history
paging without selecting or implementing a physical store.

### Context

ADR 0029 defines the durable stream state, immutable creation and transition records, canonical
bytes, domain-separated digests, rolling history roots, evidence scopes, exact reload rules, and
the future trusted mutation boundary. TASK-061 implements those pure values, codecs, and link
validators. No callable storage boundary yet owns the logical atomic operations or their closed
outcomes.

The trusted mutation boundary and the logical store boundary are different. The trusted mutation
boundary must eventually perform exact reload, validate external evidence bodies and a matching
accepted attestation, sample its injected UTC clock, finalize an attachment when applicable,
construct the sole successor and immutable TASK-061 record, and then call the store. TASK-062
cannot perform those actions because it adds no clock, evidence-body access, attestation service,
runtime composition, authority, or I/O.

### Scope

This decision freezes:

- strict immutable identity, expectation, stored-envelope, stored-history-entry, command, query,
  continuation, current-view, page, receipt, result, and retry-disposition values;
- one unused logical `ContinuousPublicTradeStreamStore` protocol;
- one public `validate_continuous_public_trade_stream_audit_page` function that binds an exact
  query and page under the complete effective policy;
- store-local structural validation and atomic ownership requirements;
- exact duplicate, conflict, absence, identity-conflict, unsupported-version, corruption,
  anchor-conflict, and unavailable-storage distinctions; and
- a bounded audit state machine with 1 through 100 new records and at most one predecessor
  overlap.

### Constraints

This decision and TASK-062 add no adapter, repository implementation, production fake, database,
SQLite, DDL, schema, index, migration, physical path, configuration, filesystem or network I/O,
clock read, UUID generation, planner/finalizer call, evidence-body access, attestation decision,
outer or child fence, lease, request budget, retry loop, automatic recovery, repair, deletion,
upsert, retention or compaction rule, capacity value, durability claim, service, scheduler, CLI,
deployment, notification, credential, permission, operator data, or runtime import.

TASK-059 behavior and TASK-061 canonical bytes and digest domains do not change. Full-range epoch
milliseconds remain exact integers and are not projected to `datetime` or epoch microseconds.
TASK-037 remains blocked and authorization remains denied.

## Decision

### Boundary selection

The port is a lower-level atomic logical store boundary. It accepts finalized TASK-061 artifacts,
not time-independent transition intent.

An intent-only store port is rejected because turning intent into a record requires capabilities
that TASK-062 explicitly excludes: trusted clock sampling, ATTACH two-pass finalization,
successor construction, external evidence-body validation, accepted-attestation validation, and
runtime authority composition.

A compare-and-swap command therefore carries exactly one
`ContinuousPublicTradeStreamTransitionRecordV1` together with its exact retained canonical bytes,
digest, resulting history root, and typed evidence scopes. The record already embeds the sole
canonical successor envelope. The command has no second successor, separate successor bytes,
separate child payload, separate successor digest, or caller-selectable `recorded_at`.

Only a future trusted mutation boundary may construct a finalized creation or transition command
for runtime use. The port revalidates the supplied finalized artifact and its retained predecessor
but cannot prove the provenance of the clock, external evidence bodies, an accepted history
attestation, or runtime authority. A successful store result is never an authorization,
attestation, fence, budget grant, readiness signal, or permission to act.

### Exact public values

Every TASK-062 value is a frozen strict Pydantic model with forbidden extra fields, validated
defaults, exact built-in scalars, exact enum and UUID instances, bounded strings and bytes, and
recursive boundary revalidation. Booleans, scalar subclasses, coerced UUIDs or enums, mutable
collection substitutes, undeclared or private fields, partial bypass construction, malformed
digests, and cross-field disagreement fail closed without echoing rejected material.

`ContinuousPublicTradeStreamIdentityV1` contains:

- stream-contract version `1`;
- exact stream UUID;
- `source`, `venue`, `instrument`, `provider_symbol`, `instrument_type`, and `request_variant`;
- exact stream-policy fingerprint; and
- exact non-negative `stream_start_epoch_ms` in the TASK-059 range.

The store-local natural identity is exactly
`(source, venue, instrument, provider_symbol, instrument_type, request_variant)`. It excludes the
stream UUID, start coordinate, and policy. Those excluded fields remain immutable expected
identity but do not permit a second stream for the same feed.

`ContinuousPublicTradeStreamExpectationV1` contains the complete identity, the exact complete
effective `ContinuousPublicTradePolicy`, and an optional exact effective bounded-child policy
fingerprint. The child fingerprint is required exactly when the retained prior or proposed
successor attachment makes it applicable. Supplying it for an unattached load is also a mismatch;
the port does not normalize unknown caller expectations.

`ContinuousPublicTradeStreamStoredEnvelopeV1` retains:

- the exact TASK-061 envelope value;
- its original canonical bytes; and
- its TASK-061 envelope digest.

The bytes must equal TASK-061's exact canonical encoding, decode to the exact supplied value, and
recompute the exact digest. Decoding and re-encoding verifies the retained authority; it never
replaces or normalizes the supplied bytes.

`ContinuousPublicTradeStreamStoredCreationV1` retains:

- one exact TASK-061 creation record and its original canonical bytes;
- its exact creation-record digest;
- the exact stored successor envelope;
- the exact initial rolling history root; and
- the exact typed governed-create evidence scope.

`ContinuousPublicTradeStreamStoredTransitionV1` retains:

- one exact TASK-061 transition record and its original canonical bytes;
- its exact transition-record digest;
- the exact stored successor envelope;
- the exact next rolling history root;
- the exact typed transition-authority scope; and
- the exact typed child-completion scope only for `CHILD_COMPLETED`.

Each wrapper must match TASK-061's public encoders, decoders, digest/root functions, scope
digests, record references, embedded successor bytes, and version bindings. Full transition-link
validation that needs a predecessor occurs after the store loads that predecessor.

### Commands and queries

Create accepts one `ContinuousPublicTradeStreamCreateCommandV1` containing:

- the complete expected identity and effective stream policy; and
- one exact stored creation entry.

The command cross-binds every identity and policy field, the pristine version-one successor,
creation bytes/digest/root, create reference, and typed create scope. The child-policy
fingerprint is absent. There is no separate mutation time or successor.

Current load accepts one `ContinuousPublicTradeStreamLoadQueryV1` containing one complete
expectation. `NOT_FOUND` is possible only when both the UUID and natural identity are absent in
one coherent store view.

Compare-and-swap accepts one `ContinuousPublicTradeStreamCompareAndSwapCommandV1` containing:

- the complete expectation;
- exact `expected_version`, `expected_envelope_digest`, and `expected_history_root`; and
- one exact stored transition entry.

The three expected prior values must exactly equal the sole transition record's prior bindings.
The successor is obtained only from that record. The future store must load the retained current
and direct predecessor, validate identity and complete policy, validate the applicable child
policy, validate TASK-061's transition link and scopes, and then classify the operation. No
command field can bypass the future trusted mutation boundary with an alternate successor.

Audit has two exact query shapes:

- `ContinuousPublicTradeStreamAuditStartQueryV1` contains the complete expectation and an exact
  limit from 1 through 100; and
- `ContinuousPublicTradeStreamAuditContinuationQueryV1` adds one exact continuation containing
  stream UUID, through-version, through-envelope digest, and through-history root.

There is no unbounded read, iterator, replay, total-count request, lookahead flag, or page size
above 100.

### Logical atomic ownership

Create logically owns one atomic store-local operation:

1. establish one coherent UUID and natural-identity view;
2. if both are absent, insert exactly one current envelope and its immutable creation
   entry/root; and
3. make both uniqueness bindings visible with that same commit.

Create never replaces, merges, repairs, normalizes, or upserts. The same UUID with different
canonical material, or the same natural identity under a different UUID, is `CONFLICT`.

Compare-and-swap logically owns one atomic store-local operation:

1. match exactly one current stream by UUID, immutable identity, policy, expected version,
   envelope digest, and accepted prior history-root value supplied by the trusted caller;
2. replace exactly that current envelope with the transition record's embedded successor; and
3. append exactly that immutable transition and resulting root.

There is one winner. A missing current stream, stale version, stale digest, stale root, identity
mismatch, or concurrent winner is `CONFLICT` with no write. There is no reload-and-continue,
alternate transition, automatic retry, delete, or repair.

These are obligations for a future physical adapter. TASK-062 implements no transaction and makes
no durability, crash-recovery, or multi-host claim.

### Closed outcomes

Create outcomes are:

- `INSERTED`: the exact creation commit won;
- `DUPLICATE`: an exact retained historical creation entry matches byte-for-byte and no write
  occurred;
- `CONFLICT`: UUID or natural-identity state disagrees;
- `UNSUPPORTED_VERSION`: recognizable retained version is unsupported;
- `CORRUPT`: retained bytes, types, projections, links, roots, or current/history agreement fail;
  and
- `UNAVAILABLE`: no coherent classification can be established because storage failed.

Current-load outcomes are:

- `FOUND`;
- `NOT_FOUND`;
- `IDENTITY_CONFLICT`;
- `UNSUPPORTED_VERSION`;
- `CORRUPT`; and
- `UNAVAILABLE`.

Compare-and-swap outcomes are:

- `UPDATED`;
- `DUPLICATE`;
- `CONFLICT`;
- `UNSUPPORTED_VERSION`;
- `CORRUPT`; and
- `UNAVAILABLE`.

Audit outcomes are:

- `PAGE`;
- `AT_TAIL`;
- `NOT_FOUND`;
- `IDENTITY_CONFLICT`;
- `ANCHOR_CONFLICT`;
- `UNSUPPORTED_VERSION`;
- `CORRUPT`; and
- `UNAVAILABLE`.

Absence is never inferred from a conflict, unsupported version, malformed or inconsistent
retained data, a history gap, or storage failure. A caller-supplied well-formed continuation that
does not equal the coherent retained anchor is `ANCHOR_CONFLICT`; a missing or inconsistent
required retained predecessor is `CORRUPT`.

`DUPLICATE` is exact historical store-local replay classification. Create duplicates return the
matching creation receipt. Compare-and-swap duplicates return the matching historical transition
receipt even if a later version is currently retained. A duplicate never claims that referenced
external evidence remains valid and never authorizes action.

Successful write results contain an immutable accepted-entry receipt, not an implied current
view. `FOUND` contains a bounded current view. `PAGE` contains one bounded audit page. Other
outcomes contain no success payload, except `AT_TAIL`, which echoes the exact validated tail
continuation.

### Bounded current view

A current view always contains the exact creation entry and the exact current history entry.

At version one, both are the same creation entry and no direct predecessor is present.

At a later version, the current entry is the latest transition and exactly one directly addressed
predecessor entry is present. The predecessor successor must match the transition's prior
version/digest/root, and the transition successor/root must match current. Version two uses the
creation entry as predecessor; later versions use the preceding transition. The creation entry
remains present to bind immutable identity and complete stream policy.

This reads a constant number of stream records and does not replay history. Store-local validation
does not validate external evidence bodies or produce an accepted attestation.

### Bounded audit state machine

The first page:

- has no predecessor overlap;
- returns 1 through the requested limit new records;
- begins with the version-one creation entry;
- contains only contiguous entries from that point; and
- returns a continuation derived exactly from the final new entry.

A continuation page:

- requires the exact supplied continuation anchor;
- returns exactly one matching predecessor overlap outside the new-record count;
- returns 1 through the requested limit new transition entries beginning at anchor version plus
  one;
- validates every link, scope, version, digest, successor, and rolling root; and
- returns a continuation derived exactly from its final new entry.

If an exact continuation already addresses the validated retained tail, the result is `AT_TAIL`,
not an empty page. A first-page payload contains at most the requested limit history records. A
continuation-page payload contains at most one predecessor plus the requested limit new records,
for an absolute maximum of 101 returned history records. The protocol exposes no `has_more` value
or lookahead result; presenting the returned continuation discovers `AT_TAIL`. A conforming future
adapter must separately prove that it obtains each page without reading beyond the same bound.

A page continuation or tail is only a store-local structural anchor. It is not ADR 0029's
externally retained accepted history attestation.

### Retry disposition

Every result carries one closed descriptive retry disposition:

- `NOT_REQUIRED` for a completed store-local classification;
- `DO_NOT_RETRY` for conflicts and fail-closed disagreements; or
- `EXACT_REQUEST_ONLY` for `UNAVAILABLE`.

`EXACT_REQUEST_ONLY` is not retry authority. It means only that any separately governed future
attempt may not mutate, widen, reconstruct, backdate, or substitute the request. TASK-062 defines
no retry count, delay, loop, recovery, or permission. `CONFLICT` and `ANCHOR_CONFLICT` end the
invocation; `NOT_FOUND` never authorizes create.

## Validation Responsibility

Strict command construction rejects caller-value disagreement before a future adapter is called.
A conforming adapter must revalidate the exact command at its method boundary so bypass-created or
mutated Pydantic instances cannot reach storage.

The standalone audit-page value proves bounded shape, exact retained bytes, direct structural
links, scopes, digests, roots, and continuation construction. A conforming adapter must also call
the public `validate_continuous_public_trade_stream_audit_page` function before returning `PAGE`.
That pure query/page validator binds start versus continuation shape and limit, the exact
continuation overlap, complete expected identity and policy, and every TASK-061 transition link
using the query's effective policy. A continuation page whose overlap is a transition is never
accepted from direct linkage alone.

After loading retained material, a future adapter must use TASK-061's public validation functions
to verify exact canonical values, creation scope, complete load bindings, transition scopes,
transition link, policy agreement, predecessor time ordering, and history roots. Unsupported
versions and retained corruption remain storage outcomes rather than invalid caller-input
exceptions.

No result means that external evidence bodies were loaded, their current authority was accepted,
an attestation was created or matched, or a runtime action is allowed. Those checks remain with a
future trusted application boundary.

## Consequences

### Positive

- The physical technology decision remains open while logical ownership and outcomes are exact.
- A caller cannot supply a second successor or timestamp beside the sole canonical TASK-061
  record.
- Original canonical bytes remain authoritative and independently verifiable.
- Exact lost-acknowledgement replay is distinguishable from stale or competing mutation.
- Current reads are constant-size and audit reads have a hard 101-history-record ceiling.
- Store failures, corruption, unsupported versions, identity mismatch, and absence cannot collapse
  into one permissive result.

### Negative

- Future adapters must retain redundant typed values, original bytes, digests, roots, scopes, and
  bounded predecessor material.
- A higher trusted mutation layer is still required before any stream mutation can be constructed.
- This port alone cannot establish evidence authority, physical durability, capacity, recovery,
  or safe continuous operation.

## Alternatives Considered

### Put transition intent, clock, and finalization inside the port

Rejected. It crosses the storage boundary into runtime authority, evidence, time, and planner
ownership and conflicts with TASK-062's exclusions.

### Accept both a transition record and a separate successor envelope

Rejected. The second value creates an alternate-successor ambiguity. TASK-061's transition record
already binds the sole canonical successor bytes and digest.

### Return decoded values without original canonical bytes

Rejected. Reconstructed values cannot replace ADR 0029's retained byte authority.

### Return `None` for absence or raise one generic storage exception

Rejected. It collapses absence, identity conflict, unsupported versions, corruption, and
unavailability and could incorrectly authorize create or retry.

### Expose an unbounded history iterator

Rejected. It violates the finite audit contract and prevents a hard per-call work bound.

### Implement an in-memory store in production source

Rejected. TASK-062 freezes the port only. Deterministic fake/spy behavior belongs in focused tests.

## Explicit Non-Goals

- Selecting or implementing any physical persistence technology.
- Creating accepted attestations or validating external evidence bodies.
- Sampling time, constructing successors, finalizing attachments, or invoking a planner.
- Granting create, mutation, child, provider, budget, fence, retry, recovery, or operator
  authority.
- Enabling continuous runtime operation, private/account access, trading, leverage, execution,
  withdrawal, or notification.
- Claiming capacity, availability, durability, backup, recovery, multi-host correctness, Phase 2
  completion, or closure of any risk.

## Review Triggers

Review this decision before:

- changing a command, query, result, outcome, retry disposition,
  `validate_continuous_public_trade_stream_audit_page`, audit bound, overlap rule, or atomic
  ownership rule;
- changing TASK-059 identity or policy, TASK-061 bytes, records, digests, roots, scopes, or
  transition validation;
- selecting an adapter, repository, database, schema, projection, migration, retention,
  compaction, backup, recovery, capacity, or physical path;
- adding a trusted mutation layer, evidence/attestation integration, clock, fence, budget,
  scheduler, service, runtime, automatic retry, or recovery; or
- changing TASK-037 authorization, operating mode, permissions, deployment, or any financial
  capability.
