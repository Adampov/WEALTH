# Governed Backlog

This file records approved, bounded work. `PROJECT_STATE.json` identifies the one canonical
`next_action`; blocked work remains open without authority, and later items are directional until
promoted through review.

## Next Action

### TASK-061 — Pure versioned continuous public-trade persistence-record and canonical-codec contracts

- **Key:** `phase2.continuous_public_trade_stream_persistence_codec_contracts`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** READY
- **Human approval:** NOT REQUIRED — pure unused domain values, deterministic codecs, tests, and
  coordinated documentation/governance only; no storage, schema, runtime, operator-data,
  permission, or operating-mode change.
- **Context:** ADR-0029 defines a conceptual persistence boundary but intentionally selects no
  repository or physical schema. TASK-059's attachment fingerprint cannot reconstruct the exact
  deterministic pristine child after a crash between stream attachment and child creation, so a
  later store will need complete canonical child-creation material rather than a digest alone.
- **Goal:** Add strict, versioned, side-effect-free persistence-record and canonical-codec
  contracts that can represent ADR-0029's exact checkpoint envelope and companion child-creation
  record without performing or enabling persistence.
- **Scope:** Add one unused pure domain module and focused tests. Model the complete deterministic
  pristine-child creation payload, including an explicit fixed-UTC `created_at`, immutable
  stream/market/request identity, exact half-open attachment range, and the distinct continuous
  and bounded-child policy fingerprints. Model the versioned stream checkpoint envelope, separate
  version-one stream-creation evidence with the exact complete stream-policy projection, and
  immutable transition records with canonical successor envelope bytes plus exactly one bounded
  transition-authority reference on every stream mutation and, for `CHILD_COMPLETED`, one accepted
  child-completion-evidence reference.
  Define canonical UTF-8 JSON codecs and six distinct domain-separated SHA-256 contracts for child
  creation, stream envelope, stream creation, transition record, evidence scope, and rolling
  history root. The supplied external evidence-body digest is not recomputed by this module. Add
  strict encode/decode validation, exact round trips, and typed fail-closed outcomes for unknown
  versions or malformed, duplicate, missing, extra, inconsistent, or non-canonical input.
- **Files:** `src/wealth/domain/continuous_public_trade_persistence.py`,
  `tests/unit/test_continuous_public_trade_persistence_contracts.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, `PROJECT_STATE.json`, `BACKLOG.md`,
  `RISK_REGISTER.md`, and governance tests only.
- **Constraints:** Pure records and codecs only. Do not add a port, repository, adapter, SQLite,
  DDL, migration execution, physical schema, filesystem state, network/provider access, runtime
  import, scheduler, trigger, daemon, service runner, CLI, configuration, deployment, operator
  path/data, credential, permission, notification, automatic pause/resume/recovery/restart, or
  operating-mode change. Do not reserve or spend request budget, create or advance a job/stream,
  inspect evidence, infer authorization, or claim cross-database atomicity, physical durability,
  capacity adequacy, continuous-operation readiness, multi-host exclusivity, recovery, or Phase 2
  completion. Preserve ADR-0028, ADR-0029, TASK-059's unused status, TASK-037 authority,
  canonical-UTC migration limits, and Stage 3.

Acceptance gates:

1. The new values are frozen, provider-independent, unused, and side-effect-free. Construction
   and decoding reject booleans, polymorphic scalar subclasses, invalid fixed-UTC timestamps,
   malformed digests/identifiers, inconsistent attachment ranges, and any identity or policy
   mismatch without normalization or fallback.
2. The companion child-creation record contains every deterministic pristine-child input needed
   after reopen, including one explicit canonical `created_at`; its domain-separated digest is
   computed from the exact canonical bytes. The stream and child policy fingerprints remain
   distinct and are both bound explicitly.
3. Record/model and serialization versions are explicit. Canonical output is compact,
   key-sorted, duplicate-free UTF-8 JSON with no BOM or trailing newline and with deterministic
   six-fractional-digit fixed-UTC and integer encoding. Embedded envelope bytes use even-length
   lowercase hexadecimal only; exact decoded canonical bytes and all six domain-separated
   digests round-trip unchanged.
4. Decoding rejects an outer record over 65,536 bytes before UTF-8/JSON work. Canonical
   `child_creation_payload` and decoded stream envelopes are capped at 8,192 and 16,384 bytes;
   `successor_envelope_hex` is even lowercase ASCII capped at 32,768 characters. Other canonical
   escaped JSON string tokens are capped at 8,192 lexical bytes. Each document permits at most 16
   nesting levels, 128 total object members, fixed ASCII keys of at most 64 characters, and 19
   decimal digits per integer token. These are parser-safety limits, not operational-capacity
   evidence. Maximal valid current model values, including control and maximal astral identifiers
   plus a maximal attached envelope, must fit without contract narrowing; exact-limit and
   limit-plus-one fixtures are required for every byte bound. Raw decode/depth failures map to one
   typed sanitized boundary, and unknown versions, duplicate keys, missing or extra keys, invalid
   UTF-8/JSON, non-canonical bytes, unsupported numeric forms, and cross-record inconsistencies
   fail before an accepted value is produced.
5. The checkpoint envelope preserves ADR-0029's exact stream identity, policy identity, monotonic
   version, exact epoch-millisecond cursor, optional immutable attachment, and complete creation
   material only. A separate stream-creation record retains exact version-one envelope bytes, the
   complete validated stream-policy projection including its caller-supplied fingerprint, and the
   governed-create reference. Decode/load validation rejects any field-level policy disagreement
   even when a caller reuses the same fingerprint. Each immutable transition record binds the
   prior digest and rolling history root, exact canonical successor-envelope bytes and digest,
   fixed-UTC command time, and typed bounded authority/completion references whose exact
   kind-specific scope digest binds the transition. Every stream mutation requires exactly one
   `STREAM_TRANSITION_AUTHORITY` reference; `CHILD_COMPLETED` additionally requires exactly one
   `CHILD_COMPLETION` reference. Create scope additionally binds the complete stream-policy
   projection; transition scope requires it to be null. To avoid a pre-time digest cycle, ATTACH
   transition authority binds exact prior version, envelope digest, accepted history root,
   successor version, candidate child UUID, and effective child-policy fingerprint while successor
   digest and child-creation fingerprint are exactly null. The finalized transition/root binds the
   resulting exact bytes; every other transition authority binds the exact prior history root and
   successor digest, and child-completion scope binds that root plus the exact child ID, policy, and
   creation fingerprints. Canonical reason scope is required for `RETAIN` and `MANUAL_HOLD`, equals
   the held checkpoint reason for `MANUAL_HOLD`, and is null for every other transition. Creation
   and chained-transition history
   roots must reproduce exactly. Historical validity is evaluated at the recorded command time;
   later expiry grants no new authority. Neither a pause reason nor completed child ID alone is
   authority or proof.
6. Golden-byte, exact-digest, round-trip, boundary, hostile, mutation, and property tests are
   deterministic, offline, secret-free, and prove rejection without I/O. TASK-059 behavior and
   every existing bounded-job contract remain unchanged. Full-range unattached TASK-059 epoch
   milliseconds round-trip exactly, while an attachment whose exact child payload is outside the
   existing bounded-child datetime range fails closed without silently tightening TASK-059. A
   pure two-pass planner proof uses one identical fixed-UTC instant as planner `now`, child
   `created_at`/`updated_at`, and transition `recorded_at`, starts only with a fixed in-memory
   all-zero provisional fingerprint, recomputes with the real child-payload digest, and requires
   exact non-fingerprint plan equality; the provisional value is never persisted or used for
   action and is not claimed to be outside the real digest space. Tests also reject cross-domain
   digest substitution, illegal ATTACH/non-ATTACH scope nullability or transplant, reason-scope
   mismatch, evidence kind/outcome/scope mismatch, equal/reversed validity intervals, history-root
   substitution, regressing record time, and an ATTACH child time unequal to its transition time.
   The pure module carries recorded time but makes no trusted-clock or authority claim.
7. The diff adds no port/repository/adapter, SQLite/DDL/migration execution/physical schema,
   runtime or network path, dependency or lockfile, scheduler/service/deployment, operator data,
   credential, permission, automatic action, capacity value, or readiness claim. TASK-037 remains
   blocked and authorization remains denied.
8. Documentation, governance assertions, formatting, lint, strict typing, focused and complete
   tests, lockfile verification, dependency audit, health slice, and CI pass.

## Blocked, Awaiting Owner-Supplied Restricted Inputs

### TASK-037 — Operator-preflight authorization package and project-owner decision

- **Key:** `phase2.canonical_utc_preflight_operator_authorization_package_owner_decision`
- **Phase:** 2 — Reliable Market Data Platform
- **Risk tier:** RISK 3 — PRODUCTION AFFECTING (authorization decision only)
- **Status:** BLOCKED
- **Human approval:** REQUIRED — project owner plus independent Risk and Security review.
- **Blocking condition:** Owner-supplied exact restricted-package inputs are not available in a
  Security-approved governance location; independent Risk and Security reviews and the
  project-owner decision therefore remain unperformed. Authorization remains `DENIED`.
- **Resume condition:** Return TASK-037 to `READY` only after the exact required inputs are
  supplied through the approved restricted boundary; repository placeholders or TASK-038
  completion cannot satisfy this condition.
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

### TASK-060 — Continuous public-trade stream persistence-contract decision

- **Key:** `phase2.continuous_public_trade_stream_persistence_contract_decision`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `docs/decisions/0029-continuous-public-trade-stream-persistence-contract.md`,
  `docs/decisions/README.md`, `README.md`, `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`,
  `PROJECT_STATE.json`, `BACKLOG.md`, `RISK_REGISTER.md`, and governance tests only.
- **Result:** ADR-0029 now records the design-only persistence contract for a possible future
  single-host continuous public-trade stream. The exact TASK-059 checkpoint is the durable current
  state; planning results, service-run state, fences, bounded-job control, market evidence,
  source health, request-budget state, and invocation-local actors remain separate domains.
  Conceptual creation, exact-identity load, and versioned compare-and-swap transitions fail closed
  on missing, unknown, corrupt, stale, or conflicting state and never authorize action by
  themselves.

  A committed attachment must bind the complete canonical deterministic `child_creation_payload`,
  including a fixed-UTC creation time, because TASK-059's SHA-256 creation fingerprint is
  intentionally non-invertible. This new evidence payload does not redefine the existing
  bounded-child store serializer. The continuous-stream and bounded-child policy fingerprints stay
  distinct. A pause reason is not authority evidence, and a completed child ID is not accepted
  completion proof. ADR-0029 preserves bounded external actor/governance references, accepted child
  completion evidence, exact bounded-child recovery, evidence-first ordering, one shared durable
  pre-request budget, and fresh independent UUID fences without moving those controls into the
  current checkpoint.

  The decision pins canonical versioned serialization plus six distinct child, stream-envelope,
  stream-creation, transition-record, evidence-scope, and rolling-history-root digest contracts.
  Separate creation/transition records retain successor-envelope bytes and the complete immutable
  stream-policy projection; typed external evidence scopes, trusted non-regressing command time,
  and an anchored history attestation prevent current load/planning alone from authorizing work.
  ATTACH authority binds the exact prior version, envelope digest, accepted history root, successor
  version, candidate child, and effective child-policy fingerprint before time sampling while the
  finalized transition/root binds its time-dependent successor. Only a separately validated
  non-integrity operational hold can preserve admission of an already-returning in-flight response;
  drift, invalid payload, quality/evidence failure, corruption, or ambiguity stops canonical
  admission and progress.
  A pure two-pass plan/payload/replan proof resolves TASK-059's fingerprint-before-range API without
  persisting its provisional value. ADR-0029 enumerates crash seams around attachment, child
  creation, request reservation, evidence, child checkpointing, completion, stream advancement,
  manual hold, and governed resume and requires exact reload after an unknown commit outcome. It
  specifies compatibility, quarantine, migration and restore prerequisites, causal retention, and
  disable-to-current-bounded-flow rollback before a physical repository is selected. TASK-059
  epoch milliseconds remain exact even where they cannot be represented as Python datetimes or
  signed-64-bit epoch microseconds.

  No production source, codec, port/repository/adapter, SQLite/DDL/migration/schema, filesystem
  state, runtime or network path, dependency, scheduler/service/deployment, operator data,
  credential, permission, automatic action, capacity, durability, recovery, multi-host,
  continuous-operation, readiness, or Phase 2 claim was added. TASK-061 is the separately governed
  pure-record and canonical-codec increment. TASK-037 remains blocked and authorization remains
  denied.

### TASK-059 — Pure continuous public-trade closed-window planner and lifecycle contracts

- **Key:** `phase2.continuous_public_trade_closed_window_planner_contracts`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/domain/continuous_public_trade.py`,
  `tests/unit/test_continuous_public_trade_contracts.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, `PROJECT_STATE.json`, `BACKLOG.md`,
  `RISK_REGISTER.md`, and governance tests only.
- **Result:** One provider-independent, frozen, side-effect-free domain boundary now exposes
  `ContinuousPublicTradePolicy`, `ContinuousPublicTradeAttachment`,
  `ContinuousPublicTradeStreamCheckpoint`, `ContinuousPublicTradePlan`, their explicit stream,
  service, plan, and transition enums, `plan_continuous_public_trade_window`, and pure stream and
  service transition validators. It accepts no clock implicitly and performs no I/O.

  Policy validation requires exact built-in integer millisecond values, rejects booleans and
  non-positive, misaligned, or out-of-range work limits, requires the finite catch-up span to be a
  whole number of windows, and preserves a complete lowercase SHA-256 policy fingerprint. Stream
  validation preserves immutable stream and market identity, the exact request variant, policy
  fingerprint, epoch-millisecond start and cursor, monotonic version, whitespace-free manual pause
  evidence, and attachment range consistency. Pure transition validators allow only explicit
  `RETAIN`, `ATTACH`, `CHILD_COMPLETED`, `MANUAL_HOLD`, and `MANUAL_RESUME` stream transitions and
  the finite service path `STARTING` to `RUNNING` to one terminal status. They reject unknown,
  version-skipping, identity-changing, fingerprint-changing, cursor-regressing,
  attachment-widening, or causally invalid transitions without mutating either value.

  The pure closed-window planner accepts one validated checkpoint and one explicit
  `datetime.UTC` instant. A paused stream returns only `HELD` while preserving its cursor and any
  attachment. An active stream with an existing attachment returns only `ATTACHED_JOB` with that
  exact immutable child identity, policy fingerprint, and half-open range. A caught-up stream
  returns only `WAITING`. Otherwise the planner returns one `ATTACHED_JOB` candidate beginning
  exactly at the durable cursor and ending at
  `min(latest_eligible_end, cursor + max_catchup_span)`. Eligibility is epoch-aligned in exact
  whole UTC milliseconds after the configured non-negative settlement lag, so no result rounds a
  cursor forward, includes an open/future window, overlaps prior work, skips a gap, widens an
  existing attachment, or exceeds the finite catch-up bound.

  These contracts are unused. They do not create, attach, persist, start, claim, invoke, schedule,
  retry, pause, resume, recover, or supervise a real job or stream. No existing runtime
  composition imports the module. The work adds no repository/adapter, SQLite or schema, network
  or provider access, wait/sleep or request-budget behavior, trigger, scheduler, daemon, service,
  CLI, dashboard, deployment, configuration loading, operator path/data, credential, permission,
  notification, dependency, lockfile, or automatic action. It establishes no cross-database
  atomicity, physical durability, capacity adequacy, multi-host exclusivity, continuous-operation,
  recovery, deployment, or Phase 2 readiness. ADR-0028 remains unchanged; TASK-060 was the
  separate design-only persistence-contract decision and is now complete under ADR-0029; and
  TASK-037 remains blocked with authorization denied.

### TASK-058 — Continuous public-trade collection operating-contract decision

- **Key:** `phase2.continuous_public_trade_collection_operating_contract_decision`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `docs/decisions/0028-continuous-public-trade-collection-operating-contract.md`,
  `docs/decisions/README.md`, `README.md`, `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`,
  `PROJECT_STATE.json`, `BACKLOG.md`, `RISK_REGISTER.md`, and governance tests only.
- **Result:** ADR-0028 accepts one conceptual single-host composition for a possible future
  continuous public-trade collector: an unselected external trigger may invoke a finite-run
  coordinator around the existing explicitly invoked bounded orchestrator only after separately
  governed implementation and deployment. The future coordinator would own continuous stream
  selection, an immutable attached child, and an outer fresh-UUID fence; the existing child keeps
  its independent fence, exact pending leaf, evidence-first checkpoint, idempotent refetch,
  causal transition/source-health evidence, adaptive finite work, and the one shared durable
  single-host request-budget gate.

  The decision separates three conceptual layers without adding serialized state. Durable stream
  control is `ACTIVE` or `PAUSED`; schema drift is a scoped pause reason, while enablement remains
  an external disabled-by-default posture. A finite service run moves from `STARTING` to `RUNNING`
  and exactly one of `STOPPED`, `PAUSED`, `FAILED`, or `RUN_LIMIT`. The existing bounded job keeps
  `PENDING`, `RUNNING`, `PAUSED`, `FAILED`, and `COMPLETED`; `waiting`, `caught_up`, and
  `work_limit_reached` are outcomes rather than lifecycle states. A clean bounded-job `PAUSED` keeps the
  stream `ACTIVE` and its exact attachment, while bounded-job failure, conflict, lost lease,
  corruption, or drift fails the service and requires a manual stream pause. A clean service stop
  leaves stream state, cursor, and attachment unchanged.

  The contract requires closed epoch-aligned half-open UTC windows, explicit settlement lag, a
  durable cursor, bounded catch-up and per-invocation work, no overlap or gap, cooperative clean
  stop and shutdown checks, and no self-scheduling state. A reopen must use fresh outer and child
  authority, retain the immutable child, finish its exact pending leaf first, and advance the
  continuous cursor only after accepted evidence and exact child completion.
  It does not claim cross-database atomicity, crash-durable per-job attempt reservations, physical
  durability, automatic recovery, or multi-host exclusivity.

  Suspected schema drift remains a manual exact-variant or inseparable-parser hold under the
  TASK-057 runbook. Governed resume requires contract review, synthetic evidence, complete
  regressions, rollback, and applicable authority; no detector, automatic pause, remediation, or
  resume was added. Source health remains causal to bounded child work, while future service
  health would separately distinguish waiting/caught-up, stale, paused, drift-held, work-limited,
  stopped, and failed observations with bounded audit correlation. The ADR identifies operator
  decisions, escalation, finite internal-alert evidence, complete provider/cadence/backlog/range/
  shared-budget/storage/control/recovery capacity inputs and measurements, failure dispositions,
  disable-to-current-bounded-flow rollback, review triggers, and the deterministic, operational,
  deployment, and rollback evidence required before implementation.

  This work changes documentation and governance only. It adds no production source, runtime,
  scheduler, service, network, persistence or SQLite schema, dependency or lockfile, deployment,
  operator path/data, credential, permission, notification, automatic action, or readiness claim.
  The selected future component is not implemented, enabled, deployed, scheduled, monitored, or
  capacity-approved. TASK-059 is a separately governed pure-contract increment; TASK-037 remains
  blocked and authorization remains denied.

### TASK-057 — Versioned public-provider schema fixtures and drift-response runbook

- **Key:** `phase2.versioned_public_provider_schema_fixtures_and_drift_runbook`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `tests/fixtures/public_provider_schema/v1/manifest.json`, exactly five synthetic JSON
  fixtures in that directory, `tests/unit/test_public_provider_schema_fixtures.py`,
  `docs/runbooks/PUBLIC_PROVIDER_SCHEMA_DRIFT.md`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, plus coordinated governance files and governance tests.
- **Result:** One strict version-1 manifest maps exactly five unique active request identities
  one-to-one to five minimal, bounded, secret-free synthetic fixture files with relative paths,
  exact-byte SHA-256, a 1,024-byte per-fixture maximum, provider/dataset/market/request identity,
  shape metadata, official contract reference, UTC review date, and reviewed status. The corpus
  covers Binance Spot and USD-M 12-position candle rows, Coinbase Exchange Spot six-position
  candle rows, and Binance Spot and USD-M aggregate-trade objects. Both aggregate variants use
  the existing shared parser contract: required fields are exactly `T`, `a`, `f`, `l`, `m`, `p`,
  and `q`, and optional fields are exactly `M` and `nq`; the Spot fixture contains `M`, while the
  USD-M fixture contains `nq`. Fixture presence does not invent a market-specific parser rule,
  and unknown fields remain rejected.

  Offline deterministic HTTP stubs and fixed UTC clocks feed every fixture's exact bytes through
  its active existing production adapter and request path. Tests pin the request variant,
  canonical values, provider identity, UTC/event-time behavior, and exact raw-byte lineage without
  a network call. Strict manifest tests reject unknown or missing keys, invalid versions/statuses
  or types, duplicate identities/paths, absolute/traversing/mislocated paths, extra or missing
  corpus files, digest mismatch, and oversized fixtures. Representative detectable adapter drift
  covers positional width minus/plus one, selected detectable reorder, wrong numeric types,
  invalid decimal values, missing required fields, invalid present optional-field values, and
  unknown fields through the existing non-retryable `INVALID_PAYLOAD` boundary without partial
  raw or canonical evidence.

  Decimal precision alone has no adapter-level bound, and some same-typed semantic positional
  reorder can remain canonically valid; either may be accepted. The tests and documentation retain
  that limitation explicitly: parser acceptance is not compatibility evidence, and either
  unreviewed change requires fail-closed pause and official-contract review. No rounding,
  normalization, parser widening, or provider-contract change was added.

  The new fixture module passes 54 tests; the focused fixture-plus-adapter regression passes 216,
  and the governance slice passes 20. An initial independent 42-mutant audit killed 25, classified
  nine as equivalent because exact metadata and redundant guards already rejected the mutation,
  and exposed eight real test gaps. After adding isolated entry-count, symlink, valid-oversize,
  non-finite fixture, corpus-shape, and cross-variant optional-field evidence, a targeted re-audit
  killed all 15 of 15 gap and sanity mutants with zero survivors or harness errors. The complete
  suite passes 1,704 tests; lockfile, formatting, lint, strict typing, dependency audit, and local
  health checks also pass.

  The linked runbook defines signals/classification, manual pause and containment, safe evidence
  handling, official-document re-review, synthetic versioning without overwriting old versions,
  regression commands, escalation, governed resume gates, and rollback. Real payload content is
  not copied into repository files, logs, issues, or fixtures; any real evidence requires an
  approved secret-free handling boundary. The work adds no production source, network, runtime,
  schema, dependency, operator path/data, credential, permission, automatic detection/pause/
  remediation/resume, continuous collector, deployment, or readiness claim. TASK-037 remains
  blocked and authorization remains denied.

### TASK-056 — Deterministic public-trade disconnect, sparse-window, and restart-recovery drill evidence

- **Key:** `phase2.public_trade_disconnect_sparse_window_restart_recovery_drill`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `tests/integration/test_recoverable_public_trade_collection.py`, `README.md`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, plus coordinated governance files and
  governance tests.
- **Result:** One new deterministic generated-fixture integration case composes the existing
  public-trade evidence, checkpoint, and shared rate-budget SQLite adapters across a process-style
  reopen. Worker A receives exactly two scripted retryable `HttpTransportError` outcomes, records
  one 0.125-second retry without wall-clock sleep, writes no market evidence, and reaches
  `FAILED` checkpoint version 3 with `UNAVAILABLE` health, `provider_unavailable`,
  `attempts_exhausted`, two source requests, one trace, one retry, the original cursor, and the
  exact first one-millisecond pending leaf. Hostile upstream detail is absent from durable
  checkpoint, health, and transition text.

  Worker B uses newly constructed adapters on the same three generated databases, the unchanged
  policy fingerprint, and a fresh UUID fence. It finishes the pending leaf first and then the
  remaining range in two bounded invocations from exact empty, one-valid-trade, and empty
  one-millisecond responses. Completion is checkpoint version 6 with five lifetime source
  requests, four traces, one retry, three completed windows, one canonical record, three raw
  captures, and zero conflicts. The exact six transition statuses are `PENDING`, `RUNNING`,
  `FAILED`, `RUNNING`, `RUNNING`, and `COMPLETED`; actors are absent, absent, worker A, absent,
  worker B, and worker B; matching health exists only at versions 3, 5, and 6.

  Across both workers, five unique granted durable reservations precede five provider attempts.
  The combined sleeper evidence is one 0.125-second retry plus two 0.25-second pacing waits.
  A completed rerun performs zero range invocations and leaves checkpoint, transition, health,
  budget, HTTP, evidence, and sleeper observations unchanged. The focused recovery integration
  file passes 14 tests, previously 13. Only test helpers and the new case changed; no production
  defect or production-source change was found. An isolated mutation audit killed all 30 of 30
  mutants with zero survivors and zero harness errors. It covered budget ordering and bypass,
  retry count and delay, pending-leaf and response chronology, adapter and UUID-fence reuse,
  failure/status/version/health/actor drift, both pacing waits, empty raw-capture admission,
  hostile-detail persistence, and completed-rerun work. The complete suite passed 1,650 tests;
  lockfile, formatting, lint, strict typing, dependency audit, and local health checks also passed.
  The drill uses no network, wall-clock sleep, operator path, operator data, or credential and
  makes no cross-database-atomicity, physical-durability, continuous-operation,
  automatic-recovery, or Phase 2 exit claim. TASK-037 remains blocked and authorization remains
  denied.

### TASK-055 — Fail-closed bounded public-HTTP response-header projection

- **Key:** `phase2.fail_closed_public_http_bounded_response_header_projection`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After the existing one-byte-sentinel body read and body-size decision, successful and
  `HTTPError` responses now use one shared bounded header snapshot before `HttpResponse`
  construction. Each path calls `headers.items()` once, starts its iterator once, requests no
  length hint, performs no direct message iteration or second pass, and pulls at most 101 times.
  Zero through 100 yielded pairs are accepted only while cumulative
  `len(name) + len(value)` is at most 65,536 Python characters. A yielded 101st pair fails before
  the pair is unpacked or either component is inspected; a 65,537th cumulative character fails
  immediately. Both limits raise exact sanitized
  `HttpTransportError("public HTTP response headers exceeded the configured limit")` without a
  partial response, retry, second body read, or later pull.

  Accepted order, duplicate names, original casing, empty strings, leading and trailing content,
  punctuation, Unicode, and `Retry-After` behavior remain exact. Body-read failure and body
  oversize retain precedence without header access. A successful-response limit failure has no
  direct cause or hidden context and exits its response context once. An `HTTPError` limit failure
  retains the originating provider error as both direct cause and active context, then attempts
  cleanup exactly once; a cleanup failure cannot replace it. Exceptions from `headers.items()`,
  iterator creation, and consumed pulls remain the same raw objects. On `HTTPError`, such a raw
  failure retains only the natural implicit provider-error context rather than being wrapped.
  Existing acquisition/read/protocol mappings, subclass identity boundaries, redirect behavior,
  cleanup-only mappings, and primary-failure precedence remain unchanged.

  Forty-one new deterministic cases bring the focused adapter suite from 518 to 559 tests. They
  cover zero, one, and 100 pairs; finite and endless 101st yields; pair-unpack poisoning; exact
  65,536 and immediate 65,537 character boundaries across names, values, cumulative Unicode, and
  multi-pair input; one-pass instrumentation; exact preservation; every header-origin seam; both
  body-precedence outcomes; both response paths; cause/context identity; and cleanup precedence.
  An isolated mutation audit killed all 24 of 24 mutants with zero survivors and zero harness
  errors. It covered removed or changed count and character limits, 101st-pair guard ordering,
  `>=` drift, omitted name or value volume, non-cumulative and UTF-8-byte counting, full
  materialization, second or direct iteration, projection before body read or size decision,
  success/error-path drift, normalization, reordering, wrong message or cause, cleanup replacing
  the primary failure, raw-origin wrapping, and `HTTPError`/`OSError` subclass-identity bypass.
  The complete suite passed 1,649 tests; lockfile, formatting, lint, strict typing, dependency
  audit, and local health checks also passed.

  This is only an adapter-controlled projection bound after standard-library parsing and prior
  allocation. It does not bound wire-header bytes, parser work or memory, total response or process
  memory, total wall-clock time, or provider work and adds no privacy, redaction,
  content-type/length/encoding, allowlist, hostname, DNS, IP-routability, or SSRF guarantee. No
  request, retry, endpoint, provider, dependency, runtime, credential, permission, TLS/proxy,
  TASK-037 authority, migration, or Stage 3 behavior changed.

### TASK-054 — Fail-closed public-HTTP maximum timeout policy

- **Key:** `phase2.fail_closed_public_http_maximum_timeout_policy`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/ports/http.py`, `src/wealth/adapters/http.py`,
  `src/wealth/adapters/binance.py`, `src/wealth/adapters/coinbase.py`,
  `src/wealth/adapters/binance_order_flow.py`, `tests/unit/test_http_adapter.py`,
  `tests/unit/test_binance_public_candles.py`,
  `tests/unit/test_coinbase_public_candles.py`,
  `tests/unit/test_binance_public_aggregate_trades.py`, `docs/contracts/MARKET_DATA.md`, plus the
  coordinated governance files and governance tests.
- **Result:** One provider-independent
  `MAX_PUBLIC_HTTP_TIMEOUT_SECONDS = 120.0` constant now governs the shared client and the Binance
  candle, Coinbase candle, and Binance aggregate-trade constructors. At all four boundaries, the
  new comparison follows TASK-041's finite-positive check. `NaN`, positive or negative infinity,
  zero, and negative values therefore retain exact context- and cause-free
  `ValueError("timeout_seconds must be finite and positive")`; an otherwise valid finite-positive
  value greater than 120 raises exact context- and cause-free
  `ValueError("timeout_seconds must be at most 120")`. The shared client rejects over-limit values
  before URL length or content, query, `Request`, opener, DNS, network, or filesystem work. Each
  provider constructor rejects them before endpoint validation, clock, query, injected HTTP,
  provider, or record work. Forty new deterministic cases bring the four focused files from
  640 to 680 tests: `test_http_adapter.py` has 518 tests (+9), Binance candle has 49 (+11),
  Coinbase candle has 54 (+9), and Binance aggregate-trade has 59 (+11). The over-limit corpus
  covers the next float above 120, 120.0001, the largest finite float, and a 1,001-digit integer at
  every boundary. Accepted identity coverage pins integer 1, fractional 0.25, the exact 10.0
  default, the next float below 120, exact built-in integer and float 120, and a float subclass at
  120. All five active provider request paths forward the configured accepted object unchanged,
  including subclass identity. An isolated mutation audit killed all 14 of 14 mutants with zero
  survivors and zero harness errors, covering a removed or changed cap, `>=` off-by-one, reordered
  finite validation, hardcoded or unshared module policy, a missing provider cap, float-subclass
  coercion or identity loss, wrong message, injected cause, late shared or provider validation,
  provider clock work before the cap, default drift, and forwarded-timeout drift. The task adds no
  exact numeric-type, subclass,
  coercion, rounding, unit-conversion, fallback, or total-wall-clock policy and does not separately
  bound DNS, multiple operations, caller/provider work, retries, waits, pacing, or rate budgets.
  URL/query/User-Agent, response body, header projection, redirect, cleanup, provider, endpoint,
  dependency, runtime, credential, permission, hostname/DNS/IP/SSRF, TASK-037 authority,
  migration, and Stage 3 behavior remain unchanged. Successful and `HTTPError` header projection
  still has no adapter-level pair-count or cumulative-character bound; TASK-055 governs that
  residual response-metadata risk.

### TASK-053 — Fail-closed public-HTTP bounded User-Agent validation

- **Key:** `phase2.fail_closed_public_http_bounded_user_agent_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After preserving `max_response_bytes` validation and its first precedence, the
  shared client now validates `user_agent` during construction by requiring an exact built-in
  `str`, then a length of 1 through 256 Python characters, then only inclusive visible ASCII
  U+0020 through U+007E. Every violation raises the exact context- and cause-free
  `ValueError("user_agent must be a built-in string of 1 to 256 visible ASCII characters")`
  before URL, query, `urlencode`, `Request`, private-opener, handler, DNS, network, or filesystem
  work. Exact-type rejection dispatches no caller string hooks; empty and 257-character values
  fail before character inspection. Sixty-six new deterministic cases within the 509-test
  adapter suite cover twelve invalid-response-limit precedence combinations; five invalid types,
  including a hostile string subclass; both invalid length boundaries; all 32 C0 controls, DEL,
  five representative non-ASCII characters, and two lone surrogates; one independent audit
  sweeping DEL through every position 0 through 255 in a maximum-length value; four accepted
  visible-ASCII cases covering lengths 1, 255, and 256 plus the complete visible-ASCII range; the
  exact 29-character default
  `"WEALTH/0.1 public-market-data"`; and one exact custom-header preservation path. One-character
  and 256-character values and the complete U+0020-through-U+007E range retain identity. An
  accepted custom value containing leading and trailing spaces and punctuation is forwarded
  exactly once as the sole `User-Agent` header while GET, `Accept`, URL, bounded sorted query,
  timeout, one bounded response read, one acquisition, redirect, error, cleanup, and direct-cause
  behavior remains unchanged. No value is normalized, trimmed, truncated, repaired, retried,
  replaced, redacted, or synthesized, and no privacy or total-header-block guarantee is made. No
  URL/query policy, hostname/provider allowlist, DNS/IP/public-routability or SSRF claim, IDNA,
  certificate, TLS/proxy, endpoint, dependency, provider, runtime, TASK-037 authority, migration,
  or Stage 3 behavior changed. Finite positive public-HTTP timeouts remain without an upper bound;
  TASK-054 governs that residual per-operation wait risk.

### TASK-052 — Fail-closed public-HTTP initial-target length bound

- **Key:** `phase2.fail_closed_public_http_initial_target_length_bound`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After finite-positive timeout validation and at the first line of the private
  initial-target validator, the shared client now measures the original string with
  non-polymorphic `str.__len__`. A target longer than 8,192 Python characters raises the exact
  context- and cause-free `ValueError("url must contain at most 8192 characters")` before literal
  membership or character scanning, `urlsplit`, hostname, username, port, or NFKC inspection,
  query access or serialization, `Request`, private-opener or handler work, DNS lookup, network
  access, or filesystem access. Lying-length and raising-length/content `str` subclasses prove
  that the true built-in string length is used without dispatching to caller overrides. Length
  intentionally precedes TASK-049 structure and TASK-050 port errors for an oversized target,
  while every target at or below the limit retains TASK-049 structure, parser-context suppression,
  TASK-050 port, and TASK-051 query precedence and exact errors. ASCII and multi-byte Unicode
  targets of exactly 8,192 characters preserve every original character through the existing
  sorted query, GET, `Accept`, `User-Agent`, timeout, one bounded response read, and one
  acquisition; corresponding 8,193-character targets fail without query or request work. An
  exact-limit valid target still reaches the existing query boundary. Nineteen new deterministic
  cases within the 443-test adapter suite cover all five invalid timeouts; two adversarial
  oversized subclasses and one exact-limit false-long subclass; three oversized combined-error
  forms; three exact-limit prior-error forms; ASCII and Unicode exact-8,192 and 8,193 boundaries;
  and exact-limit query precedence. The five active
  provider defaults are pinned to their exact unchanged lengths of 39, 42, 42, 45, and 48
  characters and remain accepted. The control counts Python characters rather than encoded bytes
  and makes no request-line compatibility or total-wall-clock claim. No target is normalized,
  truncated, repaired, retried, replaced, or redirected, and no URL-content, hostname, DNS, IP,
  SSRF, IDNA, TLS/proxy, endpoint, dependency, provider, runtime, TASK-037 authority, migration, or
  Stage 3 behavior changed. The configured User-Agent remains unbounded and without an exact
  runtime type or character policy; TASK-053 governs that residual request-construction risk.

### TASK-051 — Fail-closed public-HTTP bounded query serialization

- **Key:** `phase2.fail_closed_public_http_bounded_query_serialization`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After timeout, TASK-049 structural-target, and TASK-050 target-port validation and
  before `urlencode`, the shared client now takes one bounded query snapshot. It calls `items()`
  and starts its iterator once; it does not call `len(query)`, directly iterate the mapping, start
  a second item pass, or request a length hint, and it pulls at most 33 yielded items. It accepts zero
  through 32 exact built-in tuple pairs whose keys and values are exact built-in strings and whose
  combined key-plus-value length is at most 8,192 Python characters. A 33rd item, invalid pair
  shape or tuple subclass, non-string or string-subclass component, or 8,193rd character raises
  the exact context- and cause-free
  `ValueError("query must contain at most 32 built-in string pairs totaling at most 8192 characters")`.
  Rejection performs no `urlencode`, `Request`, private-opener, handler, DNS, network, or
  filesystem work and never partially serializes, repairs, or retries a query. Caller-originated
  failures, including `ValueError`, from `items()`, iterator creation, and the first, later, or
  33rd pull remain the same raw objects. Forty-two new deterministic cases within the 424-test
  adapter suite cover zero, one, and 32 pairs; finite and synthetic-unbounded 33rd items;
  count rejection before 33rd-item inspection;
  cumulative, key-only, value-only, and Unicode exact-8,192 and 8,193 character boundaries; nine
  invalid pair/type forms; five mapping-failure seams with both a custom runtime error and raw
  `ValueError`; all three earlier precedence boundaries; duplicate and content preservation
  through one sorted standard-library encoding; and all five active provider request variants
  with three through six pairs. Accepted request text, GET, `Accept`, `User-Agent`,
  timeout, response limit, HTTP-error, redirect, cleanup, and direct-cause behavior remains
  unchanged. The boundary adds no query-content, normalization, or multi-value policy and makes no
  total-wall-clock, hostname, DNS, IP-routability, or SSRF claim. The original initial URL text
  still has no configured size bound; TASK-052 governs that residual finite-work risk. Provider
  endpoints, dependencies, runtime wiring, TASK-037 authority, migration, and Stage 3 remain
  unchanged.

### TASK-050 — Fail-closed public-HTTP standard HTTPS target-port policy

- **Key:** `phase2.fail_closed_public_http_standard_https_target_port_policy`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, plus the coordinated governance files and governance tests.
- **Result:** After TASK-049 structural validation and before any query-mapping operation, the
  shared client now accepts only an omitted caller target port or an explicit numeric port that
  parses as 443. Structurally valid explicit ports 1, 80, 442, 444, 8,443, and 65,535, a
  zero-padded nonstandard port, and nonstandard IPv6 and IPvFuture ports raise the exact
  `ValueError("url must use the standard HTTPS target port")` with no direct cause or hidden
  context. They perform no query access or serialization, `Request` construction, private-opener
  work, DNS lookup, network access, or filesystem-handler work. Because the policy follows the
  complete structural validator, malformed, percent-encoded, empty, non-numeric, signed,
  Unicode-digit, zero, and greater-than-65,535 ports retain TASK-049's exact structural error and
  precedence. Implicit port 443 and explicit numeric 443, including zero-padded, mixed-case,
  IPv6, and IPvFuture forms, preserve the exact original URL text, sorted query, GET method,
  `Accept`, `User-Agent`, finite-positive timeout, one bounded read, and one acquisition. Tests
  also prove all five active provider default endpoints remain accepted. The policy constrains
  only the caller's target authority: a configured proxy peer may use a non-443 port, and default
  proxy and TLS behavior remains unchanged. No provider or hostname allowlist, DNS resolution,
  IP/public-routability or SSRF guarantee, IDNA policy, certificate pin, endpoint, dependency,
  provider behavior, response mapping, cleanup rule, runtime wiring, TASK-037 authority,
  migration, or Stage 3 behavior changed. Query serialization remains unbounded in item count and
  string volume; TASK-051 governs that residual finite-work risk.

### TASK-049 — Fail-closed public-HTTP initial request-target validation

- **Key:** `phase2.fail_closed_public_http_initial_request_target_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, `docs/ROADMAP.md`, and the coordinated governance files and
  governance tests.
- **Result:** After finite-positive timeout validation and before any query-mapping operation, the
  shared client now validates the original initial target as an absolute credential-free HTTPS
  URL with a non-empty CPython-parser hostname. It rejects every literal `?`, `#`, or backslash;
  every C0 or DEL control, Unicode whitespace character, and lone surrogate code point; relative,
  scheme-relative, and non-HTTPS targets; absent or malformed authorities; any userinfo; and
  empty, non-numeric,
  signed, Unicode-digit, zero, or greater-than-65,535 explicit ports. Parser and port failures
  become the exact context-suppressed
  `ValueError("url must be an absolute credential-free HTTPS endpoint without query or fragment")`.
  Every percent sign in the authority is rejected, covering encoded host characters, ports,
  userinfo delimiters, slashes, backslashes, and controls, as well as malformed escapes and IPv6
  zones, before urllib can reinterpret the authority. NFKC inspection of the authority
  additionally rejects compatibility characters that IDNA could emit as a percent sign,
  backslash, whitespace, C0, or DEL; the accepted URL is never reconstructed or normalized. A
  117-target invalid corpus
  proves the query remains untouched and `urlencode`, `Request`, the private opener, DNS, network,
  and filesystem handlers receive no work. A 63-target raw subset fails before `urlsplit`; focused
  parser tests prove failure context suppression; five invalid timeout cases retain their earlier
  precedence; and 18 valid targets preserve their exact text, sorted query, GET, `Accept`,
  `User-Agent`, timeout identity, one bounded response read, and one acquisition. This is a
  structural boundary, not a hostname, DNS, IP-routability, or SSRF guarantee: localhost, IPv4,
  IPv6, parser-accepted IPvFuture and DNS-label forms, Unicode and trailing-dot hostnames, and
  explicit ports 1 through 65,535 remain accepted. TASK-050 governs the remaining target-port
  policy. Proxy/TLS defaults, endpoints, TASK-043 through TASK-048 response and redirect mappings,
  provider behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, TASK-037
  authority, migration, and Stage 3 remain unchanged.

### TASK-048 — Fail-closed public-HTTP automatic redirect rejection

- **Key:** `phase2.fail_closed_public_http_automatic_redirect_rejection`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public transport now uses one private urllib opener whose no-follow
  handler rejects original 301, 302, 303, 307, and 308 responses before parsing `Location` or
  `URI`, reading or closing the redirect body, or opening another destination. Missing, empty,
  relative, same-origin, cross-origin, HTTPS-to-HTTP downgrade, FTP, unsupported-scheme, and
  malformed targets all remain the original response. The original 3xx enters the existing
  bounded `HTTPError` materialization path; an exact-limit body returns its original status,
  headers, and exact bytes, while a one-byte-oversize body raises
  `HttpTransportError("public HTTP error response exceeded the configured limit")` without
  truncated evidence. A deterministic five-status-by-fifteen-target real-opener matrix proves
  one original GET, one `max_response_bytes + 1` read, one cleanup attempt, no retry, no second
  read, and no follow. Additional tests preserve sanitized redirect-body read and cleanup
  mappings, direct causes, and primary-failure precedence; prove successful and non-redirect
  HTTP-error behavior, query, method, headers, timeout, proxy/TLS handler defaults; and prove in a
  fresh process that the process-global urllib opener is neither installed nor mutated. Response
  limits, endpoints, TASK-043 through TASK-047 mappings and exclusions, provider behavior,
  parsing, quality, storage, schemas, dependencies, runtime wiring, TASK-037 authority, migration,
  and Stage 3 remain unchanged. Initial request-target validation remains incomplete: the shared
  client does not yet parse and constrain its caller-supplied initial URL before request
  construction, and its private standard-library opener retains non-HTTPS scheme handlers;
  TASK-049 governs that residual risk.

### TASK-047 — Typed public-HTTP response-protocol failure mapping

- **Key:** `phase2.typed_public_http_response_protocol_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public transport now converts each real `BadStatusLine`, `LineTooLong`,
  and `UnknownProtocol` raised directly by `urlopen`, a successful-response body read, or an
  `HTTPError` body read into the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the original exception as direct cause. A
  deterministic three-exception-by-three-seam matrix proves one acquisition, one configured
  sentinel read on each body path, one cleanup attempt on the HTTP-error path, no retry or partial
  response, and no provider protocol detail in the public message. Adjacent-boundary tests prove
  direct base `HTTPException` and `InvalidURL` remain raw at all three seams, the three mapped
  protocol failures remain raw when raised by response entry or exit, and the same failures remain
  raw when raised only by HTTP-error cleanup. TASK-043 through TASK-046 mappings, TASK-045 cleanup
  and primary-failure precedence, response limits, timeouts, endpoints, queries, headers, provider
  behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, operator authority,
  migration, and Stage 3 remain unchanged. Default redirects remain enabled: urllib may still
  drain a redirect body outside the adapter cap and contact a changed destination before returning
  a handle; TASK-048 governs that residual risk.

### TASK-046 — Typed pre-response public-HTTP incomplete-read mapping

- **Key:** `phase2.typed_public_http_pre_response_incomplete_read_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** A real `IncompleteRead` raised directly by `urlopen` now becomes the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the original exception as direct cause.
  Deterministic tests prove one acquisition call, no response handle or adapter body read, no
  retry, and absence of partial provider bytes and the expected-byte count from the public
  message. Adjacent-boundary tests prove `IncompleteRead` from response entry or exit and a direct
  base `HTTPException` remain raw, preventing a broad protocol catch. Existing body-read mappings,
  HTTP-error cleanup, response limits, timeouts, endpoints, queries, headers, retries, provider
  behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, operator authority,
  migration, and Stage 3 remain unchanged. This task sanitizes only the resulting failure: default
  redirects remain enabled, the standard library may read a redirect body outside the adapter's
  response cap and follow a changed destination before returning a handle, and a separate
  governed redirect-policy task is still required.

### TASK-045 — Deterministic public-HTTP error-response resource closure

- **Key:** `phase2.deterministic_public_http_error_response_resource_closure`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** Every `HTTPError` processing path now makes one explicit cleanup attempt after no
  more than one `cap + 1` body read. A real built-in `HTTPError` smoke test and a counting
  `HTTPError` subclass prove exact-limit return, oversize failure, every supported read failure,
  later header failure, and close failure each invoke `close` exactly once; successful cleanup
  closes the underlying body before return or propagation. An `OSError`-family or `IncompleteRead`
  close-only failure becomes `HttpTransportError("public HTTP GET failed")` with that close error
  as direct cause; unsupported close-only failures remain unchanged. If any primary read,
  oversize, header, or processing failure already exists, no cleanup failure can replace its type,
  public message, or direct cause. Existing status, headers, body, read count, response limits,
  timeouts, endpoints, queries, retries, successful-response context management, provider
  behavior, parsing, quality, storage, schemas, dependencies, runtime wiring, operator authority,
  migration, and Stage 3 remain unchanged. A failed cleanup attempt is not claimed to have closed
  its resource.

### TASK-044 — Typed public-HTTP incomplete-body read-failure mapping

- **Key:** `phase2.typed_public_http_incomplete_body_read_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** A real `http.client.IncompleteRead` raised by either the successful-response body
  read or the `HTTPError` body read now becomes the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the original exception as direct cause.
  Deterministic path-symmetric tests prove one `cap + 1` read and one `urlopen` call, no retry or
  partial response, and absence of partial provider bytes and the expected-byte count from the
  public message. The successful-path handler is scoped only around `response.read`; a separate
  regression test proves an `IncompleteRead` raised during response-context entry remains
  unmapped. Existing
  TASK-043 mappings, exact-limit and oversize behavior, response limits, timeouts, endpoints,
  queries, headers, retries, resource closure, provider behavior, parsing, quality, storage,
  schemas, dependencies, runtime wiring, operator authority, migration, and Stage 3 remain
  unchanged.

### TASK-043 — Typed public-HTTP error-body read-failure mapping

- **Key:** `phase2.typed_public_http_error_response_read_failure_mapping`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared transport now converts `URLError`, `TimeoutError`, and `OSError` raised
  while reading an `HTTPError` body into the exact sanitized
  `HttpTransportError("public HTTP GET failed")`, with the read failure as the direct cause.
  Deterministic tests prove one `cap + 1` body read and one `urlopen` call, absence of untrusted
  detail from the public message, and no retry or partial response. A symmetric successful-body
  `OSError` regression test preserves the existing mapping, while the exact-limit and
  one-byte-oversize success and HTTP-error paths remain covered. `IncompleteRead`, which is not an
  `OSError`, remains outside this task and is the separately bounded next action. Response limits,
  timeouts, endpoints, queries, headers, retries, provider behavior, resource closure, parsing,
  quality, storage, schemas, dependencies, runtime wiring, operator authority, migration, and
  Stage 3 remain unchanged.

### TASK-042 — Strict bounded public-HTTP response-byte-limit validation

- **Key:** `phase2.strict_public_http_response_byte_limit_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `tests/unit/test_http_adapter.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public HTTP client now accepts only a built-in integer response limit from
  1 through the current/default hard ceiling of 2,000,000 bytes. Booleans, integer subclasses,
  integral and fractional floats, `NaN`, infinities, non-positive values, and larger integers fail
  during construction with one exact error. Deterministic tests preserve minimum, representative,
  default, and maximum valid limits and prove both successful and real `HTTPError` paths read
  `cap + 1`, accept an exact-limit body, and reject a one-byte-oversize body without returning
  truncation as evidence. This is a body-byte cap, not a total wall-clock or all-metadata memory
  bound. Provider constructors, timeouts, endpoints, queries, headers, retries, response content,
  parsing, canonical evidence, quality, storage, schemas, dependencies, runtime wiring, operator
  authority, migration, and Stage 3 remain unchanged.

### TASK-041 — Finite public HTTP timeout-boundary validation

- **Key:** `phase2.finite_public_http_timeout_boundary_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/adapters/http.py`, `src/wealth/adapters/binance.py`,
  `src/wealth/adapters/coinbase.py`, `src/wealth/adapters/binance_order_flow.py`,
  `tests/unit/test_http_adapter.py`, the three public-provider unit-test files,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** The shared public transport and all three active provider constructors now reject
  `NaN`, positive and negative infinity, zero, and negative timeouts with one exact error before
  request construction or provider HTTP work. A deterministic four-boundary matrix proves invalid
  values never reach `Request`, `urlopen`, or an injected HTTP client, while literal integer and
  fractional positive values are forwarded unchanged. The contract preserves standard urllib
  timeout semantics and does not claim a total wall-clock deadline. Endpoints, queries, headers,
  retry, pagination, range, budgets, parsing, canonical evidence, quality, storage, schemas,
  dependencies, runtime wiring, operator authority, migration, and Stage 3 remain unchanged.

### TASK-040 — Exact order-flow persistence-evidence validation

- **Key:** `phase2.exact_order_flow_persistence_evidence_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/application/order_flow_ingestion.py`,
  `src/wealth/ports/order_flow.py`, `tests/unit/test_order_flow_persistence_evidence.py`,
  `tests/integration/test_recoverable_public_trade_collection.py`,
  `docs/contracts/MARKET_DATA.md`, and the coordinated governance files and governance tests.
- **Result:** Order-flow admission now requires a passing quality report, an exact coherent raw
  identity, and one ordered status-coherent outcome per canonical batch record, each bound to the
  corresponding incoming ID and record family. Missing, extra, duplicated, reordered,
  misidentified, wrong-family, contradictory, and conflicting evidence fails closed.
  Deterministic hostile-store tests cover the complete matrix while preserving valid zero-record
  windows, and recoverable public-trade collection proves malformed returned evidence leaves its
  durable cursor and completion counters unchanged and prevents a later request even when the
  nonconforming store physically wrote the first window. Returned-outcome validation does not
  independently prove physical durability, undo store mutations, or make the evidence and control
  databases atomic. Provider, network, retry, range, quality, canonical models, raw bytes, store
  implementations, schemas, dependencies, operator authority, migration, and Stage 3 remain
  unchanged.

### TASK-039 — Exact candle persistence-evidence validation

- **Key:** `phase2.exact_candle_persistence_evidence_validation`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Files:** `src/wealth/application/ingestion.py`, `src/wealth/ports/market.py`,
  `tests/unit/test_historical_candle_persistence_evidence.py`,
  `tests/integration/test_recoverable_collection.py`, `docs/contracts/MARKET_DATA.md`, and the
  coordinated governance files and governance tests.
- **Result:** Historical candle admission now requires a passing quality report, an exact coherent
  raw-write identity, and one ordered status-coherent write outcome per batch candle, each
  matching its corresponding incoming ID. Missing, extra, duplicated, reordered, misattributed,
  contradictory, and conflicting outcomes fail closed. Deterministic hostile-store tests cover
  the full evidence matrix, and a recoverable collection test proves that incomplete returned
  evidence leaves the durable cursor and completion counters unchanged and prevents a later page
  even when the nonconforming store physically wrote the first page. The contract documentation
  states that returned-outcome validation does not independently prove physical durability, undo
  store mutations, or make the market and checkpoint databases atomic. Provider, retry,
  pagination, quality, canonical records, raw bytes, digests, lineage, store implementations,
  SQLite schemas and transactions, dependencies, order-flow ingestion, operator authority,
  migration, and Stage 3 remain unchanged.

### TASK-038 — Public-provider payload failure-boundary hardening

- **Key:** `phase2.public_provider_payload_failure_boundary_hardening`
- **Risk tier:** RISK 1 — DEVELOPMENT
- **Status:** COMPLETE
- **Result:** Binance candles, Coinbase candles, and Binance aggregate trades now convert invalid
  UTF-8, malformed JSON, excessive nesting, and decoder `ValueError` failures into each
  provider's sanitized, non-retryable typed `INVALID_PAYLOAD` error. Deterministic no-network
  tests pin every decoder cause, exact public detail, preserved cause type, and aggregate-trade
  split semantics. Endpoints, queries, byte bounds, timeouts, HTTP retry behavior, valid-payload
  canonicalization, raw-byte lineage, digests, provider schemas, storage, runtime wiring,
  dependencies, operator paths, SQLite, TASK-037 authority, migration, and Stage 3 remain
  unchanged.

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

- Implement continuous public-trade collection only after TASK-058's design-only operating
  contract is accepted and a separate production task with required evidence and approvals is
  promoted.

## Backlog Rules

- Only one item may be the canonical next action.
- A task must define goal, scope, constraints, acceptance evidence, and excluded work before code.
- Missing approval, policy, state, or critical evidence fails closed.
- Completing a task does not authorize deployment, mode promotion, private access, or trading.
