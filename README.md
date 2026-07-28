# WEALTH

WEALTH is a governed multi-agent cryptocurrency research and trading platform. The project is
currently building its reliable market-data platform. It can read bounded historical candle ranges
from Binance and Coinbase public endpoints and supervise restart-safe polling of closed candles,
but it has no account access and cannot execute trades.

## Read First

- `PROJECT_STATE.json` — validated current state and canonical next action
- `docs/PROJECT_CHARTER.md`
- `docs/AI_DEPARTMENTS.md`
- `docs/ORGANIZATION.md`
- `docs/ARCHITECTURE.md`
- `docs/POLICIES.md`
- `docs/SECURITY_POLICY.md`
- `docs/RISK_POLICY.md`
- `docs/EXECUTION_POLICY.md`
- `docs/DATA_CONTRACTS.md`
- `docs/DATA_CATALOG.md`
- `RISK_REGISTER.md`
- `BACKLOG.md`
- `docs/ROADMAP.md`
- `AGENTS.md`

Accepted Architecture Decision Records live in `docs/decisions/`. The state file names one next
action; the backlog supplies its bounded acceptance contract.

## Prerequisites

- Git
- `uv`

The repository pins Python 3.13 in `.python-version`. `uv` can install a compatible Python automatically.

## Setup

```text
uv sync --all-groups
```

Do not create or commit a real `.env` containing credentials during foundation work. `.env.example` contains safe, non-secret local defaults.

## Run the Foundation Health Slice

```text
uv run wealth-health
```

The command validates, stores, and emits one synthetic health event. It does not access the network, an exchange, or financial credentials.

## Quality Checks

```text
uv lock --check
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
uv --preview-features audit-command audit --locked
```

Run all checks before requesting review. Report any unavailable or failed check explicitly.

## Safe Runtime Defaults

Runtime identity is loaded from environment variables and fails closed to:

```text
WEALTH_ENVIRONMENT=development
WEALTH_OPERATING_MODE=research
WEALTH_PRIMARY_MARKET=crypto
WEALTH_TRADING_TYPE=spot
WEALTH_SYSTEM_TIMEZONE=UTC
WEALTH_USER_TIMEZONE=Asia/Jerusalem
WEALTH_BASE_CURRENCY=USD
WEALTH_ARCHITECTURE_STYLE=modular_monolith
WEALTH_LIVE_TRADING_ENABLED=false
WEALTH_LEVERAGE_ENABLED=false
WEALTH_WITHDRAWALS_ENABLED=false
WEALTH_EXTERNAL_NOTIFICATIONS_ENABLED=false
WEALTH_AUTONOMOUS_LIVE_EXECUTION_ENABLED=false
WEALTH_LOG_LEVEL=INFO
```

The system remains in research mode as an approved safer deviation until a real paper exchange or
simulator exists. Paper is still the target after the Phase 5 gates; calling the current runtime
paper would be misleading. Any attempt to enable a sensitive control or use the live environment
fails validation. Configuration never grants execution authority, and no current code path can
submit an order.

## Professional Algorithm Foundation

The first algorithmic capability is deterministic market-data replay, not a trading signal.
Canonical candles preserve source lineage and event, observation, and processing times. A replay
slice exposes only records observed by its evaluation time, preventing a strategy from seeing
late-arriving or future data.

## Public Binance Candle Slice

The first real provider adapter can read already-closed Spot and USD-M Futures candles from
Binance's public REST endpoints. It requires no API key and exposes no account or order capability.

## Public Coinbase Candle Slice

A second provider adapter can read already-closed Spot candles from Coinbase Exchange's public REST
endpoint. It supports the canonical one-minute, five-minute, fifteen-minute, one-hour, and one-day
timeframes with at most 300 candles per provider request. Documented pre-window rows are excluded
from the requested canonical batch, while missing in-window buckets remain explicit quality
failures rather than invented candles.

## Public-Provider Schema Review and Drift Response

An offline, versioned corpus records the reviewed synthetic payload shape for the five active
public request variants: Binance Spot and USD-M candles, Coinbase Exchange Spot candles, and
Binance Spot and USD-M aggregate trades. Its strict
[manifest](tests/fixtures/public_provider_schema/v1/manifest.json) binds every minimal fixture to
one request identity, exact-byte SHA-256, shape contract, current official reference, and UTC
review date. Deterministic stubs feed those exact bytes through the existing production adapters;
no fixture test contacts a provider or refreshes itself from a live response.

The [public-provider schema-drift runbook](docs/runbooks/PUBLIC_PROVIDER_SCHEMA_DRIFT.md) defines
manual signals, containment, safe evidence handling, official-document re-review, append-only
fixture versioning, regression, escalation, resume gates, and rollback. Fixture review does not
detect upstream drift automatically, make provider documentation immutable, or authorize an
adapter, endpoint, runtime, deployment, or continuous-collection change.

## Cross-Source Candle Reconciliation

Selected candle windows from two distinct sources can be compared after each source passes the
existing sequence-quality gate. The report retains both record identifiers, missing-source
findings, and symmetric basis-point differences for OHLC and base volume. Price tolerance is
explicit and versioned; a volume limit is optional because volume is venue-specific.

Reconciliation requires the same exact canonical instrument, instrument type, and timeframe.
`BTC-USD` is therefore not compared with `BTC-USDT` through an implicit conversion. Outcomes are
`pass`, `divergent`, or `blocked`; the report does not choose a true provider, blend prices, repair
records, or authorize a trade.

Reconciliation reports can be wrapped as immutable observations and appended to a dedicated local
SQLite history database. Exact repeats are idempotent, while reused observation identities or
comparison keys with different source streams fail explicitly. Bounded queries reload and
revalidate the original evidence. Indexed summaries expose pass, divergence, blocked,
source-quality-failure, compared-interval, and issue-code counts for one comparison series and time
window. This evidence layer does not rank providers, trigger alerts, or make trading decisions.

Every response is normalized into the provider-independent candle contract and sent through the
deterministic sequence-quality gate. Incomplete, conflicting, malformed, or time-inconsistent
batches are reported and are not written to storage.

Accepted batches can now be stored in a file-backed SQLite adapter. The adapter keeps exact raw
provider bytes separately from canonical candles, verifies them again when reading, survives
restart, treats repeats idempotently, and quarantines conflicting revisions without overwriting the
accepted record.

Provider adapters remain bounded to one window: at most 1,000 Binance candles or 300 Coinbase
candles. An explicit application flow can split a larger range into contiguous pages, pace
requests, retry only classified transient failures, honor a bounded `Retry-After`, and stop at the
first failed page with an exact resume boundary. One invocation is capped at 100,000 candles.

Operator-invoked collection jobs can persist their cursor in a dedicated SQLite control database.
Versioned worker leases prevent duplicate advancement, accepted pages checkpoint independently,
and page attempts produce append-only source-health evidence. Recovery deliberately refetches a
page when a process stops after storing market evidence but before advancing its checkpoint; the
market store handles that repeat idempotently.

A supervised polling layer can now maintain a separate durable cursor for one continuous candle
stream. It requests only timeframe boundaries that are closed beyond an explicit settlement delay,
limits every catch-up window and invocation, and attaches one exact bounded collection job before
network work begins. Restart resumes that same job. A disconnect uses bounded reconnect backoff;
unsupported failures or a configured consecutive-failure limit pause the stream until operator
review. Manual pause also blocks new network work until explicit resume. This is an application
workflow, not an automatically started daemon.

An interruptible local service runner can now invoke those cycles repeatedly under an explicit
run limit. A stop request wakes polling waits immediately, and each invocation stores validated
startup, cycle, pause, failure, stop, or run-limit evidence in append-only SQLite history. Restart
creates a new service run audit trail while retaining the existing continuous market cursor. This
is a process-lifecycle boundary, not an installed operating-system service or 24/7 deployment.

Read-only operational health reports can classify the latest or a bounded set of recent service
runs as active, stale, stopped, paused, failed, or completed. Stale and failed runs produce
structured critical alerts; paused runs produce a warning. The default ten-minute stale threshold
is longer than the collector's maximum bounded wait. These alerts remain internal data contracts:
no message is sent and no process is restarted automatically.

The next market-structure boundary defines strict canonical trades, last-price tickers, and
best-bid-ask snapshots. It preserves provider identity, event/observation/processing time, exact
decimal values, sequences, and lineage. Aggressor side can remain explicitly unknown, optional
ticker statistics require a declared window, and locked or crossed top-of-book snapshots fail
closed.

A bounded quality gate now checks one exact record family for mixed streams, event-time ordering,
window membership, duplicate identities, conflicting values, and documented provider-sequence
promises. Sequence gaps are reported only when an adapter explicitly declares a contiguous
provider contract. An idempotent in-memory store proves that equivalent repeats and conflicting
revisions never overwrite the first accepted record.

A dedicated SQLite adapter now persists exact raw captures and all three canonical order-flow
families atomically. It survives restart, retains every equivalent raw lineage link, quarantines
conflicting revisions, rejects a reused canonical identity, validates its database type, and
revalidates raw hashes and canonical records on reads.

The application admission path sends the complete bounded batch through the deterministic quality
gate before storage. Quality failure leaves raw and canonical storage untouched; raw or canonical
storage conflict makes the result explicitly unaccepted.

A bounded public Binance REST adapter now normalizes Spot and USD-M aggregate trades through that
path without an API key. It preserves the provider's aggregate ID and underlying first/last trade
IDs, maps maker evidence to aggressor side, retains exact raw bytes, and admits valid empty
windows. A response at Binance's 1,000-row cap fails as possibly truncated so partial evidence
cannot be stored as a complete window.

A bounded application flow can collect a longer public-trade range using chronological initial
windows. Dense windows split into exact left and right halves until each is complete or the
one-millisecond minimum is reached. Request count, total records, pacing, attempts, exponential
delay, and `Retry-After` are bounded; every adaptive decision is traced and every stop exposes the
exact first unadmitted event-time boundary. No live order-flow collection, replay path, trading
signal, or trading action is present.

Dedicated public-trade control contracts can now retain that bounded progress across process
restart. A checkpoint stores the durable cursor, exact end of a pending adaptive leaf, immutable
policy fingerprint, committed-outcome lifetime work counters, and a UUID-fenced worker lease.
Health evidence retains the provider symbol. A separate SQLite control store installs its schema
transactionally, normalizes indexed time to UTC, validates complete projections and summaries,
and atomically persists compare-and-swap transitions with matching source-health evidence. The
lease token is checked at the transition boundary, lease TTL is capped at one hour, every new
claim must use a UUID not previously acquired by that job, and acquisitions are retained in a
durable fencing ledger. Health evidence identifies the exact checkpoint version that committed it
and is read in bounded, causally ordered pages (100 rows by default, 1,000 maximum).

These counters do not claim to be crash-durable hard job limits. A request made before an
uncommitted checkpoint transition can be repeated after restart; the shared durable provider-rate
budget remains the pre-request capacity protection. Crash-durable per-job attempt reservation
before network access remains future work.

An explicitly invoked bounded orchestrator now validates the immutable policy fingerprint, claims
the job with a fresh UUID fencing token, and invokes finite range collection from the durable
cursor or exact pending leaf. Recovery finishes that exact leaf first and may then process the
remaining range once, so one operator invocation contains at most two bounded segments. It
classifies clean outer request- or record-limit stops as resumable `PAUSED` outcomes with truthful
`HEALTHY` or `DEGRADED` source health. Typed terminal source or admission failures become `FAILED`
with a bounded canonical control code; stop-reason text and arbitrary upstream machine-code
strings are not trusted as classifications.

The lease claim is a control-only transition; the orchestrator advances the durable work cursor
and counters only after market evidence is durable. A crash between the evidence and work-progress
commits therefore causes an idempotent refetch rather than a permanent gap. The work transition
and matching health observation commit atomically in the control database when lease authority
remains current and compare-and-swap succeeds. Lost authority or a version conflict returns an
explicit non-progress result and leaves the durable cursor unchanged for safe refetch. Append-only
transitions are now available through a typed read-only port. Each immutable result contains the
validated canonical checkpoint and the actor fencing token retained for that transition. Pages
are ascending and contiguous by checkpoint version, use a returned version as an exclusive cursor,
default to 100 records, and reject a requested limit above 1,000. The SQLite reader revalidates
canonical JSON, indexed projections, immutable job identity, UTC content, lifecycle causality,
actor authority against the durable lease-acquisition ledger, exact SQLite storage types, page
continuity, and agreement between the ledger tail and current checkpoint. Corruption fails closed
through the existing control-storage error boundary. Reads do not change the database or schema
and remain separate from source-health history. Transition time comes from an injected trusted UTC
clock. This flow is not a scheduler, daemon, continuous poller, or live stream.

A deterministic generated-fixture recovery drill now composes the evidence, checkpoint, and
shared rate-budget SQLite boundaries across a process-style reopen. One worker exhausts two
retryable disconnect outcomes and retains the exact pending one-millisecond leaf at failed
checkpoint version 3; a newly constructed worker with a fresh UUID fence then consumes exact
empty, one-trade, and empty windows and completes at version 6. The audit chain retains all six
causal transitions, health at versions 3, 5, and 6, five budgeted requests, one retry, two pacing
waits, three raw captures, one canonical trade, and zero conflicts. Reinvocation after completion
does no work and changes no observation. This generated test evidence does not establish
cross-database atomicity, physical durability, continuous operation, or automatic recovery.

## Continuous Public-Trade Operating Contract (Design Only)

The current public-trade path remains finite and explicitly invoked. A caller supplies one
immutable bounded job and one complete finite policy; when work is available, an invocation claims
fresh UUID fencing authority and processes only the durable cursor or exact retained pending leaf,
followed by at most one bounded remaining segment. Nothing in the current path schedules another
invocation, starts a background process, detects provider drift, or automatically pauses,
recovers, resumes, or rolls back collection.

[ADR-0028](docs/decisions/0028-continuous-public-trade-collection-operating-contract.md) records a
conceptual operating contract for a possible future single-host continuous public-trade
lifecycle. It separates three layers. Future durable stream control is `active` or `paused`;
`schema_drift_hold` is a scoped pause reason, while enablement remains an external
disabled-by-default posture. Each service invocation records `starting`, `running`, and exactly
one terminal `stopped`, `paused`, `failed`, or `run_limit` state. The existing bounded job retains
`pending`, `running`, `paused`, `failed`, and `completed`. `waiting`, `caught_up`, and
`work_limit_reached` are cycle outcomes, not lifecycle states, and carry no authority to schedule
another invocation.

A clean bounded-job `paused` outcome caused by a request or record work limit retains the exact
attachment and leaves the stream active for later bounded continuation. Bounded `failed`,
already-running/conflict/lost-lease, corrupt state, or source/policy drift ends the service run as
failed and places or keeps the stream on manual hold; a supervisor never retries it automatically.
Clean stop leaves stream status, cursor, counters, and attachment unchanged. Governed resume
clears only the hold and never rewrites progress.

The design requires UTC half-open closed windows derived from explicit finite window and
settlement-lag policy, with bounded catch-up and per-invocation work. It also preserves immutable
job identity and policy fingerprints, fresh fencing, exact pending-leaf restart, evidence-first
checkpoint progress, idempotent refetch, causal transition and health evidence, and the shared
durable request-budget gate. Before child creation, the future stream checkpoint must retain the
exact child UUID, target end, creation input, and bounded-policy fingerprint; an attached end is
never replanned. The stream cursor advances exactly to that end and clears the attachment only
after the child is durably `completed`. Restart may reconstruct an absent attached child exactly,
handle an existing child under the explicit manual policy, or advance a completed attachment
without a network refetch.

The same design routes a suspected schema change for the smallest exact affected request variant,
plus every variant sharing an uncertain parser or endpoint boundary when isolation is not proven,
into the TASK-057 manual hold and governed review procedure. It keeps bounded-job source health
separate from service-run liveness and terminal health and identifies future capacity,
storage-growth, outage, catch-up, failure, escalation, resume, and rollback evidence that an
implementation proposal would need. Rollback keeps the conceptual continuous path disabled or
returns operation to today's explicitly invoked bounded flow without discarding evidence or
weakening any adapter, drift, budget, fencing, checkpoint, or audit control.

This is documentation, not a running component or a readiness claim. It does not select or install
a scheduler, daemon, process manager, service, or deployment; configure capacity; deliver alerts;
add a drift detector or automatic drift response; or authorize automatic restart, recovery,
pause, remediation, resume, failover, or multi-host coordination. Production implementation,
operational validation, deployment evidence, and any applicable approval remain separate future
work. No stale-heartbeat threshold, capacity value, retention policy, outage envelope, or
settlement-lag adequacy is selected or proven. Settlement lag is not proof against late provider
events; pause cannot cancel in-flight work; one-millisecond density, separate-database commit
seams, crash-uncommitted request counts, physical durability, and cooperating-single-host-only
coordination remain residual risks.

## Continuous Public-Trade Pure Planning Contracts (Unused)

`src/wealth/domain/continuous_public_trade.py` now adds the first implementation boundary beneath
ADR-0028. It exposes frozen `ContinuousPublicTradePolicy`,
`ContinuousPublicTradeAttachment`, `ContinuousPublicTradeStreamCheckpoint`, and
`ContinuousPublicTradePlan` values; explicit stream, service, plan, and transition enums;
`plan_continuous_public_trade_window`; and pure stream and service transition validators. The
module is not imported by runtime composition. It receives the checkpoint, policy, and `now`
explicitly, performs no I/O, and returns data only.

The planner requires `now.tzinfo is datetime.UTC` and works in exact whole epoch milliseconds.
For a paused checkpoint it returns `HELD`, preserving the cursor and any existing attachment. For
an active checkpoint that already owns an immutable bounded-job attachment it returns
`ATTACHED_JOB` with that attachment unchanged. If the durable cursor has reached the latest
epoch-aligned boundary that is fully closed after the configured non-negative settlement lag, it
returns `WAITING`. Otherwise it proposes one `ATTACHED_JOB` candidate whose half-open range starts
exactly at the cursor and ends at
`min(latest_eligible_end, cursor + max_catchup_span)`. The candidate is only a value: no child is
created, attached, persisted, claimed, started, or invoked.

Pure transition validators preserve immutable stream and market identity, the request variant,
complete policy fingerprint, monotonic one-step versioning, exact cursor and attachment causality,
explicit manual hold state, and finite policy bounds. Stream transitions are only `RETAIN`,
`ATTACH`, `CHILD_COMPLETED`, `MANUAL_HOLD`, or `MANUAL_RESUME`; service status can only move from
`STARTING` to `RUNNING` and then once to a terminal status. Invalid or unknown values fail before
a result; the contracts never normalize a cursor, infer a clock, acquire authority, write
evidence, advance storage, or authorize pause, resume, restart, recovery, or scheduling.

This slice adds no repository, adapter, SQLite or schema, provider/network access, request-budget
or retry behavior, wait/sleep, scheduler, trigger, daemon, service runner, CLI, dashboard,
deployment, configuration loading, operator path/data, credential, permission, notification,
dependency, lockfile, or automatic action. It does not establish persistence, cross-database
atomicity, physical durability, capacity adequacy, multi-host exclusivity, continuous operation,
recovery, deployment, or Phase 2 readiness. ADR-0028 and the current explicitly invoked bounded
public-trade flow remain unchanged.

## Continuous Public-Trade Persistence Records and Codecs (Unused)

[ADR-0029](docs/decisions/0029-continuous-public-trade-stream-persistence-contract.md) defines the
logical persistence contract that a possible future stream store must satisfy. TASK-061 now
implements one pure, unused domain module for the strict version-one child-creation payload, stream
envelope, stream-creation record, stream-transition record, evidence reference/scope, and complete
stream-policy projection. It does not add a port, repository, adapter, SQLite database, schema,
migration, I/O, runtime import, network path, authority, action, capacity, durability, or readiness.

The future durable current state is exactly the TASK-059 stream checkpoint: immutable stream and
market identity, request variant, stream-policy fingerprint and start; exact epoch-millisecond
cursor; active/paused status and reason; optional immutable attachment; and monotonic version. The
planner result, injected clock, full policy, service run, fences, bounded-child state, source
health, market evidence, and shared budget remain invocation-local or separately durable and are
never inferred from the stream record.

The TASK-059 attachment's `creation_fingerprint` is non-invertible. An attach-before-create commit
must therefore atomically retain a new canonical `child_creation_payload` for the complete
deterministic pristine child plus stream/request binding and explicit versions, including its exact
UTC creation time and separate bounded-child policy fingerprint. This evidence payload does not
replace or redefine the existing bounded-child store serializer. The pure TASK-061 values and
validators reject missing bytes, fingerprint disagreement, policy confusion, child
identity/range mismatch, or a range that cannot construct the exact existing child model; they do
not recreate a child from a new clock or current configuration.

Conceptual `create` accepts only pristine version-one state, complete policy, governed-create
reference, and one fixed-UTC command time sampled exactly once by the future mutation boundary's
trusted injected clock; it returns inserted, exact creation/history duplicate, or conflict without
replacement. One store-local natural feed identity cannot use two stream UUIDs, without claiming
cross-store or multi-host uniqueness. Exact-identity current load
requires the full immutable identity and complete effective stream policy plus the effective child
policy while attached, compares every stream-policy field and its caller-supplied fingerprint
against immutable stream-creation evidence, checks only a constant-size latest/predecessor proof,
and distinguishes absence, conflict, unsupported version, corruption, and storage failure. Full
history audit is a separate 1-to-100-new-record paginated operation under an explicit finite outer
limit; every noninitial page loads one predecessor overlap so its first TASK-059 transition can be
validated against exact prior envelope bytes as well as the rolling-root continuation. It extends a
domain-separated rolling history root over every creation/transition byte, including bounded
evidence references whose digests/scopes bind their external bodies. Before child
creation/recovery, a claim, budget reservation, provider request, evidence admission, or stream
mutation after the sole governed-create bootstrap, a future runtime requires an externally anchored
accepted attestation matching the exact current version, envelope digest, and history root. A
bounded creation audit must attest version one before any post-create action; current load/planning
alone grants no action. ADR-0028's sole narrow exception lets an operation already past every
pre-request gate admit its already-returning evidence and finish or fail that same finite child
under its exact pre-hold attestation, fence, and authority only when validated hold evidence
explicitly preserves the response/admission contract. Drift, invalid payload, quality/evidence
failure, corruption, or ambiguous hold classification stops canonical admission/progress and may
use only a separately governed quarantine/attention path. No exception starts a new attempt/request
or mutates the stream.

The store-level compare-and-swap command is constructed inside the trusted mutation boundary only
after exact reload and one trusted clock sample. The caller cannot supply `recorded_at` or a
preconstructed ATTACH successor. The internal command requires the trusted previous version and
domain-separated stream-envelope digest, one explicit transition, a complete successor envelope at
exactly `version + 1`, and transition authority plus child-completion evidence when applicable.
Create also retains a separate
stream-creation record with lowercase-hex exact version-one envelope bytes and the complete
canonical stream-policy projection; it does not invent a TASK-059 `CREATE` kind. Current state and
a separate canonical append-only transition record commit together only inside the future stream
store. Every transition retains lowercase-hex exact successor-envelope bytes and binds
prior/successor digests, the prior rolling root, and typed bounded external evidence references,
allowing the full chain to be revalidated without embedding operator identities, secrets,
credentials, or operator paths. Kind-specific scope digests bind the exact stream/transition and,
for creation, the complete policy projection. ATTACH authority is intentionally time-independent:
it binds exact prior version, envelope digest, accepted history root, successor version, candidate
child UUID, and effective child-policy fingerprint while successor digest and creation fingerprint
are null. Every other transition authority binds that prior root and its exact successor digest;
the finalized ATTACH transition and rolling root bind its exact result.
Canonical reason scope is required for RETAIN and MANUAL_HOLD, equals the held checkpoint reason,
and is null for other transitions. A future mutation boundary samples command time from its trusted
injected UTC clock; the caller cannot backdate it. Later expiry does not corrupt history or
authorize new work, record time cannot regress, and ATTACH uses that same time for the pristine
child.
Compare-and-swap is not the outer UUID fence. A conflict/lost fence ends the invocation without
blind retry and requires the ADR-0028 failed-service/manual-hold decision.

Canonical records use an explicit record type and serialization version separate from the
TASK-059 model version, causal checkpoint version, and any future physical-store schema. TASK-061
implements exact compact sorted-key UTF-8 JSON codecs with no BOM/newline, exact UUID/enum and
integer representations, explicit nulls, duplicate/unknown keys rejected, and fixed raw,
child-payload, envelope, successor-hex, string, depth, member, key, and integer-digit bounds.
Generic dependency-version JSON output is not the byte authority. Six distinct domain-separated
contracts cover the child-creation fingerprint, stream-envelope digest, stream-creation digest,
stream-transition digest, evidence-scope digest, and initial/continued rolling history root; an
external evidence-body digest remains externally supplied. TASK-059 epoch milliseconds remain
exact, and child material fails closed rather than overflowing when it cannot be represented by
the existing fixed-UTC child model. Any physical projection to ADR-0027 epoch microseconds remains
future store work and must also fail closed.

Every datetime in `child_creation_payload` uses exact fixed-UTC six-fractional-digit `Z` text and
must round-trip from attachment epoch milliseconds without rounding; this still does not alter the
existing child-store serializer.
Version-one readers reject unknown/newer fields; newer readers dispatch original version-one bytes
through the frozen codec, and downgrade requires an untouched old generation or a proven lossless
reverse converter.

Because TASK-059's planner accepts a fingerprint before returning a new due range, attachment uses
an exact pure two-pass proof. One fixed-UTC trusted instant is the planner's `now`, the child's
`created_at`/`updated_at`, and the transition's `recorded_at`; first plan with the fixed in-memory
all-zero provisional digest, build and hash the child payload from that range, then replan with the
real digest and require every non-fingerprint result field to match. The provisional value is never
persisted or used for action, and the TASK-059 API remains unchanged.

Every attach, child-create, evidence, child-checkpoint, child-completion, stream-advance, hold, and
resume crash seam is resolved by exact reload. An attachment commits before child creation;
accepted evidence precedes child progress; verified child completion precedes stream progress; and
an already-completed attached child advances with zero provider requests. The stores remain
separate and are never described as atomic. Rollback disables the continuous path and returns to
the existing explicit bounded flow while preserving the exact stream, attachment, child, history,
hold, evidence, health, lifecycle, fence, and budget records.

TASK-061 is implemented only as that unused pure domain increment. Its pure two-pass attachment
finalizer preserves TASK-059 planning, and its validators bind the complete effective stream
policy, immutable identity, attached child policy, exact create/transition evidence scopes, and
causal predecessor/successor links. Deterministic golden-byte, hostile-input, transition-matrix,
limit-boundary, and property tests exercise those contracts. No port, repository, adapter,
SQLite/database/schema, migration, I/O, runtime/network path, authority, action, capacity,
durability, deployment, or readiness was added; TASK-059 behavior and the explicitly invoked
bounded public-trade flow remain unchanged.

The next safe bounded direction is TASK-062: pure, unused logical stream-store port, command, and
outcome contracts plus a narrow ADR-0029 consistency review, before any physical store decision.
TASK-037 remains blocked and authorization remains denied.

Operators and monitoring tools can read the separate candle collector-service state through a
dedicated JSON command:

```text
uv run wealth-collector-health --database storage/collector-service.sqlite3 --collection-id 00000000-0000-0000-0000-000000000001 --pretty
```

The database must already exist and is opened read-only. The command returns `0` for OK, `1` for a
warning, `2` for a confirmed critical alert, and `3` when the result is unknown or cannot be
trusted. It never starts, stops, or repairs the collector.

Shared request-budget coordination can prevent cooperating candle and aggregate-trade processes on
one host from exceeding an explicit combined weighted budget before network access. Binance Spot
aggregate trades use documented cost 4 and USD-M aggregate trades cost 20; capacity, period, and
shared key remain explicit configuration. Multi-host coordination, automatic scheduling, and live
WebSocket ingestion remain future tasks.

## Current Scope

Included:

- Approved foundation documents.
- A strict current-state snapshot, governed backlog, risk register, and explicit approval matrix.
- Versioned security, risk, and execution policies with fail-closed sensitive controls.
- Architecture Decision Records.
- Python package and quality-tool configuration.
- A minimal deterministic event pipeline used to prove validation, storage, logging, and testing boundaries.
- Safe runtime identity with explicit environment and operating mode.
- A first canonical candle contract and point-in-time replay boundary.
- Deterministic candle-sequence quality reports and idempotent in-memory storage.
- A bounded, public Binance REST adapter for closed Spot and USD-M Futures candles.
- A bounded, public Coinbase Exchange adapter for closed Spot candles.
- Fail-closed historical ingestion from provider response through the quality gate.
- Bounded historical pagination, pacing, retry evidence, and safe resume boundaries.
- Durable local SQLite storage for raw evidence, canonical candles, and conflict quarantine.
- Durable collection checkpoints, worker leases, crash recovery, and source-health summaries.
- Weighted shared request budgets with idempotent reservations and observable local backpressure.
- Deterministic cross-source candle reconciliation with explicit tolerances and missing evidence.
- Append-only reconciliation history with bounded source-quality and issue-code summaries.
- Supervised closed-candle polling with durable cursors, bounded reconnects, and restart recovery.
- Interruptible local collector runs with durable lifecycle heartbeats and explicit terminal
  reasons.
- Queryable collector health reports with stale-run detection and structured internal alerts.
- A read-only JSON collector-health command with monitoring-compatible exit codes.
- Strict canonical trade, ticker, and best-bid-ask contracts with point-in-time lineage.
- Bounded order-flow quality auditing and idempotent temporary storage without silent overwrite.
- Durable raw and canonical SQLite order-flow evidence with conflict quarantine.
- Fail-closed order-flow admission through the quality gate before durable storage.
- A bounded, public Binance REST adapter for Spot and USD-M aggregate trades with no credentials.
- Bounded adaptive public-trade range ingestion with retries, pacing, and safe resume evidence.
- Shared durable weighted request-budget gating for candle and public-trade sources.
- Restart-safe bounded public-trade checkpoint and health contracts with a dedicated local SQLite
  control store, UUID fencing, provider-symbol identity, and validated UTC projections.
- Explicitly invoked bounded public-trade checkpoint orchestration with policy validation,
  evidence-first progress, typed pause/failure outcomes, and restart recovery.
- Typed read-only public-trade transition history with bounded checkpoint-version pagination,
  actor-authority validation, restart replay, and fail-closed corruption detection.
- Generated-fixture public-trade disconnect, sparse-window, and process-style reopen recovery evidence
  across the local evidence, control, and shared rate-budget SQLite boundaries.
- A strict versioned synthetic fixture corpus for all five active public-provider payload variants,
  with offline adapter/drift regression and a manual schema-drift response runbook.
- A design-only operating contract for a possible future single-host continuous public-trade
  lifecycle, with explicit implementation evidence and rollback requirements.
- Continuous integration and dependency vulnerability auditing.

Not included:

- Private exchange or account access.
- An automatically started operating-system service, deployment, or live WebSocket ingestion.
- Automatic aggregate-trade scheduling, continuous collection, live WebSockets, provider gap
  recovery, an operator transition-history CLI or dashboard, or crash-durable per-job pre-request
  attempt reservations.
- Automatic public-provider schema-drift detection, online fixture refresh, automatic
  pause/remediation/resume, or continuous-readiness evidence.
- A running continuous public-trade lifecycle, scheduler, daemon, process manager, service,
  deployment, configured capacity envelope, or automatic recovery and drift response.
- Automatic historical collection scheduling.
- Multi-host request-budget coordination.
- Automatic source ranking, price blending, or cross-quote conversion.
- Reconciliation and operational dashboards or automatic remediation.
- External alert delivery, acknowledgement, escalation, or automatic service restart.
- Trading strategies.
- AI model integration.
- Portfolio, risk, or order execution.
- Real credentials or financial actions.
