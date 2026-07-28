# Project Roadmap

## Purpose

This roadmap defines the order in which the AI Trading Corporation will be built. It converts the project vision and architecture into gated phases with explicit deliverables and exit criteria.

The roadmap is capability-based rather than date-based. A phase is complete only when its evidence and exit gate are satisfied. Calendar estimates will be created later by breaking the active phase into focused tasks of approximately two hours each.

## Roadmap Rules

1. Build one dependable capability at a time.
2. Do not advance because a demo works; advance because the phase exit gate is satisfied.
3. Keep research, paper, and live environments distinguishable at every stage.
4. Reuse the same canonical data, contracts, replay, risk, audit, and evaluation concepts across operating modes.
5. Treat realistic fees, funding, slippage, latency, rejected orders, and missing data as part of the system.
6. Introduce AI analysis only after deterministic data, replay, portfolio, risk, and execution foundations exist.
7. Introduce adaptive behavior only after static behavior can be reproduced and evaluated.
8. Introduce real trading only after paper operation is stable and recoverable.
9. Every promotion requires evidence, rollback readiness, and explicit approval.
10. Codex may implement and validate changes, but cannot approve its own promotion to live operation.

## Current Status

The project is in **Phase 2 — Reliable Market Data Platform**.

Phase 1 is complete. Its accepted evidence includes:

- `docs/PROJECT_CHARTER.md`
- `docs/AI_DEPARTMENTS.md`
- `docs/ARCHITECTURE.md` and this roadmap
- Repository guidance and accepted technology, application-shape, CI, and security decisions
- A reproducible Python package with environment separation, structured health events, and
  automated format, lint, type, test, lockfile, and dependency-audit checks

The 2026-07-25 foundation-alignment checkpoint adds a validated `PROJECT_STATE.json`, one
canonical next action in `BACKLOG.md`, a linked `RISK_REGISTER.md`, and explicit governance,
security, risk, and execution policies. Sensitive runtime permissions are declared disabled and
fail closed. Development/research remains the truthful temporary baseline until a real paper
simulator exists; paper remains a later gated target.

Completed Phase 2 slices include:

- A canonical final-candle contract with source lineage and point-in-time timestamps.
- Deterministic market replay that prevents future-data leakage.
- Candle gap, duplicate, conflict, sequence, stream, and window-quality checks.
- Idempotent in-memory candle storage that never overwrites conflicts.
- A bounded public Binance REST adapter for closed Spot and USD-M Futures candles.
- Fail-closed historical ingestion through the quality gate before storage.
- File-backed SQLite storage for exact raw provider evidence and canonical candles.
- Restart-safe idempotency, read-time revalidation, and canonical conflict quarantine.
- Bounded historical pagination with contiguous pages and explicit request pacing.
- Classified transient retries, bounded `Retry-After`, retry evidence, and safe resume boundaries.
- Durable collection checkpoints with versioned worker leases and restart recovery.
- Append-only page-level source-health evidence with query-efficient job summaries.
- Streaming page planning that avoids materializing large bounded ranges.
- Durable shared request-budget coordination with weighted, idempotent reservations.
- Pre-network budget gating and observable denial, retry, and wait metrics.
- A second public provider adapter for bounded Coinbase Exchange Spot candles.
- Provider-specific range, timeframe, ordering, precision, and sparse-interval handling behind the
  shared canonical candle boundary.
- Deterministic selected-window reconciliation across two quality-audited candle streams.
- Explicit missing-source evidence, symmetric basis-point differences, and versioned price and
  optional volume tolerances.
- Append-only local reconciliation history with idempotent writes and read-time validation.
- Bounded per-series reconciliation, source-quality, compared-interval, and issue-code metrics.
- Supervised continuous closed-candle polling with a durable per-stream cursor.
- Planned disconnect, bounded reconnect, operator pause, and restart-recovery fault tests.
- Interruptible local collector service runs with bounded cycles and durable lifecycle heartbeats.
- Explicit clean-stop, run-limit, pause, competing-worker, restart, and corrupt-heartbeat tests.
- Queryable newest-first service health with configurable stale-heartbeat detection.
- Structured internal warning and critical alerts for paused, failed, and stale collector runs.
- A read-only JSON collector-health command with explicit OK, warning, critical, and unknown exits.
- Missing-path, invalid-input, stale, paused, and read-only enforcement tests for operator queries.
- Canonical trade, ticker, and best-bid-ask contracts with exact decimal values and lineage.
- Point-in-time, aggressor-side, ticker-window, and uncrossed top-of-book invariant tests.
- Bounded trade, ticker, and best-bid-ask stream-quality audits with explicit provider sequence
  policies.
- Idempotent in-memory order-flow storage with deterministic queries and no silent conflict
  replacement.
- Dedicated SQLite order-flow storage with atomic raw lineage, restart-safe idempotency,
  database-type validation, read-time revalidation, and conflict quarantine.
- Fail-closed order-flow batch admission through quality auditing before any storage mutation.
- A bounded public Binance Spot and USD-M aggregate-trade adapter with explicit provider
  aggregation evidence, empty-window evidence, and fail-closed response-cap handling.
- Bounded public-trade range ingestion with chronological initial pages, adaptive dense-window
  splitting, finite retries and pacing, typed progress traces, and an exact resume boundary.
- Shared durable weighted request-budget gating for public trade and candle sources, with explicit
  Binance Spot and USD-M aggregate-trade costs.
- Dedicated restart-safe public-trade checkpoint and source-health contracts with an immutable
  policy fingerprint, exact pending-leaf recovery boundary, committed-outcome lifetime work
  counters, non-reusable UUID-fenced leases, checkpoint-versioned and bounded-paginated
  provider-symbol-aware health evidence, and a local SQLite control-state store with transactional
  schema installation and validated UTC projections.
- Explicitly invoked bounded public-trade checkpoint orchestration with immutable-policy
  validation, fresh UUID-fenced claims, evidence-first checkpoint progress, typed clean-pause and
  terminal-failure classification, injected UTC time, and recovery tests on both cross-database
  crash seams.
- Typed read-only public-trade transition history with immutable checkpoint-and-actor records,
  ascending contiguous checkpoint-version pages, strict cursor and page bounds, full projection
  and lifecycle validation, restart replay, and fail-closed corruption tests without a schema
  migration.
- Deterministic generated-fixture public-trade recovery evidence across an exhausted disconnect,
  sparse one-millisecond windows, newly constructed evidence, checkpoint, and shared rate-budget
  SQLite adapters, fresh UUID fencing authority, exact causal transition and health history, and
  a no-work completed rerun.
- A design-only [single-host continuous public-trade operating
  contract](decisions/0028-continuous-public-trade-collection-operating-contract.md) defining the
  separate stream, service-run, and bounded-job lifecycles; UTC closed-window cadence; bounded
  catch-up; cooperating-single-host compare-and-swap and fencing; manual drift hold; health;
  capacity-evidence; failure; escalation; resume; and rollback requirements that a separately
  governed future implementation must satisfy. This is neither process-manager nor multi-host
  exclusivity.
- Unused frozen continuous public-trade policy, attachment, checkpoint, plan, and lifecycle
  transition contracts plus a pure fixed-UTC closed-window planner. It returns only `HELD`,
  `WAITING`, or `ATTACHED_JOB` data, preserves an existing immutable attachment, and bounds one due
  candidate exactly from the durable cursor without I/O, persistence, authority, runtime wiring,
  scheduling, provider access, or a continuous-operation claim.
- An evidence-backed canonical UTC boundary inventory covering every discovered model, clock,
  provider edge, JSON/text representation, SQLite projection, order, index, cursor, and test path,
  with an accepted staged compatibility, quarantine, backup, rollback, and migration plan. No
  runtime, schema, or stored-data migration has begun.
- Exact fixed-`datetime.UTC` enforcement at every scoped injected-clock boundary, with
  fail-before-ID/HTTP/storage/reservation/wait/log tests, preserved typed error mappings, and no
  persisted-contract, serialization, schema, projection, or stored-data change.
- Isolated canonical UTC codec primitives for strict fixed-zone validation, explicit aware-input
  normalization, exact six-fractional-digit RFC 3339 `Z` serialization, and strict parsing, with
  deterministic and property-style edge coverage and no existing runtime consumer.
- Exact signed epoch-microsecond bounds, projection, and inverse decoding in the same isolated
  module, using only integer and `timedelta` arithmetic with full calendar-range round trips,
  one-microsecond distinction, monotonic ordering, and no existing runtime consumer.
- Strict frozen versioned SQLite preflight contracts plus a direct immutable read-only inspector
  with pinned logical fingerprints for all eight generated fixture layouts. Exact typed marker
  bytes, normalized DDL, schema objects, columns, foreign keys, explicit and implicit indexes,
  triggers, source identity, directory stability, sidecar rejection, and mutation denial are
  verified before any timestamp row is read. A second unused generated-fixture-only layer reuses
  the same exact snapshot and connection to capture bounded deterministic storage-class and cast
  bytes for all 37 direct timestamp columns. A third unused pure layer consumes only that exact
  evidence and classifies the current writer representations, nullability, fixed-UTC policy,
  malformed values, and supported epoch range without I/O, normalization, replacement bytes, or
  runtime wiring. A fourth unused pure layer retains all outcomes and derives exact fixed-UTC,
  canonical six-digit `Z`, and epoch-microsecond candidate triples only from successful parses,
  with typed calendar overflow and no grouping or runtime wiring.

Phase 2 is not complete. Operating-system service deployment, live streaming, multi-host
rate-budget coordination, durable raw and canonical storage beyond the local SQLite baseline,
additional market-data schemas, operational dashboards, and external alert delivery remain future
tasks. The bounded application orchestrator now maps typed range evidence to `COMPLETED`, `PAUSED`,
or `FAILED`, preserves the exact pending leaf, and advances durable work progress only after market
evidence is durable. Lease claims remain control-only transitions. Crash-durable per-job
pre-request attempt reservations remain future work; the existing shared durable provider-rate
budget is the current pre-request capacity boundary. The append-only transition audit history is
now available through a bounded typed port that revalidates causal versions and the retained actor
fencing authority without mutating control state. TASK-056 now supplies one composed deterministic
recovery drill from failed checkpoint version 3 through completed version 6, with exact sparse
evidence, budget, retry, pacing, transition, health, and no-work rerun assertions. It does not
establish cross-database atomicity, physical durability, automatic recovery, or continuous
operation. TASK-057 now supplies one strict manifest and five exact-byte, versioned synthetic
fixtures for all five active provider payload variants, offline deterministic production-adapter
tests, and a manual schema-drift containment/review/resume runbook. Both Binance aggregate
variants retain the shared optional parser set `M`, `nq`; fixture presence differs without
creating a market-specific parser rule. Representative detectable drift fails closed, while
precision-only changes and some same-typed semantic reorder may parse and therefore require
manual pause and contract review rather than serving as compatibility evidence. The current
public-trade path remains one finite, explicitly invoked bounded job flow: no component schedules
another invocation or owns continuous operation. TASK-058 now records a design-only ADR and
operating contract for a possible future single-host continuous public-trade lifecycle, including
UTC closed-window cadence, bounded catch-up, fencing and restart, manual drift hold, health,
capacity-evidence, failure, escalation, governed resume, and rollback requirements. The design
preserves today's immutable policy, exact pending-leaf, evidence-first, idempotent replay, causal
audit, and shared-budget boundaries. It does not implement or approve production code, runtime,
network activity, a scheduler, daemon, process manager, service, deployment, capacity
configuration, drift detection or automatic drift response, automatic restart, automatic
recovery, automatic resume, failover, or multi-host coordination. Continuous-operation,
automatic recovery, deployment, and Phase 2 readiness remain unproven; implementation requires a
separately governed future task. No stale-heartbeat threshold, capacity value, retention policy,
outage envelope, or settlement-lag adequacy is selected or proven. Settlement lag is not proof
against late provider events; pause cannot cancel in-flight work; dense one-millisecond windows,
separate-database commit seams, crash-uncommitted request counts, physical durability, and
cooperating-single-host-only coordination remain residual risks.
TASK-059 now adds only the unused provider-independent domain boundary described by that decision.
Strict frozen values validate finite whole-millisecond policy, immutable stream and market
identity, complete policy fingerprints, exact fixed-UTC cursors, version and lifecycle causality,
manual hold state, and an optional immutable bounded-job attachment. The pure planner receives
`now` explicitly. A paused stream yields only `HELD`; an existing attachment is returned unchanged
as `ATTACHED_JOB`; a caught-up stream yields `WAITING`; and due work yields one `ATTACHED_JOB`
candidate whose exact half-open range starts at the cursor and ends at the earlier of the latest
eligible epoch-aligned closed boundary or the finite catch-up cap. These are values only: no
repository persists them, no runtime imports the module, and no job or stream is created,
attached, claimed, started, invoked, scheduled, paused, resumed, or recovered. No SQLite/schema,
network, service, deployment, operator data, permission, automatic action, capacity evidence,
multi-host guarantee, or readiness claim was added. TASK-060 was therefore limited to a design-only
persistence-contract decision before any repository or schema proposal. That decision is now
complete: TASK-060 records
[ADR-0029](decisions/0029-continuous-public-trade-stream-persistence-contract.md). The decision
pins the exact TASK-059 checkpoint as future durable current state, keeps planning, service,
fencing, child, evidence, health, and budget domains separate, and defines fail-closed conceptual
create, exact-identity load, and versioned compare-and-swap semantics. It requires a new exact
canonical `child_creation_payload` for the complete deterministic pristine child plus
stream/request binding to commit with an attachment because the TASK-059 creation fingerprint alone
is not reversible; that payload does not redefine the existing bounded-child store serializer. It
also keeps the continuous-stream and bounded-child policy fingerprints distinct. Exact transition
authority and completed-child evidence remain separate bounded references rather than being
inferred from a pause reason or child ID.

ADR-0029 fixes the canonical serialization profile and six distinct child, stream-envelope,
stream-creation, transition-record, evidence-scope, and rolling-history-root digest contracts.
Separate stream-creation and append-only transition records retain immutable
successor-envelope bytes for causal revalidation, and an immutable complete stream-policy
projection for field-level drift checks even when a caller reuses a fingerprint. It defines every
attach, child-create, evidence, checkpoint, completion, stream-advance, hold, and resume crash
disposition without treating separate stores as atomic. ATTACH authority binds exact prior
version/digest/history root, successor version, candidate child, and effective child-policy
fingerprint before trusted time sampling; the finalized transition/root binds the resulting exact
successor. An in-flight response may cross a concurrent hold only under explicit non-integrity
classification, while drift, invalid payload, quality/evidence failure, corruption, or ambiguity
stops canonical admission/progress. The decision specifies version compatibility, typed quarantine,
migration and restore prerequisites, causal retention, and
disable-to-current-bounded-flow rollback before a physical repository is selected. TASK-059 epoch
milliseconds remain exact; attachment fails before commit if the existing bounded-child model
cannot represent the required datetime, while a later physical projection must fail closed if
ADR-0027 signed epoch microseconds cannot represent the value. No code, codec, port, repository,
SQLite/DDL/schema, migration, runtime, provider, operator data, permission, automatic action,
capacity, physical-durability, multi-host, recovery, deployment, readiness, or Phase 2 claim was
added.

TASK-061 is complete as one pure, unused RISK-1 domain increment. It adds strict version-one
child-creation payload, stream-envelope, stream-creation, stream-transition,
evidence-reference/evidence-scope, and complete stream-policy projection values; exact compact
sorted-key UTF-8 JSON codecs with explicit raw and lexical bounds; and six domain-separated
child/envelope/create/transition/scope/history digest contracts. A pure two-pass attachment
finalizer joins the unchanged TASK-059 planner to exact child material. Load validators bind full
effective stream policy field by field, immutable identity and creation evidence, and the effective
child policy while attached; create-scope, transition-scope, evidence-validity, predecessor/
successor, recorded-time, and rolling-root validators enforce exact causal links. Deterministic
golden-byte, hostile-input, every-transition, exact-boundary, and property tests cover this isolated
surface.

No port, repository, adapter, SQLite/database/schema, migration, I/O, runtime/network path,
authority, action, capacity, physical durability, multi-host guarantee, automatic recovery,
deployment, or readiness was added. TASK-059 behavior, ADR-0028, and the current explicitly invoked
bounded public-trade flow remain unchanged.

ADR-0030 now freezes the current TASK-062 RISK-1 increment as one unused lower-level logical
stream-store port for finalized TASK-061 records. The contract preserves original canonical bytes,
exact decoded records, digests, rolling roots, complete identity and policy bindings, one embedded
successor per compare-and-swap, closed create/load/mutation outcomes, and audit pages of 1 through
100 new records with at most one predecessor overlap. The public
`validate_continuous_public_trade_stream_audit_page` function binds every returned page to its exact
query, complete policy, limit, anchor, and TASK-061 transition links. Current reads remain
constant-size. Every output is a store-local classification; only accepted receipts, `FOUND`,
`PAGE`, and a validated `AT_TAIL` anchor carry bounded structural evidence, while `UNAVAILABLE`
carries no coherent classification. None grants authority or constitutes an accepted attestation.

TASK-062 selects no physical store and adds no adapter, transaction implementation, database,
schema, path, migration, I/O, clock, evidence-body access, attestation service, fence, budget,
runtime, retry loop, automatic action, capacity value, durability, recovery, deployment, or
readiness claim. TASK-063 remains queued until TASK-062 is `COMPLETE`; it is only a proposed
design-only physical stream-store architecture and evidence plan resolving exact ADR-0027 epoch
representation, schema/index and transaction mapping, retention/compaction, migration,
backup/restore, crash evidence, and capacity before any adapter. It remains separately governed and
grants no physical implementation authority. TASK-037 remains blocked and authorization remains
denied.

`RISK-005` remains open: the accepted plan selects Python
datetimes in the fixed `datetime.UTC` zone, fixed microsecond RFC 3339 `Z` text, and derived
epoch-microsecond SQL projections. New injected clock values are fixed-UTC, and the isolated pure
module now provides the complete unused Stage 2 primitive foundation: strict validation, explicit
normalization, exact canonical text, and exact integer-only epoch-microsecond conversion across
Python's full calendar range. Exact fixture fingerprints and immutable inspection now establish
the unused synthetic layout prerequisite, TASK-031 captures raw storage-class and cast-byte
evidence for all 37 declared timestamp columns from generated fixtures only, and TASK-032
interprets those bytes through the exact 20 offset-preserving text, 15 fixed-UTC text, two epoch
integer, and five nullable declarations. It preserves every source byte and types malformed,
naive, absent, offset-policy, overflow, and storage-class outcomes without I/O or normalization.
TASK-033 now derives exact fixed-UTC datetime, canonical six-digit `Z` text, and epoch-microsecond
candidate triples from successful outcomes while leaving failures and year-boundary overflow
nonprojectable. Equal instants retain their distinct ordered source evidence. TASK-034 now adds
one exact family-scoped summary for every declared timestamp column, including empty columns, and
collectively reconciles all eight families, 20 tables, and 37 columns through exhaustive status
counts, source-offset and precision frequencies, and projectable epoch extrema. TASK-035 now
retains the complete canonical sequence of eight exact TASK-034 results and reconciles only their
existing evidence into one strict pure 8-family, 20-table, 37-column bundle aggregate. TASK-036
now adds only an unused strict frozen proposal contract pinned to TASK-035's private exact bundle
plan. Its eight ordered family-linked immutable literal slots are not paths, its snapshot, destination, and
retention/disposal placeholders remain unselected, and its proposal, approval, operator-access,
and Stage 3 states are fixed to no authority. The eight slots prove synthetic family coverage
only; they do not assert that a future real deployment has one path per family. No operator
database has been scanned, no replacement timestamp has been selected or emitted, and current
aware-only models, external text, sortable runtime projections, queries, and legacy stores have
not been migrated.

Within the canonical UTC migration track, the next gated action remains TASK-037,
`phase2.canonical_utc_preflight_operator_authorization_package_owner_decision`. TASK-037 remains
blocked and authorization remains denied. The repository's current executable next action is
maintained in `PROJECT_STATE.json` and may identify separately governed RISK-1 fail-closed work
that neither accesses operator data nor changes TASK-037 authority. If its resume condition is
met, TASK-037 is a RISK-3 authorization decision that requires project-owner approval plus
independent Risk and Security review. It may prepare the exact proposed
read-only path list and real cardinality, writer-fenced consistent/immutable snapshot procedure,
report destination, retention/disposal boundary, and the change, scope, environment, evidence,
approver, UTC time, expiry or review trigger, monitoring, and rollback required by policy. It
cannot inspect, resolve, check, or open a proposed operator path or database; access SQLite or
operator data; scan rows; invoke an adapter; create an operational report or manifest; add
serialization or scanner code; wire a runtime; migrate or repair data; alter a schema; or perform
or claim Stage 3. Approved governance-artifact writes are the only filesystem mutation in scope.
Missing or ambiguous authority remains denial. Any approved scanner remains a separately scoped
later task. TASK-037's exact scope and acceptance gates are in `BACKLOG.md`, and its RISK-3 human
approval and independent review requirements are mirrored in `PROJECT_STATE.json`.

## Phase 1 — Architecture and Engineering Foundation

### Objective

Create a small, enforceable engineering foundation that makes later work reproducible, reviewable, testable, and safe to change.

### Deliverables

- Approved project charter, department map, architecture, and roadmap.
- Repository-level `AGENTS.md` defining how Codex and contributors work in this repository.
- Technology decision records for language, package management, application shape, storage, workflows, and deployment approach.
- Initial repository structure aligned with architectural responsibilities.
- Dependency and configuration conventions.
- Environment separation for development, test, research, paper, and live modes.
- Secret-handling rules and committed example configuration without real credentials.
- Formatting, linting, type checking, unit-test, and security-baseline commands.
- Continuous-integration checks for every proposed change.
- Versioned domain-contract conventions.
- Structured logging, health, error, and correlation-ID conventions.
- Definition of Done and Pull Request expectations.

### Exit Gate

Phase 1 is complete only when:

- A new contributor or Codex session can understand how to build, test, and review the project from repository guidance.
- The minimal application skeleton runs in a local isolated environment.
- Automated checks pass on a clean checkout.
- No real secret is stored in source control.
- Environment and operating-mode boundaries are explicit.
- A small sample domain event can be validated, stored, logged, and tested end to end.

### Not Included

- Live exchange connectivity.
- Trading strategies.
- AI model integration.
- Real portfolio or order handling.

## Phase 2 — Reliable Market Data Platform

### Objective

Build a provider-independent data foundation that can collect, validate, normalize, store, and replay cryptocurrency market data.

### Deliverables

- Canonical schemas for venue, instrument, trade, candle, order book, ticker, funding, open interest, liquidation, and source health.
- First exchange market-data adapter using public or read-only access.
- Historical-data ingestion path.
- Live-stream ingestion path.
- Timestamp, sequence, duplicate, range, and freshness validation.
- Gap detection and explicit missing-data representation.
- Raw and canonical storage with lineage.
- Cross-source reconciliation for selected records.
- Data-quality metrics, dashboards, and alerts.
- Rate-limit, reconnect, backoff, and provider-failure handling.
- Deterministic data export for replay and research.

### Exit Gate

Phase 2 is complete only when:

- The same input produces the same canonical records.
- Known gaps, duplicates, stale records, and malformed records are detected by tests.
- A selected market can be collected continuously through planned disconnect and recovery tests.
- Stored records identify their source, event time, observation time, and processing time.
- A second adapter can be added without changing downstream domain contracts.
- Data quality is observable by asset, venue, stream, and timeframe.

### Not Included

- Trading decisions.
- Private account data.
- Order submission.
- News, sentiment, macro, or on-chain sources.

## Phase 3 — Backtesting and Market Replay

### Objective

Create a trustworthy evaluation environment that reproduces past information flow without future-data leakage.

### Deliverables

- Event-driven market replay using canonical records.
- Clock abstraction shared by replay, paper, and live workflows.
- Deterministic experiment configuration and random-seed handling.
- Baseline strategy interface and simple non-AI reference strategies.
- Fee, spread, funding, slippage, latency, partial-fill, and rejection models.
- Look-ahead, survivorship, timestamp, and data-leakage tests.
- Experiment manifests containing data, code, configuration, and policy versions.
- Performance reports with returns, drawdown, exposure, turnover, costs, and regime breakdowns.
- Reproducibility checks comparing repeated experiment runs.

### Exit Gate

Phase 3 is complete only when:

- Repeating an experiment from the same manifest produces equivalent results.
- Tests demonstrate that future information cannot enter a decision.
- Costs and execution assumptions are visible and configurable.
- A baseline strategy can be replayed from data ingestion through evaluation.
- Failed and abstained decisions are retained, not only executed trades.

### Not Included

- Claims of profitability.
- Adaptive agent weighting.
- Real orders.

## Phase 4 — Deterministic Risk and Portfolio Engine

### Objective

Build an independent capital-protection layer before any order-capable integration exists.

### Deliverables

- Canonical portfolio, balance, position, exposure, and pending-order state.
- Position-sizing interface independent of strategy logic.
- Configurable limits for trade risk, aggregate exposure, leverage, concentration, correlation, daily loss, drawdown, and open positions.
- Required invalidation and protective-order rules.
- Approval, reduction, rejection, expiry, and halt records with reason codes.
- Risk pre-check and final Risk Gateway.
- Kill switch and recovery-state model.
- Portfolio attribution and capacity reporting.
- Stress scenarios for price gaps, volatility spikes, stale data, exchange outage, and inconsistent account state.
- Policy versioning and replay support.

### Exit Gate

Phase 4 is complete only when:

- No strategy or model can bypass the Risk Gateway in tests.
- Property and scenario tests demonstrate that exposure never exceeds configured bounds.
- Stale, missing, invalid, or inconsistent critical state produces rejection or halt.
- Risk decisions can be replayed from their original evidence and policy version.
- The kill switch blocks new risk and has a tested recovery procedure.

### Not Included

- Exchange credentials.
- Real order submission.
- AI-controlled risk limits.

## Phase 5 — Execution and Paper Trading

### Objective

Build an order lifecycle that behaves safely under normal, rejected, delayed, partial, duplicated, and uncertain outcomes, first through simulation.

### Deliverables

- Versioned order-intent and execution contracts.
- Paper exchange or simulator using live and replayed market data.
- Idempotent client order IDs and duplicate prevention.
- Order acknowledgement, partial fill, fill, rejection, cancellation, and expiry handling.
- Fee, funding, slippage, and realized/unrealized P&L accounting.
- Position, balance, order, and fill reconciliation.
- Safe retry and unknown-state handling.
- Execution metrics and alerts.
- Continuous paper-trading service with restart recovery.
- Operational runbooks for pause, resume, reconcile, and incident response.

### Exit Gate

Phase 5 is complete only when:

- Fault-injection tests do not create duplicate orders or unbounded exposure.
- Restart and reconnect tests reconstruct consistent state.
- Internal state reconciles with the simulator after partial fills and failures.
- Paper trading can operate continuously for a defined observation period with no unresolved critical incidents.
- Every paper order can be traced to a proposal and risk approval.

### Not Included

- Real trading.
- Human approval interface.
- Complex AI strategies.

## Phase 6 — Core Specialist Analysis Agents

### Objective

Add the first independent analytical departments on top of the trusted data and evaluation foundation.

### Deliverables

- Common versioned opinion contract supporting direction, confidence, uncertainty, evidence, expiry, and abstention.
- Technical Analysis agents.
- Market Structure agents.
- Derivatives agents.
- Independent Bull Case and Bear Case generation.
- Data Quality Officer output for each decision cycle.
- Static strategy composition rules.
- Per-agent evaluation, calibration, drift, and regime reports.
- Shadow execution that records decisions without affecting paper orders until approved.

### Exit Gate

Phase 6 is complete only when:

- Each agent can be disabled or replaced independently.
- Agent outputs are schema-valid, versioned, time-bounded, and replayable.
- Agents can abstain and expose missing evidence.
- Evaluation includes opposing signals, rejected proposals, and regime breakdowns.
- No agent can access the execution interface directly.

### Not Included

- News, sentiment, macro, and on-chain agents.
- Adaptive live weights.
- Real orders.

## Phase 7 — Multi-Agent Investment Committee

### Objective

Coordinate specialist agents into an explainable decision process that preserves disagreement and remains subordinate to deterministic risk controls.

### Deliverables

- Decision-cycle orchestration and timeout behavior.
- Specialist, manager, Bull Case, Bear Case, Data Quality Officer, Risk Officer, and committee roles.
- Evidence and dissent aggregation.
- Committee decision contract and reason codes.
- Strategy proposal selection within preliminary risk bounds.
- Final deterministic Risk Gateway after committee selection.
- Confidence and disagreement calibration.
- Full replay of committee decisions.
- Advisory and paper-mode output formats.

### Exit Gate

Phase 7 is complete only when:

- The complete decision can be reconstructed from evidence and versions.
- A missing critical role or timed-out dependency produces a defined degraded outcome.
- Dissent is retained and visible in evaluation.
- The committee cannot override a final Risk rejection.
- Multi-agent performance is compared against simple baselines, not evaluated in isolation.

### Not Included

- Uncontrolled free-form agent communication.
- Direct committee access to exchange order APIs.
- Self-modifying agent prompts in live operation.

## Phase 8 — News, Sentiment, Macro, and On-Chain Intelligence

### Objective

Expand the evidence base with carefully validated external information while treating content as untrusted data.

### Deliverables

- Source policy, provenance, licensing, retention, and citation rules.
- News event ingestion, verification, deduplication, novelty, and asset mapping.
- Sentiment measurement with manipulation and source-quality indicators.
- Macro event calendar, cross-asset context, and event-risk windows.
- On-chain metrics with label confidence and coverage limits.
- Prompt-injection-resistant content processing boundary.
- Time-aware historical datasets for evaluation.
- Source-specific quality, latency, cost, and drift monitoring.
- Ablation reports showing the incremental contribution of each source family.

### Exit Gate

Phase 8 is complete only when:

- Every external claim retains source and time lineage.
- Untrusted content cannot invoke tools or change system instructions.
- Corrections and conflicting reports are represented explicitly.
- Historical evaluation uses only information available at the decision time.
- Each source family demonstrates measurable incremental value or remains disabled.

### Not Included

- Web content as executable instruction.
- Automatic trading based on a single article, post, or on-chain event.

## Phase 9 — Evaluation and Adaptive Agent Weighting

### Objective

Learn which agents and strategies are reliable under specific market regimes without allowing adaptation to escape controlled evaluation.

### Deliverables

- Unified decision and outcome dataset from Audit.
- Agent and strategy scorecards by asset, venue, timeframe, and regime.
- Calibration, drift, attribution, and stability analysis.
- Champion-challenger evaluation.
- Offline adaptive weighting experiments.
- Regularization, minimum-evidence, and turnover constraints.
- Shadow-mode weight updates.
- Rollback thresholds and frozen baseline comparisons.

### Exit Gate

Phase 9 is complete only when:

- Adaptive methods outperform static baselines out of sample after realistic costs.
- Weight changes are versioned, bounded, explainable, and reversible.
- Sparse evidence cannot produce extreme allocation changes.
- Drift or data-quality failure freezes adaptation safely.
- Shadow results remain consistent with replay expectations within defined tolerances.

### Not Included

- Direct online learning with unrestricted live capital.
- Automatic strategy generation and promotion.

## Phase 10 — Controlled Self-Improvement

### Objective

Create a governed loop in which the system proposes improvements and Codex turns approved proposals into tested, reviewable candidate changes.

### Deliverables

- Standard improvement-proposal contract containing hypothesis, evidence, scope, risk, and acceptance criteria.
- Experiment queue and isolated evaluation environments.
- Codex Engineering Agent workflow using repository `AGENTS.md` and scoped permissions.
- Automatic branch or worktree creation for approved tasks.
- Required unit, integration, replay, backtest, security, and regression evidence by change type.
- Pull Request preparation with limitations and rollback plan.
- Independent review and approval gate.
- Promotion ladder from research through restricted operation.
- Automatic rollback triggers for promoted candidates.

### Exit Gate

Phase 10 is complete only when:

- A proposal can travel from evidence to a reviewable Pull Request without touching the live system.
- Codex cannot merge, deploy, change risk policy, or access production trading credentials by itself.
- Failed tests and negative experiments are retained and visible.
- Promotion and rollback are exercised in a non-live environment.
- Every generated change is traceable to its originating evidence and approval.

### Not Included

- Self-authorizing code changes.
- Direct mutation of production by an AI agent.
- Removal of human or independently governed approval.

## Phase 11 — Semi-Automatic Trading

### Objective

Introduce tightly controlled real trading in which the system prepares a complete proposal and a human explicitly approves each permitted action.

### Deliverables

- First exchange private adapter with trading permission and no withdrawal permission.
- IP restrictions and narrowly scoped secret access.
- Dedicated restricted account or sub-account.
- Human approval interface with proposal, evidence, risk, price movement, expiry, and reject controls.
- Revalidation after approval and immediately before execution.
- Small, fixed exposure and leverage bounds.
- Live reconciliation, monitoring, alerts, and manual kill switch.
- Operator runbooks and incident drills.
- Paper-versus-live execution comparison.

### Exit Gate

Phase 11 is complete only when:

- Every real order has explicit human and deterministic Risk approval.
- Withdrawal capability is absent and verified.
- Approval expiry, price drift, duplicate action, and stale-state tests pass.
- Live balances, positions, orders, and fills reconcile continuously.
- Restricted live operation completes a defined observation period without unresolved critical incidents.
- The user can stop trading independently of AI and application health.

### Not Included

- Broad autonomous trading.
- Unbounded capital or leverage.
- Multiple live exchanges at initial launch.

## Phase 12 — Restricted Automatic Trading

### Objective

Allow selected, proven strategies to execute automatically inside a narrowly defined and continuously monitored safety envelope.

### Deliverables

- Strategy, asset, venue, session, exposure, leverage, and loss allowlists.
- Automatic-mode authorization separate from application deployment.
- Conservative initial capital allocation.
- Real-time risk, reconciliation, and operational-health gating.
- Automatic halt on policy breach, drift, inconsistent state, missing data, or unresolved execution uncertainty.
- Progressive rollout and rollback controls.
- Live champion-versus-shadow comparison.
- Periodic human review and reauthorization.
- Complete post-trade and incident reporting.

### Exit Gate

Phase 12 is complete only when:

- Automatic operation cannot exceed its approved envelope.
- Every safety gate and kill switch has been tested under failure injection.
- Performance remains within predefined risk, execution, and drift tolerances.
- Rollback to paper or semi-automatic mode is immediate and tested.
- Expanding assets, capital, leverage, strategies, or exchanges requires a new explicit approval.

### Not Included

- Unrestricted autonomy.
- Unlimited strategy generation or capital allocation.
- Removal of deterministic risk and audit controls.

## Cross-Cutting Workstreams

The following work continues across all phases:

### Security

Threat modeling, least privilege, secret isolation, dependency review, secure defaults, incident response, and credential rotation.

### Testing and Quality

Unit, integration, contract, property, replay, fault-injection, performance, and regression testing appropriate to each capability.

### Observability and Operations

Structured logs, metrics, traces, alerts, dashboards, runbooks, backups, recovery drills, and capacity planning.

### Data Governance

Source lineage, schema versioning, data quality, retention, privacy, licensing, reproducibility, and prevention of future-data leakage.

### Evaluation Governance

Predefined hypotheses, honest baselines, realistic costs, out-of-sample validation, regime analysis, negative-result retention, and promotion criteria.

### Documentation

Architecture decisions, contracts, operating procedures, failure modes, experiment records, release notes, and user-facing explanations.

### Cost Control

Provider cost, model usage, storage, compute, data retention, and operational capacity measured against the value of each capability.

## Major Milestones

| Milestone | Result |
|---|---|
| M0 — Defined System | Charter, departments, architecture, and roadmap are approved. |
| M1 — Reproducible Foundation | Clean checkout builds, validates, tests, and runs a minimal end-to-end event. |
| M2 — Trusted Data and Replay | Market evidence is validated, stored, and replayed deterministically. |
| M3 — Deterministic Trading Core | Portfolio, risk, execution simulation, reconciliation, and audit operate end to end. |
| M4 — Stable Paper System | Continuous paper operation survives tested failures and produces trusted evaluation data. |
| M5 — Multi-Agent Intelligence | Specialist agents and the committee produce replayable, evaluated decisions. |
| M6 — Governed Evolution | Learning and Codex produce tested Pull Requests without direct production authority. |
| M7 — Human-Approved Live | Restricted real trades require explicit human and deterministic Risk approval. |
| M8 — Restricted Autonomy | Selected strategies operate automatically inside an approved safety envelope. |

## Global Promotion Checklist

Before any capability moves to a more autonomous or higher-risk environment:

- Scope and owner are explicit.
- Inputs, outputs, versions, and lineage are defined.
- Failure and degraded-mode behavior are tested.
- Required unit, integration, replay, fault, and security checks pass.
- Results are evaluated out of sample with realistic costs.
- Known limitations and negative results are documented.
- Observability and alerts are active.
- Permissions follow least privilege.
- Audit and replay evidence is complete.
- Rollback or disablement is tested.
- Required human or independent approval is recorded.

## Task Planning Method

Only the active phase is converted into detailed work. Each work item should:

- Fit into one focused session of approximately two hours whenever practical.
- Have one clear objective and a small, reviewable scope.
- Identify exact files or artifacts to create or change.
- State prerequisites and prohibited scope.
- Define objective completion evidence.
- End with tests, review, documentation, or a decision record as appropriate.

The next task is selected only after the current task is reviewed and accepted. Record that one
action in `PROJECT_STATE.json` and its acceptance contract in `BACKLOG.md`. Later phases remain
directional until earlier evidence justifies refining them.
