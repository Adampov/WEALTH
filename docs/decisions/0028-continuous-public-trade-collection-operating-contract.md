# ADR 0028: Continuous Public-Trade Collection Operating Contract

- **Status:** Accepted
- **Date:** 2026-07-27
- **Decision owners:** Project owner, Market Data Department, Market Structure Department,
  Engineering Department, Security Department, and Audit and Assurance Department

## Task Contract

### Goal

Select one bounded, single-host operating contract for a possible future continuous public-trade
collector before any runtime, scheduler, service, deployment, or persistence implementation is
proposed.

### Context

The current public-trade path is finite and explicitly invoked. ADR 0023 defines immutable bounded
job identity, policy fingerprints, exact pending-leaf recovery, UUID-fenced leases, checkpoint
state, and causal source-health evidence. ADR 0025 composes that state with the adaptive range
collector, fail-closed evidence admission, and the shared durable request budget. ADR 0026 exposes
the append-only transition ledger through a bounded typed reader. ADR 0027 defines the canonical
UTC target and the migration limits that remain open.

TASK-057 adds an exact-byte synthetic fixture corpus and a manual public-provider schema-drift
response runbook. It does not add a detector, a runtime pause control, or permission to collect
continuously.

The existing continuous candle collector and its local service lifecycle are candle-specific
implemented capabilities. They are useful design evidence, but they are not a public-trade
scheduler, deployment contract, or authority to reuse their runtime values or stored models for
aggregate trades.

### Scope

This decision defines, conceptually:

- one future single-host component boundary and its ownership;
- lifecycle, cadence, catch-up, exclusivity, restart, clean-stop, and shutdown rules;
- preservation of the current bounded-job fencing, checkpoint, evidence, health, and request-budget
  contracts;
- a manual schema-drift hold and governed resume boundary;
- separate source-health and service-health meanings, internal alert evidence, and escalation;
- capacity inputs and measurements that must exist before implementation;
- failure, rollback, review, and future implementation-evidence requirements.

### Constraints

This is design and documentation only. It does not:

- add or change production source, runtime wiring, network access, an endpoint, adapter, parser,
  request, scheduler, daemon, operating-system service, deployment, WebSocket, persistence, SQLite
  schema, dependency, lockfile, credential, permission, notification, or operating mode;
- select a process manager, trigger technology, database layout, host path, polling interval,
  settlement lag, window duration, lease duration, heartbeat interval, stale threshold, capacity,
  retention period, or other deployment value;
- inspect an operator path or database, reserve provider capacity, start collection, or schedule
  another invocation;
- authorize automatic drift detection, pause, remediation, resume, failover, restart, or
  multi-host coordination; or
- claim continuous operation, recovery, deployment, capacity adequacy, physical durability, or
  Phase 2 readiness.

### Done When

The selected composition and its invariants, alternatives, consequences, non-goals, review
triggers, and future evidence are explicit enough that a later implementation task can be bounded
and reviewed without treating this ADR as runtime authority.

### Not Included

- Any implementation of the selected design.
- A scheduler, service manager, CLI, dashboard, notification delivery path, deployment manifest, or
  host configuration.
- New serialized contracts, control tables, migrations, retention, backups, or repair tooling.
- Provider failover, WebSockets, private data, credentials, account access, strategies, signals,
  portfolio state, Risk decisions, orders, execution, or financial action.
- TASK-037 restricted inputs, approval, migration authority, or Stage 3 work.

## Current State and Future Contract

The distinction below is normative:

| Boundary | Current accepted capability | Possible future capability selected here |
|---|---|---|
| Invocation | A caller explicitly invokes one finite bounded public-trade job. | A finite run coordinator may discover due closed windows and invoke bounded jobs, but only after a separate implementation and deployment decision. |
| Progress | One immutable `[window_start, window_end_exclusive)` job retains its exact cursor and pending leaf. | A separate continuous-stream cursor may attach one bounded child job at a time and advance only to that completed child's exact end. |
| Exclusivity | The bounded child job uses compare-and-swap and a fresh UUID fencing token. | A single-host stream owner must add an outer fenced claim so two triggers cannot select overlapping child work; the child fence remains independently mandatory. |
| Scheduling | None. Re-invocation is external and explicit. | A trigger boundary may invoke one finite run. This ADR does not choose, start, or configure that trigger. |
| Restart | A new explicit invocation can reclaim an eligible bounded job and resume its exact pending leaf. | A new explicit or separately supervised run may recover the attached child and stream cursor after validation; no automatic restart is authorized here. |
| Drift response | Typed invalid payloads fail closed; TASK-057 supplies a manual runbook. | An authorized manual hold can block the exact affected request variant and a governed manual resume can release it. |
| Health | Bounded job source-health and transition evidence are durable and causal. | Future service-lifecycle evidence may interpret liveness separately from source health; no monitor, delivery path, or restart action exists yet. |

No future column in this table describes an implemented component.

## Decision

### Selected conceptual composition

Select a **single-host, finite-run coordinator around the existing bounded public-trade
orchestrator**. The future composition has these responsibilities:

1. An unselected external trigger asks for one finite run. Triggering grants no market-data,
   storage, or recovery authority of its own.
2. A future continuous-stream coordinator validates its immutable stream identity, versioned
   operating policy, manual holds, current cursor, attached child identity, and outer single-host
   fencing authority before network work.
3. The coordinator calculates the latest eligible closed UTC boundary and either returns a clean
   no-work outcome or selects a finite catch-up interval.
4. It durably attaches one immutable bounded child job UUID, exact range end, deterministic
   creation inputs, and complete child policy fingerprint before creating or invoking that job.
5. The existing `PublicTradeCollectionOrchestrator` continues to own the bounded job, fresh UUID
   job fence, exact pending leaf, adaptive finite range work, evidence admission, source-health
   observation, and checkpoint transition.
6. Every provider request still passes through the one shared durable single-host request-budget
   gate immediately before the request.
7. Only a completed child permits the continuous cursor to advance to the child's exact exclusive
   end and clear the attachment. The attachment remains exact through bounded-job `PAUSED` and
   `FAILED` states, service stop, shutdown, lost fence, or an uncertain result.
8. A future lifecycle-evidence boundary records the run state, cursor and child versions, fence
   correlation, source/service health interpretation, and clean terminal reason. It sends no
   external alert and performs no restart.

The coordinator owns only continuous stream selection and lifecycle composition. Provider
semantics remain in adapters; range behavior remains in the adaptive collector; evidence admission
remains in the existing quality and stores; the bounded job remains in the current orchestrator;
request capacity remains in the shared budget; and deployment supervision remains outside this
decision.

### Core invariants

A future implementation must preserve all of the following:

1. **Immutable work identity.** Source, venue, canonical instrument, provider symbol, instrument
   type, request variant, initial cursor, child range, and applicable versioned policies do not
   change inside an attached child job.
2. **Exact policy identity.** Every child uses the complete existing public-trade policy
   fingerprint. A continuous-policy change is explicit and versioned and cannot silently alter an
   attached or recoverable job.
3. **Closed UTC windows only.** Every selected half-open range is aligned to the configured UTC
   grid and ends no later than the latest settled boundary. No open or future interval is
   requested.
4. **Finite work.** Every invocation, child range, number of child jobs, request count, record
   count, adaptive split, retry, pacing wait, and catch-up horizon has a validated finite bound.
   Reaching a local bound is a clean result, not permission to widen it. Per-operation network
   timeouts do not establish a total wall-clock or shutdown-duration guarantee.
5. **No overlap among cooperating local processes.** One stream has at most one outer owner and
   one attached bounded child among processes that share the same continuous-control and
   request-budget stores. The next range begins at the durable continuous cursor and no later
   cooperating trigger may bypass the attachment. This is not process-manager or multi-host
   exclusivity.
6. **Fresh fencing.** Every outer acquisition and every bounded-job acquisition uses a fresh UUID
   fencing token, exact expected version, bounded lease, and non-reuse rule. Stale, expired,
   missing, or conflicting authority cannot request work or advance control state.
7. **Evidence before progress.** Accepted raw and canonical evidence commits before the child
   checkpoint advances; the child must complete before the continuous cursor advances.
8. **Idempotent refetch.** A crash between evidence and control commits may repeat the exact
   retained leaf. Existing idempotent evidence admission resolves the replay; the coordinator does
   not skip or synthesize progress.
9. **Causal audit.** Stream lifecycle evidence identifies the run, stream, attached child,
   checkpoint and transition versions, outer and child authority where applicable, request
   variant, effective policy versions, UTC observation time, and bounded reason code.
10. **One capacity gate.** All public-trade streams sharing a provider budget on the host use the
    same durable budget key and coordinator. A trigger, stream lease, or catch-up plan does not
    reserve or bypass capacity.
11. **Fail closed.** Policy drift, clock regression, corruption, ambiguous source identity,
    unknown schema, lost authority, or missing causal evidence stops progress without changing the
    accepted cursor.

These are ordered logical guarantees. Evidence and control remain in separate SQLite databases;
there is no cross-database transaction. Committed checkpoint counters are not crash-durable
per-job reservations for every attempted request. SQLite and `synchronous=FULL` behavior do not
establish a claim of physical durability across every device, filesystem, power, or host failure.
The design is single-host only and does not provide multi-host exclusivity.

## Three Separate Lifecycle Contracts

The future composition must not collapse stream intent, one service invocation, and one bounded
child into a single state machine. The names below are conceptual; this ADR adds no enum,
serialized model, table, or runtime transition.

### Continuous stream control

| State | Meaning | Permitted transition |
|---|---|---|
| `ACTIVE` | A separately approved trigger may start one finite service invocation. The exact continuous cursor and any attached child remain authoritative. | An authorized manual pause changes the exact version to `PAUSED`. A clean service stop, caught-up result, child work bound, or run limit leaves the stream `ACTIVE`. |
| `PAUSED` | New child selection and new bounded-job recovery are blocked. The cursor, exact attachment, policies, fences, and evidence remain intact. | Only an explicit authorized resume, after the applicable failure or drift gates pass, changes the exact version to `ACTIVE`. |

A manual pause is observed before the next outer attach/claim or bounded-child invocation, after
an invoked child returns, or during a supervisor-owned interruptible wait. Once observed, it
prevents new outer work. It does not cancel or gate attempts or requests already governed inside
an invoked bounded child; those finish or fail under the child's existing finite policy and
still-valid authority. Evidence returned by that child is handled under the existing
evidence-first rules. No supervisor automatically pauses or resumes the stream.

### Per-invocation service lifecycle

Each future finite invocation has one new run identity and follows:

`STARTING -> RUNNING -> STOPPED | PAUSED | FAILED | RUN_LIMIT`

- `STARTING` validates the `ACTIVE` stream version, UTC clock, policies, holds, exact attachment,
  control/evidence identities, outer fence, and shared-budget composition before network work.
- `RUNNING` selects, recovers, or invokes one attached bounded child at a time under finite work
  bounds.
- `STOPPED` is a clean invocation end caused by `caught_up`, `waiting`, or
  `shutdown_requested`. It leaves the stream `ACTIVE`, preserves stream identity, cursor, counters,
  and any incomplete attachment, and schedules no successor.
- `PAUSED` means the stream was already manually `PAUSED` or an operator completed the manual
  pause while the invocation reached its next safe boundary. It performs no further work.
- `FAILED` records an operational failure such as lost authority, conflict, corruption, invalid
  configuration/clock, or untrusted lifecycle evidence. It does not retry or change stream state
  automatically.
- `RUN_LIMIT` is a clean finite end after the configured invocation work bound. It leaves the
  stream `ACTIVE` and preserves any incomplete attachment.

`caught_up`, `waiting`, `work_limit_reached`, and `shutdown_requested` are bounded outcomes or
reason codes, not stream states and not permission to schedule another invocation.

### Existing bounded public-trade job

The attached child retains its accepted `PENDING`, `RUNNING`, `PAUSED`, `FAILED`, and `COMPLETED`
states unchanged.

A child `PAUSED` caused by its request or record bound is a safe, finite continuation point. The
continuous stream remains `ACTIVE`; a later finite invocation must recover the same child and exact
pending leaf before selecting new work. It is not an operator hold.

A child `FAILED`, an outer or child conflict, lost lease, corrupt state, or schema suspicion stops
the service and requires an operator decision. The operator manually changes the stream to
`PAUSED` before any recovery. The supervisor does not automatically reclaim, recreate, replace,
pause, or resume the child or stream.

Only an attached child durably `COMPLETED` at its exact end permits one continuous cursor advance
and attachment clear. A completed child discovered after process reopen advances the continuous
cursor without another provider request. Child completion never completes the continuous stream.

Every transition uses the exact previous version and current fencing authority. An absent,
ambiguous, invalid, or out-of-order transition fails closed. No state schedules its own successor.

## UTC Cadence and Bounded Catch-Up

The future operating policy must supply, with no implicit default:

- a positive whole-millisecond UTC window duration and its grid anchor;
- a finite non-negative provider settlement lag;
- a finite maximum catch-up range and maximum child jobs per invocation;
- finite per-child range, request, record, split, retry, and pacing bounds;
- the shared budget key, capacity, period, and exact request cost/weight; and
- a finite service run-work bound and shutdown-check boundaries.

At one injected canonical UTC instant, define:

`eligible_end = floor_to_configured_utc_grid(now - settlement_lag)`

The durable continuous cursor is the first event-time millisecond not yet covered by a completed
child. If the cursor is at or beyond `eligible_end`, the finite run returns `caught_up` without a
provider request. Otherwise:

`target_end = min(eligible_end, cursor + max_catchup_span)`

It selects one half-open child range beginning exactly at the cursor and ending no later than
`target_end` or any tighter existing child-work bound.

Selection never rounds a cursor forward, requests the currently open window, fills a gap, or
changes an attached range. Settlement lag and window duration are provider/request-variant policy,
not facts inferred from one response. A later implementation must establish their exact values
from reviewed provider contracts and measured evidence.

The trigger cadence is deliberately unselected. A future operating task must show how its chosen
single-host trigger invokes a finite run, prevents overlap, handles missed triggers without a
backlog storm, and remains interruptible. This ADR neither installs nor recommends a particular
scheduler.

ADR 0027 remains binding. A future scheduler must not use unsafe mixed-offset version-1 timestamp
text as chronological authority. Canonical UTC validation, causal version order, and any required
compatibility or migration work must be separately completed before a time-indexed runtime relies
on that state.

## Fencing, Restart, and Recovery

The selected design uses two distinct authority boundaries:

- The future **outer stream claim** prevents two local triggers from selecting overlapping work
  for one continuous stream. It requires compare-and-swap, a fresh non-reused UUID, a bounded
  lease, and a durable acquisition/transition trail.
- The existing **bounded child claim** continues to protect the exact child checkpoint through its
  own fresh UUID, version, lease, and compare-and-swap checks.

An operating-system process lock or scheduler's non-overlap option may be additional defense but
cannot replace either durable fence. Shared local SQLite state is a single-host baseline, not a
distributed lock.

Before child creation or network work, the outer owner records the exact child job UUID,
`[start, end)` range, deterministic creation inputs, and complete policy fingerprint. It never
replans or replaces that attachment. The attachment remains unchanged while the child is
`PENDING`, `RUNNING`, `PAUSED`, `FAILED`, or while a service invocation is stopped. On a later
explicit process reopen:

1. validate the stream identity, policy versions, UTC clock, manual holds, outer state, and attached
   child identity;
2. acquire fresh outer authority without reusing an old token;
3. if the attached child does not yet exist because the prior process stopped after attachment,
   create the same job idempotently from the exact attached UUID, range, creation inputs, and
   fingerprint; any disagreement fails closed;
4. if the attached child is already durably `COMPLETED` at the exact attached end, advance the
   continuous cursor and clear the attachment without a provider request;
5. otherwise let the existing bounded orchestrator load the child's exact durable
   `next_window_start` and `pending_window_end_exclusive` and obtain fresh eligible child fencing
   authority without reusing an old token;
6. finish that retained leaf before any remaining part of the immutable child range;
7. rely on idempotent evidence admission if the prior process committed evidence but not control;
   and
8. advance the continuous cursor only after the child is durably `COMPLETED` at its exact end.

A stale service heartbeat, expired lease, failed child, or host restart is evidence for an
operator decision, not automatic recovery authority. A failed child requires a manual stream
`PAUSED` decision before any recovery. A future approved supervisor may invoke this same recovery
contract only after the stream is explicitly `ACTIVE`, but ADR acceptance does not start or
authorize that supervisor.

## Manual Schema-Drift Hold and Governed Resume

Schema drift is handled by
[`PUBLIC_PROVIDER_SCHEMA_DRIFT.md`](../runbooks/PUBLIC_PROVIDER_SCHEMA_DRIFT.md). This operating
contract adds no detector. A typed `INVALID_PAYLOAD`, fixture failure, documentation change, or
other signal ends or blocks the affected finite work and creates bounded internal attention
evidence; it does not by itself claim that upstream drift occurred or automatically write a hold.

After manual classification:

1. an authorized operator places the smallest exact provider, dataset, market, request-variant,
   endpoint-contract, adapter-version, and fixture-version identity in stream `PAUSED` state with a
   bounded schema-drift-hold reason;
2. when isolation is uncertain, every variant sharing the uncertain parser or endpoint boundary is
   manually placed in `PAUSED`;
3. no new child is selected and no failed child is retried for a held identity;
4. existing raw, canonical, conflict, checkpoint, health, transition, and budget evidence remains
   unchanged; and
5. unaffected variants continue only when their independence is explicit and evidenced.

Resume is manual and requires all applicable TASK-057 gates: exact current contract re-review,
sanitized evidence handling, a coherent synthetic fixture version or recorded proof that the
current one remains exact, production-adapter fixture and drift regressions, reconciliation or
approved quarantine of suspect intervals, completed governance and approvals for every required
adapter/parser/runtime/deployment change, complete repository and CI regression, a tested
rollback, and the required Market Data, Engineering, Audit, Risk, Security, and project-owner
authority.

Missing, ambiguous, expired, conflicting, rejected, or revise-required authority keeps the stream
`PAUSED`. There is no automatic detection, pause, fixture refresh, remediation, resume, provider
switch, or backfill.

## Source Health, Service Health, and Escalation

Source health and service health answer different questions and must never be collapsed:

- **Source health** remains causal to one bounded child checkpoint transition. Existing
  `HEALTHY`, `DEGRADED`, and `UNAVAILABLE` observations describe accepted provider/admission work,
  retries, splits, or a typed source failure. They do not prove the future process is alive.
- **Service health** would interpret the future finite run's `STARTING`, `RUNNING`, `STOPPED`,
  `PAUSED`, `FAILED`, and `RUN_LIMIT` evidence plus liveness freshness. The stream's separate
  `ACTIVE` or `PAUSED` state remains explicit. Service evidence must retain the stream ID, run ID,
  outer fence, child job ID, relevant checkpoint and transition versions, continuous cursor,
  request variant, observed UTC time, policy versions, and bounded terminal reason.

`stale` means expected lifecycle evidence has not arrived within a separately configured and
capacity-reviewed threshold. It does not mean the provider is unavailable. A `STOPPED` run with
`caught_up`, `waiting`, or `shutdown_requested` reason is not stale. A degraded source does not
automatically fail the service, and a healthy source observation does not hide a lost fence,
corrupt store, or stale service.

Future internal alert evidence must be finite, typed, deduplicable, and correlated to the exact
stream/run/child/version boundary. At minimum it must distinguish source unavailability, service
staleness, manual stream pause and schema-drift pause reasons, capacity/backlog exhaustion, lost
fence/conflict, storage or evidence failure, clock/policy drift, and terminal service failure.
This decision implements no CLI, dashboard, acknowledgement, external delivery, paging route, or
restart action.

Operator decision points are:

- continue only after a clean caught-up or bounded-work result under the approved trigger;
- hold, resume, or reconcile an exact schema-affected variant;
- manually pause before deciding whether to retry a failed child or restart a stale/failed service
  run;
- change capacity, retention, cadence, settlement, retry, or pacing policy;
- roll back or disable the continuous path; and
- escalate when scope, evidence, authority, or safe recovery is uncertain.

Market Data owns provider-contract and source-quality assessment. Engineering owns a bounded
candidate, runtime diagnosis, and tested rollback. Audit and Assurance owns the causal evidence and
decision trail. Security owns suspected secrets and evidence-handling boundaries. Risk may reject
or halt a material operational change. The project owner retains scope, deployment, permission,
resume, and operating-mode authority. An unbounded impact, ambiguous provider contract, possible
sensitive payload, irreconcilable interval, repeated capacity breach, corruption, or missing
rollback escalates immediately and remains fail-closed.

## Capacity Contract

No implementation may claim adequate capacity until exact approved configuration and measured
evidence cover all of these inputs:

| Area | Required configuration inputs | Required measurements/evidence |
|---|---|---|
| Provider | Request variant, official request weights, documented limits, exact shared-budget key/capacity/period/cost, and all competing host workloads | Requests and weights by variant, throttles, budget grants/denials/waits, provider latency, and reviewed contract references |
| Cadence and backlog | Window duration, settlement lag, trigger target, maximum catch-up age/range, child jobs per run, and outage policy | Arrival rate, collection lag, oldest pending boundary, catch-up throughput, outage duration distribution, and time to return within the target envelope |
| Range work | Child range, request, record, split, minimum-window, retry, pacing, and response-size bounds | Records per millisecond/window, response bytes, split depth, retries, accepted/rejected/conflict counts, and worst observed bounded invocation |
| Shared capacity | All streams and adapters using the same provider budget plus required headroom policy | Aggregate weighted demand, contention, fairness/starvation evidence, and behavior at and above the configured budget |
| Evidence storage | Raw, canonical, conflict, checkpoint, transition, health, lifecycle, and audit retention; backup and free-space policy | Bytes per request/record/window/run, growth over time, index/WAL behavior, write latency, free-space margin, backup/restore duration, and retention effect |
| Control and health | Lease, heartbeat, freshness, history-page, internal-alert, and audit-correlation bounds | Claim/conflict/expiry rates, checkpoint and health volume, stale evaluations, query latency, and corruption/failure exercises |
| Recovery and rollback | Maximum tolerated outage/backlog, finite recovery-work envelope, safe shutdown boundaries, and rollback target | Process-reopen drills, exact pending-leaf recovery, catch-up under budget saturation, clean shutdown at every seam, and timed rollback/restore exercises |

An implementation proposal must show that measured sustainable accepted throughput exceeds the
configured arrival rate with approved headroom and that the worst approved outage can be recovered
inside finite work, budget, storage, and operator limits. If it cannot, the continuous deployment
remains disabled or the stream is manually `PAUSED`. Increasing a limit, skipping evidence,
creating a second budget key, or adding hosts is not an implicit remedy.

This ADR supplies none of the values and makes no adequacy, availability, retention, recovery-time,
or continuous-operation claim.

## Residual Limitations

- A configured settlement lag and UTC-closed target do not prove that a provider will never emit a
  late or revised trade. Exact lag adequacy and a governed late-event/reconciliation policy remain
  unproven.
- Aggregate-trade density can exceed a provider response bound even at the existing one-millisecond
  minimum leaf. That remains a typed density failure and manual operator decision, not permission
  to split below the accepted boundary or claim complete coverage.
- A future stale-heartbeat threshold, storage retention, free-space margin, and provider/catch-up
  capacity remain unselected and unmeasured.
- A cooperative pause or shutdown may wait for an already in-flight bounded operation to return.
  No immediate cancellation or total pause/shutdown latency is guaranteed.
- Evidence, bounded-job control, future continuous control, lifecycle evidence, and rate-budget
  state remain separate commit domains. A crash can repeat idempotent evidence work, and a request
  made before an uncommitted checkpoint update may be absent from per-job counters.
- Logical SQLite commit evidence is not proof of physical durability across every device,
  filesystem, power, or host failure.
- Outer exclusivity covers only cooperating processes on one host that use the same validated
  control and budget stores. It provides no process-manager guarantee, multi-host coordination, or
  failover.

## Failure and Shutdown Semantics

| Failure or event | Required disposition |
|---|---|
| Invalid or changed stream identity/policy fingerprint | End the service `FAILED` before network work; preserve the attached child and cursor; require a manual stream `PAUSED` decision and an explicit versioned change. |
| Non-UTC/regressing clock or unsafe timestamp ordering | Fail closed; do not calculate eligibility, claim new work, or advance state. |
| Already-running owner, stale token, lost lease, token reuse, or compare-and-swap conflict | End the service `FAILED` without retry pressure or cursor movement; read validated durable state, require a manual stream `PAUSED` decision, and escalate when ownership is unclear. |
| Provider disconnect, timeout, rate limit, or unavailability | Use only the existing finite typed retry and shared-budget behavior. A resulting child `FAILED` requires manual stream `PAUSED`; no continuous retry loop is created. |
| Invalid payload or suspected provider-contract drift | Do not retry as availability. End affected work, emit bounded attention evidence, and require the manual TASK-057 classification and stream `PAUSED` hold procedure. |
| Quality rejection, raw/canonical conflict, or evidence-admission failure | End the child/service before progress; retain all admitted evidence and exact recovery boundaries; require manual stream `PAUSED` before investigation or recovery. |
| Evidence or control storage error/corruption | End the service `FAILED` and require manual stream `PAUSED`; do not repair, delete, infer a cursor, or use alternate untracked storage. |
| Bounded-job request or record bound | Keep the stream `ACTIVE`, retain the same child attachment and exact pending leaf, and end the service `RUN_LIMIT` with `work_limit_reached`. |
| Capacity, backlog, or storage envelope exceeded | End finite work, emit bounded attention evidence, and require an operator decision; never widen a bound or pause/resume automatically. |
| Host/process crash or stale nonterminal service | Preserve the last causal state. Require an operator decision and manual stream `PAUSED` before a later explicit recovery; no automatic restart is implied. |
| Shutdown or rollback request | At the next supervisor-safe boundary, select no new child, leave the stream `ACTIVE`, preserve its exact attachment, and record the finite service `STOPPED` with `shutdown_requested`. |

Shutdown is checked before an outer claim, before attaching or invoking the next child, after a
bounded child returns, and before a supervisor-owned interruptible wait. A child already invoked
remains governed by its existing finite work and per-operation timeout contracts; this design does
not add cancellation inside that invocation or promise a total shutdown duration. If the child
returns, accepted evidence and control are handled only under still-valid authority, and the
service starts no further child. If the process is externally terminated, the exact attachment,
cursor, and pending leaf remain for the process-reopen contract. Shutdown never converts uncertain
work into completion and a clean stop does not change stream `ACTIVE`.

## Rollback

The rollback baseline is a disabled continuous trigger/deployment, or the current explicitly
invoked bounded `PublicTradeCollectionOrchestrator` without a continuous trigger. `disabled` is a
deployment condition, not a continuous-stream lifecycle state.

A future tested rollback must:

1. stop new triggers and let the finite service reach `STOPPED` at its next safe boundary while
   preserving stream `ACTIVE` and the exact attachment, or manually set the stream `PAUSED` when
   investigation is required;
2. preserve every continuous and child identity, policy version, fence acquisition, checkpoint,
   pending leaf, transition, health observation, raw/canonical/conflict record, budget decision,
   and audit correlation;
3. never move a cursor backward or forward, clear a manual hold, delete a failed child, reuse a
   fence token, reset the shared budget, or accept evidence through a weaker adapter;
4. leave any incomplete attached child available to the current explicit bounded recovery flow,
   under its existing identity and controls;
5. restore the last reviewed code/configuration only when it can read the retained state exactly;
   otherwise keep the continuous path disabled and use a separately approved forward recovery;
   and
6. re-run deterministic recovery, drift, capacity, shutdown, full regression, and health evidence
   before any later resume.

Rollback is not a schema downgrade, evidence deletion, automatic provider switch, or permission to
continue on an unknown contract.

## Evidence Required Before Implementation

ADR acceptance grants no implementation authority. A separately promoted bounded task must name
the exact production files, serialized contracts, persistence and migration scope, trigger/service
technology, host topology, deployment values, risk tier, reviewers, and rollback. It must produce:

### Deterministic contract evidence

- closed-window grid and settlement boundary tests immediately before, at, and after eligibility;
- caught-up, finite catch-up, child/range/request/record/run-work limit, and backlog-exhaustion
  tests;
- separate stream `ACTIVE`/`PAUSED`, service
  `STARTING -> RUNNING -> STOPPED | PAUSED | FAILED | RUN_LIMIT`, and unchanged bounded-job
  lifecycle tests, including rejection of cross-layer state inference;
- immutable identity and policy-version tests, including refusal to replace an attached child;
- competing-trigger, outer and child fresh-UUID/non-reuse, stale-token, lease-expiry,
  compare-and-swap, and overlap-prevention tests;
- proof that exact child identity, range end, and policy are attached before child creation, remain
  through child `PAUSED`/`FAILED` and clean service stop, and clear only after exact completion;
- exact pending-leaf and same-child process-reopen tests at every attach, creation, request,
  evidence, checkpoint, continuous-cursor, and lifecycle crash seam, including an already-completed
  attached child advancing the continuous cursor with zero provider requests;
- proof that evidence precedes child progress and child completion precedes continuous progress,
  including safe idempotent refetch without a gap;
- proof that bounded request/record `PAUSED` remains a resumable same-child continuation while the
  stream stays `ACTIVE`, whereas failure/conflict/lost authority/corruption/schema suspicion
  requires manual stream `PAUSED` and no supervisor recovery;
- one shared-budget test across every configured public-trade stream, including saturation,
  weighted request cost, denial, and no alternate-key bypass;
- manual exact-variant and shared-parser stream-`PAUSED` drift-hold tests, complete resume-gate
  denial tests, and no automatic detector/pause/remediation/resume;
- source-versus-service health, staleness, bounded internal alert, causal-correlation, and history
  corruption tests;
- typed failure-table tests, clean `STOPPED` outcomes at every supervisor-safe shutdown boundary,
  and externally terminated in-flight-child process-reopen tests without a shutdown-duration
  claim; and
- disable-to-current-bounded-flow rollback and retained-state reopen tests.

### Capacity and operational evidence

- reviewed exact provider weights/limits and every configured cadence, settlement, work, lease,
  heartbeat, freshness, catch-up, storage, retention, backup, and shutdown-check boundary;
- measured workload distributions and a reproducible finite load/catch-up exercise at normal,
  saturation, outage-recovery, and storage-pressure conditions;
- single-host deployment, process supervision, startup-disabled default, filesystem/permission,
  log, backup/restore, monitoring-query, clean-stop, and rollback evidence;
- explicit evidence that a missed trigger or process crash cannot overlap work or create an
  unbounded restart/catch-up loop; and
- an operational review by Market Data, Engineering, Security, Audit and Assurance, Risk where
  applicable, and the project owner under repository policy.

The implementation must pass formatting, lint, strict typing, focused and complete tests, lockfile
verification, dependency audit, the health slice, CI, security review, and final diff/scope review.
A skipped, unavailable, partial, substituted, or failing check is not evidence.

## Safety and Authority Boundary

This ADR accepts a conceptual operating contract only. The selected future component is not
implemented, enabled, deployed, scheduled, monitored, capacity-approved, recovered, or ready for
continuous operation.

It grants no permission to access an operator path or data, add a credential, use a private
endpoint, create a signal, make a portfolio or Risk decision, submit an order, trade, change an
operating mode, or begin Stage 3. TASK-037 remains blocked and authorization remains denied.

## Consequences

### Positive

- A later implementation proposal has one explicit single-host ownership and recovery model.
- Closed-window selection, finite catch-up, outer and child fences, and evidence-before-progress
  ordering are separated and testable.
- Schema uncertainty has a precise manual hold/resume boundary without pretending a detector or
  automatic control exists.
- Source health, process/service health, internal alert evidence, and operator authority remain
  distinct.
- Capacity and rollback cannot be claimed from configuration alone.

### Negative

- Continuous public-trade collection remains unavailable until a separate implementation,
  operational review, and deployment decision are completed.
- A second outer stream-control boundary and lifecycle evidence will add design, persistence,
  migration, and test cost.
- Separate evidence, bounded-job control, continuous-control, lifecycle, and budget databases
  cannot share one atomic transaction.
- Single-host SQLite cannot provide failover or multi-host exclusivity.
- Manual drift and recovery decisions increase operator workload but prevent silent coercion or
  unsafe autonomous restart.

## Alternatives Considered

### Reuse the continuous candle collector and service unchanged

Rejected. Candle progress uses fixed timeframe rows and an attached historical candle job.
Aggregate trades use millisecond half-open ranges, adaptive density splits, exact pending leaves,
different evidence contracts, and a different bounded orchestrator. Existing candle behavior and
runtime values are not proof for public trades.

### Let a scheduler create an independent bounded job on every tick

Rejected. Scheduler non-overlap cannot replace a durable continuous cursor, attached child,
versioned policy, outer fence, or evidence that the prior child completed. Deriving progress from
the latest stored trade could skip partial, conflicting, or externally written evidence.

### Extend one public-trade job forever

Rejected. It would mutate an immutable end, weaken finite work limits and policy reproduction, and
blur one invocation's health and audit evidence.

### Select a specific operating-system scheduler or service now

Deferred. Host topology, startup policy, lifecycle integration, logging, values, deployment,
monitoring, and rollback require measured evidence and a separately approved operational task.

### Use WebSockets or provider failover for continuous coverage

Rejected for this decision. Both introduce new endpoint, gap, ordering, reconnect, identity,
capacity, evidence, and authority boundaries that the current bounded public-trade path does not
implement.

### Add distributed coordination or multiple active hosts

Rejected. Current leases, budgets, and SQLite stores are local single-host controls. Distributed
consensus, fencing, replication, failover, and capacity accounting require a separate architecture
and risk decision.

### Automatically detect drift and pause, repair, or resume

Rejected. Parser failure is not complete drift classification, and automatic remediation could
accept unknown semantics or create competing work. TASK-057 deliberately requires bounded manual
classification and governed resume.

## Explicit Non-Goals

- Claiming 24/7 availability, no data loss, exactly-once requests, cross-database atomicity,
  physical durability, recovery-time objectives, provider compatibility, or capacity adequacy.
- Replacing current public-trade adapters, contracts, evidence stores, range policies, checkpoint
  models, health observations, transition history, or the shared request budget.
- Adding automatic gap repair, history compaction, retention, migration, failover, notification,
  acknowledgement, or process control.
- Weakening any lease, drift, adapter, quality, evidence, budget, audit, Security, Risk, or human
  approval boundary.

## Review Triggers

Review this decision before:

- selecting or changing a scheduler, service manager, deployment host, runtime value, stream or
  lifecycle schema, storage location, retention, backup, or alert route;
- implementing canonical UTC migration for connected state or relying on time-indexed version-1
  storage;
- changing bounded-job identity, policy fingerprints, UUID fencing, exact pending-leaf recovery,
  evidence ordering, source-health, transition history, or shared-budget semantics;
- adding automatic restart, automatic recovery, automatic resume, drift detection, automatic
  pause/remediation, data repair, provider failover, external notifications, or an operational
  dashboard;
- adding WebSockets, incomplete-window data, a different provider/endpoint, multi-host
  coordination, distributed storage, or cross-database transactions;
- adding private/account data, credentials, signals, portfolio or Risk decisions, orders,
  execution, live trading, or a higher operating mode; or
- claiming continuous-operation, capacity, recovery, deployment, Phase 2 completion, or closure of
  `RISK-002`, `RISK-004`, or `RISK-005`.
