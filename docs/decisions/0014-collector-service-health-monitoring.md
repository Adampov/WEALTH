# ADR 0014: Collector Service Health Monitoring and Internal Alerts

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

Collector service runs now retain durable lifecycle heartbeats, but stored evidence alone does not
answer whether the latest nonterminal run is still healthy. An operator-facing system needs a
deterministic way to distinguish recent activity from a stale process, map terminal outcomes to
operational meaning, and expose structured alerts without coupling the core to email, chat, or a
specific monitoring vendor.

Freshness evaluation must account for legitimate polling and reconnect waits. The accepted
continuous collector can wait for up to 300 seconds, so a shorter default stale threshold would
produce false alerts during normal bounded behavior.

## Decision

Add a read-only operational health workflow over durable collector service state:

- The SQLite lifecycle store exposes a bounded newest-first query of latest heartbeats for service
  runs belonging to one continuous collection.
- Every returned current projection is checked against its canonical JSON record before use.
- A health monitor evaluates all selected run heartbeats at one injected, timezone-aware instant.
- `starting` and `running` heartbeats are `active` before the configured freshness threshold and
  `stale` at or beyond it.
- The default stale threshold is 600 seconds. It must be finite, positive, and no longer than seven
  days.
- `stopped` maps to an expected stopped state, `cycle_limit` maps to completed, `paused` maps to
  paused, and `failed` maps to failed.
- A stale run emits the internal code `heartbeat_stale` with critical severity.
- A paused run emits `collector_paused` with warning severity.
- A failed run emits `collector_failed` with critical severity.
- Expected stopped and completed runs do not emit alerts.
- Health reports are bounded to at most 1,000 runs. The default evaluates only the latest run for a
  current-health view; callers may request more recent runs for operational history.
- Report assessments are newest first, cannot mix collections or evaluation times, and cannot
  duplicate run identities.
- A collection with no service runs reports `not_started`. A selected report with an internal alert
  reports `attention_required`; otherwise it reports `healthy` when a selected run is active and
  `idle` when all selected runs stopped or completed as expected.
- Evaluation fails closed when the current clock precedes a durable heartbeat.

Internal alerts are data contracts only. This decision does not deliver notifications or mutate
collector state.

## Safety Boundary

This decision authorizes read-only operational interpretation of existing public-data collection
evidence. It does not authorize:

- Email, SMS, Telegram, Slack, webhook, or monitoring-vendor delivery.
- Automatic restart, pause, resume, remediation, or host-process control.
- An operating-system service, deployment, or external scheduler.
- Private exchange access, credentials, orders, or execution.
- Trading signals, investment recommendations, or AI decisions.

## Consequences

### Positive

- A stuck or crashed nonterminal process becomes machine-detectable from durable evidence.
- Alert severity and routing codes are stable and independent of presentation or delivery tools.
- Legitimate bounded waits fit safely inside the default threshold.
- Current health and bounded recent-run history use the same validated contracts.
- Tests can move the clock precisely without waiting in real time.

### Negative

- The monitor remains pull-based; a future scheduler or dashboard must invoke it.
- A network call that exceeds the threshold may appear stale even if its process still exists.
- Reports over more than the latest run include historical pause and failure alerts until those runs
  fall outside the selected bound; acknowledgement is not modeled yet.
- No external person or system receives the internal alert in this change.
- SQLite remains a single-host baseline.

## Alternatives Considered

### Use process existence as the only health signal

Rejected. A live process can be blocked or making no durable progress, and process inspection is
host-specific.

### Mark a run stale after 30 seconds

Rejected as the default. It is shorter than accepted reconnect waits and would create false
positives.

### Send Telegram or email directly from the monitor

Deferred. Core health interpretation should be deterministic and provider-neutral before delivery,
deduplication, acknowledgement, and escalation policy are selected.

### Restart stale collectors automatically

Rejected. Automatic remediation without ownership checks could create competing workers or hide a
storage or provider incident.

## Review Triggers

Revisit this decision when adding alert delivery, acknowledgements, independent wall-clock
heartbeats, process-manager integration, automatic restart policy, dashboards, or multi-host
coordination.
