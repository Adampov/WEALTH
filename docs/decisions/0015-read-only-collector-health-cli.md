# ADR 0015: Read-Only Collector Health JSON Command

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

Collector service health reports and internal alerts are available through application contracts,
but operators and monitoring processes need a stable entry point that does not import or compose
Python objects themselves. The entry point must be safe to run from a terminal, scheduler, or
future dashboard collector.

A monitoring typo must not create an empty SQLite database and report a misleading `not_started`
state. Invalid input also must not reuse the critical exit code because an automation system could
mistake a command configuration problem for a confirmed collector failure.

## Decision

Add a dedicated `wealth-collector-health` command with the following behavior:

- Require an existing collector-service SQLite path and a continuous collection UUID.
- Open the lifecycle database using SQLite URI read-only mode. The command does not create parent
  directories, initialize schemas, append heartbeats, or change database pragmas that require a
  write.
- Validate the existing schema version before reading.
- Reuse the accepted collector health monitor and its configurable freshness policy.
- Default to the latest run and the accepted 600-second stale threshold.
- Allow a bounded recent-run limit from 1 to 1,000 and a stale threshold no longer than seven days.
- Write one stable, versioned JSON envelope to standard output for a completed health evaluation.
- Include the full validated health report and an explicit alert list in the JSON envelope.
- Write one stable JSON error envelope to standard error for invalid arguments, storage failures,
  clock regression, or filesystem failures.
- Sort JSON keys and emit exactly one trailing newline. An optional `--pretty` flag changes only
  indentation, not semantics.
- Use monitoring-compatible exit codes:
  - `0` — `ok`: selected runs are active, stopped, or completed without alerts.
  - `1` — `warning`: at least one selected run is paused.
  - `2` — `critical`: at least one selected run is stale or failed.
  - `3` — `unknown`: no selected run exists, input is invalid, or health cannot be trusted.
- Highest alert severity wins when more than one recent run is requested.

The existing synthetic `wealth-health` command remains unchanged.

## Safety Boundary

This decision authorizes a local read-only operational query. It does not authorize:

- Starting, stopping, restarting, pausing, or resuming collection.
- Database creation, migration, repair, acknowledgement, or alert suppression.
- External notification delivery or dashboard hosting.
- An operating-system service, deployment, or scheduler installation.
- Private exchange access, credentials, account data, orders, or execution.
- Trading signals, investment recommendations, or AI decisions.

## Consequences

### Positive

- Humans and monitoring tools receive the same validated health interpretation.
- Exit codes distinguish confirmed warning and critical states from an untrustworthy invocation.
- Missing or misspelled database paths fail visibly without creating files.
- JSON output is stable enough for future dashboard, alert-delivery, and process-manager adapters.
- Read-only SQLite mode provides an enforceable storage boundary rather than relying on convention.

### Negative

- Operators must know the database path and collection UUID.
- `not_started` returns `unknown`, so first-run provisioning needs an explicit operational check.
- The command remains pull-based and sends no notifications.
- Read-only access to an active WAL database still depends on normal local SQLite/WAL filesystem
  behavior.
- Exit codes describe the selected query window; requesting historical runs can surface historical
  alerts.

## Alternatives Considered

### Add arguments to the synthetic foundation health command

Rejected. Synthetic application health and collector operational health have different inputs,
evidence, and exit semantics.

### Return exit code 2 for invalid arguments

Rejected. Monitoring systems conventionally interpret 2 as a confirmed critical condition, while
invalid input means the result is unknown.

### Create the database when it is missing

Rejected. A path typo would mutate the filesystem and could hide a deployment error as a valid
empty state.

### Print human text by default

Rejected. Stable JSON is easier to validate, automate, and adapt. Humans can request indented JSON.

## Review Triggers

Revisit this decision when adding subcommands, authenticated remote health APIs, dashboard
integration, alert delivery, acknowledgement, process-manager control, or database migrations.
