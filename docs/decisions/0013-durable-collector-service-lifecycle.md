# ADR 0013: Durable Local Collector Service Lifecycle

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Project owner, Market Data Department, Engineering Department, and Audit
  Department

## Context

The supervised continuous candle collector can execute restart-safe polling cycles, but a caller
still has to manage repeated cycle invocation and ordinary waits. A future unattended process needs
an interruptible stop boundary and durable evidence that distinguishes a clean stop, a configured
run limit, an operator pause, and an operational concurrency failure.

The lifecycle wrapper must not weaken the existing bounded polling, cursor, retry, lease, or
provider-safety rules. It must also remain testable without installing an operating-system service
or connecting to a private exchange.

## Decision

Add a provider-independent local service runner around the accepted continuous collector:

- Each invocation receives a new run UUID and retains the continuous collection UUID and worker
  identity.
- Each invocation has an explicit limit of at most 10,000 polling cycles. The runner does not
  create an unbounded loop.
- Shutdown state is injected behind a small boundary that supports both an immediate check and an
  interruptible timed wait.
- The runner checks for shutdown before every cycle. A shutdown request can also wake an idle or
  reconnect wait immediately.
- The existing continuous collector remains responsible for closed-candle eligibility, bounded
  jobs, reconnect policy, cursor advancement, pause state, and worker concurrency.
- Service lifecycle observations are strict, immutable, versioned heartbeats. A run starts with a
  pristine `starting` observation, records every safe cycle as `running`, and finishes as one of
  `stopped`, `paused`, `failed`, or `cycle_limit`.
- Operational concurrency outcomes (`already_running`, `checkpoint_conflict`, and `lost_lease`)
  stop the service run as explicit failures. They are not retried or hidden.
- Heartbeats retain the current continuous checkpoint version and next-window cursor. Sequences,
  time, cycle counts, checkpoint versions, and cursors cannot regress.
- Heartbeats are stored in a dedicated SQLite file as append-only history plus one validated latest
  projection per run.
- Writes use a single immediate transaction, exact next-sequence enforcement, idempotent heartbeat
  identity, WAL mode, full synchronous durability, foreign keys, and a bounded busy timeout.
- Current and historical projections are checked against their canonical JSON records when read.
  Corrupt or divergent projections fail closed.
- A later invocation gets a new service run UUID but reloads the existing continuous collection
  cursor. Service history is therefore separated by invocation without resetting market progress.

## Safety Boundary

This decision authorizes a bounded local process lifecycle for public closed-candle polling. It
does not authorize:

- Automatic startup, an operating-system service, deployment, or an external scheduler.
- Installing signal handlers or changing host service configuration.
- Live WebSocket ingestion or incomplete candles.
- Multi-host coordination or treating SQLite as distributed storage.
- Private exchange access, credentials, account data, orders, or execution.
- Trading signals, investment decisions, or automatic code changes.

## Consequences

### Positive

- An ordinary stop can interrupt a wait instead of blocking until the next polling deadline.
- Every run has durable evidence of startup, progress, and its terminal reason.
- Restart retains the accepted continuous cursor while producing a distinct run audit trail.
- Competing-worker and storage-corruption behavior is explicit and tested.
- The process-lifecycle boundary can later be composed with a host-specific service manager without
  moving provider or trading authority into it.

### Negative

- Heartbeats are cycle-driven; they do not yet provide an independent wall-clock liveness pulse
  while a network call is in progress.
- A host crash can leave a run's last heartbeat non-terminal. A future supervisor must interpret
  that alongside process state and observation time.
- The service runner is still invoked by a caller and does not provide 24/7 deployment by itself.
- SQLite remains a single-host durability baseline.

## Alternatives Considered

### Keep repeated cycles in a shell script

Rejected. Shell control would not provide validated durable lifecycle evidence or an injectable,
interruptible wait boundary.

### Make the existing collector run method unbounded

Rejected. It would combine polling policy with process lifecycle and remove an important work
bound.

### Treat concurrency conflicts as retryable

Rejected. Repeated pressure could conceal a competing worker or stale state. The service records
the conflict and stops for explicit supervision.

### Install an operating-system service now

Deferred. Host configuration, deployment, log routing, startup policy, credentials, and alerting
need a separate operational decision after the local lifecycle is proven.

## Review Triggers

Revisit this decision when adding operating-system signal binding, an external process manager,
independent wall-clock heartbeats, multi-host coordination, deployment, WebSockets, or operational
alerts.
