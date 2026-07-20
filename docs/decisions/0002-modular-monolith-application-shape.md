# ADR 0002: Modular Monolith Application Shape

- **Status:** Proposed
- **Date:** 2026-07-20
- **Decision owners:** Engineering Department and project owner

## Context

The approved architecture defines distinct information, intelligence, control, execution, assurance, and evolution planes. The implementation must preserve these boundaries while the team is small and the system is still proving its data contracts, replay model, risk rules, and operational behavior.

Starting with independently deployed microservices would add network contracts, distributed state, deployment coordination, observability, and failure modes before the system has evidence that those costs are necessary.

## Decision

Begin as a modular monolith with event-driven internal boundaries and a `src` package layout.

The initial deployable is one application package, but modules communicate through explicit domain contracts, application services, and ports rather than importing provider-specific details across boundaries.

The initial logical package families are:

- `domain` — immutable domain events, value objects, reason codes, and invariants.
- `application` — use cases and orchestration that depend on ports.
- `ports` — protocols for storage, clocks, IDs, data providers, and other external capabilities.
- `adapters` — provider-specific or in-memory implementations of ports.
- `observability` — structured logging and health instrumentation.
- `settings` — validated environment and operating-mode configuration when introduced.

Department and plane boundaries may later become separate processes, but process separation is not assumed in the domain model.

## Boundary Rules

- `domain` does not import application, adapter, infrastructure, or provider code.
- `application` depends on domain types and ports, not concrete adapters.
- `adapters` implement ports and may depend on external libraries.
- External payloads are validated before becoming domain records.
- Provider-specific formats do not escape their adapter boundary.
- External actions are idempotent and auditable.
- Wall-clock time, ID generation, persistence, and network access are injected behind ports when they affect deterministic behavior.
- No module receives a path around portfolio, deterministic risk, execution, or audit controls.

## Rationale

- A single deployable is easier to run, test, replay, debug, and recover during the foundation phase.
- Explicit module boundaries preserve future extraction options without paying distributed-systems costs now.
- Event-driven contracts align live, paper, and replay workflows.
- Injected clocks, IDs, and ports make deterministic tests and market replay practical.
- The `src` layout prevents accidental imports from the repository root and tests the installed package shape.

## Consequences

### Positive

- Lower operational complexity and faster iteration.
- Strong in-process typing and easier end-to-end tests.
- One consistent audit and observability context during early development.
- Clear seams for provider replacement and later service extraction.

### Negative

- Module discipline must be enforced by review and tests rather than network isolation.
- One process can create shared-resource contention until components are separated.
- Independent scaling and deployment are deferred.

## Alternatives Considered

### Microservices from the start

Rejected for the initial phase because distributed deployment, messaging, state, and failure handling would obscure the core data and risk model. Extraction remains available when measurement demonstrates a need.

### Flat package organized only by technical type

Rejected because it encourages cross-boundary imports and makes ownership, authority, and future extraction less explicit.

### Freqtrade as the entire application architecture

Not selected as the system core. Freqtrade may later be evaluated as a bounded adapter, execution or backtesting component, or reference implementation. The project architecture must remain provider- and framework-independent.

## Extraction Criteria

A module may be proposed for independent deployment only when evidence shows at least one of:

- Independent scaling requirements.
- Isolation required for credentials, permissions, security, or availability.
- Resource contention that cannot be resolved inside the monolith.
- A release cadence that materially differs from the rest of the application.
- A failure domain that must be isolated operationally.

Extraction requires a separate ADR, versioned external contract, observability, failure behavior, deployment plan, and rollback plan.

## Review Triggers

Revisit this ADR after continuous paper operation or when measured reliability, scaling, security, or deployment needs justify a process boundary.
