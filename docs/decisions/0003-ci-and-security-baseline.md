# ADR 0003: CI and Dependency Security Baseline

- **Status:** Accepted
- **Date:** 2026-07-24
- **Decision owners:** Engineering Department and project owner

## Context

Phase 1 requires every proposed change to run reproducible quality checks. Local commands exist,
but the repository does not yet enforce them remotely or audit locked dependencies for known
vulnerabilities.

## Decision

Use GitHub Actions as the initial continuous-integration runner.

Every push and pull request runs:

1. Locked dependency installation.
2. Lockfile verification.
3. Formatting verification.
4. Linting.
5. Strict type checking.
6. Unit and integration tests.
7. Locked dependency vulnerability auditing through `uv audit`.
8. The local-only foundation health slice.

The workflow:

- Grants the GitHub token read-only repository access.
- Does not receive application or exchange secrets.
- Pins reusable actions to full commit SHAs.
- Disables persisted checkout credentials.
- Has a bounded timeout and cancels superseded runs.

## Consequences

### Positive

- Clean-checkout behavior is continuously verified.
- Dependency vulnerabilities become a blocking, visible result.
- CI behavior matches documented local commands.
- The workflow has no financial or deployment authority.

### Negative

- Vulnerability auditing depends on an external advisory service.
- GitHub Actions availability can delay review.
- Pinned action SHAs require deliberate maintenance.
- The `uv audit` command is currently a preview feature and may require workflow maintenance.

## Alternatives Considered

### Local checks only

Rejected because results would depend on developer discipline and local state.

### A third-party security action

Deferred. The current `uv` tool already audits the locked dependency graph, reducing the number of
trusted CI components.

## Review Triggers

Revisit when deployment, container images, private dependencies, self-hosted runners, release
artifacts, or code-scanning requirements are introduced.
