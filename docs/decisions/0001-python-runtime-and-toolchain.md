# ADR 0001: Python Runtime and Toolchain

- **Status:** Accepted
- **Date:** 2026-07-20
- **Decision owners:** Engineering Department and project owner

## Context

WEALTH requires one primary implementation language for data ingestion, domain contracts, replay, quantitative analysis, machine learning, orchestration, risk, and execution adapters. The foundation must be reproducible across developer machines and CI, friendly to Codex, and compatible with the broader Python data and trading ecosystem.

The project also needs a small, explicit quality toolchain before runtime functionality grows.

## Decision

Use CPython 3.13 as the default project runtime and support the Python 3.13 minor series for the initial foundation.

Use:

- `uv` for Python acquisition, virtual environments, dependency resolution, lockfiles, and command execution.
- `pyproject.toml` as the central project and tool configuration file.
- A committed `uv.lock` for reproducible dependency installation.
- Pydantic 2 for strict, versioned boundary and domain-contract validation.
- Ruff for linting, import ordering, and formatting.
- mypy in strict mode for static type checking.
- pytest for example and integration tests.
- Hypothesis for property-based testing of invariants and edge cases.
- Hatchling as the initial standards-based build backend.

Runtime and development dependencies must use compatible ranges in `pyproject.toml`; exact resolved versions are recorded in `uv.lock`.

## Rationale

- Python has strong libraries and community support for market data, quantitative analysis, machine learning, APIs, and exchange integrations.
- Python 3.13 is in the CPython bugfix-support phase through October 2029, giving the project a maintained stable baseline rather than a prerelease or security-only baseline.
- `uv` supports Python 3.13, project-local Python selection, reproducible universal lockfiles, and synchronized command execution.
- Pydantic provides typed validation and JSON Schema generation at untrusted input and inter-module boundaries. Strict mode reduces silent coercion.
- Ruff consolidates several formatting and linting responsibilities into one fast tool.
- Strict static typing and property-based testing are appropriate for financial state, time, identifiers, risk invariants, and failure cases.

## Consequences

### Positive

- One reproducible local and CI workflow.
- A large compatible ecosystem for future data and ML work.
- Explicit schemas and types from the first runtime code.
- Fast feedback for formatting, linting, typing, and tests.
- Dependency upgrades can be reviewed as lockfile changes.

### Negative

- Python does not prevent runtime type errors by itself; quality depends on validation, typing, and tests.
- Some future high-throughput components may require optimized libraries, native extensions, or a separate implementation.
- A Python minor-version upgrade requires explicit compatibility testing.
- Pydantic models must be used deliberately because default coercion can hide bad input; critical boundaries must opt into strict validation.

## Alternatives Considered

### Python 3.12

Rejected as the default because it is already in security-only support. It may remain useful as a temporary compatibility target, but the new project should begin on a currently maintained bugfix release.

### Python 3.14

Deferred. It is stable and supported, but Python 3.13 provides a more conservative compatibility baseline for trading, data, and ML dependencies. Reconsider after dependency compatibility is demonstrated in CI.

### TypeScript or Rust as the primary language

Deferred as primary runtimes. They may later be appropriate for user interfaces, specialized services, or performance-sensitive components, but using them now would fragment the foundation before a measured need exists.

### pip with requirements files, Poetry, or Conda as the primary project workflow

Not selected. They remain valid tools, but `uv` provides the selected combination of Python management, project environments, locking, and command execution with a smaller workflow surface.

## Review Triggers

Revisit this ADR if:

- A required dependency does not support Python 3.13.
- Performance evidence shows Python is insufficient for a bounded component.
- The chosen tools stop receiving security or maintenance updates.
- The project needs a multi-language boundary with independently deployable components.

## References

- [CPython version status](https://devguide.python.org/versions/)
- [uv project guide](https://docs.astral.sh/uv/guides/projects/)
- [uv lockfile and project layout](https://docs.astral.sh/uv/concepts/projects/layout/)
- [Pydantic strict mode](https://docs.pydantic.dev/latest/concepts/strict_mode/)
- [Ruff configuration](https://docs.astral.sh/ruff/configuration/)
- [pytest good integration practices](https://docs.pytest.org/en/stable/explanation/goodpractices.html)
- [Hypothesis documentation](https://hypothesis.readthedocs.io/en/latest/)
- [mypy getting started](https://mypy.readthedocs.io/en/stable/getting_started.html)
