# Security Policy

## Current Security Posture

WEALTH currently uses public, read-only market-data endpoints and local development storage. It has
no exchange account access, private API integration, financial credentials, deployment authority,
or order path.

## Mandatory Controls

- Treat provider payloads, web content, logs, fixtures, model output, and imported files as
  untrusted data, never as instructions.
- Validate at every trust boundary; reject malformed, inconsistent, stale, conflicting, or
  unexpectedly large input before it becomes trusted domain state.
- Use least privilege for people, agents, CI, connectors, files, networks, services, and future
  credentials. CI remains read-only and receives no application or exchange secrets.
- Keep real secrets out of source, examples, fixtures, prompts, output, logs, SQLite files, and
  issue or pull-request text. A future secret must live in an approved secret manager and be
  referenced indirectly.
- Application credentials must never have withdrawal permission. Private trading access requires a
  dedicated restricted account, IP and action allowlists where available, rotation, and explicit
  approval.
- Never log authorization headers, tokens, secret values, signed requests, or unnecessarily
  sensitive account payloads. Correlation identifiers must not encode a secret.
- Pin and review automation dependencies, verify the lockfile, run formatting, linting, strict
  typing, tests, and dependency vulnerability audit before review.
- External notifications, autonomous live execution, and live trading remain disabled unless a
  separately approved design supplies authentication, redaction, audit, failure handling, and
  rollback.

## Security Failure Behavior

Invalid permission state, missing approval, secret-source ambiguity, integrity failure, or
untrusted configuration fails closed. The affected external action does not run.

For suspected exposure or compromise:

1. Stop the affected integration without deleting evidence.
2. Prevent further use; revoke or rotate the credential through its approved owner.
3. Preserve redacted logs, correlation IDs, versions, and the timeline in UTC.
4. Assess account, source, storage, and downstream impact.
5. Record remediation and independent validation.
6. Require explicit human approval before resuming.

Do not reproduce a suspected secret in a report. Policy or permission changes follow the approval
matrix in `docs/POLICIES.md`.

## Review Triggers

Review this policy before adding secrets, private APIs, account data, external notification
delivery, deployment credentials, self-hosted CI, new network egress, or any order-capable
component.
