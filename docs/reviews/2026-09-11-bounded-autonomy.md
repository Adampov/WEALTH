# Bounded continuation and prompt maintenance

## Task contract

Owner request, 2026-09-11: continue independently, attend to computer performance and cleanup,
and improve the working prompt each cycle. Scope: `AGENTS.md`, sections 10.1-10.3 of
`docs/QUANT_ORG_OS.md`, and this review note. Root is the only writer. Base:
`6a43b99e20b2d8621802ac8c799715ff752be7c5`; branch `agent/bounded-autonomy`.

This is a development-workflow clarification. No runtime, tests, required checks, approval
semantics, financial authority, source-of-truth state, TASK-064 contract, or PR #69 output changes.
The owner explicitly placed prompt maintenance in scope. Existing governance and approval
boundaries remain controlling for their respective actions.

Acceptance: inspect the exact diff; run the unchanged governance assertions; independently review
the scenarios below; confirm the prompt remains within its existing size bound; publish and verify
unchanged CI. Rejecting or reverting this commit restores the prior prompt without data migration.

## Observed problems and intended changes

- Independent continuation was requested repeatedly, but no concise resumed-cycle procedure tied
  authorized work to one concrete result and a reusable checkpoint. Section 10.1 adds that procedure.
- Repeated broad validation and artifact generations can consume resources without new evidence.
  Reuse is now tied to unchanged exact inputs; failed, stale, or skipped evidence is not upgraded.
- The host had little free RAM while temporary-file quarantine reclaimed neither RAM nor disk.
  Section 10.2 separates diagnosis, concurrency reduction, safe cleanup, and measured outcomes.
- A cleanup implementation initially trusted rename success without checking actual filenames.
  Verification found and repaired the mismatch without losing files. This motivates verified
  readback and durable pre-mutation recovery, not broader deletion authority.
- "Improve every cycle" could cause uncontrolled prompt growth. Section 10.3 requires a demonstrated
  failure and a small checked change; an unchanged prompt is appropriate when there is no new lesson.

The autonomy, instruction clarity, and proportional verification direction is consistent with
the [current official Astra guidance](https://developers.openai.com/api/docs/guides/latest-model#prompting-best-practices),
reviewed 2026-09-11. The host-cleanup constraints come from the observed local incident and existing
project integrity requirements, not from a claim that prompting guarantees safe filesystem behavior.

## Representative decision checks

| Scenario | Intended result |
|---|---|
| Owner absent; bounded development fix and acceptance checks are clear | Implement and verify without another routine confirmation |
| Same commit, same relevant environment, successful expensive suite already recorded | Reuse evidence unless a new concern warrants rerun; still satisfy required gates |
| Low RAM twice; no verified unused task-owned helper | Start no additional heavy batch; do not kill the largest application |
| Same-volume temporary-file quarantine completes | Report moved bytes separately, with zero disk-reclamation claim |
| Cleanup has only in-memory recovery, or post-move readback is uncertain | Keep unattended cleanup report-only, or stop the affected batch; preserve evidence |
| PR green but exact owner merge approval absent | Use review-ready only if all its criteria hold; otherwise keep the actual incomplete state; no merge or dependent use of unmerged outputs |
| Prompt already works and no new failure occurred | Record the outcome; do not add another rule |
| A prompt edit would relax Risk, credentials, checks, or approval requirements | Follow existing elevated gates; do not apply it as routine maintenance |

These are reviewed behavioral scenarios, not empirical model evals or proof of runtime enforcement.

## Validation and review

The unchanged `tests/unit/test_quant_org_os.py` assertions passed (5/5), including the existing
50,000-byte limit and financial/approval invariants. `git diff --check` passed. A separate read-only
reviewer requested three clarifications: permit legitimate disjoint writers, retain all lifecycle
criteria before claiming review-readiness, and scope durable journaling to cleanup rather than
ordinary code edits. All three were applied; the reviewer found no remaining blockers in the eight
scenarios. No runtime code, tests, workflow or dependency changed. Full unchanged CI is checked on
the published commit; a local full runtime-suite rerun is not claimed for this documentation diff.
