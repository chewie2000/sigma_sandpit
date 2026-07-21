# sigma-org-audit

> **Proof of concept.** This skill is a reference implementation shared to demonstrate an approach and give others something to extrapolate from — not a finished, supported, or authoritative tool. Take the ideas, adapt the patterns, build your own. Its output is illustrative; don't treat it as a definitive governance or migration verdict.

> **Disclaimer:** This skill (and the repository it lives in) contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A Claude Code skill that renders a **governance / migration-readiness audit of a
Sigma organisation** from the `sigma_org_audit` warehouse marts, optionally
cross-checked against **live** data via the `sigma-cli` sub-skill.

It reports: org inventory, R/A/G migration scoring with reasons, ownership
cleanup queue, writeback (input-table) governance with archival scoring and
reclaimable storage, orphaned/stale SIGDS tables, and drift between snapshots.

> **Scope, up front.** Read-only — it reports, it does not remediate (the
> Call-API action layer is Phase 2). It *consumes* the `sigma_org_audit` SQL
> pipeline; it does not build it. Migration scoring is a Phase-1 heuristic
> (workbook fan-out only, not full dataset chains).

## Source modes

- `--source marts` (default) — warehouse views only: deep, historical, scored.
- `--source sigcli` — live current state only, via the `sigma-cli` sub-skill.
- `--source both` — marts verified/enriched with live; **SIGCLI is authoritative
  on conflict**, and the disagreement is reported as drift.

Every value in the report is tagged with its **provenance** (`sigcli` | `marts` |
`inferred`). If live verification is requested but unavailable, the skill
degrades to marts/inference and says so at the top of the report.

## Files

| File | Purpose |
|---|---|
| `SKILL.md` | Workflow, source modes, conflict rule, behavioral rules. |
| `reference/object-catalog.md` | What each object type captures and the question it answers. |
| `reference/scoring-rubric.md` | R/A/G + writeback archival + ownership rules (cite for every score). |
| `reference/report-template.md` | Report layout with the provenance column. |

## Related

- `sigma_org_audit/` (SQL project) — the raw→stage→marts pipeline this reads.
- `sigma-cli` — the sub-skill that wraps the `sigcli` binary for live data.
- `sigma-materialization-advisor`, `sigma-model-sql-rls-audit` — sibling skills.
