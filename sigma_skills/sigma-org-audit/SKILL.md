---
name: sigma-org-audit
description: >-
  Produce a holistic governance / migration-readiness audit of a Sigma
  organisation. Reads the sigma_org_audit warehouse marts (raw->stage->marts
  pipeline over the Sigma REST API plus a writeback-schema scan) and renders a
  structured report: org inventory, R/A/G migration scoring with reasons,
  ownership cleanup queue, writeback (input-table) governance with archival
  scoring and reclaimable storage, orphaned/stale SIGDS tables, and drift
  between snapshots. Optionally cross-checks and enriches findings with LIVE
  data via the sigma-cli sub-skill — when live and warehouse data disagree,
  the live (SIGCLI) value is authoritative and the delta is reported as drift.
  Every field in the report is tagged with its provenance (sigcli | marts |
  inferred). Use whenever someone asks for a Sigma org audit, environment
  review, governance check, migration assessment, "what's orphaned / stale /
  unowned", writeback cleanup, or a holistic view of an org's setup. Read-only:
  it reports, it does not remediate (Phase-2 action layer is out of scope).
---

# Sigma Org Audit

Render a governance and migration-readiness audit of a Sigma org from the
`sigma_org_audit` marts, optionally verified against live data via `sigma-cli`.

This is **Phase 1**: reporting only. It reads the warehouse views (and live
SIGCLI where asked) and produces findings. It does **not** remediate — the
Call-API action layer (transfer ownership, retag, swap sources, archive) is
Phase 2 and out of scope.

## When to use

Invoke when the user wants to:

- Audit a Sigma organisation's setup / configuration holistically.
- Assess migration readiness (Datasets -> Data Models), with R/A/G + reasons.
- Find orphaned, stale, or unowned objects; writeback (input-table) cleanup
  candidates; reclaimable warehouse storage.
- See what changed between snapshots (drift).

**Do NOT use** for: actually changing anything in Sigma (Phase 2), data-model
materialization advice (use `sigma-materialization-advisor`), Custom-SQL RLS
strength (use `sigma-model-sql-rls-audit`), or building the warehouse pipeline
itself (that is the `sigma_org_audit` SQL project — this skill consumes it).

## Inputs and source modes

The audit has two data planes; choose with `--source`:

| `--source` | Behavior |
|---|---|
| `marts` (default) | Read the warehouse views only. Deep, historical, scored, replayable. Requires the `sigma_org_audit` pipeline to have been run. |
| `sigcli` | Read live current state via the `sigma-cli` sub-skill only. No warehouse needed; no history/scoring depth. |
| `both` | Read marts, then verify/enrich with live SIGCLI. **SIGCLI is authoritative on conflict** (see below). Best fidelity. |

Other inputs: the Snowflake `database.schema` holding the marts (for `marts`/
`both`); optional `--focus` (one of `inventory`, `migration`, `writeback`,
`ownership`, `drift`) to scope the report.

## Conflict rule — SIGCLI is authoritative

When `--source both` and a live SIGCLI value disagrees with the marts/inferred
value:

1. Use the **SIGCLI** value in the report.
2. Keep the marts value as the *prior*, and surface the difference as a **drift**
   finding (the warehouse snapshot is stale relative to live).
3. Tag the field `source: sigcli`.

When SIGCLI is unavailable (binary missing, `auth` fails — exit code 2), degrade
to marts/inference, tag fields `source: marts | inferred`, and say so explicitly
in the report header. Never silently drop to a weaker source.

## Provenance

Every value the report emits carries a provenance tag:

- `sigcli` — confirmed against live API via `sigma-cli`.
- `marts`  — read from the warehouse views (a snapshot, possibly stale).
- `inferred` — computed/heuristic (e.g. R/A/G scoring, archival score), not a
  directly observed fact.

`reference/report-template.md` shows where the provenance column goes.

## Workflow

1. **Resolve source mode and target.** Determine `--source`, and for marts the
   `database.schema`. Confirm the pipeline has been run (query `RAW_SIGMA_OBJECTS`
   snapshot coverage — see `audit_queries.sql` query 9).
2. **Pull the marts** (unless `--source sigcli`). Run the relevant
   `audit_queries.sql` queries for the requested `--focus` (all, by default).
3. **Verify live** (if `--source both` or `sigcli`). Delegate to the `sigma-cli`
   skill to fetch the corresponding live objects (connections, workbooks, data
   models, members). Apply the conflict rule.
4. **Score and classify** against `reference/scoring-rubric.md` — R/A/G migration,
   writeback archival score/confidence, ownership flags. Cite the rubric section
   behind each call. Mark scoring outputs `source: inferred`.
5. **Render** the report from `reference/report-template.md`, with the provenance
   column populated and an explicit note on which source was used and any
   degradation.

## Reference index (load on demand)

- `reference/object-catalog.md` — every object type the pipeline captures, the
  endpoint / scan behind it, and what governance question it answers.
- `reference/scoring-rubric.md` — R/A/G migration rules, writeback archival
  weighted model, ownership flags. **The auditable source of truth** — cite it.
- `reference/report-template.md` — the report layout, incl. the provenance column
  and the writeback-governance section.

## Auth

- Marts: standard Snowflake access to the `sigma_org_audit` database/schema.
- Live: delegated to the **`sigma-cli`** sub-skill, which wraps the `sigcli`
  binary and reads `SIGMA_CLIENT_ID` / `SIGMA_CLIENT_SECRET` / `SIGMA_BASE_URL`
  from the environment. This skill never handles secrets directly.

## Behavioral rules

- **Cite the rubric** for every score/classification, so the reasoning stays
  auditable as the rubric evolves.
- **Tag every emitted field** with provenance. Never present an inferred score as
  an observed fact.
- **SIGCLI wins on conflict**, and the conflict itself is a reported finding.
- **Degrade loudly**, never silently — if live verification was requested but
  unavailable, say so at the top of the report.
- **Read-only.** Recommend remediation in words; do not perform it (Phase 2).
- Use Sigma vocabulary (data model, dataset, workbook, connection, input table,
  writeback, WAL, version tag, materialization).
