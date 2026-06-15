# sigma-materialization-advisor

> **Proof of concept.** This skill is a reference implementation shared to demonstrate an approach and give others something to extrapolate from — not a finished, supported, or authoritative tool. Take the ideas, adapt the patterns, build your own. Its output is illustrative; don't treat it as a definitive materialization verdict.

> **Disclaimer:** This skill (and the repository it lives in) contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A Claude Code skill that reads a Sigma **data model** spec and recommends which
elements to **materialize**, why, and roughly how often — by static analysis,
scored entirely from a shipped best-practice rubric.

For each element it fires the rubric's positive signals / hard exclusions / soft
penalties, computes downstream fan-out, suggests a cadence band, and reports the
result with **every call cited back to a section of the best-practice doc**.

> **Scope, up front.** Data models only — not workbooks, not datasets. **Phase 1
> is pure static analysis**: it sees the spec, not runtime usage. It does not see
> query frequency, row counts, or cross-workbook fan-out, and it does not create
> schedules (the Sigma API lists and triggers, but doesn't yet create them). Read
> the recommendations as "what the spec's shape suggests", not "what your
> warehouse bill proves".

---

## How it's built

This is an **instruction-driven** skill, same shape as the sibling
`sigma-model-sql-rls-audit`: a `SKILL.md` workflow plus `reference/` catalogs
that Claude applies. There is no bundled program to run — Claude reads the spec,
applies the rubric, and writes the report.

It **reuses the existing Sigma skills** rather than reimplementing them:

- **Auth** → the `sigma-api` skill (mints `SIGMA_API_TOKEN` from
  `SIGMA_CLIENT_ID` / `SIGMA_CLIENT_SECRET` / `SIGMA_BASE_URL` in your env).
- **Spec retrieval** → the `sigma-data-models` skill's GET workflow
  (`GET /v2/dataModels/{id}/spec`).

Offline analysis of a saved spec file needs neither.

```
SKILL.md                                 workflow + cross-skill handoff
sigma_materialization_best_practice.md   the rubric's source of truth
reference/
  rubric.md         scoring catalog — signals, weights, tiers, cadence (cites the doc)
  signals.md        how to detect each signal in the spec JSON (spec paths + patterns)
  report-format.md  terse / --verbose markdown / --json templates
fixtures/
  sample-model.json                         synthetic model exercising every rule
  rls-fixture-secured-control-plus-ua.json  reused from the RLS sibling
  README.md                                 expected outcomes (regression set)
```

## What it checks

Derived rule-for-rule from the best-practice doc's quick-reference lists (see
`reference/rubric.md`):

| Family | Rules |
|---|---|
| **Positive signals** | joins 3+ sources, window functions, custom SQL, aggregation at non-leaf grain, non-leaf grouping, high fan-out, warehouse-only source, in a data model |
| **Hard exclusions** | targeted by a control, user-scoped function, dynamic time `Now()`/`Today()`, relative date filter, Input Table source, `sigma_element()` custom SQL |
| **Soft penalties** | leaf-grain materialization, joined downstream to a live element, semantic-view lineage, lookups not joins |

The Input-Table hard-skip relaxes for a `sigma-table` source kind, per the doc's
"Sigma Tables transition".

## Usage

Ask Claude (with this skill available) something like:

- "What should I materialize in `fixtures/sample-model.json`?"
- "Run the materialization advisor on data model `abc123`."
- "Why does my materialization on the Orders model keep falling back to live?"
- "Give me a verbose materialization review of `https://app.sigmacomputing.com/<org>/data-model/<id>`."

Flags Claude honours (see `SKILL.md`):

- `--verbose` — full markdown report (default is terse).
- `--json` — machine-readable mirror.
- `--no-check-schedules` — skip cross-referencing existing schedules (live mode
  checks them by default and drops already-materialised elements).
- `--min-tier high|medium|low` — only show recommendations at/above a tier.

### Auth (live mode only)

Set the env vars in your shell profile, then the `sigma-api` skill handles the
token exchange:

```
SIGMA_CLIENT_ID
SIGMA_CLIENT_SECRET
SIGMA_BASE_URL        # e.g. https://aws-api.sigmacomputing.com
```

## Output tiers

- **High / Medium / Low value** — recommend scheduling, best first.
- **Not recommended** — net non-positive score (penalties cancel the signals).
- **Already materialized** — an existing schedule was found (live mode).
- **Skipped (hard exclusions)** — would be bypassed or produce wrong results;
  each carries a suggested alternative (usually "materialise a parent/child").

## The real spec is the source of truth for detection

The `sql` and `warehouse-table` source shapes are confirmed against real
fixtures. The shapes for **joins, unions, grouping levels, controls, and
relative-date filters are inferred from the public "manage data models as code"
docs and not yet validated against a fetched spec** — they're marked **INFERRED**
in `reference/signals.md`. When you analyse a real non-trivial model, treat its
actual shape as the source of truth and tighten that guidance, then re-confirm
the fixture outcomes in `fixtures/README.md`.

## Phase 2 (designed in, not built)

Usage-weighted scoring (admin API / audit logs), warehouse row-count lookups for
the "tiny table" penalty, auto-emitted schedule payloads once the API supports
creation, GitHub data-model-manager integration, and fuller Sigma Tables source
detection. The `reference/rubric.md` catalog marks each Phase-2 hook.
