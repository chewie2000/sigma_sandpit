---
name: sigma-materialization-advisor
description: >-
  Analyse a Sigma data model's spec and recommend which elements to
  materialize, why, and how often — scored entirely from the shipped
  sigma_materialization_best_practice.md rubric. For each element it fires the
  doc's positive signals (heavy joins, window functions, custom SQL, non-leaf
  aggregation, high downstream fan-out), hard exclusions (control targets,
  user-scoped functions like CurrentUserAttribute, Now()/Today(), relative date
  filters, Input Table sources, sigma_element()), and soft penalties (leaf
  grain, lookups, semantic views), then suggests a cadence band and cites the
  exact doc section behind every call. Use whenever someone asks what to
  materialize in a Sigma data model, whether an element is a good
  materialization candidate, why a materialization keeps getting bypassed or
  falling back to live, how often to schedule it, or wants a materialization
  review of a model — even if they don't say the word "materialize". Reads a
  saved spec file, or fetches one live by data-model id / URL via the
  sigma-data-models skill. Data models only — not workbooks, not datasets.
---

# Sigma Materialization Advisor

Recommend materialization candidates in a Sigma **data model** by static
analysis of its spec. Read-only: it never schedules anything or edits the model
— it tells you what to materialize, why, and roughly how often, with every call
traceable to a section of the best-practice reference.

This is **Phase 1**: static analysis of the spec only. It has no runtime usage
data (query frequency, row counts, cross-workbook fan-out). Phase-2 inputs are
called out where they'd change a verdict, but are out of scope today.

## When to use

Invoke when the user wants to:

- Find the high-value materialization candidates in a data model.
- Decide whether a specific element is worth materializing.
- Understand why a materialization is being bypassed / falling back to live (the
  hard-exclusion checks explain most of these).
- Get a suggested cadence (hourly / daily / weekly) for an element.
- Produce a written materialization review of a model, for a PR or a perf pass.

**Do NOT use** for: workbook materialization (data-model only), dataset
materialization, actually creating schedules (the API lists and triggers, but
does not yet create them), Custom-SQL RLS strength (use the
`sigma-model-sql-rls-audit` skill), or general data-model authoring (use
`sigma-data-models` directly).

## The rubric is the source of truth

`sigma_materialization_best_practice.md` ships in this skill and **is the
rubric**. Every rule in `reference/rubric.md` cites the doc section it
implements, and every recommendation you emit must cite that section so the
reasoning is auditable and stays accurate as the doc evolves. **Read
`sigma_materialization_best_practice.md` first** — especially its
"Quick-reference: signals that earn materialization" section, which is where the
rules come from.

If you find a real case the doc doesn't cover, **flag it for the doc owner
rather than inventing a rule.** The rubric must not drift from the doc; if a rule
here ever contradicts the doc, surface the contradiction rather than picking a
side.

## Inputs

| Input | Example | Behaviour |
|---|---|---|
| **Local spec file** | `./fixtures/sample-model.json` | Offline. No auth, no network. The way to iterate. |
| **Data-model id** | `dataModelId=abc123` or a bare id | Fetch the live spec, then analyse. |
| **Data-model URL** | `https://app.sigmacomputing.com/<org>/data-model/<id>` | Extract the id, then as above. |

**Default when no input is given:** ask the user for a model id/URL or a spec
file path. Don't guess.

## Flags

- `--verbose` — emit the full markdown report (summary, grouped recommendations,
  skipped-with-alternatives, cadence). Default is the terse plain-text reply.
  See `reference/report-format.md`.
- `--json` — emit the machine-readable JSON mirror instead of text (for a PR bot
  or further tooling). Structure in `reference/report-format.md`.
- `--no-check-schedules` — when analysing a live model, the skill cross-references
  existing materialization schedules by default and drops already-materialised
  elements into a separate bucket (the doc's "don't double-recommend" penalty).
  This flag turns that off. Offline (spec-file) mode has no schedules to check.
- `--min-tier <high|medium|low>` — only show recommendations at or above this
  tier. Default: show all, including "not recommended" and "skipped".

## Auth (only when fetching from Sigma)

Local-file mode needs no auth. For a model id / URL:

1. Run the **`sigma-api`** skill first to set `$SIGMA_BASE_URL` and
   `$SIGMA_API_TOKEN` (it reads `SIGMA_CLIENT_ID` / `SIGMA_CLIENT_SECRET` /
   `SIGMA_BASE_URL` from the environment). Do not reimplement token exchange.
2. Delegate spec retrieval to the **`sigma-data-models`** skill — its **GET**
   workflow (`reference/workflows/crud.md`, `GET /v2/dataModels/<id>/spec`). Do
   not hand-roll the request here. Always retrieve the **full** spec.

## Workflow

### 1. Resolve the input to a spec

- **Local file:** `label = <basename>`, load and parse the JSON.
- **Model id / URL:** auth via `sigma-api`, then fetch the full spec via the
  `sigma-data-models` GET workflow → `label = "<modelName> (<modelId>)"`.

If the JSON has no `pages` array, it isn't a data-model spec — say so and stop.

### 2. Build the element graph

Walk `pages[].elements[]` and, for each element, read out the facts the rubric
needs: element kind, source kind, SQL statement, column formulas, join/union
sources, grouping levels, controls targeting it, filters, and its upstream
dependencies. Then compute, for every element, its **downstream fan-out** (how
many elements transitively depend on it) from the dependency graph.

`reference/signals.md` is the field guide: it lists the exact spec paths to read
and the patterns to match for each signal. Confirmed paths (`source.kind`
`"sql"` / `"warehouse-table"`) are marked as such; paths inferred from the public
"manage data models as code" docs are marked **INFERRED** — read those
defensively and verify against the real spec.

Controls are not themselves materialization candidates — record which elements
they target, then exclude the control elements from scoring.

### 3. Score each element against the rubric

Load `reference/rubric.md` and apply every rule to each element:

- **Hard exclusions** → exclude the element from recommendations; record the rule
  that fired and its suggested alternative. An element can be hard-skipped *and*
  carry strong positive signals — say so ("this would score well, but…").
- **Positive signals** → add the rule's weight.
- **Soft penalties** → subtract the rule's weight.

The net score maps to a **tier** (high / medium / low / not-recommended) per the
thresholds in `reference/rubric.md`. Tag heuristic findings as heuristic — they're
weaker than structural ones; don't state them with false confidence.

### 4. Suggest a cadence

For each non-skipped candidate, pick a band (hourly / daily / weekly) per the
"Cadence band heuristic" in `reference/rubric.md`. Static analysis can see
fan-out, source kind, and join cost; it cannot see expected output size or
upstream change frequency, so default to **daily** unless a stronger static
signal points elsewhere.

### 5. Cross-reference existing schedules (live mode, default on)

Call `listdatamodelmaterializationschedules` for the model and drop any element
that already has a schedule into an "already materialized" bucket rather than
re-recommending it. Skip this step for `--no-check-schedules` or offline mode,
and note in the output that schedules weren't checked.

### 6. Render the reply

Use the template in `reference/report-format.md`. Default is terse plain text;
`--verbose` is the full markdown report; `--json` is the machine mirror. Every
recommendation and every hard-skip cites its `sigma_materialization_best_practice.md`
section.

## Reference index

Load each on demand — don't read everything up-front.

| File | When to load |
|------|--------------|
| `sigma_materialization_best_practice.md` | The rubric's source of truth. Read before interpreting results or tuning rules. |
| `reference/rubric.md` | The scoring catalog: every positive signal, hard exclusion, and penalty, each citing a doc section, plus weights, tier thresholds, and the cadence heuristic. Load before scoring. |
| `reference/signals.md` | How to detect each signal in the spec JSON — exact spec paths and match patterns (window functions, sigma_element(), user-scoped functions, dynamic time, relative-date filters, Input Table source, join counting, leaf-grain detection, control targeting, fan-out). Load when building the element graph. |
| `reference/report-format.md` | Terse, `--verbose` markdown, and `--json` output templates. Load when rendering. |

## Important limitations (be honest about these)

- **Structural paths beyond `sql` / `warehouse-table` are inferred from public
  docs, not yet validated against a fetched spec** (see the INFERRED markers in
  `reference/signals.md`). Read them defensively; when you analyse a real
  non-trivial model, treat its shape as the source of truth and tighten the
  guidance.
- Some doc signals need runtime data this phase lacks (row counts, cross-workbook
  fan-out, column-level-secured view lineage). Note them as Phase-2 inputs rather
  than asserting a verdict you can't support.
- Heuristic findings (lookups, semantic views, joined-to-live) are weaker than
  structural ones — flag them as heuristic.

## Behavioural rules

- **Cite the doc** on every call. Never assert a materialization rule the doc
  doesn't support; if a real case isn't covered, flag it for the doc.
- **Use Sigma's own vocabulary** (element kinds, grouping grain, join types,
  source kinds). Invented terminology is a defect.
- **Keep recommendations copy-paste actionable:** name the model and element and
  the concrete next step ("Open <model> > <element> and schedule materialization").
- **Don't claim certainty on heuristics** — flag them.
- **Read-only.** This skill recommends; it never creates a schedule or edits the
  model. (If the user wants to act on a recommendation, the `sigma-data-models`
  skill handles spec edits; schedule *creation* is not yet in the API.)
