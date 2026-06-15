# Reply format

Three output modes:

- **Default — terse plain text.** Scannable in chat. No markdown structure.
- **`--verbose` — full markdown report.** Headings, grouped recommendations,
  cadence, skipped-with-alternatives. For a PR / ticket / doc.
- **`--json` — machine-readable.** For a PR bot or further tooling.

Every recommendation and every hard-skip **cites its `sigma_materialization_best_practice.md`
section** — that's the traceability guarantee. Heuristic findings are flagged.

---

## Default mode (terse)

A short plain-text reply. No tables, no heading hierarchy, no fenced blocks
(inline backticks for element/signal names are fine).

### Shape

```
<model label> — <H> high, <M> medium, <L> low, <N> not recommended; <S> skipped, <A> already materialized

  HIGH    <element name>  (score <n>, <cadence>) — <why: signals fired, plain English + doc cite>
  MEDIUM  <element name>  (score <n>, <cadence>) — <why>
  LOW     <element name>  (score <n>, <cadence>) — <why>

  not recommended: <element name> (score <n>) — <dominant penalty / "no positive signals">

  skipped: <element name> — <hard-skip reason> (<doc section>) → <suggested alternative>

  already materialized: <element name> — existing schedule found

— analysed <total> element(s); schedules <checked|not checked>
```

### Conventions

- **One line per element.** Two-space indent. `TIER` padded to 8 chars
  (`HIGH`, `MEDIUM`, `LOW`), uppercase.
- **Sort** within each tier by score, highest first.
- **Why:** the positive signals that fired, as short clauses, each with its doc
  cite in parentheses, e.g. `joins 3 sources (Positive patterns > Heavy joins)`.
  Append ` [heuristic]` to heuristic signals.
- **Cadence** in the parenthetical: `hourly` / `daily` / `weekly`.
- **Skipped** elements: one line each, prefixed `skipped:`, with the hard-skip
  reason, the doc section, and the suggested alternative after `→`. Never drop a
  skipped element silently — the alternative is the actionable part.
- **Already materialized** (live mode only): one line each, prefixed
  `already materialized:`. Omit the section and its headline count when zero or
  when schedules weren't checked.
- **Footer** states the element count and whether schedules were checked.

### Example (the `fixtures/sample-model.json` model, offline)

```
Plugs Electronics Sales (sample) — 2 high, 1 medium, 3 low, 2 not recommended; 6 skipped (schedules not checked)

  HIGH    Orders Enriched (3-source join)  (score 8, daily) — joins 3 sources, the canonical win (Positive patterns > Heavy joins and unions); aggregates above the leaf grain (Positive patterns > Aggregated grouping levels); 3 downstream dependents (Quick-reference > Positive signals)
  HIGH    Revenue Ranking (custom SQL)  (score 5, daily) — window functions LAG/OVER/RANK (Quick-reference > Positive signals); custom SQL re-runs verbatim (Positive patterns > Custom SQL elements)
  MEDIUM  Monthly Revenue by Family  (score 4, daily) — aggregation at a non-leaf grain (Positive patterns > Aggregated grouping levels); non-leaf grouping (Quick-reference > Positive signals)
  LOW     Orders (raw warehouse table)  (score 2, hourly) — 7 downstream dependents; warehouse-only source (Quick-reference > Positive signals); penalty: feeds a downstream live join [heuristic]
  LOW     Products (dimension)  (score 2, hourly) — 4 downstream dependents; warehouse-only source

  not recommended: Region Manager Lookup (score 0) — uses Lookup() not a join (Positive patterns > Heavy joins and unions) [heuristic]
  not recommended: Order Line Detail (leaf grain) (score -1) — materialised at the leaf grain (Positive patterns > Aggregated grouping levels)

  skipped: Reuses Another Element — Custom SQL uses sigma_element() (Hard exclusions) → inline the logic as warehouse SQL, then materialise
  skipped: My Region Orders — user-scoped function CurrentUserAttribute (Hard exclusions) → materialise an un-scoped parent; filter per-user in a child
  skipped: Today's Orders — dynamic time CURRENT_DATE (Hard exclusions) → materialise the full table; apply the window in a child
  skipped: Last 7 Days — relative date filter (Hard exclusions) → materialise the full table; apply the window in a child
  skipped: Manual Targets — sourced from an Input Table (Hard exclusions) → materialise a child of the Input Table
  skipped: Revenue by Region — targeted by control ctrl-region (Hard exclusions) → materialise the parent, target the control at a child

— analysed 14 elements; schedules not checked
```

---

## Verbose mode (`--verbose`)

A full markdown report. Use the structure below verbatim.

```
# Materialization Advisor — <model label>

## Summary
- <total> element(s) analysed
- <H> high-value candidate(s), <M> medium, <L> low, <N> not recommended
- <A> already materialized (skipped)        ← omit line if schedules not checked
- <S> hard-skipped — <reason (count); reason (count); …>
- <note if schedules not checked>

## Recommendations

### High value
#### <element name>
- **Why**: <signals fired, plain English, each citing the doc section>
- **Score**: <n>
- **Penalties**: <if any, each citing the doc section, [heuristic] where applicable>
- **Suggested cadence**: <hourly|daily|weekly>
- **Action**: Open <model label> > <element name> in Sigma and schedule materialization

### Medium value
…

### Low value
…

### Not recommended
- **<element name>** (score <n>) — <dominant penalty or "no material positive signals">

### Already materialized (skipped)      ← only when schedules were checked
- **<element name>** — an existing materialization schedule was found; not re-recommended

### Skipped (hard exclusions)
#### <element name>
- **Reason**: <hard-skip reason> (see <doc section>)
- **Suggested alternative**: <alternative>
```

Rules:
- Sort recommendations within each tier by score, highest first.
- One `#### ` block per recommended or skipped element; one bullet per
  not-recommended / already-materialized element.
- The **Why** clauses and **Reason** lines each carry their doc-section citation.
- Omit empty sections.

---

## JSON mode (`--json`)

Mirror the analysis field-for-field. One object per element under `elements`:

```json
{
  "data_model": { "id": "<id|null>", "name": "<label>", "warehouse": "snowflake|databricks|null" },
  "phase": 1,
  "schedules_checked": false,
  "summary": {
    "elements_analysed": 14,
    "high": 2, "medium": 1, "low": 3, "not_recommended": 2,
    "hard_skipped": 6, "already_materialized": 0
  },
  "elements": [
    {
      "element_id": "join-orders-enriched",
      "element_name": "Orders Enriched (3-source join)",
      "page": "Model",
      "source_kind": "join",
      "score": 8,
      "tier": "high",
      "signals_fired": [
        { "rule_id": "P-JOIN3", "label": "Joins 3+ sources", "weight": 3,
          "detail": "joins 3 sources …", "doc_section": "…Heavy joins and unions",
          "confidence": "structural" }
      ],
      "hard_skip_reasons": [],
      "suggested_cadence": "daily",
      "doc_section_references": ["…Heavy joins and unions", "…Positive signals"],
      "dependencies": ["tbl-orders-raw", "tbl-products", "tbl-regions"],
      "dependents": ["agg-monthly", "agg-leaf", "viz-controlled"]
    }
  ]
}
```

Field rules:
- `tier` is `high` | `medium` | `low` | `none` | `skip`, or `already-materialized`
  when an existing schedule was found.
- `hard_skip_reasons[]` carries `rule_id`, `label`, `detail`, `doc_section`,
  `suggested_alternative`.
- `signals_fired[]` includes positives (positive `weight`) and penalties
  (negative `weight`), each with `confidence` (`structural` | `heuristic`).
- `doc_section_references` is the de-duplicated set of doc sections cited by this
  element's signals and skips.
- `summary` counts must add up to `elements_analysed`.

---

## Common rules

- **Cite the doc on every call.** No signal or skip without a
  `sigma_materialization_best_practice.md` section.
- **Flag heuristics** (`[heuristic]` terse / `confidence: heuristic` JSON) — don't
  state them with structural confidence.
- **Read-only.** Never print a schedule-creation or write-back action; this skill
  recommends. The `--no-check-schedules` / offline note must appear when schedules
  weren't cross-referenced, so the reader knows already-materialized elements may
  appear in the recommendations.
- **Stdout only** unless the user asks to save.
