# Fixtures

Data-model spec excerpts used to validate the advisor's behaviour. Run the skill
against a fixture and confirm the expected outcome below. Use these as the
regression set when tuning `reference/rubric.md` or `reference/signals.md`.

## `sample-model.json`

A synthetic but realistic Plugs Electronics model built to exercise **every**
rubric rule at least once — joins, window functions, grouping grain, fan-out,
and all six hard exclusions. 14 scored elements + 1 control.

> **Synthetic, and partly INFERRED.** Its `sql` and `warehouse-table` sources
> match confirmed real shapes; its joins, grouping, controls, and relative-date
> filter use the **INFERRED** shapes from `reference/signals.md`. Replace this
> with a real fetched spec when one is available, and re-confirm the outcomes.

### Expected outcome

**14 elements analysed — 2 high, 1 medium, 3 low, 2 not recommended, 6 skipped.**

High value:
- **Orders Enriched (3-source join)** — score 8. P-JOIN3 (joins `tbl-orders-raw`,
  `tbl-products`, `tbl-regions`) + P-AGG + P-FANOUT (3 dependents) + P-DATAMODEL.
  Cadence: daily.
- **Revenue Ranking (custom SQL)** — score 5. P-WINDOW (`RANK`/`LAG`/`OVER`) +
  P-CUSTOMSQL + P-DATAMODEL. Cadence: daily.

Medium value:
- **Monthly Revenue by Family** — score 4. P-AGG + P-GROUPING + P-DATAMODEL
  (grouped at `Order Month, Product Family`, a non-leaf grain).

Low value (score 2 each): **Orders (raw warehouse table)**, **Products
(dimension)**, **Regions (dimension)** — P-WAREHOUSE + P-FANOUT + P-DATAMODEL,
the dimensions carrying an N-JOIN-LIVE heuristic penalty. The raw orders table
suggests **hourly** (high fan-out + warehouse-only).

Not recommended:
- **Region Manager Lookup** — score 0. P-DATAMODEL (+1) cancelled by N-LOOKUP
  (−1, heuristic).
- **Order Line Detail (leaf grain)** — score −1. P-DATAMODEL (+1) + N-LEAF (−2):
  grouped at "All source columns".

Skipped (hard exclusions), each with its alternative:
- **Reuses Another Element** — H-SIGMA-ELEMENT (`sigma_element('agg-monthly')`).
- **My Region Orders** — H-USER-SCOPED (`{{system::CurrentUserAttributeText::Store_Region}}`).
- **Today's Orders** — H-DYNAMIC-TIME (`CURRENT_DATE`).
- **Last 7 Days** — H-RELATIVE-DATE (`lastNDays: 7` filter).
- **Manual Targets** — H-INPUT-TABLE (`source.kind == "input-table"`).
- **Revenue by Region** — H-CONTROL (control `ctrl-region` targets it).

### Regressions this guards against

- The raw, ungrouped orders/products/regions tables must **not** trip N-LEAF —
  leaf-grain is only for grouping pinned at the base grain, not for any table
  without grouping.
- The bare `{{product-family-filter}}`-style control reference must not be
  counted as a user attribute (only `system::CurrentUser*` / `CurrentUser*` forms
  are H-USER-SCOPED).
- `sigma-table` sources must **not** fire H-INPUT-TABLE (Sigma Tables relaxation).

## `rls-fixture-secured-control-plus-ua.json`

Reused from the sibling `sigma-model-sql-rls-audit` skill — a single Custom SQL
element with window functions (`RANK`/`LAG`/`OVER`), a `{{product-family-filter}}`
control reference, **and** a `{{system::CurrentUserAttributeText::Store_Region}}`
user attribute.

### Expected outcome

**1 element analysed — 1 skipped (H-USER-SCOPED).** Even though it carries strong
positive signals (window functions + custom SQL), the per-user
`CurrentUserAttributeText` reference is a hard exclusion: the result varies per
user and can't be correctly materialised. Report it skipped, note the positives
it *would* have earned, and suggest materializing an un-scoped parent with the
per-user filter pushed to a child.

This is the "strong candidate, but hard-skipped" path — confirm the skill still
surfaces the alternative rather than just suppressing the element.
