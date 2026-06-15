# Scoring rubric

Apply every rule below to **each** element in the data model (after excluding
control elements — controls are not materialization candidates). The rules are
derived rule-for-rule from the "Quick-reference: signals that earn
materialization" section of `sigma_materialization_best_practice.md`. Each rule
cites the doc section it implements; carry that citation into the report so the
reasoning stays traceable.

`reference/signals.md` tells you **how** to detect each signal in the spec JSON.
This file tells you **what it means and what it's worth**.

## How scoring works

1. Run the **hard exclusions** first. If any fires, the element is **skipped** —
   excluded from recommendations, reported with the rule that fired and a
   suggested alternative. Still note any positive signals it would have earned
   ("would score well, but excluded because…") so the alternative is actionable.
2. For non-skipped elements, add each fired **positive signal**'s weight and
   subtract each **penalty**'s weight. The result is the element's **score**.
3. Map score → **tier**:

   | Score | Tier |
   |---|---|
   | ≥ 5 | **high** |
   | 3–4 | **medium** |
   | 1–2 | **low** |
   | ≤ 0 | **not recommended** (penalties cancel the signals, or none fired) |

Weights are the *mechanism*, not a rule from the doc — they're set here so they
can be tuned in one place. They are deliberately simple and transparent. If
tuning them changes which tier an element lands in, that's expected; if a rule
itself contradicts the doc, fix the rule (or raise it), don't silently diverge.

---

## Hard exclusions

Materializing these produces output Sigma will silently bypass at query time, or
worse, produces incorrect results. Skip the element; suggest the alternative.

### H-CONTROL — targeted by a control
- **Fires when:** a page control targets this element (see `signals.md` §
  "Control targeting").
- **Why:** changing the control value queries live data, not the materialised
  table. Doc: *Hard exclusions > "Elements targeted by a control"*.
- **Alternative:** materialise the parent and target the control at a child
  element.

### H-USER-SCOPED — user-scoped system function
- **Fires when:** any formula or SQL references `CurrentUserAttribute`,
  `CurrentUserEmail`, or the `system::CurrentUser*` SQL templating form.
- **Why:** the result varies per user, so it cannot be correctly materialised.
  Doc: *Hard exclusions > "Elements containing user-scoped system functions"*.
- **Alternative:** materialise an un-scoped parent; apply the per-user filter in
  a child.

### H-DYNAMIC-TIME — dynamic time function
- **Fires when:** a formula references `Now()` / `Today()`, or the SQL references
  `CURRENT_DATE` / `CURRENT_TIMESTAMP` / `GETDATE()` / `SYSDATE` / `NOW()`. (The
  SQL forms are heuristic — they can appear in comments; the Sigma `Now()`/`Today()`
  forms are structural.)
- **Why:** these hardcode at publish time, producing stale/incorrect downstream
  data. Doc: *Hard exclusions > "Elements with dynamic time functions"*.
- **Alternative:** materialise the full table; apply the time window in a child.

### H-RELATIVE-DATE — relative date filter
- **Fires when:** the element has a relative-date filter (e.g. "last 7 days") —
  see `signals.md` § "Relative-date filter".
- **Why:** forces a full refresh and disables incremental dynamic-table refresh.
  Doc: *Hard exclusions > "Elements with relative date filters"*.
- **Alternative:** materialise the full underlying table; apply the relative
  window in a child.

### H-INPUT-TABLE — sourced directly from an Input Table
- **Fires when:** `source.kind` is an Input Table kind **and not** a Sigma Table
  kind (see `signals.md` § "Source kind").
- **Why:** the log-replay / sequence-number model invalidates the digest on every
  edit. Doc: *Hard exclusions > "Elements sourced directly from Input Tables
  (current architecture)"*.
- **Alternative:** materialise a child of the Input Table, not the Input Table
  itself.
- **Sigma Tables relaxation:** this rule does **not** fire for a `sigma-table`
  source kind. Doc: *"Sigma Tables transition"* — input-table digest invalidation
  goes away once the source is a Sigma Table. (Phase-2: extend source-kind
  detection as the spec exposes Sigma Tables.)

### H-SIGMA-ELEMENT — Custom SQL using sigma_element()
- **Fires when:** a custom SQL statement contains `sigma_element(`.
- **Why:** documented as unsupported for materialization. Doc: *Hard exclusions >
  "Custom SQL elements using sigma_element()"*.
- **Alternative:** inline the referenced logic as warehouse SQL, then materialise.

> **PHASE-2 hook — H-WAL.** The doc also hard-excludes elements in a WAL-based
> dependency chain, but WAL lineage is not visible in a static spec. Add this
> check when a marker becomes available; don't fabricate it today.

---

## Positive signals (raise score)

### P-JOIN3 — joins 3+ sources — weight **+3**
- **Fires when:** the element's source is a join over **three or more** sources
  (see `signals.md` § "Join source counting").
- **Why:** a non-trivial warehouse plan; joins are the canonical materialization
  win. Doc: *Positive patterns > "Heavy joins and unions"* and *Quick-reference >
  Positive signals*.

### P-WINDOW — window functions — weight **+2**
- **Fires when:** any formula or SQL contains a window function — `OVER(`,
  `RANK(`, `LAG(`, `LEAD(`, `ROW_NUMBER(`, `DENSE_RANK(`, `NTILE(`, etc.
- **Why:** expensive to recompute live. Doc: *Quick-reference > Positive signals*.

### P-CUSTOMSQL — custom SQL element — weight **+2**
- **Fires when:** `source.kind == "sql"` with a statement (and it isn't already
  hard-skipped by H-SIGMA-ELEMENT).
- **Why:** custom SQL re-runs verbatim on every query; materializing stable
  expensive SQL avoids repeated warehouse cost. Doc: *Positive patterns > "Custom
  SQL elements"*.

### P-AGG — aggregation at a non-leaf grain — weight **+2**
- **Fires when:** the element aggregates (`Sum`/`Count`/`Avg`/… in a column
  formula, or grouping present) **and** is not at the leaf grain.
- **Why:** Sigma materializes this level plus all coarser levels above it. Doc:
  *Positive patterns > "Aggregated grouping levels (not the leaf grain)"*.

### P-GROUPING — non-leaf grouping level — weight **+1**
- **Fires when:** the element has grouping levels and is not pinned at the leaf
  grain.
- **Why:** materialise from the level you care about upwards. Doc:
  *Quick-reference > Positive signals*.

### P-FANOUT — high downstream fan-out — weight **+2**
- **Fires when:** **three or more** elements transitively depend on this one (see
  `signals.md` § "Fan-out"). Threshold is tunable.
- **Why:** materializing once benefits every dependent. Doc: *Quick-reference >
  Positive signals*. (Phase-2: weight by real usage / cross-workbook fan-out from
  the admin API.)

### P-WAREHOUSE — warehouse-only source — weight **+1**
- **Fires when:** `source.kind` is a warehouse-table kind or a `sigma-table` kind.
- **Why:** a stable base to materialise. Doc: *Quick-reference > Positive signals*
  ("sourced only from warehouse tables (or, in future, Sigma Tables)").

### P-DATAMODEL — in a data model — weight **+1**
- **Fires when:** always (Phase 1 only analyses data-model specs).
- **Why:** data models are the preferred home for reusable materialization. Doc:
  *Where materialization belongs*. A constant baseline, recorded for traceability;
  it's why a bare element floors at score 1 rather than 0.

---

## Soft penalties (lower score)

### N-LEAF — materialised at the leaf grain — weight **−2**
- **Fires when:** grouping is present **and** pinned at the most granular
  (leaf / "all source columns") level. A raw, ungrouped table is **not** a leaf
  case — don't penalise it.
- **Why:** materializing the leaf grain is often unnecessary and costly. Doc:
  *Positive patterns > "Aggregated grouping levels"* ("Do not materialize the most
  granular level").

### N-JOIN-LIVE — joined downstream to a live element — weight **−2** *(heuristic)*
- **Fires when:** this element feeds a downstream join that also pulls in other,
  non-materialised sources.
- **Why:** partial plans often fall back to live, losing most of the win. Doc:
  *Soft penalties* table, row 1.

### N-SEMANTIC — semantic-view lineage — weight **−1** *(heuristic)*
- **Fires when:** `source.kind` is a Snowflake semantic view.
- **Why:** semantic views carry materialization limits (no joins/unions/transpose,
  controls can't target them, derived metrics unavailable). Doc: *Soft penalties*
  table.

### N-LOOKUP — uses lookups rather than joins — weight **−1** *(heuristic)*
- **Fires when:** a formula uses `Lookup(`.
- **Why:** materialization is used by joins, not by lookups. Doc: *Positive
  patterns > "Heavy joins and unions"* ("not by lookup") and *Quick-reference >
  Negative (penalty)*.

> **PHASE-2 penalty hooks** (need runtime / lineage data not in a static spec):
> - **N-TINY** — "row count / data volume expected to be tiny" (warehouse metadata).
> - **N-CLS** — "lineage of column-level-secured warehouse views" (connection metadata).
> - **N-ALREADY** — "already has a materialization schedule": handled as a separate
>   "already materialized" bucket via the schedules API, **not** a score penalty.

---

## Cadence band heuristic

For each non-skipped candidate, pick a band. Doc: *Quick-reference > "Cadence band
heuristic"*. Static analysis sees fan-out, source kind, and join cost; it cannot
see expected output size or upstream change frequency (Phase-2 inputs), so pick
the safest band the static signals support and **default to daily**.

- **Hourly** — high fan-out **and** a warehouse-only source. Many consumers
  benefit from freshness and the base is stable enough to rebuild often.
- **Weekly** — a very expensive join (3+ sources) with **no** in-model
  dependents. Costly to rebuild, nothing downstream waiting on it.
- **Daily** — the default for everything else.
- **Manual / on-demand** — when freshness matters less than cost. Can't be
  inferred statically; suggest it only if the user says freshness is low-priority.
