# Signal detection — spec paths and patterns

How to read each signal out of a data-model spec (the JSON from
`GET /v2/dataModels/{id}/spec`). `reference/rubric.md` says what each signal is
worth; this file says how to find it.

**Confidence of the spec shape itself:**
- ✅ **CONFIRMED** against real fixtures: `source.kind == "sql"` (with
  `source.statement`) and `source.kind == "warehouse-table"`, plus
  `columns[].formula`.
- ⚠️ **INFERRED** from Sigma's public "manage data models as code" docs, **not
  yet validated against a fetched spec**: joins, unions, grouping levels,
  controls, and relative-date filters. Read these defensively — accept any of the
  listed key-name variants, and treat a missing key as "signal absent", never an
  error. When you analyse a real non-trivial model, treat its actual shape as the
  source of truth and correct this file.

Be defensive throughout: Sigma uses a few key-name variants across element kinds.
Where variants are listed, accept the first one present.

---

## Element traversal

Walk `pages[].elements[]`. For each element read:

- **id** — `id` | `elementId` | `nodeId`
- **name** — `name` | `title` (fall back to id)
- **kind** — `kind` | `type` | `elementType` (the node kind: `table`, `viz`,
  `control`, …)
- **source** — the `source` object (may be absent for child/derived elements)
- **columns** — `columns[]`, each with `id` | `columnId`, `name` | `label`,
  `formula` | `expression`

**All formulas of an element** = its `source.statement` (if any) **plus** every
non-empty `columns[].formula`. The window-function, user-scoped, dynamic-time,
and lookup checks all scan this combined text.

---

## Source kind  ✅ sql / warehouse-table CONFIRMED · ⚠️ others INFERRED

Read `source.kind` (or `source.type`), lower-cased. Classify into:

| Class | Tokens (extend as the real spec reveals more) |
|---|---|
| Custom SQL | `sql` |
| Warehouse table | `warehouse-table`, `warehousetable`, `table`, `warehouse` |
| Input Table | `input-table`, `inputtable`, `input` |
| Sigma Table | `sigma-table`, `sigmatable` |
| Join | `join` |
| Union | `union` |
| Semantic view | `semantic-view`, `semanticview` |

- **H-INPUT-TABLE** fires for an Input Table kind **but not** a Sigma Table kind.
- **P-WAREHOUSE** fires for a warehouse-table kind **or** a Sigma Table kind.
- **P-CUSTOMSQL** fires for `sql` with a non-empty `source.statement`.

For custom SQL, `source.statement` is `statement` | `sql` | `query`.

---

## Dependencies and fan-out  ⚠️ INFERRED

Collect each element's **upstream source ids** so you can build the dependency
graph:

- **Single parent** (child element / derived table): `parentId` |
  `fromElementId`, or `source.elementId` | `source.sourceId`.
- **Join / union members:** look under `source.joins` | `source.legs` |
  `source.sources` | `source.members` | `source.inputs`. Each entry may be:
  - a bare string id, or
  - an object with `elementId` | `sourceId` | `id`, or
  - a join leg with `left` / `right`, each an object carrying an id.

De-duplicate, preserving order.

**Fan-out** of an element = the count of distinct elements that transitively
depend on it (walk the "who lists me as a source" relation). **P-FANOUT** fires
at fan-out ≥ 3. Phase-1 fan-out is **in-model only** — cross-workbook fan-out is
a Phase-2 admin-API input; say so if it would change the verdict.

---

## Join source counting  ⚠️ INFERRED

For a `join` source, the number of sources combined =
`max(distinct upstream ids resolved, number of legs + 1)`. **P-JOIN3** fires at
3+. If the spec lists the joined sources directly (e.g. `source.sources: [a,b,c]`),
that count is the most reliable.

---

## Grouping grain  ⚠️ INFERRED

Read `grouping` | `groupings` — a list of levels, each a string or an object with
`name` | `id`, ordered coarsest → finest.

- **Leaf grain** when: an explicit flag (`isLeafLevel` | `allSourceColumns` is
  true) **or** a level named for the base grain — "all source columns", "base",
  "leaf", "row level" (case/underscore-insensitive).
- A **raw, ungrouped table (no levels) is NOT leaf** — grain isn't the relevant
  signal there; it must not trip N-LEAF.

So:
- **P-GROUPING** fires when there are grouping levels and the element is not leaf.
- **P-AGG** fires when the element aggregates and is not leaf. "Aggregates" =
  any column formula containing `Sum(`, `Count(`, `Avg(`, `Min(`, `Max(`,
  `Median(`, `CountDistinct(` (spaces stripped, case-insensitive), or grouping
  levels present at a non-leaf grain.
- **N-LEAF** fires when grouping levels are present and the element is leaf.

---

## Control targeting  ⚠️ INFERRED

A control is an element whose kind contains `control` and which names a target.
Read its targets from `targetId` (single), `targetIds`, `targets`, or
`controlId`. For each resolved target element id, record that this control
targets it → **H-CONTROL** fires on the targeted element.

Controls themselves are **not** materialization candidates — exclude control
elements from scoring once you've recorded their targets.

> Note: a `{{control-name}}` interpolation *inside* a Custom SQL statement is a
> templated control reference, **not** the same as a page control targeting the
> element. The doc's H-CONTROL hard-skip is about page controls. Don't conflate
> the two; a bare `{{name}}` in SQL is most often a control and must not be
> counted as a user attribute either (see the user-scoped pattern below).

---

## Relative-date filter  ⚠️ INFERRED

Read `filters[]`. A relative-date filter fires **H-RELATIVE-DATE** when an entry:
- has `kind` | `type` containing both "relative" and "date", or
- carries a `relativeDate` | `lastNDays` | `rollingWindow` key.

---

## Formula / SQL text patterns

Scan the element's combined formula text (see "All formulas" above), skipping
SQL comments where it matters.

| Signal | Match (case-insensitive) |
|---|---|
| **P-WINDOW** | `OVER (`, `RANK(`, `DENSE_RANK(`, `LAG(`, `LEAD(`, `ROW_NUMBER(`, `NTILE(`, `FIRST_VALUE(`, `LAST_VALUE(`, `PERCENT_RANK(`, `CUME_DIST(` |
| **H-USER-SCOPED** | `CurrentUserAttribute`, `CurrentUserEmail`, `CurrentUserId`, `system::CurrentUser` |
| **H-DYNAMIC-TIME** (Sigma, structural) | `Now()`, `Today()` |
| **H-DYNAMIC-TIME** (SQL, heuristic) | `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `GETDATE(`, `SYSDATE`, `NOW()` |
| **H-SIGMA-ELEMENT** | `sigma_element(` (in a SQL statement) |
| **N-LOOKUP** | `Lookup(` |

**User-attribute vs control:** `{{system::CurrentUserAttributeText::name}}` and
`CurrentUserAttributeText("name")` are user-scoped (→ H-USER-SCOPED). A bare
`{{name}}` with no `system::` / `CurrentUser` prefix is a **control**, not a user
attribute — do not treat it as user-scoped.

---

## Quick checklist per element

1. Source kind → P-CUSTOMSQL / P-WAREHOUSE / H-INPUT-TABLE class?
2. Combined formula text → P-WINDOW, H-USER-SCOPED, H-DYNAMIC-TIME,
   H-SIGMA-ELEMENT, N-LOOKUP?
3. Join? → count sources → P-JOIN3.
4. Grouping → leaf or not → P-GROUPING / P-AGG / N-LEAF.
5. Filters → relative date → H-RELATIVE-DATE.
6. Targeted by a control → H-CONTROL.
7. Fan-out from the dependency graph → P-FANOUT; feeds a downstream live join →
   N-JOIN-LIVE.
8. Semantic-view source → N-SEMANTIC.
