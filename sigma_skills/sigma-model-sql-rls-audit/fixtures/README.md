# Regression fixtures

Real (anonymized) data-model spec excerpts used to validate the audit's behavior.
Run the skill against a fixture and confirm the expected outcome below.

## `secured-control-plus-ua.json`

A **correctly-secured** Plugs Electronics model. Its one Custom SQL block contains
two `{{...}}` references that must be treated differently:

- `{{system::CurrentUserAttributeText::Store_Region}}` — a **real user attribute**,
  used unquoted in `WHERE STORE_REGION = …` inside the only row-bearing CTE (`base`).
- `{{product-family-filter}}` — a **workbook control** (the author's comment says so),
  used in `CASE WHEN {{product-family-filter}} = '' THEN TRUE …`.

### Expected outcome (corrected skill)

**Grade A — no real findings.** Specifically:

- **UA-PRESENT:** exactly **one** user attribute (`Store_Region`). The bare
  `{{product-family-filter}}` is a control and must **not** be counted.
- **UA-IN-PREDICATE:** the UA is in the `WHERE` of `base`, the only row-bearing leg;
  `ranked` and the final `SELECT` derive from it, so coverage is complete.
- **UA-SYSTEM-MISQUOTE / UA-TYPE:** no finding — the `Text` form is correctly unquoted.
- **UA-EMPTY:** at most a **Low/info** note (Sigma fails closed); never grade-lowering.

### The regression this guards against

The pre-fix skill treated bare `{{name}}` as a user attribute. On this fixture it would
(a) miscount two UAs and (b) fire a **false-positive HIGH bypass** on
`CASE WHEN {{product-family-filter}} = '' THEN TRUE …` — flagging a correctly-built,
properly-secured model as a security hole. If that finding ever reappears, the
bare-control-vs-UA distinction has regressed.

## `leaky-no-ua.json`

A genuinely **insecure** model with two Custom SQL blocks, one per failure mode:

- **`Orders (unscoped)`** — returns row-level data with **no** user-attribute reference
  at all (just `WHERE PRICE > 0`).
- **`Customers (bypassed)`** — references the real UA but neutralizes it with an
  always-true disjunct: `WHERE STORE_REGION = {{system::CurrentUserAttributeText::Store_Region}} OR 1=1`.

### Expected outcome

**Model grade F** (floor of both blocks):

- `Orders` → **CRITICAL UA-PRESENT** (no UA anywhere) → block grade F.
- `Customers` → **HIGH UA-BYPASS-OR-TRUE** (`OR 1=1` defeats the filter) → block grade D.
  UA-PRESENT/UA-IN-PREDICATE both pass here (the UA *is* in the WHERE), so the only
  finding is the bypass.

This is the positive control for the leak-detection path: if either block stops
flagging, the audit has gone blind to real leaks.

## `native-rls-no-customsql.json`

A model secured the **documented recommended way** — and it has **no Custom SQL at all**:

- Source is a plain warehouse table (`source.kind == "warehouse-table"`).
- Row-level security is a boolean formula column
  `CurrentUserAttributeText("Store Region") = [Store Region]` (`col-rls-allowed`)
  with a filter set to `isTrue`.

### Expected outcome

- **Current behavior:** the skill finds zero Custom SQL surfaces and reports
  **`N/A — no custom SQL`**. That is the *correct* result for today's scope, but it is
  also the **scope gap tracked in `sigma_sandpit-3wm`**: a fully-secured model gets a
  non-answer, and the "are my models secured" trigger phrase over-promises.
- **Desired future behavior (when 3wm lands):** detect the native-RLS pattern
  (formula column referencing `CurrentUserAttributeText(...)` bound to a filter set to
  `True`) and report **"secured via native RLS"** instead of `N/A`.

Use this fixture to confirm the N/A path today, and as the acceptance test for 3wm.

## `exempt-and-lowrisk.json`

Exercises the **disposition** logic — every block in one model, one per disposition:

- **`Orders (scoped)`** — real UA in the `WHERE` → **scored**, grade **A**.
- **`Products (public dimension)`** — annotated `-- @sigma-rls: none — public product
  dimension, no row security required` → **exempt** (`none`).
- **`Region Sales (secured upstream)`** — annotated `-- @sigma-rls: external — …
  V_REGION_SALES … RLS upstream` → **exempt** (`external`).
- **`Region Codes (constants)`** — `SELECT 'NA' … UNION ALL …` with no `FROM` →
  **low-risk** (no row-bearing source), no annotation needed.

### Expected outcome

**Model grade A** — `1 scored, 2 exempt, 1 low-risk`, **0 findings**. The report must:

- grade only the scored `Orders` block,
- list both exempt blocks with their verbatim reasons in the "Exempted" section,
- list the constants block as low-risk,
- **not** fire UA-PRESENT on any of the three non-scored blocks.

### Variations to check

- **`--strict`:** the two `@sigma-rls` blocks are scored anyway → both have no UA →
  two **CRITICAL UA-PRESENT** findings → model drops to **F**. Confirms the override
  surfaces what the annotations suppress.
- **Malformed annotation:** delete the reason (`-- @sigma-rls: none`) on `Products` →
  it is *not* exempted; fires **`low` UA-EXEMPT-MALFORMED** and is scored (CRITICAL
  UA-PRESENT, since it has no UA). Confirms blank opt-outs don't work.
