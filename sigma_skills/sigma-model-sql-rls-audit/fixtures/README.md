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
