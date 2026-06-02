# sigma-model-sql-rls-audit

A Claude Code skill that audits **Sigma data models** for the strength of
**row-level security (RLS) implemented in Custom SQL** — specifically, how well
each Custom SQL block is scoped by a Sigma **user attribute**.

It reads a data-model spec, finds every Custom SQL surface, checks whether a user
attribute actually restricts rows (and isn't quietly bypassed), and returns a
graded report with the exact offending SQL cited.

> **Scope, up front.** This skill audits **user-attribute scoping inside Custom
> SQL only**. Sigma's *recommended* way to secure a data model is **native RLS** —
> a boolean formula column (`CurrentUserAttributeText("Region") = [Region]`) with a
> filter set to `True` — which lives outside Custom SQL. A model secured that way
> has no Custom SQL and is currently reported as **`N/A — no custom SQL`**, not as
> "secure". Read the grade accordingly: it scores the SQL you wrote, not your whole
> security posture.

---

## What it checks

| Check | Flags when… |
|---|---|
| **UA-PRESENT** | a row-returning Custom SQL block references no user attribute at all |
| **UA-IN-PREDICATE** | the attribute is named but never used in a `WHERE`/`JOIN`/`HAVING` |
| **UA-BYPASS-OR-TRUE** | an always-true disjunct (`OR 1=1`, `OR TRUE`) defeats the filter |
| **UA-BYPASS-NULL** | an `OR <col> IS NULL` lets rows through |
| **UA-BYPASS-FALLBACK** | a permissive fallback (`COALESCE(ua,'admin')`, `'*'`, `'%'`) widens access |
| **UA-BYPASS-COMMENTED** | the only attribute references are inside SQL comments |
| **UA-COVERAGE** | some UNION / CTE / sub-select legs are scoped and others aren't |
| **UA-TYPE** | an attribute is quoted incorrectly for the column it compares against |
| **UA-SYSTEM-MISQUOTE** | a `system::CurrentUserAttribute*` / `#identifier` helper is wrapped in `'...'` |
| **UA-EMPTY** | (informational) no guard for an unset attribute — Sigma fails closed by default |

The only **user-attribute** forms recognized in Custom SQL are the two Sigma
documents:

- `{{system::CurrentUserAttributeText::Store_Region}}` (and the `Number` variant)
- `{{#formula CurrentUserAttributeText("Store Region")}}`

A bare `{{name}}` is a **workbook control**, *not* a user attribute, and is
deliberately ignored.

## Grading

Each Custom SQL block is graded A–F by its worst finding (one critical → F). The
model grade is the floor of its blocks. A model with **no** Custom SQL is `N/A`
(never silently treated as a pass).

## Requirements

- **Local files:** none — point the skill at a data-model JSON export.
- **Live models (by ID / URL):** a Sigma API token. Run the **`sigma-api`** skill
  first, then the spec is fetched via the **`sigma-data-models`** skill.

## How to use it

Just ask Claude Code in natural language. Trigger phrases include:

- "audit the Custom SQL in this data model"
- "check user-attribute usage in my data models"
- "score the user-attribute strength on model `<id>`"

### Inputs

| You give it… | It does… |
|---|---|
| a local `*.json` spec file | audits that one model |
| a folder of `*.json` specs | audits each (default: `./sigma-model-sql-rls-audit/*.json`) |
| a Sigma data-model **ID** or **URL** | fetches the live spec, then audits it |
| `--all-remote` | audits every data model in the org (confirms first if > 20) |

### Flags

- `--fix` — after the report, propose concrete spec edits for High/Critical
  findings and write them back **only after you approve each diff**. Off by default
  (the skill is read-only).
- `--severity <info|low|medium|high|critical>` — minimum severity to report
  (default `low`).
- `--ua <name>` — only count a specific attribute as "present".
- `--verbose` — full markdown report (grade table, grouped findings, remediation)
  instead of the default terse summary.

## Example output (terse)

```
sales_model (abc123) — grade F  (2 blocks, 3 findings)

  Page "Main" → Orders (custom-sql-source)
    CRITICAL  UA-PRESENT          no user-attribute reference in the statement — `SELECT order_id, revenue FROM orders`
    HIGH      UA-BYPASS-OR-TRUE   always-true disjunct defeats the tenant filter — `WHERE tenant_id = {{...}} OR 1=1`

— audited 1 spec, 2 custom SQL blocks, 3 findings
```

## What it does *not* do

- Audit **workbooks** (data models only).
- Review **column-level security** or folder/connection permissions (see
  `sigma-data-models`).
- Detect **native RLS** (formula-column + filter) — tracked as a known gap.
- Confirm a user attribute is correctly *configured* in Sigma — it audits **usage**,
  not **definition**.

## Files

```
sigma-model-sql-rls-audit/
├── README.md        ← this file (for humans)
├── SKILL.md         ← the skill definition Claude loads (workflow + rules)
├── reference/       ← loaded on demand by the skill
│   ├── checks.md        check catalog + severity/grading rubric
│   ├── patterns.md      regex/pattern catalog the checks use
│   └── report-format.md terse + verbose output templates
└── fixtures/        ← regression test specs with documented expected results
```
