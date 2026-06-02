# Audit Checks & Severity Rubric

Apply every check below to **each** extracted Custom SQL block (`source.statement`, calculated-column SQL argument, etc.). Findings accumulate, then the rubric at the bottom maps them to a per-block grade and a model grade.

`evidence` in every finding must be a verbatim substring of the SQL. Don't paraphrase.

## The check catalog

### UA-PRESENT — Is any user attribute referenced at all?

- **What it checks:** Does the statement contain at least one user-attribute interpolation? See `patterns.md` § "User-attribute interpolation forms" for the regex set — it covers the two documented forms only: `{{system::CurrentUserAttributeText::name}}` (and the `Number` variant), and the `{{#formula CurrentUserAttributeText("name")}}` wrapper. **Bare `{{name}}` is a workbook control, not a user attribute — do not count it** (see that section's "NOT user attributes" list).
- **Severity if missing:** **Critical** when the source returns row-level data (the common case). The model is leaking everything in the source to anyone who can run the model.
- **Downgrade to Low** only if the statement is clearly aggregate-only with no row-bearing dimension columns (`SELECT COUNT(*)`, `SELECT SUM(x) FROM ...` with no `GROUP BY` and no joins). Aggregate-only over a sensitive table is still a leak, but a much smaller one.
- **Remediation:** "Add a WHERE predicate that constrains rows by the relevant user attribute, e.g. `WHERE tenant_id = {{tenant_id}}`."

### UA-IN-PREDICATE — Is the user attribute used in a row-restricting predicate?

- **What it checks:** Of all `{{ua}}` matches found by UA-PRESENT, how many appear inside a `WHERE`, `JOIN ... ON`, or `HAVING` clause vs. in a `SELECT` list, alias, comment, or string literal?
- **Severity if every reference is non-predicate:** **High.** The attribute is *named* but does nothing to restrict rows.
- **Severity if at least one reference is predicate but others appear in SELECT-only positions:** **Info** — note for the user, not a finding.
- **How to locate predicates** (use heuristics — don't try to fully parse SQL):
  - Find the substring boundaries of `WHERE`, `JOIN`, `ON`, `HAVING`, `GROUP BY`, `ORDER BY`, `LIMIT` (case-insensitive). The `{{ua}}` is "in a predicate" if its position is between a predicate-introducing keyword and the next non-predicate keyword.
  - Discard matches inside `--` line comments or `/* ... */` block comments — see `patterns.md` § "Stripping comments".
- **Remediation:** "Move the user-attribute reference from the SELECT list into the WHERE clause."

### UA-BYPASS-OR-TRUE — Are there always-true disjuncts that defeat the filter?

- **What it checks:** Within the same boolean expression that contains a `{{ua}}` reference, does an `OR` clause introduce an always-true sub-expression? Watch for: `OR 1=1`, `OR TRUE`, `OR 'a'='a'`, `OR 1 > 0`, `OR <col> = <col>` (same column on both sides).
- **Severity:** **High.** The filter is structurally bypassable for everyone.
- **Confidence:** structural — flag without hedging.
- **Remediation:** "Remove the always-true disjunct, or restructure with `AND` so the user-attribute filter cannot be short-circuited."

### UA-BYPASS-NULL — Does an `OR <thing> IS NULL` neutralize the filter?

- **What it checks:** Within the boolean expression containing `{{ua}}`, an `OR <col> IS NULL` (or `OR {{ua}} IS NULL`) clause that lets rows through when either side is null.
- **Severity:** **Medium**. Sometimes legitimate (e.g. allowing NULL when a tenant_id is genuinely optional), so flag as **heuristic confidence**.
- **Remediation:** "If NULL passthrough is intentional, document it in a `-- security:` comment. Otherwise replace with `AND <col> IS NOT NULL AND <col> = {{ua}}`."

### UA-BYPASS-FALLBACK — Hardcoded fallback that grants broader access if the attribute is empty.

- **What it checks:** Patterns like `COALESCE({{ua}}, 'admin')`, `IFNULL({{ua}}, '*')`, `NULLIF({{ua}}, '')`, ternary-style `CASE WHEN {{ua}} IS NULL THEN 'all' ELSE {{ua}} END`, or string concatenation that introduces a wildcard when the attribute is empty (`'%' || {{ua}} || '%'` when `{{ua}}` could be empty).
- **Severity:** **High** when the fallback value is permissive (`admin`, `all`, `*`, `%`, empty); **Medium** when the fallback is a specific non-permissive value but still a silent override.
- **Remediation:** "Either remove the fallback and let an empty attribute fail closed, or add `AND {{ua}} IS NOT NULL AND {{ua}} <> ''` so the model returns zero rows when the attribute isn't set."

### UA-BYPASS-COMMENTED — The only `{{ua}}` references are inside SQL comments.

- **What it checks:** After stripping `--` line comments and `/* ... */` block comments, do **all** user-attribute references disappear? If yes, the statement's apparent scoping is decorative.
- **Severity:** **Critical**. Treat as if UA-PRESENT failed — the live statement has no user attribute at all.
- **Remediation:** "The WHERE clause referencing `{{ua}}` is commented out. Uncomment it, or remove the dead comment and add an active filter."

### UA-COVERAGE — Does every row-bearing sub-source get the attribute?

- **What it checks:** If the statement contains multiple `FROM <table>` references, sub-selects, CTEs (`WITH foo AS (SELECT ...)`), or `UNION [ALL]` legs, count whether **each row-bearing leg** has the user-attribute filter applied. A model that scopes the top-level `SELECT` but `UNION ALL`s in an unscoped source is leaking via the union.
- **Severity:** **High** if at least one row-bearing leg has no attribute predicate. **Heuristic confidence** — sub-select detection is imperfect, so word the finding tentatively.
- **Remediation:** "Apply the user-attribute predicate to **every** UNION / CTE / sub-select leg, not just the outer query."

### UA-TYPE — Is the interpolation quoted appropriately for its position?

- **What it checks:**
  - A `{{ua}}` interpolation immediately following `=`, `IN (`, `LIKE`, `<`, `>`, etc., that is **not** quoted (`WHERE name = {{ua}}` rather than `WHERE name = '{{ua}}'`), when the attribute is being compared to a string column — Sigma will inject the raw value, and if the value contains an apostrophe the query becomes a SQL syntax error or worse a logic break.
  - Conversely, a numeric column compared to a quoted attribute (`WHERE tenant_id = '{{ua}}'`) can short-circuit type coercion silently on some warehouses.
- **Carve-outs — do NOT fire UA-TYPE on these (Sigma manages the substitution syntax itself):**
  - `col = {{system::CurrentUserAttributeText::<attr>}}` or `{{system::CurrentUserAttributeText::<attr>}} = col`, with no surrounding quotes. Sigma's documented examples use the `Text` variant unquoted in value positions, so the bare form is correct — do not flag it. (Manual `'...'` wrapping is the separate **UA-SYSTEM-MISQUOTE** finding.)
  - `col = {{system::CurrentUserAttributeNumber::<attr>}}` with no surrounding quotes — treat as correct against a numeric column. (The exact emission semantics of the `Number` variant are undocumented; carve it out regardless so we don't false-positive.)
  - `{{#identifier [<control-name>]}}` in any position. The `#identifier` directive emits an unquoted identifier (column or table name) by design.
  - Skip these match positions before running the UA-TYPE detection regex. The regex in `patterns.md` § UA-TYPE carve-outs gives the exact skip-set.
- **Severity:** **Medium**. Heuristic confidence — we can't always tell the column type from regex alone, so word findings as "likely type mismatch" and ask the user to confirm.
- **Remediation:** "For bare text attributes (e.g., `{{tenant_id}}`), wrap the interpolation in single quotes: `'{{ua}}'`. For numeric attributes, leave it unquoted but ensure the attribute is constrained to integer values in Sigma. For `system::CurrentUserAttributeText::` references, leave them unquoted — Sigma adds the quotes itself."

### UA-SYSTEM-MISQUOTE — System-substitution helpers wrapped in incompatible quoting.

- **What it checks:** Manual `'...'` wrapping around a `system::CurrentUserAttribute*` reference or an `#identifier` directive. Sigma's own documented Custom SQL examples use these forms **unquoted** (e.g. `WHERE customer_name = {{system::CurrentUserAttributeText::organization_name}}`), so an author-added quote is at best redundant and likely wrong. Fire one finding per match:
  - `'{{system::CurrentUserAttributeText::<attr>}}'` — documented usage drops the `Text` form straight into a value position, unquoted. Manual quoting departs from that. (Best-grounded of the three.)
  - `'{{system::CurrentUserAttributeNumber::<attr>}}'` — a numeric attribute quoted as a string; likely a type problem against numeric columns.
  - `'{{#identifier [<control>]}}'` — `#identifier` is meant for identifier positions (schema/table/column), not quoted string literals.
- **Severity:** **Medium**, **heuristic** confidence. The Text-variant case is well grounded in Sigma's documented examples; the precise substitution mechanism — whether `Text` auto-quotes, whether `Number` emits a bare literal, whether `#identifier` strips quotes — is **not publicly documented**. Word the finding as "likely incorrect quoting" and do **not** assert a specific runtime failure mode.
- **Remediation:** "Remove the surrounding `'...'` — Sigma's documented examples use these forms unquoted. For the `Number` / `#identifier` cases, confirm against your warehouse before changing."
- **`--fix`:** auto-propose a patch **only** for the `Text` variant. Leave the `Number` and `#identifier` cases as written remediations (heuristic).

### UA-EMPTY — What happens if the user attribute resolves to empty?

- **What it checks:** Whether the statement assumes anything unsafe about an unset attribute. Sigma's documented behavior is **fail-closed**, not fail-open: if the attribute has a **default value** it is substituted; if it cannot be resolved Sigma raises a hard `Invalid SQL Parameter` error rather than silently widening access. A missing `<> ''` guard is therefore **not a leak on its own**.
- **Severity:** **Low** (informational), **heuristic** confidence. Escalate only when the empty case is paired with a permissive fallback (`COALESCE`/`IFNULL`/`CASE … THEN <all>`) — and that is already covered, at higher severity, by UA-BYPASS-FALLBACK. Note explicitly that the exact token Sigma substitutes for an *assigned-but-empty* attribute is undocumented; do not assert it.
- **Remediation:** "Sigma fails closed by default, so usually no action is needed. If your warehouse/config could widen access on an empty value, add an explicit `AND {{ua}} <> ''` guard or set a non-permissive default value on the attribute in Sigma."

### UA-MULTI-ATTR — (Informational) More than one user attribute is referenced.

- **What it checks:** Statement references `{{ua_a}}` and `{{ua_b}}`. Not a finding — just an `info` annotation so the user can confirm both are intentional.
- **Severity:** **Info**.

## Severity & grading

Severity ladder (highest → lowest): `critical > high > medium > low > info`.

### Per-block grade

Map the highest severity finding on that block to a grade. Findings of `info` severity never lower the grade.

| Highest finding | Block grade |
|---|---|
| (no findings, no info either) | **A** — at least one `{{ua}}` reference in a predicate, no bypass patterns, type and empty-guard checks pass |
| only `low` | **B** |
| at least one `medium` | **C** |
| at least one `high` | **D** |
| at least one `critical` | **F** |

### Per-model grade

Floor of the per-block grades. A model is only as strong as its weakest Custom SQL block. A model with no Custom SQL blocks at all gets `N/A` (not graded — say so explicitly in the report; don't pass it).

### When to combine findings

If the same statement produces multiple findings, list **all** of them — don't collapse. The grade is set by the worst one, but the report should show the user every issue so they can fix them together.

### Confidence labels

Every finding carries a `confidence` field:

- `structural` — the pattern is unambiguous (e.g. UA-BYPASS-OR-TRUE on `OR 1=1`). Report without hedging.
- `heuristic` — the pattern is likely but could be a false positive (e.g. UA-COVERAGE on sub-selects). Word the finding as "appears to" / "likely" and ask the user to confirm before treating it as truth or fixing it.

The grade calculation treats heuristic findings the same as structural ones — better to over-flag and let the user dismiss than under-flag and ship a leak. But `--fix` should only auto-propose patches for `structural` findings; `heuristic` findings get a written remediation hint only.
