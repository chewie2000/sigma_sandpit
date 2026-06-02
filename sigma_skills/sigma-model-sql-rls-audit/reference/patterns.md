# Pattern Catalog

Regex and substring patterns the checks in `checks.md` lean on. Apply them against the **statement string** extracted from each Custom SQL surface. All regexes are case-insensitive unless noted. None of these need to perfectly parse SQL — they're heuristics tuned for the checks above, and findings should carry the `confidence` label specified per pattern.

## User-attribute interpolation forms

Sigma references a user attribute inside raw Custom SQL through one of the two **documented** forms below (verified against Sigma docs). Match either as "a user-attribute reference":

| Form | Regex (Perl/PCRE, case-insensitive) | Notes |
|---|---|---|
| System shorthand | `\{\{\s*system::CurrentUserAttribute(?:Text\|Number)?::\s*"?([A-Za-z_][A-Za-z0-9_ ]*?)"?\s*\}\}` | The documented form, e.g. `{{system::CurrentUserAttributeText::Store_Region}}`. Attribute names containing spaces are double-quoted: `{{system::CurrentUserAttributeText::"Store Region"}}` — the optional `"` in the regex captures both. Captures the attribute name in group 1. The `Number` shorthand spelling is **not publicly documented** (the function is confirmed to exist) — match it, but treat any `Number`-specific reasoning as unverified. |
| `#formula` wrapper | `\{\{\s*#formula\s+CurrentUserAttribute(?:Text\|Number)?\s*\(\s*"([^"]+)"\s*\)\s*\}\}` | Sigma's documented way to call a system function inside raw Custom SQL, e.g. `{{#formula CurrentUserAttributeText("Store Region")}}`. Captures the attribute name in group 1. |

When `--ua <name>` is set, only attributes whose captured name matches `<name>` count toward UA-PRESENT.

### NOT user attributes — never count these

These look similar but are **not** user-attribute references. Counting them is a correctness bug (inflated UA-PRESENT, false bypass findings on correctly-built models):

- **Bare `{{name}}` / `{{ name }}`** — this is the **workbook control value** syntax (e.g. `{{product-family-filter}}`), *not* a user attribute. In particular a `CASE WHEN {{control}} = '' THEN TRUE ELSE col = {{control}} END` block is an *optional control*, not a bypassable UA filter — do **not** flag it as UA-BYPASS / UA-EMPTY.
- **Dotted `{{user.region}}` / `{{ua.region}}`** — no such syntax exists in Sigma; never match.
- **Bare `CurrentUserAttributeText('region')`** with no `{{#formula …}}` wrapper — not valid in raw Custom SQL on its own. Only the `#formula`-wrapped form above counts.
- **`{{#identifier [control]}}`** — emits an unquoted identifier (schema/table/column) driven by a control; not a row-restricting UA reference. (It is still handled by the UA-TYPE carve-outs and UA-SYSTEM-MISQUOTE below for quoting concerns.)

## Exemption annotation (UA-EXEMPT)

A block opts out of RLS scoring with an explicit annotation in a SQL comment (line or block). Match against the **original** statement (annotations live in comments, so don't use the comment-stripped copy):

| Form | Regex (case-insensitive) | Captures |
|---|---|---|
| Line comment | `--\s*@sigma-rls:\s*(none\|external)\b[ \t]*[—:-]?[ \t]*(.*?)\s*$` (multiline) | group 1 = disposition, group 2 = reason |
| Block comment | `/\*\s*@sigma-rls:\s*(none\|external)\b[ \t]*[—:-]?[ \t]*(.*?)\s*\*/` | group 1 = disposition, group 2 = reason |

- **Disposition tokens:** `none` (data is not row-sensitive) or `external` (row security enforced upstream — secured view, native RLS on the consuming model, etc.). Any other token after `@sigma-rls:` → do not exempt; treat as malformed.
- **Reason required.** If group 2 (trimmed) is empty, the block is **not** exempt → fire `low` **UA-EXEMPT-MALFORMED** and score normally. The `—`/`:`/`-` separator is optional but a non-empty reason after it is not.
- A statement with a valid annotation gets disposition `exempt` (unless `--strict`, which ignores annotations).

## Low-risk heuristics (disposition = low-risk)

`heuristic` confidence. A block reads no row-bearing source when **any** of these hold (evaluate against the comment-stripped statement):

- **No `FROM`/`JOIN`:** no `\bFROM\b` and no `\bJOIN\b` anywhere → e.g. `SELECT 1 AS flag`, `SELECT CURRENT_DATE()`.
- **Constants-only SELECT:** the top-level `SELECT` list is literals/expressions over no columns and there is no `FROM`.
- **`VALUES` source:** the statement's source is a `\bVALUES\s*\(` table constructor, not a warehouse table.

These get disposition `low-risk` (not scored, listed as such). **Do not** treat aggregate-only-over-a-real-table as low-risk here — that stays a `scored` block and is handled by UA-PRESENT's aggregate downgrade (Low), because aggregates over a sensitive table can still leak.

## Stripping comments

Before any predicate-position check (UA-IN-PREDICATE, UA-BYPASS-COMMENTED), produce a `stripped` copy of the statement that removes:

- Line comments: anything from `--` to end-of-line. Regex: `--[^\n]*`.
- Block comments: `/\* ... \*/` including newlines. Regex: `/\*[\s\S]*?\*/`.

Keep the original around — UA-BYPASS-COMMENTED needs to know which `{{ua}}` references were in the *comments*. The set difference (`refs in original` – `refs in stripped`) is the commented-out set.

## Locating predicate regions

A `{{ua}}` reference is "in a predicate" if its character offset (in the `stripped` statement) falls inside one of these regions:

- **WHERE region:** from the keyword `WHERE` (whole-word, case-insensitive) up to the next of: `GROUP BY`, `ORDER BY`, `HAVING`, `LIMIT`, `UNION`, `INTERSECT`, `EXCEPT`, end-of-string, or a closing `)` that balances the WHERE's opening context.
- **JOIN-ON region:** from a `JOIN ... ON` clause's `ON` keyword to the next of `WHERE`, the next `JOIN`, `GROUP BY`, `LIMIT`, etc.
- **HAVING region:** from `HAVING` to the next `ORDER BY` / `LIMIT` / end.

For sub-selects / CTEs, recursively re-apply the same logic to each balanced-paren sub-block. Don't try to write a perfect SQL parser — track parenthesis depth and re-scan within each depth.

When in doubt mark the finding **heuristic** rather than **structural**.

## Always-true disjunct patterns (UA-BYPASS-OR-TRUE)

Look for any of these within an `OR`-joined predicate that also contains a `{{ua}}` reference. All `structural` confidence:

| Pattern | Regex |
|---|---|
| `OR 1=1` | `\bOR\s+1\s*=\s*1\b` |
| `OR TRUE` | `\bOR\s+TRUE\b` |
| `OR 'x'='x'` (any equal literal-vs-itself) | `\bOR\s+(['"])([^'"]+)\1\s*=\s*(['"])\2\3\b` (back-reference the literal) |
| `OR <ident>=<same-ident>` | `\bOR\s+([A-Za-z_][A-Za-z0-9_.]*)\s*=\s*\1\b` |
| `OR 1>0` etc. | `\bOR\s+\d+\s*[<>!]?=?\s*\d+\b` then evaluate the literal — flag only when truly always-true |

## Null-passthrough patterns (UA-BYPASS-NULL)

`heuristic` confidence — sometimes intentional.

- `\bOR\s+([A-Za-z_][A-Za-z0-9_.]*)\s+IS\s+NULL\b` *in the same predicate region as a `{{ua}}` reference*.
- `\bOR\s+\{\{[^}]+\}\}\s+IS\s+NULL\b` (the attribute itself being optional).

## Hardcoded-fallback patterns (UA-BYPASS-FALLBACK)

`structural` confidence when the fallback literal is one of `'admin'`, `'all'`, `'*'`, `'%'`, `''`, `NULL`. `heuristic` confidence otherwise (any other literal might still be a legitimate default).

| Pattern | Regex |
|---|---|
| `COALESCE({{ua}}, <fallback>)` | `\bCOALESCE\s*\(\s*\{\{[^}]+\}\}\s*,\s*('[^']*'\|NULL\|\d+)\s*\)` |
| `IFNULL({{ua}}, <fallback>)` / `NVL` | `\b(IFNULL\|NVL)\s*\(\s*\{\{[^}]+\}\}\s*,\s*('[^']*'\|NULL\|\d+)\s*\)` |
| `NULLIF({{ua}}, '')` followed by no further guard | `\bNULLIF\s*\(\s*\{\{[^}]+\}\}\s*,\s*''\s*\)` — `NULLIF` + nothing else converts empty → NULL, which often pairs with `IS NULL`-style passthroughs |
| `CASE WHEN {{ua}} IS NULL THEN <permissive> ELSE {{ua}} END` | match the literal substring `CASE WHEN` followed by `{{` ... then look for the THEN-clause literal |
| Wildcard concat | `'%'\s*\|\|\s*\{\{[^}]+\}\}\s*\|\|\s*'%'` (Postgres/Snowflake style) or `CONCAT\('%'\s*,\s*\{\{[^}]+\}\}\s*,\s*'%'\)` |

## Commented-out-only references (UA-BYPASS-COMMENTED)

Compute: `commented_refs = refs_in_original − refs_in_stripped`. If `len(refs_in_stripped) == 0` AND `len(commented_refs) > 0`, fire UA-BYPASS-COMMENTED as `structural` Critical.

## Coverage scan (UA-COVERAGE)

Identify row-bearing legs and check each independently. Use these markers:

- **UNION legs:** split the statement on top-level `\bUNION(?:\s+ALL)?\b` (top-level = not inside parens). Each split is a leg.
- **CTEs:** a leading `\bWITH\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(([\s\S]*?)\)` block. Recurse into the body of each CTE.
- **Sub-selects in `FROM`:** `\bFROM\s*\(\s*SELECT\b` — start of an inline sub-select. Track paren depth to find its end.

For each leg, re-apply UA-IN-PREDICATE. If any row-bearing leg has zero `{{ua}}` references in a predicate region while at least one **other** leg does have them, fire UA-COVERAGE (`heuristic` High) — the asymmetry is the smell.

If **no** leg has them, that's already reported by UA-PRESENT / UA-IN-PREDICATE; don't double-report UA-COVERAGE.

## Type-mismatch patterns (UA-TYPE)

`heuristic` confidence — we can't read warehouse column types from the spec alone, so word findings as "likely".

- **Unquoted text comparison:** `{{ua}}` immediately following one of `=`, `<>`, `!=`, `IN\s*\(`, `LIKE`, `ILIKE` with **no** quote between operator and `{{`. Regex: `(=|<>|!=|\bIN\s*\(|\bLIKE\b|\bILIKE\b)\s*\{\{[^}]+\}\}`. Flag if the column being compared (the token on the LHS of the operator) is not obviously numeric.
- **Quoted numeric comparison:** `WHERE\s+([A-Za-z_][A-Za-z0-9_]*_id|[A-Za-z_][A-Za-z0-9_]*Id|count|qty|amount|price|total|num\w*)\s*=\s*'\{\{[^}]+\}\}'` — column name suggests numeric, value is single-quoted.

### UA-TYPE carve-outs (skip these before flagging)

Before firing UA-TYPE, exclude any `{{...}}` whose body matches one of the patterns below — Sigma manages the substitution syntax (auto-quoting, numeric literal, or unquoted identifier) for each. The bare, unquoted form is the *correct* usage; flagging it is a false positive.

| Pattern | Regex | Why it's safe unquoted |
|---|---|---|
| `Text` variant | `\{\{\s*system::CurrentUserAttributeText::[^}]+\}\}` | Sigma's documented examples use this form unquoted in value positions. |
| `Number` variant | `\{\{\s*system::CurrentUserAttributeNumber::[^}]+\}\}` | Used unquoted against numeric columns (exact emission semantics not publicly documented — carve out to avoid false positives). |
| `#identifier` directive | `\{\{\s*#identifier\s+\[[^\]]+\]\s*\}\}` | The directive emits an unquoted identifier (column or table name) by design. |

Wrapping any of these in `'...'` is a separate, structural error — see **System-substitution misquoting** below.

## System-substitution misquoting (UA-SYSTEM-MISQUOTE)

Sigma's documented Custom SQL examples use `system::CurrentUserAttribute*` helpers and the `#identifier` directive **unquoted**, so manual `'...'` wrapping is likely incorrect. `heuristic` confidence — the precise runtime mechanism is not publicly documented, so flag as "likely incorrect quoting," do not assert the failure mode, and only auto-`--fix` the `Text` case.

| Pattern | Regex | Why it's likely wrong |
|---|---|---|
| Quoted `Text` variant | `'\s*\{\{\s*system::CurrentUserAttributeText::[^}]+\}\}\s*'` | Documented examples use the `Text` form unquoted in value positions; manual quotes depart from that. (Best-grounded.) |
| Quoted `Number` variant | `'\s*\{\{\s*system::CurrentUserAttributeNumber::[^}]+\}\}\s*'` | A numeric attribute quoted as a string — likely a type mismatch. (Emission semantics undocumented — heuristic.) |
| Quoted `#identifier` directive | `'\s*\{\{\s*#identifier\s+\[[^\]]+\]\s*\}\}\s*'` | `#identifier` is for identifier positions, not quoted string literals. (Quote-stripping behavior not publicly documented — heuristic.) |

Match the **full quoted-token region** (including both surrounding `'`), not just the inner `{{…}}`, so the `evidence` string makes the mistake obvious to the reader.

## Empty-attribute guard (UA-EMPTY)

Severity is **Low / informational** and **heuristic** (see `checks.md`): Sigma fails closed on an unresolved attribute (default value if set, otherwise a hard `Invalid SQL Parameter` error), so a missing guard is not a leak by itself. Detection mechanics below still apply for surfacing the note.

Fire UA-EMPTY when both are true:

1. The statement has at least one `{{ua}}` reference in a predicate.
2. The same predicate region does **not** contain a guard for the same attribute. Look for any of: `\{\{[^}]+\}\}\s*<>\s*''`, `\{\{[^}]+\}\}\s*!=\s*''`, `\{\{[^}]+\}\}\s*IS\s+NOT\s+NULL`, `LEN\(\s*\{\{[^}]+\}\}\s*\)\s*>\s*0`, `LENGTH\(\s*\{\{[^}]+\}\}\s*\)\s*>\s*0`.

Only fire when the **same attribute name** is referenced in the predicate but never guarded. If multiple `{{ua}}` names exist, evaluate each independently.

## Match-region API (what to return from a pattern match)

When a check needs to cite evidence, return:

```
{
  "match": "<verbatim substring>",
  "offsetStart": <int>,
  "offsetEnd":   <int>,
  "predicateRegion": "where" | "join-on" | "having" | "select" | "from" | "comment" | "other"
}
```

The report's `evidence` field is the `match` string. The grader uses `predicateRegion` to decide which check fired.

## False-positive guardrails

- Don't fire UA-BYPASS-OR-TRUE for tautologies inside a comment.
- Don't fire UA-COVERAGE on a leg whose top-level is `SELECT <constant>` or `VALUES (...)` — it doesn't read from a row-bearing source.
- Don't fire UA-TYPE if the column on the LHS is itself a `{{ua}}` interpolation (`WHERE {{role}} = '{{tenant_id}}'`) — that's a different anti-pattern, not a type issue.
- Don't fire UA-EMPTY if the predicate region uses `IN ({{ua}})` and `{{ua}}` is known (from `--ua` flag context or the user's prior message) to be a list-type attribute — Sigma renders an empty list attribute as `NULL`, which makes `IN (NULL)` match nothing (fail closed).
