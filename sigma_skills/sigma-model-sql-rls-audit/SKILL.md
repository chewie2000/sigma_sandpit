---
name: sigma-model-sql-rls-audit
description: >-
  Audit Sigma data models for the security strength of user-attribute scoping
  inside Custom SQL sections. For each custom SQL source (and any other raw-SQL
  surface in the spec), checks whether a user attribute is referenced, whether
  it is actually enforced as a row-restricting predicate, and looks for common
  bypass patterns (OR 1=1, IS NULL fall-throughs, hardcoded fallbacks,
  commented-out filters, empty-attribute leakage). Produces a markdown report
  with a per-block finding list and a model-level grade. Defaults to read-only;
  pass --fix to propose spec edits that tighten weak filters. Accepts either
  local data-model JSON files (e.g. ./sigma-model-sql-rls-audit/*.json) or models fetched
  live from Sigma by ID/URL via the `sigma-data-models` skill. Trigger phrases:
  "validate my data model", "audit custom SQL in this model", "check user
  attribute usage in data models", "are my models secured", "score the user
  attribute strength on model X".
---

# Sigma Model SQL RLS Audit — Custom-SQL User-Attribute Audit

Audit one or more Sigma data model specs for the security strength of user-attribute usage inside Custom SQL. This skill is **read-only by default** — it never modifies a Sigma model unless invoked with `--fix` and the user explicitly approves the proposed edits.

## When to use

Invoke when the user wants to:

- Score / grade how well Custom SQL in a Sigma data model is scoped by user attributes.
- Find Custom SQL sources that have **no** user-attribute filtering at all.
- Find Custom SQL that *looks* scoped but is bypassable (`OR 1=1`, `IS NULL`, hardcoded fallbacks, commented filters, empty-attribute leakage).
- Audit a single model file you have locally, a folder full of model JSON exports, or a live Sigma data model by ID / URL.
- Produce a written report that names the offending statement and the page / element / line where it lives, with severity + remediation hint.

**Do NOT use** for: workbook-level audits (this is data-model only — workbooks are out of scope), column-level security review (use `sigma-data-models reference/column-level-security.md`), or general data-model authoring (use `sigma-data-models` directly).

## Inputs

The skill accepts any of these — pick the one that matches what the user gave you:

| Input form | Example | Behavior |
|---|---|---|
| **Local file** | `./sigma-model-sql-rls-audit/my-model.json` | Audit that one spec. |
| **Local folder** | `./sigma-model-sql-rls-audit/` (default if no input given) | Audit every `*.json` in the folder. Skip files that don't parse as a Sigma data-model spec. |
| **Sigma model ID** | `dataModelId=abc123` or a bare 22-char id | Fetch the live spec via the `sigma-data-models` skill, then audit it. |
| **Sigma model URL** | `https://app.sigmacomputing.com/<org>/data-model/<id>` | Extract the id from the URL and treat as above. |
| **`--all-remote`** | flag | List every data model in the org via `GET /v2/dataModels` and audit each. Confirm before running on large orgs (>20 models). |

**Default when no input is given:** scan `./sigma-model-sql-rls-audit/*.json` in the current working directory. If the folder is empty, ask the user what they want to point at.

## Flags

- `--fix` — Read-only audit by default. With `--fix`, after presenting the report, propose concrete spec edits for High/Critical findings (e.g. adding a `WHERE` predicate that references the user attribute, or quoting the interpolation). **Always show the diff and ask for confirmation before writing back via the `sigma-data-models` PUT workflow.** Never apply fixes silently.
- `--severity <min>` — Only include findings at or above this severity in the report (`info`, `low`, `medium`, `high`, `critical`). Default: `low`.
- `--ua <name>` — If the org has a known canonical attribute (e.g. `region`, `tenant_id`), restrict "presence" checks to expect *that* attribute. Without this flag, any `{{...}}` user-attribute interpolation counts as present.
- `--verbose` — Emit the full markdown report (model-grade table, grouped findings with remediation inline, unscored appendix, next-steps stanza). Default is the terse plain-text reply. See `reference/report-format.md`.

## Auth (only when fetching from Sigma)

For local-file mode no auth is needed. For remote mode (ID / URL / `--all-remote`):

1. Run the `sigma-api` skill first to set `$SIGMA_BASE_URL` and `$SIGMA_API_TOKEN`.
2. Delegate spec retrieval to the `sigma-data-models` skill — specifically its **GET** workflow (`GET /v2/dataModels/<id>/spec`). Do not reimplement that here.

## Workflow

### 1. Resolve inputs to a list of `{label, spec}` pairs

- **Local file:** `label = <basename>`, `spec = json.load(file)`.
- **Local folder:** glob `*.json`, filter to objects with a `pages` array, build one pair per file.
- **Remote single model:** delegate to `sigma-data-models` GET workflow → `label = "<modelName> (<modelId>)"`, `spec = the fetched JSON`.
- **`--all-remote`:** `GET /v2/dataModels` to list, then per-id GET as above. Warn before iterating if `entries.length > 20`.

### 2. Walk every spec for Custom SQL surfaces

For each spec, traverse `pages[].elements[]` and collect every Custom SQL surface. Today the relevant surfaces are:

| Surface | JSON path | What to extract |
|---|---|---|
| Custom SQL source | `pages[i].elements[j].source` where `source.kind == "sql"` | `source.statement`, plus `pages[i].id` / `name`, `elements[j].id` / `name` for location reporting. |
| Custom SQL inside a join leg | `pages[i].elements[j].source.kind == "join"` and a `joins[].left/right.kind == "sql"` (if Sigma exposes that — treat defensively) | The nested `statement`. |
| Calculated columns referencing raw SQL via `SqlText(...)` or `Passthrough(...)` formula | `pages[i].elements[j].columns[k].formula` containing `SqlText(` or `Passthrough(` | The string argument. Audit it the same way as a statement, but mark `surface = "calculated-column-sql"` with severity bumped up one notch — calculated columns evaluated server-side without a WHERE are essentially `SELECT` payloads. |

If a spec has zero Custom SQL surfaces, mark the spec as **N/A — no custom SQL** and move on. Do not score it.

### 3. Run the audit checks against each extracted SQL block

Load `reference/checks.md` and `reference/patterns.md` and apply every check. Each check produces zero or more findings of the form:

```
{
  "modelLabel": "...",
  "page": "Main",
  "element": "Orders (table-orders)",
  "surface": "custom-sql-source",
  "checkId": "UA-PRESENT",
  "severity": "critical|high|medium|low|info",
  "message": "human-readable summary",
  "evidence": "exact substring of the statement",
  "remediation": "what to change",
  "fixHint": "(only present when --fix; a proposed replacement snippet)"
}
```

### 4. Score each SQL block, then aggregate per-model

Use the grading rubric in `reference/checks.md` (§ Severity & grading). Per-block grade is the **floor** of any contributing finding (one critical → block grade = F). Model grade is the **floor** of all blocks in the model. A model with no Custom SQL gets `N/A`.

### 5. Render the reply

Use the template in `reference/report-format.md`. Two output modes:

- **Default (terse):** a short plain-text reply. One headline line per model (`<label> — grade <X>` + counts), findings indented beneath, one line per finding (`SEVERITY  CHECK-ID  short message — evidence`), one-line footer. No tables, no headings, no fenced blocks.
- **`--verbose`:** the full markdown report — model-grade table, grouped findings with remediation inline, unscored appendix, next-steps stanza.

If `--fix` is set, append a "Proposed fixes" block in whichever mode is active (small unified diffs in terse mode, full diff sections in verbose mode), one fix per structural finding, with an approval prompt.

### 6. (Optional, `--fix` only) Propose and apply patches

For each High / Critical finding, draft the smallest spec patch that resolves it. **Do not write back blindly.** Show the user the unified diff, get a yes/no per model, then hand off to the `sigma-data-models` UPDATE workflow (full PUT — partial updates are not supported, per `sigma-data-models` § Unsupported Features). Preserve all server-assigned IDs from the GET; that workflow's ID semantics table is authoritative.

## Reference index

Load each on demand — don't read everything up-front.

| File | When to load |
|------|--------------|
| `reference/checks.md` | The full check catalog (UA-PRESENT, UA-IN-PREDICATE, UA-BYPASS-*, UA-COVERAGE, UA-TYPE, UA-EMPTY) and the severity / grading rubric. Load before scoring. |
| `reference/patterns.md` | Regex catalog for detecting user-attribute interpolations (`{{...}}`, `system::CurrentUserAttribute*`), WHERE / JOIN predicate locations, and bypass patterns. Load when implementing the checks. |
| `reference/report-format.md` | The markdown report template (headline grade, findings table, appendix, --fix diff block). Load when emitting the report. |

## Out of scope

- Sigma workbook specs (this skill is data-models only).
- Permission checks at the connection / folder / dataset level — those are managed in Sigma's admin UI and not visible in the spec.
- Static analysis of SQL semantics beyond the documented checks (e.g. don't try to fully parse arbitrary SQL — use the regex / heuristic catalog).
- Detecting whether the user attribute *itself* is correctly configured in Sigma (e.g. assigned to the right teams). The skill audits **usage**, not **definition**.

## Behavioral rules

- Be explicit when a finding is heuristic. Some bypass patterns are false-positive-prone (e.g. `IS NULL` is sometimes legitimate). Mark `confidence: "heuristic"` on findings that aren't structural certainties.
- Never claim a model is "safe" if it simply has no Custom SQL — that's `N/A`, not a pass. Say so.
- An unset user attribute does **not** silently widen access. Sigma substitutes the attribute's default value if one is set, otherwise raises a hard `Invalid SQL Parameter` error (fail-closed). Treat a missing empty-guard as `UA-EMPTY` **Low / informational** — not a grade-lowering finding — and escalate only when paired with a permissive fallback (already covered by UA-BYPASS-FALLBACK). The exact token substituted for an *assigned-but-empty* attribute is undocumented; say so rather than asserting it.
- Cite the exact substring of the statement as `evidence` in every finding. Don't paraphrase the SQL.
