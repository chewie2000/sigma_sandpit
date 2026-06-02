# Reply Format

The skill has two output modes. Pick based on flags:

- **Default — terse plain text.** Scannable in chat. No markdown structure.
- **`--verbose` — full markdown report.** Tables, headings, inline remediation, appendix.

Both modes cite the verbatim SQL substring in `evidence` — never paraphrase.

---

## Default mode (terse)

A short plain-text reply. No tables, no heading hierarchy, no code fences except inline backticks for SQL evidence.

### Shape

```
<model label> — grade <X>  (<N> custom SQL block(s), <M> finding(s))

  Page "<page name>" → <element name> (<surface>)
    <SEVERITY>  <CHECK-ID>  <one-line message> — `<evidence>`
    <SEVERITY>  <CHECK-ID>  <one-line message> — `<evidence>`

  Page "<page name>" → <element name> (<surface>) — grade A, no findings

<next model …>

<label> — N/A (no custom SQL)

— audited <N> spec(s), <M> custom SQL block(s), <K> finding(s)
```

### Conventions

- **One line per finding.** Two-space indent under the element line, then `SEVERITY  CHECK-ID  message — \`evidence\``.
  - SEVERITY is uppercase (`CRITICAL`, `HIGH`, `MEDIUM`, `LOW`, `INFO`), padded to 8 chars for column alignment.
  - CHECK-ID is the identifier from `checks.md`, padded to 22 chars.
  - Message: one short clause, no terminal period.
  - Evidence in single backticks. Over 120 chars → truncate the middle with `…`, keep `{{ua}}` visible.
- **Sort findings** within each element: critical → low, then by check id.
- **Clean elements:** print `Page "X" → Element (surface) — grade A, no findings`. If a model has many clean ones, collapse to a single `(<N> other blocks clean)` line at the end of that model.
- **Confidence:** append ` [heuristic]` for heuristic findings. Omit for structural.
- **Remediation is NOT printed inline.** Looking up a fix is on-demand — if the user asks "how do I fix UA-BYPASS-OR-TRUE", consult `checks.md` then.
- **No bold, no markdown headings, no markdown tables, no fenced code blocks.** Inline backticks only.

### Example

```
sales_model (abc123) — grade F  (2 blocks, 3 findings)

  Page "Main" → Orders (custom-sql-source)
    CRITICAL  UA-PRESENT              no user-attribute reference in the statement — `SELECT order_id, customer_id, revenue FROM orders`
    HIGH      UA-BYPASS-OR-TRUE       always-true disjunct defeats the tenant filter — `WHERE tenant_id = {{tenant_id}} OR 1=1`

  Page "Main" → Customers (custom-sql-source)
    MEDIUM    UA-EMPTY                no empty-attribute guard on {{tenant_id}} — `WHERE tenant_id = {{tenant_id}}` [heuristic]

reporting_model (def456) — N/A (no custom SQL)

— audited 2 specs, 2 custom SQL blocks, 3 findings
```

### --fix in terse mode

Append a single "Proposed fixes" block after the footer. One fix per structural finding (skip heuristic findings). Diff lines use literal `- ` / `+ ` prefixes, indented two spaces, no fenced blocks.

```
Proposed fixes (read-only by default — nothing has been written back):

  sales_model (abc123) → Page "Main" → Orders → source.statement
    - SELECT order_id, customer_id, revenue FROM orders
    + SELECT order_id, customer_id, revenue FROM orders WHERE tenant_id = {{tenant_id}} AND {{tenant_id}} <> ''

Reply `approve abc123` to write back via sigma-data-models, `skip abc123`, or `edit abc123` to revise.
```

---

## Verbose mode (`--verbose`)

A full markdown report. Use this when the user wants something printable, reviewable, or pasteable into a doc / ticket.

### Top-level structure

```
# Custom SQL User-Attribute Audit

<one-line scan summary: how many models, how many custom SQL blocks, how many findings>

## Model grades

| Model | Grade | Custom SQL blocks | Findings (C/H/M/L) |
|---|---|---|---|
| <model 1 label> | **F** | 3 | 2 / 1 / 0 / 0 |
| <model 2 label> | **B** | 1 | 0 / 0 / 0 / 1 |
| <model 3 label> | **N/A** | 0 | — |

## Findings

<grouped sections, one ### per model with findings>

## Unscored / N/A

- <model name> — no Custom SQL blocks in this spec.

## Next steps

<advice, or `--fix` proposal block>
```

### Per-finding section structure (verbose)

For every model with findings, one `### <model label>` heading, then one `#### Page: <page name> — Element: <element name> — Surface: <surface>` sub-heading per element, then the findings for that element. Sort by severity (critical first), then by check id.

```
### My Sales Model (dataModelId=abc123, grade: **F**)

#### Page: Main — Element: Orders (table-orders) — Surface: custom-sql-source

- **[CRITICAL] UA-PRESENT** — confidence: structural
  - **Message:** No user-attribute interpolation found anywhere in this Custom SQL statement.
  - **Evidence:** `SELECT order_id, customer_id, revenue FROM orders` (full statement; no `{{...}}` present)
  - **Remediation:** Add a WHERE predicate that constrains rows by the relevant user attribute, e.g. `WHERE tenant_id = {{tenant_id}}`.

- **[HIGH] UA-BYPASS-OR-TRUE** — confidence: structural
  - **Message:** An always-true disjunct (`OR 1=1`) appears in the same boolean expression as the user-attribute filter, defeating it.
  - **Evidence:** `WHERE tenant_id = {{tenant_id}} OR 1=1`
  - **Remediation:** Remove the always-true disjunct, or restructure with `AND` so the user-attribute filter cannot be short-circuited.
```

### Headline grade rules (verbose)

- Use **bold** on the letter grade in the model-grades table and in the section heading.
- For `N/A`, do not bold — write `N/A` plainly and explain in the Unscored appendix.

### Evidence formatting (verbose)

- Cite the **exact substring** from the statement. Don't paraphrase.
- Wrap short matches (< 80 chars) in single backticks. For longer matches use a fenced ```sql block.
- If the evidence is a complete statement (UA-PRESENT firing for a leak with no filter), show the whole statement followed by `(full statement; no {{...}} present)`.

### --fix in verbose mode

Append at the very end of the report, after Next steps:

````
## Proposed fixes (--fix)

> Read-only audit by default. With `--fix`, the skill proposes spec edits below.
> Nothing has been written back. Approve per model before any PUT.

### My Sales Model (dataModelId=abc123)

**Block:** Page "Main" → Element "Orders" → source.statement

Diff:
```diff
-SELECT order_id, customer_id, revenue FROM orders
+SELECT order_id, customer_id, revenue FROM orders WHERE tenant_id = {{tenant_id}} AND {{tenant_id}} <> ''
```

Reasoning: Adds a tenant-scoped WHERE predicate with an empty-attribute guard so users without the attribute set get zero rows (fail-closed).

Reply `approve abc123` to write back via the `sigma-data-models` UPDATE workflow, `skip abc123` to ignore, or `edit abc123` to revise before applying.
````

### Verbose `--fix` rules

- Only auto-propose patches for findings with `confidence: structural`. For `heuristic` findings, leave a written remediation in the Findings section but do **not** include a diff block.
- One diff per finding; never bundle multiple unrelated fixes into a single diff.
- Show the unified diff with the exact original `statement` on the `-` line and the proposed `statement` on the `+` line — no whitespace reflow, no auto-formatting.
- Write-back uses the `sigma-data-models` UPDATE workflow (GET → modify → PUT full spec). Never invent the PUT shape here.

---

## Both modes — common rules

- **Evidence is verbatim.** Never paraphrase a SQL substring.
- **Confidence labels.** Heuristic findings carry `[heuristic]` (terse) or `confidence: heuristic` (verbose). Structural findings carry no label (terse) or `confidence: structural` (verbose).
- **`--fix` is opt-in.** Without the flag, never print a diff or a write-back prompt in either mode.
- **Stdout only.** The skill does not write reports to disk. If the user wants to save the output they can redirect.
