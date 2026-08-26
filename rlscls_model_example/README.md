# rlscls_model_example — additive row-level security in Sigma

Working reference implementation of **additive** row-level security: a person's
visible scope is the **union** of every team grant and every personal grant they
hold. Built and validated end to end against a real Sigma org
(`marko-eutest-mcp`) and a real warehouse (Postgres on Snowflake).

No user attributes are involved. Team membership is never replicated into the
warehouse — only *grant* data lives there, and Sigma resolves membership at query
time.

> **Headline finding:** the pattern works, but **not** in the form originally
> specified. `CurrentUserInTeam()` does **not** accept a comma-separated list of
> teams, so aggregating team names with `ListAgg` and passing the result in fails
> silently for any entity granted to more than one team — precisely the case
> additive RLS exists to handle. See [The correction](#the-correction).

---

## The correction

The original design aggregated granted team names into one string and passed it
to `CurrentUserInTeam()`:

```
Office Team Names = Lookup(ListAgg([Office Grants/Team Principal], ","), [Office], [Office Grants/Entity Value])
Office RLS        = CurrentUserInTeam([Office Team Names]) OR Contains([Office User Emails], CurrentUserEmail())
```

Measured behaviour, as a user who **is** a member of `RLS Demo Ops EMEA`:

| Office | Aggregated team string | Expected | Actual |
|---|---|---|---|
| Paris | `RLS Demo Ops EMEA` | true | **true** |
| London | `RLS Demo Ops EMEA,RLS Demo Ops APAC` | true | **false** |
| Dubai | *(personal email grant)* | true | **true** |

`CurrentUserInTeam()` compares the whole string against a single team name. One
team matches; two concatenated teams match nothing. It fails **closed**, so it is
not a security hole — but it silently under-grants, and no error is raised.

**The fix is to never aggregate team names at all.** Resolve membership one grant
row at a time, where the principal is always a single team, then aggregate the
resulting boolean:

```
-- on the grants ledger, one row per grant.
-- [Principal ID] is always a SINGLE team name here, which is the whole point.
Grant Applies     = If([Principal Type] = "team",
                       Coalesce(CurrentUserInTeam([Principal ID]), False),
                       [Principal ID] = CurrentUserEmail())
Grant Applies Num = If([Grant Applies] = True, 1, 0)

-- on the fact table, per scoping dimension
Office Key   = "office"
Office RLS   = Coalesce(Lookup(Max([Entity Grants/Grant Applies Num]),
                               [Office Key], [Entity Grants/Entity Type],
                               [Office],     [Entity Grants/Entity Value]), 0) = 1

Combined RLS = [Office RLS] Or [Company RLS] Or [Site RLS]
```

These are the formulas as actually shipped — copy them out of
`model/rls_model.spec.json` rather than retyping.

`Max()` is the additive union: one matching grant out of any number is enough,
and because `Max` is idempotent, duplicate or overlapping grants cannot
double-count. This also removes the need for `Contains()` on a concatenated email
list, eliminating a latent bug where one granted address is a substring of
another (`a@x.com` matching inside `sa@x.com`).

Two further gotchas found the same way:

- **`CurrentUserInTeam()` returns NULL, not False**, when given an empty or null
  argument. `NULL Or False` is `NULL`, which left `Combined RLS` null on all
  8,000 rows rather than false. Wrap it in `Coalesce(..., False)`. Fails closed,
  but breaks any downstream `Not(...)`.
- **`Lookup()` returns null when nothing matches** (an entity with zero grants),
  so the outer `Coalesce(..., 0)` is required.

---

## Compound-key `Lookup()` — the open question, answered

**Yes, `Lookup()` supports a compound key**, as repeated *(local key, target key)*
pairs:

```
Lookup(<value>, <localKey1>, <targetKey1>, <localKey2>, <targetKey2>, …)
```

Both shapes were built side by side in one model and compared on identical data:

- **Approach A** — one child table per dimension, filtered to its `entity_type`,
  then a single-key `Lookup` on `entity_value`.
- **Approach B** — a compound-key `Lookup` straight against the *unfiltered*
  ledger, matching on `entity_type` **and** `entity_value` together. The literal
  side of the `entity_type` key comes from a tiny constant column
  (`Office Key = "office"`).

**Result: A and B agreed on all 8,000 rows**, both on the resolved grant strings
(user-independent) and on the final RLS verdict.

**The shipped model is Approach B alone** — 2 elements and 30 columns, down from
5 and 60. The three `grants_<dim>` child elements and the duplicated `… B`
columns are gone. Rebuild the side-by-side comparison any time with:

```bash
python3 build_model.py --update <id> --compare-shapes --validation-mode
```

---

## What was built

### Warehouse — `sigma` database, schema `mark_o`, Postgres (Snowflake)

| Object | Rows | Purpose |
|---|---|---|
| `rls_shipments` | 8,000 | Fact table to secure |
| `rls_entity_grants` | 9 | The grants ledger |
| `rls_companies` / `rls_sites` / `rls_offices` | 12 / 36 / 12 | Reference dimensions |
| `rls_test_personas` / `rls_test_persona_teams` | 3 / 4 | **Validation harness only** — mirrors Sigma team membership so expected results can be computed offline. Not part of the pattern; never joined into the model. |

The ledger is additive by construction — no priority column, no ordering. The
primary key is the full natural key `(entity_type, entity_value, principal_type,
principal_id)`, so the same entity can be granted to many principals.

```sql
CREATE TABLE rls_entity_grants (
    entity_type    text NOT NULL,   -- 'company' | 'site' | 'office'
    entity_value   text NOT NULL,   -- 'Aurora Foods' | 'COB-2' | 'Paris'
    principal_type text NOT NULL,   -- 'team' | 'user'
    principal_id   text NOT NULL    -- Sigma team NAME, or user email
);
```

Synthetic data is fully deterministic — every attribute derives from
`md5(<salt> || shipment_id)`, so rebuilds are byte-identical with no seed file.
Per-attribute salts keep company / office / date / value mutually decorrelated.

### The grants (9 rows)

| Dimension | Value | Principal |
|---|---|---|
| office | Paris | team `RLS Demo Ops EMEA` |
| office | London | team `RLS Demo Ops EMEA` |
| office | London | team `RLS Demo Ops APAC` ← **same entity, two teams** |
| office | Singapore | team `RLS Demo Ops APAC` |
| office | Hamburg | **user** `…+user1@` |
| company | Aurora Foods | team `RLS Demo Key Accounts` |
| company | Baltic Steel | team `RLS Demo Key Accounts` |
| site | COB-2 | **user** `…+user1@` |
| company | Delta Textiles | **user** `…+user2@` |

`RLS Demo Observers` deliberately holds **zero** grants. Eight of twelve offices,
nine of twelve companies, and 35 of 36 sites are granted to nobody.

### Sigma

IDs from the original validated build — installing this against your own org
(see [Installing this for your own org](#installing-this-for-your-own-org))
produces different IDs, resolved automatically into your own `config.json`
rather than hardcoded like this table:

| Object | ID |
|---|---|
| Data model `RLS Additive Grants Example` | `2afde0e3-edee-4c2b-b48f-46afee2b2be8` |
| Workbook `… — Secured View` (consumer-facing) | `728b1f65-9e20-44d3-b7f1-321747fde3ed` |
| Workbook `… — Validation` (diagnostic) | `c2a3d1a5-0ecc-482c-af00-a923a06974db` |
| Folder `Customer Work / Additive RLS` | `86e607cd-f13e-4bef-a9cb-b0e8a103ae6f` |
| Connection `Postgres (Snowflake)` | `42d71980-349a-4a0d-81e9-1ace9e54c49b` |

Teams created: `RLS Demo Ops EMEA`, `RLS Demo Ops APAC`, `RLS Demo Key Accounts`,
`RLS Demo Observers`. Members created: `…+user1@`, `…+user2@` (both `analyze`).

The final model hides all 14 helper columns on `Shipments` (8 business columns
remain visible) and applies the RLS filter **on the element**, never on a control
— a control is viewer-changeable and would make the security bypassable.

### Access

Both test users hold `view` on both workbooks:

| Grantee | Secured View | Validation | Data model |
|---|---|---|---|
| `…+user1@` | view | view | none |
| `…+user2@` | view | view | none |

Granted via `POST /v2/workbooks/{id}/grants` with
`{"grants":[{"grantee":{"memberId":"…"},"permission":"view"}]}`.

**No grant on the data model is required.** Sigma evaluates the *document
owner's* access to the source when a shared document is opened, so workbook
`view` is sufficient — [data access overview](https://help.sigmacomputing.com/docs/data-permissions-overview).
This does **not** weaken the RLS: `CurrentUserInTeam()` and `CurrentUserEmail()`
still resolve to the **viewer**, so each user sees only their own rows. Owner
credentials govern the warehouse connection, not the identity functions.

Caveat: if a viewer **saves a copy**, they become its owner and their own data
permissions apply — they may then hit permission errors.

There is **no grants API for data models or folders** (`/v2/dataModels/{id}/grants`
and `/v2/files/{id}/grants` both 404). If someone needs the model itself — to
build their own workbooks on it — share it in the UI. Alternatively, put the
folder in a dedicated workspace and use `POST /v2/workspaces/{id}/grants`, which
does exist and cascades.

### The model as shipped

Two elements, 30 columns.

```
Entity Grants  (warehouse-table: mark_o.rls_entity_grants)
  Entity Type · Entity Value · Principal Type · Principal ID
  Team Principal · User Principal          diagnostics (null-producing helpers)
  Grant Applies                            <- membership resolved HERE, per row
  Grant Applies Num                        <- numeric form so Max() can aggregate

Shipments      (warehouse-table: mark_o.rls_shipments)
  Shipment ID · Shipped Date · Company · Site · Office · Mode · Revenue EUR · TEU
  ── everything below is hidden ──
  Office/Company/Site Key                  constant, feeds the compound key
  Office/Company/Site Team Names           diagnostic: who granted this row
  Office/Company/Site User Emails          diagnostic
  Office/Company/Site RLS                  per-dimension verdict
  Combined RLS                             the additive union
  Combined RLS Num                         filter target
  FILTER: Combined RLS Num number-range min 1 max 1
```

The `Team Names` / `User Emails` columns are diagnostics only — nothing depends
on them. They answer "why can this person see this row?". They must **not** be
fed to `CurrentUserInTeam()`; that is the original bug.

The filter targets a numeric mirror column, `Combined RLS Num = If([Combined RLS]
= True, 1, 0)`, using `kind: "number-range"` with `min: 1, max: 1`. Filtering the
boolean `Combined RLS` directly with `kind: "list", values: [true]` is accepted by
the API and **enforced correctly** in generated SQL, but Sigma's filter editor
renders it as **"Invalid filter"** — alarming on a security control, so the
numeric form is used instead.

---

## How the three dimensions relate

Worth being explicit about, because the data and the security model disagree —
and the difference is load-bearing.

### In the data: two different shapes

**`company ⊃ site` is strict containment.** Every one of `COB-2`'s 221 rows also
carries `company = Cobalt Pharma`, and no site maps to more than one company.
Sites are genuinely nested under companies.

**`office` is orthogonal.** Paris alone touches all 12 companies and all 36
sites. It cuts across the company/site tree rather than sitting in it — which is
what you would expect of a freight forwarder: the office is who *handled* the
shipment, not whose cargo it is.

```
company ──┬── site        (containment)
          └── site
office ───────────────    (cuts across every company and site)
```

### In the security model: no hierarchy at all

```
Combined RLS = [Office RLS] Or [Company RLS] Or [Site RLS]
```

Three independent tests joined by a flat `OR`. Nothing in the ledger records
that a site belongs to a company. `entity_type` keeps the dimensions in separate
namespaces, so an office named `Paris` can never collide with a company named
`Paris` — that separation is exactly what the compound key buys.

### Containment still works, but emergently

Because every fact row carries **all three values** and `OR` means any match
wins:

| Grant | Rows seen | Effect |
|---|---|---|
| company `Cobalt Pharma` | 682 | **covers all its sites** (COB-1, COB-2, COB-3) |
| site `COB-2` | 221 | does **not** cover COB-1 or COB-3 |
| office `Paris` | 652 | all 12 companies, all 36 sites |

A company grant cascades downward; a site grant does not cascade upward or
sideways. That is usually the desired behaviour — but note it is a **side effect
of the fact table being denormalised**, not a modelled rule. If `rls_shipments`
carried only `site` and company were resolved by joining to `rls_sites`, a
company grant would match nothing. Any real deployment must either denormalise
the parent keys onto the fact, or resolve the hierarchy inside the ledger.

Also note that `Paris` (652 rows) and `Cobalt Pharma` (682 rows) look comparable
by row count but have very different blast radius: an office grant exposes
*every client's* shipments passing through that office. Office-level grants are
the ones to govern most carefully.

### Limitation: `OR` only ever widens

Every grant added can only increase what someone sees. The ledger **cannot
express an intersection** — there is no way to say *"CompanyA, but only
shipments through Paris"*.

Expressing that needs an `AND`, i.e. a composite grant row carrying two entity
values at once, plus a `Grant Applies` that tests both. Concretely, the ledger
would gain a second, nullable entity pair:

```sql
ALTER TABLE rls_entity_grants
    ADD COLUMN qualifier_type  text,   -- null = unconditional grant
    ADD COLUMN qualifier_value text;
```

with the fact-side test becoming "this grant's primary entity matches **and**
(the qualifier is null **or** it also matches)".

**This is a schema change — cheap now, expensive later.** Confirm before
building further: if every customer rule reads "the union of things you are
entitled to", the current shape fits as-is. If any rule reads "X **but only**
Y", the ledger needs the qualifier columns.

---

## Validation results

Two independent oracles: `sql/04_validate.sql` computes what each persona *should*
see directly from the ledger, and Sigma's own query engine reports what they
*do* see.

| Persona | Teams | Personal grants | Expected | Actual | |
|---|---|---|---|---|---|
| `user1` | EMEA, APAC, Key Accounts | office Hamburg, site COB-2 | 3,659 | **3,659** | PASS |
| `user2` | Observers (zero grants) | company Delta Textiles | 646 | **646** | PASS |
| `nogrants` | none | none | 0 | **0** | PASS |

Out of 8,000 rows: user1 sees 45.7%, user2 8.1%, and **3,913 rows (49%) are
visible to nobody**.

### Acceptance criteria

| # | Criterion | Evidence | |
|---|---|---|---|
| 1 | Union across two disjoint teams | EMEA alone 1,322; Key Accounts alone 1,296; both 2,422 — strictly greater than either | PASS |
| 2 | Team grant + personal grant on one person | user1 teams only 2,984 → with personal grants 3,659 | PASS |
| 3 | Personal grant with no team involvement | user2's team contributes 0; total 646 | PASS |
| 4 | True negatives | `nogrants` sees 0 rows (confirmed live in Sigma, not just SQL); 3,913 rows reachable by no principal | PASS |
| 5 | Overlapping team grants don't double-count | London (670 rows) granted to two teams; a member of both sees exactly 670 | PASS |
| 6 | Filtered result is an obvious subset | 3,659 / 8,000 = 45.7% | PASS |
| — | No fan-out from the ledger | Fact table returns exactly 8,000 rows with all lookup columns attached | PASS |
| — | Approach A ≡ Approach B | 0 disagreements across 8,000 rows (measured before A was removed; reproduce with `--compare-shapes`) | PASS |

**How the per-persona numbers were obtained.** API credentials authenticate as
the operator, so `CurrentUserEmail()` always resolves to them. `validate_personas.py`
briefly mirrors each persona onto the operator account — same teams, personal
grants re-pointed at the operator's email — exports the secured workbook, then
restores the original state in a `finally` block. This exercises the real
mechanism through Sigma's SQL generation; the only substitution is which email
string sits in the ledger, and the email branch is independently confirmed (the
Dubai test above).

The harness **snapshots** the operator's existing demo-team membership, sets it to
exactly the persona's set, and restores the snapshot afterwards — then asserts the
restore matched. Both halves matter, and both were bugs first: blindly removing a
team the operator was already in silently revoked real membership, and leaving an
unrelated membership in place contaminated every measurement (a persona expected
to see 0 rows reported 1,335 — the operator's own scope).

### Live confirmation with a real identity

Independently of the mirroring above, the operator account was added to
`RLS Demo Ops APAC` (and no other demo team) and the secured workbook queried
normally:

| | Rows | Offices returned |
|---|---|---|
| Sigma, live | 1,335 | London 670, Singapore 665 |
| SQL oracle | 1,335 | — |

Exactly APAC's grant set. Paris is absent because it is granted to EMEA only.
This is a literal per-user result — a real session, real team membership, no
mirroring — and it matches the oracle exactly.

**A literal login as `user1` / `user2` specifically is still outstanding.** `make_embed_urls.py`
generates JWT-signed embed URLs per persona for that final confirmation, but
whether they authenticate could not be verified headlessly — the JWT is validated
client-side, and signing requires a Developer Access credential with embedding
enabled, which `SIGMA_CLIENT_ID` may or may not be. **Open one in a browser to
confirm.**

---

## Files

```
rlscls_model_example/
├── README.md                    # this file
├── .env                         # gitignored; your PG credentials
├── .env.example                 # template — copy to .env and fill in
├── .gitignore                   # excludes out/, config.json
├── config.json                  # gitignored; your org's resolved IDs (see below)
├── config.example.json          # template — copy to config.json and fill in
├── rls_common.py                # shared config-load / API-call helpers
├── setup_personas.py            # one-time bootstrap: resolves operator/teams/personas
├── run_sql.sh                   # rebuild the warehouse objects
├── sql/
│   ├── 01_schema.sql            # DDL
│   ├── 02_seed_dimensions_and_grants.sql
│   ├── 03_seed_shipments.sql    # deterministic 8,000-row fact
│   └── 04_validate.sql          # the expected-results oracle
├── build_model.py               # generates + POSTs the data model spec
├── build_workbook.py            # both workbooks (--build/--kind) + grants + CSV export
├── validate_personas.py         # per-persona expected vs actual
├── make_embed_urls.py           # JWT embed URLs per persona
├── doc/
│   └── additive-rls-approach.html  # standalone write-up of the pattern
├── model/                       # generated specs (committed for review)
│   ├── rls_model.spec.json
│   ├── rls_secured.workbook.json
│   └── rls_validation.workbook.json
└── out/                         # exported CSVs (evidence; gitignored, regenerable)
```

## Installing this for your own org

Nothing is hardcoded to the original build — every org-specific value
(warehouse connection, target folder, schema, your own identity, the demo
teams, the test personas) lives in a gitignored `config.json`, resolved once
by `setup_personas.py`. `build_model.py` / `build_workbook.py` then read and
write that same file, so IDs never need copy-pasting by hand after the first
run.

**Prerequisites:**
- A Sigma warehouse connection already configured in your org — you need its connection id.
- A Sigma folder to hold the demo objects — you need its file id.
- Your own Sigma login email.
- 2–3 test user emails you're willing to have invited into the org with `analyze` access (or that already exist) — these become the validated personas.
- `SIGMA_API_TOKEN` obtainable via the `sigma-api` skill, and direct Postgres access to the same warehouse.

**Setup:**

```bash
cp .env.example .env               # fill in PGHOST/PGUSER/PGPASSWORD/PGDATABASE/RLS_SCHEMA
cp config.example.json config.json # fill in connectionId/folderId/schema/operatorEmail/
                                    # persona emails — leave every *Id field null

source ~/.zshrc
python3 setup_personas.py
```

`setup_personas.py` is idempotent — safe to re-run:
- validates `connectionId` / `folderId` actually exist
- resolves your `operatorId` from `operatorEmail`
- finds each of the four demo teams by name, creating any that don't exist
- finds each persona by email, inviting (`analyze` role) any that don't exist
- writes every resolved id back into `config.json`

If the invite step 400s (member-invite payload shapes vary by org config),
invite the missing test users manually in Sigma (Admin → Members) and re-run —
it will find them by email instead of trying to create them again.

**Build:**

```bash
./run_sql.sh                                                          # 1. warehouse objects
python3 build_model.py                                                # 2. data model — creates first run, updates after
python3 build_workbook.py --kind secured    --build --grant-personas  # 3a. consumer-facing workbook
python3 build_workbook.py --kind validation --build                   # 3b. diagnostic workbook
python3 validate_personas.py                                          # 4. expected vs actual per persona
```

Every create step (`build_model.py`, `build_workbook.py --build`) saves its
new id — `dataModelId`, `secureWorkbookId`, `validationWorkbookId` — back into
`config.json`, so re-running the same command later updates those same
objects instead of creating duplicates. Pass `--update <id>` to either script
to target something else instead.

Diagnostic variants of the model build:

```bash
# helper columns visible + RLS filter OFF, so the validation workbook
# can inspect all 8,000 rows rather than just the viewer's own
python3 build_model.py --validation-mode

# rebuild the A-vs-B comparison (adds the child tables and " B" columns back)
python3 build_model.py --compare-shapes --validation-mode
```

After ANY change to the model or the ledger, re-run `validate_personas.py`. A
rendering workbook and a `200` from the API are both compatible with the security
being silently wrong — every real bug found in this build looked fine on the
surface.

---

## Notes for the pattern doc — Sigma API / as-code behaviour

Learned the hard way; worth folding into the written pattern.

1. **`CurrentUserInTeam()` takes one team name, not a list.** The single most
   important correction. See above.
2. **It returns NULL, not False,** on empty input. Always `Coalesce(…, False)`.
3. **`Lookup()` supports compound keys** as repeated (local, target) pairs —
   confirmed working, which makes per-dimension child tables unnecessary.
4. **`Lookup(Max(…), …)` preserves the fact table's row count.** A direct join to
   the ledger would fan out, since the ledger has many rows per entity value.
5. **Postgres connections use a TWO-segment path** — `[schema, table]` — not the
   three (`[database, schema, table]`) Snowflake uses. Verify with
   `GET /v2/connections/paths?connectionId=…`.
6. **A new table is invisible until the connection is synced.** `POST
   /v2/connections/{id}/sync` with `{"path": []}` forces a full sync. Without it
   the model POST fails with `Source not found`.
7. **`POST /v2/dataModels/spec` requires `schemaVersion: 1` and a `folderId`.**
   Both are rejected as syntax errors if absent, with unhelpful messages
   (`schemaVersion: Invalid 1: undefined`).
8. **A successful POST does not mean the formulas compiled.** Always follow with
   `GET /v2/dataModels/{id}/columns` and check for `type.type == "error"`. This
   caught nothing here in the end, but it is the only way to see masked errors.
9. **`POST /v2/workbooks/spec` returns YAML, not JSON**, despite a JSON request
   body — `json.loads` on the response fails.
10. **Element order matters.** Referenced elements must precede referrers.
11. **Formula prefixes differ by source kind.** `warehouse-table` uses the last
    path segment (`[rls_shipments/office]`); `table` and `data-model` sources use
    the referenced element's **name** (`[Entity Grants/Entity Value]`,
    `[Shipments/Office]`). Calculated columns referencing siblings in the same
    element use the display name with no prefix.
12. **Team membership is set via `PATCH`, not `POST`.** `POST
    /v2/teams/{id}/members` returns `200 {}` and silently does nothing.
13. **A `list` filter over a BOOLEAN column shows as "Invalid filter" in the UI**,
    even though the API accepts it and the generated SQL enforces it correctly.
    Filter a numeric 1/0 mirror column with `number-range` instead. Worth noting
    that the security was never actually broken — but nobody wants to see an
    error badge on an RLS rule.
14. **Workbooks are deleted via `DELETE /v2/files/{id}`, not `/v2/workbooks/{id}`**
    — the latter returns 404. The file ID and the workbook ID are the same value.
15. **`POST /v2/workbooks/spec/verify` validates a spec without writing anything**
    (`{"valid": true}`). Use it before every create/update.
16. **Update an existing workbook with `PUT /v2/workbooks/{id}/spec`** — same body
    shape as create.
17. **`PUT /v2/dataModels/{id}/spec` does not update the model's `description`.**
    It accepts the field and updates every structural part (elements, columns,
    filters) but reading `.../spec` back afterwards shows the OLD description
    unchanged. `name`/`description` are file metadata, separate from the
    structural spec — update them with `PATCH /v2/files/{id}`. Same
    file-vs-spec split as the delete route above.
18. **Column-level security was out of scope** and no parallel mechanism was
    built for it, as instructed. CLS is configured natively against teams/users.

---

## Deviations from the original plan

| Plan | What happened | Why |
|---|---|---|
| `CurrentUserInTeam([Team Names])` over a `ListAgg` string | Replaced with per-grant-row evaluation aggregated by `Max()` | The original silently fails for multi-team entities |
| `Contains([User Emails], CurrentUserEmail())` | Removed entirely | Superseded by the per-row approach, which also avoids substring collisions between emails |
| Per-dimension child tables | Built, validated, and shown to be **unnecessary** | Compound-key `Lookup` works |
| Test users already exist | Neither existed; both created via API, plus all four teams | Org had 11 members and one unrelated team |
| Snowflake `CSA.MARK_O` | Postgres on Snowflake, database `sigma`, schema `mark_o` | Corrected mid-build |
| Per-user validation by login | Mirroring personas onto the operator account, plus one live confirmation via real team membership | API credentials authenticate as the operator; embed URLs generated but unverified |
| Ship both A and B for comparison | Stripped to B alone once they were proven equivalent | Duplicate `… B` columns are confusing in the UI; comparison is reproducible via `--compare-shapes` |

## Caveats

- The three persona row counts were measured by identity mirroring, not by
  logging in as `user1` / `user2`. A live per-user result was separately confirmed
  for the operator account in `RLS Demo Ops APAC` (1,335 rows, matching the
  oracle), so the mechanism is proven against a real identity — but the specific
  multi-team `user1` case has only been measured by mirroring. The embed URLs
  remain unconfirmed.
- `rls_test_personas` / `rls_test_persona_teams` duplicate Sigma team membership
  into the warehouse **for the test harness only**. Shipping that duplication
  would reintroduce exactly the sync problem this pattern avoids.
- Both test accounts were created as `analyze` and will have received invite
  emails to the `+alias` addresses.
- The ledger expresses **unions only**. If any real customer rule reads "X but only
  Y", the schema needs qualifier columns before building further — see
  [How the three dimensions relate](#limitation-or-only-ever-widens).
- Company→site cascade depends on the fact table carrying all three keys. A
  normalised fact would break it silently.
