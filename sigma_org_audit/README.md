# sigma_org_audit

> **Proof of concept.** This is a reference implementation shared to demonstrate an approach and give others something to extrapolate from — not a finished, supported, or authoritative tool. Take the ideas, adapt the patterns, build your own.

> **Disclaimer:** This project contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A Snowflake-native toolkit that builds a **holistic, replayable audit of a Sigma
organisation** from the Sigma REST API, for internal governance checks and
migration-readiness assessment.

## The idea — a replayable three-layer pipeline

```
Sigma REST API  ─┐
                 ├─►  RAW  ──►  STAGE  ──►  MARTS  ──►  audit_queries / Sigma workbook
Writeback schemas┘   (VARIANT  (typed     (SCD2 history
                      snapshots) latest    + scoring + drift
                      append-only) state)   + governance)
```

- **Raw** — append-only `VARIANT` snapshots of every API object, one row per
  object per snapshot. Nothing is transformed; a new API field is absorbed
  without code changes. Everything downstream is rebuildable from here.
- **Stage** — views only, zero business logic, latest-state per object.
- **Marts** — SCD2 history (derived from raw snapshots, so disposable and
  replayable), plus scoring / drift / governance views built for consumption.

This shape is the distinguishing choice: a bug in transform logic, or a need to
backfill a new computed column, never requires re-hitting the API — you rebuild
stage/marts from raw.

## Files

| File | Purpose |
|---|---|
| `setup_prerequisites.sql` | One-time ACCOUNTADMIN setup: network rule, secrets, external access integration, grants. |
| `api_flow.md` | Endpoint catalog + what each call lands in `RAW_SIGMA_OBJECTS`. |
| `procs/sigma_org_extract.sql` | Raw extraction proc — lands every API object type as VARIANT snapshots. |
| `procs/sigma_writeback_scan.sql` | Writeback audit — discovers writeback/WAL schemas from connections, scans SIGDS tables + WAL activity. |
| `stage/stage_views.sql` | `STG_*` views: typed latest-state flatten, incl. `STG_CONNECTIONS` and `STG_WRITEBACK_TABLES`. |
| `marts/scd2_history.sql` | `sigma_scd2_apply` — generic type-2 history builder for any stage view. |
| `marts/mart_views.sql` | Inventory, R/A/G migration scoring, writeback governance, ownership, drift. |
| `audit_queries.sql` | Ready-to-run governance & migration-readiness queries. |

A companion **`sigma-org-audit` Claude Code skill** (in `../sigma_skills/`) drives
this pipeline and interprets the results, optionally cross-checking against live
data via the **`sigma-cli` sub-skill**.

## Setup

1. **Prerequisites (once, as ACCOUNTADMIN):** edit and run `setup_prerequisites.sql`
   — set your Sigma API host and client credentials, and the execution role.
   Admin-scoped credentials are required for org-wide visibility
   (`skipPermissionCheck`).
2. **Deploy the procedures:** run `procs/sigma_org_extract.sql` and
   `procs/sigma_writeback_scan.sql`, then `marts/scd2_history.sql`.
   > Note: when a procedure's **parameter count changes** between versions,
   > `CREATE OR REPLACE` cannot replace it (Snowflake rejects the ambiguous
   > overload). `DROP PROCEDURE <name>(<old arg types>);` first, then re-create.
3. **Extract:**
   ```sql
   CALL sigma_org_extract('MY_DB', 'MY_SCHEMA');     -- API objects
   CALL sigma_writeback_scan('MY_DB', 'MY_SCHEMA');  -- writeback schemas (run after extract)
   ```
4. **Build views:** run `stage/stage_views.sql` then `marts/mart_views.sql`
   (set `USE DATABASE/SCHEMA` to where the raw table lives first).
5. **Build history (optional but recommended):**
   ```sql
   CALL sigma_scd2_apply('STG_WORKBOOKS',        'SCD2_WORKBOOKS',        'WORKBOOK_ID');
   CALL sigma_scd2_apply('STG_DATASETS',         'SCD2_DATASETS',         'DATASET_ID');
   CALL sigma_scd2_apply('STG_CONNECTIONS',      'SCD2_CONNECTIONS',      'CONNECTION_ID');
   CALL sigma_scd2_apply('STG_WRITEBACK_TABLES', 'SCD2_WRITEBACK_TABLES', 'SIGDS_TABLE');
   ```
   Re-run these after each new extract to accrue history and power the drift views.
6. **Query:** use `audit_queries.sql`, or point a Sigma workbook at the views.

## Multiple orgs — one deployment, many orgs

The pipeline is multi-org by default. `sigma_org_extract` calls `GET /v2/whoami`
once per run and stamps the resulting **`ORG_ID`** onto every row in
`RAW_SIGMA_OBJECTS`, so a single warehouse can hold many orgs side by side — each
row attributed to the org it came from. Every `STG_*` and `V_*` view carries
`ORG_ID` through, so you scope a report with `WHERE ORG_ID = '<uuid>'` or compare
orgs with `GROUP BY ORG_ID`. (Query 0 in `audit_queries.sql` lists the orgs present.)

### What's built once vs. per-org

This is the key mental model — **almost everything is shared**:

| Built **once** (shared by all orgs) | Run **per org** |
|---|---|
| Setup objects: network rule, secrets, external access integration, role grants (`setup_prerequisites.sql`) | `sigma_org_extract(...)` — one call per org |
| The audit database + schema (e.g. `SIGMA_ORG_AUDIT.AUDIT`) | — |
| All procedures, `STG_*` / `V_*` views | — |
| `RAW_SIGMA_OBJECTS` (org-tagged; holds every org) | — |

`sigma_writeback_scan` and `sigma_scd2_apply` are **one call each, covering every
org** present in the raw table — you do not run them per org (though
`sigma_writeback_scan` accepts an optional `ORG_FILTER` if you want to limit it).
You never rebuild the views per org.

### Getting each org's credentials

Generate an admin-scoped client ID + secret **in each org** you want to audit:
its *Administration → Developer Access → Create New*. Note that org's API host
from the table in `setup_prerequisites.sql` (the base URL). The secret is shown
once at creation — copy it immediately.

### Two ways to point an extract at a different org

**Option A — rotate the Secrets** (credentials never leave the Secret store;
nothing appears in query history; best for a small, stable set of orgs). The
external access integration already references these secret *names*, so there is
nothing else to change:

```sql
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE SECRET sigma_base_url      TYPE=GENERIC_STRING SECRET_STRING='https://<ORG_B_HOST>';
CREATE OR REPLACE SECRET sigma_client_id     TYPE=GENERIC_STRING SECRET_STRING='<ORG_B_CLIENT_ID>';
CREATE OR REPLACE SECRET sigma_client_secret TYPE=GENERIC_STRING SECRET_STRING='<ORG_B_CLIENT_SECRET>';
-- then re-run the extract (below) for org B
```

**Option B — pass override parameters** at call time (one `CALL` audits any org,
no setup change; but the client secret then appears in Snowflake query history):

```sql
CALL sigma_org_extract(
  'SIGMA_ORG_AUDIT','AUDIT','RAW_SIGMA_OBJECTS', TRUE, 10,
  'https://api.eu.aws.sigmacomputing.com', '<ORG_B_CLIENT_ID>', '<ORG_B_CLIENT_SECRET>');
```

The network rule created by `setup_prerequisites.sql` already allows egress to
**every** Sigma API host, so orgs on any cloud/region are reachable without
re-running setup.

### Worked example — audit two orgs end-to-end

Setup, procedures, and views are already deployed (steps 1–2 and 4 of **Setup**).
Then:

```sql
-- Org A (the org the Secrets point at) — extract
CALL sigma_org_extract('SIGMA_ORG_AUDIT','AUDIT');

-- Org B — extract via override params (or rotate Secrets per Option A, then call with no overrides)
CALL sigma_org_extract('SIGMA_ORG_AUDIT','AUDIT','RAW_SIGMA_OBJECTS', TRUE, 10,
     'https://<ORG_B_HOST>', '<ORG_B_CLIENT_ID>', '<ORG_B_CLIENT_SECRET>');

-- Writeback scan + history — ONE call each, covers every org now in the raw table
CALL sigma_writeback_scan('SIGMA_ORG_AUDIT','AUDIT');
CALL sigma_scd2_apply('STG_WORKBOOKS',        'SCD2_WORKBOOKS',        'WORKBOOK_ID');
CALL sigma_scd2_apply('STG_DATASETS',         'SCD2_DATASETS',         'DATASET_ID');
CALL sigma_scd2_apply('STG_CONNECTIONS',      'SCD2_CONNECTIONS',      'CONNECTION_ID');
CALL sigma_scd2_apply('STG_WRITEBACK_TABLES', 'SCD2_WRITEBACK_TABLES', 'SIGDS_TABLE');

-- List the orgs now present, then scope a report to one
SELECT ORG_ID, COUNT(*) FROM RAW_SIGMA_OBJECTS GROUP BY ORG_ID;          -- query 0
SELECT * FROM V_MIGRATION_SCORE WHERE ORG_ID = '<org-a-uuid>';
```

To refresh an org later, just re-run its `sigma_org_extract` call — every run is a
new append-only snapshot, and re-running the scan + SCD2 calls accrues history and
drift across all orgs.

### Refresh many orgs from one trigger — the tenant registry + fan-out

For auditing several orgs (a parent + its tenants, unrelated orgs, or any mix)
from a single trigger, use the **registry + fan-out** rather than one `CALL` per
org. One Snowflake secret holds every org's credentials, and
`sigma_org_extract_all` loops over it.

Why one secret? A stored procedure can only read secrets declared statically in
its `SECRETS` clause — it cannot resolve a secret name at runtime. So a single
registry secret (bound once to the integration) scales to any number of orgs with
no proc/integration change.

1. **Create the registry secret** (a JSON array of orgs):
   ```sql
   USE ROLE ACCOUNTADMIN;
   CREATE OR REPLACE SECRET sigma_tenant_registry TYPE = GENERIC_STRING
     SECRET_STRING = '[
       {"label":"acme",  "baseUrl":"https://aws-api.sigmacomputing.com",   "clientId":"<id>","clientSecret":"<sec>","role":"child", "enabled":true},
       {"label":"globex","baseUrl":"https://api.eu.aws.sigmacomputing.com","clientId":"<id>","clientSecret":"<sec>","role":"parent","enabled":true}
     ]';
   ALTER EXTERNAL ACCESS INTEGRATION sigma_api_access
     SET ALLOWED_AUTHENTICATION_SECRETS = (sigma_base_url, sigma_client_id, sigma_client_secret, sigma_tenant_registry);
   GRANT READ ON SECRET sigma_tenant_registry TO ROLE <YOUR_ROLE>;
   ```
   Each org's `clientId`/`clientSecret` is generated in *that org's* Administration
   → Developer Access. `role` (parent/child/standalone) is recorded via
   `ORG_ROLE_OVERRIDE` — needed because a child org cannot self-identify via the
   tenants API. **Add/remove an org** = edit the JSON and `CREATE OR REPLACE` again
   (no proc or integration change). Inject the value via a temp file so the creds
   stay out of shell history.

2. **Run it:**
   ```sql
   CALL sigma_org_extract_all('SIGMA_ORG_AUDIT','AUDIT');                      -- all enabled orgs
   CALL sigma_org_extract_all('SIGMA_ORG_AUDIT','AUDIT','RAW_SIGMA_OBJECTS','acme');  -- just one org
   ```
   It returns a per-org summary (`orgs_selected`, `orgs_succeeded`, and each org's
   result/error). One org's failure (e.g. a 403) does not abort the batch.

3. **Trigger from Sigma:** a workbook *Refresh all* button maps to the no-label
   call; a per-row *Refresh this org* button passes the label — both via a Call API
   action to the Snowflake SQL API, or a scheduled Task. Credentials are passed to
   `sigma_org_extract` as bound call arguments, not embedded in logged SQL.

> Alternative store: keep the org list (and even secrets) in a **table** instead
> of the registry secret — easier to manage and can be a Sigma input table for
> self-service onboarding, at the cost of holding secrets in a table column.
> The fan-out can read either.

## Writeback scan — discovered, not configured

`sigma_writeback_scan` does **not** take a schema parameter. It reads the
writeback schema locations straight from each connection's detail payload
(`STG_CONNECTIONS`), so it self-discovers every writeback schema in the org and
attributes each `SIGDS_*` table back to its owning connection. This is why
`sigma_org_extract` must run first.

**WAL location:** by Sigma's design the write-access destination schema is
reserved for *all* internal write-back objects, so the `SIGDS_WAL_*` edit-log
tables live in the **writeback schema(s)** alongside the `SIGDS_*` data tables —
not necessarily in a separate audit-log schema. The scan therefore looks for WAL
tables in each writeback location first, and only also uses the connection's
`inputTableAuditLogSchema` if it is set and points somewhere different. (The API's
`inputTableAuditLogSchema` field is frequently null even when WAL tables exist, so
it is not relied on as the primary source.)

**Reachability:** the connections API reveals *where* every writeback schema is,
but the scan can only read locations the executing Snowflake role has access to.
Connections pointing at another account/warehouse (or Databricks) are still
inventoried from the API, but their table/WAL contents are skipped and the
connection is flagged `SCAN_REACHABLE = FALSE` — a Phase-2 (cross-account
runner) concern.

**Shared writeback schemas / cross-org attribution:** a single warehouse schema
can be the writeback destination for *several* Sigma orgs. A `SIGDS_*` table is
physically discovered via whichever org's connection points at the schema, but
its true owner is read from the WAL workbook URL/ID. `V_WRITEBACK_GOVERNANCE`
therefore classifies every table by `ATTRIBUTION` — `OWNED`, `ORPHANED`,
`CROSS_ORG`, or `UNATTRIBUTED` — and only `ORPHANED` tables (confidently this
org's, with no live workbook) count toward its archival score and reclaimable
storage. Without this, one org's orphan/storage numbers would be inflated by
every other org's tables sharing the schema. `V_WRITEBACK_SHARED_SCHEMAS` flags
which writeback schemas are shared across orgs.

## Out of scope (Phase 2)

- The consuming Sigma workbook spec (code representation).
- The Call-API action / remediation layer (buttons that transfer ownership,
  retag, swap sources, archive — writing to an audit trail).
- Cross-account / Databricks writeback deep scans.

## Lineage / prior art

Reuses proven patterns from sibling projects in this repo: the
external-network-access + token/backoff/secrets stored-proc pattern from
`DataSetMigrateHelper_SF`, and the SIGDS/WAL writeback inventory + archival
scoring from `writeback_info_sf`.
