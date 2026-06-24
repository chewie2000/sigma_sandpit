# writeback_info_dbx

A Databricks toolkit for inventorying and monitoring Sigma writeback (input table) activity across one or more Unity Catalog schemas. It maps every active writeback table pair — the SIGDS data table (`sigds_*`) and its SIGDS_WAL write-ahead-log table (`sigds_wal_*`) — to its Sigma workbook or data model, enriches records with Delta metadata and Sigma API ownership data, and populates a central `SIGDS_WORKBOOK_MAP` table for reporting and cleanup.

## Overview

When Sigma writebacks are enabled, Sigma creates a WAL table (`sigds_wal_*`) and a data table (`sigds_*`) in Databricks for each input table. Over time these accumulate — workbooks get archived, tables go stale, and orphaned WAL records are left behind. This toolkit provides visibility into that state so administrators can identify cleanup candidates and track the migration of writeback workbooks.

## Files

| File | Purpose |
|---|---|
| `databricks.yml` | Asset Bundle root — variables, dev/prod targets, deploy entrypoint |
| `resources/sigds_workbook_map.job.yml` | Job definition — `for_each`-over-schemas task running the populate notebook |
| `src/populate_sigds_workbook_map.py` | Main notebook — incrementally populates `SIGDS_WORKBOOK_MAP` from WAL tables and the Sigma API |
| `sql/create_sigds_workbook_map.sql` | DDL reference — the notebook auto-creates the table on first run; use this only for manual/ahead-of-time provisioning |
| `sql/archival_scoring.sql` | Weighted confidence scoring matrix — scores every record across multiple signals to surface archival candidates |
| `sql/geninfo_queries.sql` | Reporting queries — landscape overview, storage reclamation, owner accountability, multi-table workbooks, legacy WAL inventory |

This toolkit ships as a **Databricks Asset Bundle**: the populate notebook is
deployed once and run per writeback schema via a `for_each` task, configuration
is supplied as job parameters, and Sigma credentials are read from a Databricks
**secret scope** (never stored in the repo).

---

## Prerequisites

- Databricks CLI v0.218+ authenticated to the target workspace (`databricks auth login`)
- Unity Catalog access for the job's run identity: `SELECT` on the scan schema(s), `SELECT` + `MODIFY` on the map schema
- Sigma OAuth client credentials (`client_id` / `client_secret`) generated in Sigma — admin scope recommended for full org visibility (the scope to hold them is created in Setup step 1)
- Permission to create a Databricks secret scope (or an existing scope you can write to)
- Serverless job compute enabled, or a job cluster defined in `resources/sigds_workbook_map.job.yml`
- `requests` available in the runtime (standard on Databricks Runtime / serverless)

---

## Sigma details to gather first

Setup needs two values from Sigma: your **API base URL** (used in step 3) and a
pair of **OAuth client credentials** (used in step 1). Collect both before you
start.

### SIGMA_API_BASE

The base URL depends on the cloud and region your Sigma organisation is hosted on. The `/v2` suffix is required — all API calls are versioned under this path.

| Cloud / Region | Base URL |
|---|---|
| AWS US | `https://aws-api.sigmacomputing.com/v2` |
| AWS EU | `https://api.eu.aws.sigmacomputing.com/v2` |
| Azure US | `https://api.us.azure.sigmacomputing.com/v2` |
| GCP US | `https://api.us.gcp.sigmacomputing.com/v2` |

Your organisation's base URL can be found in the [Sigma API documentation](https://help.sigmacomputing.com/reference/get-started-sigma-api) — look for the **Base URL** section which lists all available endpoints by cloud provider and region. This is the value you put in the `sigma_api_base` variable in step 3.

### Generating Sigma client credentials

`client_id` and `client_secret` are OAuth 2.0 client credentials generated from within Sigma; you store them in the secret scope in step 1. You will need **Admin** access to your Sigma organisation to generate them.

To generate credentials:

1. In Sigma, go to **Administration → Developer Access**.
2. Click **Create New** under Client Credentials.
3. Give the credential a name (e.g. `sigds-workbook-map`), select **Admin** scope for full org visibility, and click **Create**.
4. Copy the **Client ID** and **Client Secret** immediately — the secret is only shown once.

Full instructions are available in the [Sigma API credentials documentation](https://help.sigmacomputing.com/reference/generate-client-credentials).

> **Note:** Admin scope is recommended so the job can resolve workbook ownership and see all workbooks regardless of folder permissions. A non-admin credential will still work but may return incomplete results for workbooks the credential owner cannot access.

---

## Setup

The bundle is deployed with the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/) (v0.218+). Two required one-time steps (secret scope, bundle variables) plus an optional table-creation step, then deploy.

> **Where everything lives / where to run commands.** The bundle root is the
> `databricks.yml` file in **this `writeback_info_dbx/` folder** — the same
> folder as this README, alongside `src/`, `sql/`, and `resources/`. Run every
> `databricks bundle ...` command from inside this folder so the CLI can find
> `databricks.yml`:
>
> ```bash
> cd path/to/sigma_sandpit/writeback_info_dbx
> ```
>
> (The CLI searches the current directory and its parents for `databricks.yml`,
> so a subfolder works too — but `cd`-ing here is the simple, reliable choice.)

### 1. Create the secret scope (once)

Store the Sigma OAuth client credentials in a Databricks secret scope so they
never enter the repo or appear in logs. The default scope name is `sigma`
(override via the `secret_scope` parameter).

```bash
databricks secrets create-scope sigma
databricks secrets put-secret sigma client_id     --string-value "<YOUR_SIGMA_CLIENT_ID>"
databricks secrets put-secret sigma client_secret --string-value "<YOUR_SIGMA_CLIENT_SECRET>"
```

> For an Azure Key Vault- or AWS Secrets Manager-backed scope, create the scope
> against that backend instead; the notebook reads `client_id` / `client_secret`
> the same way via `dbutils.secrets.get`.

See [generating Sigma client credentials](#generating-sigma-client-credentials) above for how to obtain the values.

### 2. Create the table — optional

**You can skip this.** On its first run the notebook creates
`SIGDS_WORKBOOK_MAP` automatically (in `catalog.map_schema`) if it doesn't
already exist, using the same schema definition as the upsert — so the table and
the job can't drift. It's idempotent: every later run just confirms the table is
there.

You only need to provision it by hand if you'd rather create it ahead of time
(e.g. to grant permissions first, or apply table properties). To do that, run
`sql/create_sigds_workbook_map.sql` in Databricks SQL or a notebook, after
replacing the placeholders — the schema must be your `map_schema`:

```sql
-- Replace before running:
USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;
```

> If you let the job auto-create on first run, keep the `for_each`
> `concurrency` at its default of `1` for that first run, so two schema
> iterations don't race to create the same table.

### 3. Set bundle variables

This is where you tell the job *which* catalog, schemas, and Sigma endpoint to
use. You do it by editing one file — `databricks.yml`, in this folder. If you've
never edited a YAML file, follow this exactly; the whole step is "find four
commented lines, remove the `#`, and type in your values".

#### 3a. The five variables

The first four are required; `secret_scope` only if you didn't name your scope
`sigma` in step 1.

| Variable | What to put | Where it comes from / example |
|---|---|---|
| `catalog` | Your Unity Catalog name | The catalog holding your writeback tables, e.g. `analytics` |
| `map_schema` | Schema that holds the `SIGDS_WORKBOOK_MAP` table | The same schema you used in step 2, e.g. `prod_writes` |
| `sigma_api_base` | Your Sigma API URL, ending in `/v2` | Pick your region from the [SIGMA_API_BASE](#sigma_api_base) table, e.g. `https://aws-api.sigmacomputing.com/v2` |
| `schemas` | The schema(s) to scan, as a quoted list | One: `'["prod_writes"]'`. Two: `'["prod_writes","dev_writes"]'` |
| `secret_scope` | *(optional)* scope name from step 1 | Omit unless you named it something other than `sigma` |

#### 3b. Make the edit

Open `databricks.yml` in this folder. Under the `dev:` target you'll see a host
line and a `variables:` block already filled with `<...>` placeholder tokens:

```yaml
  dev:
    mode: development
    default: true
    workspace:
      host: https://<your-databricks-workspace-url>
    variables:
      catalog: <your-catalog>
      map_schema: <your-map-schema>
      sigma_api_base: <your-sigma-api-base-url>
      schemas: '["<your-schema>"]'
```

**Replace each `<...>` token with your own value** — there's nothing to
uncomment, just overwrite the placeholders. After editing it looks like this:

```yaml
  dev:
    mode: development
    default: true
    workspace:
      host: https://acme.cloud.databricks.com
    variables:
      catalog: analytics
      map_schema: prod_writes
      sigma_api_base: https://aws-api.sigmacomputing.com/v2
      schemas: '["prod_writes"]'
```

Then save the file. (Use your own values, of course — the workspace host comes
from `databricks auth describe`, and `sigma_api_base` from the
[SIGMA_API_BASE](#sigma_api_base) table.)

To **scan several schemas in one job**, just add more names to the `schemas`
list — everything else stays the same. For example, to scan `prod_writes`,
`dev_writes`, and `team_sales` (all written into the single `map_schema` table):

```yaml
    variables:
      catalog: analytics
      map_schema: prod_writes
      sigma_api_base: https://aws-api.sigmacomputing.com/v2
      schemas: '["prod_writes","dev_writes","team_sales"]'
```

The job then runs the notebook once per entry in the list. See
[Multiple writeback schemas](#multiple-writeback-schemas) for the full worked
example.

When you're ready for production, fill in the `<...>` tokens under the `prod:`
target the same way (it already has a `schedule_pause: UNPAUSED` line above the
placeholders — leave that as is).

> If you leave a placeholder unfilled, the job fails fast with a clear
> *"Parameter(s) still contain placeholder values"* error rather than a cryptic
> catalog-not-found failure.

#### 3c. YAML rules that will bite you if you ignore them

- **Only change what's to the right of the colon.** The placeholder lines are
  already indented correctly — just overwrite the `<...>` value, don't move the
  line. Changing the leading spaces (or replacing them with a tab) breaks the
  file.
- **Spaces, never tabs.** If your editor inserts a tab, the file won't parse.
  (VS Code: click "Spaces" / "Tab" in the bottom status bar to switch.)
- **Keep the quotes around `schemas`.** It stays `schemas: '["prod_writes"]'` —
  single quotes outside, double quotes inside. It's one string the job splits
  into a list, *not* a YAML list. One schema is still a one-element list.
- **`map_schema` is fixed; `schemas` is what varies.** The job always writes its
  output into `map_schema` and scans each entry in `schemas`. To cover more
  schemas later you only add names to `schemas`.
- **`map_schema` must match the schema you used in step 2's DDL.**

#### 3d. Check it parsed

After saving, confirm the file is valid before deploying:

```bash
databricks bundle validate -t dev
```

You want `Validation OK!`. A YAML error here almost always means a stray tab, a
misaligned line, or a missing quote around `schemas` — re-check 3c.

> **Alternative (no file editing).** You can instead pass values on the command
> line at deploy time using environment variables:
>
> ```bash
> BUNDLE_VAR_catalog=analytics \
> BUNDLE_VAR_map_schema=prod_writes \
> BUNDLE_VAR_sigma_api_base=<your-sigma-api-base-url> \
> BUNDLE_VAR_schemas='["prod_writes"]' \
>   databricks bundle validate -t dev
> ```
>
> Use the `BUNDLE_VAR_` form, not `--var="schemas=..."` — the `--var` parser
> chokes on the quotes and brackets in the `schemas` value.

### 4. Deploy and run

```bash
databricks bundle validate -t dev          # check the bundle resolves
databricks bundle deploy   -t dev          # deploy the job to the workspace
databricks bundle run sigds_workbook_map -t dev   # run on demand
```

Swap `-t dev` for `-t prod` for the production target (which unpauses the daily
schedule). The `for_each` task runs the populate notebook once per schema in
`schemas`, keeping `map_schema` fixed — no file edits between schemas.

### Watching progress

`databricks bundle run` waits for the run to finish (pass `--no-wait` to return
immediately) and prints a **run-page URL** plus the run's state transitions. You
get progress at two levels:

- **In the terminal / run logs.** The notebook logs each of its seven phases as
  `[Phase n/7] …`, and the long-running steps draw an ASCII bar, e.g.:

  ```
  [Phase 3/7] Discovered 1,222 WAL tables. Running parallel DESCRIBE DETAIL…
    DESCRIBE WAL ▕███████████░░░░░░░░░░░░░▏ 560/1222
  [Phase 4/7] Extracted 87 new/updated SIGDS table records.
  ```

- **On the run page (UI).** Open the printed URL to watch the `for_each` task as
  a matrix — one tile per schema, going grey → running → green — and to see each
  iteration's full notebook output live.

For graphical `tqdm` bars in the notebook UI, set the `use_tqdm` parameter to
`true` (job parameter or `--params use_tqdm=true`). It has no effect on the
plain-text CLI logs. Other tuning parameters: `describe_workers`,
`wal_batch_size`, `max_wal_tables`.

### Compute

The job runs on **serverless** job compute by default (no cluster to manage).
If serverless is not enabled on your workspace, uncomment the `job_clusters`
block in `resources/sigds_workbook_map.job.yml` and set a `node_type_id` /
`spark_version` for your cloud, then add `job_cluster_key` to the task.

---

## Maintenance & migration notes

### Upgrading an existing table (SOURCE_SCHEMA → SCAN_SCHEMA)

If you ran an earlier version, your table will have a column called `SOURCE_SCHEMA`. Delta requires column mapping to be enabled before a rename can be performed:

```sql
-- Step 1: enable column mapping (one-time, metadata-only)
ALTER TABLE <YOUR_CATALOG>.<YOUR_MAP_SCHEMA>.SIGDS_WORKBOOK_MAP
SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion'   = '2',
  'delta.minWriterVersion'   = '5'
);

-- Step 2: rename the column
ALTER TABLE <YOUR_CATALOG>.<YOUR_MAP_SCHEMA>.SIGDS_WORKBOOK_MAP
  RENAME COLUMN SOURCE_SCHEMA TO SCAN_SCHEMA;
```

No data is rewritten — both steps are metadata-only operations.

### Multiple writeback schemas

`SIGDS_WORKBOOK_MAP` supports multiple writeback schemas in a single table. Every row is stamped with the `SCAN_SCHEMA` value from the run that produced it, so results from different schemas remain distinguishable. The MERGE key is the composite `SIGDS_TABLE + SCAN_SCHEMA`, preventing collisions when the same bare table name exists in more than one schema.

To cover multiple schemas, list them all in the `schemas` bundle variable — the
`for_each` task runs the populate notebook once per entry, with `map_schema`
held fixed. **The only thing that changes versus a single-schema setup is the
`schemas` list** — add more names and you scan more schemas.

#### Worked example — scan three schemas in one job

Say your writeback tables live across `prod_writes`, `dev_writes`, and
`team_sales`, and you want them all inventoried into one `SIGDS_WORKBOOK_MAP`
table in `prod_writes`. Configure the `prod` target like this:

```yaml
# databricks.yml
targets:
  prod:
    mode: production
    workspace:
      host: https://<your-databricks-workspace-url>
      root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
    variables:
      schedule_pause: UNPAUSED
      catalog: analytics
      map_schema: prod_writes
      sigma_api_base: <your-sigma-api-base-url>
      schemas: '["prod_writes","dev_writes","team_sales"]'
```

Then deploy and run as normal:

```bash
databricks bundle validate -t prod
databricks bundle deploy   -t prod
databricks bundle run sigds_workbook_map -t prod
```

What happens on that single run: the `for_each` task fires the populate notebook
**three times** — once with `scan_schema=prod_writes`, once with
`scan_schema=dev_writes`, once with `scan_schema=team_sales` — each scanning its
own schema's `sigds_wal_*` tables. All three write into the one
`analytics.prod_writes.SIGDS_WORKBOOK_MAP` table (the `map_schema`), each row
stamped with the `SCAN_SCHEMA` it came from. To add or drop a schema later, edit
only the `schemas` list and redeploy — nothing else changes.

> By default the iterations run **one at a time** (`concurrency: 1` in
> `resources/sigds_workbook_map.job.yml`), which keeps Sigma API and warehouse
> load predictable. If you have many schemas and want them to run in parallel,
> raise that `concurrency` value.

Every iteration writes to the same `SIGDS_WORKBOOK_MAP` table. All analysis queries (`archival_scoring.sql`, `geninfo_queries.sql`) include `SCAN_SCHEMA` in their output so you can filter or group by schema. Sigma API enrichment (workbook names, owner details) is shared across schemas — a `WORKBOOK_ID` seen in any previous run is not re-fetched.

---

## How the populate script works

Each run follows these steps:

1. **Load watermarks** — reads stored `WAL_TABLE_LAST_MODIFIED` timestamps and known `WORKBOOK_ID`s from `SIGDS_WORKBOOK_MAP` in a single query.
2. **Discover WAL tables** — runs `SHOW TABLES` to find all `sigds_wal_*` tables in the schema.
3. **Skip unchanged WAL tables** — runs `DESCRIBE DETAIL` in parallel on every WAL table and compares `lastModified` against the stored watermark. WAL tables with no new writes are skipped entirely (no row scans).
4. **Extract latest WAL entries** — reads the most recent WAL row per SIGDS table from changed WAL tables using batched `UNION ALL` queries (one Spark job per batch of up to 100 tables).
5. **Delta metadata** — runs `DESCRIBE DETAIL` in parallel for each new or changed SIGDS table to capture table ID, location, size, and timestamps.
6. **Sigma API enrichment** — fetches workbook/data-model metadata only for `WORKBOOK_ID`s not already in the table. Resolves owner names via `GET /v2/members`. `API_IS_ARCHIVED` is re-checked on every run for all known IDs.
7. **Version tag resolution** — fetches all version tags via `GET /v2/tags`, then lists workbooks per tag to build a `taggedWorkbookId → parent workbook` mapping. Tagged version records are flagged with `IS_TAGGED_VERSION=TRUE`, `VERSION_TAG_NAME`, and `PARENT_WORKBOOK_ID`.
8. **MERGE** — writes all changes into `SIGDS_WORKBOOK_MAP` via a single `MERGE` keyed on the composite `SIGDS_TABLE + SCAN_SCHEMA`. Records whose WAL table has disappeared are flagged `IS_DELETED=TRUE`; the flag is cleared if the WAL table reappears. All deletion and orphan flag updates are scoped to the current `SCAN_SCHEMA` so other schemas' rows are never affected.

---

## SIGDS_WORKBOOK_MAP — key columns

Column names use consistent prefixes to make the data source immediately obvious:
- **`WAL_`** — sourced from WAL row data or WAL table metadata
- **`SIGDS_`** — sourced from `DESCRIBE DETAIL` on the writeback data table
- **`API_`** — sourced from the Sigma REST API

| Column | Description |
|---|---|
| `SIGDS_TABLE` | Bare SIGDS table name — part of composite primary key |
| `SCAN_SCHEMA` | Schema that was scanned to produce this row — part of composite primary key |
| `WAL_TABLE_FQN` | Fully-qualified WAL table name (`catalog.schema.sigds_wal_*`) |
| `WAL_DS_ID` | Input table dataset ID extracted from the WAL record |
| `WAL_WORKBOOK_URL` | Workbook URL extracted from WAL METADATA (`sigmaUrl` / `workbookUrl`) |
| `WAL_INPUT_TABLE_NAME` | Input table element title extracted from WAL METADATA |
| `WAL_LAST_EDIT_AT` | Timestamp of the latest WAL row for this SIGDS table |
| `WAL_LAST_EDIT_BY` | Email of the user who made the last edit, from WAL METADATA |
| `WAL_MAX_EDIT_NUM` | Highest `EDIT_NUM` seen in the WAL for this SIGDS table |
| `WAL_TABLE_LAST_MODIFIED` | Watermark — `lastModified` from `DESCRIBE DETAIL` on the WAL table at last processing |
| `SIGDS_TABLE_ID` | Delta table GUID from `DESCRIBE DETAIL` on the SIGDS table |
| `SIGDS_TABLE_LOCATION` | Cloud storage path of the SIGDS Delta table |
| `SIGDS_TABLE_CREATED_AT` | Timestamp when the SIGDS Delta table was first created |
| `SIGDS_TABLE_LAST_MODIFIED` | Timestamp of the most recent write to the SIGDS Delta table |
| `SIGDS_TABLE_SIZE_BYTES` | Current on-disk size of the SIGDS Delta table in bytes |
| `WORKBOOK_ID` | Sigma workbook or data model ID |
| `WORKBOOK_NAME / PATH` | Display name and folder path (from Sigma API) |
| `OBJECT_TYPE` | `WORKBOOK` or `DATA_MODEL` |
| `ORG_SLUG` | Sigma org slug parsed from the workbook URL |
| `IS_ORPHANED` | `TRUE` when the SIGDS data table no longer exists in Databricks |
| `IS_DELETED` | `TRUE` when the WAL table has disappeared from the schema |
| `IS_LEGACY_WAL` | `TRUE` for old `sigds_wal_<uuid>` naming (pre-DS_ID convention) |
| `IS_TAGGED_VERSION` | `TRUE` when the workbook ID is a version tag (e.g. Prod, QA) |
| `VERSION_TAG_NAME` | Name of the version tag when `IS_TAGGED_VERSION` is TRUE |
| `PARENT_WORKBOOK_ID` | Source workbook ID when `IS_TAGGED_VERSION` is TRUE |
| `API_WORKBOOK_URL` | Workbook URL from the Sigma API — set once on first enrichment |
| `API_OWNER_ID` | Sigma member UUID of the workbook owner (from Sigma API) |
| `API_IS_ARCHIVED` | Archived state from Sigma API — refreshed every run |
| `API_OWNER_FIRST_NAME` | Owner first name resolved via `GET /v2/members` — set once |
| `API_OWNER_LAST_NAME` | Owner last name resolved via `GET /v2/members` — set once |

---

## Analysis Queries

`geninfo_queries.sql` covers the same analytical dimensions as `archival_scoring.sql` (status flags, edit recency, edit volume, storage, legacy WAL, version tags) but as exploratory reporting views rather than a scoring engine. Replace `<YOUR_CATALOG>` and `<YOUR_SCHEMA>` before running.

| Query | What it shows |
|---|---|
| 1. Landscape overview | Per-schema summary: total tables, orphaned/deleted/archived counts, total and reclaimable storage (GB) |
| 2. Storage reclamation opportunity | All tables with a clear archival signal (orphaned, deleted, or archived workbook), ranked by size with the primary reason surfaced |
| 3. Active workbooks going stale | Active workbooks where writeback activity has dropped off, grouped into inactivity bands (31–90 / 91–180 / 181–365 / >365 days) |
| 4. Most active writeback tables | Highest-edit-volume input tables — the inverse archival view; useful for identifying business-critical tables before any cleanup nearby |
| 5. Owner accountability summary | Cleanup burden rolled up by workbook owner: archived, orphaned, stale counts and reclaimable GB per owner |
| 6. Workbooks with multiple input tables | Groups by source workbook (resolving tagged versions to their parent) to find workbooks with more than one named input element. Shows named element count, total SIGDS file count (inflated by repeated tag updates), and how many schemas the workbook spans |
| 7. Legacy WAL inventory | All `sigds_wal_<uuid>` tables, split by migration priority: active legacy WALs (still being written) flagged as urgent; inactive as low-priority |

---

## Archival Scoring (`archival_scoring.sql`)

Scores every record in `SIGDS_WORKBOOK_MAP` across eight weighted signals to produce a ranked list of archival candidates.

### Scoring model (total = 100 pts, higher = stronger archival candidate)

| Dimension | Max | Logic |
|---|---|---|
| Archival / deletion status | 30 | `IS_ORPHANED`=TRUE → 30 / `IS_DELETED`=TRUE → 25 / `API_IS_ARCHIVED`=TRUE → 20 / workbook absent from API (deleted or never saved) → 15 |
| WAL edit recency | 25 | >365 days (or NULL) → 25 / >180 → 18 / >90 → 10 / >30 → 4 |
| SIGDS table modification | 15 | >365 days (or NULL) → 15 / >180 → 10 / >90 → 5 |
| Edit volume (`WAL_MAX_EDIT_NUM`) | 10 | 0/NULL → 10 / ≤10 → 8 / ≤50 → 5 / ≤200 → 2 |
| Legacy WAL flag | 10 | Legacy + active (<180 days) → 10 / Legacy + inactive → 5 |
| Storage reclamation | 10 | >1 GB → 10 / >100 MB → 7 / >10 MB → 4 / else → 1 |

**Risk penalty:** `IS_TAGGED_VERSION` = TRUE → subtract 15 pts (floor at 0). Tagged versions (Prod, QA) are high-risk to archive and are penalised to prevent automatic tier promotion.

### Confidence tiers

| Score | Tier | Recommendation |
|---|---|---|
| ≥ 75 | **TIER 1** | Strong candidate — quarantine now |
| 50–74 | **TIER 2** | Likely candidate — review with owner |
| 25–49 | **TIER 3** | Monitor — check in 90 days |
| < 25 | **TIER 4** | Keep — active or protected |

The query outputs every individual score component alongside the total, making it easy to understand why a record scored highly and to tune thresholds for your organisation. A tier summary rollup at the end of the file groups record counts, total storage (GB), average score, and min/max edit age by tier.

> **Important — read before taking any action based on these scores.**
>
> The confidence tiers and weights in this model are entirely subjective. What constitutes an appropriate threshold for archival will vary significantly from customer to customer depending on usage patterns, business criticality, data retention policies, and team workflows. The scores are a starting point for investigation, not a directive.
>
> **Incorrectly removing a SIGDS table or its associated WAL table can cause irreparable impact to the related Sigma content.** Workbooks and input tables that depend on these objects will break immediately and, if the tables have been dropped rather than moved, may not be recoverable. Always follow the safe deletion process (move to quarantine first, monitor, then delete) and ensure the record has been reviewed and approved by the workbook owner before any action is taken.

---

## Safe Deletion of SIGDS and WAL Tables

> **Best practice: move first, delete later — never drop directly.**

Sigma stores the exact fully-qualified table name of both the SIGDS data table and the WAL table in its internal metadata. When Sigma looks up an input table it searches for a table matching the exact `SIGDS_<uuid>` identifier in the writeback schema configured on the connection. If the table has been dropped or renamed, workbooks will immediately fail with errors such as:

```
Object '<DB>.<SCHEMA>."SIGDS_WAL_xxx"' does not exist or not authorized
```

This applies equally to the WAL table — Sigma holds the WAL table name in its metadata and requires an exact match.

### Recommended process

1. **Identify candidates** using `geninfo_queries.sql` or `archival_scoring.sql` (archived, orphaned, or stale records).
2. **Move** the SIGDS and WAL tables to a quarantine schema (e.g. `<SCHEMA>_quarantine`) using `ALTER TABLE ... RENAME TO`. Do not drop them yet.
3. **Monitor** for a safe period (recommended: 30 days minimum) to confirm no workbook errors are raised and no users report missing data.
4. **Drop** the tables from the quarantine schema once the safe period has passed.

### Why this matters

If a table is moved or renamed rather than dropped outright, recovery is straightforward — rename the table back to its original location (`<CATALOG>.<SCHEMA>.<SIGDS_TABLE_NAME>`) and the workbook resumes functioning immediately. A direct `DROP TABLE` is irreversible and eliminates this recovery path.

