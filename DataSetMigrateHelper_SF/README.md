# DataSetMigrateHelper_SF

A Snowflake-native toolkit for mapping Sigma dataset dependency chains and workbook source relationships to support Dataset → Data Model migration planning.

## Overview

Sigma is deprecating Datasets in favour of Data Models. This toolkit uses the Sigma REST API to discover which datasets exist, how they depend on each other, which ones have been migrated, and which workbooks are still sourcing from unmigrated datasets. All output lands in Snowflake tables for reporting and ad-hoc analysis.

## Files

| File | Purpose |
|---|---|
| `setup_prerequisites.sql` | One-time ACCOUNTADMIN setup — network rule, Snowflake Secrets, external access integration, and grants |
| `dataset_relations_sf_proc.sql` | Snowflake stored procedure — builds the full dataset dependency graph into `SIGMA_DATASET_DEPENDENCIES` |
| `workbook_source_map_sf_proc.sql` | Snowflake stored procedure — maps workbook sources against the dependency graph into summary and detail tables |
| `artifact_grants_sf_proc.sql` | Snowflake stored procedure — fetches Sigma permission grants for every dataset, data model, and workbook into `SIGMA_ARTIFACT_GRANTS` |
| `dataset_chains_pivoted.sql` | Analysis query — flattens ROOT → INTERNAL → LEAF chains into one row per path |
| `crossover_analysis.sql` | Analysis queries — identifies fork points (one dataset feeds many) and merge points (one dataset pulls from many) |
| `migration_overview.sql` | High-level dashboard queries — org-wide progress, terminal datasets, inconsistencies, re-pointing candidates, and migration readiness pipeline |
| `views/V_NODES_DATA.sql` | Sigma-ready view — one row per node (dataset, workbook, data model) for graph visualisation |
| `views/V_EDGES_DATA.sql` | Sigma-ready view — one row per edge (dependency relationship) for graph visualisation |
| `views/V_CHAIN_SUMMARY.sql` | Sigma-ready view — one row per ROOT→LEAF chain path with migration status; use as a dropdown source in Sigma to drive graph filtering |
| `views/V_CHAIN_MEMBERS.sql` | Sigma-ready view — one row per (chain path, node ID); join table between the chain picker and V_NODES_DATA to filter the graph by selected chain |

---

## Prerequisites

### Snowflake

- Role with `CREATE PROCEDURE`, `CREATE TABLE`, `USAGE ON INTEGRATION`, and `READ ON SECRET` privileges
- `ACCOUNTADMIN` access to run `setup_prerequisites.sql` (one-time)

### Sigma API credentials

You will need **Admin** access to your Sigma organisation to generate API credentials.

**Admin scope is required** for this toolkit. Both stored procedures use `skipPermissionCheck=true` on the datasets and workbooks list endpoints — without Admin credentials, these calls return only content owned by the API user rather than the full org, producing incomplete results.

#### Generating credentials

1. In Sigma, open the left navigation and go to **Administration**.
2. Select **Developer Access** from the Administration menu.
3. Under **Client Credentials**, click **Create New**.
4. Enter a descriptive name (e.g. `dataset-migrate-helper`) so it is identifiable later.
5. Set the **Permission** to **Admin**.
6. Click **Create**.
7. Copy both the **Client ID** and **Client Secret** immediately and store them somewhere secure — the secret is displayed only once and cannot be retrieved again. If lost, you must delete the credential and create a new one.

Once copied, fill in all three secrets in `setup_prerequisites.sql`:

```sql
CREATE SECRET IF NOT EXISTS sigma_base_url
  TYPE          = GENERIC_STRING
  SECRET_STRING = 'https://api.eu.aws.sigmacomputing.com';  -- your cloud/region URL

CREATE SECRET IF NOT EXISTS sigma_client_id
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_ID>';      -- paste Client ID here

CREATE SECRET IF NOT EXISTS sigma_client_secret
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_SECRET>';  -- paste Client Secret here
```

> **Note:** Client credentials authenticate as a service identity, not as an individual user. Actions taken via the API using these credentials will be attributed to the credential owner in Sigma audit logs. Using a dedicated named credential (rather than a personal one) makes it easier to identify and rotate if needed.

Full instructions: [Generate client credentials](https://help.sigmacomputing.com/reference/generate-client-credentials)

### Sigma API base URL

The `SIGMA_BASE_URL` in each procedure depends on the cloud and region your Sigma organisation is hosted on:

| Cloud / Region | Base URL |
|---|---|
| AWS US | `https://aws-api.sigmacomputing.com` |
| AWS EU | `https://api.eu.aws.sigmacomputing.com` |
| Azure US | `https://api.us.azure.sigmacomputing.com` |
| GCP US | `https://api.us.gcp.sigmacomputing.com` |

See [Sigma API getting started](https://help.sigmacomputing.com/reference/get-started-sigma-api) for the full list. The base URL is stored as the `sigma_base_url` secret in `setup_prerequisites.sql` — the same host used in the network rule, so it only needs to be set in one place.

### Credential storage — Snowflake Secrets (recommended)

Credentials are stored as Snowflake Secrets and read at runtime via `_snowflake.get_generic_secret_string()`. They are never embedded in procedure source code, visible in query history, or exposed in version control. `setup_prerequisites.sql` creates the secrets and grants the necessary permissions.

---

## Setup

### 1. Run `setup_prerequisites.sql` (ACCOUNTADMIN — once only)

Before deploying either stored procedure, run `setup_prerequisites.sql` as `ACCOUNTADMIN`. This creates:

- **Network rule** — allows outbound HTTPS from Snowflake to the Sigma API host
- **Snowflake Secrets** — stores your `SIGMA_CLIENT_ID` and `SIGMA_CLIENT_SECRET` securely; credentials are read at procedure runtime via `_snowflake.get_generic_secret_string()` and never appear in procedure source or query history
- **External access integration** — authorises procedures to use the network rule and secrets
- **Grants** — `USAGE ON INTEGRATION` and `READ ON SECRET` for the role that will run the procedures

Replace all `<LIKE_THIS>` placeholders in the script before running — the API host, credentials, and role name.

### 2. Deploy the stored procedures

Run the `CREATE OR REPLACE PROCEDURE` statement from each procedure file in a Snowflake worksheet using the role granted in step 1. No edits are required — all credentials are read from Snowflake Secrets at runtime and all configuration is passed as parameters at call time.

### 3. Call the procedures in order

```sql
CALL sigma_dataset_dependencies('MY_DATABASE', 'MY_SCHEMA');

CALL sigma_workbook_source_map('MY_DATABASE', 'MY_SCHEMA');

CALL sigma_artifact_grants('MY_DATABASE', 'MY_SCHEMA');
```

`sigma_artifact_grants()` must be called after the first two — it reads artifact IDs from their output tables.

---

## Stored Procedures

### 1. `sigma_dataset_dependencies()` — `dataset_relations_sf_proc.sql`

Crawls all datasets org-wide via the Sigma API and writes one row per dataset-to-parent relationship into `SIGMA_DATASET_DEPENDENCIES`.

**Output table columns (key ones):**

| Column | Description |
|---|---|
| `RUN_ID` | UUID for this execution — use to filter to the latest run |
| `RELATION_TYPE` | `ROOT` (no upstream), `INTERNAL` (mid-chain), or `LEAF` (terminal) |
| `DATASET_ID / NAME / PATH / URL` | Identity of the child dataset |
| `DATASET_CREATED_BY / OWNER` | Ownership metadata |
| `DATASET_MIGRATION_STATUS` | `not-migrated`, `migrated`, or `not-required` |
| `MIGRATED_AT / MIGRATED_BY` | When and who migrated this dataset to a data model |
| `DATA_MODEL_ID / NAME / URL / PATH` | Target data model (NULL if not yet migrated) |
| `PARENT_ID / NAME / PARENT_MIGRATION_STATUS` | Direct upstream dataset (NULL for ROOT rows) |
| `UPSTREAM_PARENT_COUNT` | Number of direct parents (>1 = merge point) |
| `DOWNSTREAM_CHILD_COUNT` | Number of direct children (>1 = fork point) |
| `CONNECTION_ID / NAME / TYPE` | Warehouse connection the dataset sources from (NULL for datasets with no direct table sources) |

**Parameters — pass at call time, no editing of the procedure required:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `TARGET_DATABASE` | Yes | — | Snowflake database where the output table will be written |
| `TARGET_SCHEMA` | Yes | — | Snowflake schema where the output table will be written |
| `TARGET_TABLE` | No | `SIGMA_DATASET_DEPENDENCIES` | Output table name |
| `TRUNCATE_BEFORE_INSERT` | No | `TRUE` | `TRUE` = snapshot mode, replace on each run (recommended). `FALSE` = append every run as a new `RUN_ID` — useful for tracking migration progress over time, but the table grows unboundedly. All analysis queries filter to `MAX(RUN_ID)` so results are always correct either way. |

`SIGMA_BASE_URL`, `SIGMA_CLIENT_ID`, and `SIGMA_CLIENT_SECRET` are all read at runtime from Snowflake Secrets — they are never passed as parameters or hardcoded. All three are set once in `setup_prerequisites.sql`.

**Example call:**

```sql
CALL sigma_dataset_dependencies('MY_DATABASE', 'MY_SCHEMA');
```

> **Note:** Uses the deprecated `GET /v2/datasets` endpoint intentionally — it is the only endpoint that exposes `migrationStatus`.

---

### 2. `sigma_workbook_source_map()` — `workbook_source_map_sf_proc.sql`

Scans all workbooks org-wide, resolves each source against `SIGMA_DATASET_DEPENDENCIES`, and writes migration status per workbook.

**Prerequisite:** Run `sigma_dataset_dependencies()` first — this procedure reads from `SIGMA_DATASET_DEPENDENCIES` to enrich workbook source data.

**Parameters — pass at call time, no editing of the procedure required:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `TARGET_DATABASE` | Yes | — | Snowflake database where output tables will be written |
| `TARGET_SCHEMA` | Yes | — | Snowflake schema where output tables will be written |
| `DEPENDENCIES_TABLE` | No | `SIGMA_DATASET_DEPENDENCIES` | Source table populated by `sigma_dataset_dependencies()` |
| `SUMMARY_TABLE` | No | `SIGMA_WORKBOOK_MIGRATION_SUMMARY` | Output summary table name |
| `DETAILS_TABLE` | No | `SIGMA_WORKBOOK_SOURCE_DETAILS` | Output details table name |
| `TRUNCATE_BEFORE_INSERT` | No | `TRUE` | `TRUE` = snapshot mode, replace on each run (recommended). `FALSE` = append every run as a new `RUN_ID` — useful for tracking migration progress over time, but the table grows unboundedly. All analysis queries filter to `MAX(RUN_ID)` so results are always correct either way. |

`SIGMA_BASE_URL`, `SIGMA_CLIENT_ID`, and `SIGMA_CLIENT_SECRET` are all read at runtime from Snowflake Secrets — they are never passed as parameters or hardcoded. All three are set once in `setup_prerequisites.sql`.

**Example call:**

```sql
CALL sigma_workbook_source_map('MY_DATABASE', 'MY_SCHEMA');
```

**Performance:** Workbook sources are fetched concurrently using a thread pool. The default is 10 parallel workers (`MAX_WORKERS = 10` at the top of the `main()` function in the procedure). Increase this if the Sigma API can handle more concurrency; reduce it if you see 429 rate-limit errors. Rate-limit backoff is handled automatically.

**Output tables:**

#### `SIGMA_WORKBOOK_MIGRATION_SUMMARY` — one row per workbook (migration-scope only)

| Column | Description |
|---|---|
| `WORKBOOK_ID / NAME / URL / PATH` | Workbook identity |
| `OWNER_NAME / OWNER_EMAIL` | Workbook owner |
| `WORKBOOK_CREATED_AT / WORKBOOK_UPDATED_AT` | Workbook timestamps |
| `TOTAL_SOURCES` | Count of migration-scope sources (datasets + data models) |
| `DATASET_SOURCE_COUNT` | Sources still pointing at legacy datasets |
| `DATA_MODEL_SOURCE_COUNT` | Sources already pointing at data models |
| `TABLE_SOURCE_COUNT` | Sources pointing directly at warehouse tables (not migration-scope, counted for completeness) |
| `MIGRATION_STATUS` | `FULLY MIGRATED`, `PARTIALLY MIGRATED`, or `NOT MIGRATED` |

> **Note:** If you have an existing `SIGMA_WORKBOOK_MIGRATION_SUMMARY` table from a previous deployment, drop it before calling the procedure so it is recreated with the current schema: `DROP TABLE IF EXISTS <DATABASE>.<SCHEMA>.SIGMA_WORKBOOK_MIGRATION_SUMMARY;`

#### `SIGMA_WORKBOOK_SOURCE_DETAILS` — one row per workbook → source

| Column | Description |
|---|---|
| `WORKBOOK_ID / NAME / PATH` | Workbook identity |
| `SOURCE_INODE_ID` | Inode ID of the source as returned by the Sigma API — join key used by analysis queries |
| `SOURCE_DATA_MODEL_ID` | Data model ID (populated when `SOURCE_TYPE = 'data-model'`) |
| `SOURCE_TYPE` | `dataset`, `data-model`, or `table` |
| `IN_MIGRATION_SCOPE` | `TRUE` if this source was matched against the dependency graph |
| `DATASET_ID / NAME / PATH / URL` | Resolved dataset identity (from `SIGMA_DATASET_DEPENDENCIES`) |
| `DATASET_MIGRATION_STATUS` | `not-migrated`, `migrated`, or `not-required` |
| `DATASET_RELATION_TYPE` | `ROOT`, `INTERNAL`, or `LEAF` |
| `MIGRATED_AT / MIGRATED_BY` | When and who migrated the dataset |
| `DATA_MODEL_ID / NAME / URL / PATH` | Target data model (NULL if dataset not yet migrated) |
| `UPSTREAM_PARENT_COUNT` | Number of direct parents in the dependency graph |
| `DOWNSTREAM_CHILD_COUNT` | Number of direct children in the dependency graph |

---

### 3. `sigma_artifact_grants()` — `artifact_grants_sf_proc.sql`

Fetches Sigma permission grants for every dataset, data model, and workbook in the latest run of the upstream tables. Artifact IDs are read directly from `SIGMA_DATASET_DEPENDENCIES` and `SIGMA_WORKBOOK_MIGRATION_SUMMARY` — no additional API list calls needed. Grants are fetched concurrently (`MAX_WORKERS = 10`) using `GET /v2/grants?inodeId=<ID>&directGrantsOnly=true`. Team and member display names are hydrated from a single pre-fetch of `/v2/teams` and `/v2/members`.

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `TARGET_DATABASE` | Yes | — | Snowflake database where the output table will be written |
| `TARGET_SCHEMA` | Yes | — | Snowflake schema where the output table will be written |
| `DEPENDENCIES_TABLE` | No | `SIGMA_DATASET_DEPENDENCIES` | Source table from `sigma_dataset_dependencies()` |
| `WORKBOOK_SUMMARY_TABLE` | No | `SIGMA_WORKBOOK_MIGRATION_SUMMARY` | Source table from `sigma_workbook_source_map()` |
| `GRANTS_TABLE` | No | `SIGMA_ARTIFACT_GRANTS` | Output table name |
| `TRUNCATE_BEFORE_INSERT` | No | `TRUE` | Snapshot mode (replace) vs append with new `RUN_ID` |

**Example call:**

```sql
CALL sigma_artifact_grants('MY_DATABASE', 'MY_SCHEMA');
```

#### `SIGMA_ARTIFACT_GRANTS` — one row per artifact → grantee

| Column | Description |
|---|---|
| `RUN_ID` | UUID for this execution — use to filter to the latest run |
| `ARTIFACT_TYPE` | `dataset`, `datamodel`, or `workbook` |
| `ARTIFACT_ID / NAME / PATH / URL` | Identity of the artifact |
| `GRANTEE_TYPE` | `team` or `member` |
| `GRANTEE_ID` | Team ID or member ID from Sigma |
| `GRANTEE_NAME` | Resolved display name (team name, or member first + last name) |
| `PERMISSION_LEVEL` | Permission level as returned by the Sigma API (e.g. `view`, `edit`, `explore`) |

> **Note on `directGrantsOnly`:** The proc fetches only grants assigned directly to each artifact, not permissions inherited from parent folders. This reflects the explicit access control decisions made on each artifact and avoids inflating results with inherited folder-level access.

---

## Analysis Queries

### `dataset_chains_pivoted.sql`

Recursively walks the dependency graph from each ROOT to its terminal LEAFs, producing one row per unique chain path. Columns are pivoted to `L0` (root) through `L4` (up to 5 levels deep; extend for deeper chains).

Useful for: understanding full lineage of any dataset end-to-end, and seeing the overall `CHAIN_MIGRATION_STATUS` (`FULLY MIGRATED` / `PARTIALLY MIGRATED` / `NOT MIGRATED`).

### `crossover_analysis.sql`

Two queries:

- **Fork Points** — datasets with `DOWNSTREAM_CHILD_COUNT > 1`. High-priority migration targets: migrating (or failing to migrate) these affects multiple downstream datasets. Includes actionable `MIGRATION_GUIDANCE`.
- **Merge Points** — datasets with `UPSTREAM_PARENT_COUNT > 1`. These are blocked until all parents are migrated. Includes `MIGRATION_READINESS` status.

### `views/V_NODES_DATA.sql`

Sigma-ready view combining datasets, workbooks, and data models into a single node list for graph visualisation. Dataset nodes include `UPSTREAM_PARENT_COUNT`, `DOWNSTREAM_CHILD_COUNT`, `IS_MERGE_POINT`, and `IS_FORK_POINT` for filter controls. Non-dataset node types have NULL for those columns.

### `views/V_EDGES_DATA.sql`

Sigma-ready view of all dependency edges — one row per dataset-to-dataset or dataset-to-workbook relationship.

### `views/V_CHAIN_SUMMARY.sql`

One row per unique ROOT→LEAF chain path. Columns: `ROOT_ID`, `ROOT_NAME`, `CHAIN_PATH` (e.g. `"A → B → C"`), `CHAIN_DEPTH`, `CHAIN_MIGRATION_STATUS` (`FULLY MIGRATED` / `PARTIALLY MIGRATED` / `NOT MIGRATED`). Use as a dropdown/list source in a Sigma workbook to let users pick a chain and drive graph filtering.

### `views/V_CHAIN_MEMBERS.sql`

Join table: one row per (chain path, node ID). Columns: `CHAIN_ROOT_ID`, `CHAIN_ROOT_NAME`, `CHAIN_PATH`, `CHAIN_MIGRATION_STATUS`, `NODE_ID`. A node shared by multiple chains appears once per chain. Use this view to connect a chain picker (sourced from `V_CHAIN_SUMMARY`) to the graph node source (`V_NODES_DATA`) — filter `V_CHAIN_MEMBERS` by the selected `CHAIN_PATH`, then join on `NODE_ID = node_id` to restrict the graph to only the nodes on that chain.

---

### `migration_overview.sql`

Six high-level queries combining all three output tables for an org-wide migration dashboard:

| Query | Purpose |
|---|---|
| **1. Dataset migration progress** | Total datasets, counts and % by status (`migrated` / `not-migrated` / `not-required`), breakdown by graph role (ROOT / INTERNAL / LEAF), and terminal dataset counts |
| **2. Workbook migration summary** | Total workbooks and counts by `MIGRATION_STATUS`, plus aggregate legacy dataset source counts across the org |
| **3. Terminal datasets** | Datasets with no downstream dataset dependants AND no workbook references. Classified as: `EASY WIN` (not migrated, truly isolated), `DONE` (migrated, nothing left to do), or `ORPHANED` (not-required, unused) |
| **4. Datasets needing immediate action** | Two sub-queries: **(A) Child ahead of parent** — a child dataset's data model exists but the parent is still not-migrated (data integrity risk); **(B) Blocking chains** — not-migrated datasets that are directly blocking other not-migrated datasets |
| **5. Workbooks needing re-pointing** | Workbooks still sourcing from a legacy dataset that has already been migrated — the workbook source needs updating to the data model |
| **6. Migration readiness pipeline** | Every not-migrated dataset classified as `READY` (all parents migrated, can go now) or `BLOCKED by N parent(s)`, sorted by downstream impact so you know which READY datasets to prioritise |
