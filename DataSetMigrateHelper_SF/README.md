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
| `dataset_chains_pivoted.sql` | Analysis query — flattens ROOT → INTERNAL → LEAF chains into one row per path |
| `crossover_analysis.sql` | Analysis queries — identifies fork points (one dataset feeds many) and merge points (one dataset pulls from many) |

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

Edit the non-credential configuration constants at the top of each procedure's Python block (`SIGMA_BASE_URL`, `TARGET_DATABASE`, `TARGET_SCHEMA`) then run the `CREATE OR REPLACE PROCEDURE` statement in a Snowflake worksheet using the role granted in step 1.

### 3. Call the procedures in order

```sql
CALL sigma_dataset_dependencies();   -- populates SIGMA_DATASET_DEPENDENCIES
CALL sigma_workbook_source_map();    -- populates SIGMA_WORKBOOK_MIGRATION_SUMMARY
                                     --           SIGMA_WORKBOOK_SOURCE_DETAILS
```

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

**Configuration constants (edit before deploying):**

```python
SIGMA_BASE_URL         = "https://api.eu.aws.sigmacomputing.com"  # match your cloud/region
TARGET_DATABASE        = "YOUR_DATABASE"
TARGET_SCHEMA          = "YOUR_SCHEMA"
TRUNCATE_BEFORE_INSERT = True    # snapshot mode (recommended)
API_CALL_DELAY_SECONDS = 0.1     # increase if Sigma rate-limits you
```

Credentials (`SIGMA_CLIENT_ID`, `SIGMA_CLIENT_SECRET`) are **not** set here — they are read at runtime from Snowflake Secrets created in `setup_prerequisites.sql`. Do not hardcode them.

> **Note:** Uses the deprecated `GET /v2/datasets` endpoint intentionally — it is the only endpoint that exposes `migrationStatus`.

---

### 2. `sigma_workbook_source_map()` — `workbook_source_map_sf_proc.sql`

Scans all workbooks org-wide, resolves each source against `SIGMA_DATASET_DEPENDENCIES`, and writes migration status per workbook.

**Prerequisite:** Run `sigma_dataset_dependencies()` first — this procedure reads from `SIGMA_DATASET_DEPENDENCIES` to enrich workbook source data.

**Configuration constants (edit before deploying):**

```python
SIGMA_BASE_URL         = "https://api.eu.aws.sigmacomputing.com"  # match your cloud/region
TARGET_DATABASE        = "YOUR_DATABASE"
TARGET_SCHEMA          = "YOUR_SCHEMA"
TRUNCATE_BEFORE_INSERT = True    # snapshot mode (recommended)
API_CALL_DELAY_SECONDS = 0.1     # increase if Sigma rate-limits you
```

Credentials are read at runtime from Snowflake Secrets — do not hardcode them.

**Output tables:**

#### `SIGMA_WORKBOOK_MIGRATION_SUMMARY` — one row per workbook (migration-scope only)

| Column | Description |
|---|---|
| `WORKBOOK_ID / NAME / URL / PATH` | Workbook identity |
| `OWNER_NAME / OWNER_EMAIL` | Workbook owner |
| `TOTAL_SOURCES` | Count of migration-scope sources (datasets + data models) |
| `DATASET_SOURCE_COUNT` | Sources still pointing at legacy datasets |
| `DATA_MODEL_SOURCE_COUNT` | Sources already pointing at data models |
| `MIGRATION_STATUS` | `FULLY MIGRATED`, `PARTIALLY MIGRATED`, or `NOT MIGRATED` |

#### `SIGMA_WORKBOOK_SOURCE_DETAILS` — one row per workbook → source

Each row is a single source resolved against the dependency graph, including `DATASET_MIGRATION_STATUS`, `MIGRATED_AT`, `DATA_MODEL_ID`, and graph metrics (`UPSTREAM_PARENT_COUNT`, `DOWNSTREAM_CHILD_COUNT`).

---

## Analysis Queries

### `dataset_chains_pivoted.sql`

Recursively walks the dependency graph from each ROOT to its terminal LEAFs, producing one row per unique chain path. Columns are pivoted to `L0` (root) through `L4` (up to 5 levels deep; extend for deeper chains).

Useful for: understanding full lineage of any dataset end-to-end, and seeing the overall `CHAIN_MIGRATION_STATUS` (`FULLY MIGRATED` / `PARTIALLY MIGRATED` / `NOT MIGRATED`).

### `crossover_analysis.sql`

Two queries:

- **Fork Points** — datasets with `DOWNSTREAM_CHILD_COUNT > 1`. High-priority migration targets: migrating (or failing to migrate) these affects multiple downstream datasets. Includes actionable `MIGRATION_GUIDANCE`.
- **Merge Points** — datasets with `UPSTREAM_PARENT_COUNT > 1`. These are blocked until all parents are migrated. Includes `MIGRATION_READINESS` status.

---

## Prerequisites

### Snowflake

- Role with `CREATE PROCEDURE`, `CREATE TABLE`, `USAGE ON INTEGRATION`, and `READ ON SECRET` privileges
- `ACCOUNTADMIN` access to run `setup_prerequisites.sql` (one-time)

### Sigma API credentials

These are OAuth 2.0 client credentials generated from within Sigma. **Admin** scope is required for `skipPermissionCheck` access (org-wide dataset and workbook visibility).

To generate credentials:
1. In Sigma, go to **Administration → Developer Access**.
2. Click **Create New** under Client Credentials.
3. Give the credential a name, select **Admin** scope, and click **Create**.
4. Copy the **Client ID** and **Client Secret** immediately — the secret is only shown once.

Full instructions: [Sigma API credentials documentation](https://help.sigmacomputing.com/reference/generate-client-credentials)

### Sigma API base URL

The `SIGMA_BASE_URL` in each procedure depends on the cloud and region your Sigma organisation is hosted on:

| Cloud / Region | Base URL |
|---|---|
| AWS US | `https://aws-api.sigmacomputing.com` |
| AWS EU | `https://api.eu.aws.sigmacomputing.com` |
| Azure US | `https://api.us.azure.sigmacomputing.com` |
| GCP US | `https://api.us.gcp.sigmacomputing.com` |

See [Sigma API getting started](https://help.sigmacomputing.com/reference/get-started-sigma-api) for the full list. The same host (without `/v2`) is used as both `SIGMA_BASE_URL` in the procedure and in `setup_prerequisites.sql`.

### Credential storage — Snowflake Secrets (recommended)

Credentials are stored as Snowflake Secrets and read at runtime via `_snowflake.get_generic_secret_string()`. They are never embedded in procedure source code, visible in query history, or exposed in version control. `setup_prerequisites.sql` creates the secrets and grants the necessary permissions.
