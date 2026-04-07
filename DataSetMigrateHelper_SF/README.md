# DataSetMigrateHelper_SF

A Snowflake-native toolkit for mapping Sigma dataset dependency chains and workbook source relationships to support Dataset → Data Model migration planning.

## Overview

Sigma is deprecating Datasets in favour of Data Models. This toolkit uses the Sigma REST API to discover which datasets exist, how they depend on each other, which ones have been migrated, and which workbooks are still sourcing from unmigrated datasets. All output lands in Snowflake tables for reporting and ad-hoc analysis.

## Files

| File | Purpose |
|---|---|
| `dataset_relations_sf_proc.sql` | Snowflake stored procedure — builds the full dataset dependency graph into `SIGMA_DATASET_DEPENDENCIES` |
| `workbook_source_map_sf_proc.sql` | Snowflake stored procedure — maps workbook sources against the dependency graph into summary and detail tables |
| `dataset_relations_sf.py` | Standalone Snowflake Notebook / Snowpark script — earlier, simpler version of the dataset dependency logic |
| `dataset_chains_pivoted.sql` | Analysis query — flattens ROOT → INTERNAL → LEAF chains into one row per path |
| `crossover_analysis.sql` | Analysis queries — identifies fork points (one dataset feeds many) and merge points (one dataset pulls from many) |

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

```sql
SIGMA_BASE_URL      = 'https://api.staging.us.aws.sigmacomputing.io'
SIGMA_CLIENT_ID     = 'YOUR_SIGMA_CLIENT_ID'
SIGMA_CLIENT_SECRET = 'YOUR_SIGMA_CLIENT_SECRET'
TARGET_DATABASE     = 'YOUR_DATABASE'
TARGET_SCHEMA       = 'YOUR_SCHEMA'
TRUNCATE_BEFORE_INSERT = True   -- snapshot mode (recommended)
```

> **Note:** Uses the deprecated `GET /v2/datasets` endpoint intentionally — it is the only endpoint that exposes `migrationStatus`.

---

### 2. `sigma_workbook_source_map()` — `workbook_source_map_sf_proc.sql`

Scans all workbooks org-wide, resolves each source against `SIGMA_DATASET_DEPENDENCIES`, and writes migration status per workbook.

**Prerequisite:** Run `sigma_dataset_dependencies()` first — this procedure reads from `SIGMA_DATASET_DEPENDENCIES` to enrich workbook source data.

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

## Execution Order

```
1. Run prerequisites (ACCOUNTADMIN — once only)
   └── Create network rule + external access integration
       (see top of dataset_relations_sf_proc.sql)

2. CALL sigma_dataset_dependencies();
   └── Populates: SIGMA_DATASET_DEPENDENCIES

3. CALL sigma_workbook_source_map();
   └── Populates: SIGMA_WORKBOOK_MIGRATION_SUMMARY
                  SIGMA_WORKBOOK_SOURCE_DETAILS
```

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

## Snowflake Notebook Script

`dataset_relations_sf.py` is an earlier, standalone version of the dataset dependency logic designed for use inside a Snowflake Notebook or run externally with a local Snowpark session. It produces a simpler table schema (no data model columns, no crossover metrics) and is retained as a lightweight alternative.

---

## Prerequisites

- Snowflake role with `CREATE PROCEDURE`, `CREATE TABLE`, and `USAGE ON INTEGRATION` privileges
- Sigma API client credentials with admin scope (`skipPermissionCheck` access)
- Snowflake external network access to `api.staging.us.aws.sigmacomputing.io:443` (or production equivalent)
