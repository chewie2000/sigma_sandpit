# DataModelGraph_SF

> **IN DEVELOPMENT — Do not use.** This toolkit is currently under active development. Content may be incomplete, incorrect, or subject to breaking changes without notice.

> **Disclaimer:** This repository contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A Snowflake-native toolkit for mapping the Sigma data model dependency graph — which data models source from which other data models, how deep the chains run, and which warehouse connection each model sources from.

## Overview

As organisations migrate from Sigma Datasets to Data Models, data models increasingly source from other data models rather than from warehouse tables directly. This toolkit discovers and maps those inter-data-model dependencies, classifying each model by its position in the graph and surfacing the warehouse connection it ultimately sources from.

Designed as a complement to [DataSetMigrateHelper_SF](../DataSetMigrateHelper_SF/README.md) — use that toolkit for dataset dependency analysis, and this one once data models are in place.

## Files

| File | Purpose |
|---|---|
| `setup_prerequisites.sql` | One-time ACCOUNTADMIN setup — network rule, Snowflake Secrets, external access integration, and grants |
| `data_model_graph_sf_proc.sql` | Snowflake stored procedure — builds the data model dependency graph into `SIGMA_DATA_MODEL_GRAPH` |
| `data_model_graph_analysis.sql` | Analysis queries — overview, leaf models, dependency chains, fork points, merge points |

---

## Prerequisites

### Snowflake

- Role with `CREATE PROCEDURE`, `CREATE TABLE`, `USAGE ON INTEGRATION`, and `READ ON SECRET` privileges
- `ACCOUNTADMIN` access to run `setup_prerequisites.sql` (one-time)

### Sigma API credentials

**Admin** credentials are recommended. The `/v2/dataModels` list endpoint does not support `skipPermissionCheck` reliably across all API versions — without Admin credentials, only data models owned by the API user are returned. Data models referenced by others as sources are fetched individually as they are encountered, but org-wide completeness is best achieved with Admin access.

Credential setup follows the same pattern as DataSetMigrateHelper_SF — see [that README](../DataSetMigrateHelper_SF/README.md#sigma-api-credentials) for step-by-step instructions. Store the same three secrets (`sigma_base_url`, `sigma_client_id`, `sigma_client_secret`) using `setup_prerequisites.sql`.

---

## Setup

### 1. Run `setup_prerequisites.sql` (ACCOUNTADMIN — once only)

Creates the network rule, Snowflake Secrets, external access integration, and grants. Replace all `<LIKE_THIS>` placeholders before running.

### 2. Deploy the stored procedure

Run the `CREATE OR REPLACE PROCEDURE` statement from `data_model_graph_sf_proc.sql` in a Snowflake worksheet using the role granted in step 1.

### 3. Call the procedure

```sql
CALL sigma_data_model_graph('MY_DATABASE', 'MY_SCHEMA');
```

---

## Stored Procedure

### `sigma_data_model_graph()` — `data_model_graph_sf_proc.sql`

Crawls all data models org-wide via the Sigma API and writes one row per data-model-to-parent-data-model relationship into `SIGMA_DATA_MODEL_GRAPH`. Sources are fetched concurrently (default 10 workers) for speed.

**Output table columns:**

| Column | Description |
|---|---|
| `RUN_ID` | UUID for this execution — use to filter to the latest run |
| `RELATION_TYPE` | `ROOT` (no upstream DM parents), `INTERNAL` (mid-chain), or `LEAF` (terminal) |
| `DATA_MODEL_ID / NAME / PATH / URL` | Identity of the child data model |
| `DATA_MODEL_CREATED_AT / UPDATED_AT` | Data model timestamps |
| `UPSTREAM_PARENT_COUNT` | Number of direct data model parents (>1 = merge point) |
| `DOWNSTREAM_CHILD_COUNT` | Number of data models that directly source from this one (>1 = fork point) |
| `PARENT_DATA_MODEL_ID / NAME / PATH` | Direct upstream data model (NULL for ROOT rows) |
| `CONNECTION_ID / NAME / TYPE` | Warehouse connection the data model sources from (NULL for data models with no direct table sources) |

**Parameters:**

| Parameter | Required | Default | Description |
|---|---|---|---|
| `TARGET_DATABASE` | Yes | — | Snowflake database where the output table will be written |
| `TARGET_SCHEMA` | Yes | — | Snowflake schema where the output table will be written |
| `TARGET_TABLE` | No | `SIGMA_DATA_MODEL_GRAPH` | Output table name |
| `TRUNCATE_BEFORE_INSERT` | No | `TRUE` | `TRUE` = snapshot mode, replace on each run. `FALSE` = append every run as a new `RUN_ID` — all analysis queries filter to `MAX(RUN_ID)` so results are always correct either way. |

**Performance:** Source fetches run concurrently using a thread pool (`MAX_WORKERS = 10` at the top of `main()`). Increase for faster fetching; reduce if you hit 429 rate-limit errors. Rate-limit backoff is handled automatically.

**Example call:**

```sql
CALL sigma_data_model_graph('MY_DATABASE', 'MY_SCHEMA');
```

> **Note on completeness:** `GET /v2/dataModels` does not support `skipPermissionCheck` at all API versions. Data models owned by other users may be absent from the list but can still appear as referenced parents; those are fetched individually and included in the graph with whatever metadata the API returns.

---

## Analysis Queries

### `data_model_graph_analysis.sql`

Five queries for exploring the data model graph:

| Query | Purpose |
|---|---|
| **1. Overview** | Org-wide counts by graph role (ROOT / INTERNAL / LEAF), crossover metrics (fork and merge point counts), and depth statistics |
| **2. Leaf data models** | Data models nothing else depends on — candidates for review or consolidation |
| **3. Dependency chains** | Full ROOT→LEAF paths (up to 5 levels deep), one row per unique chain path |
| **4. Fork points** | Data models with `DOWNSTREAM_CHILD_COUNT > 1` — changes to these affect multiple downstream models |
| **5. Merge points** | Data models with `UPSTREAM_PARENT_COUNT > 1` — depend on multiple upstream models |
