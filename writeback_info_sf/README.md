# writeback_info_sf

> **IN DEVELOPMENT — Do not use.** This toolkit is currently under active development. Content may be incomplete, incorrect, or subject to breaking changes without notice.

A Snowflake toolkit for inventorying and monitoring Sigma writeback (input table) activity across a schema. It maps every active writeback WAL table to its Sigma workbook or data model, enriches records with Snowflake table metadata and Sigma API ownership data, and populates a central `SIGDS_WORKBOOK_MAP` table for reporting and cleanup.

## Overview

When Sigma writebacks are enabled, Sigma creates a WAL table (`SIGDS_WAL_*`) and a data table (`SIGDS_*`) in Snowflake for each input table. Over time these accumulate — workbooks get archived, tables go stale, and orphaned WAL records are left behind. This toolkit provides visibility into that state so administrators can identify cleanup candidates and track the migration of writeback workbooks.

## Files

| File | Purpose |
|---|---|
| `create_sigds_workbook_map.sql` | DDL — creates the `SIGDS_WORKBOOK_MAP` table in Snowflake (run once) |
| `populate_sigds_workbook_map.py` | Main script — incrementally populates `SIGDS_WORKBOOK_MAP` from WAL tables and the Sigma API |
| `archival_scoring.sql` | Weighted confidence scoring matrix — scores every record across multiple signals to surface archival candidates |
| `geninfo_queries.sql` | Reporting queries — landscape overview, storage reclamation, owner accountability, multi-table workbooks, legacy WAL inventory |

---

## Setup

### Prerequisites

- Snowflake account with access to the database and schema containing your `SIGDS_WAL_*` and `SIGDS_*` tables
- A Snowflake user with `SELECT` on the writeback schema and `SELECT` + `INSERT` + `UPDATE` + `MERGE` on the schema where `SIGDS_WORKBOOK_MAP` lives
- Python 3.8+ with `snowflake-connector-python` and `requests` installed:
  ```
  pip install snowflake-connector-python requests
  ```
- Sigma API client credentials (admin scope recommended for full org visibility)

### 1. Create the table (once)

Edit `create_sigds_workbook_map.sql` and replace the placeholders, then run in Snowflake:

```sql
-- Replace before running:
USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;
```

### 2. Configure and run the populate script

Edit the configuration block at the top of `populate_sigds_workbook_map.py`:

```python
ACCOUNT             = "<YOUR_ACCOUNT>"       # e.g. xy12345.us-east-1
USER                = "<YOUR_USER>"
PASSWORD            = "<YOUR_PASSWORD>"
ROLE                = "<YOUR_ROLE>"
WAREHOUSE           = "<YOUR_WAREHOUSE>"
DATABASE            = "<YOUR_DATABASE>"
SCAN_SCHEMA         = "<YOUR_SCAN_SCHEMA>"   # schema containing SIGDS_WAL_* tables to scan
MAP_SCHEMA          = "<YOUR_MAP_SCHEMA>"    # schema where SIGDS_WORKBOOK_MAP lives
SIGMA_API_BASE      = "<YOUR_API_BASE_URL>/v2"
SIGMA_CLIENT_ID     = "<YOUR_SIGMA_CLIENT_ID>"
SIGMA_CLIENT_SECRET = "<YOUR_SIGMA_CLIENT_SECRET>"
```

For a single-schema setup, set `SCAN_SCHEMA` and `MAP_SCHEMA` to the same value.

#### ACCOUNT

The Snowflake account identifier. Found in your Snowflake URL: for `https://xy12345.us-east-1.snowflakecomputing.com` the account is `xy12345.us-east-1`. For accounts in the AWS US East region the identifier is just the account locator (e.g. `xy12345`).

#### SIGMA_API_BASE

The base URL depends on the cloud and region your Sigma organisation is hosted on. The `/v2` suffix is required — all API calls are versioned under this path.

| Cloud / Region | Base URL |
|---|---|
| AWS US | `https://aws-api.sigmacomputing.com/v2` |
| AWS EU | `https://api.eu.aws.sigmacomputing.com/v2` |
| Azure US | `https://api.us.azure.sigmacomputing.com/v2` |
| GCP US | `https://api.us.gcp.sigmacomputing.com/v2` |

#### SIGMA_CLIENT_ID and SIGMA_CLIENT_SECRET

These are OAuth 2.0 client credentials generated from within Sigma. You will need **Admin** access to generate them.

1. In Sigma, go to **Administration → Developer Access**.
2. Click **Create New** under Client Credentials.
3. Give the credential a name, select **Admin** scope, and click **Create**.
4. Copy the **Client ID** and **Client Secret** immediately — the secret is only shown once.

Full instructions: [Sigma API credentials documentation](https://help.sigmacomputing.com/reference/generate-client-credentials).

> **Note:** Admin scope is recommended so the script can resolve workbook ownership and see all workbooks regardless of folder permissions.

Run the script from any Python environment with network access to Snowflake and the Sigma API:

```bash
python populate_sigds_workbook_map.py
```

### Multiple writeback schemas

`SIGDS_WORKBOOK_MAP` supports multiple writeback schemas in a single table. Every row is stamped with the `SCAN_SCHEMA` value from the run that produced it. The MERGE key is the composite `SIGDS_TABLE + SCAN_SCHEMA`, preventing collisions when the same bare table name exists in more than one schema.

To cover multiple schemas, run the script once per schema:

```python
# Run 1 — production writeback schema
SCAN_SCHEMA = "PROD_WRITES"
MAP_SCHEMA  = "PROD_WRITES"

# Run 2 — development writeback schema (map table stays the same)
SCAN_SCHEMA = "DEV_WRITES"
MAP_SCHEMA  = "PROD_WRITES"
```

---

## How the populate script works

Each run follows these steps:

1. **Authenticate** — obtains a Sigma OAuth bearer token.
2. **Load watermarks** — reads stored `WAL_MAX_EDIT_NUM` values and known `WORKBOOK_ID`s from `SIGDS_WORKBOOK_MAP` in a single query.
3. **Discover WAL tables** — queries `INFORMATION_SCHEMA.TABLES` for all `SIGDS_WAL_*` tables in the schema. Detects tables that have disappeared since the last run.
4. **Extract latest WAL entries** — reads the most recent WAL row per SIGDS table using batched `UNION ALL` queries (one Snowflake query per batch of up to 100 tables). Records where `WAL_MAX_EDIT_NUM` has not increased since the last run are filtered out before the MERGE.
5. **SIGDS table metadata** — resolves size and timestamp information for each discovered SIGDS table from the `INFORMATION_SCHEMA.TABLES` data already fetched in step 3 (no additional queries needed).
6. **Sigma API enrichment** — fetches workbook/data-model metadata only for `WORKBOOK_ID`s not already in the table. Resolves owner names via `GET /v2/members`. `API_IS_ARCHIVED` is re-checked on every run for all known IDs.
7. **Version tag resolution** — fetches all version tags via `GET /v2/tags`, then lists workbooks per tag to build a `taggedWorkbookId → parent workbook` mapping. Tagged version records are flagged with `IS_TAGGED_VERSION=TRUE`, `VERSION_TAG_NAME`, and `PARENT_WORKBOOK_ID`.
8. **MERGE** — writes all changes into `SIGDS_WORKBOOK_MAP` via a temp staging table and a single MERGE statement keyed on `SIGDS_TABLE + SCAN_SCHEMA`. Records whose WAL table has disappeared are flagged `IS_DELETED=TRUE`; the flag is cleared if the WAL table reappears. Deletion and orphan flag updates are scoped to the current `SCAN_SCHEMA`.

### Differences from the Databricks version

| Aspect | Databricks | Snowflake |
|---|---|---|
| Runtime | PySpark notebook on a cluster | Standard Python script (`snowflake-connector-python`) |
| WAL change detection | `DESCRIBE DETAIL` (metadata-only, no table scan) compared to stored timestamp watermark | All WAL tables processed every run; MERGE skips rows where `WAL_MAX_EDIT_NUM` is unchanged |
| Table metadata | `DESCRIBE DETAIL` per table, parallelised via thread pool | Single `INFORMATION_SCHEMA.TABLES` query covering all tables |
| Storage path | `SIGDS_TABLE_LOCATION` (cloud path from `DESCRIBE DETAIL`) | Not applicable — Snowflake is fully managed; column omitted |
| JSON extraction | `get_json_object(col, '$.key')` | `TRY_PARSE_JSON(col):key::VARCHAR` |
| `TIMESTAMP` column | Unquoted | Double-quoted (`"TIMESTAMP"`) — reserved word in Snowflake |
| MERGE staging | Spark DataFrame registered as temp view | Snowflake `TEMPORARY TABLE` with `executemany` insert |
| Date arithmetic | `INTERVAL 30 DAY` | `DATEADD(DAY, -30, CURRENT_TIMESTAMP())` |

---

## SIGDS_WORKBOOK_MAP — key columns

Column names use consistent prefixes to make the data source immediately obvious:
- **`WAL_`** — sourced from WAL row data or WAL table metadata
- **`SIGDS_`** — sourced from `INFORMATION_SCHEMA.TABLES` on the writeback data table
- **`API_`** — sourced from the Sigma REST API

| Column | Description |
|---|---|
| `SIGDS_TABLE` | Bare SIGDS table name — part of composite primary key |
| `SCAN_SCHEMA` | Schema that was scanned to produce this row — part of composite primary key |
| `WAL_TABLE_FQN` | Fully-qualified WAL table name (`database.schema.SIGDS_WAL_*`) |
| `WAL_DS_ID` | Input table dataset ID extracted from the WAL record |
| `WAL_WORKBOOK_URL` | Workbook URL extracted from WAL METADATA (`sigmaUrl` / `workbookUrl`) |
| `WAL_INPUT_TABLE_NAME` | Input table element title extracted from WAL METADATA |
| `WAL_LAST_EDIT_AT` | Timestamp of the latest WAL row for this SIGDS table |
| `WAL_LAST_EDIT_BY` | Email of the user who made the last edit, from WAL METADATA |
| `WAL_MAX_EDIT_NUM` | Highest `EDIT_NUM` seen in the WAL for this SIGDS table; used as watermark |
| `WAL_TABLE_LAST_MODIFIED` | `LAST_ALTERED` from `INFORMATION_SCHEMA.TABLES` on the WAL table at last processing |
| `SIGDS_TABLE_ID` | Snowflake internal table ID from `INFORMATION_SCHEMA.TABLES` |
| `SIGDS_TABLE_CREATED_AT` | Timestamp when the SIGDS table was first created |
| `SIGDS_TABLE_LAST_MODIFIED` | `LAST_ALTERED` from `INFORMATION_SCHEMA.TABLES` for the SIGDS table |
| `SIGDS_TABLE_SIZE_BYTES` | Active bytes from `INFORMATION_SCHEMA.TABLES` |
| `WORKBOOK_ID` | Sigma workbook or data model ID |
| `WORKBOOK_NAME / PATH` | Display name and folder path (from Sigma API) |
| `OBJECT_TYPE` | `WORKBOOK` or `DATA_MODEL` |
| `ORG_SLUG` | Sigma org slug parsed from the workbook URL |
| `IS_ORPHANED` | `TRUE` when the SIGDS data table no longer exists in Snowflake |
| `IS_DELETED` | `TRUE` when the WAL table has disappeared from the schema |
| `IS_LEGACY_WAL` | `TRUE` for old `SIGDS_WAL_<uuid>` naming (pre-DS_ID convention) |
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

`geninfo_queries.sql` covers the same analytical dimensions as `archival_scoring.sql` but as exploratory reporting views rather than a scoring engine. Replace `<YOUR_DATABASE>` and `<YOUR_SCHEMA>` before running.

| Query | What it shows |
|---|---|
| 1. Landscape overview | Per-schema summary: total tables, orphaned/deleted/archived counts, total and reclaimable storage (GB) |
| 2. Storage reclamation opportunity | All tables with a clear archival signal (orphaned, deleted, or archived workbook), ranked by size with the primary reason surfaced |
| 3. Active workbooks going stale | Active workbooks where writeback activity has dropped off, grouped into inactivity bands (31–90 / 91–180 / 181–365 / >365 days) |
| 4. Most active writeback tables | Highest-edit-volume input tables — the inverse archival view; useful for identifying business-critical tables before any cleanup nearby |
| 5. Owner accountability summary | Cleanup burden rolled up by workbook owner: archived, orphaned, stale counts and reclaimable GB per owner |
| 6. Workbooks with multiple input tables | Groups by source workbook (resolving tagged versions to their parent) to find workbooks with more than one named input element |
| 7. Legacy WAL inventory | All `SIGDS_WAL_<uuid>` tables, split by migration priority: active legacy WALs (still being written) flagged as urgent; inactive as low-priority |

---

## Archival Scoring (`archival_scoring.sql`)

Scores every record in `SIGDS_WORKBOOK_MAP` across six weighted signals to produce a ranked list of archival candidates.

### Scoring model (total = 100 pts, higher = stronger archival candidate)

| Dimension | Max | Logic |
|---|---|---|
| Archival / deletion status | 30 | `IS_ORPHANED`=TRUE → 30 / `IS_DELETED`=TRUE → 25 / `API_IS_ARCHIVED`=TRUE → 20 / workbook absent from API → 15 |
| WAL edit recency | 25 | >365 days (or NULL) → 25 / >180 → 18 / >90 → 10 / >30 → 4 |
| SIGDS table modification | 15 | >365 days (or NULL) → 15 / >180 → 10 / >90 → 5 |
| Edit volume (`WAL_MAX_EDIT_NUM`) | 10 | 0/NULL → 10 / ≤10 → 8 / ≤50 → 5 / ≤200 → 2 |
| Legacy WAL flag | 10 | Legacy + active (<180 days) → 10 / Legacy + inactive → 5 |
| Storage reclamation | 10 | >1 GB → 10 / >100 MB → 7 / >10 MB → 4 / else → 1 |

**Risk penalty:** `IS_TAGGED_VERSION` = TRUE → subtract 15 pts (floor at 0).

### Confidence tiers

| Score | Tier | Recommendation |
|---|---|---|
| ≥ 75 | **TIER 1** | Strong candidate — quarantine now |
| 50–74 | **TIER 2** | Likely candidate — review with owner |
| 25–49 | **TIER 3** | Monitor — check in 90 days |
| < 25 | **TIER 4** | Keep — active or protected |

> **Important — read before taking any action based on these scores.**
>
> The confidence tiers and weights in this model are entirely subjective. What constitutes an appropriate threshold for archival will vary significantly from customer to customer. The scores are a starting point for investigation, not a directive.
>
> **Incorrectly removing a SIGDS table or its associated WAL table can cause irreparable impact to the related Sigma content.** Always follow the safe deletion process (move to quarantine first, monitor, then delete) and ensure the record has been reviewed and approved by the workbook owner before any action is taken.

---

## Safe Deletion of SIGDS and WAL Tables

> **Best practice: move first, delete later — never drop directly.**

Sigma stores the exact fully-qualified table name of both the SIGDS data table and the WAL table in its internal metadata. If the table has been dropped or renamed, workbooks will immediately fail with errors.

### Recommended process

1. **Identify candidates** using `geninfo_queries.sql` or `archival_scoring.sql`.
2. **Move** the SIGDS and WAL tables to a quarantine schema using `ALTER TABLE ... RENAME TO`:
   ```sql
   ALTER TABLE <DATABASE>.<SCHEMA>.<TABLE>
     RENAME TO <DATABASE>.<SCHEMA>_QUARANTINE.<TABLE>;
   ```
3. **Monitor** for a minimum of 30 days to confirm no workbook errors are raised.
4. **Drop** the tables from the quarantine schema once the safe period has passed.

Recovery is straightforward if a table is moved rather than dropped — rename it back to its original location and the workbook resumes functioning immediately. A direct `DROP TABLE` is irreversible.
