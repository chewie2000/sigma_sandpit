# writeback_info_dbx

A Databricks toolkit for inventorying and monitoring Sigma writeback (input table) activity across a Unity Catalog schema. It maps every active writeback WAL table to its Sigma workbook or data model, enriches records with Delta metadata and Sigma API ownership data, and populates a central `SIGDS_WORKBOOK_MAP` table for reporting and cleanup.

## Overview

When Sigma writebacks are enabled, Sigma creates a WAL table (`sigds_wal_*`) and a data table (`sigds_*`) in Databricks for each input table. Over time these accumulate — workbooks get archived, tables go stale, and orphaned WAL records are left behind. This toolkit provides visibility into that state so administrators can identify cleanup candidates and track the migration of writeback workbooks.

## Files

| File | Purpose |
|---|---|
| `create_sigds_workbook_map.sql` | DDL — creates the `SIGDS_WORKBOOK_MAP` table in Unity Catalog (run once) |
| `populate_sigds_workbook_map.py` | Main script — incrementally populates `SIGDS_WORKBOOK_MAP` from WAL tables and the Sigma API |
| `archival_scoring.sql` | Weighted confidence scoring matrix — scores every record across multiple signals to surface archival candidates |
| `helper_queries.sql` | Housekeeping queries — identifies stale, orphaned, archived, and legacy records for cleanup |
| `geninfo_queries.sql` | General information queries — same query set as helper_queries, for read-only reporting |

---

## Setup

### 1. Create the table (once)

Edit `create_sigds_workbook_map.sql` and replace the placeholders, then run in Databricks SQL or a notebook:

```sql
-- Replace before running:
USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;
```

### 2. Configure and run the populate script

Edit the configuration block at the top of `populate_sigds_workbook_map.py`:

```python
CATALOG             = "<YOUR_CATALOG>"
SCHEMA              = "<YOUR_SCHEMA>"
SIGMA_API_BASE      = "<YOUR_API_BASE_URL>/v2"
SIGMA_CLIENT_ID     = "<YOUR_SIGMA_CLIENT_ID>"
SIGMA_CLIENT_SECRET = "<YOUR_SIGMA_CLIENT_SECRET>"
```

Run the script from a Databricks notebook or job attached to a cluster with access to the Unity Catalog schema containing your `sigds_wal_*` and `sigds_*` tables.

### Multiple writeback schemas

`SIGDS_WORKBOOK_MAP` supports multiple writeback schemas in a single table. Every row is stamped with a `SOURCE_SCHEMA` column (defaults to `SCHEMA`) so results from different schemas remain distinguishable. The MERGE key is the composite `SIGDS_TABLE + SOURCE_SCHEMA`, preventing collisions when the same bare table name exists in more than one schema.

To cover multiple schemas, run the script once per schema — either manually or as separate tasks in a Databricks job:

```python
# Run 1 — production writeback schema
CATALOG       = "my_catalog"
SCHEMA        = "prod_writes"
SOURCE_SCHEMA = "prod_writes"   # optional label override, e.g. "prod"

# Run 2 — development writeback schema
CATALOG       = "my_catalog"
SCHEMA        = "dev_writes"
SOURCE_SCHEMA = "dev_writes"    # optional label override, e.g. "dev"
```

Both runs write to the same `SIGDS_WORKBOOK_MAP` table. All analysis queries (`archival_scoring.sql`, `helper_queries.sql`, `geninfo_queries.sql`) include `SOURCE_SCHEMA` in their output so you can filter or group by schema. Sigma API enrichment (workbook names, owner details) is shared across schemas — a `WORKBOOK_ID` seen in any previous run is not re-fetched.

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
8. **MERGE** — writes all changes into `SIGDS_WORKBOOK_MAP` via a single `MERGE` keyed on `SIGDS_TABLE`. Records whose WAL table has disappeared are flagged `IS_DELETED=TRUE`; the flag is cleared if the WAL table reappears.

---

## SIGDS_WORKBOOK_MAP — key columns

Column names use consistent prefixes to make the data source immediately obvious:
- **`WAL_`** — sourced from WAL row data or WAL table metadata
- **`SIGDS_`** — sourced from `DESCRIBE DETAIL` on the writeback data table
- **`API_`** — sourced from the Sigma REST API

| Column | Description |
|---|---|
| `SIGDS_TABLE` | Bare SIGDS table name — logical primary key |
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

Both `helper_queries.sql` and `geninfo_queries.sql` contain the same five queries. Replace `<YOUR_CATALOG>` and `<YOUR_SCHEMA>` before running.

| Query | What it finds |
|---|---|
| 1. Archived workbooks | SIGDS tables still present, workbook archived, no edits in 90+ days — safe DROP candidates |
| 2. Active workbooks (stale) | SIGDS tables still present, workbook active, no edits in 180+ days — review with owners |
| 3. Orphaned WAL records | SIGDS table dropped but WAL table still exists — WAL DROP candidates |
| 4. Stale legacy WAL tables | Old UUID-named WAL tables with no edits in 180+ days |
| 5. Status flag summary | Rollup of record counts and total storage by status flag combination |

---

## Archival Scoring (`archival_scoring.sql`)

Scores every record in `SIGDS_WORKBOOK_MAP` across eight weighted signals to produce a ranked list of archival candidates.

### Scoring model (total = 100 pts, higher = stronger archival candidate)

| Dimension | Max | Logic |
|---|---|---|
| Archival / deletion status | 30 | `IS_ORPHANED`=TRUE → 30 / `IS_DELETED`=TRUE → 25 / `API_IS_ARCHIVED`=TRUE → 20 |
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

1. **Identify candidates** using the helper queries (archived, orphaned, or stale records).
2. **Move** the SIGDS and WAL tables to a quarantine schema (e.g. `<SCHEMA>_quarantine`) using `ALTER TABLE ... RENAME TO`. Do not drop them yet.
3. **Monitor** for a safe period (recommended: 30 days minimum) to confirm no workbook errors are raised and no users report missing data.
4. **Drop** the tables from the quarantine schema once the safe period has passed.

### Why this matters

If a table is moved or renamed rather than dropped outright, recovery is straightforward — rename the table back to its original location (`<CATALOG>.<SCHEMA>.<SIGDS_TABLE_NAME>`) and the workbook resumes functioning immediately. A direct `DROP TABLE` is irreversible and eliminates this recovery path.

---

## Prerequisites

- Databricks cluster with Unity Catalog access and `SELECT` + `MODIFY` on the target catalog/schema
- Sigma API client credentials (admin scope recommended for full org visibility)
- `requests` available in the cluster environment (standard on Databricks Runtime)
