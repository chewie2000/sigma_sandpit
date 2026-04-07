# writeback_info_dbx

A Databricks toolkit for inventorying and monitoring Sigma writeback (input table) activity across a Unity Catalog schema. It maps every active writeback WAL table to its Sigma workbook or data model, enriches records with Delta metadata and Sigma API ownership data, and populates a central `SIGDS_WORKBOOK_MAP` table for reporting and cleanup.

## Overview

When Sigma writebacks are enabled, Sigma creates a WAL table (`sigds_wal_*`) and a data table (`sigds_*`) in Databricks for each input table. Over time these accumulate — workbooks get archived, tables go stale, and orphaned WAL records are left behind. This toolkit provides visibility into that state so administrators can identify cleanup candidates and track the migration of writeback workbooks.

## Files

| File | Purpose |
|---|---|
| `create_sigds_workbook_map.sql` | DDL — creates the `SIGDS_WORKBOOK_MAP` table in Unity Catalog (run once) |
| `populate_sigds_workbook_map.py` | Main script — incrementally populates `SIGDS_WORKBOOK_MAP` from WAL tables and the Sigma API |
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

---

## How the populate script works

Each run follows these steps:

1. **Load watermarks** — reads stored `WAL_LAST_ALTERED` timestamps and known `WORKBOOK_ID`s from `SIGDS_WORKBOOK_MAP` in a single query.
2. **Discover WAL tables** — runs `SHOW TABLES` to find all `sigds_wal_*` tables in the schema.
3. **Skip unchanged WAL tables** — runs `DESCRIBE DETAIL` in parallel on every WAL table and compares `lastModified` against the stored watermark. WAL tables with no new writes are skipped entirely (no row scans).
4. **Extract latest WAL entries** — reads the most recent WAL row per SIGDS table from changed WAL tables using batched `UNION ALL` queries (one Spark job per batch of up to 100 tables).
5. **Delta metadata** — runs `DESCRIBE DETAIL` in parallel for each new or changed SIGDS table to capture table ID, location, size, and timestamps.
6. **Sigma API enrichment** — fetches workbook/data-model metadata only for `WORKBOOK_ID`s not already in the table. Resolves owner names via `GET /v2/members`. `api_is_archived` is re-checked on every run for all known IDs.
7. **Version tag resolution** — fetches all version tags via `GET /v2/tags`, then lists workbooks per tag to build a `taggedWorkbookId → parent workbook` mapping. Tagged version records are flagged with `IS_TAGGED_VERSION=TRUE`, `VERSION_TAG_NAME`, and `PARENT_WORKBOOK_ID`.
8. **MERGE** — writes all changes into `SIGDS_WORKBOOK_MAP` via a single `MERGE` keyed on `SIGDS_TABLE`. Records whose WAL table has disappeared are flagged `IS_DELETED=TRUE`; the flag is cleared if the WAL table reappears.

---

## SIGDS_WORKBOOK_MAP — key columns

| Column | Description |
|---|---|
| `SIGDS_TABLE` | Bare SIGDS table name — logical primary key |
| `WAL_TABLE` | Fully-qualified WAL table name |
| `DS_ID` | Input table dataset ID from the WAL record |
| `WORKBOOK_ID` | Sigma workbook or data model ID |
| `WORKBOOK_URL` | Direct URL to the workbook in Sigma |
| `WORKBOOK_NAME / PATH` | Display name and folder path (from Sigma API) |
| `OBJECT_TYPE` | `WORKBOOK` or `DATA_MODEL` |
| `LAST_EDIT_AT / BY` | Timestamp and user email of the most recent writeback edit |
| `TABLE_SIZE_BYTES` | Current on-disk size of the SIGDS Delta table |
| `WAL_LAST_ALTERED` | Watermark — `lastModified` of the WAL table at last processing |
| `IS_ORPHANED` | `TRUE` when the SIGDS data table no longer exists in Databricks |
| `IS_DELETED` | `TRUE` when the WAL table has disappeared from the schema |
| `IS_LEGACY_WAL` | `TRUE` for old `sigds_wal_<uuid>` naming (pre-DS_ID convention) |
| `IS_TAGGED_VERSION` | `TRUE` when the workbook ID is a version tag (e.g. Prod, QA) |
| `api_is_archived` | Archived state from Sigma API — refreshed every run |
| `api_owner_first_name / last_name` | Owner name resolved via Sigma API — set once on first discovery |

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
