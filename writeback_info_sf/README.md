# writeback_info_sf

> **IN DEVELOPMENT — Do not use.** This toolkit is under active development. Content may be incomplete, incorrect, or subject to breaking changes without notice. The v2 changes below have **not yet been run against a live Snowflake account** (the Python compiles and the SQL files split cleanly, nothing more) — treat first runs as a test.

A standalone, **read-only information tool** for Sigma writeback (input table) estates on Snowflake. It answers three questions about every `SIGDS_*` table:

1. **Whose is it?** — maps each WAL / data table to its Sigma workbook or data model, owner, folder, and archived / deleted state.
2. **Is anyone using it?** — combines the WAL (who *edits*) with Snowflake query history (who *reads*), so a table that is read all day but never edited is not mistaken for a dead one.
3. **What could be reclaimed?** — a transparent, tunable score and tier per table, plus reclaimable storage including Time Travel / Fail-safe.

It never drops, renames, or alters writeback tables. It needs no ACCOUNTADMIN, no external access integration, and no other toolkit — just a Python runner, a Snowflake role, and Sigma API credentials.

## Overview

When Sigma writebacks are enabled, Sigma creates a WAL table (`SIGDS_WAL_*`) and a data table (`SIGDS_*`) in Snowflake for each input table. Over time these accumulate — workbooks get archived or deleted, tables go stale, WAL records are orphaned. This toolkit gives administrators visibility into that state so cleanup can be planned safely.

## Files

| File | Purpose |
|---|---|
| `populate_sigds_workbook_map.py` | The runner. `--init` creates objects; a normal run scans, enriches, loads the access log, and snapshots scores. Configured by environment variables. |
| `create_sigds_workbook_map.sql` | DDL: `SIGDS_WORKBOOK_MAP`, `SIGDS_ACCESS_LOG`, `SIGDS_ACCESS_LOAD_RUNS`, `SIGDS_SCORE_HISTORY`. Idempotent; upgrades pre-v2 maps in place. |
| `scoring_views.sql` | The scoring model, in **one place**: `SIGDS_SCORING_CONFIG` (tunable weights), `V_SIGDS_ACCESS_SUMMARY`, `V_SIGDS_ARCHIVAL_SCORE`, `V_SIGDS_TIER_SUMMARY`. |
| `archival_scoring.sql` | Read-only queries over the scoring views, plus the safe-deletion procedure. |
| `geninfo_queries.sql` | Exploratory reports: landscape, reclamation, staleness, owners, multi-table workbooks, legacy WALs, read-only consumers, tier movement, quarantine watch. |

---

## Setup

### Prerequisites

- Python 3.8+ with `snowflake-connector-python` and `requests`:
  ```
  pip install snowflake-connector-python requests
  ```
- A Snowflake role with `SELECT` on the writeback schema(s) and `CREATE TABLE` / `CREATE VIEW` + DML on the schema that will hold the map.
- **Recommended — for access evidence:** Snowflake **Enterprise Edition** or higher, and
  ```sql
  GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <YOUR_ROLE>;
  ```
  This unlocks `ACCOUNT_USAGE.ACCESS_HISTORY` / `QUERY_HISTORY` (who reads and writes each table) and `TABLE_STORAGE_METRICS` (Time Travel / Fail-safe bytes). Without it the tool still works, but scores reflect **writes only** and nothing can reach TIER 1 (see [Access evidence](#access-evidence-and-the-safety-cap)).
- Sigma API client credentials with **Admin** scope: *Administration → Developer Access → Create New*. Admin scope is what lets the tool list **every** workbook in the org (`skipPermissionCheck`); without it, "workbook not found" cannot be trusted and the tool turns that signal off for the run. [Docs](https://help.sigmacomputing.com/reference/generate-client-credentials).

### Configuration — environment variables

Nothing secret lives in the source any more.

| Variable | Required | Meaning |
|---|---|---|
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE` | yes | Connection. Account e.g. `xy12345.us-east-1`. |
| `SNOWFLAKE_PASSWORD` **or** `SNOWFLAKE_AUTHENTICATOR` (e.g. `externalbrowser`) **or** `SNOWFLAKE_PRIVATE_KEY_PATH` (+ `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`) | one | Authentication. Key-pair / SSO preferred over a password. |
| `SIGDS_DATABASE` | yes | Database containing the writeback schema(s). |
| `SIGDS_SCAN_SCHEMAS` | yes | **Comma-separated** writeback schemas to scan in one run, e.g. `PROD_WRITES,DEV_WRITES`. |
| `SIGDS_MAP_SCHEMA` | no | Where the map/log tables live. Default: first scan schema. |
| `SIGMA_BASE_URL` | yes | e.g. `https://aws-api.sigmacomputing.com` (`/v2` optional). Table below. |
| `SIGMA_CLIENT_ID`, `SIGMA_CLIENT_SECRET` | yes | Sigma API credentials. |
| `SIGDS_BACKFILL_DAYS` | no | First access-log load look-back. Default 90 (QUERY_HISTORY keeps 365). |
| `SIGDS_OVERLAP_HOURS` | no | Re-scan buffer for ACCESS_HISTORY latency (~3 h). Default 12. |
| `SIGDS_MAX_WAL_TABLES` | no | Cap WAL tables for testing; disables deletion detection. |

Database and schema names are treated as unquoted (upper-cased) identifiers.

| Cloud / Region | `SIGMA_BASE_URL` |
|---|---|
| AWS US | `https://aws-api.sigmacomputing.com` |
| AWS EU | `https://api.eu.aws.sigmacomputing.com` |
| Azure US | `https://api.us.azure.sigmacomputing.com` |
| GCP US | `https://api.us.gcp.sigmacomputing.com` |

### Run — first time, step by step

The database and the map schema (`SIGDS_MAP_SCHEMA`) must **already exist** — `--init` creates tables and views in them, not the database or schema themselves. Your role needs `CREATE TABLE` / `CREATE VIEW` there.

```bash
# 1. Install dependencies (a virtualenv is a good idea)
pip install snowflake-connector-python requests

# 2. Configure — example values; keep these out of git
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=you@example.com
export SNOWFLAKE_ROLE=SYSADMIN
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_AUTHENTICATOR=externalbrowser     # or SNOWFLAKE_PASSWORD / SNOWFLAKE_PRIVATE_KEY_PATH
export SIGDS_DATABASE=MY_DB
export SIGDS_SCAN_SCHEMAS=WRITEBACK                # comma-separate several: WRITEBACK,WRITEBACK_DEV
export SIGDS_MAP_SCHEMA=WRITEBACK                  # optional; defaults to the first scan schema
export SIGMA_BASE_URL=https://aws-api.sigmacomputing.com
export SIGMA_CLIENT_ID=...
export SIGMA_CLIENT_SECRET=...

# 3. Once: create the tables and scoring views (safe to re-run; keeps your config tuning)
python populate_sigds_workbook_map.py --init

# 4. Scan. Repeat daily/weekly — the access-log load is incremental
python populate_sigds_workbook_map.py
```

Flags: `--init` (create objects, then exit) and `--skip-access-log` (skip the query-history load, e.g. if the grant isn't in place yet). If the tables don't exist the script stops and tells you to run `--init`.

A successful scan prints, in order: Sigma token → per-schema WAL/MERGE counts → Sigma enrichment counts → access-log window (or a `SKIPPED` reason) → score snapshot and a tier summary. The first access-log load backfills 90 days and can take a while on a busy account.

### Then look at the results

Open `archival_scoring.sql` and `geninfo_queries.sql` in Snowsight (or `snow sql -f`) and replace the placeholders **by hand** — `--init` only substitutes them in the two setup files:

| Placeholder | Where | Replace with |
|---|---|---|
| `<YOUR_DATABASE>` | both files | your `SIGDS_DATABASE` |
| `<YOUR_SCHEMA>` | both files | your `SIGDS_MAP_SCHEMA` |
| `<WRITEBACK_SCHEMA>` | `geninfo_queries.sql`, query 10 only | the writeback schema you quarantined tables from |

Start with `archival_scoring.sql` query 3 (is access evidence loaded?), then query 2 (tier summary), then query 1 (ranked candidates).

### If something goes wrong

| Symptom | Likely cause |
|---|---|
| `Missing required environment variable: …` | An export above is missing in this shell. |
| `Tables not found … Run with --init first.` | `--init` hasn't run, or `SIGDS_MAP_SCHEMA` points somewhere else than at `--init` time. |
| `SKIPPED: cannot read … ACCESS_HISTORY` | Not Enterprise Edition, or the `IMPORTED PRIVILEGES` grant is missing. The rest still works; scores are write-only and TIER 1 is capped. |
| `WARN: org-wide workbook list refused` | The Sigma credential isn't Admin scope. Enrichment still runs but `API_NOT_FOUND` is off for the run. |
| `WARN: no WAL tables visible but N previously known` | The role lost `USAGE`/`SELECT` on the writeback schema. Nothing was flagged deleted. |
| `Discovered 0 WAL table(s)` on a first run | Wrong `SIGDS_SCAN_SCHEMAS`, or the role can't see the schema. |

Run it daily or weekly: the access-log load is incremental, and score history only becomes useful with more than one snapshot.

---

## How a run works

1. **Scan each schema** — discovers `SIGDS_WAL_*` tables from `INFORMATION_SCHEMA.TABLES`, reads the latest WAL entry per SIGDS table (batched `UNION ALL`, 100 tables per query), attaches physical metadata (size, created, last altered; Time Travel + Fail-safe from `TABLE_STORAGE_METRICS` when readable), and MERGEs into `SIGDS_WORKBOOK_MAP`.
   **Every row is refreshed on every run.** (The pre-v2 script only updated rows whose WAL edit number had grown, so size, owner, and archive state silently froze.)
2. **Flag what vanished** — WAL tables gone from the schema → `IS_DELETED`; SIGDS data tables gone → `IS_ORPHANED`. If *zero* WAL tables are visible but some were known, the tool refuses to flag deletions (almost certainly a grants problem, not a mass deletion).
3. **Enrich from Sigma** — one fetch of workbooks (org-wide), data models, version tags, and members; then refresh name, path, owner, archived state, and tagged-version status for **every** known `WORKBOOK_ID`. A workbook the API no longer returns is flagged `API_NOT_FOUND` (previously this could only fire for IDs that had *never* been enriched). Not set for data models — their API list is visibility-scoped.
4. **Load the access log** — see below.
5. **Snapshot scores** into `SIGDS_SCORE_HISTORY` and print a tier summary.

### Access evidence and the safety cap

Sigma appends a JSON comment (`-- Sigma Σ {...}`) to every warehouse query it generates, carrying the source workbook URL and the acting user's email. The tool joins `QUERY_HISTORY` × `ACCESS_HISTORY`, keeps those queries that touched a scanned writeback schema, and lands one row per (query, table, `READ`/`INSERT`) in `SIGDS_ACCESS_LOG`.

This matters because the WAL only records *edits*. An input table that dashboards read daily but nobody edits looks abandoned from the WAL alone; query history shows it is live. In scoring, recent access applies a large penalty, and **absence of access is never treated as proof of abandonment** — it just removes a protection.

Each row carries `ACCESS_EVIDENCE`:

| Value | Meaning |
|---|---|
| `RECENT` | Read/written within `ACCESS_RECENT_DAYS` (90) — large penalty |
| `OLDER` | Last access within `ACCESS_OLDER_DAYS` (365) — small penalty |
| `NONE_IN_WINDOW` | Log loaded, nothing seen — no penalty |
| `NOT_LOADED` | Access log never loaded — **score reflects writes only** |

**Coverage cap:** until `SIGDS_ACCESS_LOAD_RUNS` shows successfully-loaded history covering `MIN_ACCESS_COVERAGE_DAYS` (default 90), no table can reach TIER 1 ("quarantine now") — it is held at TIER 2 with an explanatory `SCORE_NOTE`. (Already-`IS_ORPHANED` rows are exempt; their data table is gone, so there is nothing left to break.) Coverage is derived from the loader's own run log, not from the log's earliest row, because a table nobody touched leaves no rows to prove how long it has been watched.

**Caveats:** `ACCESS_HISTORY` lags up to ~3 h (hence the overlap window); `ACCOUNT_USAGE` retains 365 days (the log keeps accumulating beyond that); it only sees the **Snowflake account the tool runs in** — writeback by another account is invisible; and only `SELECT` / `INSERT` statements are captured. Reads by tools other than Sigma are not attributed.

### What changed from v1

| | v1 | v2 |
|---|---|---|
| Activity signal | WAL edits only | WAL edits **+ query-history reads/writes** |
| Row refresh | Only rows with a higher WAL edit number | Every row, every run |
| "Workbook deleted" detection | Only never-enriched IDs | Every known ID, every run (`API_NOT_FOUND`) |
| Owner / name / path | Set once, never refreshed | Refreshed every run |
| Scoring logic | Copy-pasted in two places | One view, weights in `SIGDS_SCORING_CONFIG` |
| History | Overwritten in place | `SIGDS_SCORE_HISTORY` snapshot per run |
| Storage | Active bytes of the data table | Data table incl. Time Travel/Fail-safe + WAL table |
| Credentials | Plaintext constants in the script | Environment variables; key-pair / SSO supported |
| Schemas | Edit the script, re-run per schema | `SIGDS_SCAN_SCHEMAS=A,B,C` |
| Setup | Edit placeholders in SQL by hand | `--init` |

---

## SIGDS_WORKBOOK_MAP — key columns

Prefixes show the data source:
- **`WAL_`** — WAL row data or WAL table metadata
- **`SIGDS_`** — `INFORMATION_SCHEMA` / storage metrics for the writeback data table
- **`API_`** — Sigma REST API

| Column | Description |
|---|---|
| `SIGDS_TABLE`, `SCAN_SCHEMA` | Composite primary key |
| `WAL_TABLE_FQN` | Fully-qualified WAL table name |
| `WAL_DS_ID`, `WAL_WORKBOOK_URL`, `WAL_INPUT_TABLE_NAME` | From the latest WAL record |
| `WAL_LAST_EDIT_AT` / `_BY` / `WAL_MAX_EDIT_NUM` | Latest WAL edit time, editor, highest edit number |
| `WAL_TABLE_LAST_MODIFIED`, `WAL_TABLE_SIZE_BYTES` | WAL table `LAST_ALTERED` and active bytes |
| `SIGDS_TABLE_ID` / `_CREATED_AT` / `_LAST_MODIFIED` | From `INFORMATION_SCHEMA.TABLES` (`LAST_ALTERED` tracks DDL, not edits) |
| `SIGDS_TABLE_SIZE_BYTES` | Active bytes |
| `SIGDS_TABLE_TOTAL_BYTES` | Active + Time Travel + Fail-safe (NULL if `ACCOUNT_USAGE` unreadable) |
| `WORKBOOK_ID` / `_NAME` / `_PATH`, `OBJECT_TYPE` | Sigma workbook or data model (`WORKBOOK` / `DATA_MODEL`) |
| `ORG_SLUG` | Org slug parsed from the WAL workbook URL |
| `IS_ORPHANED` | SIGDS data table no longer exists |
| `IS_DELETED` / `DELETED_AT` | WAL table gone from the schema (auto-cleared if it reappears) |
| `IS_LEGACY_WAL` | Old `SIGDS_WAL_<uuid>` naming |
| `IS_TAGGED_VERSION`, `VERSION_TAG_NAME`, `PARENT_WORKBOOK_ID` | Version-tag (Prod/QA) resolution |
| `API_OWNER_ID` / `_FIRST_NAME` / `_LAST_NAME` | Owner, refreshed each run |
| `API_IS_ARCHIVED` | Archived in Sigma, refreshed each run |
| `API_NOT_FOUND` | Not returned by the API on the latest run (workbooks only) |
| `API_LAST_SEEN_AT`, `LAST_SCANNED_AT` | Last API sighting / last scan (UTC) |

Access data lives in `SIGDS_ACCESS_LOG` and is rolled up by `V_SIGDS_ACCESS_SUMMARY` (last read, last write, counts, distinct users, last source workbook URL and user).

---

## Scoring

All logic is in `V_SIGDS_ARCHIVAL_SCORE` (`scoring_views.sql`). Weights, bands, tier cut-offs, and the access penalty are **rows in `SIGDS_SCORING_CONFIG`** — tune with `UPDATE`, no SQL edits:

```sql
UPDATE SIGDS_SCORING_CONFIG SET WAL_DAYS_1 = 540, TIER1_MIN = 80 WHERE CONFIG_ID = 1;
```

### Model (positive components total 100; higher = stronger archival candidate)

| Dimension | Max | Logic (defaults) |
|---|---|---|
| Status | 30 | orphaned 30 / WAL deleted 25 / workbook archived 20 / workbook not in API 15 |
| WAL edit recency | 25 | >365 d or never → 25 / >180 → 18 / >90 → 10 / >30 → 4 |
| SIGDS table altered | 15 | >365 d → 15 / >180 → 10 / >90 → 5 (weak: `LAST_ALTERED` is DDL) |
| Edit volume | 10 | 0 → 10 / ≤10 → 8 / ≤50 → 5 / ≤200 → 2 (calibrate to your org) |
| Legacy WAL | 10 | legacy & active (<180 d) → 10 / legacy & inactive → 5 |
| Storage | 10 | >1 GB → 10 / >100 MB → 7 / >10 MB → 4 / else 1 |

| Penalty | Default |
|---|---|
| Tagged version (Prod/QA) | −15 |
| Access within 90 days (`RECENT`) | **−40** |
| Last access 91–365 days (`OLDER`) | −15 |

Score is floored at 0 and capped at 100.

### Tiers

| Score | Tier | Recommendation |
|---|---|---|
| ≥ 75 | **TIER 1** | Strong candidate — quarantine now *(only once access coverage ≥ 90 days)* |
| 50–74 | **TIER 2** | Likely candidate — review with owner |
| 25–49 | **TIER 3** | Monitor — check in 90 days |
| < 25 | **TIER 4** | Keep — active or protected |

> **Important — read before taking any action based on these scores.**
>
> The weights and tiers are entirely subjective. What counts as an appropriate threshold varies significantly from customer to customer. Scores are a starting point for investigation, not a directive. Note too that this tool cannot see readers outside the Snowflake account it runs in, or anything not issued by Sigma.
>
> **Incorrectly removing a SIGDS table or its WAL table can cause irreparable impact to the related Sigma content.** Always follow the safe deletion process below and have the workbook owner approve before any action.

## Analysis queries (`geninfo_queries.sql`)

| Query | What it shows |
|---|---|
| 1. Landscape overview | Per-schema totals: orphaned / deleted / archived counts, total and reclaimable GB |
| 2. Storage reclamation opportunity | Tables with a clear archival signal, ranked by size |
| 3. Active workbooks going stale | Inactivity bands (31–90 / 91–180 / 181–365 / >365 days) |
| 4. Most active writeback tables | Highest edit volume — the inverse view; business-critical tables |
| 5. Owner accountability | Cleanup burden per workbook owner |
| 6. Workbooks with multiple input tables | Resolves tagged versions to their parent workbook |
| 7. Legacy WAL inventory | `SIGDS_WAL_<uuid>` tables, active (urgent) vs inactive |
| 8. Read-only consumers | **New.** Live tables with recent reads but no recent edits — the false positives a write-only view would produce |
| 9. Tier movement | **New.** Tables whose tier changed since the previous run |
| 10. Quarantine watch | **New.** Failed Sigma queries (error 2003) against SIGDS names during a quarantine period |

---

## Safe Deletion of SIGDS and WAL Tables

> **Best practice: move first, delete later — never drop directly.** This tool reports; it does not remediate.

Sigma stores the exact fully-qualified name of both the SIGDS data table and the WAL table in its metadata. If either is dropped or renamed, workbooks fail immediately.

1. **Identify candidates** with `archival_scoring.sql`. Prefer TIER 1/2 rows whose `ACCESS_EVIDENCE` is `NONE_IN_WINDOW`, and check `SCORE_NOTE`.
2. **Move** *both* the SIGDS and WAL tables to a quarantine schema:
   ```sql
   ALTER TABLE <DATABASE>.<SCHEMA>.<TABLE>
     RENAME TO <DATABASE>.<SCHEMA>_QUARANTINE.<TABLE>;
   ```
3. **Monitor ≥ 30 days.** Run `geninfo_queries.sql` query 10 — any failed Sigma query against a moved name means something still depended on it: rename it straight back.
4. **Drop** from quarantine only after a clean period and owner sign-off.

Recovery is simple when a table was moved rather than dropped — rename it back and the workbook resumes. A direct `DROP TABLE` is irreversible.

## Known limitations

- **Untested against a live account** as of this revision — see the note at the top.
- Access evidence needs Enterprise Edition + `IMPORTED PRIVILEGES`; single Snowflake account only; `SELECT`/`INSERT` only; relies on Sigma's query-comment format.
- A data model missing from the Sigma API is never flagged `API_NOT_FOUND` (its list is visibility-scoped), and a non-admin token disables the signal for workbooks too.
- Changing `SIGDS_SCAN_SCHEMAS` casing between runs creates separate rows (names are upper-cased; pre-v2 rows stored with a different case will not merge).
- Writeback to Databricks is out of scope here — see `writeback_info_dbx`.
