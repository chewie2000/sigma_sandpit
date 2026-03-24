"""
populate_sigds_workbook_map.py
Databricks notebook / job script.

Scans Sigma Integration Data Store (SIGDS) WAL tables and builds a
mapping from SIGDS tables to their parent Sigma workbooks/data models.

Option B archival detection
---------------------------
api_is_archived is populated exactly once, when a WORKBOOK_ID is first
seen.  It reflects the workbook's archived state at the moment of first
enrichment and is never refreshed unless the table is rebuilt from
scratch.  This avoids repeated API calls and keeps the column stable as
a discovery-time snapshot.

WAL table assumptions
---------------------
Each SIGDS output table has a companion WAL table named
<sigds_table_name>_WAL (suffix configurable via pipeline.wal_table_suffix).
The WAL table must expose at minimum:
  WORKBOOK_ID  STRING     -- Sigma workbook / data-model UUID
  ALTERED_AT   TIMESTAMP  -- row write timestamp
"""

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# dbutils shim (auto-injected in notebooks; needs import in job context)
# ---------------------------------------------------------------------------
try:
    dbutils  # noqa: F821
except NameError:
    from databricks.sdk.runtime import dbutils  # noqa: F821

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Configuration  (override via Databricks job / cluster spark conf)
# ---------------------------------------------------------------------------
SIGDS_CATALOG    = spark.conf.get("pipeline.sigds_catalog",    "main")
SIGDS_SCHEMA     = spark.conf.get("pipeline.sigds_schema",     "sigds")
TARGET_TABLE     = spark.conf.get("pipeline.target_table",     "main.sigds_ops.sigds_workbook_map")
SIGMA_BASE_URL   = spark.conf.get("pipeline.sigma_base_url",   "https://aws-api.sigmacomputing.com")
SECRET_SCOPE     = spark.conf.get("pipeline.secret_scope",     "sigma")
SECRET_CLIENT_ID = spark.conf.get("pipeline.secret_client_id", "client_id")
SECRET_SECRET    = spark.conf.get("pipeline.secret_secret",    "client_secret")
WAL_TABLE_SUFFIX = spark.conf.get("pipeline.wal_table_suffix", "_WAL")

# ---------------------------------------------------------------------------
# Sigma API helpers
# ---------------------------------------------------------------------------

def get_sigma_token(base_url, client_id, client_secret):
    resp = requests.post(
        f"{base_url}/v2/auth/token",
        data={
            "grant_type":    "client_credentials",
            "client_id":     client_id,
            "client_secret": client_secret,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def sigma_paginate(token, endpoint, base_url):
    """Fetch all pages from a Sigma v2 list endpoint; returns flat entry list."""
    url     = f"{base_url}/v2/{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    entries = []
    while url:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        # All Sigma v2 list endpoints use "entries" as the root key
        for key in ("entries", "workbooks", "dataModels", "members", "data", "items"):
            if key in body:
                entries.extend(body[key])
                break
        url = body.get("nextPage")
    return entries


def build_id_index(entries, target_ids):
    """
    Return {id.lower(): entry} for entries whose workbookId / dataModelId
    is present in target_ids (case-insensitive comparison).
    """
    target_norm = {t.strip().lower() for t in target_ids}
    index = {}
    for e in entries:
        eid = e.get("workbookId") or e.get("dataModelId") or e.get("documentId")
        if eid and eid.strip().lower() in target_norm:
            index[eid.strip().lower()] = e
    return index


# ===========================================================================
# Step 1 – Ensure target table exists
# ===========================================================================
print("Step 1: Ensuring target table exists...")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        SIGDS_TABLE             STRING    NOT NULL  COMMENT 'Fully-qualified SIGDS table name (merge key)',
        WAL_TABLE               STRING              COMMENT 'WAL table this record was sourced from',
        WAL_LAST_ALTERED        TIMESTAMP           COMMENT 'Watermark: latest ALTERED_AT seen in the WAL',
        WORKBOOK_ID             STRING              COMMENT 'Sigma workbook or data model UUID',
        WORKBOOK_NAME           STRING              COMMENT 'Display name from Sigma API',
        WORKBOOK_PATH           STRING              COMMENT 'Folder path from Sigma API',
        OBJECT_TYPE             STRING              COMMENT 'WORKBOOK or DATA_MODEL',
        api_url                 STRING              COMMENT 'Direct browser URL to the workbook/data model',
        api_owner_id            STRING              COMMENT 'Sigma member UUID of the workbook owner',
        api_is_archived         BOOLEAN             COMMENT 'Option B: set once on first enrichment; TRUE if archived at time of discovery',
        api_owner_first_name    STRING              COMMENT 'Owner first name resolved via GET /v2/members',
        api_owner_last_name     STRING              COMMENT 'Owner last name resolved via GET /v2/members'
    )
    USING DELTA
    COMMENT 'Maps SIGDS tables to their parent Sigma workbooks/data models.'
""")

print(f"Step 1: Target table ready: {TARGET_TABLE}")


# ===========================================================================
# Step 2 – Load existing state (watermarks + enrichment cache)
# ===========================================================================
print("Step 2: Loading existing watermarks and enrichment cache...")

stored_rows = spark.sql(f"""
    SELECT
        WAL_TABLE,
        WAL_LAST_ALTERED,
        WORKBOOK_ID,
        WORKBOOK_NAME,
        WORKBOOK_PATH,
        OBJECT_TYPE,
        api_url,
        api_owner_id,
        api_is_archived,
        api_owner_first_name,
        api_owner_last_name
    FROM {TARGET_TABLE}
""").collect()

watermarks       = {}  # {WAL_TABLE -> max WAL_LAST_ALTERED}
known_wb_ids     = set()
known_enrichment = {}  # {WORKBOOK_ID -> enrichment dict}

for row in stored_rows:
    wt, ts = row["WAL_TABLE"], row["WAL_LAST_ALTERED"]
    if ts and (wt not in watermarks or ts > watermarks[wt]):
        watermarks[wt] = ts
    wid = row["WORKBOOK_ID"]
    if wid:
        known_wb_ids.add(wid)
        if wid not in known_enrichment:
            known_enrichment[wid] = {
                "WORKBOOK_NAME":        row["WORKBOOK_NAME"],
                "WORKBOOK_PATH":        row["WORKBOOK_PATH"],
                "OBJECT_TYPE":          row["OBJECT_TYPE"],
                "api_url":              row["api_url"],
                "api_owner_id":         row["api_owner_id"],
                "api_is_archived":      row["api_is_archived"],
                "api_owner_first_name": row["api_owner_first_name"],
                "api_owner_last_name":  row["api_owner_last_name"],
            }

print(f"Step 2: {len(watermarks)} WAL watermark(s) loaded; {len(known_wb_ids)} known workbook ID(s).")


# ===========================================================================
# Step 3 – Discover WAL tables
# ===========================================================================
print(f"Step 3: Discovering WAL tables in {SIGDS_CATALOG}.{SIGDS_SCHEMA}...")

wal_tables = [
    row["full_name"]
    for row in spark.sql(f"""
        SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
        FROM   {SIGDS_CATALOG}.information_schema.tables
        WHERE  table_schema = '{SIGDS_SCHEMA}'
          AND  table_name   LIKE '%{WAL_TABLE_SUFFIX}'
          AND  table_type   = 'BASE TABLE'
        ORDER BY table_name
    """).collect()
]

print(f"Step 3: Found {len(wal_tables)} WAL table(s).")

if not wal_tables:
    print("Step 3: No WAL tables found — nothing to process.")
    dbutils.notebook.exit("No WAL tables found")  # noqa: F821


# ===========================================================================
# Step 4 – Scan WAL tables for records newer than watermark
# ===========================================================================
print("Step 4: Scanning WAL tables for new activity...")

new_records = []  # list of {SIGDS_TABLE, WAL_TABLE, WAL_LAST_ALTERED, WORKBOOK_ID}

for wal_table in wal_tables:
    # Derive the SIGDS table name by stripping the WAL suffix
    sigds_table = (
        wal_table[: -len(WAL_TABLE_SUFFIX)]
        if wal_table.upper().endswith(WAL_TABLE_SUFFIX.upper())
        else wal_table
    )

    wm = watermarks.get(wal_table)
    filter_clause = f"WHERE ALTERED_AT > TIMESTAMP '{wm.isoformat()}'" if wm else ""

    try:
        rows = spark.sql(f"""
            SELECT
                WORKBOOK_ID,
                MAX(ALTERED_AT) AS WAL_LAST_ALTERED
            FROM {wal_table}
            {filter_clause}
            GROUP BY WORKBOOK_ID
            HAVING WORKBOOK_ID IS NOT NULL
        """).collect()
    except Exception as exc:
        print(f"  WARNING: Could not scan {wal_table}: {exc}")
        continue

    for row in rows:
        new_records.append({
            "SIGDS_TABLE":      sigds_table,
            "WAL_TABLE":        wal_table,
            "WAL_LAST_ALTERED": row["WAL_LAST_ALTERED"],
            "WORKBOOK_ID":      row["WORKBOOK_ID"],
        })

print(f"Step 4: {len(new_records)} new/updated record(s) to process.")

if not new_records:
    print("Step 4: Nothing new — exiting early.")
    dbutils.notebook.exit("No new WAL activity")  # noqa: F821


# ===========================================================================
# Step 5 – Obtain Sigma API token
# ===========================================================================
print("Step 5: Obtaining Sigma API token...")

client_id     = dbutils.secrets.get(SECRET_SCOPE, SECRET_CLIENT_ID)  # noqa: F821
client_secret = dbutils.secrets.get(SECRET_SCOPE, SECRET_SECRET)     # noqa: F821
sigma_token   = get_sigma_token(SIGMA_BASE_URL, client_id, client_secret)

print("Step 5: Token obtained.")


# ===========================================================================
# Step 6 – Enrich new WORKBOOK_IDs via Sigma API  (Option B: first-seen only)
# ===========================================================================
new_wb_ids = {
    r["WORKBOOK_ID"]
    for r in new_records
    if r["WORKBOOK_ID"] and r["WORKBOOK_ID"] not in known_wb_ids
}

wb_meta = {}  # {WORKBOOK_ID -> enrichment dict} for freshly-fetched IDs

if new_wb_ids:
    print(f"Step 6: Fetching Sigma metadata for {len(new_wb_ids)} new WORKBOOK_ID(s)...")

    wb_index = build_id_index(
        sigma_paginate(sigma_token, "workbooks",   SIGMA_BASE_URL), new_wb_ids
    )
    dm_index = build_id_index(
        sigma_paginate(sigma_token, "datamodels",  SIGMA_BASE_URL), new_wb_ids
    )

    for wid in new_wb_ids:
        norm  = wid.strip().lower()
        entry = wb_index.get(norm) or dm_index.get(norm)
        if not entry:
            continue
        is_wb    = norm in wb_index
        raw_path = entry.get("path")
        wb_meta[wid] = {
            "WORKBOOK_NAME":        entry.get("name"),
            "WORKBOOK_PATH":        "/".join(raw_path) if isinstance(raw_path, list) else raw_path,
            "OBJECT_TYPE":          "WORKBOOK" if is_wb else "DATA_MODEL",
            "api_url":              entry.get("url"),
            "api_owner_id":         entry.get("ownerId"),
            # Option B: archived state captured once at discovery time.
            # Data models found in the active list are treated as not archived.
            "api_is_archived":      entry.get("isArchived", False) if is_wb else False,
            "api_owner_first_name": None,
            "api_owner_last_name":  None,
        }

    # Resolve owner display names via /v2/members
    owner_ids = {m["api_owner_id"] for m in wb_meta.values() if m.get("api_owner_id")}
    if owner_ids:
        print(f"Step 6: Fetching Sigma members to resolve {len(owner_ids)} owner ID(s)...")
        all_members  = sigma_paginate(sigma_token, "members", SIGMA_BASE_URL)
        member_index = {
            m["memberId"].strip().lower(): m
            for m in all_members
            if m.get("memberId")
        }
        for meta in wb_meta.values():
            oid = meta.get("api_owner_id")
            if oid:
                member = member_index.get(oid.strip().lower(), {})
                meta["api_owner_first_name"] = member.get("firstName")
                meta["api_owner_last_name"]  = member.get("lastName")

    resolved = sum(1 for m in wb_meta.values() if m.get("WORKBOOK_NAME"))
    print(f"Step 6: Resolved {resolved} of {len(new_wb_ids)} new WORKBOOK_ID(s).")
else:
    print("Step 6: No new WORKBOOK_IDs — Sigma API fetch skipped.")


# ===========================================================================
# Step 7 – Assemble merge DataFrame and upsert
# ===========================================================================
print("Step 7: Merging into target table...")

MERGE_SCHEMA = StructType([
    StructField("SIGDS_TABLE",             StringType(),    False),
    StructField("WAL_TABLE",               StringType(),    True),
    StructField("WAL_LAST_ALTERED",        TimestampType(), True),
    StructField("WORKBOOK_ID",             StringType(),    True),
    StructField("WORKBOOK_NAME",           StringType(),    True),
    StructField("WORKBOOK_PATH",           StringType(),    True),
    StructField("OBJECT_TYPE",             StringType(),    True),
    StructField("api_url",                 StringType(),    True),
    StructField("api_owner_id",            StringType(),    True),
    StructField("api_is_archived",         BooleanType(),   True),
    StructField("api_owner_first_name",    StringType(),    True),
    StructField("api_owner_last_name",     StringType(),    True),
])

rows = []
for r in new_records:
    wid = r["WORKBOOK_ID"]
    # Prefer freshly-fetched enrichment; fall back to cached known enrichment
    enrichment = (
        wb_meta.get(wid)
        if wid and wid in wb_meta
        else known_enrichment.get(wid, {})
        if wid
        else {}
    )
    rows.append((
        r["SIGDS_TABLE"],
        r["WAL_TABLE"],
        r["WAL_LAST_ALTERED"],
        wid,
        enrichment.get("WORKBOOK_NAME"),
        enrichment.get("WORKBOOK_PATH"),
        enrichment.get("OBJECT_TYPE"),
        enrichment.get("api_url"),
        enrichment.get("api_owner_id"),
        enrichment.get("api_is_archived"),
        enrichment.get("api_owner_first_name"),
        enrichment.get("api_owner_last_name"),
    ))

source_df = spark.createDataFrame(rows, MERGE_SCHEMA)
source_df.createOrReplaceTempView("_sigds_workbook_map_source")

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS target
    USING _sigds_workbook_map_source AS source
    ON target.SIGDS_TABLE = source.SIGDS_TABLE
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

print(f"Step 7: Merge complete — {len(rows)} row(s) processed.")


# ===========================================================================
# Step 8 – Sanity check
# ===========================================================================
print("Step 8: Post-merge sanity check...")

spark.sql(f"""
    SELECT
        COUNT(*)                                                    AS total_rows,
        COUNT(DISTINCT WORKBOOK_ID)                                 AS distinct_workbooks,
        COUNT(DISTINCT WAL_TABLE)                                   AS distinct_wal_tables,
        SUM(CASE WHEN OBJECT_TYPE = 'WORKBOOK'   THEN 1 ELSE 0 END) AS workbook_count,
        SUM(CASE WHEN OBJECT_TYPE = 'DATA_MODEL' THEN 1 ELSE 0 END) AS data_model_count,
        SUM(CASE WHEN api_is_archived = TRUE      THEN 1 ELSE 0 END) AS archived_count,
        SUM(CASE WHEN api_owner_id   IS NOT NULL  THEN 1 ELSE 0 END) AS rows_with_owner,
        SUM(CASE WHEN WORKBOOK_NAME  IS NULL      THEN 1 ELSE 0 END) AS unresolved_workbook_ids
    FROM {TARGET_TABLE}
""").show()

print("Done.")
