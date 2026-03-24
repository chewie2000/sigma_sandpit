"""
populate_sigds_workbook_map.py

Incrementally populates SIGDS_WORKBOOK_MAP from Sigma writeback WAL tables
and the Sigma REST API.

Run logic per execution
-----------------------
1. Load stored WAL_LAST_ALTERED watermarks and known WORKBOOK_IDs from the
   target table (single query).
2. Discover all sigds_wal_* tables via SHOW TABLES (single query).
3. Run DESCRIBE DETAIL in parallel on every WAL table to read lastModified
   (metadata-only, no table scans).  Skip WAL tables whose lastModified has
   not changed since the stored watermark.
4. Extract the latest WAL entry per SIGDS table from changed WAL tables via
   batched UNION ALL queries (one Spark job per batch, not per table).
5. Run DESCRIBE DETAIL in parallel for each new/changed SIGDS table to
   capture Delta metadata (id, location, size, timestamps).
6. Fetch Sigma workbook/data-model metadata only for WORKBOOK_IDs not already
   present in the target table.
7. Merge all changes into SIGDS_WORKBOOK_MAP via a single MERGE statement
   keyed on SIGDS_TABLE.

Design notes
------------
- WAL_LAST_ALTERED stores the lastModified timestamp returned by DESCRIBE
  DETAIL on the WAL table.  On each run this value is compared against the
  stored watermark; WAL tables with no new writes are skipped without reading
  any row data.  DESCRIBE DETAIL is metadata-only and requires no special
  permissions beyond SELECT on the table.
- MAX_EDIT_NUM is the EDIT_NUM of the highest-numbered row for that SIGDS
  table; it is derived from the WAL extract (rn=1 row) with no extra queries.
- All DESCRIBE DETAIL calls (WAL tables and SIGDS tables) are parallelised
  via a shared thread pool — safe because DESCRIBE DETAIL does not scan data.
- Sigma API calls are skipped entirely when all WORKBOOK_IDs are already known.
"""

import base64
import concurrent.futures
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    LongType, StringType, StructField, StructType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Configuration — update before running
# ---------------------------------------------------------------------------
CATALOG          = "customer_success"   # Databricks Unity Catalog name
SCHEMA           = "marko_wb"           # Schema containing SIGDS + WAL tables
TARGET_TABLE     = f"{CATALOG}.{SCHEMA}.SIGDS_WORKBOOK_MAP"
SIGMA_API_BASE   = "https://api.eu.aws.sigmacomputing.com/v2"  # EU AWS endpoint
SIGMA_CLIENT_ID  = "<YOUR_SIGMA_CLIENT_ID>"
SIGMA_CLIENT_SECRET = "<YOUR_SIGMA_CLIENT_SECRET>"
MAX_WAL_TABLES   = 0   # 0 = all; set > 0 to cap WAL tables for testing
DESCRIBE_WORKERS = 16  # thread-pool size for all parallel DESCRIBE DETAIL calls
WAL_BATCH_SIZE   = 100 # max WAL tables per UNION ALL query
# ---------------------------------------------------------------------------

if SIGMA_CLIENT_ID.startswith("<YOUR_") or SIGMA_CLIENT_SECRET.startswith("<YOUR_"):
    raise ValueError("Set SIGMA_CLIENT_ID and SIGMA_CLIENT_SECRET before running.")


# ===========================================================================
# Sigma API helpers
# ===========================================================================

def get_sigma_token(client_id: str, client_secret: str) -> str:
    """Obtain a Sigma OAuth bearer token using the client credentials flow."""
    auth_b64 = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        f"{SIGMA_API_BASE}/auth/token",
        headers={
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Sigma token response did not contain access_token.")
    return token


def sigma_paginate(token: str, endpoint: str) -> list:
    """
    Fetch all pages from a Sigma list endpoint and return a flat list of items.
    Tries common root-key names (entries, workbooks, dataModels, data, items)
    to handle variation across Sigma API response shapes.
    """
    headers = {"Authorization": f"Bearer {token}"}
    items, params = [], {}
    while True:
        resp = requests.get(
            f"{SIGMA_API_BASE}/{endpoint}",
            headers=headers, params=params, timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        for key in ("entries", "workbooks", "dataModels", "data", "items"):
            chunk = data.get(key)
            if isinstance(chunk, list):
                items.extend(chunk)
                break
        next_page = data.get("nextPage")
        if not next_page:
            break
        params["page"] = next_page
    return items


def build_id_index(entries: list, target_ids: set) -> dict:
    """
    Index a list of Sigma API objects by the ID field that best overlaps
    with target_ids.  Inspects every key containing 'id' in the first entry
    and picks the one with the highest match count against target_ids.  This
    avoids hard-coding field names that differ between API versions.
    Returns {normalised_id_string: entry_dict}.
    """
    if not entries or not target_ids:
        return {}
    target_norm = {v.strip().lower() for v in target_ids}
    candidates  = [k for k in entries[0] if "id" in k.lower()] or ["id"]
    best_key    = max(
        candidates,
        key=lambda k: len(
            {e[k].strip().lower() for e in entries if e.get(k)} & target_norm
        ),
    )
    return {
        e[best_key].strip().lower(): e
        for e in entries if e.get(best_key)
    }


# ===========================================================================
# Databricks / Delta helpers
# ===========================================================================

def _describe_detail_fq(full_name: str) -> dict:
    """
    Run DESCRIBE DETAIL on a fully-qualified table name and return the raw row
    as a dict.  Returns an empty dict on failure.
    Used internally by both WAL-table discovery and SIGDS-table enrichment.
    """
    try:
        return spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0].asDict()
    except Exception as exc:
        print(f"  WARN: DESCRIBE DETAIL failed for {full_name}: {exc}")
        return {}


def get_wal_last_modified(full_wal_name: str) -> tuple:
    """
    Return (full_wal_name, lastModified) for a single WAL table.
    lastModified is the Delta transaction timestamp — a reliable, cheap signal
    that the table has received new writes, with no row data scanned.
    Returns (full_wal_name, None) on failure.
    """
    detail = _describe_detail_fq(full_wal_name)
    return full_wal_name, detail.get("lastModified")


def parallel_wal_last_modified(wal_names: list) -> dict:
    """
    Run get_wal_last_modified concurrently for all WAL tables.
    Returns {full_wal_name: lastModified_timestamp}.
    """
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=DESCRIBE_WORKERS) as pool:
        futures = {pool.submit(get_wal_last_modified, w): w for w in wal_names}
        for future in concurrent.futures.as_completed(futures):
            name, ts = future.result()
            results[name] = ts
    return results


def describe_sigds_table(table_name: str) -> dict:
    """
    Run DESCRIBE DETAIL for a single SIGDS table and return physical metadata.
    Accepts a bare table name (no catalog/schema prefix); CATALOG and SCHEMA
    constants are used to build the fully-qualified backtick-quoted identifier.

    Returns a dict with keys:
        TABLE_ID, TABLE_LOCATION, TABLE_CREATED_AT,
        TABLE_LAST_MODIFIED, TABLE_SIZE_BYTES.
    Returns an empty dict if the table does not exist or the call fails.
    """
    full   = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
    detail = _describe_detail_fq(full)
    if not detail:
        return {}
    return {
        "TABLE_ID":            str(detail["id"]) if detail.get("id") else None,
        "TABLE_LOCATION":      detail.get("location"),
        "TABLE_CREATED_AT":    detail.get("createdAt"),
        "TABLE_LAST_MODIFIED": detail.get("lastModified"),
        "TABLE_SIZE_BYTES":    detail.get("sizeInBytes"),
    }


def parallel_describe_sigds(table_names: list) -> dict:
    """
    Run describe_sigds_table concurrently across all table_names using a
    thread pool of size DESCRIBE_WORKERS.
    Returns {bare_table_name: detail_dict}.
    """
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=DESCRIBE_WORKERS) as pool:
        futures = {pool.submit(describe_sigds_table, t): t for t in table_names}
        for future in concurrent.futures.as_completed(futures):
            results[futures[future]] = future.result()
    return results


def extract_wal_records_batch(wal_batch: list) -> list:
    """
    Build and execute a single UNION ALL query across a batch of WAL tables.
    Returns the latest WAL entry per SIGDS table (by highest EDIT_NUM) as a
    list of Spark Row objects.

    Using UNION ALL over a batch means one Spark job per batch rather than
    one per WAL table, which is significantly faster at scale.

    Notes:
    - MAX_EDIT_NUM is taken from EDIT_NUM of the rn=1 row (the highest
      EDIT_NUM for that SIGDS table partition), so no separate MAX() query
      is needed.
    - Each WAL table is expected to contain entries for a single SIGDS table;
      the PARTITION BY handles edge cases where multiple tables appear.
    """
    parts = []
    for wal in wal_batch:
        parts.append(f"""
        SELECT
            '{wal}'   AS WAL_TABLE,
            EDIT_NUM  AS MAX_EDIT_NUM,
            DS_ID,
            TIMESTAMP AS LAST_EDIT_AT,
            get_json_object(METADATA, '$.tableName')            AS SIGDS_TABLE,
            get_json_object(METADATA, '$.workbookId')           AS WORKBOOK_ID,
            coalesce(
                get_json_object(METADATA, '$.sigmaUrl'),
                get_json_object(METADATA, '$.workbookUrl')
            )                                                   AS WORKBOOK_URL,
            coalesce(
                get_json_object(METADATA, '$.elementTitle'),
                get_json_object(METADATA, '$.inputTableTitle')
            )                                                   AS INPUT_TABLE_NAME,
            coalesce(
                get_json_object(METADATA, '$.userEmail'),
                get_json_object(EDIT, '$.updateRow.blameInfo.updatedBy'),
                get_json_object(EDIT, '$.addRow.blameInfo.updatedBy')
            )                                                   AS LAST_EDIT_BY,
            element_at(split(coalesce(
                get_json_object(METADATA, '$.sigmaUrl'),
                get_json_object(METADATA, '$.workbookUrl')
            ), '/'), 4)                                         AS ORG_SLUG,
            row_number() OVER (
                PARTITION BY get_json_object(METADATA, '$.tableName')
                ORDER BY EDIT_NUM DESC
            )                                                   AS rn
        FROM {wal}
        """)
    union_sql = "\nUNION ALL\n".join(parts)
    return spark.sql(f"""
        SELECT * EXCEPT(rn)
        FROM   ({union_sql})
        WHERE  rn = 1
          AND  SIGDS_TABLE IS NOT NULL
    """).collect()


# ===========================================================================
# Main
# ===========================================================================

# ---------------------------------------------------------------------------
# Step 1 — Authenticate with Sigma
# ---------------------------------------------------------------------------
sigma_token = get_sigma_token(SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET)
print("Step 1: Sigma token obtained.")

# ---------------------------------------------------------------------------
# Step 2 — Load stored watermarks and known WORKBOOK_IDs from target table
# ---------------------------------------------------------------------------
# WAL_LAST_ALTERED mirrors the DESCRIBE DETAIL lastModified of each WAL table
# at the time it was last processed.  Comparing it against the current
# lastModified tells us whether the WAL table has had new writes without
# reading any row data.
stored_rows = spark.sql(f"""
    SELECT WAL_TABLE, WAL_LAST_ALTERED, WORKBOOK_ID
    FROM   {TARGET_TABLE}
""").collect()

watermarks   = {}  # {wal_table -> most recent WAL_LAST_ALTERED stored}
known_wb_ids = set()
for row in stored_rows:
    wt, ts = row["WAL_TABLE"], row["WAL_LAST_ALTERED"]
    if ts and (wt not in watermarks or ts > watermarks[wt]):
        watermarks[wt] = ts
    if row["WORKBOOK_ID"]:
        known_wb_ids.add(row["WORKBOOK_ID"])

print(
    f"Step 2: Loaded watermarks for {len(watermarks)} WAL tables; "
    f"{len(known_wb_ids)} WORKBOOK_IDs already enriched."
)

# ---------------------------------------------------------------------------
# Step 3 — Discover WAL tables; pre-filter via parallel DESCRIBE DETAIL
# ---------------------------------------------------------------------------
# SHOW TABLES returns all tables in the schema in a single query.  We then
# run DESCRIBE DETAIL on every sigds_wal_* table in parallel to read their
# lastModified timestamps — metadata-only, no row scans, no special permissions
# beyond SELECT on the tables themselves.
wal_rows = (
    spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
         .filter("lower(tableName) LIKE 'sigds_wal%'")
         .collect()
)
all_wal_names = [
    f"`{CATALOG}`.`{SCHEMA}`.`{r.tableName}`"
    for r in wal_rows
]
if MAX_WAL_TABLES > 0:
    all_wal_names = all_wal_names[:MAX_WAL_TABLES]

print(
    f"Step 3: Discovered {len(all_wal_names)} WAL tables. "
    f"Running parallel DESCRIBE DETAIL to check for changes..."
)
wal_modified = parallel_wal_last_modified(all_wal_names)  # {full_name -> lastModified}

to_process = [
    (name, wal_modified[name])
    for name in all_wal_names
    if name not in watermarks
    or (wal_modified.get(name) and wal_modified[name] > watermarks[name])
]

print(
    f"Step 3: {len(to_process)} WAL tables require reprocessing; "
    f"{len(all_wal_names) - len(to_process)} unchanged and skipped."
)

if not to_process:
    print("SIGDS_WORKBOOK_MAP is already up to date. Nothing to do.")
    raise SystemExit(0)

# ---------------------------------------------------------------------------
# Step 4 — Extract latest WAL records for changed tables (batched UNION ALL)
# ---------------------------------------------------------------------------
wal_last_altered = {full: ts for full, ts in to_process}  # stored as watermark
wal_names        = [full for full, _ in to_process]
batches          = [
    wal_names[i : i + WAL_BATCH_SIZE]
    for i in range(0, len(wal_names), WAL_BATCH_SIZE)
]

new_records = []
for idx, batch in enumerate(batches, start=1):
    print(f"  Step 4: Extracting WAL batch {idx}/{len(batches)} ({len(batch)} tables)...")
    new_records.extend(extract_wal_records_batch(batch))

print(f"Step 4: Extracted {len(new_records)} new/updated SIGDS table records.")

# ---------------------------------------------------------------------------
# Step 5 — DESCRIBE DETAIL (parallel) for each new/updated SIGDS table
# ---------------------------------------------------------------------------
sigds_bare_names = [r["SIGDS_TABLE"] for r in new_records if r["SIGDS_TABLE"]]
print(
    f"Step 5: Running DESCRIBE DETAIL on {len(sigds_bare_names)} SIGDS tables "
    f"using {DESCRIBE_WORKERS} workers..."
)
detail_map = parallel_describe_sigds(sigds_bare_names)  # {bare_name: detail_dict}
print("Step 5: DESCRIBE DETAIL complete.")

# ---------------------------------------------------------------------------
# Step 6 — Sigma API enrichment for new WORKBOOK_IDs only
# ---------------------------------------------------------------------------
# Workbooks and data models already present in the target table are skipped;
# only IDs introduced by this run trigger an API call.
new_wb_ids = {
    r["WORKBOOK_ID"]
    for r in new_records
    if r["WORKBOOK_ID"] and r["WORKBOOK_ID"] not in known_wb_ids
}
wb_meta = {}  # {WORKBOOK_ID -> {WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE}}

if new_wb_ids:
    print(f"Step 6: Fetching Sigma metadata for {len(new_wb_ids)} new WORKBOOK_IDs...")
    wb_index = build_id_index(sigma_paginate(sigma_token, "workbooks"),   new_wb_ids)
    dm_index = build_id_index(sigma_paginate(sigma_token, "dataModels"),  new_wb_ids)
    for wid in new_wb_ids:
        norm  = wid.strip().lower()
        entry = wb_index.get(norm) or dm_index.get(norm)
        if not entry:
            continue
        raw_path = entry.get("path")
        wb_meta[wid] = {
            "WORKBOOK_NAME": entry.get("name"),
            "WORKBOOK_PATH": "/".join(raw_path) if isinstance(raw_path, list) else raw_path,
            "OBJECT_TYPE":   "WORKBOOK" if norm in wb_index else "DATA_MODEL",
        }
    print(f"Step 6: Resolved {len(wb_meta)} of {len(new_wb_ids)} new WORKBOOK_IDs.")
else:
    print("Step 6: No new WORKBOOK_IDs — Sigma API fetch skipped.")

# ---------------------------------------------------------------------------
# Step 7 — Assemble rows and MERGE into SIGDS_WORKBOOK_MAP
# ---------------------------------------------------------------------------
MERGE_SCHEMA = StructType([
    StructField("WAL_TABLE",            StringType(),    True),
    StructField("DS_ID",                StringType(),    True),
    StructField("SIGDS_TABLE",          StringType(),    True),
    StructField("WORKBOOK_ID",          StringType(),    True),
    StructField("WORKBOOK_URL",         StringType(),    True),
    StructField("ORG_SLUG",             StringType(),    True),
    StructField("INPUT_TABLE_NAME",     StringType(),    True),
    StructField("LAST_EDIT_AT",         TimestampType(), True),
    StructField("LAST_EDIT_BY",         StringType(),    True),
    StructField("WORKBOOK_NAME",        StringType(),    True),
    StructField("WORKBOOK_PATH",        StringType(),    True),
    StructField("OBJECT_TYPE",          StringType(),    True),
    StructField("TABLE_ID",             StringType(),    True),
    StructField("TABLE_LOCATION",       StringType(),    True),
    StructField("TABLE_CREATED_AT",     TimestampType(), True),
    StructField("TABLE_LAST_MODIFIED",  TimestampType(), True),
    StructField("TABLE_SIZE_BYTES",     LongType(),      True),
    StructField("MAX_EDIT_NUM",         LongType(),      True),
    StructField("WAL_LAST_ALTERED",     TimestampType(), True),
])

rows = []
for r in new_records:
    detail = detail_map.get(r["SIGDS_TABLE"], {})
    meta   = wb_meta.get(r["WORKBOOK_ID"],    {})
    rows.append((
        r["WAL_TABLE"],
        r["DS_ID"],
        r["SIGDS_TABLE"],
        r["WORKBOOK_ID"],
        r["WORKBOOK_URL"],
        r["ORG_SLUG"],
        r["INPUT_TABLE_NAME"],
        r["LAST_EDIT_AT"],
        r["LAST_EDIT_BY"],
        meta.get("WORKBOOK_NAME"),
        meta.get("WORKBOOK_PATH"),
        meta.get("OBJECT_TYPE"),
        detail.get("TABLE_ID"),
        detail.get("TABLE_LOCATION"),
        detail.get("TABLE_CREATED_AT"),
        detail.get("TABLE_LAST_MODIFIED"),
        detail.get("TABLE_SIZE_BYTES"),
        r["MAX_EDIT_NUM"],
        wal_last_altered.get(r["WAL_TABLE"]),
    ))

updates_df = spark.createDataFrame(rows, MERGE_SCHEMA)
updates_df.createOrReplaceTempView("_SIGDS_UPDATES")

spark.sql(f"""
    MERGE INTO {TARGET_TABLE} AS t
    USING _SIGDS_UPDATES AS s
      ON  t.SIGDS_TABLE = s.SIGDS_TABLE
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")
print(f"Step 7: MERGE complete — {len(rows)} rows upserted into {TARGET_TABLE}.")

# Sanity check — show most recently modified entries
spark.sql(f"""
    SELECT SIGDS_TABLE, WORKBOOK_NAME, OBJECT_TYPE,
           TABLE_SIZE_BYTES, TABLE_LAST_MODIFIED, MAX_EDIT_NUM, WAL_LAST_ALTERED
    FROM   {TARGET_TABLE}
    ORDER  BY TABLE_LAST_MODIFIED DESC NULLS LAST
    LIMIT  20
""").show(truncate=False)
