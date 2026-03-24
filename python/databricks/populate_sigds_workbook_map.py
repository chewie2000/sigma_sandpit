"""
populate_sigds_workbook_map.py

Incrementally populates SIGDS_WORKBOOK_MAP from Sigma writeback WAL tables
and the Sigma REST API.

Run logic per execution
-----------------------
1. Load stored WAL_LAST_ALTERED watermarks and known WORKBOOK_IDs from the
   target table (single query).
2. Query information_schema.tables for current last_altered timestamps of all
   sigds_wal_* tables (single query — no WAL data read at this stage).
3. Skip WAL tables whose last_altered has not changed since the last run;
   process only tables with new writes.
4. Extract the latest WAL entry per SIGDS table from changed WAL tables via
   batched UNION ALL queries (one Spark job per batch, not per table).
5. Run DESCRIBE DETAIL in parallel threads for each new/changed SIGDS table
   to capture Delta metadata (id, location, size, timestamps).
6. Fetch Sigma workbook/data-model metadata only for WORKBOOK_IDs not already
   present in the target table.
7. Merge all changes into SIGDS_WORKBOOK_MAP via a single MERGE statement
   keyed on SIGDS_TABLE.

Design notes
------------
- WAL_LAST_ALTERED (from information_schema) is the sole change-detection
  signal.  It is stored per row so the next run can compare without reading
  any WAL table that has not been written to.
- MAX_EDIT_NUM is the EDIT_NUM of the highest-numbered row for that SIGDS
  table; it is derived from the WAL extract (rn=1 row) with no extra queries.
- DESCRIBE DETAIL is metadata-only and safe to parallelise across threads.
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
DESCRIBE_WORKERS = 16  # thread-pool size for parallel DESCRIBE DETAIL calls
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

def describe_table_detail(table_name: str) -> dict:
    """
    Run DESCRIBE DETAIL for a single Delta table and return physical metadata.
    Accepts a bare table name (no catalog/schema prefix); CATALOG and SCHEMA
    constants are used to build the fully-qualified backtick-quoted identifier.

    Returns a dict with keys:
        TABLE_ID, TABLE_LOCATION, TABLE_CREATED_AT,
        TABLE_LAST_MODIFIED, TABLE_SIZE_BYTES.
    Returns an empty dict if the table does not exist or the call fails.
    """
    full = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
    try:
        row = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
        return {
            "TABLE_ID":            str(row["id"]) if row["id"] else None,
            "TABLE_LOCATION":      row["location"],
            "TABLE_CREATED_AT":    row["createdAt"],
            "TABLE_LAST_MODIFIED": row["lastModified"],
            "TABLE_SIZE_BYTES":    row["sizeInBytes"],
        }
    except Exception as exc:
        print(f"  WARN: DESCRIBE DETAIL failed for {full}: {exc}")
        return {}


def parallel_describe_detail(table_names: list) -> dict:
    """
    Run describe_table_detail concurrently across all table_names using a
    thread pool of size DESCRIBE_WORKERS.  DESCRIBE DETAIL is metadata-only
    and does not scan table data, making it safe to parallelise.
    Returns {bare_table_name: detail_dict}.
    """
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=DESCRIBE_WORKERS) as pool:
        futures = {pool.submit(describe_table_detail, t): t for t in table_names}
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
# WAL_LAST_ALTERED mirrors information_schema.last_altered at the time each
# WAL table was last processed.  Comparing it against the current last_altered
# tells us whether the WAL table has had new writes without reading any WAL data.
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
# Step 3 — Discover WAL tables; pre-filter via information_schema.last_altered
# ---------------------------------------------------------------------------
# A single information_schema query returns current last_altered for every
# sigds_wal_* table.  Only tables whose last_altered exceeds the stored
# watermark (or which are newly discovered) proceed to WAL extraction.
info_rows = spark.sql(f"""
    SELECT table_name,
           last_altered
    FROM   {CATALOG}.information_schema.tables
    WHERE  table_schema = '{SCHEMA}'
      AND  lower(table_name) LIKE 'sigds_wal%'
""").collect()

all_wal = [
    (f"{CATALOG}.{SCHEMA}.{r.table_name}", r.last_altered)
    for r in info_rows
]
if MAX_WAL_TABLES > 0:
    all_wal = all_wal[:MAX_WAL_TABLES]

to_process = [
    (full_name, last_altered)
    for full_name, last_altered in all_wal
    if full_name not in watermarks
    or (last_altered and last_altered > watermarks[full_name])
]

print(
    f"Step 3: Discovered {len(all_wal)} WAL tables. "
    f"{len(to_process)} require reprocessing; "
    f"{len(all_wal) - len(to_process)} unchanged and skipped."
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
    f"Step 5: Running DESCRIBE DETAIL on {len(sigds_bare_names)} tables "
    f"using {DESCRIBE_WORKERS} workers..."
)
detail_map = parallel_describe_detail(sigds_bare_names)  # {bare_name: detail_dict}
print("Step 5: DESCRIBE DETAIL complete.")

# ---------------------------------------------------------------------------
# Step 6 — Sigma API enrichment for new WORKBOOK_IDs only
# ---------------------------------------------------------------------------
# Workbooks and data models already present in the target table are skipped;
# only IDs introduced by this run require an API call.
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
