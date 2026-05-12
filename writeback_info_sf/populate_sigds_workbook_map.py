"""
populate_sigds_workbook_map.py  (Snowflake)

Incrementally populates SIGDS_WORKBOOK_MAP from Sigma writeback WAL tables
and the Sigma REST API.

Run logic per execution
-----------------------
1. Authenticate with Sigma to obtain a bearer token.
2. Connect to Snowflake; load stored watermarks (WAL_MAX_EDIT_NUM per WAL table),
   known WORKBOOK_IDs, and enrichment cache from SIGDS_WORKBOOK_MAP.
3. Discover all SIGDS_WAL_* tables via INFORMATION_SCHEMA.TABLES (single query).
   Detect tables that have been deleted from the schema since the last run.
4. Extract the latest WAL entry per SIGDS table from all WAL tables via batched
   UNION ALL queries (one Snowflake query per batch of up to WAL_BATCH_SIZE tables).
5. Get SIGDS table metadata (size, timestamps) from INFORMATION_SCHEMA.TABLES
   in a single query for all discovered SIGDS table names.
6. Fetch Sigma workbook/data-model metadata only for WORKBOOK_IDs not already
   present in the target table.  Resolve owner names via /v2/members.
   Re-check API_IS_ARCHIVED for all known WORKBOOK_IDs on every run.
7. Assemble rows and MERGE into SIGDS_WORKBOOK_MAP via a temp staging table.
   Flag records whose WAL table has disappeared as IS_DELETED=TRUE.
   Flag records whose SIGDS data table no longer exists as IS_ORPHANED=TRUE.

Design notes
------------
- Unlike the Databricks version which uses DESCRIBE DETAIL (metadata-only, no
  table scan) to watermark WAL tables, Snowflake's INFORMATION_SCHEMA.TABLES
  tracks LAST_ALTERED for DDL operations — it does not reliably reflect DML
  inserts into the WAL tables. The Snowflake version therefore processes all
  WAL tables on every run. The MERGE is keyed on (SIGDS_TABLE, SCAN_SCHEMA)
  and only updates rows where WAL_MAX_EDIT_NUM has increased, so re-processing
  unchanged WAL tables is safe and produces no spurious updates.
- All WAL table metadata (size, timestamps, existence) is fetched in a single
  INFORMATION_SCHEMA.TABLES query rather than per-table DESCRIBE DETAIL calls.
  This eliminates the need for a thread pool.
- SIGDS data table metadata is also fetched in a single INFORMATION_SCHEMA.TABLES
  query after the WAL extraction identifies which SIGDS tables to look up.
- Sigma API logic is identical to the Databricks version.
- WAL table JSON columns (METADATA, EDIT) are treated as VARCHAR containing JSON.
  TRY_PARSE_JSON is used for safe extraction with graceful handling of malformed rows.
- The TIMESTAMP column in WAL tables is a reserved word in Snowflake and is
  referenced with double-quotes ("TIMESTAMP") in all queries.
"""

import base64
import requests
import snowflake.connector
from snowflake.connector import DictCursor
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration — update before running
# ---------------------------------------------------------------------------
ACCOUNT             = "<YOUR_ACCOUNT>"          # e.g. xy12345.us-east-1
USER                = "<YOUR_USER>"
PASSWORD            = "<YOUR_PASSWORD>"
ROLE                = "<YOUR_ROLE>"
WAREHOUSE           = "<YOUR_WAREHOUSE>"
DATABASE            = "<YOUR_DATABASE>"
SCAN_SCHEMA         = "<YOUR_SCAN_SCHEMA>"      # Schema to scan for SIGDS_WAL_* tables
MAP_SCHEMA          = "<YOUR_MAP_SCHEMA>"        # Schema where SIGDS_WORKBOOK_MAP lives
#   Single schema:  set SCAN_SCHEMA = MAP_SCHEMA = your writeback schema
#   Multi-schema:   keep MAP_SCHEMA fixed; change SCAN_SCHEMA each run
TARGET_TABLE        = f'"{DATABASE}"."{MAP_SCHEMA}"."SIGDS_WORKBOOK_MAP"'
# Sigma API base URL — find your regional endpoint at:
# https://help.sigmacomputing.com/reference/get-started-sigma-api
SIGMA_API_BASE      = "<YOUR_API_BASE_URL>/v2"
SIGMA_CLIENT_ID     = "<YOUR_SIGMA_CLIENT_ID>"
SIGMA_CLIENT_SECRET = "<YOUR_SIGMA_CLIENT_SECRET>"
MAX_WAL_TABLES      = 0    # 0 = all; set > 0 to cap WAL tables for testing
WAL_BATCH_SIZE      = 100  # max WAL tables per UNION ALL query
# ---------------------------------------------------------------------------

if any(v.startswith("<YOUR_") for v in [
    ACCOUNT, USER, PASSWORD, ROLE, WAREHOUSE, DATABASE,
    SCAN_SCHEMA, MAP_SCHEMA, SIGMA_API_BASE, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET,
]):
    raise ValueError(
        "Set ACCOUNT, USER, PASSWORD, ROLE, WAREHOUSE, DATABASE, SCAN_SCHEMA, "
        "MAP_SCHEMA, SIGMA_API_BASE, SIGMA_CLIENT_ID and SIGMA_CLIENT_SECRET before running."
    )

# Column list used for both the staging temp table DDL and the MERGE statement.
# Order must match the tuple layout in the rows list assembled in Step 7.
COLUMNS = [
    "WAL_TABLE_FQN",
    "WAL_DS_ID",
    "SIGDS_TABLE",
    "SCAN_SCHEMA",
    "WORKBOOK_ID",
    "WAL_WORKBOOK_URL",
    "ORG_SLUG",
    "WAL_INPUT_TABLE_NAME",
    "WORKBOOK_NAME",
    "WORKBOOK_PATH",
    "OBJECT_TYPE",
    "WAL_LAST_EDIT_AT",
    "WAL_LAST_EDIT_BY",
    "WAL_MAX_EDIT_NUM",
    "SIGDS_TABLE_ID",
    "SIGDS_TABLE_CREATED_AT",
    "SIGDS_TABLE_LAST_MODIFIED",
    "SIGDS_TABLE_SIZE_BYTES",
    "WAL_TABLE_LAST_MODIFIED",
    "IS_ORPHANED",
    "IS_DELETED",
    "DELETED_AT",
    "IS_LEGACY_WAL",
    "IS_TAGGED_VERSION",
    "VERSION_TAG_NAME",
    "PARENT_WORKBOOK_ID",
    "API_WORKBOOK_URL",
    "API_OWNER_ID",
    "API_IS_ARCHIVED",
    "API_OWNER_FIRST_NAME",
    "API_OWNER_LAST_NAME",
]

STAGING_DDL = """
CREATE TEMPORARY TABLE IF NOT EXISTS _SIGDS_UPDATES (
    WAL_TABLE_FQN        VARCHAR,
    WAL_DS_ID            VARCHAR,
    SIGDS_TABLE          VARCHAR,
    SCAN_SCHEMA          VARCHAR,
    WORKBOOK_ID          VARCHAR,
    WAL_WORKBOOK_URL     VARCHAR,
    ORG_SLUG             VARCHAR,
    WAL_INPUT_TABLE_NAME VARCHAR,
    WORKBOOK_NAME        VARCHAR,
    WORKBOOK_PATH        VARCHAR,
    OBJECT_TYPE          VARCHAR,
    WAL_LAST_EDIT_AT     TIMESTAMP_NTZ,
    WAL_LAST_EDIT_BY     VARCHAR,
    WAL_MAX_EDIT_NUM     NUMBER(38,0),
    SIGDS_TABLE_ID       VARCHAR,
    SIGDS_TABLE_CREATED_AT    TIMESTAMP_NTZ,
    SIGDS_TABLE_LAST_MODIFIED TIMESTAMP_NTZ,
    SIGDS_TABLE_SIZE_BYTES    NUMBER(38,0),
    WAL_TABLE_LAST_MODIFIED   TIMESTAMP_NTZ,
    IS_ORPHANED          BOOLEAN,
    IS_DELETED           BOOLEAN,
    DELETED_AT           TIMESTAMP_NTZ,
    IS_LEGACY_WAL        BOOLEAN,
    IS_TAGGED_VERSION    BOOLEAN,
    VERSION_TAG_NAME     VARCHAR,
    PARENT_WORKBOOK_ID   VARCHAR,
    API_WORKBOOK_URL     VARCHAR,
    API_OWNER_ID         VARCHAR,
    API_IS_ARCHIVED      BOOLEAN,
    API_OWNER_FIRST_NAME VARCHAR,
    API_OWNER_LAST_NAME  VARCHAR
)
"""


# ===========================================================================
# Sigma API helpers  (identical to the Databricks version)
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
    Tries common root-key names to handle variation across API response shapes.
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
        matched = False
        for key in ("entries", "workbooks", "dataModels", "data", "items"):
            chunk = data.get(key)
            if isinstance(chunk, list):
                items.extend(chunk)
                matched = True
                break
        if not matched:
            print(f"  WARN: sigma_paginate({endpoint!r}) — no recognised list key in response: {list(data.keys())}")
        next_page = data.get("nextPage")
        if not next_page:
            break
        params["page"] = next_page
    return items


def build_id_index(entries: list, target_ids: set) -> dict:
    """
    Index a list of Sigma API objects by the ID field that best overlaps
    with target_ids.  Returns {normalised_id_string: entry_dict}.
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
# Snowflake WAL extraction helper
# ===========================================================================

def extract_wal_records_batch(cur, wal_batch: list) -> list:
    """
    Build and execute a single UNION ALL query across a batch of WAL tables.
    Returns the latest WAL entry per SIGDS table (by highest EDIT_NUM).

    JSON fields (METADATA, EDIT) are treated as VARCHAR; TRY_PARSE_JSON is used
    for safe extraction.  TIMESTAMP is double-quoted because it is a reserved
    word in Snowflake.  SPLIT_PART(..., '/', 4) is the 1-indexed equivalent of
    the Databricks get(split(..., '/'), 3) call (0-indexed position 3 = 4th part).
    """
    parts = []
    for wal_fqn in wal_batch:
        # wal_fqn is already fully qualified: "DB"."SCHEMA"."TABLE_NAME"
        parts.append(f"""
        SELECT
            '{wal_fqn.replace("'", "''")}'                                      AS WAL_TABLE_FQN,
            EDIT_NUM                                                             AS WAL_MAX_EDIT_NUM,
            DS_ID                                                                AS WAL_DS_ID,
            "TIMESTAMP"                                                          AS WAL_LAST_EDIT_AT,
            TRY_PARSE_JSON(METADATA):tableName::VARCHAR                         AS SIGDS_TABLE,
            TRY_PARSE_JSON(METADATA):workbookId::VARCHAR                        AS WORKBOOK_ID,
            COALESCE(
                TRY_PARSE_JSON(METADATA):sigmaUrl::VARCHAR,
                TRY_PARSE_JSON(METADATA):workbookUrl::VARCHAR
            )                                                                    AS WAL_WORKBOOK_URL,
            COALESCE(
                TRY_PARSE_JSON(METADATA):elementTitle::VARCHAR,
                TRY_PARSE_JSON(METADATA):inputTableTitle::VARCHAR
            )                                                                    AS WAL_INPUT_TABLE_NAME,
            COALESCE(
                TRY_PARSE_JSON(METADATA):userEmail::VARCHAR,
                TRY_PARSE_JSON(EDIT):updateRow:blameInfo:updatedBy::VARCHAR,
                TRY_PARSE_JSON(EDIT):addRow:blameInfo:updatedBy::VARCHAR
            )                                                                    AS WAL_LAST_EDIT_BY,
            SPLIT_PART(COALESCE(
                TRY_PARSE_JSON(METADATA):sigmaUrl::VARCHAR,
                TRY_PARSE_JSON(METADATA):workbookUrl::VARCHAR
            ), '/', 4)                                                           AS ORG_SLUG,
            ROW_NUMBER() OVER (
                PARTITION BY TRY_PARSE_JSON(METADATA):tableName::VARCHAR
                ORDER BY EDIT_NUM DESC
            )                                                                    AS rn
        FROM {wal_fqn}
        """)
    union_sql = "\nUNION ALL\n".join(parts)
    cur.execute(f"""
        SELECT * EXCLUDE rn
        FROM   ({union_sql})
        WHERE  rn = 1
          AND  SIGDS_TABLE IS NOT NULL
    """)
    return cur.fetchall()


# ===========================================================================
# Main
# ===========================================================================

# ---------------------------------------------------------------------------
# Step 1 — Authenticate with Sigma
# ---------------------------------------------------------------------------
sigma_token = get_sigma_token(SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET)
print("Step 1: Sigma token obtained.")

# ---------------------------------------------------------------------------
# Step 2 — Connect to Snowflake; load stored watermarks and enrichment cache
# ---------------------------------------------------------------------------
conn = snowflake.connector.connect(
    account=ACCOUNT,
    user=USER,
    password=PASSWORD,
    role=ROLE,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=MAP_SCHEMA,
)
cur = conn.cursor(DictCursor)

stored_rows = cur.execute(f"""
    SELECT WAL_TABLE_FQN, WAL_MAX_EDIT_NUM, WORKBOOK_ID,
           WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE,
           API_WORKBOOK_URL, API_OWNER_ID, API_IS_ARCHIVED,
           API_OWNER_FIRST_NAME, API_OWNER_LAST_NAME,
           IS_TAGGED_VERSION, VERSION_TAG_NAME, PARENT_WORKBOOK_ID,
           IS_DELETED, IS_ORPHANED, SIGDS_TABLE, SCAN_SCHEMA
    FROM   {TARGET_TABLE}
""").fetchall()

# WAL edit-num watermarks: {wal_fqn -> highest WAL_MAX_EDIT_NUM stored}
# Used to determine whether a WAL table has new rows since last run.
watermarks               = {}   # {wal_table_fqn -> stored WAL_MAX_EDIT_NUM}
known_wb_ids             = set()
known_enrichment         = {}   # {WORKBOOK_ID -> enrichment dict}
known_wal_tables         = set()
previously_deleted_wals  = set()
known_non_orphaned_sigds = set()
known_orphaned_sigds     = set()

for row in stored_rows:
    wid = row["WORKBOOK_ID"]
    if wid:
        known_wb_ids.add(wid)
        if wid not in known_enrichment:
            known_enrichment[wid] = {
                "WORKBOOK_NAME":        row["WORKBOOK_NAME"],
                "WORKBOOK_PATH":        row["WORKBOOK_PATH"],
                "OBJECT_TYPE":          row["OBJECT_TYPE"],
                "API_WORKBOOK_URL":     row["API_WORKBOOK_URL"],
                "API_OWNER_ID":         row["API_OWNER_ID"],
                "API_IS_ARCHIVED":      row["API_IS_ARCHIVED"],
                "API_OWNER_FIRST_NAME": row["API_OWNER_FIRST_NAME"],
                "API_OWNER_LAST_NAME":  row["API_OWNER_LAST_NAME"],
                "IS_TAGGED_VERSION":    row["IS_TAGGED_VERSION"],
                "VERSION_TAG_NAME":     row["VERSION_TAG_NAME"],
                "PARENT_WORKBOOK_ID":   row["PARENT_WORKBOOK_ID"],
            }
    # WAL tracking and orphan state scoped to current SCAN_SCHEMA only
    if row["SCAN_SCHEMA"] != SCAN_SCHEMA:
        continue
    wt = row["WAL_TABLE_FQN"]
    if wt:
        known_wal_tables.add(wt)
        if row["IS_DELETED"]:
            previously_deleted_wals.add(wt)
        stored_edit_num = row["WAL_MAX_EDIT_NUM"]
        if stored_edit_num is not None:
            if wt not in watermarks or stored_edit_num > watermarks[wt]:
                watermarks[wt] = stored_edit_num
    st = row["SIGDS_TABLE"]
    if st:
        if row["IS_ORPHANED"]:
            known_orphaned_sigds.add(st)
        else:
            known_non_orphaned_sigds.add(st)

print(
    f"Step 2: Loaded watermarks for {len(watermarks)} WAL tables (schema={SCAN_SCHEMA}); "
    f"{len(known_wb_ids)} WORKBOOK_IDs already enriched (all schemas); "
    f"{len(previously_deleted_wals)} previously flagged as deleted; "
    f"{len(known_orphaned_sigds)} previously flagged as orphaned."
)

# ---------------------------------------------------------------------------
# Step 3 — Discover WAL tables from INFORMATION_SCHEMA; detect deletions
# ---------------------------------------------------------------------------
# Fetch all tables in the schema in one query.
# WAL tables: SIGDS_WAL_*
# We also capture LAST_ALTERED for reference (stored as WAL_TABLE_LAST_MODIFIED).
info_rows = cur.execute(f"""
    SELECT TABLE_NAME, LAST_ALTERED, CREATED, BYTES, TABLE_ID
    FROM "{DATABASE}".INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = '{SCAN_SCHEMA.upper()}'
""").fetchall()

all_table_meta = {r["TABLE_NAME"].upper(): r for r in info_rows}

wal_table_names = sorted(
    name for name in all_table_meta if name.startswith("SIGDS_WAL")
)
if MAX_WAL_TABLES > 0:
    wal_table_names = wal_table_names[:MAX_WAL_TABLES]

# Build fully-qualified names using double-quoted identifiers
all_wal_fqns = [
    f'"{DATABASE}"."{SCAN_SCHEMA}"."{name}"'
    for name in wal_table_names
]
all_wal_fqn_set = set(all_wal_fqns)

# Build a lookup: fqn -> TABLE_NAME (bare) for metadata access
fqn_to_bare = {fqn: name for fqn, name in zip(all_wal_fqns, wal_table_names)}

# Deletion / reappearance detection (skip if capped)
if MAX_WAL_TABLES == 0:
    newly_deleted_wals = known_wal_tables - all_wal_fqn_set
    reappeared_wals    = previously_deleted_wals & all_wal_fqn_set
    if newly_deleted_wals:
        print(f"Step 3: {len(newly_deleted_wals)} WAL table(s) no longer in schema — will be flagged as deleted:")
        for w in sorted(newly_deleted_wals):
            print(f"  {w}")
    if reappeared_wals:
        print(f"Step 3: {len(reappeared_wals)} previously deleted WAL table(s) have reappeared — deletion flag will be cleared.")
else:
    newly_deleted_wals = set()
    reappeared_wals    = set()
    print("Step 3: Deletion detection skipped (MAX_WAL_TABLES is set — full WAL list not available).")

print(f"Step 3: Discovered {len(all_wal_fqns)} WAL tables in {SCAN_SCHEMA}.")

# ---------------------------------------------------------------------------
# Step 4 — Extract latest WAL records via batched UNION ALL (all WAL tables)
# ---------------------------------------------------------------------------
# Note: all WAL tables are processed on every run (see design note at top of file).
batches = [
    all_wal_fqns[i: i + WAL_BATCH_SIZE]
    for i in range(0, len(all_wal_fqns), WAL_BATCH_SIZE)
]

new_records = []
for idx, batch in enumerate(batches, start=1):
    print(f"  Step 4: Extracting WAL batch {idx}/{len(batches)} ({len(batch)} tables)...")
    new_records.extend(extract_wal_records_batch(cur, batch))

print(f"Step 4: Extracted {len(new_records)} WAL entries.")

# Deduplicate by SIGDS_TABLE, keeping the highest WAL_MAX_EDIT_NUM.
# Sigma can maintain two WAL tables for the same dataset during migration from
# the old random-UUID naming to the DS_ID-based naming.
_seen: dict = {}
for r in new_records:
    t = r["SIGDS_TABLE"]
    if t and (t not in _seen or (r["WAL_MAX_EDIT_NUM"] or 0) > (_seen[t]["WAL_MAX_EDIT_NUM"] or 0)):
        _seen[t] = r
new_records = list(_seen.values())
print(f"Step 4: {len(new_records)} unique SIGDS tables after deduplication.")

# Filter to records where WAL_MAX_EDIT_NUM has increased since last run.
# This avoids unnecessary MERGE updates for unchanged records.
to_update   = [
    r for r in new_records
    if r["WAL_TABLE_FQN"] not in watermarks
    or (r["WAL_MAX_EDIT_NUM"] or 0) > (watermarks.get(r["WAL_TABLE_FQN"]) or -1)
]
print(
    f"Step 4: {len(to_update)} record(s) have new WAL data; "
    f"{len(new_records) - len(to_update)} unchanged and will be skipped in MERGE."
)

# ---------------------------------------------------------------------------
# Step 5 — SIGDS table metadata from INFORMATION_SCHEMA.TABLES (single query)
# ---------------------------------------------------------------------------
all_sigds_names = [r["SIGDS_TABLE"] for r in new_records if r["SIGDS_TABLE"]]

# Orphaned: SIGDS table name not found in the schema's table list
orphaned_tables  = {t for t in all_sigds_names if t.upper() not in all_table_meta}
sigds_to_fetch   = [t for t in all_sigds_names if t.upper() in all_table_meta]

if orphaned_tables:
    print(f"Step 5: {len(orphaned_tables)} orphaned WAL record(s) — SIGDS table not found in schema:")
    for t in sorted(orphaned_tables):
        print(f"  {t}")

# Build detail map from the INFORMATION_SCHEMA data already fetched in Step 3
detail_map = {}
for bare_name in sigds_to_fetch:
    meta = all_table_meta.get(bare_name.upper(), {})
    detail_map[bare_name] = {
        "SIGDS_TABLE_ID":            str(meta["TABLE_ID"]) if meta.get("TABLE_ID") else None,
        "SIGDS_TABLE_CREATED_AT":    meta.get("CREATED"),
        "SIGDS_TABLE_LAST_MODIFIED": meta.get("LAST_ALTERED"),
        "SIGDS_TABLE_SIZE_BYTES":    meta.get("BYTES"),
    }

print(f"Step 5: Metadata resolved for {len(detail_map)} SIGDS tables; {len(orphaned_tables)} orphaned.")

# Check for orphan status changes in existing records not part of this run
new_record_sigds = {r["SIGDS_TABLE"] for r in new_records if r["SIGDS_TABLE"]}
if MAX_WAL_TABLES == 0:
    newly_orphaned_existing = {
        t for t in known_non_orphaned_sigds
        if t.upper() not in all_table_meta and t not in new_record_sigds
    }
    recovered_existing = {
        t for t in known_orphaned_sigds
        if t.upper() in all_table_meta and t not in new_record_sigds
    }
    if newly_orphaned_existing:
        print(f"Step 5: {len(newly_orphaned_existing)} existing record(s) newly orphaned.")
    if recovered_existing:
        print(f"Step 5: {len(recovered_existing)} previously orphaned record(s) have recovered.")
else:
    newly_orphaned_existing = set()
    recovered_existing      = set()

# ---------------------------------------------------------------------------
# Step 6 — Sigma API enrichment (new IDs) + archive status refresh (all IDs)
# ---------------------------------------------------------------------------
new_wb_ids = {
    r["WORKBOOK_ID"]
    for r in to_update
    if r["WORKBOOK_ID"] and r["WORKBOOK_ID"] not in known_wb_ids
}
wb_meta         = {}   # {WORKBOOK_ID -> full enrichment dict} for newly-seen IDs
archive_updates = {}   # {WORKBOOK_ID -> new_is_archived} for changed existing IDs

all_wb_ids_to_check = new_wb_ids | known_wb_ids

if all_wb_ids_to_check:
    print(
        f"Step 6: Fetching Sigma workbook/data-model list "
        f"({len(new_wb_ids)} new enrichment, {len(known_wb_ids)} archive re-check)..."
    )
    workbooks  = sigma_paginate(sigma_token, "workbooks")
    datamodels = sigma_paginate(sigma_token, "dataModels")
    print(f"Step 6: Sigma API returned {len(workbooks)} workbook(s), {len(datamodels)} data model(s).")
    wb_index = build_id_index(workbooks,  all_wb_ids_to_check)
    dm_index = build_id_index(datamodels, all_wb_ids_to_check)

    # Fetch version tags and build tagged-workbook index
    all_tags = sigma_paginate(sigma_token, "tags")
    print(f"Step 6: Sigma API returned {len(all_tags)} version tag(s).")
    tagged_wb_index = {}
    for tag in all_tags:
        tag_id = tag.get("versionTagId")
        if not tag_id:
            continue
        tagged_wbs = sigma_paginate(sigma_token, f"tags/{tag_id}/workbooks")
        for wb in tagged_wbs:
            for t in wb.get("tags", []):
                twid = t.get("taggedWorkbookId")
                if twid:
                    raw_path = wb.get("path")
                    tagged_wb_index[twid.strip().lower()] = {
                        "parent_workbook_id": wb.get("workbookId"),
                        "tag_name":           t.get("name"),
                        "workbook_name":      wb.get("name"),
                        "workbook_path":      "/".join(raw_path) if isinstance(raw_path, list) else raw_path,
                        "workbook_url":       wb.get("url"),
                        "ownerId":            wb.get("ownerId"),
                    }
    print(f"Step 6: Built tagged workbook index with {len(tagged_wb_index)} entry(ies).")

    # Full enrichment for newly-seen WORKBOOK_IDs
    for wid in new_wb_ids:
        norm   = wid.strip().lower()
        entry  = wb_index.get(norm) or dm_index.get(norm)
        tagged = tagged_wb_index.get(norm)
        if entry:
            is_wb    = norm in wb_index
            raw_path = entry.get("path")
            wb_meta[wid] = {
                "WORKBOOK_NAME":        entry.get("name"),
                "WORKBOOK_PATH":        "/".join(raw_path) if isinstance(raw_path, list) else raw_path,
                "OBJECT_TYPE":          "WORKBOOK" if is_wb else "DATA_MODEL",
                "API_WORKBOOK_URL":     entry.get("url"),
                "API_OWNER_ID":         entry.get("ownerId"),
                "API_IS_ARCHIVED":      entry.get("isArchived", False) if is_wb else False,
                "API_OWNER_FIRST_NAME": None,
                "API_OWNER_LAST_NAME":  None,
                "IS_TAGGED_VERSION":    False,
                "VERSION_TAG_NAME":     None,
                "PARENT_WORKBOOK_ID":   None,
            }
        elif tagged:
            parent_norm = (tagged["parent_workbook_id"] or "").strip().lower()
            parent_wb   = wb_index.get(parent_norm, {})
            wb_meta[wid] = {
                "WORKBOOK_NAME":        tagged.get("workbook_name"),
                "WORKBOOK_PATH":        tagged.get("workbook_path"),
                "OBJECT_TYPE":          "WORKBOOK",
                "API_WORKBOOK_URL":     tagged.get("workbook_url"),
                "API_OWNER_ID":         tagged.get("ownerId"),
                "API_IS_ARCHIVED":      parent_wb.get("isArchived", False) if parent_wb else False,
                "API_OWNER_FIRST_NAME": None,
                "API_OWNER_LAST_NAME":  None,
                "IS_TAGGED_VERSION":    True,
                "VERSION_TAG_NAME":     tagged.get("tag_name"),
                "PARENT_WORKBOOK_ID":   tagged.get("parent_workbook_id"),
            }

    # Resolve owner display names for new IDs
    owner_ids = {m["API_OWNER_ID"] for m in wb_meta.values() if m.get("API_OWNER_ID")}
    if owner_ids:
        print(f"Step 6: Fetching Sigma members to resolve {len(owner_ids)} owner ID(s)...")
        all_members  = sigma_paginate(sigma_token, "members")
        print(f"Step 6: Sigma API returned {len(all_members)} member(s).")
        member_index = {
            m["memberId"].strip().lower(): m
            for m in all_members
            if m.get("memberId")
        }
        for meta in wb_meta.values():
            oid = meta.get("API_OWNER_ID")
            if oid:
                member = member_index.get(oid.strip().lower(), {})
                meta["API_OWNER_FIRST_NAME"] = member.get("firstName")
                meta["API_OWNER_LAST_NAME"]  = member.get("lastName")

    if new_wb_ids:
        print(f"Step 6: Resolved {len(wb_meta)} of {len(new_wb_ids)} new WORKBOOK_IDs.")

    # Archive status re-check for all existing WORKBOOK_IDs
    for wid in known_wb_ids:
        norm            = wid.strip().lower()
        stored          = known_enrichment.get(wid, {})
        stored_archived = stored.get("API_IS_ARCHIVED")
        if norm in wb_index:
            current_archived = wb_index[norm].get("isArchived", False)
        elif norm in dm_index or stored.get("OBJECT_TYPE") == "DATA_MODEL":
            current_archived = False
        elif stored.get("IS_TAGGED_VERSION"):
            parent_id = stored.get("PARENT_WORKBOOK_ID")
            if parent_id:
                parent_norm = parent_id.strip().lower()
                if parent_norm in wb_index:
                    current_archived = wb_index[parent_norm].get("isArchived", False)
                else:
                    continue
            else:
                continue
        else:
            continue
        if current_archived != stored_archived:
            archive_updates[wid] = current_archived

    if archive_updates:
        print(f"Step 6: Archive status changed for {len(archive_updates)} existing WORKBOOK_ID(s).")
    else:
        print("Step 6: No archive status changes detected.")
else:
    print("Step 6: No WORKBOOK_IDs to process — Sigma API fetch skipped.")

# ---------------------------------------------------------------------------
# Step 7 — Assemble rows and MERGE into SIGDS_WORKBOOK_MAP
# ---------------------------------------------------------------------------
now_utc = datetime.now(timezone.utc).replace(tzinfo=None)  # TIMESTAMP_NTZ

rows = []
for r in to_update:
    detail     = detail_map.get(r["SIGDS_TABLE"], {})
    wal_bare   = fqn_to_bare.get(r["WAL_TABLE_FQN"], "")
    wal_meta   = all_table_meta.get(wal_bare.upper(), {})
    enrichment = (
        wb_meta.get(r["WORKBOOK_ID"])
        if r["WORKBOOK_ID"] and r["WORKBOOK_ID"] in wb_meta
        else known_enrichment.get(r["WORKBOOK_ID"], {})
        if r["WORKBOOK_ID"]
        else {}
    )
    rows.append((
        r["WAL_TABLE_FQN"],
        r["WAL_DS_ID"],
        r["SIGDS_TABLE"],
        SCAN_SCHEMA,
        r["WORKBOOK_ID"],
        r["WAL_WORKBOOK_URL"],
        r["ORG_SLUG"],
        r["WAL_INPUT_TABLE_NAME"],
        enrichment.get("WORKBOOK_NAME"),
        enrichment.get("WORKBOOK_PATH"),
        enrichment.get("OBJECT_TYPE"),
        r["WAL_LAST_EDIT_AT"],
        r["WAL_LAST_EDIT_BY"],
        r["WAL_MAX_EDIT_NUM"],
        detail.get("SIGDS_TABLE_ID"),
        detail.get("SIGDS_TABLE_CREATED_AT"),
        detail.get("SIGDS_TABLE_LAST_MODIFIED"),
        detail.get("SIGDS_TABLE_SIZE_BYTES"),
        wal_meta.get("LAST_ALTERED"),           # WAL_TABLE_LAST_MODIFIED
        r["SIGDS_TABLE"] in orphaned_tables,    # IS_ORPHANED
        False,                                  # IS_DELETED
        None,                                   # DELETED_AT
        "sigds_wal_ds_" not in (r["WAL_TABLE_FQN"] or "").lower(),  # IS_LEGACY_WAL
        enrichment.get("IS_TAGGED_VERSION", False),
        enrichment.get("VERSION_TAG_NAME"),
        enrichment.get("PARENT_WORKBOOK_ID"),
        enrichment.get("API_WORKBOOK_URL"),
        enrichment.get("API_OWNER_ID"),
        enrichment.get("API_IS_ARCHIVED"),
        enrichment.get("API_OWNER_FIRST_NAME"),
        enrichment.get("API_OWNER_LAST_NAME"),
    ))

if rows:
    # Create / truncate staging temp table, bulk-insert, then MERGE
    cur.execute(STAGING_DDL)
    cur.execute("DELETE FROM _SIGDS_UPDATES")
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    cur.executemany(f"INSERT INTO _SIGDS_UPDATES VALUES ({placeholders})", rows)

    col_list   = ", ".join(COLUMNS)
    src_cols   = ", ".join(f"s.{c}" for c in COLUMNS)
    update_set = ", ".join(
        f"t.{c} = s.{c}"
        for c in COLUMNS
        if c not in ("SIGDS_TABLE", "SCAN_SCHEMA")
    )

    cur.execute(f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING _SIGDS_UPDATES AS s
          ON  t.SIGDS_TABLE = s.SIGDS_TABLE
          AND t.SCAN_SCHEMA = s.SCAN_SCHEMA
        WHEN MATCHED AND s.WAL_MAX_EDIT_NUM > t.WAL_MAX_EDIT_NUM THEN
            UPDATE SET {update_set}
        WHEN NOT MATCHED THEN
            INSERT ({col_list}) VALUES ({src_cols})
    """)
    print(f"Step 7: MERGE complete — {len(rows)} row(s) upserted into {TARGET_TABLE}.")
else:
    print("Step 7: No new WAL data — MERGE skipped.")

# Flag records whose WAL table has disappeared
if newly_deleted_wals:
    wal_csv = ", ".join(f"'{w.replace(chr(39), chr(39)*2)}'" for w in newly_deleted_wals)
    cur.execute(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_DELETED = TRUE,
               DELETED_AT = CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
        WHERE  SCAN_SCHEMA    = '{SCAN_SCHEMA}'
          AND  WAL_TABLE_FQN IN ({wal_csv})
          AND  (IS_DELETED IS NULL OR IS_DELETED = FALSE)
    """)
    print(f"Step 7: Flagged {len(newly_deleted_wals)} record(s) as deleted.")

# Apply archive status changes for existing WORKBOOK_IDs
if archive_updates:
    newly_archived   = [w for w, v in archive_updates.items() if v]
    newly_unarchived = [w for w, v in archive_updates.items() if not v]
    if newly_archived:
        wid_csv = ", ".join(f"'{w}'" for w in newly_archived)
        cur.execute(f"UPDATE {TARGET_TABLE} SET API_IS_ARCHIVED = TRUE  WHERE WORKBOOK_ID IN ({wid_csv})")
        print(f"Step 7: Marked {len(newly_archived)} workbook(s) as archived.")
    if newly_unarchived:
        wid_csv = ", ".join(f"'{w}'" for w in newly_unarchived)
        cur.execute(f"UPDATE {TARGET_TABLE} SET API_IS_ARCHIVED = FALSE WHERE WORKBOOK_ID IN ({wid_csv})")
        print(f"Step 7: Marked {len(newly_unarchived)} workbook(s) as unarchived.")

# Update IS_ORPHANED for existing records not covered by the MERGE
if newly_orphaned_existing:
    sigds_csv = ", ".join(f"'{t}'" for t in newly_orphaned_existing)
    cur.execute(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_ORPHANED = TRUE
        WHERE  SCAN_SCHEMA = '{SCAN_SCHEMA}'
          AND  SIGDS_TABLE IN ({sigds_csv})
    """)
    print(f"Step 7: Marked {len(newly_orphaned_existing)} existing record(s) as orphaned.")

if recovered_existing:
    sigds_csv = ", ".join(f"'{t}'" for t in recovered_existing)
    cur.execute(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_ORPHANED = FALSE
        WHERE  SCAN_SCHEMA = '{SCAN_SCHEMA}'
          AND  SIGDS_TABLE IN ({sigds_csv})
    """)
    print(f"Step 7: Cleared orphan flag for {len(recovered_existing)} recovered record(s).")

# Clear the deletion flag for WAL tables that have reappeared
if reappeared_wals:
    wal_csv = ", ".join(f"'{w.replace(chr(39), chr(39)*2)}'" for w in reappeared_wals)
    cur.execute(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_DELETED = FALSE,
               DELETED_AT = NULL
        WHERE  SCAN_SCHEMA    = '{SCAN_SCHEMA}'
          AND  WAL_TABLE_FQN IN ({wal_csv})
    """)
    print(f"Step 7: Cleared deletion flag for {len(reappeared_wals)} reappeared record(s).")

# Sanity check — show most recently modified entries
print("\nMost recently modified entries:")
result = cur.execute(f"""
    SELECT SIGDS_TABLE, SCAN_SCHEMA, WORKBOOK_NAME, OBJECT_TYPE, IS_ORPHANED, IS_DELETED,
           API_IS_ARCHIVED, API_OWNER_FIRST_NAME, API_OWNER_LAST_NAME,
           SIGDS_TABLE_SIZE_BYTES, SIGDS_TABLE_LAST_MODIFIED, WAL_MAX_EDIT_NUM, WAL_TABLE_LAST_MODIFIED
    FROM   {TARGET_TABLE}
    WHERE  SCAN_SCHEMA = '{SCAN_SCHEMA}'
    ORDER  BY SIGDS_TABLE_LAST_MODIFIED DESC NULLS LAST
    LIMIT  20
""").fetchall()
for row in result:
    print(row)

cur.close()
conn.close()
print("\nDone.")
