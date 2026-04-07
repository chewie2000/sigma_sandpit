"""
populate_sigds_workbook_map.py

Incrementally populates SIGDS_WORKBOOK_MAP from Sigma writeback WAL tables
and the Sigma REST API.

Run logic per execution
-----------------------
1. Load stored WAL_TABLE_LAST_MODIFIED watermarks and known WORKBOOK_IDs from the
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
   present in the target table.  Resolve owner names via /v2/members.
7. Merge all changes into SIGDS_WORKBOOK_MAP via a single MERGE statement
   keyed on SIGDS_TABLE.  Flag records whose WAL table has disappeared as
   IS_DELETED=TRUE with a DELETED_AT timestamp; clear the flag if the WAL
   table reappears.

Design notes
------------
- WAL_TABLE_LAST_MODIFIED stores the lastModified timestamp returned by DESCRIBE
  DETAIL on the WAL table.  On each run this value is compared against the
  stored watermark; WAL tables with no new writes are skipped without reading
  any row data.  DESCRIBE DETAIL is metadata-only and requires no special
  permissions beyond SELECT on the table.
- WAL_MAX_EDIT_NUM is the EDIT_NUM of the highest-numbered row for that SIGDS
  table; it is derived from the WAL extract (rn=1 row) with no extra queries.
- All DESCRIBE DETAIL calls (WAL tables and SIGDS tables) are parallelised
  via a shared thread pool — safe because DESCRIBE DETAIL does not scan data.
- Sigma API calls are skipped entirely when all WORKBOOK_IDs are already known.
- API_IS_ARCHIVED is refreshed on every run for all known WORKBOOK_IDs.
  Only workbooks explicitly returned by the API are updated; a workbook
  absent from the API response is left unchanged, as the API will always
  include archived workbooks with isArchived=True rather than omitting them.
  Data models are always treated as non-archived.  Tagged versions inherit
  their parent workbook's archive status.  Other api_* columns (name, path,
  owner) are set once on first discovery and not refreshed.
- Version tags: Sigma workbooks can be tagged (e.g. Prod, QA), which creates
  a new workbook ID (taggedWorkbookId) for each tag.  The script fetches all
  tags via GET /v2/tags, then lists workbooks per tag via
  GET /v2/tags/{tagId}/workbooks to build a taggedWorkbookId → parent
  workbook mapping.  WAL records referencing a taggedWorkbookId are enriched
  from the parent workbook metadata and flagged with IS_TAGGED_VERSION=TRUE,
  VERSION_TAG_NAME, and PARENT_WORKBOOK_ID.
"""

import base64
import concurrent.futures
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType, LongType, StringType, StructField, StructType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# Configuration — update before running
# ---------------------------------------------------------------------------
CATALOG          = "<YOUR_CATALOG>"     # Databricks Unity Catalog name
SCHEMA           = "<YOUR_SCHEMA>"      # Schema containing SIGDS + WAL tables
TARGET_TABLE     = f"{CATALOG}.{SCHEMA}.SIGDS_WORKBOOK_MAP"
# Sigma API base URL — find your regional endpoint at:
# https://help.sigmacomputing.com/reference/get-started-sigma-api
SIGMA_API_BASE      = "<YOUR_API_BASE_URL>/v2"
SIGMA_CLIENT_ID     = "<YOUR_SIGMA_CLIENT_ID>"
SIGMA_CLIENT_SECRET = "<YOUR_SIGMA_CLIENT_SECRET>"
MAX_WAL_TABLE_FQNS   = 0   # 0 = all; set > 0 to cap WAL tables for testing
DESCRIBE_WORKERS = 16  # thread-pool size for all parallel DESCRIBE DETAIL calls
WAL_BATCH_SIZE   = 100 # max WAL tables per UNION ALL query
# ---------------------------------------------------------------------------

if any(v.startswith("<YOUR_") for v in [CATALOG, SCHEMA, SIGMA_API_BASE, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET]):
    raise ValueError("Set CATALOG, SCHEMA, SIGMA_API_BASE, SIGMA_CLIENT_ID and SIGMA_CLIENT_SECRET before running.")


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
    """
    try:
        return spark.sql(f"DESCRIBE DETAIL {full_name}").collect()[0].asDict()
    except Exception as exc:
        print(f"  WARN: DESCRIBE DETAIL failed for {full_name}: {exc}")
        return {}


def get_wal_last_modified(full_wal_name: str) -> tuple:
    """
    Return (full_wal_name, lastModified) for a single WAL table.
    Returns (full_wal_name, None) on failure.
    """
    detail = _describe_detail_fq(full_wal_name)
    return full_wal_name, detail.get("lastModified")


def parallel_wal_last_modified(wal_names: list) -> dict:
    """Run get_wal_last_modified concurrently. Returns {full_wal_name: lastModified}."""
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
    Returns a dict with SIGDS_TABLE_ID, SIGDS_TABLE_LOCATION, SIGDS_TABLE_CREATED_AT,
    SIGDS_TABLE_LAST_MODIFIED, SIGDS_TABLE_SIZE_BYTES.
    """
    full   = f"`{CATALOG}`.`{SCHEMA}`.`{table_name}`"
    detail = _describe_detail_fq(full)
    if not detail:
        return {}
    return {
        "SIGDS_TABLE_ID":            str(detail["id"]) if detail.get("id") else None,
        "SIGDS_TABLE_LOCATION":      detail.get("location"),
        "SIGDS_TABLE_CREATED_AT":    detail.get("createdAt"),
        "SIGDS_TABLE_LAST_MODIFIED": detail.get("lastModified"),
        "SIGDS_TABLE_SIZE_BYTES":    detail.get("sizeInBytes"),
    }


def parallel_describe_sigds(table_names: list) -> dict:
    """Run describe_sigds_table concurrently. Returns {bare_table_name: detail_dict}."""
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=DESCRIBE_WORKERS) as pool:
        futures = {pool.submit(describe_sigds_table, t): t for t in table_names}
        for future in concurrent.futures.as_completed(futures):
            results[futures[future]] = future.result()
    return results


def extract_wal_records_batch(wal_batch: list) -> list:
    """
    Build and execute a single UNION ALL query across a batch of WAL tables.
    Returns the latest WAL entry per SIGDS table (by highest EDIT_NUM).
    """
    parts = []
    for wal in wal_batch:
        parts.append(f"""
        SELECT
            '{wal}'   AS WAL_TABLE_FQN,
            EDIT_NUM  AS WAL_MAX_EDIT_NUM,
            WAL_DS_ID,
            TIMESTAMP AS WAL_LAST_EDIT_AT,
            get_json_object(METADATA, '$.tableName')            AS SIGDS_TABLE,
            get_json_object(METADATA, '$.workbookId')           AS WORKBOOK_ID,
            coalesce(
                get_json_object(METADATA, '$.sigmaUrl'),
                get_json_object(METADATA, '$.workbookUrl')
            )                                                   AS WAL_WORKBOOK_URL,
            coalesce(
                get_json_object(METADATA, '$.elementTitle'),
                get_json_object(METADATA, '$.inputTableTitle')
            )                                                   AS WAL_INPUT_TABLE_NAME,
            coalesce(
                get_json_object(METADATA, '$.userEmail'),
                get_json_object(EDIT, '$.updateRow.blameInfo.updatedBy'),
                get_json_object(EDIT, '$.addRow.blameInfo.updatedBy')
            )                                                   AS WAL_LAST_EDIT_BY,
            get(split(coalesce(
                get_json_object(METADATA, '$.sigmaUrl'),
                get_json_object(METADATA, '$.workbookUrl')
            ), '/'), 3)                                         AS ORG_SLUG,
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
# Step 2 — Load stored watermarks, known WORKBOOK_IDs, and enrichment cache
# ---------------------------------------------------------------------------
stored_rows = spark.sql(f"""
    SELECT WAL_TABLE_FQN, WAL_TABLE_LAST_MODIFIED, WORKBOOK_ID,
           WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE,
           API_WORKBOOK_URL, API_OWNER_ID, API_IS_ARCHIVED,
           API_OWNER_FIRST_NAME, API_OWNER_LAST_NAME,
           IS_TAGGED_VERSION, VERSION_TAG_NAME, PARENT_WORKBOOK_ID,
           IS_DELETED, IS_ORPHANED, SIGDS_TABLE
    FROM   {TARGET_TABLE}
""").collect()

watermarks                = {}   # {wal_table -> most recent WAL_TABLE_LAST_MODIFIED stored}
known_wb_ids              = set()
known_enrichment          = {}   # {WORKBOOK_ID -> enrichment dict} for already-seen IDs
known_wal_tables          = set() # all WAL_TABLE_FQNs currently tracked in the map
previously_deleted_wals   = set() # WAL_TABLE_FQNs already flagged IS_DELETED=TRUE
known_non_orphaned_sigds  = set() # SIGDS_TABLEs currently IS_ORPHANED=FALSE
known_orphaned_sigds      = set() # SIGDS_TABLEs currently IS_ORPHANED=TRUE

for row in stored_rows:
    wt, ts = row["WAL_TABLE_FQN"], row["WAL_TABLE_LAST_MODIFIED"]
    if wt:
        known_wal_tables.add(wt)
        if row["IS_DELETED"]:
            previously_deleted_wals.add(wt)
    if ts and (wt not in watermarks or ts > watermarks[wt]):
        watermarks[wt] = ts
    st = row["SIGDS_TABLE"]
    if st:
        if row["IS_ORPHANED"]:
            known_orphaned_sigds.add(st)
        else:
            known_non_orphaned_sigds.add(st)
    wid = row["WORKBOOK_ID"]
    if wid:
        known_wb_ids.add(wid)
        if wid not in known_enrichment:
            known_enrichment[wid] = {
                "WORKBOOK_NAME":        row["WORKBOOK_NAME"],
                "WORKBOOK_PATH":        row["WORKBOOK_PATH"],
                "OBJECT_TYPE":          row["OBJECT_TYPE"],
                "API_WORKBOOK_URL":              row["API_WORKBOOK_URL"],
                "API_OWNER_ID":         row["API_OWNER_ID"],
                "API_IS_ARCHIVED":      row["API_IS_ARCHIVED"],
                "API_OWNER_FIRST_NAME": row["API_OWNER_FIRST_NAME"],
                "API_OWNER_LAST_NAME":  row["API_OWNER_LAST_NAME"],
                "IS_TAGGED_VERSION":    row["IS_TAGGED_VERSION"],
                "VERSION_TAG_NAME":     row["VERSION_TAG_NAME"],
                "PARENT_WORKBOOK_ID":   row["PARENT_WORKBOOK_ID"],
            }

print(
    f"Step 2: Loaded watermarks for {len(watermarks)} WAL tables; "
    f"{len(known_wb_ids)} WORKBOOK_IDs already enriched; "
    f"{len(previously_deleted_wals)} previously flagged as deleted; "
    f"{len(known_orphaned_sigds)} previously flagged as orphaned."
)

# ---------------------------------------------------------------------------
# Step 3 — Discover WAL tables; pre-filter via parallel DESCRIBE DETAIL
# ---------------------------------------------------------------------------
wal_rows = (
    spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`")
         .filter("lower(tableName) LIKE 'sigds_wal%'")
         .collect()
)
all_wal_names = [
    f"`{CATALOG}`.`{SCHEMA}`.`{r.tableName}`"
    for r in wal_rows
]
if MAX_WAL_TABLE_FQNS > 0:
    all_wal_names = all_wal_names[:MAX_WAL_TABLE_FQNS]

all_wal_set = set(all_wal_names)

# Deletion detection requires the full WAL table list to be reliable.
# When MAX_WAL_TABLE_FQNS is set, all_wal_names is only a subset, so any table
# outside the cap would be wrongly flagged as deleted — skip in that case.
if MAX_WAL_TABLE_FQNS == 0:
    newly_deleted_wals = known_wal_tables - all_wal_set        # in map, gone from schema
    reappeared_wals    = previously_deleted_wals & all_wal_set # was deleted, now back
    if newly_deleted_wals:
        print(f"Step 3: {len(newly_deleted_wals)} WAL table(s) no longer in schema — will be flagged as deleted:")
        for w in sorted(newly_deleted_wals):
            print(f"  {w}")
    if reappeared_wals:
        print(f"Step 3: {len(reappeared_wals)} previously deleted WAL table(s) have reappeared — deletion flag will be cleared.")
else:
    newly_deleted_wals = set()
    reappeared_wals    = set()
    print("Step 3: Deletion detection skipped (MAX_WAL_TABLE_FQNS is set — full WAL list not available).")

print(
    f"Step 3: Discovered {len(all_wal_names)} WAL tables. "
    f"Running parallel DESCRIBE DETAIL to check for changes..."
)
wal_modified = parallel_wal_last_modified(all_wal_names)

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

if not to_process and not newly_deleted_wals and not reappeared_wals:
    if MAX_WAL_TABLE_FQNS > 0:
        # Capped run — orphan checks on existing records are unreliable, so exit.
        print("SIGDS_WORKBOOK_MAP is already up to date. Nothing to do.")
        raise SystemExit(0)
    print("Step 3: No WAL changes — proceeding to check existing records for orphan status changes.")

# ---------------------------------------------------------------------------
# Step 4 — Extract latest WAL records for changed tables (batched UNION ALL)
# ---------------------------------------------------------------------------
wal_last_altered = {full: ts for full, ts in to_process}
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

# Deduplicate by SIGDS_TABLE (the MERGE key), keeping the highest WAL_MAX_EDIT_NUM.
# Sigma can maintain two WAL tables for the same dataset when it migrates from
# the old random-UUID naming (sigds_wal_<uuid>) to the WAL_DS_ID-based naming
# (sigds_wal_ds_<ds_id>).  Both appear in SHOW TABLES and both pass the
# rn=1 filter within their own UNION ALL subquery, so we must dedup here.
_seen: dict = {}
for r in new_records:
    t = r["SIGDS_TABLE"]
    if t and (t not in _seen or (r["WAL_MAX_EDIT_NUM"] or 0) > (_seen[t]["WAL_MAX_EDIT_NUM"] or 0)):
        _seen[t] = r
new_records = list(_seen.values())
print(f"Step 4: {len(new_records)} unique SIGDS tables after deduplication.")

# ---------------------------------------------------------------------------
# Step 5 — DESCRIBE DETAIL (parallel) for each new/updated SIGDS table
# ---------------------------------------------------------------------------
# Pre-flight: fetch all table names in the schema once so we can identify
# orphaned WAL records without attempting (and failing) DESCRIBE DETAIL on
# tables that no longer exist.
existing_tables = {
    r.tableName.lower()
    for r in spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`").collect()
}

all_sigds_names  = [r["SIGDS_TABLE"] for r in new_records if r["SIGDS_TABLE"]]
orphaned_tables  = {t for t in all_sigds_names if t.lower() not in existing_tables}
sigds_bare_names = [t for t in all_sigds_names if t.lower() in existing_tables]

if orphaned_tables:
    print(f"Step 5: {len(orphaned_tables)} orphaned WAL record(s) — SIGDS table not found in schema:")
    for t in sorted(orphaned_tables):
        print(f"  {t}")

print(
    f"Step 5: Running DESCRIBE DETAIL on {len(sigds_bare_names)} SIGDS tables "
    f"using {DESCRIBE_WORKERS} workers..."
)
detail_map = parallel_describe_sigds(sigds_bare_names)
print(f"Step 5: DESCRIBE DETAIL complete. {len(sigds_bare_names)} valid, {len(orphaned_tables)} orphaned.")

# When running a full scan (MAX_WAL_TABLE_FQNS == 0), also check existing records
# in the map that were not part of this run's WAL changes.
new_record_sigds = {r["SIGDS_TABLE"] for r in new_records if r["SIGDS_TABLE"]}
if MAX_WAL_TABLE_FQNS == 0:
    newly_orphaned_existing = {
        t for t in known_non_orphaned_sigds
        if t.lower() not in existing_tables and t not in new_record_sigds
    }
    recovered_existing = {
        t for t in known_orphaned_sigds
        if t.lower() in existing_tables and t not in new_record_sigds
    }
    if newly_orphaned_existing:
        print(f"Step 5: {len(newly_orphaned_existing)} existing record(s) newly orphaned:")
        for t in sorted(newly_orphaned_existing):
            print(f"  {t}")
    if recovered_existing:
        print(f"Step 5: {len(recovered_existing)} previously orphaned record(s) have recovered.")
else:
    newly_orphaned_existing = set()
    recovered_existing      = set()
    print("Step 5: Orphan check for existing records skipped (MAX_WAL_TABLE_FQNS is set).")

# ---------------------------------------------------------------------------
# Step 6 — Sigma API enrichment (new IDs) + archive status refresh (all IDs)
# ---------------------------------------------------------------------------
# New WORKBOOK_IDs receive full enrichment (name, path, owner, archived state).
# All existing WORKBOOK_IDs have their archive status re-checked on every run;
# a workbook absent from the active API list is treated as archived.
# Other api_* columns (name, path, owner) are not refreshed for known IDs.
new_wb_ids = {
    r["WORKBOOK_ID"]
    for r in new_records
    if r["WORKBOOK_ID"] and r["WORKBOOK_ID"] not in known_wb_ids
}
wb_meta         = {}  # {WORKBOOK_ID -> full enrichment dict} for newly-seen IDs
archive_updates = {}  # {WORKBOOK_ID -> new_is_archived} for changed existing IDs

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

    # Fetch version tags and build tagged-workbook index so that
    # taggedWorkbookId values found in WAL records can be resolved
    # back to their parent workbook metadata.
    all_tags = sigma_paginate(sigma_token, "tags")
    print(f"Step 6: Sigma API returned {len(all_tags)} version tag(s).")
    tagged_wb_index = {}   # {taggedWorkbookId.lower() -> parent/tag info}
    for tag in all_tags:
        tag_id   = tag.get("versionTagId")
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

    # Full enrichment for newly-seen WORKBOOK_IDs.
    for wid in new_wb_ids:
        norm  = wid.strip().lower()
        entry = wb_index.get(norm) or dm_index.get(norm)
        tagged = tagged_wb_index.get(norm)
        if entry:
            # Standard workbook or data model.
            is_wb    = norm in wb_index
            raw_path = entry.get("path")
            wb_meta[wid] = {
                "WORKBOOK_NAME":        entry.get("name"),
                "WORKBOOK_PATH":        "/".join(raw_path) if isinstance(raw_path, list) else raw_path,
                "OBJECT_TYPE":          "WORKBOOK" if is_wb else "DATA_MODEL",
                "API_WORKBOOK_URL":              entry.get("url"),
                "API_OWNER_ID":         entry.get("ownerId"),
                "API_IS_ARCHIVED":      entry.get("isArchived", False) if is_wb else False,
                "API_OWNER_FIRST_NAME": None,
                "API_OWNER_LAST_NAME":  None,
                "IS_TAGGED_VERSION":    False,
                "VERSION_TAG_NAME":     None,
                "PARENT_WORKBOOK_ID":   None,
            }
        elif tagged:
            # Tagged version — enrich from parent workbook info.
            parent_norm = (tagged["parent_workbook_id"] or "").strip().lower()
            parent_wb   = wb_index.get(parent_norm, {})
            wb_meta[wid] = {
                "WORKBOOK_NAME":        tagged.get("workbook_name"),
                "WORKBOOK_PATH":        tagged.get("workbook_path"),
                "OBJECT_TYPE":          "WORKBOOK",
                "API_WORKBOOK_URL":              tagged.get("workbook_url"),
                "API_OWNER_ID":         tagged.get("ownerId"),
                "API_IS_ARCHIVED":      parent_wb.get("isArchived", False) if parent_wb else False,
                "API_OWNER_FIRST_NAME": None,
                "API_OWNER_LAST_NAME":  None,
                "IS_TAGGED_VERSION":    True,
                "VERSION_TAG_NAME":     tagged.get("tag_name"),
                "PARENT_WORKBOOK_ID":   tagged.get("parent_workbook_id"),
            }

    # Resolve owner display names for new IDs via /v2/members.
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

    # Archive status re-check for all existing WORKBOOK_IDs.
    # Only update when the API explicitly provides an archived state:
    #   - Workbooks in the API response → use the isArchived flag from the record.
    #   - Data models (in dm_index or previously stored as DATA_MODEL) → False.
    #   - IDs absent from both indexes → skip; the API is the authoritative source
    #     for archived state and will always include a workbook record with
    #     isArchived=True rather than omitting it when archived.
    for wid in known_wb_ids:
        norm            = wid.strip().lower()
        stored          = known_enrichment.get(wid, {})
        stored_archived = stored.get("API_IS_ARCHIVED")
        if norm in wb_index:
            current_archived = wb_index[norm].get("isArchived", False)
        elif norm in dm_index or stored.get("OBJECT_TYPE") == "DATA_MODEL":
            current_archived = False
        elif stored.get("IS_TAGGED_VERSION"):
            # Tagged version — inherit archive status from the parent workbook.
            parent_id = stored.get("PARENT_WORKBOOK_ID")
            if parent_id:
                parent_norm = parent_id.strip().lower()
                if parent_norm in wb_index:
                    current_archived = wb_index[parent_norm].get("isArchived", False)
                else:
                    continue   # parent not in API response — leave unchanged
            else:
                continue
        else:
            continue   # not in API response — do not assume archived
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
MERGE_SCHEMA = StructType([
    StructField("WAL_TABLE_FQN",            StringType(),    True),
    StructField("WAL_DS_ID",                StringType(),    True),
    StructField("SIGDS_TABLE",          StringType(),    True),
    StructField("WORKBOOK_ID",          StringType(),    True),
    StructField("WAL_WORKBOOK_URL",         StringType(),    True),
    StructField("ORG_SLUG",             StringType(),    True),
    StructField("WAL_INPUT_TABLE_NAME",     StringType(),    True),
    StructField("WAL_LAST_EDIT_AT",         TimestampType(), True),
    StructField("WAL_LAST_EDIT_BY",         StringType(),    True),
    StructField("WORKBOOK_NAME",        StringType(),    True),
    StructField("WORKBOOK_PATH",        StringType(),    True),
    StructField("OBJECT_TYPE",          StringType(),    True),
    StructField("SIGDS_TABLE_ID",             StringType(),    True),
    StructField("SIGDS_TABLE_LOCATION",       StringType(),    True),
    StructField("SIGDS_TABLE_CREATED_AT",     TimestampType(), True),
    StructField("SIGDS_TABLE_LAST_MODIFIED",  TimestampType(), True),
    StructField("SIGDS_TABLE_SIZE_BYTES",     LongType(),      True),
    StructField("WAL_MAX_EDIT_NUM",         LongType(),      True),
    StructField("WAL_TABLE_LAST_MODIFIED",     TimestampType(), True),
    StructField("IS_ORPHANED",          BooleanType(),   True),
    StructField("IS_DELETED",           BooleanType(),   True),
    StructField("DELETED_AT",           TimestampType(), True),
    StructField("IS_LEGACY_WAL",        BooleanType(),   True),
    # Version tag fields
    StructField("IS_TAGGED_VERSION",   BooleanType(),   True),
    StructField("VERSION_TAG_NAME",    StringType(),    True),
    StructField("PARENT_WORKBOOK_ID",  StringType(),    True),
    # Sigma API enrichment fields
    StructField("API_WORKBOOK_URL",              StringType(),    True),
    StructField("API_OWNER_ID",         StringType(),    True),
    StructField("API_IS_ARCHIVED",      BooleanType(),   True),
    StructField("API_OWNER_FIRST_NAME", StringType(),    True),
    StructField("API_OWNER_LAST_NAME",  StringType(),    True),
])

rows = []
for r in new_records:
    detail = detail_map.get(r["SIGDS_TABLE"], {})
    # Prefer freshly-fetched enrichment; fall back to cached values for known IDs
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
        r["WORKBOOK_ID"],
        r["WAL_WORKBOOK_URL"],
        r["ORG_SLUG"],
        r["WAL_INPUT_TABLE_NAME"],
        r["WAL_LAST_EDIT_AT"],
        r["WAL_LAST_EDIT_BY"],
        enrichment.get("WORKBOOK_NAME"),
        enrichment.get("WORKBOOK_PATH"),
        enrichment.get("OBJECT_TYPE"),
        detail.get("SIGDS_TABLE_ID"),
        detail.get("SIGDS_TABLE_LOCATION"),
        detail.get("SIGDS_TABLE_CREATED_AT"),
        detail.get("SIGDS_TABLE_LAST_MODIFIED"),
        detail.get("SIGDS_TABLE_SIZE_BYTES"),
        r["WAL_MAX_EDIT_NUM"],
        wal_last_altered.get(r["WAL_TABLE_FQN"]),
        r["SIGDS_TABLE"] in orphaned_tables,
        False,   # IS_DELETED — active record
        None,    # DELETED_AT — active record
        'sigds_wal_ds_' not in (r["WAL_TABLE_FQN"] or "").lower(),  # IS_LEGACY_WAL
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
else:
    print("Step 7: No WAL changes — MERGE skipped.")

# Flag records whose WAL table has disappeared since the last run.
if newly_deleted_wals:
    wal_csv = ", ".join(f"'{w}'" for w in newly_deleted_wals)
    spark.sql(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_DELETED = TRUE,
               DELETED_AT = current_timestamp()
        WHERE  WAL_TABLE_FQN IN ({wal_csv})
          AND  (IS_DELETED IS NULL OR IS_DELETED = FALSE)
    """)
    print(f"Step 7: Flagged {len(newly_deleted_wals)} record(s) as deleted.")

# Apply archive status changes for existing WORKBOOK_IDs.
if archive_updates:
    newly_archived   = [w for w, v in archive_updates.items() if v]
    newly_unarchived = [w for w, v in archive_updates.items() if not v]
    if newly_archived:
        wid_csv = ", ".join(f"'{w}'" for w in newly_archived)
        spark.sql(f"UPDATE {TARGET_TABLE} SET API_IS_ARCHIVED = TRUE  WHERE WORKBOOK_ID IN ({wid_csv})")
        print(f"Step 7: Marked {len(newly_archived)} workbook(s) as archived.")
    if newly_unarchived:
        wid_csv = ", ".join(f"'{w}'" for w in newly_unarchived)
        spark.sql(f"UPDATE {TARGET_TABLE} SET API_IS_ARCHIVED = FALSE WHERE WORKBOOK_ID IN ({wid_csv})")
        print(f"Step 7: Marked {len(newly_unarchived)} workbook(s) as unarchived.")

# Update IS_ORPHANED for existing records not covered by the MERGE.
if newly_orphaned_existing:
    sigds_csv = ", ".join(f"'{t}'" for t in newly_orphaned_existing)
    spark.sql(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_ORPHANED = TRUE
        WHERE  SIGDS_TABLE IN ({sigds_csv})
    """)
    print(f"Step 7: Marked {len(newly_orphaned_existing)} existing record(s) as orphaned.")

if recovered_existing:
    sigds_csv = ", ".join(f"'{t}'" for t in recovered_existing)
    spark.sql(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_ORPHANED = FALSE
        WHERE  SIGDS_TABLE IN ({sigds_csv})
    """)
    print(f"Step 7: Cleared orphan flag for {len(recovered_existing)} recovered record(s).")

# Clear the deletion flag for WAL tables that have reappeared.
if reappeared_wals:
    wal_csv = ", ".join(f"'{w}'" for w in reappeared_wals)
    spark.sql(f"""
        UPDATE {TARGET_TABLE}
        SET    IS_DELETED = FALSE,
               DELETED_AT = NULL
        WHERE  WAL_TABLE_FQN IN ({wal_csv})
    """)
    print(f"Step 7: Cleared deletion flag for {len(reappeared_wals)} reappeared record(s).")

# Sanity check — show most recently modified entries
spark.sql(f"""
    SELECT SIGDS_TABLE, WORKBOOK_NAME, OBJECT_TYPE, IS_ORPHANED, IS_DELETED, DELETED_AT,
           API_IS_ARCHIVED, API_OWNER_FIRST_NAME, API_OWNER_LAST_NAME,
           SIGDS_TABLE_SIZE_BYTES, SIGDS_TABLE_LAST_MODIFIED, WAL_MAX_EDIT_NUM, WAL_TABLE_LAST_MODIFIED
    FROM   {TARGET_TABLE}
    ORDER  BY SIGDS_TABLE_LAST_MODIFIED DESC NULLS LAST
    LIMIT  20
""").show(truncate=False)
