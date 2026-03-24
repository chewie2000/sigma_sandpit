import base64
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.getOrCreate()

# --- CONFIGURE THESE VALUES ---
catalog = "customer_success"        # e.g. your Unity catalog or 'hive_metastore'
writeback_schema = "marko_wb"       # schema with sigds_* and sigds_wal_* tables
target_table = f"{catalog}.{writeback_schema}.SIGDS_WORKBOOK_MAP"

# Limit how many WAL tables (sigds_wal_*) to process:
# 0 or None = process all WAL tables
# >0        = process only that many WAL tables (for testing)
# Note: 1 SIG table per input table; you may get fewer entries than SIGDS files.
MAX_WAL_TABLES = 10

# Sigma API (EU AWS)
SIGMA_API_BASE = "https://api.eu.aws.sigmacomputing.com/v2"

# From Sigma Admin > API credentials
SIGMA_CLIENT_ID     = "<YOUR_SIGMA_CLIENT_ID>"
SIGMA_CLIENT_SECRET = "<YOUR_SIGMA_CLIENT_SECRET>"

# -------------------------------

if SIGMA_CLIENT_ID.startswith("<YOUR_") or SIGMA_CLIENT_SECRET.startswith("<YOUR_"):
    raise ValueError("Set SIGMA_CLIENT_ID and SIGMA_CLIENT_SECRET before running.")


def get_sigma_access_token(client_id: str, client_secret: str) -> str:
    """Fetch OAuth access_token from Sigma using client credentials."""
    auth_str = f"{client_id}:{client_secret}"
    auth_b64 = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")
    token_url = f"{SIGMA_API_BASE}/auth/token"
    resp = requests.post(
        token_url,
        headers={
            "Authorization": f"Basic {auth_b64}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials"},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to get Sigma access token ({resp.status_code}): {resp.text}"
        )
    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError("Sigma token response missing access_token field.")
    return token


def extract_name_and_path(obj: dict):
    """Normalize 'name' and 'path' from workbook / data model JSON."""
    if obj is None:
        return None, None
    name = obj.get("name")
    raw_path = obj.get("path")
    if raw_path is None:
        path = None
    elif isinstance(raw_path, list):
        path = "/".join(str(p) for p in raw_path)
    else:
        path = str(raw_path)
    return name, path


def normalize_id(val) -> str:
    """Normalize ID strings for matching (strip, lowercase)."""
    if val is None:
        return ""
    return str(val).strip().lower()


def _extract_items_from_list_response(data: dict, kind: str):
    """
    Find the list of items in a Sigma list response.
    For your tenant, this will normally pick 'entries'.
    """
    candidate_keys = ["entries", "workbooks", "dataModels", "data", "items"]
    for key in candidate_keys:
        val = data.get(key)
        if isinstance(val, list):
            return val
    # Fallback: first list-of-dicts field
    for key, val in data.items():
        if isinstance(val, list) and val and isinstance(val[0], dict):
            return val
    return []


def fetch_all_workbooks(access_token: str):
    """Fetch all workbooks via /v2/workbooks with pagination."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    all_items = []
    url = f"{SIGMA_API_BASE}/workbooks"
    params = {}
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to list workbooks ({resp.status_code}): {resp.text}"
            )
        data = resp.json()
        items = _extract_items_from_list_response(data, "workbooks")
        all_items.extend(items)
        next_page = data.get("nextPage")
        if not next_page:
            break
        params["page"] = next_page
    return all_items


def fetch_all_data_models(access_token: str):
    """Fetch all data models via /v2/dataModels with pagination."""
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    all_items = []
    url = f"{SIGMA_API_BASE}/dataModels"
    params = {}
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to list data models ({resp.status_code}): {resp.text}"
            )
        data = resp.json()
        items = _extract_items_from_list_response(data, "dataModels")
        all_items.extend(items)
        next_page = data.get("nextPage")
        if not next_page:
            break
        params["page"] = next_page
    return all_items


def build_guid_index_for_kind(entries, work_ids, kind: str):
    """
    Build an index keyed on the best GUID-style ID field for this kind.
    - Look at all keys containing 'id' in the first entry (e.g. 'id', 'inodeId').
    - For each candidate key, compute how many values overlap with the WORKBOOK_ID set.
    - Pick the key with the highest overlap and index on that.
    """
    index = {}
    if not entries or not work_ids:
        return index
    work_ids_set = {normalize_id(w) for w in work_ids if w is not None}
    if not work_ids_set:
        return index
    first = entries[0]
    candidate_keys = [k for k in first.keys() if "id" in k.lower()]
    if not candidate_keys:
        candidate_keys = ["id"]  # best guess
    best_key = None
    best_overlap = -1
    for key in candidate_keys:
        values = {
            normalize_id(e.get(key))
            for e in entries
            if e.get(key) is not None
        }
        overlap = len(work_ids_set & values)
        if overlap > best_overlap:
            best_overlap = overlap
            best_key = key
    if best_key is None:
        best_key = "id"
    for e in entries:
        v = e.get(best_key)
        k = normalize_id(v)
        if k:
            index[k] = e
    return index


# ---------------------------------------------------------------------------
# 0) Get access token
# ---------------------------------------------------------------------------
access_token = get_sigma_access_token(SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET)
print("Fetched Sigma access token successfully.")

# ---------------------------------------------------------------------------
# 1) Clear existing data in SIGDS_WORKBOOK_MAP
# ---------------------------------------------------------------------------
spark.sql(f"TRUNCATE TABLE {target_table}")
print(f"Truncated {target_table}")

# ---------------------------------------------------------------------------
# 2) Find all WAL tables in the writeback schema (sigds_wal%)
# ---------------------------------------------------------------------------
wal_df = spark.sql(f"SHOW TABLES IN {catalog}.{writeback_schema}")
wal_df = wal_df.filter("lower(tableName) LIKE 'sigds_wal%'")
wal_tables = [
    f"{catalog}.{writeback_schema}.{row.tableName}"
    for row in wal_df.collect()
]
total_wal = len(wal_tables)

# Apply limit if MAX_WAL_TABLES > 0
if MAX_WAL_TABLES and MAX_WAL_TABLES > 0:
    wal_tables = wal_tables[:MAX_WAL_TABLES]
num_wal = len(wal_tables)
print(f"Found {total_wal} WAL tables; processing {num_wal}")

# ---------------------------------------------------------------------------
# 3) Populate SIGDS_WORKBOOK_MAP from each WAL table using MAX(EDIT_NUM)
# ---------------------------------------------------------------------------
for i, wal in enumerate(wal_tables, start=1):
    print(f"Loading WAL table {i}/{num_wal}")
    insert_sql = f"""
    INSERT INTO {target_table} (
      WAL_TABLE,
      DS_ID,
      SIGDS_TABLE,
      WORKBOOK_ID,
      WORKBOOK_URL,
      ORG_SLUG,
      INPUT_TABLE_NAME,
      LAST_EDIT_AT,
      LAST_EDIT_BY,
      WORKBOOK_NAME,
      WORKBOOK_PATH,
      OBJECT_TYPE
    )
    SELECT
      '{wal}' AS WAL_TABLE,
      ds_id          AS DS_ID,
      sigds_table    AS SIGDS_TABLE,
      workbook_id    AS WORKBOOK_ID,
      workbook_url   AS WORKBOOK_URL,
      element_at(split(workbook_url, '/'), 4) AS ORG_SLUG,
      input_table_name AS INPUT_TABLE_NAME,
      last_edit_at   AS LAST_EDIT_AT,
      last_edit_by   AS LAST_EDIT_BY,
      NULL           AS WORKBOOK_NAME,
      NULL           AS WORKBOOK_PATH,
      NULL           AS OBJECT_TYPE
    FROM (
      SELECT
        DS_ID                                              AS ds_id,
        EDIT_NUM                                           AS edit_num,
        TIMESTAMP                                          AS last_edit_at,
        get_json_object(METADATA, '$.tableName')           AS sigds_table,
        get_json_object(METADATA, '$.workbookId')          AS workbook_id,
        coalesce(
          get_json_object(METADATA, '$.sigmaUrl'),
          get_json_object(METADATA, '$.workbookUrl')
        )                                                  AS workbook_url,
        coalesce(
          get_json_object(METADATA, '$.elementTitle'),
          get_json_object(METADATA, '$.inputTableTitle')
        )                                                  AS input_table_name,
        coalesce(
          get_json_object(METADATA, '$.userEmail'),
          get_json_object(EDIT, '$.updateRow.blameInfo.updatedBy'),
          get_json_object(EDIT, '$.addRow.blameInfo.updatedBy')
        )                                                  AS last_edit_by,
        row_number() OVER (
          PARTITION BY get_json_object(METADATA, '$.tableName')
          ORDER BY EDIT_NUM DESC
        )                                                  AS rn
      FROM {wal}
    ) src
    WHERE rn = 1
      AND sigds_table IS NOT NULL
    """
    spark.sql(insert_sql)

print(f"Base WAL mapping loaded into {target_table}")

# ---------------------------------------------------------------------------
# 4) Get distinct non-null WORKBOOK_IDs from the mapping table
# ---------------------------------------------------------------------------
id_rows = spark.sql(f"""
    SELECT DISTINCT WORKBOOK_ID
    FROM {target_table}
    WHERE WORKBOOK_ID IS NOT NULL
""").collect()
work_ids = [r.WORKBOOK_ID for r in id_rows]
print(f"Found {len(work_ids)} distinct IDs to enrich (workbooks or data models).")

if work_ids:
    # -----------------------------------------------------------------------
    # 5) Bulk-fetch all workbooks and data models once
    # -----------------------------------------------------------------------
    print("Fetching all workbooks via bulk API...")
    all_workbooks = fetch_all_workbooks(access_token)
    print(f"Loaded {len(all_workbooks)} workbooks from Sigma.")

    print("Fetching all data models via bulk API...")
    all_data_models = fetch_all_data_models(access_token)
    print(f"Loaded {len(all_data_models)} data models from Sigma.")

    # Build GUID-based ID indexes using the best-matching ID-like key
    wb_index = build_guid_index_for_kind(all_workbooks,  work_ids, "workbooks")
    dm_index = build_guid_index_for_kind(all_data_models, work_ids, "dataModels")

    records    = []
    wb_hit     = 0
    dm_hit     = 0
    unresolved = 0

    for wid in work_ids:
        wid_norm    = normalize_id(wid)
        name        = None
        path        = None
        object_type = None

        if wid_norm in wb_index:
            entry       = wb_index[wid_norm]
            name, path  = extract_name_and_path(entry)
            object_type = "WORKBOOK"
            wb_hit     += 1
        elif wid_norm in dm_index:
            entry       = dm_index[wid_norm]
            name, path  = extract_name_and_path(entry)
            object_type = "DATA_MODEL"
            dm_hit     += 1
        else:
            unresolved += 1
            continue

        records.append({
            "WORKBOOK_ID":   str(wid),
            "WORKBOOK_NAME": name,
            "WORKBOOK_PATH": path,
            "OBJECT_TYPE":   object_type,
        })

    print(
        f"Successfully built detail records for {len(records)} IDs "
        f"(workbooks={wb_hit}, data_models={dm_hit}, unresolved={unresolved})."
    )

    # -----------------------------------------------------------------------
    # 6) Merge enrichment back into SIGDS_WORKBOOK_MAP
    # -----------------------------------------------------------------------
    if records:
        schema = StructType([
            StructField("WORKBOOK_ID",   StringType(), True),
            StructField("WORKBOOK_NAME", StringType(), True),
            StructField("WORKBOOK_PATH", StringType(), True),
            StructField("OBJECT_TYPE",   StringType(), True),
        ])
        rows = [
            (
                r["WORKBOOK_ID"],
                r["WORKBOOK_NAME"],
                r["WORKBOOK_PATH"],
                r["OBJECT_TYPE"],
            )
            for r in records
        ]
        wb_details_df = spark.createDataFrame(rows, schema=schema)
        wb_details_df.createOrReplaceTempView("WB_DETAILS_TEMP")

        merge_sql = f"""
            MERGE INTO {target_table} AS t
            USING WB_DETAILS_TEMP AS s
            ON t.WORKBOOK_ID = s.WORKBOOK_ID
            WHEN MATCHED THEN UPDATE SET
                t.WORKBOOK_NAME = s.WORKBOOK_NAME,
                t.WORKBOOK_PATH = s.WORKBOOK_PATH,
                t.OBJECT_TYPE   = s.OBJECT_TYPE
        """
        spark.sql(merge_sql)

        print(
            f"Enriched {target_table} with workbook/data model details "
            f"using bulk API IDs only."
        )

        # Quick sanity check
        spark.sql(f"""
            SELECT DISTINCT WORKBOOK_ID, WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE
            FROM {target_table}
            ORDER BY WORKBOOK_NAME NULLS LAST
            LIMIT 20
        """).show(truncate=False)
    else:
        print("No detail records to merge; all enrichment fields remain unchanged.")
else:
    print("No non-null WORKBOOK_ID values found; skipping enrichment.")
