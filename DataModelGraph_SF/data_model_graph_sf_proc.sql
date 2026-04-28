-- ==============================================================================
-- Prerequisites
-- Run setup_prerequisites.sql as ACCOUNTADMIN before deploying this procedure.
-- That script creates the network rule, Snowflake Secrets, external access
-- integration, and all required grants.
-- ==============================================================================

-- ==============================================================================
-- Create the stored procedure
-- Credentials are read at runtime from Snowflake Secrets — do not hardcode them.
-- All other configuration is passed as procedure parameters at call time.
--
-- Parameters:
--   TARGET_DATABASE        — Snowflake database where the output table will be written (required)
--   TARGET_SCHEMA          — Snowflake schema where the output table will be written (required)
--   TARGET_TABLE           — Output table name (optional, default: SIGMA_DATA_MODEL_GRAPH)
--   TRUNCATE_BEFORE_INSERT — TRUE = snapshot mode, replace on each run (recommended, default: TRUE)
--                            FALSE = append each run as a new RUN_ID; the table grows over time
--                            but all analysis queries filter to MAX(RUN_ID) so results stay correct.
--
--   SIGMA_BASE_URL, SIGMA_CLIENT_ID, and SIGMA_CLIENT_SECRET are read at runtime
--   from Snowflake Secrets — never passed as parameters or hardcoded.
--
-- API coverage:
--   GET /v2/dataModels                         — list all data models (paginated)
--   GET /v2/dataModels/{dataModelId}/sources   — sources for each data model (concurrent)
--   GET /v2/dataModels/{dataModelId}           — fetch individual data models referenced
--                                                as parents but absent from the list response
--
-- NOTE on org-wide completeness:
--   GET /v2/dataModels does not reliably support skipPermissionCheck at all API
--   versions. If the credential is an admin user, pass skipPermissionCheck=true and
--   verify that the returned count matches your expected org total. Data models owned
--   by other users may be absent from the list but can still appear as referenced parents;
--   those are fetched individually and included in the graph with whatever metadata
--   the API returns.
--
-- NOTE on sources response shape:
--   The /sources endpoint response shape should be verified via Postman before
--   relying on field names in production. The procedure handles both a bare array
--   and an 'entries'-wrapped response. Source entries are expected to have:
--     type        — 'data-model' | 'table' | 'input-table' | 'csv-upload' | ...
--     dataModelId — present when type identifies another data model
--
-- Example call:
--   CALL sigma_data_model_graph('MY_DATABASE', 'MY_SCHEMA');
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_data_model_graph(
    TARGET_DATABASE        STRING,
    TARGET_SCHEMA          STRING,
    TARGET_TABLE           STRING  DEFAULT 'SIGMA_DATA_MODEL_GRAPH',
    TRUNCATE_BEFORE_INSERT BOOLEAN DEFAULT TRUE
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (sigma_api_access)
SECRETS = ('sigma_base_url' = sigma_base_url, 'sigma_client_id' = sigma_client_id, 'sigma_client_secret' = sigma_client_secret)
HANDLER = 'main'
AS
$$
import _snowflake
import requests
import threading
import uuid
import time
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# MODULE-LEVEL SECRETS
# Read once at module load so all helpers can reference them as globals.
# ------------------------------------------------------------------------------
SIGMA_BASE_URL      = _snowflake.get_generic_secret_string('sigma_base_url')
SIGMA_CLIENT_ID     = _snowflake.get_generic_secret_string('sigma_client_id')
SIGMA_CLIENT_SECRET = _snowflake.get_generic_secret_string('sigma_client_secret')

# ------------------------------------------------------------------------------
# TOKEN MANAGEMENT
# Lock-protected so concurrent worker threads can safely share one token manager.
# ------------------------------------------------------------------------------

class SigmaTokenManager:

    def __init__(self):
        self._token      = None
        self._expires_at = 0.0
        self._lock       = threading.Lock()

    def get_token(self):
        with self._lock:
            if time.time() >= self._expires_at - 60:
                self._fetch()
            return self._token

    def _fetch(self):
        resp = requests.post(
            f"{SIGMA_BASE_URL}/v2/auth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type":    "client_credentials",
                "client_id":     SIGMA_CLIENT_ID,
                "client_secret": SIGMA_CLIENT_SECRET,
            },
            timeout=30,
        )
        resp.raise_for_status()
        body             = resp.json()
        self._token      = body["access_token"]
        self._expires_at = time.time() + body.get("expires_in", 3600)

# ------------------------------------------------------------------------------
# HTTP HELPER
# ------------------------------------------------------------------------------

def _get_with_backoff(url, headers, params=None, max_retries=4, timeout=60):
    """GET with exponential backoff on 429 and transient 5xx errors."""
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp

# ------------------------------------------------------------------------------
# SIGMA API HELPERS
# ------------------------------------------------------------------------------

def list_data_models(token_mgr):
    """
    List all data models via GET /v2/dataModels (paginated).
    skipPermissionCheck=true is passed; if not supported it is silently ignored
    and only models owned by the credential user are returned.
    """
    url    = f"{SIGMA_BASE_URL}/v2/dataModels"
    params = {"limit": 500, "skipPermissionCheck": "true"}
    data_models = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body    = _get_with_backoff(url, headers=headers, params=params).json()
        data_models.extend(body.get("entries", []))
        next_token = body.get("nextPageToken") or body.get("nextPage")
        if not next_token:
            break
        params["page"] = next_token
    return data_models


def get_data_model(token_mgr, dm_id):
    """Fetch a single data model by ID via GET /v2/dataModels/{dataModelId}."""
    url     = f"{SIGMA_BASE_URL}/v2/dataModels/{dm_id}"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    return _get_with_backoff(url, headers=headers).json()


def get_data_model_sources(token_mgr, dm_id):
    """
    Get all sources for a data model via GET /v2/dataModels/{dataModelId}/sources.
    Handles both a bare-array response and an 'entries'-wrapped response.
    Verify response shape against Postman if field names change.
    """
    url     = f"{SIGMA_BASE_URL}/v2/dataModels/{dm_id}/sources"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    body    = _get_with_backoff(url, headers=headers).json()
    if isinstance(body, list):
        return body
    return body.get("entries", body.get("sources", []))

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

def parse_ts(iso_str):
    """Parse ISO 8601 timestamp to a tz-naive datetime for TIMESTAMP_NTZ."""
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)

# ------------------------------------------------------------------------------
# MAIN HANDLER
# ------------------------------------------------------------------------------

def main(session,
         TARGET_DATABASE:        str,
         TARGET_SCHEMA:          str,
         TARGET_TABLE:           str  = 'SIGMA_DATA_MODEL_GRAPH',
         TRUNCATE_BEFORE_INSERT: bool = True):

    # Number of concurrent source-fetch threads. Increase carefully — higher
    # values fetch faster but raise the risk of hitting Sigma API rate limits.
    MAX_WORKERS = 10

    FQ_TABLE_SQL      = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    FQ_TABLE_SNOWPARK = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"

    # 1) Authenticate — fail fast on bad credentials before making bulk API calls
    token_mgr = SigmaTokenManager()
    token_mgr.get_token()

    # 2) List all data models
    all_data_models    = list_data_models(token_mgr)
    data_models_by_id  = {dm["dataModelId"]: dm for dm in all_data_models}

    # 3) Fetch sources for each data model concurrently
    #    upstream[dm_id] = set of parent data model IDs (data-model-type sources only)
    upstream     = defaultdict(set)
    fetch_errors = 0

    def fetch_sources(dm):
        dm_id = dm["dataModelId"]
        try:
            sources = get_data_model_sources(token_mgr, dm_id)
            return dm_id, sources, None
        except Exception as exc:
            return dm_id, [], str(exc)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_sources, dm): dm for dm in all_data_models}
        for future in as_completed(futures):
            dm_id, sources, error = future.result()
            if error:
                fetch_errors += 1
                continue
            for src in sources:
                # Normalise type strings: 'data-model', 'dataModel', 'data_model'
                src_type  = (src.get("type") or "").lower().replace("_", "-").replace(" ", "-")
                parent_id = src.get("dataModelId") or src.get("id")
                if src_type == "data-model" and parent_id:
                    upstream[dm_id].add(parent_id)

    # 4) Some parent data models may not appear in the list response (e.g. owned
    #    by a different user). Fetch them individually so the graph is complete.
    referenced_ids = {pid for pids in upstream.values() for pid in pids}
    missing_ids    = referenced_ids - set(data_models_by_id.keys())
    for dm_id in missing_ids:
        try:
            dm = get_data_model(token_mgr, dm_id)
            data_models_by_id[dm_id] = dm
        except Exception:
            # Insert a stub so every referenced ID has an entry in the lookup
            data_models_by_id[dm_id] = {"dataModelId": dm_id}

    # 5) Compute graph topology
    all_parents_set  = {pid for pids in upstream.values() for pid in pids}
    dms_with_parents = set(upstream.keys())

    upstream_parent_count  = {dm_id: len(pids) for dm_id, pids in upstream.items()}
    downstream_child_count = Counter(
        pid for pids in upstream.values() for pid in pids
    )

    fork_points  = {dm_id for dm_id, n in downstream_child_count.items() if n > 1}
    merge_points = {dm_id for dm_id, n in upstream_parent_count.items()   if n > 1}

    def graph_role(dm_id):
        """
        ROOT     — no upstream data model parents (sources directly from warehouse or has no DM parents)
        INTERNAL — has upstream DM parents and is also referenced as a parent by others
        LEAF     — has upstream DM parents but nothing else depends on it
        """
        has_parents  = dm_id in dms_with_parents
        has_children = dm_id in all_parents_set
        if not has_parents:
            return "ROOT"
        elif has_children:
            return "INTERNAL"
        else:
            return "LEAF"

    # 6) Create output table if it does not yet exist; truncate for snapshot mode
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE_SQL} (
            -- Run metadata
            RUN_ID                    STRING,
            CREATED_AT                TIMESTAMP_NTZ,

            -- Graph position of this data model in the dependency graph
            -- ROOT     = no upstream data model parents
            -- INTERNAL = has parents and is also a parent to others
            -- LEAF     = has parents but nothing depends on it
            RELATION_TYPE             STRING,

            -- Data model identity
            DATA_MODEL_ID             STRING,
            DATA_MODEL_NAME           STRING,
            DATA_MODEL_PATH           STRING,
            DATA_MODEL_URL            STRING,
            DATA_MODEL_CREATED_AT     TIMESTAMP_NTZ,
            DATA_MODEL_UPDATED_AT     TIMESTAMP_NTZ,

            -- Crossover metrics
            -- UPSTREAM_PARENT_COUNT > 1  = MERGE point (depends on multiple parent data models)
            -- DOWNSTREAM_CHILD_COUNT > 1 = FORK point  (multiple data models depend on this one)
            UPSTREAM_PARENT_COUNT     NUMBER,
            DOWNSTREAM_CHILD_COUNT    NUMBER,

            -- Direct upstream parent (NULL for ROOT rows)
            PARENT_DATA_MODEL_ID      STRING,
            PARENT_DATA_MODEL_NAME    STRING,
            PARENT_DATA_MODEL_PATH    STRING
        )
    """).collect()

    if TRUNCATE_BEFORE_INSERT:
        session.sql(f"TRUNCATE TABLE {FQ_TABLE_SQL}").collect()

    # 7) Build rows — one row per (data model, parent) edge; one row per ROOT
    run_id  = str(uuid.uuid4())
    now_ts  = datetime.now(timezone.utc).replace(tzinfo=None)
    rows    = []

    schema_cols = [
        "RUN_ID", "CREATED_AT",
        "RELATION_TYPE",
        "DATA_MODEL_ID", "DATA_MODEL_NAME", "DATA_MODEL_PATH", "DATA_MODEL_URL",
        "DATA_MODEL_CREATED_AT", "DATA_MODEL_UPDATED_AT",
        "UPSTREAM_PARENT_COUNT", "DOWNSTREAM_CHILD_COUNT",
        "PARENT_DATA_MODEL_ID", "PARENT_DATA_MODEL_NAME", "PARENT_DATA_MODEL_PATH",
    ]

    def make_row(dm_id, dm_meta, parent_id, parent_meta):
        return (
            run_id,
            now_ts,
            graph_role(dm_id),
            dm_id,
            dm_meta.get("name"),
            dm_meta.get("path"),
            dm_meta.get("url"),
            parse_ts(dm_meta.get("createdAt")),
            parse_ts(dm_meta.get("updatedAt")),
            upstream_parent_count.get(dm_id, 0),
            downstream_child_count.get(dm_id, 0),
            parent_id,
            parent_meta.get("name") if parent_meta else None,
            parent_meta.get("path") if parent_meta else None,
        )

    # INTERNAL and LEAF: one row per upstream parent edge
    for dm_id, parent_ids in upstream.items():
        dm_meta = data_models_by_id.get(dm_id, {"dataModelId": dm_id})
        for parent_id in parent_ids:
            parent_meta = data_models_by_id.get(parent_id)
            rows.append(make_row(dm_id, dm_meta, parent_id, parent_meta))

    # ROOT: one row per data model with NULL parent columns
    for dm_id, dm_meta in data_models_by_id.items():
        if dm_id not in dms_with_parents:
            rows.append(make_row(dm_id, dm_meta, None, None))

    root_rows     = [r for r in rows if r[2] == "ROOT"]
    internal_rows = [r for r in rows if r[2] == "INTERNAL"]
    leaf_rows     = [r for r in rows if r[2] == "LEAF"]

    # 8) Write to Snowflake
    if rows:
        df = session.create_dataframe(rows, schema=schema_cols)
        df.write.mode("append").save_as_table(FQ_TABLE_SNOWPARK)

    return (
        f"data_models_found={len(all_data_models)} | "
        f"root={len(root_rows)} | "
        f"internal={len(internal_rows)} | "
        f"leaf={len(leaf_rows)} | "
        f"fork_points={len(fork_points)} | "
        f"merge_points={len(merge_points)} | "
        f"fetch_errors={fetch_errors} | "
        f"total_rows_inserted={len(rows)} | "
        f"run_id={run_id}"
    )
$$;
