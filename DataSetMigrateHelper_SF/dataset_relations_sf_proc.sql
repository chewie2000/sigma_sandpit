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
--   TARGET_TABLE           — Output table name (optional, default: SIGMA_DATASET_DEPENDENCIES)
--   TRUNCATE_BEFORE_INSERT — TRUE = snapshot mode, replace on each run (optional, default: TRUE)
--
--   SIGMA_BASE_URL, SIGMA_CLIENT_ID, and SIGMA_CLIENT_SECRET are all read at
--   runtime from Snowflake Secrets created in setup_prerequisites.sql.
--   They are never passed as parameters or hardcoded.
--
-- Example call:
--   CALL sigma_dataset_dependencies('MY_DATABASE', 'MY_SCHEMA');
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_dataset_dependencies(
    TARGET_DATABASE        STRING,
    TARGET_SCHEMA          STRING,
    TARGET_TABLE           STRING DEFAULT 'SIGMA_DATASET_DEPENDENCIES',
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
import uuid
import time
from collections import defaultdict
from datetime import datetime, timezone

def main(session,
         TARGET_DATABASE: str,
         TARGET_SCHEMA: str,
         TARGET_TABLE: str = 'SIGMA_DATASET_DEPENDENCIES',
         TRUNCATE_BEFORE_INSERT: bool = True):

    # All Sigma API configuration is read from Snowflake Secrets at runtime.
    # Secrets are created in setup_prerequisites.sql.
    SIGMA_BASE_URL      = _snowflake.get_generic_secret_string('sigma_base_url')
    SIGMA_CLIENT_ID     = _snowflake.get_generic_secret_string('sigma_client_id')
    SIGMA_CLIENT_SECRET = _snowflake.get_generic_secret_string('sigma_client_secret')

    # Pause between per-dataset API calls. Increase if Sigma rate-limits you.
    API_CALL_DELAY_SECONDS = 0.1

    FQ_TABLE_SQL      = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{TARGET_TABLE}"'
    FQ_TABLE_SNOWPARK = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"

# ------------------------------------------------------------------------------
# TOKEN MANAGEMENT
# ------------------------------------------------------------------------------

class SigmaTokenManager:
    """Fetches and transparently refreshes the Sigma OAuth token before it expires."""

    def __init__(self):
        self._token = None
        self._expires_at = 0.0

    def get_token(self):
        if time.time() >= self._expires_at - 60:
            self._fetch()
        return self._token

    def _fetch(self):
        resp = requests.post(
            f"{SIGMA_BASE_URL}/v2/auth/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": SIGMA_CLIENT_ID,
                "client_secret": SIGMA_CLIENT_SECRET,
            },
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()
        self._token = body["access_token"]
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
                wait = 2 ** (attempt + 1)
                time.sleep(wait)
                continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp

# ------------------------------------------------------------------------------
# SIGMA API HELPERS
# Note: these endpoints are deprecated by Sigma but used intentionally here
# because they expose migrationStatus, required for migration planning.
# ------------------------------------------------------------------------------

def list_datasets(token_mgr):
    """
    List all datasets org-wide via GET /v2/datasets.
    skipPermissionCheck=true requires admin credentials.
    Each dataset includes migrationStatus: "not-migrated" | "migrated" | "not-required"
    """
    url = f"{SIGMA_BASE_URL}/v2/datasets"
    params = {"skipPermissionCheck": "true", "limit": 500}
    datasets = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        datasets.extend(body.get("entries", []))
        next_token = body.get("nextPageToken") or body.get("nextPage")
        if not next_token:
            break
        params["page"] = next_token
    return datasets


def list_dataset_sources(token_mgr, dataset_id):
    """List sources for a single dataset via GET /v2/datasets/{id}/sources."""
    url = f"{SIGMA_BASE_URL}/v2/datasets/{dataset_id}/sources"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    return _get_with_backoff(url, headers=headers).json()


def list_data_models(token_mgr):
    """
    List all data models org-wide via GET /v2/datamodels.
    Note: this endpoint does not support skipPermissionCheck, so it only
    returns data models owned by the API credential user.
    """
    url = f"{SIGMA_BASE_URL}/v2/datamodels"
    params = {"limit": 500}
    data_models = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        data_models.extend(body.get("entries", []))
        next_token = body.get("nextPageToken") or body.get("nextPage")
        if not next_token:
            break
        params["page"] = next_token
    return data_models


def get_data_model(token_mgr, data_model_id):
    """Fetch a single data model by ID via GET /v2/datamodels/{dataModelId}."""
    url = f"{SIGMA_BASE_URL}/v2/datamodels/{data_model_id}"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    return _get_with_backoff(url, headers=headers).json()

    # 1) Authenticate — validate credentials early so we fail fast on bad config
    token_mgr = SigmaTokenManager()
    token_mgr.get_token()

    # 2) List all datasets and data models
    all_data_models = list_data_models(token_mgr)
    data_models_by_id = {dm["dataModelId"]: dm for dm in all_data_models}

    # 2b) Collect dataModelIds referenced by migrated datasets that are
    #     missing from the list response (owned by other users)
    all_datasets = list_datasets(token_mgr)
    missing_dm_ids = set()
    for d in all_datasets:
        dm_id = (d.get("migrationToDataModel") or {}).get("dataModelId")
        if dm_id and dm_id not in data_models_by_id:
            missing_dm_ids.add(dm_id)

    # Fetch missing data models individually
    for dm_id in missing_dm_ids:
        try:
            dm = get_data_model(token_mgr, dm_id)
            data_models_by_id[dm_id] = dm
        except requests.HTTPError:
            pass
        time.sleep(API_CALL_DELAY_SECONDS)

    # Index by both datasetId and inodeId so source lookups match either key
    datasets_by_id = {}
    for d in all_datasets:
        datasets_by_id[d["datasetId"]] = d
        if d.get("inodeId"):
            datasets_by_id[d["inodeId"]] = d

    # Graph: child dataset -> set of parent dataset IDs (direct upstream only)
    upstream = defaultdict(set)

    # Capture a sample source item for diagnostics
    sample_source = None

    # 3) Fetch sources for each dataset and record dataset->dataset edges
    for i, ds in enumerate(all_datasets, start=1):
        ds_id   = ds["datasetId"]
        ds_name = ds.get("name", ds_id)

        try:
            sources = list_dataset_sources(token_mgr, ds_id)
        except requests.HTTPError:
            continue

        source_list = sources if isinstance(sources, list) else sources.get("sources", [])
        for src in source_list:
            if sample_source is None:
                sample_source = str(src)  # capture first source seen for diagnostics
            if src.get("type") == "dataset":
                parent_id = src.get("inodeId") or src.get("datasetId") or src.get("id")
                if parent_id and parent_id in datasets_by_id:
                    upstream[ds_id].add(parent_id)

        time.sleep(API_CALL_DELAY_SECONDS)

    # 4) Ensure target table exists
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE_SQL} (
            -- Run metadata
            RUN_ID                   STRING,
            CREATED_AT               TIMESTAMP_NTZ,

            -- Graph position of this dataset
            RELATION_TYPE            STRING,

            -- Dataset attributes
            DATASET_ID               STRING,
            DATASET_NAME             STRING,
            DATASET_PATH             STRING,
            DATASET_URL              STRING,
            DATASET_CREATED_BY       STRING,
            DATASET_OWNER            STRING,
            DATASET_MIGRATION_STATUS STRING,

            -- Migration event (when/who migrated this dataset to a data model)
            MIGRATED_AT              TIMESTAMP_NTZ,
            MIGRATED_BY              STRING,

            -- Target data model attributes (NULL if not yet migrated)
            DATA_MODEL_ID            STRING,
            DATA_MODEL_NAME          STRING,
            DATA_MODEL_URL           STRING,
            DATA_MODEL_PATH          STRING,
            DATA_MODEL_CREATED_AT    TIMESTAMP_NTZ,
            DATA_MODEL_UPDATED_AT    TIMESTAMP_NTZ,

            -- Parent dataset (NULL for ROOT rows)
            PARENT_ID                STRING,
            PARENT_NAME              STRING,
            PARENT_MIGRATION_STATUS  STRING,

            -- Crossover analysis
            -- UPSTREAM_PARENT_COUNT > 1  = MERGE point (depends on multiple parents)
            -- DOWNSTREAM_CHILD_COUNT > 1 = FORK point  (feeds multiple downstream datasets)
            UPSTREAM_PARENT_COUNT    NUMBER,
            DOWNSTREAM_CHILD_COUNT   NUMBER
        )
    """).collect()

    if TRUNCATE_BEFORE_INSERT:
        session.sql(f"TRUNCATE TABLE {FQ_TABLE_SQL}").collect()

    # 5) Compute graph roles and crossover metrics
    all_parents = {pid for parent_ids in upstream.values() for pid in parent_ids}
    datasets_with_parents = set(upstream.keys())

    # UPSTREAM_PARENT_COUNT: how many dataset parents each dataset has
    upstream_parent_count = {ds_id: len(pids) for ds_id, pids in upstream.items()}

    # DOWNSTREAM_CHILD_COUNT: how many datasets directly source from each dataset
    from collections import Counter
    downstream_child_count = Counter(
        pid for parent_ids in upstream.values() for pid in parent_ids
    )

    fork_points  = {ds_id for ds_id, n in downstream_child_count.items() if n > 1}
    merge_points = {ds_id for ds_id, n in upstream_parent_count.items()   if n > 1}

    def graph_role(ds_id):
        """
        ROOT     — no upstream dataset parents (top of chain / sources tables directly)
        INTERNAL — has upstream parents and is also a parent (intermediate node)
        LEAF     — has upstream parents but nothing depends on it (bottom child)
        """
        has_parents  = ds_id in datasets_with_parents
        has_children = ds_id in all_parents
        if not has_parents:
            return "ROOT"
        elif has_children:
            return "INTERNAL"
        else:
            return "LEAF"

    def parse_ts(iso_str):
        """Parse ISO 8601 timestamp string to a tz-naive datetime for TIMESTAMP_NTZ."""
        if not iso_str:
            return None
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)

    # 6) Build rows
    run_id = str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    rows = []

    def migration_fields(ds_meta):
        """Return (MIGRATED_AT, MIGRATED_BY) from migrationToDataModel."""
        mig = ds_meta.get("migrationToDataModel") or {}
        return (
            parse_ts(mig.get("migratedAt")),
            mig.get("migratedBy"),
        )

    def data_model_fields(ds_meta):
        """Return 6 data model attribute columns, looked up from data_models_by_id."""
        dm_id = (ds_meta.get("migrationToDataModel") or {}).get("dataModelId")
        dm    = data_models_by_id.get(dm_id, {}) if dm_id else {}
        return (
            dm_id,
            dm.get("name"),
            dm.get("url"),
            dm.get("path"),
            parse_ts(dm.get("createdAt")),
            parse_ts(dm.get("updatedAt")),
        )

    def make_row(run_id, now_ts, role, ds_id, ds_meta, parent_id, parent_meta):
        return (
            # Run metadata
            run_id,
            now_ts,
            # Graph position
            role,
            # Dataset attributes
            ds_id,
            ds_meta.get("name", ds_id),
            ds_meta.get("path"),
            ds_meta.get("url"),
            ds_meta.get("createdBy"),
            ds_meta.get("owner"),
            ds_meta.get("migrationStatus"),
            # Migration event
            *migration_fields(ds_meta),
            # Data model attributes
            *data_model_fields(ds_meta),
            # Parent dataset
            parent_id,
            parent_meta.get("name", parent_id) if parent_meta else None,
            parent_meta.get("migrationStatus") if parent_meta else None,
            # Crossover analysis
            upstream_parent_count.get(ds_id, 0),
            downstream_child_count.get(ds_id, 0),
        )

    # Datasets with at least one upstream parent (INTERNAL or LEAF)
    for ds_id, parent_ids in upstream.items():
        ds_meta = datasets_by_id.get(ds_id, {})
        role    = graph_role(ds_id)
        for parent_id in parent_ids:
            parent_meta = datasets_by_id.get(parent_id, {})
            rows.append(make_row(run_id, now_ts, role, ds_id, ds_meta, parent_id, parent_meta))

    # ROOT datasets — no upstream dataset dependencies; PARENT_* columns are NULL
    for ds_id, ds_meta in datasets_by_id.items():
        if ds_id not in datasets_with_parents:
            rows.append(make_row(run_id, now_ts, "ROOT", ds_id, ds_meta, None, None))

    root_rows     = [r for r in rows if r[2] == "ROOT"]
    internal_rows = [r for r in rows if r[2] == "INTERNAL"]
    leaf_rows     = [r for r in rows if r[2] == "LEAF"]

    # 7) Write to Snowflake
    if rows:
        schema_cols = [
            "RUN_ID", "CREATED_AT",
            "RELATION_TYPE",
            "DATASET_ID", "DATASET_NAME", "DATASET_PATH", "DATASET_URL", "DATASET_CREATED_BY", "DATASET_OWNER", "DATASET_MIGRATION_STATUS",
            "MIGRATED_AT", "MIGRATED_BY",
            "DATA_MODEL_ID", "DATA_MODEL_NAME", "DATA_MODEL_URL", "DATA_MODEL_PATH",
            "DATA_MODEL_CREATED_AT", "DATA_MODEL_UPDATED_AT",
            "PARENT_ID", "PARENT_NAME", "PARENT_MIGRATION_STATUS",
            "UPSTREAM_PARENT_COUNT", "DOWNSTREAM_CHILD_COUNT",
        ]
        df = session.create_dataframe(rows, schema=schema_cols)
        df.write.mode("append").save_as_table(FQ_TABLE_SNOWPARK)

    return (
        f"datasets_found={len(all_datasets)} | "
        f"data_models_found={len(all_data_models)} | "
        f"root={len(root_rows)} | "
        f"internal={len(internal_rows)} | "
        f"leaf={len(leaf_rows)} | "
        f"fork_points={len(fork_points)} | "
        f"merge_points={len(merge_points)} | "
        f"total_inserted={len(rows)} | "
        f"run_id={run_id}"
    )
$$;

