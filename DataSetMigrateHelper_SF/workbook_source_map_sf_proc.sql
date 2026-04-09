-- ==============================================================================
-- Prerequisites
-- Run setup_prerequisites.sql as ACCOUNTADMIN before deploying this procedure.
-- That script creates the network rule, Snowflake Secrets, external access
-- integration, and all required grants.
-- ==============================================================================

-- ==============================================================================
-- Create the stored procedure
-- Edit the non-credential configuration constants inside the Python block
-- before running (SIGMA_BASE_URL, TARGET_DATABASE, TARGET_SCHEMA).
-- Credentials are read at runtime from Snowflake Secrets — do not hardcode them.
--
-- Creates two tables:
--   SIGMA_WORKBOOK_MIGRATION_SUMMARY  — one row per workbook with source counts
--                                       and overall migration status
--                                       (only workbooks with migration-scope sources)
--   SIGMA_WORKBOOK_SOURCE_DETAILS     — one row per workbook → source, enriched
--                                       with migration data from SIGMA_DATASET_DEPENDENCIES
--
-- Requires: SIGMA_DATASET_DEPENDENCIES must already be populated by
--           sigma_dataset_dependencies() before calling this procedure.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_workbook_source_map()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (sigma_api_access)
SECRETS = ('sigma_client_id' = sigma_client_id, 'sigma_client_secret' = sigma_client_secret)
HANDLER = 'main'
AS
$$
import _snowflake
import requests
import uuid
import time
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# CONFIGURATION — EDIT THESE VALUES
# ------------------------------------------------------------------------------

SIGMA_BASE_URL      = "https://api.staging.us.aws.sigmacomputing.io"

# Credentials are read from Snowflake Secrets at runtime — never hardcoded.
# Secrets are created in setup_prerequisites.sql.
SIGMA_CLIENT_ID     = _snowflake.get_generic_secret_string('sigma_client_id')
SIGMA_CLIENT_SECRET = _snowflake.get_generic_secret_string('sigma_client_secret')

TARGET_DATABASE = "YOUR_DATABASE"
TARGET_SCHEMA   = "YOUR_SCHEMA"

SUMMARY_TABLE      = "SIGMA_WORKBOOK_MIGRATION_SUMMARY"
DETAILS_TABLE      = "SIGMA_WORKBOOK_SOURCE_DETAILS"
DEPENDENCIES_TABLE = "SIGMA_DATASET_DEPENDENCIES"

# True  = truncate and replace on each run (snapshot mode — recommended)
# False = append; filter by RUN_ID or CREATED_AT for the latest run
TRUNCATE_BEFORE_INSERT = True

# Pause between per-workbook API calls. Increase if Sigma rate-limits you.
API_CALL_DELAY_SECONDS = 0.1

FQ_SUMMARY_SQL      = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{SUMMARY_TABLE}"'
FQ_DETAILS_SQL      = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{DETAILS_TABLE}"'
FQ_DEPS_SQL         = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{DEPENDENCIES_TABLE}"'
FQ_SUMMARY_SNOWPARK = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{SUMMARY_TABLE}"
FQ_DETAILS_SNOWPARK = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{DETAILS_TABLE}"

# ------------------------------------------------------------------------------
# TOKEN MANAGEMENT
# ------------------------------------------------------------------------------

class SigmaTokenManager:
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
# ------------------------------------------------------------------------------

def list_workbooks(token_mgr):
    """
    List all workbooks org-wide via GET /v2/workbooks.
    skipPermissionCheck=true requires admin credentials.
    Paginated via nextPage / hasMore.
    """
    url = f"{SIGMA_BASE_URL}/v2/workbooks"
    params = {"skipPermissionCheck": "true", "limit": 50}
    workbooks = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        workbooks.extend(body.get("entries", []))
        if not body.get("hasMore"):
            break
        params["page"] = body.get("nextPage")
    return workbooks


def get_workbook_sources(token_mgr, workbook_id):
    """Get all sources for a single workbook via GET /v2/workbooks/{id}/sources."""
    url = f"{SIGMA_BASE_URL}/v2/workbooks/{workbook_id}/sources"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    return _get_with_backoff(url, headers=headers).json()

# ------------------------------------------------------------------------------
# HELPERS
# ------------------------------------------------------------------------------

def parse_ts(iso_str):
    if not iso_str:
        return None
    return datetime.fromisoformat(iso_str.replace("Z", "+00:00")).replace(tzinfo=None)


def load_dependency_lookups(session):
    """
    Read the latest run from SIGMA_DATASET_DEPENDENCIES and build two dicts:
      datasets_by_id  — keyed by DATASET_ID → row dict
      data_models_by_id — keyed by DATA_MODEL_ID → row dict
    These are used to match workbook sources back to the migration exercise.
    """
    rows = session.sql(f"""
        SELECT
            DATASET_ID,
            DATASET_NAME,
            DATASET_PATH,
            DATASET_URL,
            DATASET_MIGRATION_STATUS,
            RELATION_TYPE,
            MIGRATED_AT,
            MIGRATED_BY,
            DATA_MODEL_ID,
            DATA_MODEL_NAME,
            DATA_MODEL_URL,
            DATA_MODEL_PATH,
            UPSTREAM_PARENT_COUNT,
            DOWNSTREAM_CHILD_COUNT
        FROM {FQ_DEPS_SQL}
        WHERE RUN_ID = (SELECT MAX(RUN_ID) FROM {FQ_DEPS_SQL})
    """).collect()

    datasets_by_id = {}
    data_models_by_id = {}
    for r in rows:
        ds_id = r["DATASET_ID"]
        if ds_id not in datasets_by_id:
            datasets_by_id[ds_id] = {
                "DATASET_ID":               ds_id,
                "DATASET_NAME":             r["DATASET_NAME"],
                "DATASET_PATH":             r["DATASET_PATH"],
                "DATASET_URL":              r["DATASET_URL"],
                "DATASET_MIGRATION_STATUS": r["DATASET_MIGRATION_STATUS"],
                "RELATION_TYPE":            r["RELATION_TYPE"],
                "MIGRATED_AT":              r["MIGRATED_AT"],
                "MIGRATED_BY":              r["MIGRATED_BY"],
                "DATA_MODEL_ID":            r["DATA_MODEL_ID"],
                "DATA_MODEL_NAME":          r["DATA_MODEL_NAME"],
                "DATA_MODEL_URL":           r["DATA_MODEL_URL"],
                "DATA_MODEL_PATH":          r["DATA_MODEL_PATH"],
                "UPSTREAM_PARENT_COUNT":    r["UPSTREAM_PARENT_COUNT"],
                "DOWNSTREAM_CHILD_COUNT":   r["DOWNSTREAM_CHILD_COUNT"],
            }
        dm_id = r["DATA_MODEL_ID"]
        if dm_id and dm_id not in data_models_by_id:
            data_models_by_id[dm_id] = datasets_by_id[ds_id]

    return datasets_by_id, data_models_by_id

# ------------------------------------------------------------------------------
# MAIN HANDLER
# ------------------------------------------------------------------------------

def main(session):
    # 1) Authenticate
    token_mgr = SigmaTokenManager()
    token_mgr.get_token()

    # 2) Load migration-scope lookups from SIGMA_DATASET_DEPENDENCIES
    datasets_by_id, data_models_by_id = load_dependency_lookups(session)
    known_dataset_ids    = set(datasets_by_id.keys())
    known_data_model_ids = set(data_models_by_id.keys())

    # 3) List all workbooks
    all_workbooks = list_workbooks(token_mgr)

    # 4) Ensure tables exist
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ_SUMMARY_SQL} (
            -- Run metadata
            RUN_ID                   STRING,
            CREATED_AT               TIMESTAMP_NTZ,

            -- Workbook attributes
            WORKBOOK_ID              STRING,
            WORKBOOK_NAME            STRING,
            WORKBOOK_URL             STRING,
            WORKBOOK_PATH            STRING,
            OWNER_NAME               STRING,
            OWNER_EMAIL              STRING,
            WORKBOOK_CREATED_AT      TIMESTAMP_NTZ,
            WORKBOOK_UPDATED_AT      TIMESTAMP_NTZ,

            -- Source counts (migration-scope sources only)
            TOTAL_SOURCES            NUMBER,
            DATASET_SOURCE_COUNT     NUMBER,
            DATA_MODEL_SOURCE_COUNT  NUMBER,
            TABLE_SOURCE_COUNT       NUMBER,

            -- Overall migration status across migration-scope sources
            -- 'FULLY MIGRATED'     = all in-scope sources are data-model
            -- 'PARTIALLY MIGRATED' = mix of dataset and data-model sources
            -- 'NOT MIGRATED'       = all in-scope sources are dataset
            -- 'NO SOURCES'         = workbook has no migration-scope sources
            MIGRATION_STATUS         STRING
        )
    """).collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ_DETAILS_SQL} (
            -- Run metadata
            RUN_ID                   STRING,
            CREATED_AT               TIMESTAMP_NTZ,

            -- Workbook
            WORKBOOK_ID              STRING,
            WORKBOOK_NAME            STRING,
            WORKBOOK_PATH            STRING,

            -- Source from workbook API
            SOURCE_INODE_ID          STRING,
            SOURCE_DATA_MODEL_ID     STRING,
            SOURCE_TYPE              STRING,
            IN_MIGRATION_SCOPE       BOOLEAN,

            -- Enriched from SIGMA_DATASET_DEPENDENCIES
            DATASET_ID               STRING,
            DATASET_NAME             STRING,
            DATASET_PATH             STRING,
            DATASET_URL              STRING,
            DATASET_MIGRATION_STATUS STRING,
            DATASET_RELATION_TYPE    STRING,
            MIGRATED_AT              TIMESTAMP_NTZ,
            MIGRATED_BY              STRING,
            DATA_MODEL_ID            STRING,
            DATA_MODEL_NAME          STRING,
            DATA_MODEL_URL           STRING,
            DATA_MODEL_PATH          STRING,
            UPSTREAM_PARENT_COUNT    NUMBER,
            DOWNSTREAM_CHILD_COUNT   NUMBER
        )
    """).collect()

    if TRUNCATE_BEFORE_INSERT:
        session.sql(f"TRUNCATE TABLE {FQ_SUMMARY_SQL}").collect()
        session.sql(f"TRUNCATE TABLE {FQ_DETAILS_SQL}").collect()

    # 5) Fetch sources per workbook and build rows
    run_id  = str(uuid.uuid4())
    now_ts  = datetime.now(timezone.utc).replace(tzinfo=None)

    summary_rows = []
    detail_rows  = []

    failed = 0

    for wb in all_workbooks:
        wb_id   = wb.get("workbookId")
        wb_name = wb.get("name", wb_id)
        wb_url  = wb.get("url")
        wb_path = wb.get("path")
        owner   = wb.get("owner") or {}

        try:
            body    = get_workbook_sources(token_mgr, wb_id)
            sources = body if isinstance(body, list) else body.get("entries", [])
        except requests.HTTPError:
            failed += 1
            sources = []

        dataset_count    = 0
        data_model_count = 0
        table_count      = 0
        wb_detail_rows   = []

        for src in sources:
            src_type     = src.get("type", "")
            inode_id     = src.get("inodeId")
            dm_id        = src.get("dataModelId")
            in_scope     = False
            dep_row      = None

            if src_type == "dataset" and inode_id in known_dataset_ids:
                in_scope = True
                dep_row  = datasets_by_id[inode_id]
                dataset_count += 1
            elif src_type == "data-model" and dm_id in known_data_model_ids:
                in_scope = True
                dep_row  = data_models_by_id[dm_id]
                data_model_count += 1
            elif src_type == "table":
                table_count += 1

            if in_scope:
                wb_detail_rows.append((
                    run_id,
                    now_ts,
                    wb_id,
                    wb_name,
                    wb_path,
                    # Source from API
                    inode_id,
                    dm_id,
                    src_type,
                    in_scope,
                    # Enriched from SIGMA_DATASET_DEPENDENCIES
                    dep_row.get("DATASET_ID"),
                    dep_row.get("DATASET_NAME"),
                    dep_row.get("DATASET_PATH"),
                    dep_row.get("DATASET_URL"),
                    dep_row.get("DATASET_MIGRATION_STATUS"),
                    dep_row.get("RELATION_TYPE"),
                    dep_row.get("MIGRATED_AT"),
                    dep_row.get("MIGRATED_BY"),
                    dep_row.get("DATA_MODEL_ID"),
                    dep_row.get("DATA_MODEL_NAME"),
                    dep_row.get("DATA_MODEL_URL"),
                    dep_row.get("DATA_MODEL_PATH"),
                    dep_row.get("UPSTREAM_PARENT_COUNT"),
                    dep_row.get("DOWNSTREAM_CHILD_COUNT"),
                ))

        # Only include workbooks that have at least one migration-scope source
        in_scope_total = dataset_count + data_model_count
        if in_scope_total > 0:
            if data_model_count == in_scope_total:
                migration_status = "FULLY MIGRATED"
            elif dataset_count == in_scope_total:
                migration_status = "NOT MIGRATED"
            else:
                migration_status = "PARTIALLY MIGRATED"

            summary_rows.append((
                run_id,
                now_ts,
                wb_id,
                wb_name,
                wb_url,
                wb_path,
                owner.get("name"),
                owner.get("email"),
                parse_ts(wb.get("createdAt")),
                parse_ts(wb.get("updatedAt")),
                in_scope_total,
                dataset_count,
                data_model_count,
                table_count,
                migration_status,
            ))

            detail_rows.extend(wb_detail_rows)

        time.sleep(API_CALL_DELAY_SECONDS)

    # 6) Write summary table
    if summary_rows:
        summary_cols = [
            "RUN_ID", "CREATED_AT",
            "WORKBOOK_ID", "WORKBOOK_NAME", "WORKBOOK_URL", "WORKBOOK_PATH",
            "OWNER_NAME", "OWNER_EMAIL",
            "WORKBOOK_CREATED_AT", "WORKBOOK_UPDATED_AT",
            "TOTAL_SOURCES", "DATASET_SOURCE_COUNT", "DATA_MODEL_SOURCE_COUNT",
            "TABLE_SOURCE_COUNT",
            "MIGRATION_STATUS",
        ]
        session.create_dataframe(summary_rows, schema=summary_cols) \
               .write.mode("append").save_as_table(FQ_SUMMARY_SNOWPARK)

    # 7) Write details table
    if detail_rows:
        detail_cols = [
            "RUN_ID", "CREATED_AT",
            "WORKBOOK_ID", "WORKBOOK_NAME", "WORKBOOK_PATH",
            "SOURCE_INODE_ID", "SOURCE_DATA_MODEL_ID", "SOURCE_TYPE",
            "IN_MIGRATION_SCOPE",
            "DATASET_ID", "DATASET_NAME", "DATASET_PATH", "DATASET_URL",
            "DATASET_MIGRATION_STATUS", "DATASET_RELATION_TYPE",
            "MIGRATED_AT", "MIGRATED_BY",
            "DATA_MODEL_ID", "DATA_MODEL_NAME", "DATA_MODEL_URL", "DATA_MODEL_PATH",
            "UPSTREAM_PARENT_COUNT", "DOWNSTREAM_CHILD_COUNT",
        ]
        session.create_dataframe(detail_rows, schema=detail_cols) \
               .write.mode("append").save_as_table(FQ_DETAILS_SNOWPARK)

    fully_migrated = sum(1 for r in summary_rows if r[14] == "FULLY MIGRATED")
    partially      = sum(1 for r in summary_rows if r[14] == "PARTIALLY MIGRATED")
    not_migrated   = sum(1 for r in summary_rows if r[14] == "NOT MIGRATED")

    return (
        f"workbooks_scanned={len(all_workbooks)} | "
        f"workbooks_in_scope={len(summary_rows)} | "
        f"fully_migrated={fully_migrated} | "
        f"partially_migrated={partially} | "
        f"not_migrated={not_migrated} | "
        f"source_relationships={len(detail_rows)} | "
        f"datasets_loaded={len(datasets_by_id)} | "
        f"data_models_loaded={len(data_models_by_id)} | "
        f"failed_source_calls={failed} | "
        f"run_id={run_id}"
    )
$$;

