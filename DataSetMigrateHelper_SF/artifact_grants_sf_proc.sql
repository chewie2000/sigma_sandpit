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
--   DEPENDENCIES_TABLE     — Source table populated by sigma_dataset_dependencies()
--                            (optional, default: SIGMA_DATASET_DEPENDENCIES)
--   WORKBOOK_SUMMARY_TABLE — Source table populated by sigma_workbook_source_map()
--                            (optional, default: SIGMA_WORKBOOK_MIGRATION_SUMMARY)
--   GRANTS_TABLE           — Output table name (optional, default: SIGMA_ARTIFACT_GRANTS)
--   TRUNCATE_BEFORE_INSERT — TRUE = snapshot mode, replace on each run (recommended, default: TRUE)
--                            FALSE = append each run as a new RUN_ID; analysis queries
--                            always filter to MAX(RUN_ID) so results are correct either way.
--
--   SIGMA_BASE_URL, SIGMA_CLIENT_ID, and SIGMA_CLIENT_SECRET are all read at
--   runtime from Snowflake Secrets created in setup_prerequisites.sql.
--   They are never passed as parameters or hardcoded.
--
-- Requires:
--   sigma_dataset_dependencies()  must be run first (provides dataset and data model IDs)
--   sigma_workbook_source_map()   must be run first (provides workbook IDs)
--
-- Example call:
--   CALL sigma_artifact_grants('MY_DATABASE', 'MY_SCHEMA');
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_artifact_grants(
    TARGET_DATABASE        STRING,
    TARGET_SCHEMA          STRING,
    DEPENDENCIES_TABLE     STRING  DEFAULT 'SIGMA_DATASET_DEPENDENCIES',
    WORKBOOK_SUMMARY_TABLE STRING  DEFAULT 'SIGMA_WORKBOOK_MIGRATION_SUMMARY',
    GRANTS_TABLE           STRING  DEFAULT 'SIGMA_ARTIFACT_GRANTS',
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# MODULE-LEVEL SECRETS
# Read once at module load so all helpers below can reference them as globals.
# ------------------------------------------------------------------------------
SIGMA_BASE_URL      = _snowflake.get_generic_secret_string('sigma_base_url')
SIGMA_CLIENT_ID     = _snowflake.get_generic_secret_string('sigma_client_id')
SIGMA_CLIENT_SECRET = _snowflake.get_generic_secret_string('sigma_client_secret')

# ------------------------------------------------------------------------------
# TOKEN MANAGEMENT
# Lock-protected so concurrent threads can safely share one token manager.
# ------------------------------------------------------------------------------

class SigmaTokenManager:
    def __init__(self):
        self._token = None
        self._expires_at = 0.0
        self._lock = threading.Lock()

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
                time.sleep(2 ** (attempt + 1))
                continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp

# ------------------------------------------------------------------------------
# SIGMA API HELPERS
# ------------------------------------------------------------------------------

def list_teams(token_mgr):
    """List all teams org-wide via GET /v2/teams."""
    url    = f"{SIGMA_BASE_URL}/v2/teams"
    params = {"limit": 500}
    teams  = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        teams.extend(body.get("entries", []))
        next_page = body.get("nextPage") or body.get("nextPageToken")
        if not next_page:
            break
        params["page"] = next_page
    return teams


def list_members(token_mgr):
    """List all members org-wide via GET /v2/members."""
    url     = f"{SIGMA_BASE_URL}/v2/members"
    params  = {"limit": 500}
    members = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        members.extend(body.get("entries", []))
        next_page = body.get("nextPage") or body.get("nextPageToken")
        if not next_page:
            break
        params["page"] = next_page
    return members


def get_grants(token_mgr, inode_id):
    """
    Fetch all direct grants for a single artifact via GET /v2/grants.
    directGrantsOnly=true returns grants assigned directly to this artifact
    rather than inherited from parent folders.
    Paginated with limit=1000 (API maximum).
    """
    url    = f"{SIGMA_BASE_URL}/v2/grants"
    params = {"inodeId": inode_id, "directGrantsOnly": "true", "limit": 1000}
    grants = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        for key in ("grants", "entries", "items", "data"):
            chunk = body.get(key)
            if isinstance(chunk, list):
                grants.extend(chunk)
                break
        next_page = body.get("nextPage") or body.get("nextPageToken")
        if not next_page:
            break
        params["page"] = next_page
    return grants

# ------------------------------------------------------------------------------
# MAIN HANDLER
# Snowflake calls this function. All processing logic lives here.
# ------------------------------------------------------------------------------

def main(session,
         TARGET_DATABASE: str,
         TARGET_SCHEMA: str,
         DEPENDENCIES_TABLE: str     = 'SIGMA_DATASET_DEPENDENCIES',
         WORKBOOK_SUMMARY_TABLE: str = 'SIGMA_WORKBOOK_MIGRATION_SUMMARY',
         GRANTS_TABLE: str           = 'SIGMA_ARTIFACT_GRANTS',
         TRUNCATE_BEFORE_INSERT: bool = True):

    # Number of concurrent threads for grants fetching.
    # Increase if the Sigma API can handle higher concurrency; reduce if you hit 429s.
    MAX_WORKERS = 10

    FQ_DEPS_SQL        = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{DEPENDENCIES_TABLE}"'
    FQ_WB_SQL          = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{WORKBOOK_SUMMARY_TABLE}"'
    FQ_GRANTS_SQL      = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{GRANTS_TABLE}"'
    FQ_GRANTS_SNOWPARK = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{GRANTS_TABLE}"

    # 1) Authenticate — validate credentials early so we fail fast on bad config
    token_mgr = SigmaTokenManager()
    token_mgr.get_token()

    # 2) Pre-fetch all teams and members for grantee name hydration (single calls each)
    all_teams   = list_teams(token_mgr)
    all_members = list_members(token_mgr)

    teams_by_id = {
        t.get("teamId", "").strip().lower(): t
        for t in all_teams if t.get("teamId")
    }
    members_by_id = {
        m.get("memberId", "").strip().lower(): m
        for m in all_members if m.get("memberId")
    }

    # 3) Read artifact IDs from the output of the upstream procedures
    max_deps_run = session.sql(
        f"SELECT MAX(RUN_ID) FROM {FQ_DEPS_SQL}"
    ).collect()[0][0]

    max_wb_run = session.sql(
        f"SELECT MAX(RUN_ID) FROM {FQ_WB_SQL}"
    ).collect()[0][0]

    dep_rows = session.sql(f"""
        SELECT DISTINCT DATASET_ID, DATASET_NAME, DATASET_PATH, DATASET_URL
        FROM {FQ_DEPS_SQL}
        WHERE RUN_ID = '{max_deps_run}'
    """).collect()

    dm_rows = session.sql(f"""
        SELECT DISTINCT DATA_MODEL_ID, DATA_MODEL_NAME, DATA_MODEL_PATH, DATA_MODEL_URL
        FROM {FQ_DEPS_SQL}
        WHERE RUN_ID = '{max_deps_run}'
          AND DATA_MODEL_ID IS NOT NULL
    """).collect()

    wb_rows = session.sql(f"""
        SELECT DISTINCT WORKBOOK_ID, WORKBOOK_NAME, WORKBOOK_PATH, WORKBOOK_URL
        FROM {FQ_WB_SQL}
        WHERE RUN_ID = '{max_wb_run}'
    """).collect()

    # Flatten into a single artifact list: (type, id, name, path, url)
    artifacts = []
    for r in dep_rows:
        artifacts.append(("dataset",   r["DATASET_ID"],    r["DATASET_NAME"],    r["DATASET_PATH"],    r["DATASET_URL"]))
    for r in dm_rows:
        artifacts.append(("datamodel", r["DATA_MODEL_ID"], r["DATA_MODEL_NAME"], r["DATA_MODEL_PATH"], r["DATA_MODEL_URL"]))
    for r in wb_rows:
        artifacts.append(("workbook",  r["WORKBOOK_ID"],   r["WORKBOOK_NAME"],   r["WORKBOOK_PATH"],   r["WORKBOOK_URL"]))

    # 4) Ensure output table exists
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ_GRANTS_SQL} (
            -- Run metadata
            RUN_ID           STRING,
            CREATED_AT       TIMESTAMP_NTZ,

            -- Artifact the grant is on
            ARTIFACT_TYPE    STRING,        -- 'dataset' / 'datamodel' / 'workbook'
            ARTIFACT_ID      STRING,
            ARTIFACT_NAME    STRING,
            ARTIFACT_PATH    STRING,
            ARTIFACT_URL     STRING,

            -- Who the grant is for
            GRANTEE_TYPE     STRING,        -- 'team' / 'member'
            GRANTEE_ID       STRING,
            GRANTEE_NAME     STRING,

            -- What level of access they have
            PERMISSION_LEVEL STRING
        )
    """).collect()

    if TRUNCATE_BEFORE_INSERT:
        session.sql(f"TRUNCATE TABLE {FQ_GRANTS_SQL}").collect()

    # 5) Fetch grants for all artifacts concurrently
    run_id = str(uuid.uuid4())
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    def fetch_grants(artifact_type, artifact_id, artifact_name, artifact_path, artifact_url):
        """Fetch grants for a single artifact. Runs in a thread pool worker."""
        try:
            grants = get_grants(token_mgr, artifact_id)
            return artifact_type, artifact_id, artifact_name, artifact_path, artifact_url, grants, None
        except requests.HTTPError as e:
            return artifact_type, artifact_id, artifact_name, artifact_path, artifact_url, [], e

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_grants, atype, aid, aname, apath, aurl): (atype, aid)
            for atype, aid, aname, apath, aurl in artifacts
        }
        for future in as_completed(futures):
            results.append(future.result())

    # 6) Parse grants and build rows
    rows   = []
    failed = 0
    artifacts_with_grants = 0

    for artifact_type, artifact_id, artifact_name, artifact_path, artifact_url, grants, error in results:
        if error:
            failed += 1
            continue
        if grants:
            artifacts_with_grants += 1
        for grant in grants:
            # Infer grantee type from which ID field is populated
            team_id   = grant.get("teamId")
            member_id = grant.get("memberId") or grant.get("userId")
            if not (team_id or member_id):
                continue

            grantee_type = "team" if team_id else "member"
            grantee_id   = team_id or member_id

            # Hydrate display name from pre-fetched index
            if grantee_type == "team":
                t            = teams_by_id.get((grantee_id or "").strip().lower(), {})
                grantee_name = t.get("name") or t.get("teamName")
            else:
                m     = members_by_id.get((grantee_id or "").strip().lower(), {})
                first = m.get("firstName", "")
                last  = m.get("lastName",  "")
                full  = f"{first} {last}".strip()
                grantee_name = full or m.get("email") or m.get("name")

            # Permission level — defensive lookup; field name not confirmed in docs
            permission_level = (
                grant.get("permission") or
                grant.get("access")     or
                grant.get("level")      or
                grant.get("role")
            )

            rows.append((
                run_id,
                now_ts,
                artifact_type,
                artifact_id,
                artifact_name,
                artifact_path,
                artifact_url,
                grantee_type,
                grantee_id,
                grantee_name,
                permission_level,
            ))

    # 7) Write to Snowflake
    if rows:
        schema_cols = [
            "RUN_ID", "CREATED_AT",
            "ARTIFACT_TYPE", "ARTIFACT_ID", "ARTIFACT_NAME", "ARTIFACT_PATH", "ARTIFACT_URL",
            "GRANTEE_TYPE", "GRANTEE_ID", "GRANTEE_NAME",
            "PERMISSION_LEVEL",
        ]
        session.create_dataframe(rows, schema=schema_cols) \
               .write.mode("append").save_as_table(FQ_GRANTS_SNOWPARK)

    dataset_grants   = sum(1 for r in rows if r[2] == "dataset")
    datamodel_grants = sum(1 for r in rows if r[2] == "datamodel")
    workbook_grants  = sum(1 for r in rows if r[2] == "workbook")
    team_grants      = sum(1 for r in rows if r[7] == "team")
    member_grants    = sum(1 for r in rows if r[7] == "member")

    return (
        f"artifacts_queried={len(artifacts)} | "
        f"datasets={len(dep_rows)} | "
        f"data_models={len(dm_rows)} | "
        f"workbooks={len(wb_rows)} | "
        f"teams_resolved={len(teams_by_id)} | "
        f"members_resolved={len(members_by_id)} | "
        f"artifacts_with_grants={artifacts_with_grants} | "
        f"dataset_grants={dataset_grants} | "
        f"datamodel_grants={datamodel_grants} | "
        f"workbook_grants={workbook_grants} | "
        f"team_grants={team_grants} | "
        f"member_grants={member_grants} | "
        f"total_rows_inserted={len(rows)} | "
        f"failed_calls={failed} | "
        f"run_id={run_id}"
    )
$$;
