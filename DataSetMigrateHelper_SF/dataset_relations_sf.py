# Single Snowflake Notebook / Snowpark script to:
# - Call Sigma REST API (v2/datasets — deprecated but used intentionally for migration planning)
# - Build dataset -> dataset dependencies (upstream, direct)
# - Capture migrationStatus from List datasets
# - Write results into a Snowflake table in a configurable DATABASE.SCHEMA

from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import requests
import uuid
import time
from collections import defaultdict
from datetime import datetime, timezone

# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------

# If you are in a Snowflake Notebook, `session` is already defined.
# If running externally, uncomment and configure the connection below.

# connection_parameters = {
#     "account": "...",
#     "user": "...",
#     "password": "...",
#     "role": "...",
#     "warehouse": "...",
#     "database": "...",
#     "schema": "...",
# }
# session = Session.builder.configs(connection_parameters).create()

# ---- Sigma API configuration: EDIT THESE VALUES ----
SIGMA_BASE_URL      = "https://api.sigmacomputing.com"
SIGMA_CLIENT_ID     = "YOUR_SIGMA_CLIENT_ID"
SIGMA_CLIENT_SECRET = "YOUR_SIGMA_CLIENT_SECRET"

# ---- Target location in Snowflake: EDIT THESE VALUES ----
TARGET_DATABASE = "YOUR_DATABASE"
TARGET_SCHEMA   = "YOUR_SCHEMA"
TARGET_TABLE    = "SIGMA_DATASET_DEPENDENCIES"

# Set True to replace table contents on each run (snapshot mode — recommended).
# Set False to append and keep full history; filter by RUN_ID or CREATED_AT for the latest run.
TRUNCATE_BEFORE_INSERT = True

# Pause between per-dataset API calls (seconds). Increase if Sigma rate-limits you.
API_CALL_DELAY_SECONDS = 0.1

# Fully-qualified identifiers in two forms:
#   SQL strings  → quoted identifiers preserve case and special characters
#   Snowpark API → unquoted dotted notation (session.table / save_as_table)
FQ_TABLE_SQL      = f'"{TARGET_DATABASE}"."{TARGET_SCHEMA}"."{TARGET_TABLE}"'
FQ_TABLE_SNOWPARK = f"{TARGET_DATABASE}.{TARGET_SCHEMA}.{TARGET_TABLE}"

# ------------------------------------------------------------------------------
# TOKEN MANAGEMENT
# ------------------------------------------------------------------------------

class SigmaTokenManager:
    """Fetches and transparently refreshes the Sigma OAuth token before it expires."""

    def __init__(self):
        self._token: str | None = None
        self._expires_at: float = 0.0

    def get_token(self) -> str:
        if time.time() >= self._expires_at - 60:   # refresh 60 s before expiry
            self._fetch()
        return self._token  # type: ignore[return-value]

    def _fetch(self):
        url = f"{SIGMA_BASE_URL}/v2/auth/token"
        resp = requests.post(
            url,
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
        print("Sigma access token refreshed.")


# ------------------------------------------------------------------------------
# HTTP HELPER
# ------------------------------------------------------------------------------

def _get_with_backoff(url: str, headers: dict, params: dict | None = None,
                      max_retries: int = 4, timeout: int = 60) -> requests.Response:
    """GET with exponential backoff on 429 (rate-limited) and transient 5xx errors."""
    for attempt in range(max_retries):
        resp = requests.get(url, headers=headers, params=params, timeout=timeout)
        if resp.status_code == 429 or 500 <= resp.status_code < 600:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"HTTP {resp.status_code} — retrying in {wait}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
                continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()  # unreachable but satisfies type checkers
    return resp              # type: ignore[return-value]


# ------------------------------------------------------------------------------
# SIGMA API HELPERS
# Note: these endpoints are deprecated by Sigma but are used here intentionally
# because they expose migrationStatus, which is required for migration planning.
# ------------------------------------------------------------------------------

def list_datasets(token_mgr: SigmaTokenManager) -> list[dict]:
    """
    List all datasets org-wide via GET /v2/datasets.
    skipPermissionCheck=true requires admin credentials.
    Each dataset includes migrationStatus: "not-migrated" | "migrated" | "not-required"
    """
    url = f"{SIGMA_BASE_URL}/v2/datasets"
    params: dict = {"skipPermissionCheck": "true", "pageSize": 500}
    datasets: list[dict] = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        datasets.extend(body.get("items", []))
        next_token = body.get("nextPageToken")
        if not next_token:
            break
        params["pageToken"] = next_token
    return datasets


def list_dataset_sources(token_mgr: SigmaTokenManager, dataset_id: str) -> dict:
    """List sources for a single dataset via GET /v2/datasets/{id}/sources."""
    url = f"{SIGMA_BASE_URL}/v2/datasets/{dataset_id}/sources"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    return _get_with_backoff(url, headers=headers).json()


# ------------------------------------------------------------------------------
# MAIN LOGIC
# ------------------------------------------------------------------------------

def main(session: Session):
    session.sql(f'USE DATABASE "{TARGET_DATABASE}"').collect()
    session.sql(f'USE SCHEMA "{TARGET_SCHEMA}"').collect()

    # 1) Authenticate — validate credentials early so we fail fast on bad config
    token_mgr = SigmaTokenManager()
    token_mgr.get_token()

    # 2) List all datasets (includes migrationStatus)
    all_datasets = list_datasets(token_mgr)
    print(f"Found {len(all_datasets)} datasets.")

    datasets_by_id: dict[str, dict] = {d["datasetId"]: d for d in all_datasets}

    # Graph: child dataset -> set of parent dataset IDs (direct upstream only)
    upstream: dict[str, set[str]] = defaultdict(set)

    # 3) Fetch sources for each dataset and record dataset->dataset edges
    for i, ds in enumerate(all_datasets, start=1):
        ds_id   = ds["datasetId"]
        ds_name = ds.get("name", ds_id)
        if i % 50 == 0:
            print(f"Processed {i}/{len(all_datasets)} datasets...")

        try:
            sources = list_dataset_sources(token_mgr, ds_id)
        except requests.HTTPError as e:
            print(f"Warning: failed to get sources for '{ds_name}' ({ds_id}): {e}")
            continue

        for src in sources.get("sources", []):
            if src.get("type") == "dataset":
                parent_id = src["id"]
                if parent_id in datasets_by_id:
                    upstream[ds_id].add(parent_id)

        time.sleep(API_CALL_DELAY_SECONDS)

    print("Finished building upstream dependency graph.")

    # 4) Ensure target table exists
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ_TABLE_SQL} (
            RUN_ID                   STRING,
            DATASET_ID               STRING,
            DATASET_NAME             STRING,
            DATASET_MIGRATION_STATUS STRING,
            PARENT_ID                STRING,
            PARENT_NAME              STRING,
            PARENT_MIGRATION_STATUS  STRING,
            RELATION_TYPE            STRING,
            CREATED_AT               TIMESTAMP_NTZ
        )
    """).collect()

    if TRUNCATE_BEFORE_INSERT:
        session.sql(f"TRUNCATE TABLE {FQ_TABLE_SQL}").collect()
        print(f"Truncated {FQ_TABLE_SQL} (snapshot mode).")

    # 5) Build rows
    run_id = str(uuid.uuid4())
    # Strip tzinfo for TIMESTAMP_NTZ compatibility
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    rows: list[tuple] = []

    # Datasets that have at least one upstream parent
    for ds_id, parent_ids in upstream.items():
        ds_meta    = datasets_by_id.get(ds_id, {})
        ds_name    = ds_meta.get("name", ds_id)
        ds_mig_status = ds_meta.get("migrationStatus")

        for parent_id in parent_ids:
            parent_meta = datasets_by_id.get(parent_id, {})
            rows.append((
                run_id,
                ds_id,
                ds_name,
                ds_mig_status,
                parent_id,
                parent_meta.get("name", parent_id),
                parent_meta.get("migrationStatus"),
                "UPSTREAM",
                now_ts,
            ))

    # Leaf datasets — no upstream dependencies; PARENT_* columns are NULL.
    # These are recorded so every dataset appears in at least one row.
    datasets_with_parents = set(upstream.keys())
    for ds_id, ds_meta in datasets_by_id.items():
        if ds_id not in datasets_with_parents:
            rows.append((
                run_id,
                ds_id,
                ds_meta.get("name", ds_id),
                ds_meta.get("migrationStatus"),
                None,   # PARENT_ID
                None,   # PARENT_NAME
                None,   # PARENT_MIGRATION_STATUS
                "LEAF",
                now_ts,
            ))

    print(f"Prepared {len(rows)} rows for insert (run_id={run_id}).")

    # 6) Write to Snowflake
    if rows:
        schema_cols = [
            "RUN_ID", "DATASET_ID", "DATASET_NAME", "DATASET_MIGRATION_STATUS",
            "PARENT_ID", "PARENT_NAME", "PARENT_MIGRATION_STATUS",
            "RELATION_TYPE", "CREATED_AT",
        ]
        df = session.create_dataframe(rows, schema=schema_cols)
        df.write.mode("append").save_as_table(FQ_TABLE_SNOWPARK)
        print(f"Inserted {len(rows)} rows into {FQ_TABLE_SQL}.")
    else:
        print("No datasets found; nothing inserted.")

    # Quick validation sample
    session.table(FQ_TABLE_SNOWPARK).order_by("DATASET_NAME", "PARENT_NAME").show(20)


session = get_active_session()
main(session)
