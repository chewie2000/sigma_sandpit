-- ==============================================================================
-- sigma_org_extract.sql
--
-- Raw extraction layer for sigma_org_audit.
--
-- Hits the Sigma REST API across every org object type and lands the UNPARSED
-- JSON payloads, one row per object per snapshot, into a single append-only
-- VARIANT table (RAW_SIGMA_OBJECTS). No business logic, no flattening -- the
-- stage and mart layers are rebuildable from this table at any time, and a new
-- API field is absorbed without changing this procedure.
--
-- Prerequisites
--   Run setup_prerequisites.sql as ACCOUNTADMIN first. It creates the network
--   rule, Snowflake Secrets, external access integration, and grants.
--
-- Parameters
--   TARGET_DATABASE   -- database for the raw table (required)
--   TARGET_SCHEMA     -- schema for the raw table (required)
--   TARGET_TABLE      -- raw table name (optional, default RAW_SIGMA_OBJECTS)
--   INCLUDE_GRANTS    -- also fetch per-artifact permission grants (default TRUE)
--   MAX_WORKERS       -- thread pool size for per-object detail fan-out (default 10)
--   BASE_URL_OVERRIDE      -- audit a DIFFERENT org than the secrets point to:
--   CLIENT_ID_OVERRIDE        pass that org's API base URL + client credentials.
--   CLIENT_SECRET_OVERRIDE    All three default NULL -> fall back to the Secrets.
--
-- Multi-org
--   The org identity is NOT a parameter -- it is auto-detected by calling
--   GET /v2/whoami once and stamped onto every row as ORG_ID. So a single
--   deployment can audit any number of orgs into one RAW_SIGMA_OBJECTS table,
--   each row tagged with the org it came from. Two ways to point at another org:
--     1. Rotate the Secrets (CREATE OR REPLACE SECRET ...) -- credentials never
--        leave the Secret store; nothing appears in query history. Best for a
--        small, stable set of orgs.
--     2. Pass *_OVERRIDE params at call time -- one CALL audits any org with no
--        setup change. NOTE: the client secret then appears in query history.
--   For cross-region orgs, the network rule egress list must include that org's
--   API host (setup_prerequisites.sql lists all Sigma hosts by default).
--
--   SIGMA_BASE_URL / SIGMA_CLIENT_ID / SIGMA_CLIENT_SECRET are read at runtime
--   from Snowflake Secrets unless overridden by the *_OVERRIDE params above.
--
-- Example
--   CALL sigma_org_extract('MY_DB', 'MY_SCHEMA');                  -- secrets org
--   CALL sigma_org_extract('MY_DB', 'MY_SCHEMA', 'RAW_SIGMA_OBJECTS',
--        TRUE, 10, 'https://api.eu.aws.sigmacomputing.com',
--        '<client_id>', '<client_secret>');                        -- another org
--
-- Notes
--   - Append-only. Every run is a new SNAPSHOT_ID; nothing is overwritten.
--   - Connections are fetched twice: the list endpoint for the inventory, then a
--     per-connection detail fetch (GET /v2/connections/{id}) which is the only
--     place the writeback / input-table-WAL schema locations are exposed. Those
--     detail rows drive sigma_writeback_scan.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_org_extract(
    TARGET_DATABASE STRING,
    TARGET_SCHEMA   STRING,
    TARGET_TABLE    STRING  DEFAULT 'RAW_SIGMA_OBJECTS',
    INCLUDE_GRANTS  BOOLEAN DEFAULT TRUE,
    MAX_WORKERS     NUMBER  DEFAULT 10,
    BASE_URL_OVERRIDE     STRING DEFAULT NULL,
    CLIENT_ID_OVERRIDE    STRING DEFAULT NULL,
    CLIENT_SECRET_OVERRIDE STRING DEFAULT NULL
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
import json
import uuid
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from snowflake.snowpark.functions import parse_json, col
from snowflake.snowpark.types import (
    StructType, StructField, StringType, TimestampType,
)

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
# HTTP HELPERS
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


def list_paginated(token_mgr, path, extra_params=None):
    """
    Generic list-endpoint pager. Handles both pagination dialects seen across
    the Sigma API: hasMore/nextPage and nextPageToken. Returns the raw object
    dicts from the 'entries' array.
    """
    url = f"{SIGMA_BASE_URL}{path}"
    params = {"limit": 500}
    if extra_params:
        params.update(extra_params)
    out = []
    while True:
        headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
        body = _get_with_backoff(url, headers=headers, params=params).json()
        out.extend(body.get("entries", []))
        next_page = body.get("nextPage") or body.get("nextPageToken")
        if not next_page or not body.get("entries"):
            # hasMore is authoritative when present; otherwise stop on no token.
            if body.get("hasMore") and next_page:
                params["page"] = next_page
                continue
            break
        params["page"] = next_page
    return out


def get_json(token_mgr, path):
    """GET a single object/detail endpoint and return its parsed JSON body."""
    url = f"{SIGMA_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {token_mgr.get_token()}"}
    return _get_with_backoff(url, headers=headers).json()

# ------------------------------------------------------------------------------
# RAW COLLECTOR
# Accumulates (object_type, object_id, payload) tuples; thread-safe append.
# ------------------------------------------------------------------------------

class RawCollector:
    # Per-object-type key used to pull a stable id out of a payload.
    ID_KEYS = {
        "workbook":        ("workbookId", "id"),
        "datamodel":       ("dataModelId", "modelId", "id"),
        "dataset":         ("datasetId", "id"),
        "connection":      ("connectionId", "id"),
        "connection_detail": ("connectionId", "id"),
        "member":          ("memberId", "id"),
        "team":            ("teamId", "id"),
    }

    def __init__(self):
        self._rows = []
        self._lock = threading.Lock()

    @staticmethod
    def _extract_id(object_type, payload, fallback=None):
        for k in RawCollector.ID_KEYS.get(object_type, ("id",)):
            v = payload.get(k)
            if v:
                return str(v)
        return fallback

    def add(self, object_type, payload, object_id=None):
        oid = object_id or self._extract_id(object_type, payload)
        with self._lock:
            self._rows.append((object_type, oid, json.dumps(payload)))

    def add_many(self, object_type, payloads):
        for p in payloads:
            self.add(object_type, p)

    @property
    def rows(self):
        return self._rows

# ------------------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------------------

def main(session, target_database, target_schema, target_table,
         include_grants, max_workers,
         base_url_override, client_id_override, client_secret_override):

    # Credential resolution: optional per-call overrides win over the Secrets,
    # letting one deployment audit any org without rotating Secrets. The token
    # manager + HTTP helpers read these as module globals at call time, so
    # reassigning them here (before the manager is built) is sufficient.
    global SIGMA_BASE_URL, SIGMA_CLIENT_ID, SIGMA_CLIENT_SECRET
    if base_url_override:
        SIGMA_BASE_URL = base_url_override.rstrip("/")
    if client_id_override:
        SIGMA_CLIENT_ID = client_id_override
    if client_secret_override:
        SIGMA_CLIENT_SECRET = client_secret_override

    snapshot_id  = str(uuid.uuid4())
    snapshot_ts  = datetime.now(timezone.utc).replace(tzinfo=None)
    token_mgr    = SigmaTokenManager()
    collector    = RawCollector()
    max_workers  = int(max_workers or 10)

    fq_table = f'"{target_database}"."{target_schema}"."{target_table}"'

    # --- 0) Identify the org -- auto-detected, never a parameter ----------------
    # GET /v2/whoami returns {userId, organizationId}. Every landed row is stamped
    # with this ORG_ID so one raw table can hold many orgs side by side.
    org_id = get_json(token_mgr, "/v2/whoami").get("organizationId")

    # --- 1) Org-wide list endpoints --------------------------------------------
    # skipPermissionCheck=true needs admin credentials and gives org-wide visibility.
    workbooks   = list_paginated(token_mgr, "/v2/workbooks",  {"skipPermissionCheck": "true"})
    datamodels  = list_paginated(token_mgr, "/v2/dataModels")
    datasets    = list_paginated(token_mgr, "/v2/datasets",   {"skipPermissionCheck": "true"})
    connections = list_paginated(token_mgr, "/v2/connections")
    members     = list_paginated(token_mgr, "/v2/members")
    teams       = list_paginated(token_mgr, "/v2/teams")

    collector.add_many("workbook",   workbooks)
    collector.add_many("datamodel",  datamodels)
    collector.add_many("dataset",    datasets)
    collector.add_many("connection", connections)
    collector.add_many("member",     members)
    collector.add_many("team",       teams)

    counts = {
        "workbook":   len(workbooks),
        "datamodel":  len(datamodels),
        "dataset":    len(datasets),
        "connection": len(connections),
        "member":     len(members),
        "team":       len(teams),
    }
    errors = {}

    # --- 2) Per-connection detail (writeback + WAL schema locations) -----------
    # The list endpoint omits writebackSchemas / inputTableAuditLogSchema; only
    # GET /v2/connections/{id} returns them. These rows drive sigma_writeback_scan.
    conn_ids = [RawCollector._extract_id("connection", c) for c in connections]
    conn_ids = [c for c in conn_ids if c]

    def fetch_connection_detail(cid):
        try:
            return cid, get_json(token_mgr, f"/v2/connections/{cid}"), None
        except requests.HTTPError as e:
            return cid, None, str(e)

    detail_ok = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for cid, detail, err in ex.map(fetch_connection_detail, conn_ids):
            if detail is not None:
                collector.add("connection_detail", detail, object_id=cid)
                detail_ok += 1
    counts["connection_detail"] = detail_ok

    # --- 3) Per-workbook sources -----------------------------------------------
    def fetch_workbook_sources(wb):
        wb_id = RawCollector._extract_id("workbook", wb)
        try:
            body = get_json(token_mgr, f"/v2/workbooks/{wb_id}/sources")
            srcs = body if isinstance(body, list) else body.get("entries", [])
            return wb_id, {"workbookId": wb_id, "sources": srcs}, None
        except requests.HTTPError as e:
            return wb_id, None, str(e)

    src_ok = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(fetch_workbook_sources, wb) for wb in workbooks]
        for f in as_completed(futures):
            wb_id, payload, err = f.result()
            if payload is not None:
                collector.add("workbook_sources", payload, object_id=wb_id)
                src_ok += 1
    counts["workbook_sources"] = src_ok

    # --- 4) Per-data-model detail (full spec/metadata) -------------------------
    def fetch_datamodel_detail(dm):
        dm_id = RawCollector._extract_id("datamodel", dm)
        try:
            return dm_id, get_json(token_mgr, f"/v2/dataModels/{dm_id}"), None
        except requests.HTTPError as e:
            return dm_id, None, str(e)

    dm_ok = 0
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(fetch_datamodel_detail, dm) for dm in datamodels]
        for f in as_completed(futures):
            dm_id, payload, err = f.result()
            if payload is not None:
                collector.add("datamodel_detail", payload, object_id=dm_id)
                dm_ok += 1
    counts["datamodel_detail"] = dm_ok

    # --- 5) Per-artifact grants (optional) -------------------------------------
    if include_grants:
        inodes = []
        for wb in workbooks:
            iid = wb.get("inodeId") or wb.get("workbookId")
            if iid:
                inodes.append(("workbook", iid))
        for ds in datasets:
            iid = ds.get("inodeId") or ds.get("datasetId")
            if iid:
                inodes.append(("dataset", iid))
        for dm in datamodels:
            iid = dm.get("inodeId") or dm.get("dataModelId")
            if iid:
                inodes.append(("datamodel", iid))

        def fetch_grants(item):
            artifact_type, inode_id = item
            try:
                body = get_json(token_mgr, f"/v2/grants?inodeId={inode_id}")
                grants = body if isinstance(body, list) else body.get("entries", [])
                return inode_id, {"inodeId": inode_id,
                                  "artifactType": artifact_type,
                                  "grants": grants}, None
            except requests.HTTPError as e:
                return inode_id, None, str(e)

        grant_ok = 0
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(fetch_grants, it) for it in inodes]
            for f in as_completed(futures):
                inode_id, payload, err = f.result()
                if payload is not None:
                    collector.add("grant", payload, object_id=inode_id)
                    grant_ok += 1
        counts["grant"] = grant_ok

    # --- 6) Land everything as raw VARIANT snapshots ---------------------------
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_table} (
            SNAPSHOT_ID   STRING,
            SNAPSHOT_TS   TIMESTAMP_NTZ,
            ORG_ID        STRING,
            OBJECT_TYPE   STRING,
            OBJECT_ID     STRING,
            PAYLOAD       VARIANT,
            EXTRACTED_AT  TIMESTAMP_NTZ
        )
    """).collect()

    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
    stage_schema = StructType([
        StructField("OBJECT_TYPE",  StringType()),
        StructField("OBJECT_ID",    StringType()),
        StructField("PAYLOAD_STR",  StringType()),
    ])
    df = session.create_dataframe(collector.rows, schema=stage_schema)
    df = df.select(
        col("OBJECT_TYPE"),
        col("OBJECT_ID"),
        parse_json(col("PAYLOAD_STR")).alias("PAYLOAD"),
    )
    # Stamp constant snapshot metadata as literal columns across all rows.
    from snowflake.snowpark.functions import lit, to_timestamp_ntz
    final_df = df.select(
        lit(snapshot_id).alias("SNAPSHOT_ID"),
        to_timestamp_ntz(lit(snapshot_ts.isoformat())).alias("SNAPSHOT_TS"),
        lit(org_id).alias("ORG_ID"),
        col("OBJECT_TYPE"),
        col("OBJECT_ID"),
        col("PAYLOAD"),
        to_timestamp_ntz(lit(extracted_at.isoformat())).alias("EXTRACTED_AT"),
    )
    final_df.write.mode("append").save_as_table(
        [target_database, target_schema, target_table]
    )

    return json.dumps({
        "snapshot_id": snapshot_id,
        "snapshot_ts": snapshot_ts.isoformat(),
        "org_id": org_id,
        "rows_landed": len(collector.rows),
        "counts": counts,
        "errors": errors,
    })
$$;
