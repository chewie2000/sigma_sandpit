"""
core.py — warehouse-agnostic, importable core for the writeback inventory.

Pure Python plus the Sigma REST client; **no Spark, no dbutils, no import-time
side effects**, so it can be imported and unit-tested directly (unlike the
notebook entrypoint, which binds widgets/secrets at import). The populate
notebook imports these helpers; the Snowflake port (writeback_info_sf) can reuse
the same module since none of it depends on the warehouse.

What lives here:
- Sigma REST client: a retry-enabled requests Session, OAuth token, paginator.
- Pure transforms used by the populate flow: ID indexing, WAL-record dedup,
  enrichment selection, legacy-WAL detection, and the progress bar renderer.

Warehouse-specific work (WAL extraction, DESCRIBE DETAIL, the MERGE) stays in
the notebook — it is inherently Spark/Snowflake-specific and not shared here.
"""

import base64

# NB: requests / urllib3 are imported lazily inside build_session() so that the
# pure helpers below (bar, build_id_index, dedup_latest_by_edit_num, …) can be
# imported and unit-tested with no third-party dependencies installed.


# ===========================================================================
# Sigma REST client
# ===========================================================================

def build_session():
    """
    Shared HTTP session with automatic retry/backoff for all Sigma API calls.
    Retries transient failures — 429 and 5xx responses plus connection/read
    errors — with exponential backoff, honouring any Retry-After header. A
    single transient blip no longer aborts a scheduled run. raise_on_status is
    False so callers' resp.raise_for_status() stays the single error path once
    retries are exhausted; the Session also pools connections across the many
    paginated requests.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,                       # waits ~0, 2, 4, 8, 16s
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_sigma_token(session, api_base: str,
                    client_id: str, client_secret: str) -> str:
    """Obtain a Sigma OAuth bearer token using the client credentials flow."""
    auth_b64 = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = session.post(
        f"{api_base}/auth/token",
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


def sigma_paginate(session, api_base: str,
                   token: str, endpoint: str) -> list:
    """
    Fetch all pages from a Sigma list endpoint and return a flat list of items.
    Tries common root-key names (entries, workbooks, dataModels, data, items)
    to handle variation across Sigma API response shapes.
    """
    headers = {"Authorization": f"Bearer {token}"}
    items, params = [], {}
    while True:
        resp = session.get(
            f"{api_base}/{endpoint}",
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
# Pure transforms used by the populate flow
# ===========================================================================

def bar(done: int, total: int, width: int = 24) -> str:
    """Render an ASCII progress bar like ▕███████░░░░░░░░░░░▏ 3/5."""
    if not total:
        return "0/0"
    filled = int(width * done / total)
    return "▕" + "█" * filled + "░" * (width - filled) + f"▏ {done}/{total}"


def is_legacy_wal(wal_table_fqn) -> bool:
    """
    TRUE for the old random-UUID WAL naming (sigds_wal_<uuid>) rather than the
    current DS_ID-based naming (sigds_wal_ds_<ds_id>).
    """
    return 'sigds_wal_ds_' not in (wal_table_fqn or "").lower()


def dedup_latest_by_edit_num(records: list) -> list:
    """
    Deduplicate WAL records by SIGDS_TABLE (the MERGE key), keeping the one with
    the highest WAL_MAX_EDIT_NUM. Sigma can maintain two WAL tables for the same
    dataset when it migrates from the old random-UUID naming to the DS_ID-based
    naming; both surface here, so the most-edited wins. Accepts anything
    indexable by column name (Spark Row or dict).
    """
    seen = {}
    for r in records:
        t = r["SIGDS_TABLE"]
        if t and (t not in seen or (r["WAL_MAX_EDIT_NUM"] or 0) > (seen[t]["WAL_MAX_EDIT_NUM"] or 0)):
            seen[t] = r
    return list(seen.values())


def select_enrichment(workbook_id, wb_meta: dict, known_enrichment: dict) -> dict:
    """
    Pick the enrichment record for a workbook ID: prefer freshly-fetched
    enrichment (wb_meta), fall back to cached values for already-known IDs,
    and an empty dict when there is no workbook ID at all.
    """
    if not workbook_id:
        return {}
    if workbook_id in wb_meta:
        return wb_meta[workbook_id]
    return known_enrichment.get(workbook_id, {})
