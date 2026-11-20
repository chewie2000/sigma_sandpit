-- =============================================================================
-- sigds_scan_procedure.sql  (Snowflake)
--
-- The whole writeback_info_sf scan, running INSIDE Snowflake as a Python stored
-- procedure -- nothing runs on your machine. Call it from a Snowsight worksheet
-- or notebook cell, or schedule it with a TASK (example at the bottom).
--
-- Read-only with respect to writeback: it never drops, renames or alters SIGDS /
-- WAL tables. It writes only its own bookkeeping tables (SIGDS_WORKBOOK_MAP,
-- SIGDS_ACCESS_LOG, SIGDS_ACCESS_LOAD_RUNS, SIGDS_SCORE_HISTORY).
--
-- Run order (once):  setup_prerequisites.sql (ACCOUNTADMIN)
--                    create_sigds_workbook_map.sql
--                    scoring_views.sql
--                    this file
-- Then:              CALL sigds_scan('MY_DB', 'WRITEBACK');
--
-- Find & replace <YOUR_DATABASE> / <YOUR_SCHEMA> first (the schema holding the map
-- tables; the secrets and integration from setup_prerequisites.sql live there).
--
-- Parameters
--   WB_DATABASE     database containing the writeback schema(s)           (required)
--   SCAN_SCHEMAS    comma-separated writeback schemas, e.g. 'PROD_WB,DEV_WB' (required)
--   MAP_SCHEMA      schema holding the map/log tables. Pass it explicitly; if omitted it
--                   falls back to CURRENT_SCHEMA() of the calling session
--   SKIP_ACCESS_LOG TRUE to skip the query-history load                  (default FALSE)
--   BACKFILL_DAYS   first access-log load look-back; QUERY_HISTORY keeps 365 (default 90)
--   OVERLAP_HOURS   re-scan buffer for ACCESS_HISTORY latency (~3 h)        (default 12)
--   MAX_WAL_TABLES  cap WAL tables for testing; disables deletion detection (default 0=all)
--
-- Returns the run log as text (also visible in the worksheet result).
-- Database/schema names are treated as unquoted (upper-cased) identifiers.
--
-- Sizing: the first run backfills query history, which can be heavy on a busy
-- account -- use a larger warehouse for it; later runs are incremental and small.
-- =============================================================================

USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;

CREATE OR REPLACE PROCEDURE sigds_scan(
    WB_DATABASE     STRING,
    SCAN_SCHEMAS    STRING,
    MAP_SCHEMA      STRING  DEFAULT NULL,
    SKIP_ACCESS_LOG BOOLEAN DEFAULT FALSE,
    BACKFILL_DAYS   NUMBER  DEFAULT 90,
    OVERLAP_HOURS   NUMBER  DEFAULT 12,
    MAX_WAL_TABLES  NUMBER  DEFAULT 0
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (sigds_sigma_access)
SECRETS = ('sigma_base_url'      = sigds_sigma_base_url,
           'sigma_client_id'     = sigds_sigma_client_id,
           'sigma_client_secret' = sigds_sigma_client_secret)
HANDLER = 'main'
AS
$$
import uuid
from datetime import datetime, timedelta, timezone

import _snowflake
import requests
from snowflake.snowpark.types import (
    BooleanType, LongType, StringType, StructField, StructType,
    TimestampTimeZone, TimestampType,
)

# Sigma appends "-- Sigma Σ {json}" (U+03A3) to every query it generates.
SIGMA_PREFIX = "-- Sigma Σ "

# (name, snowflake type) -- one definition drives the staging table, MERGE and rows.
MAIN_COLUMNS = [
    ("WAL_TABLE_FQN",             "VARCHAR"),
    ("WAL_DS_ID",                 "VARCHAR"),
    ("SIGDS_TABLE",               "VARCHAR"),
    ("SCAN_SCHEMA",               "VARCHAR"),
    ("WORKBOOK_ID",               "VARCHAR"),
    ("WAL_WORKBOOK_URL",          "VARCHAR"),
    ("ORG_SLUG",                  "VARCHAR"),
    ("WAL_INPUT_TABLE_NAME",      "VARCHAR"),
    ("WAL_LAST_EDIT_AT",          "TIMESTAMP_NTZ"),
    ("WAL_LAST_EDIT_BY",          "VARCHAR"),
    ("WAL_MAX_EDIT_NUM",          "NUMBER"),
    ("SIGDS_TABLE_ID",            "VARCHAR"),
    ("SIGDS_TABLE_CREATED_AT",    "TIMESTAMP_NTZ"),
    ("SIGDS_TABLE_LAST_MODIFIED", "TIMESTAMP_NTZ"),
    ("SIGDS_TABLE_SIZE_BYTES",    "NUMBER"),
    ("SIGDS_TABLE_TOTAL_BYTES",   "NUMBER"),
    ("WAL_TABLE_LAST_MODIFIED",   "TIMESTAMP_NTZ"),
    ("WAL_TABLE_SIZE_BYTES",      "NUMBER"),
    ("IS_ORPHANED",               "BOOLEAN"),
    ("IS_DELETED",                "BOOLEAN"),
    ("DELETED_AT",                "TIMESTAMP_NTZ"),
    ("IS_LEGACY_WAL",             "BOOLEAN"),
    ("LAST_SCANNED_AT",           "TIMESTAMP_NTZ"),
]
KEY_COLUMNS = ("SIGDS_TABLE", "SCAN_SCHEMA")

PHYSICAL_COLUMNS = [
    ("SIGDS_TABLE", "VARCHAR"), ("SCAN_SCHEMA", "VARCHAR"), ("ID", "VARCHAR"),
    ("CREATED", "TIMESTAMP_NTZ"), ("ALTERED", "TIMESTAMP_NTZ"), ("BYTES", "NUMBER"),
    ("TOTAL", "NUMBER"), ("WAL_BYTES", "NUMBER"), ("IS_ORPHANED", "BOOLEAN"),
    ("SCANNED", "TIMESTAMP_NTZ"),
]

ENRICH_COLUMNS = [
    ("WORKBOOK_ID", "VARCHAR"), ("WORKBOOK_NAME", "VARCHAR"), ("WORKBOOK_PATH", "VARCHAR"),
    ("OBJECT_TYPE", "VARCHAR"), ("API_WORKBOOK_URL", "VARCHAR"), ("API_OWNER_ID", "VARCHAR"),
    ("API_IS_ARCHIVED", "BOOLEAN"), ("API_OWNER_FIRST_NAME", "VARCHAR"),
    ("API_OWNER_LAST_NAME", "VARCHAR"), ("IS_TAGGED_VERSION", "BOOLEAN"),
    ("VERSION_TAG_NAME", "VARCHAR"), ("PARENT_WORKBOOK_ID", "VARCHAR"),
    ("API_NOT_FOUND", "BOOLEAN"), ("API_LAST_SEEN_AT", "TIMESTAMP_NTZ"),
]

# Module state set by main() (the helpers below are plain functions).
_S = None          # Snowpark session
_LOG = []          # run log, returned to the caller


# ===========================================================================
# Helpers
# ===========================================================================

def log(msg):
    _LOG.append(str(msg))
    print(msg)


def run(sql):
    """Execute SQL; return rows as dicts keyed by (upper-case) column name."""
    return [r.as_dict() for r in _S.sql(sql).collect()]


def q(val):
    """Single-quoted SQL string literal."""
    return "'" + str(val).replace("'", "''") + "'"


def qn(val):
    """Like q() but None -> NULL."""
    return "NULL" if val is None else q(val)


def ident(name):
    """Double-quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ntz(dt):
    """Naive-UTC datetime (or None) -- keeps TIMESTAMP_NTZ staging well-defined."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def ts(dt):
    """SQL TIMESTAMP_NTZ literal (or NULL)."""
    dt = ntz(dt)
    return "NULL" if dt is None else f"'{dt.isoformat(sep=' ')}'::TIMESTAMP_NTZ"


def bare_name(fqn):
    """Last part of a '"DB"."SCHEMA"."NAME"' style name, unquoted."""
    return fqn.split(".")[-1].replace('"', "")


def _snow_type(t):
    if t == "VARCHAR":
        return StringType()
    if t == "TIMESTAMP_NTZ":
        return TimestampType(TimestampTimeZone.NTZ)
    if t == "BOOLEAN":
        return BooleanType()
    return LongType()


def stage(table_fq, columns, rows):
    """Write python rows to a TEMPORARY table (session-scoped) via Snowpark."""
    schema = StructType([StructField(c, _snow_type(t), True) for c, t in columns])
    df = _S.create_dataframe(rows, schema=schema)
    df.write.save_as_table(table_fq, mode="overwrite", table_type="temporary")


def first_value(rows):
    """First column of the first row (e.g. 'number of rows inserted' from DML)."""
    return int(list(rows[0].values())[0]) if rows else 0


# ===========================================================================
# Sigma API
# ===========================================================================

def sigma_config():
    base = _snowflake.get_generic_secret_string("sigma_base_url").rstrip("/")
    if not base.endswith("/v2"):
        base += "/v2"
    return {
        "base": base,
        "id": _snowflake.get_generic_secret_string("sigma_client_id"),
        "secret": _snowflake.get_generic_secret_string("sigma_client_secret"),
    }


def get_sigma_token(sc):
    resp = requests.post(
        f"{sc['base']}/auth/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials",
              "client_id": sc["id"], "client_secret": sc["secret"]},
        timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Sigma token response did not contain access_token.")
    return token


def sigma_paginate(sc, token, endpoint, extra_params=None):
    headers = {"Authorization": f"Bearer {token}"}
    items, params = [], dict(extra_params or {})
    while True:
        resp = requests.get(f"{sc['base']}/{endpoint}", headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for key in ("entries", "workbooks", "dataModels", "data", "items"):
            if isinstance(data.get(key), list):
                items.extend(data[key])
                break
        else:
            log(f"  WARN: {endpoint!r} - no recognised list key in response: {list(data.keys())}")
        if not data.get("nextPage"):
            return items
        params["page"] = data["nextPage"]


def build_id_index(entries, target_ids):
    """Index API objects by whichever ID field best overlaps the WAL's workbook IDs."""
    if not entries or not target_ids:
        return {}
    target_norm = {v.strip().lower() for v in target_ids}
    candidates = [k for k in entries[0] if "id" in k.lower()] or ["id"]
    best_key = max(candidates, key=lambda k: len(
        {e[k].strip().lower() for e in entries if e.get(k)} & target_norm))
    return {e[best_key].strip().lower(): e for e in entries if e.get(best_key)}


def join_path(raw_path):
    return "/".join(raw_path) if isinstance(raw_path, list) else raw_path


def fetch_sigma_inventory(sc, token, target_ids):
    """One fetch per run: workbooks (org-wide), data models, version tags, members."""
    try:
        workbooks = sigma_paginate(sc, token, "workbooks", {"skipPermissionCheck": "true", "limit": 500})
        org_wide = True
    except requests.HTTPError as e:
        # Non-admin credentials can't use skipPermissionCheck. Fall back, but then a
        # workbook missing from the list may be invisible rather than deleted.
        log(f"  WARN: org-wide workbook list refused ({e}); using visible workbooks only. "
            "API_NOT_FOUND will NOT be set this run.")
        workbooks = sigma_paginate(sc, token, "workbooks", {"limit": 500})
        org_wide = False
    datamodels = sigma_paginate(sc, token, "dataModels")
    log(f"Sigma API: {len(workbooks)} workbook(s) (org-wide={org_wide}), {len(datamodels)} data model(s).")

    wb_index = build_id_index(workbooks, target_ids)
    dm_index = build_id_index(datamodels, target_ids)

    tagged = {}
    for tag in sigma_paginate(sc, token, "tags"):
        tag_id = tag.get("versionTagId")
        if not tag_id:
            continue
        for wb in sigma_paginate(sc, token, f"tags/{tag_id}/workbooks"):
            for t in wb.get("tags", []):
                twid = t.get("taggedWorkbookId")
                if twid:
                    tagged[twid.strip().lower()] = {
                        "parent_workbook_id": wb.get("workbookId"),
                        "tag_name": t.get("name"),
                        "name": wb.get("name"),
                        "path": join_path(wb.get("path")),
                        "url": wb.get("url"),
                        "ownerId": wb.get("ownerId"),
                    }
    log(f"Sigma API: {len(tagged)} tagged workbook version(s).")

    members = {m["memberId"].strip().lower(): m
               for m in sigma_paginate(sc, token, "members") if m.get("memberId")}
    log(f"Sigma API: {len(members)} member(s).")
    return {"wb": wb_index, "dm": dm_index, "tagged": tagged, "members": members, "org_wide": org_wide}


def resolve_workbook(wid, inv, stored, now):
    """Current enrichment for one WORKBOOK_ID. `stored` is the last-known row (fallback)."""
    norm = wid.strip().lower()
    wb, dm, tg = inv["wb"].get(norm), inv["dm"].get(norm), inv["tagged"].get(norm)
    out = {
        "WORKBOOK_ID": wid,
        "WORKBOOK_NAME": stored.get("WORKBOOK_NAME"), "WORKBOOK_PATH": stored.get("WORKBOOK_PATH"),
        "OBJECT_TYPE": stored.get("OBJECT_TYPE"), "API_WORKBOOK_URL": stored.get("API_WORKBOOK_URL"),
        "API_OWNER_ID": stored.get("API_OWNER_ID"), "API_IS_ARCHIVED": stored.get("API_IS_ARCHIVED"),
        "IS_TAGGED_VERSION": stored.get("IS_TAGGED_VERSION") or False,
        "VERSION_TAG_NAME": stored.get("VERSION_TAG_NAME"),
        "PARENT_WORKBOOK_ID": stored.get("PARENT_WORKBOOK_ID"),
        "API_NOT_FOUND": False, "API_LAST_SEEN_AT": ntz(stored.get("API_LAST_SEEN_AT")),
    }
    if wb:
        out.update(WORKBOOK_NAME=wb.get("name"), WORKBOOK_PATH=join_path(wb.get("path")),
                   OBJECT_TYPE="WORKBOOK", API_WORKBOOK_URL=wb.get("url"),
                   API_OWNER_ID=wb.get("ownerId"), API_IS_ARCHIVED=wb.get("isArchived", False),
                   IS_TAGGED_VERSION=False, VERSION_TAG_NAME=None, PARENT_WORKBOOK_ID=None,
                   API_LAST_SEEN_AT=now)
    elif dm:
        out.update(WORKBOOK_NAME=dm.get("name"), WORKBOOK_PATH=join_path(dm.get("path")),
                   OBJECT_TYPE="DATA_MODEL", API_WORKBOOK_URL=dm.get("url"),
                   API_OWNER_ID=dm.get("ownerId"), API_IS_ARCHIVED=False,
                   IS_TAGGED_VERSION=False, VERSION_TAG_NAME=None, PARENT_WORKBOOK_ID=None,
                   API_LAST_SEEN_AT=now)
    elif tg:
        parent = inv["wb"].get((tg["parent_workbook_id"] or "").strip().lower(), {})
        out.update(WORKBOOK_NAME=tg["name"], WORKBOOK_PATH=tg["path"], OBJECT_TYPE="WORKBOOK",
                   API_WORKBOOK_URL=tg["url"], API_OWNER_ID=tg["ownerId"],
                   API_IS_ARCHIVED=parent.get("isArchived", False) if parent else False,
                   IS_TAGGED_VERSION=True, VERSION_TAG_NAME=tg["tag_name"],
                   PARENT_WORKBOOK_ID=tg["parent_workbook_id"], API_LAST_SEEN_AT=now)
    else:
        # Not returned by the API. Only meaningful as "deleted" when the list was
        # org-wide, and never for data models (their list is visibility-scoped).
        out["API_NOT_FOUND"] = bool(inv["org_wide"] and out["OBJECT_TYPE"] != "DATA_MODEL")

    member = inv["members"].get((out["API_OWNER_ID"] or "").strip().lower(), {})
    out["API_OWNER_FIRST_NAME"] = member.get("firstName") or stored.get("API_OWNER_FIRST_NAME")
    out["API_OWNER_LAST_NAME"] = member.get("lastName") or stored.get("API_OWNER_LAST_NAME")
    return out


# ===========================================================================
# WAL extraction and physical metadata
# ===========================================================================

def extract_wal_records_batch(wal_batch):
    """One UNION ALL over a batch of WAL tables -> latest WAL entry per SIGDS table."""
    parts = []
    for wal_fqn in wal_batch:
        parts.append(f"""
        SELECT
            {q(wal_fqn)}                                                         AS WAL_TABLE_FQN,
            EDIT_NUM                                                             AS WAL_MAX_EDIT_NUM,
            DS_ID                                                                AS WAL_DS_ID,
            "TIMESTAMP"                                                          AS WAL_LAST_EDIT_AT,
            TRY_PARSE_JSON(METADATA):tableName::VARCHAR                          AS SIGDS_TABLE,
            TRY_PARSE_JSON(METADATA):workbookId::VARCHAR                         AS WORKBOOK_ID,
            COALESCE(TRY_PARSE_JSON(METADATA):sigmaUrl::VARCHAR,
                     TRY_PARSE_JSON(METADATA):workbookUrl::VARCHAR)              AS WAL_WORKBOOK_URL,
            COALESCE(TRY_PARSE_JSON(METADATA):elementTitle::VARCHAR,
                     TRY_PARSE_JSON(METADATA):inputTableTitle::VARCHAR)          AS WAL_INPUT_TABLE_NAME,
            COALESCE(TRY_PARSE_JSON(METADATA):userEmail::VARCHAR,
                     TRY_PARSE_JSON(EDIT):updateRow:blameInfo:updatedBy::VARCHAR,
                     TRY_PARSE_JSON(EDIT):addRow:blameInfo:updatedBy::VARCHAR)   AS WAL_LAST_EDIT_BY,
            SPLIT_PART(COALESCE(TRY_PARSE_JSON(METADATA):sigmaUrl::VARCHAR,
                                TRY_PARSE_JSON(METADATA):workbookUrl::VARCHAR), '/', 4) AS ORG_SLUG,
            ROW_NUMBER() OVER (
                PARTITION BY TRY_PARSE_JSON(METADATA):tableName::VARCHAR
                ORDER BY EDIT_NUM DESC)                                          AS rn
        FROM {wal_fqn}
        """)
    return run(f"""
        SELECT * EXCLUDE rn
        FROM   ({" UNION ALL ".join(parts)})
        WHERE  rn = 1 AND SIGDS_TABLE IS NOT NULL
    """)


def load_storage_metrics(database, schema):
    """{TABLE_NAME_UPPER: active+time-travel+fail-safe bytes}; {} if ACCOUNT_USAGE unreadable."""
    try:
        rows = run(f"""
            SELECT TABLE_NAME, ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES AS TOTAL_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE TABLE_CATALOG = {q(database)} AND TABLE_SCHEMA = {q(schema)} AND DELETED = FALSE
        """)
        return {r["TABLE_NAME"].upper(): r["TOTAL_BYTES"] for r in rows}
    except Exception as e:
        log(f"  WARN: TABLE_STORAGE_METRICS unreadable ({str(e).splitlines()[0]}); "
            "falling back to active bytes only.")
        return {}


def scan_schema(cfg, map_fq, fq, schema, now):
    """WAL + physical load for one writeback schema. Returns rows merged."""
    db = cfg["database"]
    log(f"\n=== Scanning {db}.{schema} ===")

    meta = {r["TABLE_NAME"].upper(): r for r in run(f"""
        SELECT TABLE_NAME, LAST_ALTERED, CREATED, BYTES, TABLE_ID
        FROM {ident(db)}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = {q(schema)}
    """)}
    totals = load_storage_metrics(db, schema)

    wal_names = sorted(n for n in meta if n.startswith("SIGDS_WAL"))
    capped = cfg["max_wal_tables"] > 0
    if capped:
        wal_names = wal_names[: cfg["max_wal_tables"]]
    wal_fqns = [f"{ident(db)}.{ident(schema)}.{ident(meta[n]['TABLE_NAME'])}" for n in wal_names]
    log(f"Discovered {len(wal_fqns)} WAL table(s).")

    stored = run(f"SELECT SIGDS_TABLE, WAL_TABLE_FQN, IS_DELETED FROM {map_fq} WHERE SCAN_SCHEMA = {q(schema)}")

    # --- WAL extraction (batched) -------------------------------------------
    records, batch_size = [], 100
    for i in range(0, len(wal_fqns), batch_size):
        batch = wal_fqns[i: i + batch_size]
        log(f"  WAL batch {i // batch_size + 1}/{-(-len(wal_fqns) // batch_size)} ({len(batch)} tables)")
        records.extend(extract_wal_records_batch(batch))

    # Two WALs can cover one SIGDS table during legacy->DS_ID migration: keep highest edit.
    best = {}
    for r in records:
        t = r["SIGDS_TABLE"]
        if t not in best or (r["WAL_MAX_EDIT_NUM"] or 0) > (best[t]["WAL_MAX_EDIT_NUM"] or 0):
            best[t] = r
    records = list(best.values())
    log(f"{len(records)} unique SIGDS table(s) from WAL.")

    def physical(sigds_name):
        m = meta.get(sigds_name.upper())
        if m is None:
            return dict(exists=False)
        return dict(exists=True, id=str(m["TABLE_ID"]) if m.get("TABLE_ID") else None,
                    created=ntz(m.get("CREATED")), altered=ntz(m.get("LAST_ALTERED")),
                    bytes=m.get("BYTES"), total=totals.get(sigds_name.upper()))

    # --- Merge WAL + physical -------------------------------------------------
    rows = []
    for r in records:
        p = physical(r["SIGDS_TABLE"])
        wal_meta = meta.get(bare_name(r["WAL_TABLE_FQN"]).upper(), {})
        rows.append((
            r["WAL_TABLE_FQN"], r["WAL_DS_ID"], r["SIGDS_TABLE"], schema, r["WORKBOOK_ID"],
            r["WAL_WORKBOOK_URL"], r["ORG_SLUG"], r["WAL_INPUT_TABLE_NAME"],
            ntz(r["WAL_LAST_EDIT_AT"]), r["WAL_LAST_EDIT_BY"], r["WAL_MAX_EDIT_NUM"],
            p.get("id"), p.get("created"), p.get("altered"), p.get("bytes"), p.get("total"),
            ntz(wal_meta.get("LAST_ALTERED")), wal_meta.get("BYTES"),
            not p["exists"], False, None,
            "sigds_wal_ds_" not in (r["WAL_TABLE_FQN"] or "").lower(), now,
        ))
    if rows:
        cols = [c for c, _ in MAIN_COLUMNS]
        upd = fq("_SIGDS_UPDATES")
        stage(upd, MAIN_COLUMNS, rows)
        update_set = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in KEY_COLUMNS)
        run(f"""
            MERGE INTO {map_fq} AS t USING {upd} AS s
              ON t.SIGDS_TABLE = s.SIGDS_TABLE AND t.SCAN_SCHEMA = s.SCAN_SCHEMA
            WHEN MATCHED THEN UPDATE SET {update_set}
            WHEN NOT MATCHED THEN INSERT ({', '.join(cols)})
                 VALUES ({', '.join('s.' + c for c in cols)})
        """)
        log(f"MERGE: {len(rows)} row(s) refreshed.")
    n_orphaned = sum(1 for row in rows if row[18])
    if n_orphaned:
        log(f"  {n_orphaned} orphaned (SIGDS data table no longer exists).")

    # --- WAL tables that vanished ---------------------------------------------
    if capped:
        log("Deletion detection skipped (MAX_WAL_TABLES set).")
    else:
        current = set(wal_fqns)
        gone = {s["WAL_TABLE_FQN"] for s in stored
                if s["WAL_TABLE_FQN"] and s["WAL_TABLE_FQN"] not in current and not s["IS_DELETED"]}
        if gone and not wal_fqns:
            # Zero WAL tables visible but some were known: far more likely a grants
            # problem than a mass deletion. Don't poison the data.
            log(f"  WARN: no WAL tables visible but {len(gone)} previously known - refusing to flag "
                "deletions. Check the role's USAGE/SELECT on the schema.")
        elif gone:
            run(f"""
                UPDATE {map_fq}
                SET IS_DELETED = TRUE, DELETED_AT = {ts(now)}
                WHERE SCAN_SCHEMA = {q(schema)} AND (IS_DELETED IS NULL OR IS_DELETED = FALSE)
                  AND WAL_TABLE_FQN IN ({', '.join(q(g) for g in sorted(gone))})
            """)
            log(f"Flagged {len(gone)} WAL table(s) as deleted.")

    # --- Physical refresh for rows the WAL no longer feeds --------------------
    fresh = {r["SIGDS_TABLE"] for r in records}
    stale = [s for s in stored if s["SIGDS_TABLE"] and s["SIGDS_TABLE"] not in fresh]
    if stale:
        prow = []
        for s in stale:
            p = physical(s["SIGDS_TABLE"])
            wal_bytes = (meta.get(bare_name(s["WAL_TABLE_FQN"]).upper(), {}).get("BYTES")
                         if s["WAL_TABLE_FQN"] else None)
            prow.append((s["SIGDS_TABLE"], schema, p.get("id"), p.get("created"), p.get("altered"),
                         p.get("bytes"), p.get("total"), wal_bytes, not p["exists"], now))
        phys = fq("_SIGDS_PHYSICAL")
        stage(phys, PHYSICAL_COLUMNS, prow)
        run(f"""
            MERGE INTO {map_fq} t USING {phys} s
              ON t.SIGDS_TABLE = s.SIGDS_TABLE AND t.SCAN_SCHEMA = s.SCAN_SCHEMA
            WHEN MATCHED THEN UPDATE SET
              t.SIGDS_TABLE_ID = s.ID, t.SIGDS_TABLE_CREATED_AT = s.CREATED,
              t.SIGDS_TABLE_LAST_MODIFIED = s.ALTERED, t.SIGDS_TABLE_SIZE_BYTES = s.BYTES,
              t.SIGDS_TABLE_TOTAL_BYTES = s.TOTAL, t.WAL_TABLE_SIZE_BYTES = s.WAL_BYTES,
              t.IS_ORPHANED = s.IS_ORPHANED, t.LAST_SCANNED_AT = s.SCANNED
        """)
        log(f"Physical metadata refreshed for {len(stale)} row(s) no longer fed by a WAL.")
    return len(rows)


# ===========================================================================
# Enrichment refresh (Sigma API) across the whole map
# ===========================================================================

def refresh_enrichment(map_fq, fq, sc, now):
    stored = {r["WORKBOOK_ID"]: r for r in run(f"""
        SELECT WORKBOOK_ID, WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE, API_WORKBOOK_URL, API_OWNER_ID,
               API_IS_ARCHIVED, API_OWNER_FIRST_NAME, API_OWNER_LAST_NAME, IS_TAGGED_VERSION,
               VERSION_TAG_NAME, PARENT_WORKBOOK_ID, API_LAST_SEEN_AT
        FROM {map_fq} WHERE WORKBOOK_ID IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY WORKBOOK_ID ORDER BY API_LAST_SEEN_AT DESC NULLS LAST) = 1
    """)}
    if not stored:
        log("\nEnrichment: no WORKBOOK_IDs in the map - Sigma API skipped.")
        return
    log(f"\n=== Sigma enrichment ({len(stored)} distinct WORKBOOK_ID(s)) ===")
    token = get_sigma_token(sc)
    inv = fetch_sigma_inventory(sc, token, set(stored))

    enrich = [resolve_workbook(wid, inv, st, now) for wid, st in stored.items()]
    log(f"Resolved {len(enrich)} workbook(s): {sum(1 for e in enrich if e['API_IS_ARCHIVED'])} archived, "
        f"{sum(1 for e in enrich if e['API_NOT_FOUND'])} not found in Sigma.")

    cols = [c for c, _ in ENRICH_COLUMNS]
    tmp = fq("_SIGDS_ENRICH")
    stage(tmp, ENRICH_COLUMNS, [tuple(e.get(c) for c in cols) for e in enrich])
    run(f"""
        MERGE INTO {map_fq} t USING {tmp} s ON t.WORKBOOK_ID = s.WORKBOOK_ID
        WHEN MATCHED THEN UPDATE SET {', '.join(f't.{c} = s.{c}' for c in cols[1:])}
    """)


# ===========================================================================
# Access log (query history x access history)
# ===========================================================================

def load_access_log(cfg, fq):
    """Incrementally land Sigma-attributed reads/writes of writeback tables."""
    run_id, now = str(uuid.uuid4()), utcnow()
    runs, access = fq("SIGDS_ACCESS_LOAD_RUNS"), fq("SIGDS_ACCESS_LOG")
    log("\n=== Access log (QUERY_HISTORY x ACCESS_HISTORY) ===")

    def record(status, ws, we, rows, detail):
        run(f"INSERT INTO {runs} VALUES ({q(run_id)}, {ts(now)}, {ts(ws)}, {ts(we)}, "
            f"{q(status)}, {int(rows)}, {qn(detail)})")

    try:
        run("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1")
    except Exception as e:
        msg = ("cannot read SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY - needs Enterprise Edition+ and "
               "GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE to the procedure owner's role")
        log(f"  SKIPPED: {msg}\n  ({str(e).splitlines()[0]})\n"
            "  Scores will reflect WRITES ONLY and TIER 1 will be capped until this works.")
        record("SKIPPED", None, None, 0, msg)
        return

    last_end = run(f"SELECT MAX(WINDOW_END) AS WE FROM {runs} WHERE STATUS = 'OK'")[0]["WE"]
    if last_end is None:
        window_start = now - timedelta(days=cfg["backfill_days"])
        log(f"  First run - backfilling {cfg['backfill_days']} day(s).")
    else:
        window_start = ntz(last_end) - timedelta(hours=cfg["overlap_hours"])
        log(f"  Resuming from {ntz(last_end).isoformat()} (-{cfg['overlap_hours']}h overlap).")
    window_end = now

    schemas_in = ", ".join(q(s) for s in cfg["scan_schemas"])
    prefix = q(SIGMA_PREFIX)
    ws_lit = f"'{window_start.isoformat()}+00:00'::TIMESTAMP_TZ"
    we_lit = f"'{window_end.isoformat()}+00:00'::TIMESTAMP_TZ"
    try:
        res = run(f"""
        MERGE INTO {access} t USING (
            WITH scoped AS (
                SELECT QUERY_ID, START_TIME, USER_NAME, ROLE_NAME, QUERY_TYPE, ROWS_INSERTED,
                       TRY_PARSE_JSON(SUBSTR(QUERY_TEXT, POSITION({prefix} IN QUERY_TEXT)
                                             + LENGTH({prefix}))) AS m
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE START_TIME >= {ws_lit} AND START_TIME < {we_lit}
                  AND QUERY_TYPE IN ('SELECT', 'INSERT')
                  AND QUERY_TEXT ILIKE '%SIGDS%'
                  AND POSITION({prefix} IN QUERY_TEXT) > 0
            ),
            events AS (
                SELECT s.*, 'READ' AS ACCESS_KIND, ao.value:objectName::STRING AS OBJ
                FROM scoped s
                JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah ON ah.QUERY_ID = s.QUERY_ID,
                     LATERAL FLATTEN(input => ah.BASE_OBJECTS_ACCESSED) ao
                WHERE s.QUERY_TYPE = 'SELECT' AND s.m IS NOT NULL
                UNION ALL
                SELECT s.*, 'INSERT', om.value:objectName::STRING
                FROM scoped s
                JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah ON ah.QUERY_ID = s.QUERY_ID,
                     LATERAL FLATTEN(input => ah.OBJECTS_MODIFIED) om
                WHERE s.QUERY_TYPE = 'INSERT' AND s.ROWS_INSERTED > 0 AND s.m IS NOT NULL
            )
            SELECT QUERY_ID, ACCESS_KIND,
                   UPPER(REPLACE(SPLIT_PART(OBJ, '.', 3), '"', ''))  AS OBJECT_NAME,
                   UPPER(SPLIT_PART(OBJ, '.', 1))                    AS OBJECT_DATABASE,
                   UPPER(SPLIT_PART(OBJ, '.', 2))                    AS OBJECT_SCHEMA,
                   CONVERT_TIMEZONE('UTC', START_TIME)::TIMESTAMP_NTZ AS START_TIME,
                   USER_NAME, ROLE_NAME, QUERY_TYPE, ROWS_INSERTED,
                   m:sourceUrl::STRING AS SIGMA_SOURCE_URL, m:kind::STRING AS SIGMA_KIND,
                   m:email::STRING AS SIGMA_USER_EMAIL
            FROM events
            WHERE OBJ IS NOT NULL
              AND UPPER(SPLIT_PART(OBJ, '.', 1)) = {q(cfg['database'])}
              AND UPPER(SPLIT_PART(OBJ, '.', 2)) IN ({schemas_in})
            QUALIFY ROW_NUMBER() OVER (PARTITION BY QUERY_ID, ACCESS_KIND, OBJ ORDER BY START_TIME) = 1
        ) s
        ON  t.QUERY_ID = s.QUERY_ID AND t.ACCESS_KIND = s.ACCESS_KIND
        AND t.OBJECT_NAME = s.OBJECT_NAME AND t.OBJECT_SCHEMA = s.OBJECT_SCHEMA
        WHEN NOT MATCHED THEN INSERT
            (QUERY_ID, ACCESS_KIND, OBJECT_NAME, OBJECT_DATABASE, OBJECT_SCHEMA, START_TIME, USER_NAME,
             ROLE_NAME, QUERY_TYPE, ROWS_INSERTED, SIGMA_SOURCE_URL, SIGMA_KIND, SIGMA_USER_EMAIL, LOADED_AT)
        VALUES
            (s.QUERY_ID, s.ACCESS_KIND, s.OBJECT_NAME, s.OBJECT_DATABASE, s.OBJECT_SCHEMA, s.START_TIME,
             s.USER_NAME, s.ROLE_NAME, s.QUERY_TYPE, s.ROWS_INSERTED, s.SIGMA_SOURCE_URL, s.SIGMA_KIND,
             s.SIGMA_USER_EMAIL, {ts(now)})
        """)
        inserted = first_value(res)
    except Exception as e:
        # Don't record OK: a failed load must not extend coverage.
        log(f"  ERROR loading access log: {str(e).splitlines()[0]}")
        record("SKIPPED", window_start, window_end, 0, f"load failed: {str(e).splitlines()[0]}")
        return
    record("OK", window_start, window_end, inserted, None)
    log(f"  {inserted} new access row(s) landed for window "
        f"{window_start:%Y-%m-%d %H:%M} -> {window_end:%Y-%m-%d %H:%M} UTC.")


# ===========================================================================
# Score snapshot + summary
# ===========================================================================

def snapshot_scores(cfg, fq, now):
    run_id = str(uuid.uuid4())
    schemas_in = ", ".join(q(s) for s in cfg["scan_schemas"])
    try:
        res = run(f"""
            INSERT INTO {fq('SIGDS_SCORE_HISTORY')}
            SELECT {q(run_id)}, {ts(now)}, SCAN_SCHEMA, SIGDS_TABLE, WORKBOOK_NAME, TOTAL_SCORE, TIER_CODE,
                   ACCESS_EVIDENCE, TOTAL_BYTES, IS_ORPHANED, IS_DELETED, API_IS_ARCHIVED, API_NOT_FOUND
            FROM {fq('V_SIGDS_ARCHIVAL_SCORE')}
            WHERE UPPER(SCAN_SCHEMA) IN ({schemas_in})
        """)
        log(f"\nScore snapshot saved ({first_value(res)} row(s)).")
        log("\nTier summary:")
        for r in run(f"SELECT * FROM {fq('V_SIGDS_TIER_SUMMARY')} ORDER BY TIER_CODE"):
            log(f"  {r['ARCHIVAL_TIER']:<48} {r['RECORD_COUNT']:>5} table(s)  {r['TOTAL_SIZE_GB']} GB"
                + (f"  [{r['HELD_BY_COVERAGE_CAP']} held by coverage cap]" if r["HELD_BY_COVERAGE_CAP"] else "")
                + (f"  [{r['WRITE_ONLY_SCORED']} scored on writes only]" if r["WRITE_ONLY_SCORED"] else ""))
    except Exception as e:
        log(f"\nWARN: score snapshot skipped ({str(e).splitlines()[0]}). "
            "Has scoring_views.sql been run?")


# ===========================================================================
# Entry point
# ===========================================================================

def main(session, database, scan_schemas, map_schema, skip_access_log,
         backfill_days, overlap_hours, max_wal_tables):
    global _S, _LOG
    _S, _LOG = session, []

    schemas = [s.strip().upper() for s in (scan_schemas or "").split(",") if s.strip()]
    if not database or not schemas:
        raise ValueError("DATABASE and SCAN_SCHEMAS (comma-separated) are required.")
    cfg = {
        "database": database.upper(),
        "scan_schemas": schemas,
        "map_schema": (map_schema or run("SELECT CURRENT_SCHEMA() AS S")[0]["S"]).upper(),
        "backfill_days": int(backfill_days or 90),
        "overlap_hours": int(overlap_hours or 12),
        "max_wal_tables": int(max_wal_tables or 0),
    }
    now = utcnow()

    def fq(name):
        return f"{ident(cfg['database'])}.{ident(cfg['map_schema'])}.{ident(name)}"
    map_fq = fq("SIGDS_WORKBOOK_MAP")

    log(f"sigds_scan: scanning {cfg['database']}.[{', '.join(schemas)}], map in "
        f"{cfg['database']}.{cfg['map_schema']}")
    try:
        run(f"SELECT COUNT(*) AS N FROM {map_fq}")
        run(f"SELECT COUNT(*) AS N FROM {fq('SIGDS_ACCESS_LOG')}")
    except Exception as e:
        raise RuntimeError(
            f"Bookkeeping tables not found in {cfg['database']}.{cfg['map_schema']} "
            f"({str(e).splitlines()[0]}). Run create_sigds_workbook_map.sql and scoring_views.sql first, "
            "or pass MAP_SCHEMA explicitly.")

    sc = sigma_config()

    for schema in schemas:
        scan_schema(cfg, map_fq, fq, schema, now)

    refresh_enrichment(map_fq, fq, sc, now)

    if skip_access_log:
        log("\nAccess log skipped (SKIP_ACCESS_LOG).")
    else:
        load_access_log(cfg, fq)

    snapshot_scores(cfg, fq, now)
    log("\nDone.")
    return "\n".join(_LOG)
$$;


-- =============================================================================
-- Run it
-- =============================================================================
--   CALL sigds_scan('MY_DB', 'WRITEBACK');
--   CALL sigds_scan('MY_DB', 'PROD_WB,DEV_WB', 'WRITEBACK');   -- several schemas, map in WRITEBACK
--   CALL sigds_scan('MY_DB', 'WRITEBACK', NULL, TRUE);         -- skip the query-history load
--
-- Schedule it (optional). Needs EXECUTE TASK (see setup_prerequisites.sql) and a
-- warehouse. Daily at 06:00 UTC:
--
-- CREATE OR REPLACE TASK sigds_scan_daily
--   WAREHOUSE = <YOUR_WAREHOUSE>
--   SCHEDULE  = 'USING CRON 0 6 * * * UTC'
-- AS
--   CALL sigds_scan('MY_DB', 'WRITEBACK');
-- ALTER TASK sigds_scan_daily RESUME;
