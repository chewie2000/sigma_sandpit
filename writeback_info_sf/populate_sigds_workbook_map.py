#!/usr/bin/env python3
"""
populate_sigds_workbook_map.py  (Snowflake)

Read-only information tool for Sigma writeback (input table) estates. Inventories
every SIGDS_WAL_* / SIGDS_* table in one or more writeback schemas, maps each to its
Sigma workbook / data model, and records who actually READS and WRITES it, so the
scoring views can tell a dead table from a quiet-but-live one.

It never drops, renames or modifies writeback tables. The only things it writes are
its own bookkeeping tables (SIGDS_WORKBOOK_MAP and friends).

What a run does
---------------
 1. Connect to Snowflake; load the Sigma API token.
 2. For each scan schema: discover WAL tables, read the latest WAL entry per SIGDS
    table, attach physical metadata (INFORMATION_SCHEMA + optional ACCOUNT_USAGE
    storage incl. Time Travel / Fail-safe), MERGE into SIGDS_WORKBOOK_MAP, flag
    WAL tables that vanished (IS_DELETED) and data tables that vanished (IS_ORPHANED).
    EVERY row is refreshed on EVERY run (the old script only refreshed rows whose
    WAL edit number had grown, which froze size/owner/archive state).
 3. Fetch the Sigma inventory once (workbooks org-wide, data models, version tags,
    members) and refresh enrichment for every known WORKBOOK_ID: name, path, owner,
    archived, tagged version, and API_NOT_FOUND (a workbook that has since been
    deleted from Sigma).
 4. Load the access log: join ACCOUNT_USAGE.QUERY_HISTORY x ACCESS_HISTORY, keep
    queries carrying Sigma's "-- Sigma Σ {json}" comment that touched a scanned
    writeback schema, MERGE into SIGDS_ACCESS_LOG. Incremental via
    SIGDS_ACCESS_LOAD_RUNS; degrades to a recorded SKIPPED run if ACCESS_HISTORY is
    unreadable (needs Enterprise Edition + IMPORTED PRIVILEGES on SNOWFLAKE).
 5. Snapshot every table's score + tier to SIGDS_SCORE_HISTORY.

Setup
-----
    python populate_sigds_workbook_map.py --init     # create tables + scoring views
    python populate_sigds_workbook_map.py            # scan

Configuration (environment variables -- nothing secret lives in this file)
-------------------------------------------------------------------------
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE   required
  one of: SNOWFLAKE_PASSWORD | SNOWFLAKE_AUTHENTICATOR (e.g. externalbrowser)
          | SNOWFLAKE_PRIVATE_KEY_PATH [+ SNOWFLAKE_PRIVATE_KEY_PASSPHRASE]
  SIGDS_DATABASE            required  database containing the writeback schema(s)
  SIGDS_SCAN_SCHEMAS        required  comma-separated writeback schemas to scan
  SIGDS_MAP_SCHEMA          optional  where the map/log tables live (default: first scan schema)
  SIGMA_BASE_URL            required  e.g. https://aws-api.sigmacomputing.com  (/v2 optional)
  SIGMA_CLIENT_ID / SIGMA_CLIENT_SECRET   required (admin scope recommended)
  SIGDS_BACKFILL_DAYS       optional  first access-log load look-back (default 90)
  SIGDS_OVERLAP_HOURS       optional  re-scan buffer for ACCESS_HISTORY latency (default 12)
  SIGDS_MAX_WAL_TABLES      optional  cap WAL tables for testing (disables deletion detection)

Database / schema names are treated as unquoted identifiers (upper-cased).

Design notes
------------
- Snowflake's INFORMATION_SCHEMA.TABLES.LAST_ALTERED tracks DDL, not DML, so there is
  no cheap "did this WAL table change" check; every WAL table is read every run.
- WAL METADATA / EDIT columns are VARCHAR JSON; TRY_PARSE_JSON tolerates bad rows.
- "TIMESTAMP" is reserved in Snowflake and is double-quoted wherever it is used.
"""

import argparse
import base64
import os
import re
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import snowflake.connector
from snowflake.connector import DictCursor

HERE = Path(__file__).resolve().parent

# Sigma appends "-- Sigma Σ {json}" (U+03A3) to every query it generates.
SIGMA_PREFIX = "-- Sigma Σ "

# Staging / MERGE column layout for the per-schema WAL + physical load.
# (name, snowflake type) -- single definition drives the temp table, MERGE and rows.
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
    ("WAL_MAX_EDIT_NUM",          "NUMBER(38,0)"),
    ("SIGDS_TABLE_ID",            "VARCHAR"),
    ("SIGDS_TABLE_CREATED_AT",    "TIMESTAMP_NTZ"),
    ("SIGDS_TABLE_LAST_MODIFIED", "TIMESTAMP_NTZ"),
    ("SIGDS_TABLE_SIZE_BYTES",    "NUMBER(38,0)"),
    ("SIGDS_TABLE_TOTAL_BYTES",   "NUMBER(38,0)"),
    ("WAL_TABLE_LAST_MODIFIED",   "TIMESTAMP_NTZ"),
    ("WAL_TABLE_SIZE_BYTES",      "NUMBER(38,0)"),
    ("IS_ORPHANED",               "BOOLEAN"),
    ("IS_DELETED",                "BOOLEAN"),
    ("DELETED_AT",                "TIMESTAMP_NTZ"),
    ("IS_LEGACY_WAL",             "BOOLEAN"),
    ("LAST_SCANNED_AT",           "TIMESTAMP_NTZ"),
]
KEY_COLUMNS = ("SIGDS_TABLE", "SCAN_SCHEMA")


# ===========================================================================
# Small helpers
# ===========================================================================

def q(val) -> str:
    """Single-quoted SQL string literal."""
    return "'" + str(val).replace("'", "''") + "'"


def ident(name: str) -> str:
    """Double-quoted SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


def utcnow() -> datetime:
    """Naive UTC now (matches TIMESTAMP_NTZ columns)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def bare_name(fqn: str) -> str:
    """Last part of a '"DB"."SCHEMA"."NAME"' style name, unquoted."""
    return fqn.split(".")[-1].replace('"', "")


def log(msg: str) -> None:
    print(msg, flush=True)


# ===========================================================================
# Configuration
# ===========================================================================

def _env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and not val:
        raise SystemExit(f"Missing required environment variable: {name}")
    return val


def load_config():
    scan_schemas = [s.strip().upper() for s in _env("SIGDS_SCAN_SCHEMAS", required=True).split(",") if s.strip()]
    if not scan_schemas:
        raise SystemExit("SIGDS_SCAN_SCHEMAS must list at least one schema.")
    base = _env("SIGMA_BASE_URL", required=True).rstrip("/")
    if not base.endswith("/v2"):
        base += "/v2"
    cfg = {
        "account":   _env("SNOWFLAKE_ACCOUNT", required=True),
        "user":      _env("SNOWFLAKE_USER", required=True),
        "role":      _env("SNOWFLAKE_ROLE", required=True),
        "warehouse": _env("SNOWFLAKE_WAREHOUSE", required=True),
        "password":      _env("SNOWFLAKE_PASSWORD"),
        "authenticator": _env("SNOWFLAKE_AUTHENTICATOR"),
        "key_path":      _env("SNOWFLAKE_PRIVATE_KEY_PATH"),
        "key_pass":      _env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        "database":    _env("SIGDS_DATABASE", required=True).upper(),
        "scan_schemas": scan_schemas,
        "map_schema":  _env("SIGDS_MAP_SCHEMA", scan_schemas[0]).upper(),
        "sigma_base":  base,
        "sigma_id":     _env("SIGMA_CLIENT_ID", required=True),
        "sigma_secret": _env("SIGMA_CLIENT_SECRET", required=True),
        "backfill_days":  int(_env("SIGDS_BACKFILL_DAYS", "90")),
        "overlap_hours":  int(_env("SIGDS_OVERLAP_HOURS", "12")),
        "max_wal_tables": int(_env("SIGDS_MAX_WAL_TABLES", "0")),
    }
    if not (cfg["password"] or cfg["authenticator"] or cfg["key_path"]):
        raise SystemExit("Set one of SNOWFLAKE_PASSWORD, SNOWFLAKE_AUTHENTICATOR or SNOWFLAKE_PRIVATE_KEY_PATH.")
    return cfg


def connect(cfg):
    kwargs = dict(
        account=cfg["account"], user=cfg["user"], role=cfg["role"],
        warehouse=cfg["warehouse"], database=cfg["database"], schema=cfg["map_schema"],
    )
    if cfg["key_path"]:
        from cryptography.hazmat.primitives import serialization
        with open(cfg["key_path"], "rb") as fh:
            key = serialization.load_pem_private_key(
                fh.read(), password=cfg["key_pass"].encode() if cfg["key_pass"] else None)
        kwargs["private_key"] = key.private_bytes(
            serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption())
    elif cfg["authenticator"]:
        kwargs["authenticator"] = cfg["authenticator"]
    else:
        kwargs["password"] = cfg["password"]
    return snowflake.connector.connect(**kwargs)


# ===========================================================================
# --init : create tables and scoring views from the SQL files
# ===========================================================================

def split_sql(text: str):
    """Split a SQL script on ';' outside single-quoted strings, dropping '--' comments."""
    stmts, buf, in_str, i = [], [], False, 0
    while i < len(text):
        ch = text[i]
        if in_str:
            buf.append(ch)
            if ch == "'":
                if text[i + 1: i + 2] == "'":      # escaped quote
                    buf.append("'")
                    i += 1
                else:
                    in_str = False
        elif ch == "'":
            in_str = True
            buf.append(ch)
        elif ch == "-" and text[i + 1: i + 2] == "-":
            while i < len(text) and text[i] != "\n":
                i += 1
            continue
        elif ch == ";":
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def run_init(cfg):
    conn = connect(cfg)
    cur = conn.cursor()
    for fname in ("create_sigds_workbook_map.sql", "scoring_views.sql"):
        sql = (HERE / fname).read_text(encoding="utf-8")
        sql = sql.replace("<YOUR_DATABASE>", cfg["database"]).replace("<YOUR_SCHEMA>", cfg["map_schema"])
        stmts = split_sql(sql)
        log(f"--init: {fname}: {len(stmts)} statement(s)")
        for stmt in stmts:
            cur.execute(stmt)
    cur.close()
    conn.close()
    log("--init complete.")


# ===========================================================================
# Sigma API
# ===========================================================================

def get_sigma_token(cfg) -> str:
    auth_b64 = base64.b64encode(f"{cfg['sigma_id']}:{cfg['sigma_secret']}".encode()).decode()
    resp = requests.post(
        f"{cfg['sigma_base']}/auth/token",
        headers={"Authorization": f"Basic {auth_b64}",
                 "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"}, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError("Sigma token response did not contain access_token.")
    return token


def sigma_paginate(cfg, token, endpoint, extra_params=None) -> list:
    """Fetch all pages of a Sigma list endpoint and return a flat list."""
    headers = {"Authorization": f"Bearer {token}"}
    items, params = [], dict(extra_params or {})
    while True:
        resp = requests.get(f"{cfg['sigma_base']}/{endpoint}", headers=headers, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        for key in ("entries", "workbooks", "dataModels", "data", "items"):
            if isinstance(data.get(key), list):
                items.extend(data[key])
                break
        else:
            log(f"  WARN: {endpoint!r} — no recognised list key in response: {list(data.keys())}")
        if not data.get("nextPage"):
            return items
        params["page"] = data["nextPage"]


def build_id_index(entries: list, target_ids: set) -> dict:
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


def fetch_sigma_inventory(cfg, token, target_ids):
    """One fetch per run: workbooks (org-wide), data models, version tags, members."""
    try:
        workbooks = sigma_paginate(cfg, token, "workbooks", {"skipPermissionCheck": "true", "limit": 500})
        org_wide = True
    except requests.HTTPError as e:
        # Non-admin tokens can't use skipPermissionCheck. Fall back, but then a
        # workbook missing from the list may just be invisible, not deleted.
        log(f"  WARN: org-wide workbook list refused ({e}); falling back to visible workbooks only. "
            "API_NOT_FOUND will NOT be set this run.")
        workbooks = sigma_paginate(cfg, token, "workbooks", {"limit": 500})
        org_wide = False
    datamodels = sigma_paginate(cfg, token, "dataModels")
    log(f"Sigma API: {len(workbooks)} workbook(s) (org-wide={org_wide}), {len(datamodels)} data model(s).")

    wb_index = build_id_index(workbooks, target_ids)
    dm_index = build_id_index(datamodels, target_ids)

    tagged = {}
    for tag in sigma_paginate(cfg, token, "tags"):
        tag_id = tag.get("versionTagId")
        if not tag_id:
            continue
        for wb in sigma_paginate(cfg, token, f"tags/{tag_id}/workbooks"):
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
               for m in sigma_paginate(cfg, token, "members") if m.get("memberId")}
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
        "API_NOT_FOUND": False, "API_LAST_SEEN_AT": stored.get("API_LAST_SEEN_AT"),
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
# Snowflake: WAL extraction and physical metadata
# ===========================================================================

def extract_wal_records_batch(cur, wal_batch: list) -> list:
    """One UNION ALL query over a batch of WAL tables -> latest WAL entry per SIGDS table."""
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
    cur.execute(f"""
        SELECT * EXCLUDE rn
        FROM   ({" UNION ALL ".join(parts)})
        WHERE  rn = 1 AND SIGDS_TABLE IS NOT NULL
    """)
    return cur.fetchall()


def load_storage_metrics(cur, database, schema) -> dict:
    """{TABLE_NAME_UPPER: active+time-travel+fail-safe bytes}; {} if ACCOUNT_USAGE unreadable."""
    try:
        cur.execute("""
            SELECT TABLE_NAME, ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES AS TOTAL_BYTES
            FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
            WHERE TABLE_CATALOG = %s AND TABLE_SCHEMA = %s AND DELETED = FALSE
        """, (database, schema))
        return {r["TABLE_NAME"].upper(): r["TOTAL_BYTES"] for r in cur.fetchall()}
    except Exception as e:
        log(f"  WARN: TABLE_STORAGE_METRICS unreadable ({str(e).splitlines()[0]}); "
            "falling back to active bytes only.")
        return {}


def stage_and_merge(cur, map_fq, rows):
    """Bulk-load rows into a temp table and MERGE into the map (update ALL columns)."""
    cols = [c for c, _ in MAIN_COLUMNS]
    ddl = ", ".join(f"{c} {t}" for c, t in MAIN_COLUMNS)
    cur.execute(f"CREATE OR REPLACE TEMPORARY TABLE _SIGDS_UPDATES ({ddl})")
    cur.executemany(f"INSERT INTO _SIGDS_UPDATES VALUES ({', '.join(['%s'] * len(cols))})", rows)
    update_set = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in KEY_COLUMNS)
    cur.execute(f"""
        MERGE INTO {map_fq} AS t
        USING _SIGDS_UPDATES AS s
          ON t.SIGDS_TABLE = s.SIGDS_TABLE AND t.SCAN_SCHEMA = s.SCAN_SCHEMA
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({', '.join(cols)})
             VALUES ({', '.join('s.' + c for c in cols)})
    """)


def scan_schema(cur, cfg, map_fq, schema, now):
    """WAL + physical load for one writeback schema. Returns count of rows merged."""
    db = cfg["database"]
    log(f"\n=== Scanning {db}.{schema} ===")

    cur.execute(f"""
        SELECT TABLE_NAME, LAST_ALTERED, CREATED, BYTES, TABLE_ID
        FROM {ident(db)}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = %s
    """, (schema,))
    meta = {r["TABLE_NAME"].upper(): r for r in cur.fetchall()}
    totals = load_storage_metrics(cur, db, schema)

    wal_names = sorted(n for n in meta if n.startswith("SIGDS_WAL"))
    capped = cfg["max_wal_tables"] > 0
    if capped:
        wal_names = wal_names[: cfg["max_wal_tables"]]
    wal_fqns = [f"{ident(db)}.{ident(schema)}.{ident(meta[n]['TABLE_NAME'])}" for n in wal_names]
    log(f"Discovered {len(wal_fqns)} WAL table(s).")

    # Stored state for this schema (deletion detection + physical refresh).
    cur.execute(f"SELECT SIGDS_TABLE, WAL_TABLE_FQN, IS_DELETED FROM {map_fq} WHERE SCAN_SCHEMA = %s", (schema,))
    stored = cur.fetchall()

    # --- WAL extraction (batched) -------------------------------------------
    records, batch_size = [], 100
    for i in range(0, len(wal_fqns), batch_size):
        batch = wal_fqns[i: i + batch_size]
        log(f"  WAL batch {i // batch_size + 1}/{-(-len(wal_fqns) // batch_size)} ({len(batch)} tables)")
        records.extend(extract_wal_records_batch(cur, batch))

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
                    created=m.get("CREATED"), altered=m.get("LAST_ALTERED"), bytes=m.get("BYTES"),
                    total=totals.get(sigds_name.upper()))

    # --- Merge WAL + physical -------------------------------------------------
    rows = []
    for r in records:
        p = physical(r["SIGDS_TABLE"])
        wal_meta = meta.get(bare_name(r["WAL_TABLE_FQN"]).upper(), {})
        rows.append((
            r["WAL_TABLE_FQN"], r["WAL_DS_ID"], r["SIGDS_TABLE"], schema, r["WORKBOOK_ID"],
            r["WAL_WORKBOOK_URL"], r["ORG_SLUG"], r["WAL_INPUT_TABLE_NAME"],
            r["WAL_LAST_EDIT_AT"], r["WAL_LAST_EDIT_BY"], r["WAL_MAX_EDIT_NUM"],
            p.get("id"), p.get("created"), p.get("altered"), p.get("bytes"), p.get("total"),
            wal_meta.get("LAST_ALTERED"), wal_meta.get("BYTES"),
            not p["exists"], False, None,
            "sigds_wal_ds_" not in (r["WAL_TABLE_FQN"] or "").lower(), now,
        ))
    if rows:
        stage_and_merge(cur, map_fq, rows)
        log(f"MERGE: {len(rows)} row(s) refreshed.")
    n_orphaned = sum(1 for row in rows if row[18])
    if n_orphaned:
        log(f"  {n_orphaned} orphaned (SIGDS data table no longer exists).")

    # --- WAL tables that vanished ---------------------------------------------
    if capped:
        log("Deletion detection skipped (SIGDS_MAX_WAL_TABLES set).")
    else:
        current = set(wal_fqns)
        gone = {s["WAL_TABLE_FQN"] for s in stored
                if s["WAL_TABLE_FQN"] and s["WAL_TABLE_FQN"] not in current and not s["IS_DELETED"]}
        if gone and not wal_fqns:
            # Zero WAL tables visible but we previously knew of some: far more likely a
            # grants/role problem than a mass deletion. Don't poison the data.
            log(f"  WARN: no WAL tables visible but {len(gone)} previously known — refusing to flag "
                "deletions. Check the role's USAGE/SELECT on the schema.")
        elif gone:
            cur.execute(f"""
                UPDATE {map_fq}
                SET IS_DELETED = TRUE, DELETED_AT = %s
                WHERE SCAN_SCHEMA = %s AND (IS_DELETED IS NULL OR IS_DELETED = FALSE)
                  AND WAL_TABLE_FQN IN ({', '.join(q(g) for g in sorted(gone))})
            """, (now, schema))
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
        cur.execute("""CREATE OR REPLACE TEMPORARY TABLE _SIGDS_PHYSICAL (
            SIGDS_TABLE VARCHAR, SCAN_SCHEMA VARCHAR, ID VARCHAR, CREATED TIMESTAMP_NTZ,
            ALTERED TIMESTAMP_NTZ, BYTES NUMBER(38,0), TOTAL NUMBER(38,0), WAL_BYTES NUMBER(38,0),
            IS_ORPHANED BOOLEAN, SCANNED TIMESTAMP_NTZ)""")
        cur.executemany("INSERT INTO _SIGDS_PHYSICAL VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", prow)
        cur.execute(f"""
            MERGE INTO {map_fq} t USING _SIGDS_PHYSICAL s
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

def refresh_enrichment(cur, cfg, map_fq, token, now):
    cur.execute(f"""
        SELECT WORKBOOK_ID, WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE, API_WORKBOOK_URL, API_OWNER_ID,
               API_IS_ARCHIVED, API_OWNER_FIRST_NAME, API_OWNER_LAST_NAME, IS_TAGGED_VERSION,
               VERSION_TAG_NAME, PARENT_WORKBOOK_ID, API_LAST_SEEN_AT
        FROM {map_fq} WHERE WORKBOOK_ID IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY WORKBOOK_ID ORDER BY API_LAST_SEEN_AT DESC NULLS LAST) = 1
    """)
    stored = {r["WORKBOOK_ID"]: r for r in cur.fetchall()}
    if not stored:
        log("\nEnrichment: no WORKBOOK_IDs in the map — Sigma API skipped.")
        return
    log(f"\n=== Sigma enrichment ({len(stored)} distinct WORKBOOK_ID(s)) ===")
    inv = fetch_sigma_inventory(cfg, token, set(stored))

    enrich = [resolve_workbook(wid, inv, st, now) for wid, st in stored.items()]
    not_found = sum(1 for e in enrich if e["API_NOT_FOUND"])
    archived = sum(1 for e in enrich if e["API_IS_ARCHIVED"])
    log(f"Resolved {len(enrich)} workbook(s): {archived} archived, {not_found} not found in Sigma.")

    cols = ["WORKBOOK_ID", "WORKBOOK_NAME", "WORKBOOK_PATH", "OBJECT_TYPE", "API_WORKBOOK_URL",
            "API_OWNER_ID", "API_IS_ARCHIVED", "API_OWNER_FIRST_NAME", "API_OWNER_LAST_NAME",
            "IS_TAGGED_VERSION", "VERSION_TAG_NAME", "PARENT_WORKBOOK_ID", "API_NOT_FOUND",
            "API_LAST_SEEN_AT"]
    types = ["VARCHAR"] * 6 + ["BOOLEAN", "VARCHAR", "VARCHAR", "BOOLEAN", "VARCHAR", "VARCHAR",
                               "BOOLEAN", "TIMESTAMP_NTZ"]
    cur.execute("CREATE OR REPLACE TEMPORARY TABLE _SIGDS_ENRICH (%s)" %
                ", ".join(f"{c} {t}" for c, t in zip(cols, types)))
    cur.executemany(f"INSERT INTO _SIGDS_ENRICH VALUES ({', '.join(['%s'] * len(cols))})",
                    [tuple(e.get(c) for c in cols) for e in enrich])
    cur.execute(f"""
        MERGE INTO {map_fq} t USING _SIGDS_ENRICH s ON t.WORKBOOK_ID = s.WORKBOOK_ID
        WHEN MATCHED THEN UPDATE SET {', '.join(f't.{c} = s.{c}' for c in cols[1:])}
    """)


# ===========================================================================
# Access log (query history x access history)
# ===========================================================================

def load_access_log(cur, cfg, fq):
    """Incrementally land Sigma-attributed reads/writes of writeback tables."""
    run_id, now = str(uuid.uuid4()), utcnow()
    runs, access = fq("SIGDS_ACCESS_LOAD_RUNS"), fq("SIGDS_ACCESS_LOG")
    log("\n=== Access log (QUERY_HISTORY x ACCESS_HISTORY) ===")

    def record(status, ws, we, rows, detail):
        cur.execute(f"INSERT INTO {runs} VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (run_id, now, ws, we, status, rows, detail))

    try:
        cur.execute("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1")
        cur.fetchall()
    except Exception as e:
        msg = ("cannot read SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY — needs Enterprise Edition+ and "
               f"GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE {cfg['role']}")
        log(f"  SKIPPED: {msg}\n  ({str(e).splitlines()[0]})\n"
            "  Scores will reflect WRITES ONLY and TIER 1 will be capped until this works.")
        record("SKIPPED", None, None, 0, msg)
        return

    cur.execute(f"SELECT MAX(WINDOW_END) AS WE FROM {runs} WHERE STATUS = 'OK'")
    last_end = cur.fetchone()["WE"]
    if last_end is None:
        window_start = now - timedelta(days=cfg["backfill_days"])
        log(f"  First run — backfilling {cfg['backfill_days']} day(s).")
    else:
        window_start = last_end - timedelta(hours=cfg["overlap_hours"])
        log(f"  Resuming from {last_end.isoformat()} (-{cfg['overlap_hours']}h overlap).")
    window_end = now

    schemas_in = ", ".join(q(s) for s in cfg["scan_schemas"])
    prefix = q(SIGMA_PREFIX)
    ws_lit, we_lit = f"'{window_start.isoformat()}+00:00'::TIMESTAMP_TZ", f"'{window_end.isoformat()}+00:00'::TIMESTAMP_TZ"
    try:
        cur.execute(f"""
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
             s.SIGMA_USER_EMAIL, '{now.isoformat()}'::TIMESTAMP_NTZ)
        """)
        inserted = int(list(cur.fetchone().values())[0])
    except Exception as e:
        # Don't record OK: a failed load must not extend coverage.
        log(f"  ERROR loading access log: {str(e).splitlines()[0]}")
        record("SKIPPED", window_start, window_end, 0, f"load failed: {str(e).splitlines()[0]}")
        return
    record("OK", window_start, window_end, inserted, None)
    log(f"  {inserted} new access row(s) landed for window {window_start:%Y-%m-%d %H:%M} → {window_end:%Y-%m-%d %H:%M} UTC.")


# ===========================================================================
# Score snapshot + summary
# ===========================================================================

def snapshot_scores(cur, cfg, fq, now):
    run_id = str(uuid.uuid4())
    schemas_in = ", ".join(q(s) for s in cfg["scan_schemas"])
    try:
        cur.execute(f"""
            INSERT INTO {fq('SIGDS_SCORE_HISTORY')}
            SELECT {q(run_id)}, %s, SCAN_SCHEMA, SIGDS_TABLE, WORKBOOK_NAME, TOTAL_SCORE, TIER_CODE,
                   ACCESS_EVIDENCE, TOTAL_BYTES, IS_ORPHANED, IS_DELETED, API_IS_ARCHIVED, API_NOT_FOUND
            FROM {fq('V_SIGDS_ARCHIVAL_SCORE')}
            WHERE UPPER(SCAN_SCHEMA) IN ({schemas_in})
        """, (now,))
        log(f"\nScore snapshot saved ({cur.rowcount} row(s)).")
        cur.execute(f"SELECT * FROM {fq('V_SIGDS_TIER_SUMMARY')} ORDER BY TIER_CODE")
        log("\nTier summary:")
        for r in cur.fetchall():
            log(f"  {r['ARCHIVAL_TIER']:<48} {r['RECORD_COUNT']:>5} table(s)  {r['TOTAL_SIZE_GB']} GB"
                + (f"  [{r['HELD_BY_COVERAGE_CAP']} held by coverage cap]" if r["HELD_BY_COVERAGE_CAP"] else "")
                + (f"  [{r['WRITE_ONLY_SCORED']} scored on writes only]" if r["WRITE_ONLY_SCORED"] else ""))
    except Exception as e:
        log(f"\nWARN: score snapshot skipped ({str(e).splitlines()[0]}). Run with --init to create the scoring views.")


# ===========================================================================
# Main
# ===========================================================================

def main():
    ap = argparse.ArgumentParser(description="Populate SIGDS_WORKBOOK_MAP and the writeback access log.")
    ap.add_argument("--init", action="store_true", help="create tables and scoring views, then exit")
    ap.add_argument("--skip-access-log", action="store_true", help="skip the query-history load")
    args = ap.parse_args()

    cfg = load_config()
    if args.init:
        run_init(cfg)
        return

    now = utcnow()
    conn = connect(cfg)
    cur = conn.cursor(DictCursor)

    def fq(name):
        return f"{ident(cfg['database'])}.{ident(cfg['map_schema'])}.{ident(name)}"
    map_fq = fq("SIGDS_WORKBOOK_MAP")

    try:
        cur.execute(f"SELECT COUNT(*) AS N FROM {map_fq}")
        cur.execute(f"SELECT COUNT(*) AS N FROM {fq('SIGDS_ACCESS_LOG')}")
    except Exception as e:
        raise SystemExit(f"Tables not found ({str(e).splitlines()[0]}). Run with --init first.")

    token = get_sigma_token(cfg)
    log("Sigma token obtained.")

    for schema in cfg["scan_schemas"]:
        scan_schema(cur, cfg, map_fq, schema, now)

    refresh_enrichment(cur, cfg, map_fq, token, now)

    if args.skip_access_log:
        log("\nAccess log skipped (--skip-access-log).")
    else:
        load_access_log(cur, cfg, fq)

    snapshot_scores(cur, cfg, fq, now)
    cur.close()
    conn.close()
    log("\nDone.")


if __name__ == "__main__":
    main()
