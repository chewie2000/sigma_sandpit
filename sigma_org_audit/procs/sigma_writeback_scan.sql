-- ==============================================================================
-- sigma_writeback_scan.sql
--
-- Writeback (input-table) audit layer for sigma_org_audit.
--
-- Scans the writeback schemas of every Sigma connection and lands raw rows for
-- the SIGDS_* input-table inventory and the input-table write-ahead-log (WAL)
-- activity. Scan targets are DISCOVERED from the connection_detail rows landed
-- by sigma_org_extract -- there is no manual schema parameter.
--
-- Prerequisites
--   1. Run setup_prerequisites.sql as ACCOUNTADMIN.
--   2. Run sigma_org_extract first, in the same RAW_SIGMA_OBJECTS table, so the
--      connection_detail rows (with writeback / WAL schema locations) exist.
--   3. Grant the execution role USAGE + SELECT on each writeback DB/schema you
--      want scanned in depth (see setup_prerequisites.sql section 5). Locations
--      the role cannot read are inventoried from the API only and flagged
--      SCAN_REACHABLE = FALSE.
--
-- Parameters
--   TARGET_DATABASE   -- database holding RAW_SIGMA_OBJECTS (required)
--   TARGET_SCHEMA     -- schema holding RAW_SIGMA_OBJECTS (required)
--   TARGET_TABLE      -- raw table name (optional, default RAW_SIGMA_OBJECTS)
--   CONNECTION_FILTER -- optional comma-separated connectionId allow-list
--   ORG_FILTER        -- optional ORG_ID; scope the scan to one org's connections
--                        (default NULL = every org present in the raw table)
--
-- Multi-org
--   Each writeback/WAL row inherits the ORG_ID of the connection that owns it,
--   read from the connection_detail rows. So writeback findings stay attributed
--   to the correct org in a multi-org raw table. Pass ORG_FILTER to limit a scan
--   to a single org.
--
-- Example
--   CALL sigma_writeback_scan('MY_DB', 'MY_SCHEMA');
--
-- This proc reads warehouse metadata directly (no Sigma API calls) and appends
-- to the SAME snapshot family as the extract -- it stamps its own SNAPSHOT_ID
-- but records the source extract's snapshot in the payload for joining.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_writeback_scan(
    TARGET_DATABASE   STRING,
    TARGET_SCHEMA     STRING,
    TARGET_TABLE      STRING DEFAULT 'RAW_SIGMA_OBJECTS',
    CONNECTION_FILTER STRING DEFAULT NULL,
    ORG_FILTER        STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import json
import uuid
from datetime import datetime, timezone

from snowflake.snowpark.functions import parse_json, col, lit, to_timestamp_ntz
from snowflake.snowpark.types import StructType, StructField, StringType


def _q_ident(part):
    """Quote a single SQL identifier, escaping embedded double-quotes."""
    return '"' + part.replace('"', '""') + '"'


def _q_literal(val):
    """Single-quote a SQL string literal."""
    return "'" + val.replace("'", "''") + "'"


def _jsonable(v):
    """Coerce warehouse values that json.dumps cannot handle. SIGDS_WAL tables can
    carry BINARY columns (e.g. DS_ID) that come back as bytes/bytearray; decode as
    UTF-8 where possible, else hex. Decimals fall back to float."""
    if isinstance(v, (bytes, bytearray)):
        try:
            return bytes(v).decode("utf-8")
        except Exception:
            return bytes(v).hex()
    try:
        import decimal
        if isinstance(v, decimal.Decimal):
            return int(v) if v == v.to_integral_value() else float(v)
    except Exception:
        pass
    return v


def latest_connection_details(session, fq_raw, conn_filter, org_filter):
    """
    Read the connection_detail rows from the most recent extract snapshot and
    return, per connection, the distinct writeback + WAL (db, schema) locations.
    Each connection carries its ORG_ID so downstream rows stay org-attributed.
    Latest snapshot is resolved per (ORG_ID, connection) so multiple orgs in the
    same raw table do not shadow one another.
    """
    filt = ""
    if conn_filter:
        ids = [c.strip() for c in conn_filter.split(",") if c.strip()]
        if ids:
            in_list = ", ".join(_q_literal(i) for i in ids)
            filt = f"AND OBJECT_ID IN ({in_list})"
    if org_filter:
        filt += f" AND ORG_ID = {_q_literal(org_filter)}"

    rows = session.sql(f"""
        WITH latest AS (
            SELECT ORG_ID, OBJECT_ID, PAYLOAD,
                   ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID
                                      ORDER BY SNAPSHOT_TS DESC) AS rn
            FROM {fq_raw}
            WHERE OBJECT_TYPE = 'connection_detail'
            {filt}
        )
        SELECT
            ORG_ID                                       AS ORG_ID,
            OBJECT_ID                                   AS CONNECTION_ID,
            PAYLOAD:type::STRING                         AS CONNECTION_TYPE,
            PAYLOAD:account::STRING                      AS ACCOUNT,
            PAYLOAD:warehouse::STRING                    AS WAREHOUSE,
            PAYLOAD:inputTableAuditLogSchema:database::STRING AS WAL_DB,
            PAYLOAD:inputTableAuditLogSchema:schema::STRING   AS WAL_SCHEMA,
            PAYLOAD:writebackSchemas                     AS WB_SCHEMAS,
            PAYLOAD:writebacks                           AS WB_LEGACY
        FROM latest
        WHERE rn = 1
    """).collect()

    conns = []
    for r in rows:
        wb_locs = set()
        for arr_json in (r["WB_SCHEMAS"], r["WB_LEGACY"]):
            if not arr_json:
                continue
            try:
                arr = json.loads(arr_json) if isinstance(arr_json, str) else arr_json
            except (TypeError, ValueError):
                arr = []
            for item in (arr or []):
                db, sch = item.get("database"), item.get("schema")
                if db and sch:
                    wb_locs.add((db, sch))
        conns.append({
            "org_id":          r["ORG_ID"],
            "connection_id":   r["CONNECTION_ID"],
            "connection_type": r["CONNECTION_TYPE"],
            "account":         r["ACCOUNT"],
            "warehouse":       r["WAREHOUSE"],
            "wal_db":          r["WAL_DB"],
            "wal_schema":      r["WAL_SCHEMA"],
            "writeback_locations": sorted(wb_locs),
        })
    return conns


def list_tables(session, db, schema, name_prefix=None):
    """
    Inventory tables in a schema via INFORMATION_SCHEMA. When name_prefix is
    given, match it literally with STARTSWITH (avoids LIKE wildcard/escape
    ambiguity on the '_' in SIGDS_WAL_*). Returns dicts; raises if unreadable.
    """
    prefix_filter = ""
    if name_prefix:
        prefix_filter = f"AND STARTSWITH(UPPER(TABLE_NAME), {_q_literal(name_prefix.upper())})"
    rows = session.sql(f"""
        SELECT TABLE_NAME, ROW_COUNT, BYTES, CREATED, LAST_ALTERED
        FROM {_q_ident(db)}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = {_q_literal(schema)}
          AND TABLE_TYPE = 'BASE TABLE'
          {prefix_filter}
    """).collect()
    return [{
        "table_name":   r["TABLE_NAME"],
        "row_count":    r["ROW_COUNT"],
        "bytes":        r["BYTES"],
        "created":      r["CREATED"].isoformat() if r["CREATED"] else None,
        "last_altered": r["LAST_ALTERED"].isoformat() if r["LAST_ALTERED"] else None,
    } for r in rows]


def scan_wal(session, db, schema, table_name):
    """
    Read the WAL watermark + latest edit metadata for one SIGDS WAL table.
    TIMESTAMP is a reserved word -> double-quoted. METADATA/EDIT are JSON.
    """
    fqn = f"{_q_ident(db)}.{_q_ident(schema)}.{_q_ident(table_name)}"
    rows = session.sql(f"""
        SELECT * EXCLUDE rn FROM (
            SELECT
                EDIT_NUM                                              AS WAL_MAX_EDIT_NUM,
                DS_ID                                                 AS WAL_DS_ID,
                "TIMESTAMP"                                           AS WAL_LAST_EDIT_AT,
                TRY_PARSE_JSON(METADATA):tableName::VARCHAR           AS SIGDS_TABLE,
                TRY_PARSE_JSON(METADATA):workbookId::VARCHAR          AS WORKBOOK_ID,
                COALESCE(
                    TRY_PARSE_JSON(METADATA):sigmaUrl::VARCHAR,
                    TRY_PARSE_JSON(METADATA):workbookUrl::VARCHAR
                )                                                     AS WAL_WORKBOOK_URL,
                COALESCE(
                    TRY_PARSE_JSON(METADATA):elementTitle::VARCHAR,
                    TRY_PARSE_JSON(METADATA):inputTableTitle::VARCHAR
                )                                                     AS WAL_INPUT_TABLE_NAME,
                COALESCE(
                    TRY_PARSE_JSON(METADATA):userEmail::VARCHAR,
                    TRY_PARSE_JSON(EDIT):updateRow:blameInfo:updatedBy::VARCHAR,
                    TRY_PARSE_JSON(EDIT):addRow:blameInfo:updatedBy::VARCHAR
                )                                                     AS WAL_LAST_EDIT_BY,
                COUNT(*)        OVER ()                               AS WAL_EDIT_COUNT,
                ROW_NUMBER()    OVER (ORDER BY EDIT_NUM DESC)         AS rn
            FROM {fqn}
        )
        WHERE rn = 1
    """).collect()
    if not rows:
        return None
    r = rows[0]
    return {k: _jsonable(v) for k, v in {
        "wal_max_edit_num":     r["WAL_MAX_EDIT_NUM"],
        "wal_edit_count":       r["WAL_EDIT_COUNT"],
        "wal_ds_id":            r["WAL_DS_ID"],
        "wal_last_edit_at":     r["WAL_LAST_EDIT_AT"].isoformat() if r["WAL_LAST_EDIT_AT"] else None,
        "sigds_table":          r["SIGDS_TABLE"],
        "workbook_id":          r["WORKBOOK_ID"],
        "wal_workbook_url":     r["WAL_WORKBOOK_URL"],
        "wal_input_table_name": r["WAL_INPUT_TABLE_NAME"],
        "wal_last_edit_by":     r["WAL_LAST_EDIT_BY"],
    }.items()}


def main(session, target_database, target_schema, target_table, connection_filter, org_filter):
    snapshot_id = str(uuid.uuid4())
    snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    fq_raw = f'"{target_database}"."{target_schema}"."{target_table}"'

    conns = latest_connection_details(session, fq_raw, connection_filter, org_filter)
    if not conns:
        return json.dumps({"error": "no connection_detail rows found -- run sigma_org_extract first"})

    raw_rows = []   # (object_type, object_id, org_id, payload_json)
    stats = {"connections": len(conns), "writeback_table": 0,
             "writeback_wal": 0, "unreachable": 0}

    for c in conns:
        cid = c["connection_id"]
        org_id = c["org_id"]
        reachable = True

        # --- writeback_table: inventory the input/writeback DATA tables in each
        #     writeback output schema. These have arbitrary (user-given) names --
        #     the SIGDS_WAL_* WAL tables are excluded; they are scanned below.
        for (db, sch) in c["writeback_locations"]:
            try:
                tables = list_tables(session, db, sch)
            except Exception as e:
                reachable = False
                raw_rows.append(("writeback_table",
                                 f"{cid}:{db}.{sch}", org_id,
                                 json.dumps({"connectionId": cid, "database": db,
                                             "schema": sch, "scanReachable": False,
                                             "error": str(e)})))
                continue
            for t in tables:
                if t["table_name"].upper().startswith("SIGDS_WAL"):
                    continue  # WAL table, not a data table
                payload = {"connectionId": cid, "connectionType": c["connection_type"],
                           "database": db, "schema": sch, "scanReachable": True, **t}
                raw_rows.append(("writeback_table",
                                 f"{cid}:{db}.{sch}.{t['table_name']}", org_id,
                                 json.dumps(payload)))
                stats["writeback_table"] += 1

        # --- writeback_wal: watermark per SIGDS_WAL_* edit-log table.
        #     By Sigma's design the write-access destination schema is reserved for
        #     ALL internal write-back objects, so the SIGDS_WAL_* tables live in the
        #     writeback location(s) alongside the SIGDS_ data tables. The dedicated
        #     inputTableAuditLogSchema API field is unreliable (frequently null even
        #     when WAL tables exist), so scan the writeback location(s) as the PRIMARY
        #     path and add inputTableAuditLogSchema only if set and distinct.
        wal_locations = list(c["writeback_locations"])
        wal_db, wal_sch = c["wal_db"], c["wal_schema"]
        if wal_db and wal_sch and (wal_db, wal_sch) not in wal_locations:
            wal_locations.append((wal_db, wal_sch))

        for (wdb, wsch) in wal_locations:
            try:
                wal_tables = list_tables(session, wdb, wsch, name_prefix="SIGDS_WAL")
            except Exception as e:
                reachable = False
                raw_rows.append(("writeback_wal",
                                 f"{cid}:{wdb}.{wsch}", org_id,
                                 json.dumps({"connectionId": cid, "database": wdb,
                                             "schema": wsch, "scanReachable": False,
                                             "error": str(e)})))
                continue
            for t in wal_tables:
                try:
                    wal = scan_wal(session, wdb, wsch, t["table_name"])
                except Exception:
                    wal = None
                if wal is None:
                    continue
                payload = {"connectionId": cid, "walDatabase": wdb,
                           "walSchema": wsch, "walTable": t["table_name"],
                           "scanReachable": True, **wal}
                raw_rows.append(("writeback_wal",
                                 f"{cid}:{wal['sigds_table'] or t['table_name']}", org_id,
                                 json.dumps(payload)))
                stats["writeback_wal"] += 1

        if not reachable:
            stats["unreachable"] += 1

    # --- land everything as raw VARIANT snapshots ------------------------------
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_raw} (
            SNAPSHOT_ID   STRING,
            SNAPSHOT_TS   TIMESTAMP_NTZ,
            ORG_ID        STRING,
            OBJECT_TYPE   STRING,
            OBJECT_ID     STRING,
            PAYLOAD       VARIANT,
            EXTRACTED_AT  TIMESTAMP_NTZ
        )
    """).collect()

    if raw_rows:
        stage_schema = StructType([
            StructField("OBJECT_TYPE", StringType()),
            StructField("OBJECT_ID",   StringType()),
            StructField("ORG_ID",      StringType()),
            StructField("PAYLOAD_STR", StringType()),
        ])
        df = session.create_dataframe(raw_rows, schema=stage_schema)
        final_df = df.select(
            lit(snapshot_id).alias("SNAPSHOT_ID"),
            to_timestamp_ntz(lit(snapshot_ts.isoformat())).alias("SNAPSHOT_TS"),
            col("ORG_ID"),
            col("OBJECT_TYPE"),
            col("OBJECT_ID"),
            parse_json(col("PAYLOAD_STR")).alias("PAYLOAD"),
            to_timestamp_ntz(lit(snapshot_ts.isoformat())).alias("EXTRACTED_AT"),
        )
        final_df.write.mode("append").save_as_table(
            [target_database, target_schema, target_table]
        )

    return json.dumps({
        "snapshot_id": snapshot_id,
        "snapshot_ts": snapshot_ts.isoformat(),
        "rows_landed": len(raw_rows),
        "stats": stats,
    })
$$;
