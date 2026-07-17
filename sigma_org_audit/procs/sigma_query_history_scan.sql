-- ==============================================================================
-- sigma_query_history_scan.sql
--
-- Query-history enrichment layer for the sigma_org_audit writeback governance.
--
-- Sigma appends a JSON comment to every generated warehouse query, prefixed
-- "-- Sigma <U+03A3> " (the Greek capital sigma), carrying the source workbook
-- URL, the acting user's email, and a request id. By joining ACCOUNT_USAGE
-- QUERY_HISTORY to ACCESS_HISTORY we get GROUND-TRUTH lineage: which workbook /
-- user actually read or wrote each writeback (input) table -- a far stronger
-- ownership signal than the WAL-watermark heuristic. This is an ENHANCEMENT to
-- V_WRITEBACK_GOVERNANCE, never a replacement: it can only ever make a table
-- look MORE owned / less archivable, never more orphaned (absence of access
-- history is treated as inconclusive, not as evidence of orphanhood).
--
-- Scope is DISCOVERED, not hardcoded: the writeback (database, schema) pairs are
-- read from the same connection_detail rows sigma_writeback_scan uses, so this
-- stays aligned with exactly the schemas the writeback scan already covers.
--
-- Incremental / watermark load: the first run backfills BACKFILL_DAYS (default
-- 90); every later run resumes from the latest startTime already landed, minus
-- OVERLAP_HOURS (default 12) to cover ACCESS_HISTORY's latency. Raw is
-- append-only, so the overlap creates duplicate rows that the stage view
-- (STG_WRITEBACK_ACCESS) collapses latest-wins by (queryId, object, accessKind).
--
-- Prerequisites
--   1. Run setup_prerequisites.sql as ACCOUNTADMIN, INCLUDING section 7:
--        GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <YOUR_ROLE>;
--      Without it QUERY_HISTORY / ACCESS_HISTORY are unreadable and this proc
--      degrades loudly (logs a WARN, lands nothing, returns skipped=true).
--   2. Run sigma_org_extract first (for the connection_detail rows) and ideally
--      sigma_writeback_scan (they share the same RAW_SIGMA_OBJECTS table).
--
-- Caveats (documented in README):
--   * ACCESS_HISTORY is a Snowflake Enterprise Edition (or higher) feature.
--   * ACCOUNT_USAGE latency: QUERY_HISTORY up to ~45 min, ACCESS_HISTORY up to
--     ~3 h -- treat the most recent few hours as provisional.
--   * ACCOUNT_USAGE retention is 365 days; landing to raw accumulates history
--     beyond that window over successive runs.
--   * Per-account only: sees activity in the Snowflake account this proc runs
--     in. Cross-account writeback stays out of scope (tracked separately).
--
-- Parameters
--   TARGET_DATABASE  -- database holding RAW_SIGMA_OBJECTS (required)
--   TARGET_SCHEMA    -- schema holding RAW_SIGMA_OBJECTS (required)
--   TARGET_TABLE     -- raw table name (optional, default RAW_SIGMA_OBJECTS)
--   BACKFILL_DAYS    -- first-run look-back window in days (optional, default 90)
--   OVERLAP_HOURS    -- re-scan buffer before the watermark (optional, default 12)
--   ORG_FILTER       -- optional ORG_ID to scope discovery to one org
--
-- Example
--   CALL sigma_query_history_scan('MY_DB', 'MY_SCHEMA');
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_query_history_scan(
    TARGET_DATABASE STRING,
    TARGET_SCHEMA   STRING,
    TARGET_TABLE    STRING DEFAULT 'RAW_SIGMA_OBJECTS',
    BACKFILL_DAYS   NUMBER DEFAULT 90,
    OVERLAP_HOURS   NUMBER DEFAULT 12,
    ORG_FILTER      STRING DEFAULT NULL
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

# The Sigma comment prefix: "-- Sigma <sigma> " where <sigma> is U+03A3.
SIGMA_PREFIX = "-- Sigma Σ "


def _q_literal(val):
    """Single-quote a SQL string literal, escaping embedded quotes."""
    return "'" + str(val).replace("'", "''") + "'"


def discover_writeback_locations(session, fq_raw, org_filter):
    """Distinct (database, schema) writeback pairs from the latest connection_detail
    snapshot -- the same discovery sigma_writeback_scan uses, so scope stays aligned."""
    filt = f"AND ORG_ID = {_q_literal(org_filter)}" if org_filter else ""
    rows = session.sql(f"""
        WITH latest AS (
            SELECT ORG_ID, OBJECT_ID, PAYLOAD,
                   ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID
                                      ORDER BY SNAPSHOT_TS DESC) AS rn
            FROM {fq_raw}
            WHERE OBJECT_TYPE = 'connection_detail' {filt}
        )
        SELECT PAYLOAD:writebackSchemas AS WB_SCHEMAS,
               PAYLOAD:writebacks       AS WB_LEGACY
        FROM latest WHERE rn = 1
    """).collect()

    locs = set()
    for r in rows:
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
                    locs.add((db.upper(), sch.upper()))
    return sorted(locs)


def main(session, target_database, target_schema, target_table,
         backfill_days, overlap_hours, org_filter):
    snapshot_id = str(uuid.uuid4())
    snapshot_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    fq_raw = f'"{target_database}"."{target_schema}"."{target_table}"'
    fq_log = f'"{target_database}"."{target_schema}"."SIGMA_EXTRACT_LOG"'

    try:
        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {fq_log} (
                LOGGED_AT TIMESTAMP_NTZ, RUN_ID STRING, ORG_ID STRING,
                PHASE STRING, DETAIL STRING)
        """).collect()
    except Exception:
        pass

    def log(phase, detail=None):
        try:
            session.sql(
                f"INSERT INTO {fq_log} (LOGGED_AT, RUN_ID, ORG_ID, PHASE, DETAIL) "
                f"VALUES (CURRENT_TIMESTAMP(), ?, NULL, ?, ?)",
                params=[snapshot_id, phase, None if detail is None else str(detail)],
            ).collect()
        except Exception:
            pass

    log("query-history start", "scan beginning")

    # Raw table must exist (extract/writeback normally created it already).
    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {fq_raw} (
            SNAPSHOT_ID STRING, SNAPSHOT_TS TIMESTAMP_NTZ, ORG_ID STRING,
            OBJECT_TYPE STRING, OBJECT_ID STRING, PAYLOAD VARIANT,
            EXTRACTED_AT TIMESTAMP_NTZ)
    """).collect()

    # --- Fail fast + loud if ACCOUNT_USAGE is unreadable (no IMPORTED PRIVILEGES
    #     grant) or ACCESS_HISTORY is absent (edition below Enterprise). ----------
    try:
        session.sql("""
            SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY LIMIT 1
        """).collect()
    except Exception as e:
        msg = ("cannot read SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY -- ensure "
               "GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE (setup section 7) "
               "and that this is Enterprise Edition or higher. Skipped.")
        log("query-history done", f"SKIPPED: {msg} ({e})")
        return json.dumps({"skipped": True, "reason": msg})

    # --- Scope: discovered writeback (db, schema) pairs -------------------------
    locs = discover_writeback_locations(session, fq_raw, org_filter)
    if not locs:
        log("query-history done", "no writeback locations discovered "
            "(run sigma_org_extract / sigma_writeback_scan first)")
        return json.dumps({"skipped": True, "reason": "no writeback locations"})
    log("locations discovered", f"{len(locs)} writeback schema(s)")
    loc_clause = " OR ".join(
        f"(UPPER(SPLIT_PART(object_name,'.',1))={_q_literal(db)} "
        f"AND UPPER(SPLIT_PART(object_name,'.',2))={_q_literal(sch)})"
        for (db, sch) in locs
    )

    # --- Watermark: resume from latest landed startTime, minus overlap buffer;
    #     first run backfills BACKFILL_DAYS. ------------------------------------
    wm = session.sql(f"""
        SELECT MAX(PAYLOAD:startTime::TIMESTAMP_NTZ) AS WM
        FROM {fq_raw} WHERE OBJECT_TYPE = 'writeback_access'
    """).collect()
    watermark = wm[0]["WM"] if wm else None
    if watermark is None:
        since_sql = f"DATEADD(day, -{int(backfill_days)}, CURRENT_TIMESTAMP())"
        log("watermark", f"first run -- backfilling {int(backfill_days)} days")
    else:
        since_sql = (f"DATEADD(hour, -{int(overlap_hours)}, "
                     f"'{watermark.isoformat()}'::TIMESTAMP_NTZ)")
        log("watermark", f"resuming from {watermark.isoformat()} "
                         f"(-{int(overlap_hours)}h overlap)")

    prefix_lit = _q_literal(SIGMA_PREFIX)

    # --- Land: one INSERT..SELECT joining QUERY_HISTORY x ACCESS_HISTORY.
    #     Reads flatten BASE_OBJECTS_ACCESSED; writes flatten OBJECTS_MODIFIED.
    #     ORG_ID is left NULL here and resolved in STG_WRITEBACK_ACCESS via the
    #     workbook URL, so attribution stays a stage-layer concern. ------------
    insert_sql = f"""
    INSERT INTO {fq_raw}
        (SNAPSHOT_ID, SNAPSHOT_TS, ORG_ID, OBJECT_TYPE, OBJECT_ID, PAYLOAD, EXTRACTED_AT)
    WITH scoped AS (
        SELECT QUERY_ID, START_TIME, USER_NAME, ROLE_NAME, QUERY_TYPE, ROWS_INSERTED,
               TRY_PARSE_JSON(
                   SUBSTR(QUERY_TEXT, POSITION({prefix_lit} IN QUERY_TEXT)
                          + LENGTH({prefix_lit}))) AS m
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE START_TIME >= {since_sql}
          AND QUERY_TYPE IN ('SELECT','INSERT')
          AND POSITION({prefix_lit} IN QUERY_TEXT) > 0
    ),
    events AS (
        SELECT s.QUERY_ID, s.START_TIME, s.USER_NAME, s.ROLE_NAME, s.QUERY_TYPE,
               s.ROWS_INSERTED, s.m, 'READ' AS access_kind,
               ao.value:objectName::STRING AS object_name
        FROM scoped s
        JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah ON ah.QUERY_ID = s.QUERY_ID,
             LATERAL FLATTEN(input => ah.BASE_OBJECTS_ACCESSED) ao
        WHERE s.QUERY_TYPE = 'SELECT' AND s.m IS NOT NULL
        UNION ALL
        SELECT s.QUERY_ID, s.START_TIME, s.USER_NAME, s.ROLE_NAME, s.QUERY_TYPE,
               s.ROWS_INSERTED, s.m, 'INSERT' AS access_kind,
               om.value:objectName::STRING AS object_name
        FROM scoped s
        JOIN SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah ON ah.QUERY_ID = s.QUERY_ID,
             LATERAL FLATTEN(input => ah.OBJECTS_MODIFIED) om
        WHERE s.QUERY_TYPE = 'INSERT' AND s.ROWS_INSERTED > 0 AND s.m IS NOT NULL
    ),
    filtered AS (
        SELECT * FROM events WHERE object_name IS NOT NULL AND ({loc_clause})
    )
    SELECT
        '{snapshot_id}' AS SNAPSHOT_ID,
        '{snapshot_ts.isoformat()}'::TIMESTAMP_NTZ AS SNAPSHOT_TS,
        NULL AS ORG_ID,
        'writeback_access' AS OBJECT_TYPE,
        QUERY_ID || ':' || object_name || ':' || access_kind AS OBJECT_ID,
        OBJECT_CONSTRUCT(
            'queryId',        QUERY_ID,
            'startTime',      TO_VARCHAR(START_TIME),
            'userName',       USER_NAME,
            'roleName',       ROLE_NAME,
            'queryType',      QUERY_TYPE,
            'accessKind',     access_kind,
            'objectName',     object_name,
            'rowsInserted',   ROWS_INSERTED,
            'sourceUrl',      m:sourceUrl::STRING,
            'sigmaKind',      m:kind::STRING,
            'sigmaUserEmail', m:email::STRING,
            'sigmaUserId',    m:"user-id"::STRING,
            'requestId',      m:"request-id"::STRING
        ) AS PAYLOAD,
        '{snapshot_ts.isoformat()}'::TIMESTAMP_NTZ AS EXTRACTED_AT
    FROM filtered
    """
    log("landing", "querying account_usage")
    session.sql(insert_sql).collect()

    landed = session.sql(
        f"SELECT COUNT(*) AS N FROM {fq_raw} "
        f"WHERE OBJECT_TYPE='writeback_access' AND SNAPSHOT_ID='{snapshot_id}'"
    ).collect()[0]["N"]

    log("query-history done", f"{landed} access rows landed across {len(locs)} schema(s)")
    return json.dumps({
        "snapshot_id": snapshot_id,
        "snapshot_ts": snapshot_ts.isoformat(),
        "rows_landed": int(landed),
        "writeback_locations": len(locs),
    })
$$;
