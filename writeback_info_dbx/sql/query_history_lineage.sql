-- =============================================================================
-- query_history_lineage.sql
--
-- Ground-truth lineage between Sigma workbooks/data models and their input
-- (writeback) tables, mined from Unity Catalog system tables rather than the
-- WAL-watermark heuristic SIGDS_WORKBOOK_MAP relies on.
--
-- Sigma tags every warehouse query it issues with a trailing SQL comment
-- carrying the originating workbook/data-model URL, e.g.:
--
--   -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/acme/workbook/My-WB-527Ldxl0hT3JKHuLw1USp4?:displayNodeId=fk0QY3zA9x","kind":"adhoc","request-id":"g019f...","user-id":"8Uom...","email":"user@example.com"}
--   -- Sigma Σ {"sourceUrl":"https://app.sigmacomputing.com/acme/data-model/model-dbx-test-viQz50PbiSg7zYkxBgo4P?:displayNodeId=NRYAsMEvRF","kind":"adhoc",...}
--
-- system.access.table_lineage records every table read/write with its
-- statement_id; joining to system.query.history on statement_id recovers the
-- full statement_text (and therefore the comment) for SELECTs. This is an
-- ENHANCEMENT to SIGDS_WORKBOOK_MAP, never a replacement: absence of a lineage
-- row means "not observed in the lookback window", not "orphaned" — the WAL
-- heuristic remains the primary signal.
--
-- This is the Databricks analog of sigma_org_audit's
-- procs/sigma_query_history_scan.sql (Snowflake ACCOUNT_USAGE QUERY_HISTORY x
-- ACCESS_HISTORY). Key difference: Unity Catalog system tables are already
-- persisted with their own retention (no separate "land into raw" step is
-- needed the way Snowflake's ACCOUNT_USAGE required landing before it could be
-- queried cross-session) — this view can be queried live.
--
-- Prerequisites
--   * System tables schema enabled on the metastore (Catalog Explorer ->
--     System Tables -> enable "access" and "query" schemas, or ask your
--     Databricks account admin — this is a one-time, account-level step).
--   * SELECT granted on system.access.table_lineage and system.query.history
--     to the identity running this (or querying the view).
--
-- Caveats
--   * system.access.table_lineage retention is 365 days per Databricks docs;
--     query beyond that window returns nothing (not an error).
--   * table_lineage / query.history land with some delay (typically minutes,
--     occasionally longer under load) — treat the most recent few minutes as
--     provisional.
--   * kind='adhoc' covers interactive/API-triggered queries. Scheduled
--     materializations or other non-adhoc kinds may carry a different (or no)
--     tag — LINEAGE_TAG_STATUS below makes that visible rather than
--     silently dropping the row.
--   * Per-metastore scope: only sees activity against Unity Catalog tables in
--     this metastore. Cross-workspace/cross-account writeback is out of scope.
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> with the schema
-- being scanned (the same schema(s) listed in the job's `schemas` variable).
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;


-- =============================================================================
-- V_SIGMA_QUERY_LINEAGE
--   One row per (statement, source table) SELECT observed against a writeback
--   table in this schema, with the Sigma comment tag parsed out.
-- =============================================================================
CREATE OR REPLACE VIEW V_SIGMA_QUERY_LINEAGE AS
WITH tagged AS (
    SELECT
        tl.event_time,
        tl.event_date,
        tl.statement_id,
        tl.created_by,
        tl.source_table_full_name,
        tl.source_table_catalog,
        tl.source_table_schema,
        tl.source_table_name,
        tl.source_type,
        tl.entity_type,
        tl.entity_metadata,
        tl.direct_access,
        qh.statement_text,
        qh.statement_type,
        qh.executed_by,
        qh.execution_status,
        -- The comment sits at the tail of the generated SQL; pull the JSON
        -- object out of it regardless of what precedes it on the line.
        -- NULLIF: regexp_extract returns '' (not NULL) on no match, which
        -- would otherwise make every downstream NULL-check silently wrong.
        NULLIF(regexp_extract(qh.statement_text, 'Sigma\\s+Σ\\s+(\\{.*\\})', 1), '') AS sigma_tag_json
    FROM system.access.table_lineage tl
    LEFT JOIN system.query.history qh
      ON tl.statement_id = qh.statement_id
    WHERE tl.event_date >= CURRENT_DATE() - INTERVAL 90 DAYS   -- partition prune
      AND tl.event_time >= NOW() - INTERVAL 90 DAYS            -- precise boundary
      -- lower(): system.access.table_lineage reports Unity Catalog
      -- identifiers in lowercase regardless of the case used at creation.
      AND lower(tl.source_table_schema) = lower('<YOUR_SCHEMA>')
      AND tl.source_type = 'TABLE'
      AND qh.statement_type = 'SELECT'
),
parsed AS (
    SELECT
        *,
        get_json_object(sigma_tag_json, '$.sourceUrl')    AS source_url,
        get_json_object(sigma_tag_json, '$.kind')         AS sigma_kind,
        get_json_object(sigma_tag_json, '$."request-id"') AS request_id,
        get_json_object(sigma_tag_json, '$."user-id"')    AS sigma_user_id,
        get_json_object(sigma_tag_json, '$.email')        AS sigma_user_email
    FROM tagged
),
with_path AS (
    SELECT
        *,
        -- The path segment after /workbook/ or /data-model/, query string
        -- stripped: "<slug>-<id>", e.g. "My-WB-527Ldxl0hT3JKHuLw1USp4".
        -- get(array, idx), not array[idx]: under ANSI SQL mode, [idx] THROWS
        -- on out-of-bounds access (e.g. splitting a /workbook/ URL on
        -- '/data-model/' yields a 1-element array, and [1] on that errors)
        -- rather than returning NULL. get() stays NULL-safe regardless.
        get(split(
            coalesce(get(split(source_url, '/data-model/'), 1),
                     get(split(source_url, '/workbook/'), 1)),
            '\\?'
        ), 0) AS source_path_segment
    FROM parsed
)
SELECT
    event_time,
    event_date,
    statement_id,
    created_by,
    source_table_full_name,
    source_table_catalog,
    source_table_schema,
    source_table_name,
    -- Bare SIGDS table name, matching SIGDS_WORKBOOK_MAP.SIGDS_TABLE. NOT
    -- upper()'d: the map keeps the table's original mixed case, and Delta
    -- string comparison is case-sensitive -- joins against it would silently
    -- match nothing otherwise.
    source_table_name                                                   AS SIGDS_TABLE,
    entity_type,
    entity_metadata,
    direct_access,
    statement_type,
    executed_by,
    execution_status,
    source_url                                                          AS WORKBOOK_URL,
    -- WORKBOOK vs DATA_MODEL: which path segment the sourceUrl carries.
    CASE
        WHEN source_url LIKE '%/data-model/%' THEN 'DATA_MODEL'
        WHEN source_url LIKE '%/workbook/%'    THEN 'WORKBOOK'
        ELSE NULL
    END                                                                  AS SOURCE_OBJECT_KIND,
    -- Object id = the trailing '-'-delimited token of the /workbook/<slug>-<id>
    -- or /data-model/<slug>-<id> path segment (ids carry no dash); org slug =
    -- the first path segment after the host. Same convention as the SF port's
    -- STG_WRITEBACK_ACCESS (sigma_org_audit/stage/stage_views.sql).
    -- Explicit capture group + idx=1: regexp_extract's default idx is 1
    -- (expects a capturing group to exist), unlike Snowflake's REGEXP_SUBSTR
    -- this was ported from.
    regexp_extract(source_path_segment, '([^-]+)$', 1)                  AS SOURCE_OBJECT_ID,
    get(split(get(split(source_url, 'sigmacomputing.com/'), 1), '/'), 0) AS SOURCE_ORG_SLUG,
    sigma_kind                                                           AS SIGMA_KIND,
    request_id                                                           AS SIGMA_REQUEST_ID,
    sigma_user_id                                                        AS SIGMA_USER_ID,
    sigma_user_email                                                     AS SIGMA_USER_EMAIL,
    CASE WHEN sigma_tag_json IS NOT NULL THEN 'query_history' ELSE 'none' END
                                                                         AS LINEAGE_TAG_STATUS
FROM with_path;


-- =============================================================================
-- V_SIGMA_QUERY_LINEAGE_SUMMARY
--   One row per SIGDS_TABLE — rolls the raw lineage rows up to the grain that
--   joins cleanly onto SIGDS_WORKBOOK_MAP (SIGDS_TABLE + SCAN_SCHEMA), so this
--   can enrich the WAL-based map with a ground-truth "actually queried"
--   signal without needing every raw statement row.
-- =============================================================================
CREATE OR REPLACE VIEW V_SIGMA_QUERY_LINEAGE_SUMMARY AS
SELECT
    source_table_schema                                       AS SCAN_SCHEMA,
    SIGDS_TABLE,
    COUNT(*)                                                   AS SELECT_COUNT_90D,
    COUNT(DISTINCT statement_id)                               AS DISTINCT_QUERY_COUNT_90D,
    MAX(event_time)                                             AS LAST_QUERIED_AT,
    MIN(event_time)                                             AS FIRST_QUERIED_AT_90D,
    -- Most recently observed workbook/data-model driving this table, so a
    -- table with multiple sources still shows its freshest attribution.
    max_by(WORKBOOK_URL, event_time)                            AS LAST_QUERIED_OBJECT_URL,
    max_by(SOURCE_OBJECT_KIND, event_time)                      AS LAST_QUERIED_OBJECT_KIND,
    max_by(SOURCE_OBJECT_ID, event_time)                        AS LAST_QUERIED_OBJECT_ID,
    max_by(SOURCE_ORG_SLUG, event_time)                         AS LAST_QUERIED_ORG_SLUG,
    max_by(SIGMA_USER_EMAIL, event_time)                        AS LAST_QUERIED_BY_EMAIL,
    COUNT(CASE WHEN LINEAGE_TAG_STATUS = 'query_history' THEN 1 END)
                                                                AS TAGGED_SELECT_COUNT_90D
FROM V_SIGMA_QUERY_LINEAGE
GROUP BY source_table_schema, SIGDS_TABLE;


-- =============================================================================
-- Example: cross-check SIGDS_WORKBOOK_MAP against ground-truth lineage.
-- Surfaces tables the WAL heuristic flags as orphaned/archived/stale that
-- were nonetheless actually SELECTed in the last 90 days (a correction
-- candidate) and, separately, active-looking tables with no recent queries at
-- all (a corroboration of staleness). Never used to make a table look MORE
-- archivable on its own — only to catch false positives / add confidence.
-- =============================================================================
SELECT
    m.SCAN_SCHEMA,
    m.SIGDS_TABLE,
    m.WORKBOOK_ID,
    m.WORKBOOK_NAME,
    m.IS_ORPHANED,
    m.IS_DELETED,
    m.API_IS_ARCHIVED,
    l.SELECT_COUNT_90D,
    l.LAST_QUERIED_AT,
    l.LAST_QUERIED_OBJECT_URL,
    l.LAST_QUERIED_OBJECT_KIND,
    l.LAST_QUERIED_BY_EMAIL,
    CASE
        WHEN (m.IS_DELETED = TRUE OR m.API_IS_ARCHIVED = TRUE)
             AND l.SELECT_COUNT_90D > 0
            THEN 'CHECK — flagged for cleanup but actively queried in last 90d'
        WHEN COALESCE(m.IS_DELETED, FALSE) = FALSE
             AND COALESCE(m.API_IS_ARCHIVED, FALSE) = FALSE
             AND COALESCE(l.SELECT_COUNT_90D, 0) = 0
            THEN 'No corroborating query activity in last 90d'
        ELSE 'Consistent'
    END                                                          AS LINEAGE_CROSS_CHECK
FROM SIGDS_WORKBOOK_MAP m
LEFT JOIN V_SIGMA_QUERY_LINEAGE_SUMMARY l
  -- upper(): l.SIGDS_TABLE derives from system.access.table_lineage
  -- (lowercase), m.SIGDS_TABLE keeps Sigma's original mixed case.
  ON upper(l.SIGDS_TABLE) = upper(m.SIGDS_TABLE)
  AND upper(l.SCAN_SCHEMA) = upper(m.SCAN_SCHEMA)
ORDER BY LINEAGE_CROSS_CHECK, m.SCAN_SCHEMA, m.SIGDS_TABLE;
