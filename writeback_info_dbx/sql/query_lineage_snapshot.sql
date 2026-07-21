-- =============================================================================
-- query_lineage_snapshot.sql
--
-- Reporting view over the TWO artifacts Phase 8 of populate_sigds_workbook_map.py
-- (enable_query_lineage=true) already writes:
--   1. SIGDS_WORKBOOK_MAP.LINEAGE_* — the rolling-window summary (one row per
--      SIGDS_TABLE + SCAN_SCHEMA), recomputed every run.
--   2. SIGMA_QUERY_LINEAGE_RAW      — the watermarked landing table of individual
--      parsed lineage rows (one row per statement x source table) Phase 8
--      accumulates incrementally.
--
-- This file does NOT query system.access.table_lineage / system.query.history
-- itself — unlike sql/query_history_lineage.sql (the manual/exploratory
-- version), it only reads the two tables above. That means:
--   * No system-table grant is needed to run this — just SELECT on
--     SIGDS_WORKBOOK_MAP and SIGMA_QUERY_LINEAGE_RAW, which any reporting/BI
--     identity already has reason to hold.
--   * It only has data once Phase 8 has actually run for a given SCAN_SCHEMA —
--     see LINEAGE_CROSS_CHECK below for how "not yet scanned" is distinguished
--     from "scanned, zero activity".
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> with the Unity
-- Catalog catalog and schema where SIGDS_WORKBOOK_MAP / SIGMA_QUERY_LINEAGE_RAW
-- reside (i.e. your map_schema).
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;


-- =============================================================================
-- V_SIGDS_LINEAGE_SNAPSHOT
--   One row per SIGDS_TABLE + SCAN_SCHEMA. Combines the map's WAL/API context
--   and Phase 8's LINEAGE_* summary with a couple of extra facts only the raw
--   table can answer (distinct querying users, distinct source objects — e.g.
--   more than one workbook driving the same input table), plus a cross-check
--   flag that surfaces disagreement between the WAL-based cleanup signals and
--   observed query activity.
-- =============================================================================
CREATE OR REPLACE VIEW V_SIGDS_LINEAGE_SNAPSHOT AS
WITH raw_agg AS (
    SELECT
        SCAN_SCHEMA,
        SIGDS_TABLE,
        COUNT(DISTINCT COALESCE(SOURCE_OBJECT_ID, SOURCE_URL)) AS DISTINCT_SOURCE_OBJECTS,
        COUNT(DISTINCT SIGMA_USER_EMAIL)                       AS DISTINCT_QUERYING_USERS
    FROM SIGMA_QUERY_LINEAGE_RAW
    GROUP BY SCAN_SCHEMA, SIGDS_TABLE
)
SELECT
    m.SCAN_SCHEMA,
    m.SIGDS_TABLE,
    m.WORKBOOK_ID,
    m.WORKBOOK_NAME,
    m.IS_ORPHANED,
    m.IS_DELETED,
    m.API_IS_ARCHIVED,
    m.LINEAGE_SELECT_COUNT,
    m.LINEAGE_DISTINCT_QUERY_COUNT,
    m.LINEAGE_LAST_QUERIED_AT,
    m.LINEAGE_LAST_QUERIED_OBJECT_URL,
    m.LINEAGE_LAST_QUERIED_OBJECT_KIND,
    m.LINEAGE_LAST_QUERIED_OBJECT_ID,
    m.LINEAGE_LAST_QUERIED_BY_EMAIL,
    m.LINEAGE_TAG_STATUS,
    m.LINEAGE_REFRESHED_AT,
    r.DISTINCT_SOURCE_OBJECTS,
    r.DISTINCT_QUERYING_USERS,
    -- Three-way distinction, not just a boolean:
    --   'Lineage not yet scanned' — Phase 8 has never run for this SCAN_SCHEMA
    --     (LINEAGE_REFRESHED_AT is NULL); absence of data, not evidence.
    --   'No corroborating query activity' — Phase 8 HAS run and found zero
    --     SELECTs in the lookback window; corroborates (does not by itself
    --     prove) that the WAL-based flags are right.
    --   'CHECK — ...' — Phase 8 found activity that contradicts a table
    --     already flagged for cleanup by the WAL heuristic; the one case
    --     worth a human looking at before acting.
    CASE
        WHEN (COALESCE(m.IS_DELETED, FALSE) = TRUE OR COALESCE(m.API_IS_ARCHIVED, FALSE) = TRUE)
             AND COALESCE(m.LINEAGE_SELECT_COUNT, 0) > 0
            THEN 'CHECK — flagged for cleanup but actively queried in the lookback window'
        WHEN m.LINEAGE_REFRESHED_AT IS NULL
            THEN 'Lineage not yet scanned for this schema (enable_query_lineage not yet run)'
        WHEN COALESCE(m.IS_DELETED, FALSE) = FALSE
             AND COALESCE(m.API_IS_ARCHIVED, FALSE) = FALSE
             AND COALESCE(m.LINEAGE_SELECT_COUNT, 0) = 0
            THEN 'No corroborating query activity in lookback window'
        ELSE 'Consistent'
    END                                                          AS LINEAGE_CROSS_CHECK
FROM SIGDS_WORKBOOK_MAP m
LEFT JOIN raw_agg r
  -- upper(): r.SIGDS_TABLE derives from system.access.table_lineage
  -- (lowercase), m.SIGDS_TABLE keeps Sigma's original mixed case.
  ON upper(r.SCAN_SCHEMA) = upper(m.SCAN_SCHEMA)
  AND upper(r.SIGDS_TABLE) = upper(m.SIGDS_TABLE);


-- =============================================================================
-- Example: just the tables worth a second look.
-- =============================================================================
SELECT *
FROM   V_SIGDS_LINEAGE_SNAPSHOT
WHERE  LINEAGE_CROSS_CHECK LIKE 'CHECK%'
ORDER  BY LINEAGE_SELECT_COUNT DESC;
