-- =============================================================================
-- geninfo_queries.sql
--
-- Read-only reporting queries for SIGDS_WORKBOOK_MAP.
-- These queries cover the same analytical dimensions as archival_scoring.sql
-- (status flags, edit recency, edit volume, storage, legacy WAL, version tags)
-- but are structured as exploratory reporting views rather than a scoring engine.
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> with the Unity
-- Catalog catalog and schema where SIGDS_WORKBOOK_MAP resides.
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;


-- =============================================================================
-- 1) Landscape overview
--    High-level summary of the writeback estate by schema.
--    Useful as a starting point before diving into detail queries.
--    RECLAIMABLE_SIZE_GB = storage held by orphaned, deleted, or archived tables.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  COUNT(*)                                                                       AS TOTAL_TABLES,
  COUNT(CASE WHEN IS_ORPHANED = TRUE THEN 1 END)                                AS ORPHANED,
  COUNT(CASE WHEN IS_DELETED  = TRUE THEN 1 END)                                AS DELETED,
  COUNT(CASE WHEN API_IS_ARCHIVED = TRUE
              AND IS_ORPHANED = FALSE
              AND IS_DELETED  = FALSE THEN 1 END)                               AS ARCHIVED_ACTIVE,
  COUNT(CASE WHEN IS_LEGACY_WAL     = TRUE THEN 1 END)                          AS LEGACY_WAL,
  COUNT(CASE WHEN IS_TAGGED_VERSION = TRUE THEN 1 END)                          AS TAGGED_VERSIONS,
  ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3)            AS TOTAL_SIZE_GB,
  ROUND(
    SUM(
      CASE WHEN IS_ORPHANED = TRUE OR IS_DELETED = TRUE OR API_IS_ARCHIVED = TRUE
           THEN COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)
           ELSE 0
      END
    ) / 1073741824.0, 3
  )                                                                              AS RECLAIMABLE_SIZE_GB
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
GROUP BY SCAN_SCHEMA
ORDER BY SCAN_SCHEMA;


-- =============================================================================
-- 2) Storage reclamation opportunity
--    All tables with a clear archival signal (orphaned, deleted, or archived
--    workbook), ranked by size. This is the foundation for Tier 1/2 candidates
--    in archival_scoring.sql — any row here is worth investigating.
--    REASON shows the strongest archival signal for each row.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  SIGDS_TABLE,
  WAL_TABLE_FQN,
  WORKBOOK_NAME,
  TRIM(
    COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')
  )                                                                              AS OWNER_FULL_NAME,
  CASE
    WHEN IS_ORPHANED = TRUE THEN 'Orphaned — SIGDS table gone'
    WHEN IS_DELETED  = TRUE THEN 'Deleted — WAL table gone'
    WHEN API_IS_ARCHIVED = TRUE THEN 'Workbook archived in Sigma'
    ELSE 'Other'
  END                                                                            AS ARCHIVAL_SIGNAL,
  IS_TAGGED_VERSION,
  VERSION_TAG_NAME,
  ROUND(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) / 1073741824.0, 3)                 AS SIZE_GB,
  WAL_LAST_EDIT_AT,
  DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())                          AS DAYS_SINCE_LAST_EDIT,
  WAL_MAX_EDIT_NUM,
  WAL_WORKBOOK_URL
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED    = TRUE
  OR IS_DELETED  = TRUE
  OR API_IS_ARCHIVED = TRUE
ORDER BY SCAN_SCHEMA, SIGDS_TABLE_SIZE_BYTES DESC NULLS LAST;


-- =============================================================================
-- 3) Active workbooks going stale — review with owners
--    Tables where the workbook is still active in Sigma but writeback
--    activity has dropped off. Grouped into inactivity bands that align
--    with the WAL recency scoring in archival_scoring.sql.
--    These are Tier 3 candidates trending toward Tier 2.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  SIGDS_TABLE,
  WORKBOOK_NAME,
  TRIM(
    COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')
  )                                                                              AS OWNER_FULL_NAME,
  WAL_LAST_EDIT_BY,
  WAL_LAST_EDIT_AT,
  DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())                          AS DAYS_SINCE_LAST_EDIT,
  CASE
    WHEN WAL_LAST_EDIT_AT IS NULL
      OR DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 365  THEN '>365 days'
    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 180  THEN '181–365 days'
    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 90   THEN '91–180 days'
    ELSE '31–90 days'
  END                                                                            AS INACTIVITY_BAND,
  WAL_MAX_EDIT_NUM,
  ROUND(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) / 1048576.0, 2)                    AS SIZE_MB,
  IS_TAGGED_VERSION,
  VERSION_TAG_NAME,
  WAL_WORKBOOK_URL
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED    = FALSE
  AND IS_DELETED = FALSE
  AND API_IS_ARCHIVED = FALSE
  AND (
    WAL_LAST_EDIT_AT IS NULL
    OR WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 30 DAY
  )
ORDER BY SCAN_SCHEMA, WAL_LAST_EDIT_AT ASC NULLS FIRST;


-- =============================================================================
-- 4) Most active writeback tables
--    Inverse of the archival view — surfaces the most heavily used input
--    tables by total edit count and recent activity.
--    Useful for understanding which workbooks are business-critical before
--    any cleanup actions are taken nearby.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  SIGDS_TABLE,
  WORKBOOK_NAME,
  TRIM(
    COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')
  )                                                                              AS OWNER_FULL_NAME,
  WAL_LAST_EDIT_BY,
  WAL_MAX_EDIT_NUM,
  WAL_LAST_EDIT_AT,
  DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())                          AS DAYS_SINCE_LAST_EDIT,
  ROUND(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) / 1048576.0, 2)                    AS SIZE_MB,
  IS_TAGGED_VERSION,
  VERSION_TAG_NAME,
  WAL_WORKBOOK_URL
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED    = FALSE
  AND IS_DELETED = FALSE
  AND WAL_MAX_EDIT_NUM IS NOT NULL
  AND WAL_MAX_EDIT_NUM > 0
ORDER BY SCAN_SCHEMA, WAL_MAX_EDIT_NUM DESC, WAL_LAST_EDIT_AT DESC;


-- =============================================================================
-- 5) Owner accountability summary
--    Rolls up cleanup burden by workbook owner. Highlights which owners
--    have the most archived, orphaned, or long-inactive tables in their name.
--    Useful for triaging outreach before initiating cleanup.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  TRIM(
    COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')
  )                                                                              AS OWNER_FULL_NAME,
  API_OWNER_ID,
  COUNT(*)                                                                       AS TOTAL_TABLES,
  COUNT(CASE WHEN API_IS_ARCHIVED = TRUE THEN 1 END)                            AS ARCHIVED_COUNT,
  COUNT(CASE WHEN IS_ORPHANED     = TRUE THEN 1 END)                            AS ORPHANED_COUNT,
  COUNT(CASE WHEN IS_DELETED      = TRUE THEN 1 END)                            AS DELETED_COUNT,
  COUNT(CASE WHEN WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 180 DAY
               OR WAL_LAST_EDIT_AT IS NULL THEN 1 END)                          AS STALE_180D,
  ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3)            AS TOTAL_SIZE_GB,
  ROUND(
    SUM(
      CASE WHEN IS_ORPHANED = TRUE OR IS_DELETED = TRUE OR API_IS_ARCHIVED = TRUE
           THEN COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) ELSE 0 END
    ) / 1073741824.0, 3
  )                                                                              AS RECLAIMABLE_SIZE_GB
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
WHERE API_OWNER_ID IS NOT NULL
GROUP BY
  SCAN_SCHEMA,
  TRIM(COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')),
  API_OWNER_ID
ORDER BY SCAN_SCHEMA, RECLAIMABLE_SIZE_GB DESC, ARCHIVED_COUNT DESC;


-- =============================================================================
-- 6) Workbooks with multiple input tables
--    Identifies workbooks that have more than one SIGDS table associated.
--    Multi-table workbooks are higher-risk cleanup targets because removing
--    one table may break sibling tables in the same workbook.
--    TABLES_AT_RISK = count of input tables that are archived, orphaned, or stale.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  WORKBOOK_ID,
  WORKBOOK_NAME,
  TRIM(
    COALESCE(MAX(API_OWNER_FIRST_NAME), '') || ' ' || COALESCE(MAX(API_OWNER_LAST_NAME), '')
  )                                                                              AS OWNER_FULL_NAME,
  COUNT(*)                                                                       AS INPUT_TABLE_COUNT,
  COUNT(CASE WHEN API_IS_ARCHIVED = TRUE
               OR IS_ORPHANED     = TRUE
               OR (WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 180 DAY)
             THEN 1 END)                                                         AS TABLES_AT_RISK,
  SUM(WAL_MAX_EDIT_NUM)                                                         AS TOTAL_EDITS,
  MAX(WAL_LAST_EDIT_AT)                                                         AS LAST_EDIT_AT,
  ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3)            AS TOTAL_SIZE_GB,
  MAX(CASE WHEN API_IS_ARCHIVED = TRUE THEN 'Yes' ELSE 'No' END)                AS WORKBOOK_ARCHIVED
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
WHERE WORKBOOK_ID IS NOT NULL
GROUP BY SCAN_SCHEMA, WORKBOOK_ID, WORKBOOK_NAME
HAVING COUNT(*) > 1
ORDER BY SCAN_SCHEMA, INPUT_TABLE_COUNT DESC, TABLES_AT_RISK DESC;


-- =============================================================================
-- 7) Legacy WAL inventory
--    Old sigds_wal_<uuid> tables predate the DS_ID-based naming convention.
--    Broken into active (still being written) vs inactive — active legacy WALs
--    are the higher priority because they are still accumulating data in a
--    format that is harder to track and associate with workbooks.
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  WAL_TABLE_FQN,
  SIGDS_TABLE,
  WORKBOOK_NAME,
  TRIM(
    COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')
  )                                                                              AS OWNER_FULL_NAME,
  WAL_LAST_EDIT_AT,
  DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())                          AS DAYS_SINCE_LAST_EDIT,
  WAL_MAX_EDIT_NUM,
  CASE
    WHEN WAL_LAST_EDIT_AT IS NULL
      OR DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) >= 180
        THEN 'Inactive — low urgency'
    ELSE 'Active — migrate urgently'
  END                                                                            AS MIGRATION_PRIORITY,
  IS_ORPHANED,
  API_IS_ARCHIVED,
  ROUND(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) / 1048576.0, 2)                    AS SIZE_MB,
  WAL_WORKBOOK_URL
FROM <YOUR_CATALOG>.<YOUR_SCHEMA>.SIGDS_WORKBOOK_MAP
WHERE IS_LEGACY_WAL = TRUE
ORDER BY
  SCAN_SCHEMA,
  CASE WHEN WAL_LAST_EDIT_AT IS NULL
            OR DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) >= 180
       THEN 1 ELSE 0 END ASC,   -- active first
  WAL_LAST_EDIT_AT DESC NULLS LAST;
