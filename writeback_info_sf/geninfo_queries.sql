-- =============================================================================
-- geninfo_queries.sql
--
-- Read-only reporting queries for SIGDS_WORKBOOK_MAP (Snowflake).
-- Queries 1-7 are exploratory reporting over the map (status flags, edit recency,
-- edit volume, storage, legacy WAL, version tags). Queries 8-10 use the v2 additions:
-- query-history access evidence, score history, and quarantine monitoring.
-- The scoring model itself lives in scoring_views.sql (see archival_scoring.sql).
--
-- Before running, replace <YOUR_DATABASE> and <YOUR_SCHEMA> with the Snowflake
-- database and schema where SIGDS_WORKBOOK_MAP resides.
-- =============================================================================

USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;


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
FROM SIGDS_WORKBOOK_MAP
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
FROM SIGDS_WORKBOOK_MAP
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
FROM SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED    = FALSE
  AND IS_DELETED = FALSE
  AND API_IS_ARCHIVED = FALSE
  AND (
    WAL_LAST_EDIT_AT IS NULL
    OR WAL_LAST_EDIT_AT < DATEADD(DAY, -30, CURRENT_TIMESTAMP())
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
FROM SIGDS_WORKBOOK_MAP
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
  COUNT(CASE WHEN WAL_LAST_EDIT_AT < DATEADD(DAY, -180, CURRENT_TIMESTAMP())
               OR WAL_LAST_EDIT_AT IS NULL THEN 1 END)                          AS STALE_180D,
  ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3)            AS TOTAL_SIZE_GB,
  ROUND(
    SUM(
      CASE WHEN IS_ORPHANED = TRUE OR IS_DELETED = TRUE OR API_IS_ARCHIVED = TRUE
           THEN COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) ELSE 0 END
    ) / 1073741824.0, 3
  )                                                                              AS RECLAIMABLE_SIZE_GB
FROM SIGDS_WORKBOOK_MAP
WHERE API_OWNER_ID IS NOT NULL
GROUP BY
  SCAN_SCHEMA,
  TRIM(COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')),
  API_OWNER_ID
ORDER BY SCAN_SCHEMA, RECLAIMABLE_SIZE_GB DESC, ARCHIVED_COUNT DESC;


-- =============================================================================
-- 6) Workbooks with multiple input tables
--    Groups by SOURCE workbook — resolving tagged versions (IS_TAGGED_VERSION=TRUE)
--    back to their PARENT_WORKBOOK_ID so QA/Prod versions are not counted as
--    separate workbooks. A tagged workbook version that is updated repeatedly
--    creates new SIGDS files each time (for the same input element), so raw
--    COUNT(*) is misleading. This query separates:
--      NAMED_INPUT_ELEMENTS — COUNT(DISTINCT WAL_INPUT_TABLE_NAME): unique named
--        elements, stable across version updates. Best proxy for true input count.
--      TOTAL_SIGDS_FILES    — COUNT(*): all SIGDS files including version duplicates.
--        Will exceed NAMED_INPUT_ELEMENTS for workbooks with repeated tag updates.
--      TAGGED_VERSION_FILES — count of files that belong to a tagged version,
--        useful for understanding how much of the footprint is from versioning.
--      SCHEMA_COUNT         — how many writeback schemas this workbook spans
--        (source schema + any Prod/QA tag schemas).
--    HAVING filters to workbooks with more than one named input element.
-- =============================================================================

SELECT
  COALESCE(PARENT_WORKBOOK_ID, WORKBOOK_ID)                                    AS SOURCE_WORKBOOK_ID,
  COALESCE(
    MAX(CASE WHEN IS_TAGGED_VERSION = FALSE THEN WORKBOOK_NAME END),
    MAX(WORKBOOK_NAME)
  )                                                                              AS WORKBOOK_NAME,
  TRIM(
    COALESCE(MAX(API_OWNER_FIRST_NAME), '') || ' ' || COALESCE(MAX(API_OWNER_LAST_NAME), '')
  )                                                                              AS OWNER_FULL_NAME,
  COUNT(DISTINCT WAL_INPUT_TABLE_NAME)                                           AS NAMED_INPUT_ELEMENTS,
  COUNT(*)                                                                       AS TOTAL_SIGDS_FILES,
  COUNT(CASE WHEN IS_TAGGED_VERSION = TRUE THEN 1 END)                          AS TAGGED_VERSION_FILES,
  COUNT(DISTINCT SCAN_SCHEMA)                                                    AS SCHEMA_COUNT,
  SUM(WAL_MAX_EDIT_NUM)                                                         AS TOTAL_EDITS,
  MAX(WAL_LAST_EDIT_AT)                                                         AS LAST_EDIT_AT,
  ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3)            AS TOTAL_SIZE_GB,
  MAX(CASE WHEN API_IS_ARCHIVED = TRUE THEN 'Yes' ELSE 'No' END)                AS WORKBOOK_ARCHIVED
FROM SIGDS_WORKBOOK_MAP
WHERE WORKBOOK_ID IS NOT NULL
GROUP BY COALESCE(PARENT_WORKBOOK_ID, WORKBOOK_ID)
HAVING COUNT(DISTINCT WAL_INPUT_TABLE_NAME) > 1
ORDER BY NAMED_INPUT_ELEMENTS DESC, TOTAL_SIGDS_FILES DESC;


-- =============================================================================
-- 7) Legacy WAL inventory
--    Old SIGDS_WAL_<uuid> tables predate the DS_ID-based naming convention.
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
FROM SIGDS_WORKBOOK_MAP
WHERE IS_LEGACY_WAL = TRUE
ORDER BY
  SCAN_SCHEMA,
  CASE WHEN WAL_LAST_EDIT_AT IS NULL
            OR DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) >= 180
       THEN 1 ELSE 0 END ASC,   -- active first
  WAL_LAST_EDIT_AT DESC NULLS LAST;


-- =============================================================================
-- 8) Read-only consumers — live tables the WAL alone would call stale
--    Tables with recent workbook READS but no recent edits. A write-only view
--    (queries 3 and 4, or the pre-v2 scoring) would class these as abandoned;
--    query history shows they are in active use. Never quarantine these.
--    Requires SIGDS_ACCESS_LOG to be loaded (python script step "access log").
-- =============================================================================

SELECT
  SCAN_SCHEMA,
  SIGDS_TABLE,
  WORKBOOK_NAME,
  OWNER_FULL_NAME,
  DAYS_SINCE_LAST_EDIT,
  WAL_MAX_EDIT_NUM,
  DAYS_SINCE_ACCESS,
  ACCESS_READ_COUNT,
  ACCESS_DISTINCT_USERS,
  ACCESS_LAST_USER_EMAIL,
  ACCESS_LAST_SOURCE_URL,
  TOTAL_SIZE_MB
FROM V_SIGDS_ARCHIVAL_SCORE
WHERE ACCESS_EVIDENCE = 'RECENT'
  AND (DAYS_SINCE_LAST_EDIT IS NULL OR DAYS_SINCE_LAST_EDIT > 90)
ORDER BY ACCESS_READ_COUNT DESC;


-- =============================================================================
-- 9) Tier movement since the previous run
--    Compares each table's latest SIGDS_SCORE_HISTORY snapshot with the one
--    before it. Tables moving toward TIER 1 deserve an owner conversation;
--    tables moving away were revived (or the config changed — check
--    SIGDS_SCORING_CONFIG before reading much into mass movement).
-- =============================================================================

WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY SCAN_SCHEMA, SIGDS_TABLE ORDER BY SCORED_AT DESC) AS RN
  FROM SIGDS_SCORE_HISTORY
)
SELECT
  cur.SCAN_SCHEMA,
  cur.SIGDS_TABLE,
  cur.WORKBOOK_NAME,
  prev.TIER_CODE     AS PREVIOUS_TIER,
  cur.TIER_CODE      AS CURRENT_TIER,
  prev.TOTAL_SCORE   AS PREVIOUS_SCORE,
  cur.TOTAL_SCORE    AS CURRENT_SCORE,
  cur.ACCESS_EVIDENCE,
  prev.SCORED_AT     AS PREVIOUS_RUN_AT,
  cur.SCORED_AT      AS CURRENT_RUN_AT
FROM ranked cur
JOIN ranked prev
  ON  prev.SCAN_SCHEMA = cur.SCAN_SCHEMA
  AND prev.SIGDS_TABLE = cur.SIGDS_TABLE
  AND cur.RN = 1 AND prev.RN = 2
WHERE cur.TIER_CODE <> prev.TIER_CODE
ORDER BY cur.TIER_CODE ASC, cur.TOTAL_SCORE DESC;


-- =============================================================================
-- 10) Quarantine watch — failed Sigma queries since a table was moved
--     During the 30-day quarantine period, any Sigma-generated query that fails
--     with "object does not exist" (error 2003) against a SIGDS name is a sign
--     something still depended on it: rename it back immediately.
--     Replace <WRITEBACK_SCHEMA> with the schema name (matched against query text)
--     and adjust the window to the start of your quarantine period.
--     Indicative, not exhaustive: it depends on the failed statement still carrying
--     Sigma's query comment, and ACCOUNT_USAGE lags by up to ~45 minutes.
-- =============================================================================

SELECT
  START_TIME,
  USER_NAME,
  ERROR_CODE,
  ERROR_MESSAGE,
  LEFT(QUERY_TEXT, 400) AS QUERY_TEXT_PREVIEW
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
  AND ERROR_CODE IN ('2003', '2043')
  AND POSITION('-- Sigma Σ ' IN QUERY_TEXT) > 0
  AND QUERY_TEXT ILIKE '%SIGDS%'
  AND QUERY_TEXT ILIKE '%<WRITEBACK_SCHEMA>%'
ORDER BY START_TIME DESC;
