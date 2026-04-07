-- =============================================================================
-- geninfo_queries.sql
--
-- General information queries for SIGDS_WORKBOOK_MAP.
-- Use these to identify cleanup candidates: stale SIGDS tables, orphaned WAL
-- records, legacy WAL tables, and archived workbooks.
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> below with the
-- Unity Catalog catalog and schema where SIGDS_WORKBOOK_MAP resides.
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;

-- =============================================================================
-- 1) Archived workbooks — largest stale SIGDS tables
--    SIGDS table still exists, workbook is archived, no edits in 90+ days.
--    Good candidates to DROP the SIGDS table (and optionally the WAL rows).
-- =============================================================================

SELECT
  SOURCE_SCHEMA,
  SIGDS_TABLE,
  WAL_TABLE_FQN,
  WORKBOOK_ID,
  WORKBOOK_NAME,
  WAL_WORKBOOK_URL,
  SIGDS_TABLE_SIZE_BYTES,
  WAL_LAST_EDIT_AT,
  API_IS_ARCHIVED,
  IS_TAGGED_VERSION,
  VERSION_TAG_NAME
FROM SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED = FALSE
  AND API_IS_ARCHIVED = TRUE
  AND WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 90 DAY
ORDER BY SOURCE_SCHEMA, SIGDS_TABLE_SIZE_BYTES DESC NULLS LAST;

-- =============================================================================
-- 2) Active workbooks — largest stale SIGDS tables (manual review)
--    SIGDS table still exists, workbook is active, no edits in 180+ days.
--    Review with owners before taking action.
-- =============================================================================

SELECT
  SOURCE_SCHEMA,
  SIGDS_TABLE,
  WAL_TABLE_FQN,
  WORKBOOK_ID,
  WORKBOOK_NAME,
  WAL_WORKBOOK_URL,
  SIGDS_TABLE_SIZE_BYTES,
  WAL_LAST_EDIT_AT
FROM SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED = FALSE
  AND API_IS_ARCHIVED = FALSE
  AND WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 180 DAY
ORDER BY SOURCE_SCHEMA, SIGDS_TABLE_SIZE_BYTES DESC NULLS LAST;

-- =============================================================================
-- 3) Orphaned WAL records — SIGDS table dropped, WAL still present
--    Candidates to DROP the WAL table, since the data table no longer exists.
-- =============================================================================

SELECT
  SOURCE_SCHEMA,
  WAL_TABLE_FQN,
  WAL_DS_ID,
  WORKBOOK_ID,
  WORKBOOK_NAME,
  WAL_WORKBOOK_URL,
  WAL_LAST_EDIT_AT,
  IS_DELETED,
  DELETED_AT
FROM SIGDS_WORKBOOK_MAP
WHERE
  IS_ORPHANED = TRUE
  AND IS_DELETED = FALSE
  AND WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 90 DAY
ORDER BY SOURCE_SCHEMA, WAL_LAST_EDIT_AT ASC;

-- =============================================================================
-- 4) Stale legacy WAL tables
--    Old random-UUID-style WAL tables with no edits in 180+ days.
--    Useful for decommissioning legacy naming (sigds_wal_<uuid>).
-- =============================================================================

SELECT
  SOURCE_SCHEMA,
  WAL_TABLE_FQN,
  WAL_DS_ID,
  WORKBOOK_ID,
  WORKBOOK_NAME,
  WAL_LAST_EDIT_AT,
  IS_ORPHANED
FROM SIGDS_WORKBOOK_MAP
WHERE
  IS_LEGACY_WAL = TRUE
  AND WAL_LAST_EDIT_AT < CURRENT_TIMESTAMP() - INTERVAL 180 DAY
ORDER BY SOURCE_SCHEMA, WAL_LAST_EDIT_AT ASC;

-- =============================================================================
-- 5) Status flag summary — overview of cleanup opportunity
--    High-level rollup showing record counts and total size by status.
-- =============================================================================

SELECT
  SOURCE_SCHEMA,
  IS_ORPHANED,
  IS_DELETED,
  IS_LEGACY_WAL,
  API_IS_ARCHIVED,
  COUNT(*)              AS num_entries,
  SUM(SIGDS_TABLE_SIZE_BYTES) AS total_size_bytes
FROM SIGDS_WORKBOOK_MAP
GROUP BY
  SOURCE_SCHEMA,
  IS_ORPHANED,
  IS_DELETED,
  IS_LEGACY_WAL,
  API_IS_ARCHIVED
ORDER BY SOURCE_SCHEMA, num_entries DESC;
