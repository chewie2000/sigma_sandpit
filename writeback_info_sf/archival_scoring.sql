-- =============================================================================
-- archival_scoring.sql (Snowflake)
--
-- Read-only queries over the scoring views. The model itself (weights, bands,
-- tiers, the access-history penalty, the coverage safety cap) lives in ONE place:
-- scoring_views.sql, tunable through SIGDS_SCORING_CONFIG. Nothing is duplicated
-- here, so changing a threshold can never leave two copies disagreeing.
--
-- Run order: create_sigds_workbook_map.sql -> scoring_views.sql ->
--            sigds_scan_procedure.sql -> CALL sigds_scan(...) -> this file.
--
-- Before running, replace <YOUR_DATABASE> and <YOUR_SCHEMA>.
--
-- !! IMPORTANT — READ BEFORE ACTING ON THESE SCORES !!
-- The weights and tiers are subjective starting points for investigation, not a
-- directive. Appropriate thresholds differ between customers. Incorrectly removing
-- a SIGDS table or its WAL table can cause IRREPARABLE impact to related Sigma
-- content: workbooks and input tables depending on them break immediately, and a
-- DROP (as opposed to a move) may not be recoverable.
--
-- SAFE DELETION PROCESS — always follow this sequence:
--   1. Move to a quarantine schema (never DROP directly):
--        ALTER TABLE <DB>.<SCHEMA>.<TABLE> RENAME TO <DB>.<SCHEMA>_QUARANTINE.<TABLE>;
--   2. Monitor >= 30 days. geninfo_queries.sql query 10 shows failed queries
--      against quarantined names -- that is the signal something still depended on it.
--   3. DROP from quarantine only after a clean period AND owner sign-off.
-- =============================================================================

USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;


-- 1) Ranked candidates -------------------------------------------------------
--    SCORE_NOTE explains caps and write-only scoring. ACCESS_EVIDENCE says whether
--    query history backs the score.
SELECT
    SIGDS_TABLE, SCAN_SCHEMA, WORKBOOK_NAME, OWNER_FULL_NAME,
    ARCHIVAL_TIER, TOTAL_SCORE, SCORE_NOTE,
    ACCESS_EVIDENCE, DAYS_SINCE_ACCESS, ACCESS_READ_COUNT, ACCESS_DISTINCT_USERS,
    DAYS_SINCE_LAST_EDIT, WAL_MAX_EDIT_NUM, TOTAL_SIZE_MB,
    API_IS_ARCHIVED, API_NOT_FOUND, IS_ORPHANED, IS_DELETED, IS_LEGACY_WAL,
    IS_TAGGED_VERSION, VERSION_TAG_NAME,
    SCORE_STATUS, SCORE_WAL_RECENCY, SCORE_SIGDS_RECENCY, SCORE_EDIT_VOLUME,
    SCORE_LEGACY_WAL, SCORE_STORAGE, PENALTY_TAGGED_VERSION, PENALTY_ACCESS,
    WAL_WORKBOOK_URL
FROM V_SIGDS_ARCHIVAL_SCORE
ORDER BY TOTAL_SCORE DESC, TOTAL_SIZE_MB DESC;


-- 2) Tier roll-up ------------------------------------------------------------
SELECT * FROM V_SIGDS_TIER_SUMMARY ORDER BY TIER_CODE;


-- 3) Is the access evidence trustworthy yet? ---------------------------------
--    STATUS = SKIPPED means ACCESS_HISTORY was unreadable (needs Enterprise
--    Edition + GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE). Until there is an
--    OK run covering MIN_ACCESS_COVERAGE_DAYS, TIER 1 is capped at TIER 2.
SELECT RUN_AT, STATUS, WINDOW_START, WINDOW_END, ROWS_LOADED, DETAIL
FROM SIGDS_ACCESS_LOAD_RUNS
ORDER BY RUN_AT DESC
LIMIT 20;
