-- ==============================================================================
-- audit_queries.sql
--
-- Ready-to-run governance and migration-readiness queries over the sigma_org_audit
-- marts. Copy/paste into a Snowflake worksheet (or back a Sigma workbook with
-- them). Set the database/schema holding the views first:
--   USE DATABASE MY_DB; USE SCHEMA MY_SCHEMA;
--
-- Multi-org: every view exposes ORG_ID. As written, these queries aggregate
-- ACROSS ALL orgs present in the raw table. To scope a report to one org, add
--   WHERE ORG_ID = '<org-uuid>'
-- (or add ORG_ID to the GROUP BY / SELECT to compare orgs side by side). Use
-- query 0 to list the orgs available.
-- ==============================================================================

-- 0) Orgs present in this warehouse -------------------------------------------
SELECT ORG_ID, COUNT(*) AS OBJECTS, MAX(SNAPSHOT_TS) AS LATEST_SNAPSHOT
FROM RAW_SIGMA_OBJECTS
GROUP BY ORG_ID
ORDER BY OBJECTS DESC;

-- 1) Org inventory at a glance -------------------------------------------------
SELECT OBJECT_TYPE, COUNT(*) AS OBJECTS,
       COUNT_IF(OWNER_MISSING)  AS OWNER_MISSING
FROM V_INVENTORY
GROUP BY OBJECT_TYPE
ORDER BY OBJECTS DESC;

-- 2) Migration readiness summary (R/A/G) --------------------------------------
SELECT RAG, COUNT(*) AS DATASETS,
       SUM(DOWNSTREAM_WORKBOOK_COUNT) AS TOTAL_DOWNSTREAM_WORKBOOKS
FROM V_MIGRATION_SCORE
GROUP BY RAG
ORDER BY CASE RAG WHEN 'RED' THEN 1 WHEN 'AMBER' THEN 2 ELSE 3 END;

-- 3) Highest-risk datasets to migrate first (RED, most downstream) ------------
SELECT DATASET_ID, NAME, PATH, DOWNSTREAM_WORKBOOK_COUNT, RAG_REASON
FROM V_MIGRATION_SCORE
WHERE RAG = 'RED'
ORDER BY DOWNSTREAM_WORKBOOK_COUNT DESC
LIMIT 50;

-- 4) Ownership cleanup queue (owner no longer an active member) ----------------
--    OWNER_MISSING covers deleted + archived/deactivated owners (archived
--    members are absent from /v2/members).
SELECT OBJECT_TYPE, OBJECT_ID, NAME, PATH, OWNER_EMAIL, OWNER_ACCOUNT_TYPE,
       OWNER_MISSING
FROM V_OWNERSHIP_CLEANUP
ORDER BY OBJECT_TYPE, NAME;

-- 5) Writeback storage reclamation opportunity --------------------------------
--    Only ORPHANED tables (confidently owned by this org and unmapped) count;
--    CROSS_ORG / UNATTRIBUTED tables are excluded from reclaimable totals.
SELECT CONNECTION_NAME, ARCHIVAL_CONFIDENCE,
       COUNT(*)                          AS SIGDS_TABLES,
       SUM(RECLAIMABLE_BYTES) / POW(1024, 3) AS RECLAIMABLE_GB
FROM V_WRITEBACK_GOVERNANCE
WHERE ARCHIVAL_SCORE >= 50
GROUP BY CONNECTION_NAME, ARCHIVAL_CONFIDENCE
ORDER BY RECLAIMABLE_GB DESC;

-- 5a) Writeback attribution breakdown -- how SIGDS tables split by true owner.
--     A large CROSS_ORG / UNATTRIBUTED share means the writeback schema is shared
--     across orgs; only ORPHANED is this org's genuine cleanup queue.
SELECT ATTRIBUTION, COUNT(*) AS SIGDS_TABLES,
       SUM(RECLAIMABLE_BYTES) / POW(1024, 3) AS RECLAIMABLE_GB
FROM V_WRITEBACK_GOVERNANCE
GROUP BY ATTRIBUTION
ORDER BY SIGDS_TABLES DESC;

-- 5b) Writeback schemas shared across multiple Sigma orgs ----------------------
SELECT CONNECTION_NAME, WB_DATABASE, WB_SCHEMA, SIGDS_TABLES,
       DISTINCT_OWNING_ORG_SLUGS, OWNING_ORG_SLUGS
FROM V_WRITEBACK_SHARED_SCHEMAS
WHERE IS_SHARED_ACROSS_ORGS = TRUE
ORDER BY SIGDS_TABLES DESC;

-- 6) Orphaned writeback tables (this org's genuine cleanup queue) -------------
--    ATTRIBUTION = 'ORPHANED': WAL attributes them to this org but no live
--    workbook maps. Excludes other orgs' tables on a shared writeback schema.
SELECT CONNECTION_NAME, WB_DATABASE, WB_SCHEMA, SIGDS_TABLE,
       ROW_COUNT, BYTES, DAYS_SINCE_EDIT, ARCHIVAL_SCORE
FROM V_WRITEBACK_GOVERNANCE
WHERE IS_ORPHANED = TRUE
ORDER BY BYTES DESC NULLS LAST
LIMIT 100;

-- 7) Connections whose writeback location could not be scanned ----------------
--    (inventoried from the API, deep scan deferred -- Phase-2 cross-account)
SELECT CONNECTION_ID, NAME, TYPE, ACCOUNT, WAREHOUSE,
       WAL_DATABASE, WAL_SCHEMA, WB_DATABASE, WB_SCHEMA
FROM STG_CONNECTIONS
WHERE CONNECTION_ID NOT IN (
    SELECT DISTINCT CONNECTION_ID FROM V_WRITEBACK_GOVERNANCE
)
ORDER BY NAME;

-- 7a) Query-history ground-truth: tables the WAL heuristic would flag but real
--     workbook access proves are live (the sigma_query_history_scan enrichment).
--     ATTRIBUTION_SOURCE = 'access_history' => rescued from ORPHANED/UNATTRIBUTED
--     purely by observed reads/writes; 'both' => corroborated. Requires the
--     query-history scan to have run (IMPORTED PRIVILEGES + Enterprise edition).
SELECT WB_DATABASE, WB_SCHEMA, SIGDS_TABLE, ATTRIBUTION, ATTRIBUTION_SOURCE,
       ACCESS_WORKBOOK_ID, ACCESS_DISTINCT_WORKBOOKS, ACCESS_DISTINCT_USERS,
       ACCESS_READ_COUNT, ACCESS_WRITE_COUNT,
       ACCESS_LAST_READ_AT, ACCESS_LAST_WRITE_AT, HAS_RECENT_ACCESS
FROM V_WRITEBACK_GOVERNANCE
WHERE ATTRIBUTION_SOURCE IN ('access_history', 'both')
ORDER BY ACCESS_READ_COUNT + ACCESS_WRITE_COUNT DESC
LIMIT 100;

-- 8) Recent workbook drift (changes captured between snapshots) ----------------
--    Requires SCD2_WORKBOOKS (CALL sigma_scd2_apply('STG_WORKBOOKS',...)).
SELECT WORKBOOK_ID, NAME, PATH, CHANGED_FROM, CHANGED_TO
FROM V_WORKBOOK_DRIFT
LIMIT 100;

-- 8b) Recent data model drift (changes captured between snapshots) -------------
--    Requires SCD2_DATAMODELS (CALL sigma_scd2_apply('STG_DATAMODELS',...)).
SELECT DATA_MODEL_ID, NAME, PATH, CHANGED_FROM, CHANGED_TO
FROM V_DATAMODEL_DRIFT
LIMIT 100;

-- 9) Snapshot coverage -- what was captured, and when --------------------------
SELECT OBJECT_TYPE, COUNT(*) AS ROWS_LANDED,
       COUNT(DISTINCT SNAPSHOT_ID) AS SNAPSHOTS,
       MAX(SNAPSHOT_TS) AS LATEST_SNAPSHOT
FROM RAW_SIGMA_OBJECTS
GROUP BY OBJECT_TYPE
ORDER BY OBJECT_TYPE;
