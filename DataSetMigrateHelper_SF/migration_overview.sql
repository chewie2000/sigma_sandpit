-- ==============================================================================
-- SIGMA MIGRATION OVERVIEW
-- High-level migration status combining all three output tables.
--
-- Run sigma_dataset_dependencies() then sigma_workbook_source_map() first.
--
-- Set your session context before running:
--   USE DATABASE <YOUR_DATABASE>;
--   USE SCHEMA   <YOUR_SCHEMA>;
--
-- Queries in this file:
--   1. Dataset migration progress          — org-wide counts by status and graph role
--   2. Workbook migration summary          — workbook counts by migration status
--   3. Terminal datasets (no dependants)   — datasets no dataset or workbook depends on
--   4. Datasets needing immediate action   — migration inconsistencies and blockers
--   5. Workbooks needing re-pointing       — still pointing at migrated datasets
--   6. Migration readiness pipeline        — what is unblocked and ready to migrate now
-- ==============================================================================


-- ==============================================================================
-- QUERY 1: DATASET MIGRATION PROGRESS
-- Org-wide counts and percentages by migration status and graph role.
-- Percentages exclude not-required datasets from the denominator so they
-- reflect meaningful migration progress (not-required is done by definition).
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATASET_DEPENDENCIES
),

datasets AS (
    SELECT DISTINCT
        DATASET_ID,
        DATASET_MIGRATION_STATUS,
        RELATION_TYPE,
        DOWNSTREAM_CHILD_COUNT
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
),

-- Counts excluding not-required (the "migration scope" denominator)
scope AS (
    SELECT COUNT(*) AS IN_SCOPE
    FROM datasets
    WHERE DATASET_MIGRATION_STATUS IN ('migrated', 'not-migrated')
)

SELECT
    -- Overall counts
    COUNT(*)                                                                          AS TOTAL_DATASETS,
    COUNT(CASE WHEN DATASET_MIGRATION_STATUS = 'migrated'     THEN 1 END)            AS MIGRATED,
    COUNT(CASE WHEN DATASET_MIGRATION_STATUS = 'not-migrated' THEN 1 END)            AS NOT_MIGRATED,
    COUNT(CASE WHEN DATASET_MIGRATION_STATUS = 'not-required' THEN 1 END)            AS NOT_REQUIRED,

    -- Progress % within migration scope (excludes not-required)
    (SELECT IN_SCOPE FROM scope)                                                      AS MIGRATION_SCOPE_TOTAL,
    ROUND(
        COUNT(CASE WHEN DATASET_MIGRATION_STATUS = 'migrated' THEN 1 END) * 100.0
        / NULLIF((SELECT IN_SCOPE FROM scope), 0),
    1)                                                                                AS PCT_MIGRATED_OF_SCOPE,

    -- By graph role
    COUNT(CASE WHEN RELATION_TYPE = 'ROOT'     THEN 1 END)                           AS ROOT_COUNT,
    COUNT(CASE WHEN RELATION_TYPE = 'INTERNAL' THEN 1 END)                           AS INTERNAL_COUNT,
    COUNT(CASE WHEN RELATION_TYPE = 'LEAF'     THEN 1 END)                           AS LEAF_COUNT,

    -- Terminal datasets: nothing downstream depends on them
    COUNT(CASE WHEN DOWNSTREAM_CHILD_COUNT = 0 THEN 1 END)                           AS TERMINAL_DATASETS,
    COUNT(CASE WHEN DOWNSTREAM_CHILD_COUNT = 0
               AND DATASET_MIGRATION_STATUS = 'not-migrated' THEN 1 END)             AS TERMINAL_NOT_MIGRATED,
    COUNT(CASE WHEN DOWNSTREAM_CHILD_COUNT = 0
               AND DATASET_MIGRATION_STATUS = 'migrated'     THEN 1 END)             AS TERMINAL_MIGRATED

FROM datasets;


-- ==============================================================================
-- QUERY 2: WORKBOOK MIGRATION SUMMARY
-- Workbook counts by migration status and source breakdown.
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY
)

SELECT
    COUNT(*)                                                                          AS TOTAL_WORKBOOKS,
    COUNT(CASE WHEN MIGRATION_STATUS = 'FULLY MIGRATED'     THEN 1 END)              AS FULLY_MIGRATED,
    COUNT(CASE WHEN MIGRATION_STATUS = 'PARTIALLY MIGRATED' THEN 1 END)              AS PARTIALLY_MIGRATED,
    COUNT(CASE WHEN MIGRATION_STATUS = 'NOT MIGRATED'       THEN 1 END)              AS NOT_MIGRATED,
    ROUND(
        COUNT(CASE WHEN MIGRATION_STATUS = 'FULLY MIGRATED' THEN 1 END) * 100.0
        / NULLIF(COUNT(*), 0),
    1)                                                                                AS PCT_FULLY_MIGRATED,

    -- Source type totals across all workbooks
    SUM(DATASET_SOURCE_COUNT)                                                         AS TOTAL_LEGACY_DATASET_SOURCES,
    SUM(DATA_MODEL_SOURCE_COUNT)                                                      AS TOTAL_DATA_MODEL_SOURCES,
    SUM(TABLE_SOURCE_COUNT)                                                           AS TOTAL_TABLE_SOURCES

FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY
WHERE RUN_ID = (SELECT RUN_ID FROM latest_run);


-- ==============================================================================
-- QUERY 3: TERMINAL DATASETS — NOTHING DEPENDS ON THEM (DATASETS OR WORKBOOKS)
-- Datasets with DOWNSTREAM_CHILD_COUNT = 0 AND no workbook references.
-- Use this to find:
--   a) Easy wins: not-migrated but truly isolated — safe to migrate or retire
--   b) Candidates for retirement: migrated datasets with no remaining dependants
--   c) Orphaned: not-required datasets nothing uses at all
-- ==============================================================================

WITH dep_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES
),
det_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_WORKBOOK_SOURCE_DETAILS
),

datasets AS (
    SELECT DISTINCT
        DATASET_ID,
        DATASET_NAME,
        DATASET_PATH,
        DATASET_URL,
        DATASET_MIGRATION_STATUS,
        RELATION_TYPE,
        DATA_MODEL_NAME,
        DATA_MODEL_URL,
        MIGRATED_AT,
        UPSTREAM_PARENT_COUNT,
        DOWNSTREAM_CHILD_COUNT
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM dep_run)
      AND DOWNSTREAM_CHILD_COUNT = 0       -- no dataset dependants
),

-- Workbooks that reference each dataset
workbook_refs AS (
    SELECT
        SOURCE_INODE_ID                      AS DATASET_ID,
        COUNT(DISTINCT WORKBOOK_ID)          AS WORKBOOK_COUNT
    FROM SIGMA_WORKBOOK_SOURCE_DETAILS
    WHERE RUN_ID      = (SELECT RUN_ID FROM det_run)
      AND SOURCE_TYPE = 'dataset'
    GROUP BY SOURCE_INODE_ID
)

SELECT
    ds.DATASET_ID,
    ds.DATASET_NAME,
    ds.DATASET_PATH,
    ds.DATASET_URL,
    ds.DATASET_MIGRATION_STATUS,
    ds.RELATION_TYPE,
    ds.UPSTREAM_PARENT_COUNT,
    ds.DATA_MODEL_NAME,
    ds.DATA_MODEL_URL,
    ds.MIGRATED_AT,

    -- Classification
    CASE
        WHEN ds.DATASET_MIGRATION_STATUS = 'migrated'
            THEN 'DONE — migrated, no dependants'
        WHEN ds.DATASET_MIGRATION_STATUS = 'not-migrated'
            THEN 'EASY WIN — not migrated, no dependants'
        WHEN ds.DATASET_MIGRATION_STATUS = 'not-required'
            THEN 'ORPHANED — not-required, no dependants'
        ELSE 'REVIEW'
    END                                      AS TERMINAL_STATUS

FROM datasets ds
LEFT JOIN workbook_refs wr ON wr.DATASET_ID = ds.DATASET_ID
WHERE COALESCE(wr.WORKBOOK_COUNT, 0) = 0   -- no workbook dependants either
ORDER BY
    CASE ds.DATASET_MIGRATION_STATUS
        WHEN 'not-migrated' THEN 0
        WHEN 'migrated'     THEN 1
        ELSE 2
    END,
    ds.DATASET_NAME;


-- ==============================================================================
-- QUERY 4: DATASETS NEEDING IMMEDIATE ACTION — MIGRATION INCONSISTENCIES
-- Surfaces two classes of problem:
--
--   (A) CHILD AHEAD OF PARENT — a child dataset (or its data model) is migrated
--       but its direct parent dataset is still not-migrated. The data model is
--       effectively sourcing from a legacy dataset, which defeats the migration.
--
--   (B) BLOCKING CHAINS — not-migrated datasets that have at least one
--       not-migrated downstream dependent. Shows how many datasets each blocks.
-- ==============================================================================

-- (A) Child migrated, parent not

WITH dep_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES
),

snap AS (
    SELECT *
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM dep_run)
),

child_ahead AS (
    SELECT DISTINCT
        s.PARENT_ID                          AS BLOCKER_DATASET_ID,
        s.PARENT_NAME                        AS BLOCKER_DATASET_NAME,
        pu.DATASET_URL                       AS BLOCKER_URL,
        s.DATASET_ID                         AS MIGRATED_CHILD_ID,
        s.DATASET_NAME                       AS MIGRATED_CHILD_NAME,
        s.DATASET_URL                        AS MIGRATED_CHILD_URL,
        s.DATA_MODEL_NAME                    AS CHILD_DATA_MODEL_NAME,
        s.DATA_MODEL_URL                     AS CHILD_DATA_MODEL_URL
    FROM snap s
    JOIN (
        SELECT DISTINCT DATASET_ID, DATASET_URL
        FROM snap
    ) pu ON pu.DATASET_ID = s.PARENT_ID
    WHERE s.PARENT_MIGRATION_STATUS  = 'not-migrated'
      AND s.DATASET_MIGRATION_STATUS = 'migrated'
)

SELECT
    'CHILD AHEAD OF PARENT'              AS ISSUE_TYPE,
    BLOCKER_DATASET_ID,
    BLOCKER_DATASET_NAME,
    BLOCKER_URL                          AS DATASET_URL,
    'Parent is not-migrated but child data model exists: '
        || MIGRATED_CHILD_NAME
        || ' → '
        || COALESCE(CHILD_DATA_MODEL_NAME, 'unknown data model')  AS DETAIL,
    MIGRATED_CHILD_URL                   AS CHILD_DATASET_URL,
    CHILD_DATA_MODEL_URL
FROM child_ahead
ORDER BY BLOCKER_DATASET_NAME, MIGRATED_CHILD_NAME;


-- (B) Blocking chains: not-migrated datasets with downstream not-migrated dependants

WITH dep_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES
),

snap AS (
    SELECT DISTINCT
        DATASET_ID,
        DATASET_NAME,
        DATASET_URL,
        DATASET_MIGRATION_STATUS,
        DOWNSTREAM_CHILD_COUNT,
        RELATION_TYPE
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM dep_run)
),

-- Not-migrated datasets that directly parent at least one not-migrated child
blockers AS (
    SELECT DISTINCT
        s.PARENT_ID                         AS BLOCKER_ID,
        COUNT(DISTINCT s.DATASET_ID)        AS NOT_MIGRATED_DEPENDANTS
    FROM SIGMA_DATASET_DEPENDENCIES s
    WHERE s.RUN_ID = (SELECT RUN_ID FROM dep_run)
      AND s.PARENT_MIGRATION_STATUS  = 'not-migrated'
      AND s.DATASET_MIGRATION_STATUS = 'not-migrated'
    GROUP BY s.PARENT_ID
)

SELECT
    d.DATASET_ID,
    d.DATASET_NAME,
    d.DATASET_URL,
    d.RELATION_TYPE,
    d.DOWNSTREAM_CHILD_COUNT                AS TOTAL_DIRECT_CHILDREN,
    b.NOT_MIGRATED_DEPENDANTS               AS NOT_MIGRATED_DIRECT_CHILDREN,
    'Migrate this dataset to unblock '
        || b.NOT_MIGRATED_DEPENDANTS::STRING
        || ' downstream dataset(s).'        AS RECOMMENDATION
FROM snap d
JOIN blockers b ON b.BLOCKER_ID = d.DATASET_ID
ORDER BY b.NOT_MIGRATED_DEPENDANTS DESC, d.DATASET_NAME;


-- ==============================================================================
-- QUERY 5: WORKBOOKS NEEDING RE-POINTING
-- Workbooks that are sourcing from a legacy dataset that has ALREADY been
-- migrated to a data model. The workbook needs its source updated to point
-- at the data model instead.
-- ==============================================================================

WITH dep_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES
),
det_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_WORKBOOK_SOURCE_DETAILS
),
sum_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY
)

SELECT
    w.WORKBOOK_ID,
    w.WORKBOOK_NAME,
    w.WORKBOOK_URL,
    w.OWNER_NAME,
    w.OWNER_EMAIL,
    w.MIGRATION_STATUS,

    -- The legacy dataset the workbook is still pointing at
    d.SOURCE_INODE_ID                        AS DATASET_ID,
    d.DATASET_NAME,
    d.DATASET_URL,

    -- The data model it should be re-pointed to
    d.DATA_MODEL_ID,
    d.DATA_MODEL_NAME,
    d.DATA_MODEL_URL,
    d.MIGRATED_AT                            AS DATASET_MIGRATED_AT

FROM SIGMA_WORKBOOK_SOURCE_DETAILS d
JOIN SIGMA_WORKBOOK_MIGRATION_SUMMARY w
  ON  w.WORKBOOK_ID = d.WORKBOOK_ID
  AND w.RUN_ID      = (SELECT RUN_ID FROM sum_run)
WHERE d.RUN_ID             = (SELECT RUN_ID FROM det_run)
  AND d.SOURCE_TYPE        = 'dataset'
  AND d.DATASET_MIGRATION_STATUS = 'migrated'    -- dataset IS migrated ...
                                                  -- ... but workbook still points at legacy
ORDER BY w.OWNER_NAME, w.WORKBOOK_NAME, d.DATASET_NAME;


-- ==============================================================================
-- QUERY 6: MIGRATION READINESS PIPELINE
-- For each not-migrated dataset, shows whether it is immediately ready to
-- migrate or is still waiting on upstream parents.
--
-- READY     — all parents are migrated (or it is a ROOT with no parents);
--             can be migrated now with no dependency risk.
-- BLOCKED   — at least one direct parent is still not-migrated.
-- ==============================================================================

WITH dep_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES
),

snap AS (
    SELECT *
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM dep_run)
),

datasets AS (
    SELECT DISTINCT
        DATASET_ID,
        DATASET_NAME,
        DATASET_PATH,
        DATASET_URL,
        DATASET_MIGRATION_STATUS,
        RELATION_TYPE,
        UPSTREAM_PARENT_COUNT,
        DOWNSTREAM_CHILD_COUNT
    FROM snap
    WHERE DATASET_MIGRATION_STATUS = 'not-migrated'
),

-- Count of not-migrated direct parents per dataset
blocked_by AS (
    SELECT
        DATASET_ID,
        COUNT(CASE WHEN PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END) AS PARENTS_BLOCKING,
        COUNT(CASE WHEN PARENT_MIGRATION_STATUS = 'migrated'     THEN 1 END) AS PARENTS_MIGRATED
    FROM snap
    WHERE DATASET_MIGRATION_STATUS = 'not-migrated'
      AND PARENT_ID IS NOT NULL
    GROUP BY DATASET_ID
)

SELECT
    d.DATASET_ID,
    d.DATASET_NAME,
    d.DATASET_PATH,
    d.DATASET_URL,
    d.RELATION_TYPE,
    d.UPSTREAM_PARENT_COUNT,
    d.DOWNSTREAM_CHILD_COUNT,
    COALESCE(b.PARENTS_BLOCKING, 0)         AS PARENTS_BLOCKING,
    COALESCE(b.PARENTS_MIGRATED, 0)         AS PARENTS_MIGRATED,

    CASE
        WHEN COALESCE(b.PARENTS_BLOCKING, 0) = 0
            THEN 'READY'
        ELSE 'BLOCKED by ' || b.PARENTS_BLOCKING::STRING || ' parent(s)'
    END                                     AS READINESS,

    -- Priority signal: READY datasets that block the most downstream work go first
    CASE
        WHEN COALESCE(b.PARENTS_BLOCKING, 0) = 0
         AND d.DOWNSTREAM_CHILD_COUNT > 0
            THEN 'HIGH — migrating this unblocks ' || d.DOWNSTREAM_CHILD_COUNT::STRING || ' dataset(s)'
        WHEN COALESCE(b.PARENTS_BLOCKING, 0) = 0
            THEN 'LOW — ready to migrate, no downstream impact'
        ELSE 'WAITING — resolve ' || b.PARENTS_BLOCKING::STRING || ' upstream blockers first'
    END                                     AS PRIORITY

FROM datasets d
LEFT JOIN blocked_by b ON b.DATASET_ID = d.DATASET_ID
ORDER BY
    COALESCE(b.PARENTS_BLOCKING, 0) ASC,   -- READY rows first
    d.DOWNSTREAM_CHILD_COUNT DESC,          -- highest downstream impact first
    d.DATASET_NAME;
