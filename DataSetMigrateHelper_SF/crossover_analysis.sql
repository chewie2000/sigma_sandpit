-- ==============================================================================
-- SIGMA DATASET CROSSOVER ANALYSIS
--
-- Two queries:
--   1. FORK POINTS  — datasets that feed multiple downstream datasets
--                     (migrating or not migrating these has broad impact)
--
--   2. MERGE POINTS — datasets that depend on multiple upstream parents
--                     (migration may be blocked until all parents are done)
-- ==============================================================================


-- ==============================================================================
-- QUERY 1: FORK POINTS
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATASET_DEPENDENCIES
),

snap AS (
    SELECT *
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
),

-- Deduplicated URL lookup (one row per dataset, avoids join fan-out)
dataset_urls AS (
    SELECT DISTINCT DATASET_ID, DATASET_URL, DATA_MODEL_URL
    FROM snap
),

-- One row per unique fork point dataset
fork_points AS (
    SELECT DISTINCT
        DATASET_ID,
        DATASET_NAME,
        DATASET_PATH,
        DATASET_URL,
        DATASET_MIGRATION_STATUS,
        DATA_MODEL_ID,
        DATA_MODEL_NAME,
        DATA_MODEL_URL,
        DOWNSTREAM_CHILD_COUNT
    FROM snap
    WHERE DOWNSTREAM_CHILD_COUNT > 1
),

-- All direct children of each fork point, with their URLs
fork_children AS (
    SELECT DISTINCT
        s.PARENT_ID                  AS FORK_DATASET_ID,
        s.DATASET_ID                 AS CHILD_ID,
        s.DATASET_NAME               AS CHILD_NAME,
        s.DATASET_MIGRATION_STATUS   AS CHILD_MIGRATION_STATUS,
        u.DATASET_URL                AS CHILD_DATASET_URL,
        u.DATA_MODEL_URL             AS CHILD_DATA_MODEL_URL
    FROM snap s
    JOIN dataset_urls u ON u.DATASET_ID = s.DATASET_ID
    WHERE s.PARENT_ID IN (SELECT DATASET_ID FROM fork_points)
)

SELECT
    -- Fork point identity
    f.DATASET_ID                                        AS FORK_DATASET_ID,
    f.DATASET_NAME                                      AS FORK_DATASET_NAME,
    f.DATASET_PATH                                      AS FORK_DATASET_PATH,
    f.DATASET_URL                                       AS FORK_DATASET_URL,
    f.DATASET_MIGRATION_STATUS                          AS FORK_MIGRATION_STATUS,
    f.DATA_MODEL_ID                                     AS FORK_DATA_MODEL_ID,
    f.DATA_MODEL_NAME                                   AS FORK_DATA_MODEL_NAME,
    f.DATA_MODEL_URL                                    AS FORK_DATA_MODEL_URL,

    -- Child counts
    f.DOWNSTREAM_CHILD_COUNT                            AS TOTAL_CHILDREN,
    COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'migrated'     THEN 1 END)
                                                        AS CHILDREN_MIGRATED,
    COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'not-migrated' THEN 1 END)
                                                        AS CHILDREN_NOT_MIGRATED,
    COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'not-required' THEN 1 END)
                                                        AS CHILDREN_NOT_REQUIRED,

    -- Child detail: name, status, ID, dataset URL, data model URL
    LISTAGG(
        c.CHILD_NAME
            || '  [' || c.CHILD_MIGRATION_STATUS || ']'
            || '  ID: ' || c.CHILD_ID
            || '  Dataset: ' || COALESCE(c.CHILD_DATASET_URL, 'no url')
            || '  DataModel: ' || COALESCE(c.CHILD_DATA_MODEL_URL, 'not migrated'),
        '  ||  '
    ) WITHIN GROUP (ORDER BY c.CHILD_NAME)              AS CHILD_DATASET_DETAILS,

    -- Migration guidance
    CASE
        WHEN f.DATASET_MIGRATION_STATUS = 'not-migrated'
         AND COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'migrated' THEN 1 END) > 0
            THEN 'ACTION REQUIRED — ' ||
                 COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'migrated' THEN 1 END)::STRING ||
                 ' child(ren) already migrated but this source dataset is not. ' ||
                 'Child data models are sitting on an unmigrated source.'
        WHEN f.DATASET_MIGRATION_STATUS = 'not-migrated'
            THEN 'HIGH PRIORITY — migrate this dataset first. ' ||
                 'It blocks ' || f.DOWNSTREAM_CHILD_COUNT::STRING ||
                 ' downstream dataset(s).'
        WHEN f.DATASET_MIGRATION_STATUS = 'migrated'
         AND COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'not-migrated' THEN 1 END) > 0
            THEN 'IN PROGRESS — this fork point is migrated. ' ||
                 COUNT(CASE WHEN c.CHILD_MIGRATION_STATUS = 'not-migrated' THEN 1 END)::STRING ||
                 ' child(ren) still pending.'
        WHEN f.DATASET_MIGRATION_STATUS = 'migrated'
            THEN 'COMPLETE — this fork point and all its children are migrated.'
        ELSE 'REVIEW — migration not required or status unknown.'
    END                                                 AS MIGRATION_GUIDANCE

FROM fork_points f
JOIN fork_children c ON c.FORK_DATASET_ID = f.DATASET_ID
GROUP BY
    f.DATASET_ID, f.DATASET_NAME, f.DATASET_PATH, f.DATASET_URL,
    f.DATASET_MIGRATION_STATUS, f.DATA_MODEL_ID, f.DATA_MODEL_NAME,
    f.DATA_MODEL_URL, f.DOWNSTREAM_CHILD_COUNT
ORDER BY
    CASE f.DATASET_MIGRATION_STATUS WHEN 'not-migrated' THEN 0 ELSE 1 END,
    f.DOWNSTREAM_CHILD_COUNT DESC,
    f.DATASET_NAME;


-- ==============================================================================
-- QUERY 2: MERGE POINTS
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATASET_DEPENDENCIES
),

snap AS (
    SELECT *
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
),

-- Deduplicated URL lookup — avoids fan-out when self-joining snap
dataset_urls AS (
    SELECT DISTINCT DATASET_ID, DATASET_URL, DATA_MODEL_URL
    FROM snap
),

-- One row per unique merge point dataset
merge_points AS (
    SELECT DISTINCT
        DATASET_ID,
        DATASET_NAME,
        DATASET_PATH,
        DATASET_URL,
        DATASET_MIGRATION_STATUS,
        DATA_MODEL_ID,
        DATA_MODEL_NAME,
        DATA_MODEL_URL,
        MIGRATED_AT,
        UPSTREAM_PARENT_COUNT
    FROM snap
    WHERE UPSTREAM_PARENT_COUNT > 1
),

-- All direct parents of each merge point, with their URLs from the lookup
merge_parents AS (
    SELECT DISTINCT
        s.DATASET_ID                 AS MERGE_DATASET_ID,
        s.PARENT_ID,
        s.PARENT_NAME,
        s.PARENT_MIGRATION_STATUS,
        u.DATASET_URL                AS PARENT_DATASET_URL,
        u.DATA_MODEL_URL             AS PARENT_DATA_MODEL_URL
    FROM snap s
    JOIN dataset_urls u ON u.DATASET_ID = s.PARENT_ID
    WHERE s.DATASET_ID IN (SELECT DATASET_ID FROM merge_points)
      AND s.PARENT_ID IS NOT NULL
)

SELECT
    -- Merge point identity
    m.DATASET_ID                                        AS MERGE_DATASET_ID,
    m.DATASET_NAME                                      AS MERGE_DATASET_NAME,
    m.DATASET_PATH                                      AS MERGE_DATASET_PATH,
    m.DATASET_URL                                       AS MERGE_DATASET_URL,
    m.DATASET_MIGRATION_STATUS                          AS MERGE_MIGRATION_STATUS,
    m.DATA_MODEL_ID                                     AS MERGE_DATA_MODEL_ID,
    m.DATA_MODEL_NAME                                   AS MERGE_DATA_MODEL_NAME,
    m.DATA_MODEL_URL                                    AS MERGE_DATA_MODEL_URL,
    m.MIGRATED_AT                                       AS MERGE_MIGRATED_AT,

    -- Parent counts
    m.UPSTREAM_PARENT_COUNT                             AS TOTAL_PARENTS,
    COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'migrated'     THEN 1 END)
                                                        AS PARENTS_MIGRATED,
    COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END)
                                                        AS PARENTS_NOT_MIGRATED,
    COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-required' THEN 1 END)
                                                        AS PARENTS_NOT_REQUIRED,

    -- Parent detail: name, status, ID, dataset URL, data model URL
    LISTAGG(
        p.PARENT_NAME
            || '  [' || p.PARENT_MIGRATION_STATUS || ']'
            || '  ID: ' || p.PARENT_ID
            || '  Dataset: ' || COALESCE(p.PARENT_DATASET_URL, 'no url')
            || '  DataModel: ' || COALESCE(p.PARENT_DATA_MODEL_URL, 'not migrated'),
        '  ||  '
    ) WITHIN GROUP (ORDER BY p.PARENT_NAME)             AS PARENT_DATASET_DETAILS,

    -- Migration readiness
    CASE
        WHEN COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END) > 0
         AND m.DATASET_MIGRATION_STATUS = 'migrated'
            THEN 'ACTION REQUIRED — this dataset is migrated but ' ||
                 COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END)::STRING ||
                 ' parent(s) are not. Data model is sourcing from unmigrated datasets.'
        WHEN COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END) > 0
            THEN 'BLOCKED — ' ||
                 COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END)::STRING ||
                 ' of ' || m.UPSTREAM_PARENT_COUNT::STRING ||
                 ' parent(s) not yet migrated. Migrate parents first.'
        WHEN m.DATASET_MIGRATION_STATUS = 'not-migrated'
            THEN 'READY — all ' || m.UPSTREAM_PARENT_COUNT::STRING ||
                 ' parent(s) are migrated. This dataset can be migrated now.'
        WHEN m.DATASET_MIGRATION_STATUS = 'migrated'
            THEN 'COMPLETE — all parents and this dataset are migrated.'
        ELSE 'REVIEW — migration not required or status unknown.'
    END                                                 AS MIGRATION_READINESS

FROM merge_points m
JOIN merge_parents p ON p.MERGE_DATASET_ID = m.DATASET_ID
GROUP BY
    m.DATASET_ID, m.DATASET_NAME, m.DATASET_PATH, m.DATASET_URL,
    m.DATASET_MIGRATION_STATUS, m.DATA_MODEL_ID, m.DATA_MODEL_NAME,
    m.DATA_MODEL_URL, m.MIGRATED_AT, m.UPSTREAM_PARENT_COUNT
ORDER BY
    CASE
        WHEN m.DATASET_MIGRATION_STATUS = 'migrated'
         AND COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END) > 0 THEN 0
        WHEN COUNT(CASE WHEN p.PARENT_MIGRATION_STATUS = 'not-migrated' THEN 1 END) > 0 THEN 1
        WHEN m.DATASET_MIGRATION_STATUS = 'not-migrated'                               THEN 2
        ELSE 3
    END,
    m.UPSTREAM_PARENT_COUNT DESC,
    m.DATASET_NAME;
