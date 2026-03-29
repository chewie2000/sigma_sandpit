-- ==============================================================================
-- SIGMA DATASET DEPENDENCY CHAINS — PIVOTED VIEW
-- Flattens ROOT → [INTERNAL...] → LEAF chains into one row per unique path.
--
-- Handles chains of any depth using a recursive CTE.
-- Columns are shown for levels L0 (ROOT) through L4 — NULL beyond actual depth.
-- Extend L5+ columns if chains deeper than 5 are expected in your org.
-- ==============================================================================

WITH

latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATASET_DEPENDENCIES
),

snap AS (
    SELECT DISTINCT
        RELATION_TYPE,
        DATASET_ID,
        DATASET_NAME,
        DATASET_PATH,
        DATASET_MIGRATION_STATUS,
        DATA_MODEL_NAME,
        MIGRATED_AT,
        PARENT_ID
    FROM SIGMA_DATASET_DEPENDENCIES
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
),

-- Recursive traversal: walk from ROOT downward, accumulating arrays per path
chain (
    DATASET_ID,
    RELATION_TYPE,
    DEPTH,
    ROOT_ID,
    ID_CHAIN,
    NAME_CHAIN,
    PATH_CHAIN,
    STATUS_CHAIN,
    DM_CHAIN,
    MIGRATED_AT_CHAIN
) AS (
    -- Anchor: ROOT nodes
    SELECT
        DATASET_ID,
        RELATION_TYPE,
        0                                             AS DEPTH,
        DATASET_ID                                    AS ROOT_ID,
        ARRAY_CONSTRUCT(DATASET_ID)                   AS ID_CHAIN,
        ARRAY_CONSTRUCT(DATASET_NAME)                 AS NAME_CHAIN,
        ARRAY_CONSTRUCT(DATASET_PATH)                 AS PATH_CHAIN,
        ARRAY_CONSTRUCT(DATASET_MIGRATION_STATUS)     AS STATUS_CHAIN,
        ARRAY_CONSTRUCT(DATA_MODEL_NAME)              AS DM_CHAIN,
        ARRAY_CONSTRUCT(MIGRATED_AT::VARCHAR)         AS MIGRATED_AT_CHAIN
    FROM snap
    WHERE RELATION_TYPE = 'ROOT'

    UNION ALL

    -- Recursive: add each child onto the parent's accumulated arrays
    SELECT
        s.DATASET_ID,
        s.RELATION_TYPE,
        c.DEPTH + 1,
        c.ROOT_ID,
        ARRAY_APPEND(c.ID_CHAIN,           s.DATASET_ID),
        ARRAY_APPEND(c.NAME_CHAIN,         s.DATASET_NAME),
        ARRAY_APPEND(c.PATH_CHAIN,         s.DATASET_PATH),
        ARRAY_APPEND(c.STATUS_CHAIN,       s.DATASET_MIGRATION_STATUS),
        ARRAY_APPEND(c.DM_CHAIN,           s.DATA_MODEL_NAME),
        ARRAY_APPEND(c.MIGRATED_AT_CHAIN,  s.MIGRATED_AT::VARCHAR)
    FROM snap s
    JOIN chain c ON s.PARENT_ID = c.DATASET_ID
    WHERE c.DEPTH < 9   -- safety cap: max 10 levels
),

-- Keep only complete paths: terminal LEAFs + standalone ROOTs (no children)
paths AS (
    SELECT * FROM chain
    WHERE RELATION_TYPE = 'LEAF'

    UNION ALL

    SELECT c.* FROM chain c
    WHERE c.RELATION_TYPE = 'ROOT'
      AND c.DEPTH = 0
      AND NOT EXISTS (
          SELECT 1 FROM snap s WHERE s.PARENT_ID = c.DATASET_ID
      )
)

SELECT
    ROOT_ID,
    ARRAY_SIZE(ID_CHAIN)                AS CHAIN_DEPTH,

    -- Full path string for readability
    ARRAY_TO_STRING(NAME_CHAIN, ' → ')  AS CHAIN_PATH,

    -- ------------------------------------------------------------------
    -- L0 — ROOT
    -- ------------------------------------------------------------------
    GET(ID_CHAIN,     0)::STRING        AS L0_ID,
    GET(NAME_CHAIN,   0)::STRING        AS L0_NAME,
    GET(PATH_CHAIN,   0)::STRING        AS L0_PATH,
    GET(STATUS_CHAIN, 0)::STRING        AS L0_MIGRATION_STATUS,
    GET(DM_CHAIN,     0)::STRING        AS L0_DATA_MODEL,
    GET(MIGRATED_AT_CHAIN, 0)::STRING   AS L0_MIGRATED_AT,

    -- ------------------------------------------------------------------
    -- L1 — first INTERNAL (or LEAF if chain depth = 2)
    -- ------------------------------------------------------------------
    GET(ID_CHAIN,     1)::STRING        AS L1_ID,
    GET(NAME_CHAIN,   1)::STRING        AS L1_NAME,
    GET(PATH_CHAIN,   1)::STRING        AS L1_PATH,
    GET(STATUS_CHAIN, 1)::STRING        AS L1_MIGRATION_STATUS,
    GET(DM_CHAIN,     1)::STRING        AS L1_DATA_MODEL,
    GET(MIGRATED_AT_CHAIN, 1)::STRING   AS L1_MIGRATED_AT,

    -- ------------------------------------------------------------------
    -- L2
    -- ------------------------------------------------------------------
    GET(ID_CHAIN,     2)::STRING        AS L2_ID,
    GET(NAME_CHAIN,   2)::STRING        AS L2_NAME,
    GET(PATH_CHAIN,   2)::STRING        AS L2_PATH,
    GET(STATUS_CHAIN, 2)::STRING        AS L2_MIGRATION_STATUS,
    GET(DM_CHAIN,     2)::STRING        AS L2_DATA_MODEL,
    GET(MIGRATED_AT_CHAIN, 2)::STRING   AS L2_MIGRATED_AT,

    -- ------------------------------------------------------------------
    -- L3
    -- ------------------------------------------------------------------
    GET(ID_CHAIN,     3)::STRING        AS L3_ID,
    GET(NAME_CHAIN,   3)::STRING        AS L3_NAME,
    GET(PATH_CHAIN,   3)::STRING        AS L3_PATH,
    GET(STATUS_CHAIN, 3)::STRING        AS L3_MIGRATION_STATUS,
    GET(DM_CHAIN,     3)::STRING        AS L3_DATA_MODEL,
    GET(MIGRATED_AT_CHAIN, 3)::STRING   AS L3_MIGRATED_AT,

    -- ------------------------------------------------------------------
    -- L4 — extend further if chains deeper than 5 exist in your org
    -- ------------------------------------------------------------------
    GET(ID_CHAIN,     4)::STRING        AS L4_ID,
    GET(NAME_CHAIN,   4)::STRING        AS L4_NAME,
    GET(PATH_CHAIN,   4)::STRING        AS L4_PATH,
    GET(STATUS_CHAIN, 4)::STRING        AS L4_MIGRATION_STATUS,
    GET(DM_CHAIN,     4)::STRING        AS L4_DATA_MODEL,
    GET(MIGRATED_AT_CHAIN, 4)::STRING   AS L4_MIGRATED_AT,

    -- ------------------------------------------------------------------
    -- Derived: overall chain migration status
    -- ------------------------------------------------------------------
    CASE
        WHEN NOT ARRAY_CONTAINS('not-migrated'::VARIANT, STATUS_CHAIN)
            THEN 'FULLY MIGRATED'
        WHEN ARRAY_CONTAINS('migrated'::VARIANT, STATUS_CHAIN)
         AND ARRAY_CONTAINS('not-migrated'::VARIANT, STATUS_CHAIN)
            THEN 'PARTIALLY MIGRATED'
        ELSE 'NOT MIGRATED'
    END                                 AS CHAIN_MIGRATION_STATUS

FROM paths
ORDER BY CHAIN_PATH;
