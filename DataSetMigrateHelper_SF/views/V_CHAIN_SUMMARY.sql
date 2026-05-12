-- ==============================================================================
-- V_CHAIN_SUMMARY
-- One row per unique ROOT→LEAF chain path.
-- Intended as a dropdown/list source in Sigma to drive graph filtering by chain.
--
-- Columns:
--   ROOT_ID                — DATASET_ID of the chain's root dataset
--   ROOT_NAME              — name of the root dataset
--   CHAIN_PATH             — full path string: "Root → Internal → Leaf"
--   CHAIN_DEPTH            — number of datasets in the chain (1 = standalone root)
--   CHAIN_MIGRATION_STATUS — FULLY MIGRATED / PARTIALLY MIGRATED / NOT MIGRATED
-- ==============================================================================

CREATE OR REPLACE VIEW V_CHAIN_SUMMARY AS

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
        DATASET_MIGRATION_STATUS,
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
    ROOT_NAME,
    NAME_CHAIN,
    STATUS_CHAIN
) AS (
    -- Anchor: ROOT nodes
    SELECT
        DATASET_ID,
        RELATION_TYPE,
        0                                          AS DEPTH,
        DATASET_ID                                 AS ROOT_ID,
        DATASET_NAME                               AS ROOT_NAME,
        ARRAY_CONSTRUCT(DATASET_NAME)              AS NAME_CHAIN,
        ARRAY_CONSTRUCT(DATASET_MIGRATION_STATUS)  AS STATUS_CHAIN
    FROM snap
    WHERE RELATION_TYPE = 'ROOT'

    UNION ALL

    -- Recursive: extend each path one level down
    SELECT
        s.DATASET_ID,
        s.RELATION_TYPE,
        c.DEPTH + 1,
        c.ROOT_ID,
        c.ROOT_NAME,
        ARRAY_APPEND(c.NAME_CHAIN,   s.DATASET_NAME),
        ARRAY_APPEND(c.STATUS_CHAIN, s.DATASET_MIGRATION_STATUS)
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
    ROOT_NAME,
    ARRAY_TO_STRING(NAME_CHAIN, ' → ')    AS chain_path,
    ARRAY_SIZE(NAME_CHAIN)                AS chain_depth,
    CASE
        WHEN NOT ARRAY_CONTAINS('not-migrated'::VARIANT, STATUS_CHAIN)
            THEN 'FULLY MIGRATED'
        WHEN ARRAY_CONTAINS('migrated'::VARIANT, STATUS_CHAIN)
         AND ARRAY_CONTAINS('not-migrated'::VARIANT, STATUS_CHAIN)
            THEN 'PARTIALLY MIGRATED'
        ELSE 'NOT MIGRATED'
    END                                   AS chain_migration_status

FROM paths
ORDER BY chain_path;
