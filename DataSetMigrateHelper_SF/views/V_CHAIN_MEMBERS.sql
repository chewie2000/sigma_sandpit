-- ==============================================================================
-- V_CHAIN_MEMBERS
-- One row per (chain path, node ID) combination.
-- Used as the join table between a chain picker control (V_CHAIN_SUMMARY) and
-- the graph node source (V_NODES_DATA).
--
-- A node can appear in multiple chains (e.g. an INTERNAL dataset shared by two
-- ROOT chains).  All chains are emitted for each node — this is intentional and
-- supports filtering: selecting a chain shows every node on that path, and a
-- shared node will appear in any selected chain that passes through it.
--
-- Columns:
--   CHAIN_ROOT_ID          — DATASET_ID of the chain's root dataset
--   CHAIN_ROOT_NAME        — display name of the root dataset
--   CHAIN_PATH             — full path string: "Root → Internal → Leaf"
--   CHAIN_MIGRATION_STATUS — FULLY MIGRATED / PARTIALLY MIGRATED / NOT MIGRATED
--   NODE_ID                — DATASET_ID of a node that belongs to this chain
-- ==============================================================================

CREATE OR REPLACE VIEW V_CHAIN_MEMBERS AS

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

-- Recursive traversal: same pattern as V_CHAIN_SUMMARY but also tracks
-- the array of node IDs along each path so they can be unnested below.
chain (
    DATASET_ID,
    RELATION_TYPE,
    DEPTH,
    ROOT_ID,
    ROOT_NAME,
    NAME_CHAIN,
    STATUS_CHAIN,
    ID_CHAIN
) AS (
    -- Anchor: ROOT nodes
    SELECT
        DATASET_ID,
        RELATION_TYPE,
        0                                         AS DEPTH,
        DATASET_ID                                AS ROOT_ID,
        DATASET_NAME                              AS ROOT_NAME,
        ARRAY_CONSTRUCT(DATASET_NAME)             AS NAME_CHAIN,
        ARRAY_CONSTRUCT(DATASET_MIGRATION_STATUS) AS STATUS_CHAIN,
        ARRAY_CONSTRUCT(DATASET_ID)               AS ID_CHAIN
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
        ARRAY_APPEND(c.STATUS_CHAIN, s.DATASET_MIGRATION_STATUS),
        ARRAY_APPEND(c.ID_CHAIN,     s.DATASET_ID)
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
),

chain_rows AS (
    SELECT
        ROOT_ID                              AS CHAIN_ROOT_ID,
        ROOT_NAME                            AS CHAIN_ROOT_NAME,
        ARRAY_TO_STRING(NAME_CHAIN, ' → ')  AS CHAIN_PATH,
        CASE
            WHEN NOT ARRAY_CONTAINS('not-migrated'::VARIANT, STATUS_CHAIN)
                THEN 'FULLY MIGRATED'
            WHEN ARRAY_CONTAINS('migrated'::VARIANT,     STATUS_CHAIN)
             AND ARRAY_CONTAINS('not-migrated'::VARIANT, STATUS_CHAIN)
                THEN 'PARTIALLY MIGRATED'
            ELSE 'NOT MIGRATED'
        END                                  AS CHAIN_MIGRATION_STATUS,
        ID_CHAIN
    FROM paths
)

SELECT
    CHAIN_ROOT_ID,
    CHAIN_ROOT_NAME,
    CHAIN_PATH,
    CHAIN_MIGRATION_STATUS,
    f.VALUE::VARCHAR   AS NODE_ID
FROM chain_rows,
     LATERAL FLATTEN(input => ID_CHAIN) f
ORDER BY CHAIN_PATH, f.INDEX;
