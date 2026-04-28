-- ==============================================================================
-- DATA MODEL GRAPH ANALYSIS
-- Analysis queries for the SIGMA_DATA_MODEL_GRAPH table.
--
-- Run sigma_data_model_graph() first to populate the table.
--
-- Set your session context before running:
--   USE DATABASE <YOUR_DATABASE>;
--   USE SCHEMA   <YOUR_SCHEMA>;
--
-- Queries in this file:
--   1. Overview                  — org-wide counts by graph role and crossover type
--   2. Leaf data models          — nothing depends on them; candidates for review
--   3. Dependency chains         — full ROOT → LEAF paths (up to 5 levels deep)
--   4. Fork points               — data models that feed multiple downstream models
--   5. Merge points              — data models that depend on multiple upstream models
-- ==============================================================================


-- ==============================================================================
-- QUERY 1: OVERVIEW
-- Org-wide summary: counts by graph role, crossover metrics, depth stats.
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATA_MODEL_GRAPH
),

data_models AS (
    SELECT DISTINCT
        DATA_MODEL_ID,
        RELATION_TYPE,
        UPSTREAM_PARENT_COUNT,
        DOWNSTREAM_CHILD_COUNT
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
)

SELECT
    COUNT(*)                                                                   AS TOTAL_DATA_MODELS,

    -- By graph role
    COUNT(CASE WHEN RELATION_TYPE = 'ROOT'     THEN 1 END)                    AS ROOT_COUNT,
    COUNT(CASE WHEN RELATION_TYPE = 'INTERNAL' THEN 1 END)                    AS INTERNAL_COUNT,
    COUNT(CASE WHEN RELATION_TYPE = 'LEAF'     THEN 1 END)                    AS LEAF_COUNT,

    -- Crossover types
    COUNT(CASE WHEN DOWNSTREAM_CHILD_COUNT > 1 THEN 1 END)                    AS FORK_POINTS,
    COUNT(CASE WHEN UPSTREAM_PARENT_COUNT  > 1 THEN 1 END)                    AS MERGE_POINTS,

    -- Terminal nodes (nothing depends on them — includes all LEAFs and standalone ROOTs)
    COUNT(CASE WHEN DOWNSTREAM_CHILD_COUNT = 0 THEN 1 END)                    AS TERMINAL_COUNT,

    -- Depth distribution
    MAX(UPSTREAM_PARENT_COUNT)                                                 AS MAX_UPSTREAM_PARENTS,
    MAX(DOWNSTREAM_CHILD_COUNT)                                                AS MAX_DOWNSTREAM_CHILDREN,
    ROUND(AVG(UPSTREAM_PARENT_COUNT), 2)                                       AS AVG_UPSTREAM_PARENTS

FROM data_models;


-- ==============================================================================
-- QUERY 2: LEAF DATA MODELS
-- Data models with no downstream dependants — nothing in the org sources from them.
-- Useful for identifying:
--   • Unused / orphaned data models safe to retire
--   • End-user-facing models at the bottom of the dependency chain
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATA_MODEL_GRAPH
),

data_models AS (
    SELECT DISTINCT
        DATA_MODEL_ID,
        DATA_MODEL_NAME,
        DATA_MODEL_PATH,
        DATA_MODEL_URL,
        DATA_MODEL_CREATED_AT,
        DATA_MODEL_UPDATED_AT,
        RELATION_TYPE,
        UPSTREAM_PARENT_COUNT,
        DOWNSTREAM_CHILD_COUNT
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND DOWNSTREAM_CHILD_COUNT = 0
)

SELECT
    DATA_MODEL_ID,
    DATA_MODEL_NAME,
    DATA_MODEL_PATH,
    DATA_MODEL_URL,
    DATA_MODEL_CREATED_AT,
    DATA_MODEL_UPDATED_AT,
    RELATION_TYPE,
    UPSTREAM_PARENT_COUNT,

    CASE
        WHEN RELATION_TYPE = 'ROOT' THEN 'STANDALONE — no parents or children'
        ELSE                             'LEAF — end of a dependency chain'
    END                                                                        AS CLASSIFICATION

FROM data_models
ORDER BY RELATION_TYPE, DATA_MODEL_NAME;


-- ==============================================================================
-- QUERY 3: DEPENDENCY CHAINS
-- Recursive walk from each ROOT data model down to LEAF, producing one row
-- per unique path. Paths are capped at 5 levels (L0–L4) to guard against
-- cycles; increase MAX_DEPTH in the anchor CTE comment if your org has deeper
-- chains.
--
-- NULL values at L1–L4 indicate the chain terminated at an earlier level.
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATA_MODEL_GRAPH
),

edges AS (
    SELECT DISTINCT
        DATA_MODEL_ID   AS CHILD_ID,
        DATA_MODEL_NAME AS CHILD_NAME,
        DATA_MODEL_PATH AS CHILD_PATH,
        DATA_MODEL_URL  AS CHILD_URL,
        PARENT_DATA_MODEL_ID   AS PARENT_ID,
        PARENT_DATA_MODEL_NAME AS PARENT_NAME
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND PARENT_DATA_MODEL_ID IS NOT NULL
),

roots AS (
    SELECT DISTINCT
        DATA_MODEL_ID   AS ROOT_ID,
        DATA_MODEL_NAME AS ROOT_NAME,
        DATA_MODEL_PATH AS ROOT_PATH,
        DATA_MODEL_URL  AS ROOT_URL
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND RELATION_TYPE = 'ROOT'
),

-- Recursive CTE: accumulate the chain as arrays so we can pivot at the end.
-- DEPTH is tracked to stop at 5 hops (adjust if needed).
chain (
    ROOT_ID, ROOT_NAME, ROOT_PATH, ROOT_URL,
    CURRENT_ID, CURRENT_NAME,
    ID_ARRAY, NAME_ARRAY,
    DEPTH
) AS (
    -- Anchor: start at each ROOT
    SELECT
        r.ROOT_ID,
        r.ROOT_NAME,
        r.ROOT_PATH,
        r.ROOT_URL,
        r.ROOT_ID,
        r.ROOT_NAME,
        ARRAY_CONSTRUCT(r.ROOT_ID),
        ARRAY_CONSTRUCT(r.ROOT_NAME),
        0
    FROM roots r

    UNION ALL

    -- Recursive: step from current node to its children
    SELECT
        c.ROOT_ID,
        c.ROOT_NAME,
        c.ROOT_PATH,
        c.ROOT_URL,
        e.CHILD_ID,
        e.CHILD_NAME,
        ARRAY_APPEND(c.ID_ARRAY,   e.CHILD_ID),
        ARRAY_APPEND(c.NAME_ARRAY, e.CHILD_NAME),
        c.DEPTH + 1
    FROM chain c
    JOIN edges e ON e.PARENT_ID = c.CURRENT_ID
    WHERE c.DEPTH < 4   -- cap at 5 levels total (L0–L4)
),

-- Keep only terminal rows (the chain has reached a node with no further children)
terminal_chains AS (
    SELECT *
    FROM chain
    WHERE CURRENT_ID NOT IN (SELECT PARENT_ID FROM edges)
       OR DEPTH = 4
)

SELECT
    -- L0: ROOT
    ROOT_ID                           AS L0_ID,
    ROOT_NAME                         AS L0_NAME,
    ROOT_PATH                         AS L0_PATH,
    ROOT_URL                          AS L0_URL,

    -- L1–L4: downstream levels (NULL if chain is shorter)
    GET(ID_ARRAY,   1)::STRING        AS L1_ID,
    GET(NAME_ARRAY, 1)::STRING        AS L1_NAME,
    GET(ID_ARRAY,   2)::STRING        AS L2_ID,
    GET(NAME_ARRAY, 2)::STRING        AS L2_NAME,
    GET(ID_ARRAY,   3)::STRING        AS L3_ID,
    GET(NAME_ARRAY, 3)::STRING        AS L3_NAME,
    GET(ID_ARRAY,   4)::STRING        AS L4_ID,
    GET(NAME_ARRAY, 4)::STRING        AS L4_NAME,

    DEPTH + 1                         AS CHAIN_LENGTH

FROM terminal_chains
ORDER BY ROOT_NAME, CHAIN_LENGTH, L1_NAME, L2_NAME, L3_NAME, L4_NAME;


-- ==============================================================================
-- QUERY 4: FORK POINTS
-- Data models with DOWNSTREAM_CHILD_COUNT > 1 — multiple other data models
-- source from them. Changes to a fork point ripple out to all its dependants.
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATA_MODEL_GRAPH
),

data_models AS (
    SELECT DISTINCT
        DATA_MODEL_ID,
        DATA_MODEL_NAME,
        DATA_MODEL_PATH,
        DATA_MODEL_URL,
        RELATION_TYPE,
        DOWNSTREAM_CHILD_COUNT,
        UPSTREAM_PARENT_COUNT
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND DOWNSTREAM_CHILD_COUNT > 1
),

-- Aggregate names of direct children for context
children AS (
    SELECT
        PARENT_DATA_MODEL_ID                         AS FORK_ID,
        COUNT(DISTINCT DATA_MODEL_ID)                AS CHILD_COUNT_CHECK,
        LISTAGG(DISTINCT DATA_MODEL_NAME, ' | ')
            WITHIN GROUP (ORDER BY DATA_MODEL_NAME)  AS CHILD_NAMES
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND PARENT_DATA_MODEL_ID IS NOT NULL
    GROUP BY PARENT_DATA_MODEL_ID
)

SELECT
    dm.DATA_MODEL_ID,
    dm.DATA_MODEL_NAME,
    dm.DATA_MODEL_PATH,
    dm.DATA_MODEL_URL,
    dm.RELATION_TYPE,
    dm.DOWNSTREAM_CHILD_COUNT                         AS DIRECT_DEPENDANTS,
    dm.UPSTREAM_PARENT_COUNT,
    ch.CHILD_NAMES                                    AS DEPENDANT_DATA_MODELS

FROM data_models dm
LEFT JOIN children ch ON ch.FORK_ID = dm.DATA_MODEL_ID
ORDER BY dm.DOWNSTREAM_CHILD_COUNT DESC, dm.DATA_MODEL_NAME;


-- ==============================================================================
-- QUERY 5: MERGE POINTS
-- Data models with UPSTREAM_PARENT_COUNT > 1 — they source from multiple
-- upstream data models. All upstream parents must be healthy for this model
-- to function correctly.
-- ==============================================================================

WITH latest_run AS (
    SELECT MAX(RUN_ID) AS RUN_ID
    FROM SIGMA_DATA_MODEL_GRAPH
),

data_models AS (
    SELECT DISTINCT
        DATA_MODEL_ID,
        DATA_MODEL_NAME,
        DATA_MODEL_PATH,
        DATA_MODEL_URL,
        RELATION_TYPE,
        UPSTREAM_PARENT_COUNT,
        DOWNSTREAM_CHILD_COUNT
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND UPSTREAM_PARENT_COUNT > 1
),

-- Aggregate names of direct parents for context
parents AS (
    SELECT
        DATA_MODEL_ID                                AS MERGE_ID,
        LISTAGG(DISTINCT PARENT_DATA_MODEL_NAME, ' | ')
            WITHIN GROUP (ORDER BY PARENT_DATA_MODEL_NAME) AS PARENT_NAMES
    FROM SIGMA_DATA_MODEL_GRAPH
    WHERE RUN_ID = (SELECT RUN_ID FROM latest_run)
      AND PARENT_DATA_MODEL_ID IS NOT NULL
    GROUP BY DATA_MODEL_ID
)

SELECT
    dm.DATA_MODEL_ID,
    dm.DATA_MODEL_NAME,
    dm.DATA_MODEL_PATH,
    dm.DATA_MODEL_URL,
    dm.RELATION_TYPE,
    dm.UPSTREAM_PARENT_COUNT                          AS DIRECT_PARENTS,
    dm.DOWNSTREAM_CHILD_COUNT,
    p.PARENT_NAMES                                    AS UPSTREAM_DATA_MODELS

FROM data_models dm
LEFT JOIN parents p ON p.MERGE_ID = dm.DATA_MODEL_ID
ORDER BY dm.UPSTREAM_PARENT_COUNT DESC, dm.DATA_MODEL_NAME;
