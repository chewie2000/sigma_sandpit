CREATE OR REPLACE VIEW V_NODES_DATA AS

WITH latest_deps AS (SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES),
     latest_wb   AS (SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY)

-- Dataset nodes
SELECT DISTINCT
    DATASET_ID                                AS node_id,
    DATASET_NAME                              AS node_name,
    'dataset'                                 AS node_type,
    RELATION_TYPE                             AS node_subtype,
    DATASET_MIGRATION_STATUS                  AS status,
    GREATEST(DOWNSTREAM_CHILD_COUNT + 10, 10) AS symbol_size,
    DATASET_URL                               AS url
FROM SIGMA_DATASET_DEPENDENCIES
WHERE RUN_ID = (SELECT RUN_ID FROM latest_deps)

UNION ALL

-- Workbook nodes
SELECT DISTINCT
    WORKBOOK_ID      AS node_id,
    WORKBOOK_NAME    AS node_name,
    'workbook'       AS node_type,
    MIGRATION_STATUS AS node_subtype,
    MIGRATION_STATUS AS status,
    10               AS symbol_size,
    WORKBOOK_URL     AS url
FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY
WHERE RUN_ID = (SELECT RUN_ID FROM latest_wb)

UNION ALL

-- Data model nodes: migration targets surfaced from dataset dependencies.
-- symbol_size scales with the number of datasets that migrated to each model.
SELECT DISTINCT
    DATA_MODEL_ID                                                      AS node_id,
    DATA_MODEL_NAME                                                    AS node_name,
    'datamodel'                                                        AS node_type,
    'MIGRATED'                                                         AS node_subtype,
    'MIGRATED'                                                         AS status,
    GREATEST(COUNT(*) OVER (PARTITION BY DATA_MODEL_ID) + 10, 15)     AS symbol_size,
    DATA_MODEL_URL                                                     AS url
FROM SIGMA_DATASET_DEPENDENCIES
WHERE RUN_ID  = (SELECT RUN_ID FROM latest_deps)
  AND DATA_MODEL_ID IS NOT NULL;
