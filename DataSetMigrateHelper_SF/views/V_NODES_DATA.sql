CREATE OR REPLACE VIEW V_NODES_DATA AS

WITH latest_deps AS (SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES),
     latest_wb   AS (SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY)

SELECT DISTINCT
    DATASET_ID                                AS node_id,
    DATASET_NAME                              AS node_name,
    'dataset'                                 AS node_type,
    RELATION_TYPE                             AS node_subtype,
    DATASET_MIGRATION_STATUS                  AS status,
    GREATEST(DOWNSTREAM_CHILD_COUNT + 10, 10) AS symbol_size
FROM SIGMA_DATASET_DEPENDENCIES
WHERE RUN_ID = (SELECT RUN_ID FROM latest_deps)

UNION ALL

SELECT DISTINCT
    WORKBOOK_ID      AS node_id,
    WORKBOOK_NAME    AS node_name,
    'workbook'       AS node_type,
    MIGRATION_STATUS AS node_subtype,
    MIGRATION_STATUS AS status,
    10               AS symbol_size
FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY
WHERE RUN_ID = (SELECT RUN_ID FROM latest_wb);
