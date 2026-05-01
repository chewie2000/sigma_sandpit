CREATE OR REPLACE VIEW V_EDGES_DATA AS

WITH latest_deps AS (SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_DATASET_DEPENDENCIES),
     latest_wb   AS (SELECT MAX(RUN_ID) AS RUN_ID FROM SIGMA_WORKBOOK_MIGRATION_SUMMARY)

-- Dataset to Dataset (parent-child dependency chain)
SELECT DISTINCT
    PARENT_ID  AS source,
    DATASET_ID AS target,
    'ds-ds'    AS edge_type
FROM SIGMA_DATASET_DEPENDENCIES
WHERE PARENT_ID IS NOT NULL
  AND RUN_ID = (SELECT RUN_ID FROM latest_deps)

UNION ALL

-- Dataset to Workbook
-- Only shown where the dataset has NOT been migrated to a data model.
-- Once a dataset is migrated the canonical path becomes ds-dm then dm-wb instead.
SELECT DISTINCT
    d.DATASET_ID  AS source,
    w.WORKBOOK_ID AS target,
    'ds-wb'       AS edge_type
FROM SIGMA_WORKBOOK_SOURCE_DETAILS w
JOIN SIGMA_DATASET_DEPENDENCIES d
  ON  w.DATASET_ID = d.DATASET_ID
  AND d.RUN_ID     = (SELECT RUN_ID FROM latest_deps)
WHERE w.DATASET_ID    IS NOT NULL
  AND w.DATA_MODEL_ID IS NULL
  AND w.RUN_ID = (SELECT RUN_ID FROM latest_wb)

UNION ALL

-- Dataset to Data Model (migration edge)
SELECT DISTINCT
    DATASET_ID    AS source,
    DATA_MODEL_ID AS target,
    'ds-dm'       AS edge_type
FROM SIGMA_DATASET_DEPENDENCIES
WHERE DATA_MODEL_ID IS NOT NULL
  AND RUN_ID = (SELECT RUN_ID FROM latest_deps)

UNION ALL

-- Data Model to Workbook
-- SOURCE_DATA_MODEL_ID set: workbook directly sources the data model
-- DATA_MODEL_ID set: workbook dataset was migrated; data model is now the upstream
SELECT DISTINCT
    COALESCE(w.SOURCE_DATA_MODEL_ID, w.DATA_MODEL_ID) AS source,
    w.WORKBOOK_ID                                      AS target,
    'dm-wb'                                            AS edge_type
FROM SIGMA_WORKBOOK_SOURCE_DETAILS w
WHERE (w.SOURCE_DATA_MODEL_ID IS NOT NULL OR w.DATA_MODEL_ID IS NOT NULL)
  AND w.RUN_ID = (SELECT RUN_ID FROM latest_wb);
