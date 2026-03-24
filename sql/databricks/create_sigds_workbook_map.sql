-- =============================================================================
-- create_sigds_workbook_map.sql
--
-- DDL for the SIGDS workbook map table.
--
-- Usage
-- -----
-- Drop the existing table and run this script to rebuild from scratch.
-- The Python job (populate_sigds_workbook_map.py) will also CREATE TABLE
-- IF NOT EXISTS on first run, so this file is primarily for explicit
-- resets / schema documentation.
--
-- To reset:
--   DROP TABLE IF EXISTS main.sigds_ops.sigds_workbook_map;
--   <run this script>
--
-- Column notes
-- ------------
-- SIGDS_TABLE          : merge key; fully-qualified Delta table name written
--                        by Sigma Integration Data Store
-- WAL_TABLE            : WAL companion table that triggered this row
-- WAL_LAST_ALTERED     : high-water mark used by the job to skip already-
--                        processed WAL entries
-- api_is_archived      : Option B snapshot — populated once when the
--                        WORKBOOK_ID is first seen; never refreshed unless
--                        the table is rebuilt.  FALSE for data models that
--                        appear in the active /v2/datamodels list.
-- =============================================================================

CREATE TABLE IF NOT EXISTS main.sigds_ops.sigds_workbook_map (

    -- Core mapping columns
    SIGDS_TABLE             STRING    NOT NULL  COMMENT 'Fully-qualified SIGDS table name (merge key)',
    WAL_TABLE               STRING              COMMENT 'WAL table this record was sourced from',
    WAL_LAST_ALTERED        TIMESTAMP           COMMENT 'Watermark: latest ALTERED_AT seen in the WAL for this table',
    WORKBOOK_ID             STRING              COMMENT 'Sigma workbook or data model UUID',

    -- Sigma API workbook/data-model metadata
    WORKBOOK_NAME           STRING              COMMENT 'Display name from Sigma API',
    WORKBOOK_PATH           STRING              COMMENT 'Folder path from Sigma API',
    OBJECT_TYPE             STRING              COMMENT 'WORKBOOK or DATA_MODEL',

    -- Enrichment columns (Option B: populated on first-seen only)
    api_url                 STRING              COMMENT 'Direct browser URL to the workbook/data model',
    api_owner_id            STRING              COMMENT 'Sigma member UUID of the workbook owner',
    api_is_archived         BOOLEAN             COMMENT 'Archived state at time of first discovery; never re-checked',
    api_owner_first_name    STRING              COMMENT 'Owner first name resolved via GET /v2/members',
    api_owner_last_name     STRING              COMMENT 'Owner last name resolved via GET /v2/members'

)
USING DELTA
COMMENT 'Maps SIGDS (Sigma Integration Data Store) tables to their parent Sigma workbooks/data models.';
