-- =============================================================================
-- create_sigds_workbook_map.sql
--
-- DDL for the SIGDS_WORKBOOK_MAP table in Databricks Unity Catalog.
-- Run this once before executing populate_sigds_workbook_map.py.
--
-- Logical primary key : SIGDS_TABLE  (one row per Sigma input/writeback table)
-- Merge key           : SIGDS_TABLE  (used in the MERGE statement in the script)
-- =============================================================================

CREATE TABLE IF NOT EXISTS customer_success.marko_wb.SIGDS_WORKBOOK_MAP (

  -- ---------------------------------------------------------------------------
  -- Writeback source identifiers
  -- ---------------------------------------------------------------------------
  WAL_TABLE        STRING    COMMENT 'Fully-qualified WAL table name (catalog.schema.sigds_wal_*)',
  DS_ID            STRING    COMMENT 'Input table dataset ID (DS_ID) from the WAL record',
  SIGDS_TABLE      STRING    COMMENT 'Bare SIGDS table name within the writeback schema (logical PK)',

  -- ---------------------------------------------------------------------------
  -- Sigma workbook / data-model metadata
  -- ---------------------------------------------------------------------------
  WORKBOOK_ID      STRING    COMMENT 'Sigma workbook or data-model ID',
  WORKBOOK_URL     STRING    COMMENT 'Direct URL to the workbook or input-table element in Sigma',
  ORG_SLUG         STRING    COMMENT 'Sigma org slug parsed from the workbook URL (path segment 4)',
  INPUT_TABLE_NAME STRING    COMMENT 'Element title of the input / writeback table in Sigma',
  WORKBOOK_NAME    STRING    COMMENT 'Workbook or data-model display name (from Sigma API)',
  WORKBOOK_PATH    STRING    COMMENT 'Folder path of the workbook or data model (from Sigma API)',
  OBJECT_TYPE      STRING    COMMENT 'WORKBOOK or DATA_MODEL',

  -- ---------------------------------------------------------------------------
  -- WAL audit fields
  -- ---------------------------------------------------------------------------
  LAST_EDIT_AT     TIMESTAMP COMMENT 'Timestamp of the latest WAL entry for this SIGDS table',
  LAST_EDIT_BY     STRING    COMMENT 'Email of the user who made the last edit (from WAL metadata)',
  MAX_EDIT_NUM     BIGINT    COMMENT 'Highest EDIT_NUM seen in the WAL for this SIGDS table',

  -- ---------------------------------------------------------------------------
  -- SIGDS Delta table physical metadata  (populated via DESCRIBE DETAIL)
  -- ---------------------------------------------------------------------------
  TABLE_ID             STRING    COMMENT 'Delta table GUID returned by DESCRIBE DETAIL',
  TABLE_LOCATION       STRING    COMMENT 'Cloud storage path of the Delta table',
  TABLE_CREATED_AT     TIMESTAMP COMMENT 'Timestamp when the Delta table was first created',
  TABLE_LAST_MODIFIED  TIMESTAMP COMMENT 'Timestamp of the most recent write to the Delta table',
  TABLE_SIZE_BYTES     BIGINT    COMMENT 'Current on-disk size of the Delta table in bytes',

  -- ---------------------------------------------------------------------------
  -- Incremental processing watermark
  -- ---------------------------------------------------------------------------
  WAL_LAST_ALTERED TIMESTAMP COMMENT 'lastModified from DESCRIBE DETAIL on the WAL table at the time it was last processed; compared against the current lastModified on each run to skip WAL tables that have not changed, without reading any row data'

);
