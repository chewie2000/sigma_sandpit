-- =============================================================================
-- create_sigds_workbook_map.sql
--
-- DDL for the SIGDS_WORKBOOK_MAP table in Databricks Unity Catalog.
-- Run this once before executing populate_sigds_workbook_map.py.
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> below with the
-- Unity Catalog catalog and schema where the table should be created.
-- These must match the CATALOG and SCHEMA values set in the Python script.
--
-- Logical primary key : SIGDS_TABLE  (one row per Sigma input/writeback table)
-- Merge key           : SIGDS_TABLE  (used in the MERGE statement in the script)
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;

CREATE TABLE IF NOT EXISTS SIGDS_WORKBOOK_MAP (

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
  WAL_LAST_ALTERED TIMESTAMP COMMENT 'lastModified from DESCRIBE DETAIL on the WAL table at the time it was last processed; compared against the current lastModified on each run to skip WAL tables that have not changed, without reading any row data',

  -- ---------------------------------------------------------------------------
  -- Data quality flags
  -- ---------------------------------------------------------------------------
  IS_ORPHANED      BOOLEAN   COMMENT 'TRUE when the SIGDS table referenced by the WAL no longer exists in Databricks (e.g. it was dropped). Physical metadata columns will be NULL for orphaned rows.',
  IS_DELETED       BOOLEAN   COMMENT 'TRUE when the WAL table for this record is no longer present in the schema. Set on the run that first detects the absence; cleared automatically if the WAL table reappears.',
  DELETED_AT       TIMESTAMP COMMENT 'Timestamp of the run that first flagged this record as deleted. NULL when IS_DELETED is FALSE or when the record has been reinstated.',
  IS_LEGACY_WAL    BOOLEAN   COMMENT 'TRUE when the WAL table follows the old random-UUID naming convention (sigds_wal_<uuid>) rather than the current DS_ID-based convention (sigds_wal_ds_<ds_id>). Legacy WAL tables may have multiple SIGDS tables associated with them.',

  -- ---------------------------------------------------------------------------
  -- Version tag metadata
  -- ---------------------------------------------------------------------------
  IS_TAGGED_VERSION    BOOLEAN   COMMENT 'TRUE when the WORKBOOK_ID is a tagged version (e.g. Prod, QA) rather than the source workbook.',
  VERSION_TAG_NAME     STRING    COMMENT 'Name of the version tag (e.g. Prod (SDLC), QA (SDLC)) when IS_TAGGED_VERSION is TRUE.',
  PARENT_WORKBOOK_ID   STRING    COMMENT 'Source workbook ID when IS_TAGGED_VERSION is TRUE; NULL for untagged workbooks.',

  -- ---------------------------------------------------------------------------
  -- Sigma API enrichment  (set once on first-seen WORKBOOK_ID; api_is_archived re-checked every run)
  -- ---------------------------------------------------------------------------
  api_url              STRING  COMMENT 'Workbook/data-model URL from Sigma API (set once on first enrichment)',
  api_owner_id         STRING  COMMENT 'Sigma member UUID of the workbook owner (from Sigma API)',
  api_is_archived      BOOLEAN COMMENT 'Archived state from Sigma API; re-checked on every run. FALSE for data models. IDs absent from the API response are left unchanged.',
  api_owner_first_name STRING  COMMENT 'Owner first name resolved via GET /v2/members',
  api_owner_last_name  STRING  COMMENT 'Owner last name resolved via GET /v2/members'

);
