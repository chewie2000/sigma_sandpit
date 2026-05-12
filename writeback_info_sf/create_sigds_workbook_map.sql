-- =============================================================================
-- create_sigds_workbook_map.sql
--
-- DDL for the SIGDS_WORKBOOK_MAP table in Snowflake.
-- Run this once before executing populate_sigds_workbook_map.py.
--
-- Before running, replace <YOUR_DATABASE> and <YOUR_SCHEMA> below with the
-- Snowflake database and schema where the table should be created.
-- These must match the DATABASE and MAP_SCHEMA values set in the Python script.
--
-- Logical primary key : SIGDS_TABLE + SCAN_SCHEMA  (one row per table per schema)
-- Merge key           : SIGDS_TABLE + SCAN_SCHEMA  (composite key in the MERGE statement)
-- =============================================================================

USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;

CREATE TABLE IF NOT EXISTS SIGDS_WORKBOOK_MAP (

  -- ---------------------------------------------------------------------------
  -- Writeback source identifiers
  -- ---------------------------------------------------------------------------
  WAL_TABLE_FQN        VARCHAR       COMMENT 'Fully-qualified WAL table name (database.schema.SIGDS_WAL_*)',
  WAL_DS_ID            VARCHAR       COMMENT 'Input table dataset ID (WAL_DS_ID) from the WAL record',
  SIGDS_TABLE          VARCHAR       COMMENT 'Bare SIGDS table name within the writeback schema (part of composite PK)',
  SCAN_SCHEMA          VARCHAR       COMMENT 'Schema where the WAL and SIGDS tables reside; part of the composite merge key (SIGDS_TABLE + SCAN_SCHEMA). Allows multiple writeback schemas to share one SIGDS_WORKBOOK_MAP table.',

  -- ---------------------------------------------------------------------------
  -- Sigma workbook / data-model metadata
  -- ---------------------------------------------------------------------------
  WORKBOOK_ID          VARCHAR       COMMENT 'Sigma workbook or data-model ID',
  WAL_WORKBOOK_URL     VARCHAR       COMMENT 'Direct URL to the workbook or input-table element in Sigma',
  ORG_SLUG             VARCHAR       COMMENT 'Sigma org slug parsed from the workbook URL (path segment 4)',
  WAL_INPUT_TABLE_NAME VARCHAR       COMMENT 'Element title of the input / writeback table in Sigma',
  WORKBOOK_NAME        VARCHAR       COMMENT 'Workbook or data-model display name (from Sigma API)',
  WORKBOOK_PATH        VARCHAR       COMMENT 'Folder path of the workbook or data model (from Sigma API)',
  OBJECT_TYPE          VARCHAR       COMMENT 'WORKBOOK or DATA_MODEL',

  -- ---------------------------------------------------------------------------
  -- WAL audit fields
  -- ---------------------------------------------------------------------------
  WAL_LAST_EDIT_AT     TIMESTAMP_NTZ COMMENT 'Timestamp of the latest WAL entry for this SIGDS table',
  WAL_LAST_EDIT_BY     VARCHAR       COMMENT 'Email of the user who made the last edit (from WAL metadata)',
  WAL_MAX_EDIT_NUM     NUMBER(38,0)  COMMENT 'Highest EDIT_NUM seen in the WAL for this SIGDS table',

  -- ---------------------------------------------------------------------------
  -- SIGDS Snowflake table physical metadata  (populated via INFORMATION_SCHEMA.TABLES)
  -- ---------------------------------------------------------------------------
  SIGDS_TABLE_ID             VARCHAR       COMMENT 'Snowflake internal table ID from INFORMATION_SCHEMA.TABLES',
  SIGDS_TABLE_CREATED_AT     TIMESTAMP_NTZ COMMENT 'Timestamp when the SIGDS table was first created',
  SIGDS_TABLE_LAST_MODIFIED  TIMESTAMP_NTZ COMMENT 'Timestamp of the most recent alteration to the SIGDS table (LAST_ALTERED from INFORMATION_SCHEMA.TABLES)',
  SIGDS_TABLE_SIZE_BYTES     NUMBER(38,0)  COMMENT 'Current active size of the SIGDS table in bytes (BYTES from INFORMATION_SCHEMA.TABLES)',

  -- ---------------------------------------------------------------------------
  -- Incremental processing watermark
  -- ---------------------------------------------------------------------------
  WAL_TABLE_LAST_MODIFIED TIMESTAMP_NTZ COMMENT 'LAST_ALTERED from INFORMATION_SCHEMA.TABLES on the WAL table at the time it was last processed. Note: in Snowflake, LAST_ALTERED reflects DDL changes; all WAL tables are processed on every run and this field is stored for reference.',

  -- ---------------------------------------------------------------------------
  -- Data quality flags
  -- ---------------------------------------------------------------------------
  IS_ORPHANED      BOOLEAN       COMMENT 'TRUE when the SIGDS table referenced by the WAL no longer exists in Snowflake (e.g. it was dropped). Metadata columns will be NULL for orphaned rows.',
  IS_DELETED       BOOLEAN       COMMENT 'TRUE when the WAL table for this record is no longer present in the schema. Set on the run that first detects the absence; cleared automatically if the WAL table reappears.',
  DELETED_AT       TIMESTAMP_NTZ COMMENT 'Timestamp of the run that first flagged this record as deleted. NULL when IS_DELETED is FALSE or when the record has been reinstated.',
  IS_LEGACY_WAL    BOOLEAN       COMMENT 'TRUE when the WAL table follows the old random-UUID naming convention (SIGDS_WAL_<uuid>) rather than the current DS_ID-based convention (SIGDS_WAL_DS_<ds_id>). Legacy WAL tables may have multiple SIGDS tables associated with them.',

  -- ---------------------------------------------------------------------------
  -- Version tag metadata
  -- ---------------------------------------------------------------------------
  IS_TAGGED_VERSION    BOOLEAN       COMMENT 'TRUE when the WORKBOOK_ID is a tagged version (e.g. Prod, QA) rather than the source workbook.',
  VERSION_TAG_NAME     VARCHAR       COMMENT 'Name of the version tag (e.g. Prod (SDLC), QA (SDLC)) when IS_TAGGED_VERSION is TRUE.',
  PARENT_WORKBOOK_ID   VARCHAR       COMMENT 'Source workbook ID when IS_TAGGED_VERSION is TRUE; NULL for untagged workbooks.',

  -- ---------------------------------------------------------------------------
  -- Sigma API enrichment  (set once on first-seen WORKBOOK_ID; API_IS_ARCHIVED re-checked every run)
  -- ---------------------------------------------------------------------------
  API_WORKBOOK_URL     VARCHAR       COMMENT 'Workbook/data-model URL from Sigma API (set once on first enrichment)',
  API_OWNER_ID         VARCHAR       COMMENT 'Sigma member UUID of the workbook owner (from Sigma API)',
  API_IS_ARCHIVED      BOOLEAN       COMMENT 'Archived state from Sigma API; re-checked on every run. FALSE for data models. IDs absent from the API response are left unchanged.',
  API_OWNER_FIRST_NAME VARCHAR       COMMENT 'Owner first name resolved via GET /v2/members',
  API_OWNER_LAST_NAME  VARCHAR       COMMENT 'Owner last name resolved via GET /v2/members'

);
