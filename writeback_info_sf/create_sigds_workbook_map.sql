-- =============================================================================
-- create_sigds_workbook_map.sql
--
-- DDL for the SIGDS_WORKBOOK_MAP table in Snowflake.
-- Run this once (then scoring_views.sql and sigds_scan_procedure.sql) before CALL sigds_scan.
--
-- Before running, replace <YOUR_DATABASE> and <YOUR_SCHEMA> (find & replace) with the
-- Snowflake database and schema where the tables should be created. The schema must
-- already exist. Pass the same schema as MAP_SCHEMA when you CALL sigds_scan.
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
  API_OWNER_LAST_NAME  VARCHAR       COMMENT 'Owner last name resolved via GET /v2/members',

  -- ---------------------------------------------------------------------------
  -- Added in v2 (also applied to existing deployments by the ALTERs below)
  -- ---------------------------------------------------------------------------
  WAL_TABLE_SIZE_BYTES   NUMBER(38,0)  COMMENT 'Active size of the WAL table in bytes (INFORMATION_SCHEMA.TABLES). Reclaimed together with the SIGDS table, so it counts toward reclaimable storage.',
  SIGDS_TABLE_TOTAL_BYTES NUMBER(38,0) COMMENT 'Active + Time Travel + Fail-safe bytes from ACCOUNT_USAGE.TABLE_STORAGE_METRICS. NULL when ACCOUNT_USAGE is unreadable or the table is too new to appear (~2h latency). Scoring falls back to SIGDS_TABLE_SIZE_BYTES.',
  API_NOT_FOUND          BOOLEAN       COMMENT 'TRUE when this WORKBOOK_ID was NOT returned by the Sigma API on the latest run (deleted, or not visible to the token). Never set for data models. Re-evaluated every run.',
  API_LAST_SEEN_AT       TIMESTAMP_NTZ COMMENT 'UTC time of the last run in which the Sigma API returned this WORKBOOK_ID.',
  LAST_SCANNED_AT        TIMESTAMP_NTZ COMMENT 'UTC time of the last run that processed this row (every row is refreshed every run).'

);

-- Upgrade path for deployments created before v2 (no-ops on a fresh install).
ALTER TABLE SIGDS_WORKBOOK_MAP ADD COLUMN IF NOT EXISTS WAL_TABLE_SIZE_BYTES    NUMBER(38,0);
ALTER TABLE SIGDS_WORKBOOK_MAP ADD COLUMN IF NOT EXISTS SIGDS_TABLE_TOTAL_BYTES NUMBER(38,0);
ALTER TABLE SIGDS_WORKBOOK_MAP ADD COLUMN IF NOT EXISTS API_NOT_FOUND           BOOLEAN;
ALTER TABLE SIGDS_WORKBOOK_MAP ADD COLUMN IF NOT EXISTS API_LAST_SEEN_AT        TIMESTAMP_NTZ;
ALTER TABLE SIGDS_WORKBOOK_MAP ADD COLUMN IF NOT EXISTS LAST_SCANNED_AT         TIMESTAMP_NTZ;


-- =============================================================================
-- SIGDS_ACCESS_LOG -- ground-truth reads/writes of writeback tables.
--
-- Sigma appends a JSON comment ("-- Sigma Σ {...}") to every warehouse query it
-- generates, carrying the source workbook URL and the acting user's email.
-- The sigds_scan procedure joins SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY to
-- ACCESS_HISTORY, keeps queries carrying that comment that touched an object in
-- a scanned writeback schema, and lands one row per (query, object, access kind).
-- This is what tells a table that is READ all day but never edited apart from one
-- that is genuinely dead -- the WAL alone cannot.
-- Requires Snowflake Enterprise Edition+ and GRANT IMPORTED PRIVILEGES ON
-- DATABASE SNOWFLAKE; without them the loader records a SKIPPED run and the
-- scoring views cap their recommendations (see SIGDS_SCORING_CONFIG).
-- =============================================================================
CREATE TABLE IF NOT EXISTS SIGDS_ACCESS_LOG (
  QUERY_ID          VARCHAR,
  ACCESS_KIND       VARCHAR       COMMENT 'READ (BASE_OBJECTS_ACCESSED of a SELECT) or INSERT (OBJECTS_MODIFIED of an INSERT that added rows)',
  OBJECT_NAME       VARCHAR       COMMENT 'Bare table name, upper-cased and unquoted (match case-insensitively)',
  OBJECT_DATABASE   VARCHAR       COMMENT 'Upper-cased database',
  OBJECT_SCHEMA     VARCHAR       COMMENT 'Upper-cased schema',
  START_TIME        TIMESTAMP_NTZ COMMENT 'Query start time, UTC',
  USER_NAME         VARCHAR       COMMENT 'Snowflake user the query ran as (usually the Sigma connection user)',
  ROLE_NAME         VARCHAR,
  QUERY_TYPE        VARCHAR,
  ROWS_INSERTED     NUMBER(38,0),
  SIGMA_SOURCE_URL  VARCHAR       COMMENT 'Workbook URL from the Sigma query comment',
  SIGMA_KIND        VARCHAR       COMMENT 'Sigma query kind from the comment',
  SIGMA_USER_EMAIL  VARCHAR       COMMENT 'Sigma user who triggered the query',
  LOADED_AT         TIMESTAMP_NTZ
);

-- One row per loader run. Coverage (how far back the log is trustworthy) is
-- derived from this, NOT from MIN(START_TIME) of the log: a table nobody touched
-- leaves no log rows, so the log alone cannot prove how long it has been watched.
CREATE TABLE IF NOT EXISTS SIGDS_ACCESS_LOAD_RUNS (
  RUN_ID        VARCHAR,
  RUN_AT        TIMESTAMP_NTZ,
  WINDOW_START  TIMESTAMP_NTZ,
  WINDOW_END    TIMESTAMP_NTZ,
  STATUS        VARCHAR       COMMENT 'OK or SKIPPED',
  ROWS_LOADED   NUMBER(38,0),
  DETAIL        VARCHAR
);

-- Per-run snapshot of every row's score + tier, so movement over time is visible
-- (see geninfo_queries.sql, query 9).
CREATE TABLE IF NOT EXISTS SIGDS_SCORE_HISTORY (
  RUN_ID          VARCHAR,
  SCORED_AT       TIMESTAMP_NTZ,
  SCAN_SCHEMA     VARCHAR,
  SIGDS_TABLE     VARCHAR,
  WORKBOOK_NAME   VARCHAR,
  TOTAL_SCORE     NUMBER(5,0),
  TIER_CODE       NUMBER(1,0),
  ACCESS_EVIDENCE VARCHAR,
  TOTAL_BYTES     NUMBER(38,0),
  IS_ORPHANED     BOOLEAN,
  IS_DELETED      BOOLEAN,
  API_IS_ARCHIVED BOOLEAN,
  API_NOT_FOUND   BOOLEAN
);
