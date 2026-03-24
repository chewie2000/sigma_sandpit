-- SIGDS_WORKBOOK_MAP target table
-- Run this once in your target catalog/schema before executing
-- the populate_sigds_workbook_map.py Databricks notebook.

CREATE TABLE IF NOT EXISTS customer_success.marko_wb.SIGDS_WORKBOOK_MAP (
  WAL_TABLE        STRING COMMENT 'Fully-qualified WAL table name',
  DS_ID            STRING COMMENT 'Input table ID (DS_ID)',
  SIGDS_TABLE      STRING COMMENT 'SIGDS table name in writeback schema',
  WORKBOOK_ID      STRING COMMENT 'Sigma workbook ID',
  WORKBOOK_URL     STRING COMMENT 'URL to workbook/input table element',
  ORG_SLUG         STRING COMMENT 'Org slug parsed from workbook URL',
  INPUT_TABLE_NAME STRING COMMENT 'Element/input table title',
  LAST_EDIT_AT     TIMESTAMP COMMENT 'Timestamp of last WAL entry for this SIGDS table',
  LAST_EDIT_BY     STRING COMMENT 'User email from WAL / metadata',
  WORKBOOK_NAME    STRING COMMENT 'Workbook Name',
  WORKBOOK_PATH    STRING COMMENT 'Path to Workbook',
  OBJECT_TYPE      STRING COMMENT 'Workbook or Model'
);
