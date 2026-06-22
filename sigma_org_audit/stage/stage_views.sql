-- ==============================================================================
-- stage_views.sql
--
-- Stage layer for sigma_org_audit. Views only -- ZERO business logic, fully
-- rebuildable, latest-state per object. Each view flattens the relevant
-- OBJECT_TYPE out of RAW_SIGMA_OBJECTS, keeping only the most recent snapshot
-- per object.
--
-- Multi-org: every row in RAW_SIGMA_OBJECTS carries an ORG_ID (stamped by
-- sigma_org_extract from GET /v2/whoami). Latest-state is resolved per
-- (ORG_ID, OBJECT_ID) so several orgs can share one raw table without shadowing
-- each other, and every STG_* view exposes ORG_ID for scoping/grouping.
--
-- Run after at least one sigma_org_extract (and sigma_writeback_scan for the
-- writeback views). Set the database/schema holding RAW_SIGMA_OBJECTS first:
--   USE DATABASE MY_DB; USE SCHEMA MY_SCHEMA;
-- All views are created in the current schema alongside RAW_SIGMA_OBJECTS.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- STG_WORKBOOKS
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_WORKBOOKS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'workbook'
)
SELECT
    ORG_ID,
    OBJECT_ID                            AS WORKBOOK_ID,
    PAYLOAD:name::STRING                 AS NAME,
    PAYLOAD:path::STRING                 AS PATH,
    PAYLOAD:url::STRING                  AS URL,
    PAYLOAD:ownerId::STRING              AS OWNER_ID,
    PAYLOAD:createdBy::STRING            AS CREATED_BY,
    PAYLOAD:createdAt::TIMESTAMP_NTZ     AS CREATED_AT,
    PAYLOAD:updatedAt::TIMESTAMP_NTZ     AS UPDATED_AT,
    PAYLOAD:isArchived::BOOLEAN          AS IS_ARCHIVED,
    PAYLOAD:latestVersion::NUMBER        AS LATEST_VERSION,
    SNAPSHOT_TS, SNAPSHOT_ID, PAYLOAD
FROM latest WHERE rn = 1;

-- ------------------------------------------------------------------------------
-- STG_DATAMODELS  (list merged with detail payload where available)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_DATAMODELS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'datamodel'
),
detail AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'datamodel_detail'
)
SELECT
    l.ORG_ID,
    l.OBJECT_ID                          AS DATA_MODEL_ID,
    l.PAYLOAD:name::STRING               AS NAME,
    l.PAYLOAD:path::STRING               AS PATH,
    l.PAYLOAD:url::STRING                AS URL,
    l.PAYLOAD:ownerId::STRING            AS OWNER_ID,
    l.PAYLOAD:createdAt::TIMESTAMP_NTZ   AS CREATED_AT,
    l.PAYLOAD:updatedAt::TIMESTAMP_NTZ   AS UPDATED_AT,
    d.PAYLOAD                            AS DETAIL_PAYLOAD,
    l.SNAPSHOT_TS, l.SNAPSHOT_ID, l.PAYLOAD
FROM latest l
LEFT JOIN detail d ON d.ORG_ID = l.ORG_ID AND d.OBJECT_ID = l.OBJECT_ID AND d.rn = 1
WHERE l.rn = 1;

-- ------------------------------------------------------------------------------
-- STG_DATASETS  (exposes migrationStatus for migration scoring)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_DATASETS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'dataset'
)
SELECT
    ORG_ID,
    OBJECT_ID                            AS DATASET_ID,
    PAYLOAD:name::STRING                 AS NAME,
    PAYLOAD:path::STRING                 AS PATH,
    PAYLOAD:url::STRING                  AS URL,
    PAYLOAD:ownerId::STRING              AS OWNER_ID,
    PAYLOAD:migrationStatus::STRING      AS MIGRATION_STATUS,
    PAYLOAD:createdAt::TIMESTAMP_NTZ     AS CREATED_AT,
    PAYLOAD:updatedAt::TIMESTAMP_NTZ     AS UPDATED_AT,
    SNAPSHOT_TS, SNAPSHOT_ID, PAYLOAD
FROM latest WHERE rn = 1;

-- ------------------------------------------------------------------------------
-- STG_CONNECTIONS  (one row per connection; writeback/WAL locations flattened)
-- The detail payload is the source of truth for writeback fields.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_CONNECTIONS AS
WITH lst AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'connection'
),
det AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'connection_detail'
)
SELECT
    l.ORG_ID,
    l.OBJECT_ID                                       AS CONNECTION_ID,
    l.PAYLOAD:name::STRING                            AS NAME,
    COALESCE(d.PAYLOAD:type::STRING, l.PAYLOAD:type::STRING) AS TYPE,
    d.PAYLOAD:account::STRING                         AS ACCOUNT,
    d.PAYLOAD:warehouse::STRING                       AS WAREHOUSE,
    -- input-table WAL schema (single object)
    d.PAYLOAD:inputTableAuditLogSchema:database::STRING AS WAL_DATABASE,
    d.PAYLOAD:inputTableAuditLogSchema:schema::STRING   AS WAL_SCHEMA,
    -- primary write-back output location (first of writebackSchemas, else writebacks)
    COALESCE(d.PAYLOAD:writebackSchemas[0]:database::STRING,
             d.PAYLOAD:writebacks[0]:database::STRING) AS WB_DATABASE,
    COALESCE(d.PAYLOAD:writebackSchemas[0]:schema::STRING,
             d.PAYLOAD:writebacks[0]:schema::STRING)   AS WB_SCHEMA,
    ARRAY_SIZE(COALESCE(d.PAYLOAD:writebackSchemas, d.PAYLOAD:writebacks, ARRAY_CONSTRUCT())) AS WB_LOCATION_COUNT,
    (d.OBJECT_ID IS NOT NULL)                         AS HAS_DETAIL,
    l.SNAPSHOT_TS, l.SNAPSHOT_ID, d.PAYLOAD AS DETAIL_PAYLOAD
FROM lst l
LEFT JOIN det d ON d.ORG_ID = l.ORG_ID AND d.OBJECT_ID = l.OBJECT_ID AND d.rn = 1
WHERE l.rn = 1;

-- ------------------------------------------------------------------------------
-- STG_CONNECTION_WRITEBACKS  (one row per connection per writeback location)
-- Flattened from writebackSchemas[] + writebacks[].
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_CONNECTION_WRITEBACKS AS
WITH det AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'connection_detail'
)
SELECT
    d.ORG_ID,
    d.OBJECT_ID                          AS CONNECTION_ID,
    wb.value:database::STRING            AS WB_DATABASE,
    wb.value:schema::STRING              AS WB_SCHEMA,
    wb.value:writebackSchemaId::STRING   AS WRITEBACK_SCHEMA_ID,
    d.SNAPSHOT_TS, d.SNAPSHOT_ID
FROM det d,
     LATERAL FLATTEN(input => COALESCE(d.PAYLOAD:writebackSchemas,
                                       d.PAYLOAD:writebacks,
                                       ARRAY_CONSTRUCT())) wb
WHERE d.rn = 1;

-- ------------------------------------------------------------------------------
-- STG_MEMBERS / STG_TEAMS  (display-name resolution sources)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_MEMBERS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'member'
)
SELECT
    ORG_ID,
    OBJECT_ID                            AS MEMBER_ID,
    PAYLOAD:email::STRING                AS EMAIL,
    COALESCE(PAYLOAD:firstName::STRING || ' ' || PAYLOAD:lastName::STRING,
             PAYLOAD:memberName::STRING) AS DISPLAY_NAME,
    PAYLOAD:memberType::STRING           AS MEMBER_TYPE,
    PAYLOAD:accountType::STRING          AS ACCOUNT_TYPE,
    PAYLOAD:isArchived::BOOLEAN          AS IS_ARCHIVED,
    SNAPSHOT_TS, SNAPSHOT_ID
FROM latest WHERE rn = 1;

CREATE OR REPLACE VIEW STG_TEAMS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'team'
)
SELECT
    ORG_ID,
    OBJECT_ID                            AS TEAM_ID,
    PAYLOAD:name::STRING                 AS NAME,
    PAYLOAD:description::STRING          AS DESCRIPTION,
    SNAPSHOT_TS, SNAPSHOT_ID
FROM latest WHERE rn = 1;

-- ------------------------------------------------------------------------------
-- STG_GRANTS  (one row per artifact -> grantee)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_GRANTS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'grant'
)
SELECT
    l.ORG_ID,
    l.OBJECT_ID                          AS INODE_ID,
    l.PAYLOAD:artifactType::STRING       AS ARTIFACT_TYPE,
    g.value:granteeId::STRING            AS GRANTEE_ID,
    g.value:granteeType::STRING          AS GRANTEE_TYPE,
    g.value:permission::STRING           AS PERMISSION,
    l.SNAPSHOT_TS, l.SNAPSHOT_ID
FROM latest l,
     LATERAL FLATTEN(input => COALESCE(l.PAYLOAD:grants, ARRAY_CONSTRUCT())) g
WHERE l.rn = 1;

-- ------------------------------------------------------------------------------
-- STG_WRITEBACK_TABLES
-- Latest writeback_table + writeback_wal per SIGDS table, joined to its
-- connection, owning workbook, and last editor. Replaces the in-Python
-- SIGDS_WORKBOOK_MAP merge from writeback_info_sf. ORG_ID flows from the
-- writeback_table row; all joins are org-scoped.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_WRITEBACK_TABLES AS
WITH wt AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'writeback_table'
),
wal AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'writeback_wal'
),
wal_latest AS (
    SELECT
        ORG_ID                           AS ORG_ID,
        PAYLOAD:connectionId::STRING     AS CONNECTION_ID,
        PAYLOAD:sigds_table::STRING      AS SIGDS_TABLE,
        PAYLOAD:walTable::STRING         AS WAL_TABLE,
        PAYLOAD:workbook_id::STRING      AS WORKBOOK_ID,
        PAYLOAD:wal_input_table_name::STRING AS INPUT_TABLE_NAME,
        PAYLOAD:wal_workbook_url::STRING AS WORKBOOK_URL,
        PAYLOAD:wal_max_edit_num::NUMBER AS WAL_MAX_EDIT_NUM,
        PAYLOAD:wal_edit_count::NUMBER   AS WAL_EDIT_COUNT,
        PAYLOAD:wal_last_edit_at::TIMESTAMP_NTZ AS WAL_LAST_EDIT_AT,
        PAYLOAD:wal_last_edit_by::STRING AS WAL_LAST_EDIT_BY
    FROM wal WHERE rn = 1
)
SELECT
    t.ORG_ID,
    t.PAYLOAD:connectionId::STRING       AS CONNECTION_ID,
    c.NAME                               AS CONNECTION_NAME,
    c.TYPE                               AS CONNECTION_TYPE,
    t.PAYLOAD:database::STRING           AS WB_DATABASE,
    t.PAYLOAD:schema::STRING             AS WB_SCHEMA,
    t.PAYLOAD:table_name::STRING         AS SIGDS_TABLE,
    t.PAYLOAD:row_count::NUMBER          AS ROW_COUNT,
    t.PAYLOAD:bytes::NUMBER              AS BYTES,
    t.PAYLOAD:created::TIMESTAMP_NTZ     AS CREATED_AT,
    t.PAYLOAD:last_altered::TIMESTAMP_NTZ AS LAST_ALTERED,
    t.PAYLOAD:scanReachable::BOOLEAN     AS SCAN_REACHABLE,
    w.WORKBOOK_ID,
    wb.ORG_ID                            AS OWNING_ORG_ID,
    -- org slug parsed from the WAL workbook URL (reveals true owner even when the
    -- owning org was not extracted, e.g. a writeback schema shared across orgs).
    SPLIT_PART(SPLIT_PART(w.WORKBOOK_URL, 'sigmacomputing.com/', 2), '/', 1) AS WAL_ORG_SLUG,
    wb.NAME                              AS WORKBOOK_NAME,
    (wb.WORKBOOK_ID IS NOT NULL)         AS WORKBOOK_EXISTS,
    w.INPUT_TABLE_NAME,
    w.WORKBOOK_URL,
    w.WAL_MAX_EDIT_NUM,
    w.WAL_EDIT_COUNT,
    w.WAL_LAST_EDIT_AT,
    w.WAL_LAST_EDIT_BY,
    m.DISPLAY_NAME                       AS LAST_EDIT_BY_NAME,
    t.SNAPSHOT_TS, t.SNAPSHOT_ID
FROM wt t
LEFT JOIN wal_latest w
       ON w.ORG_ID = t.ORG_ID
      AND w.CONNECTION_ID = t.PAYLOAD:connectionId::STRING
      AND w.SIGDS_TABLE  = t.PAYLOAD:table_name::STRING
LEFT JOIN STG_CONNECTIONS c ON c.ORG_ID = t.ORG_ID AND c.CONNECTION_ID = t.PAYLOAD:connectionId::STRING
-- workbook match is org-agnostic (workbook IDs are globally unique): finds the
-- TRUE owning workbook/org even when the table was discovered via another org's
-- connection on a shared writeback schema. Editor is resolved within the owning org.
LEFT JOIN STG_WORKBOOKS  wb ON wb.WORKBOOK_ID  = w.WORKBOOK_ID
LEFT JOIN STG_MEMBERS    m  ON m.ORG_ID = wb.ORG_ID AND LOWER(m.EMAIL) = LOWER(w.WAL_LAST_EDIT_BY)
WHERE t.rn = 1;

-- ------------------------------------------------------------------------------
-- TENANCY + DEPLOYMENT TOPOLOGY (multi-tenant migration)
-- STG_ORGANIZATION carries the per-org role summary stamped by sigma_org_extract
-- (role, tenant/policy counts, and any tenants-access error such as a 403 when
-- the org cannot enumerate tenants). Tenant/policy payload shapes are passed
-- through raw until a populated org is available to model their fields.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_ORGANIZATION AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'organization'
)
SELECT
    ORG_ID,
    PAYLOAD:role::STRING                    AS ORG_ROLE,
    PAYLOAD:tenantCount::NUMBER             AS TENANT_COUNT,
    PAYLOAD:tenantsAccessError::STRING      AS TENANTS_ACCESS_ERROR,
    PAYLOAD:deploymentPolicyCount::NUMBER   AS DEPLOYMENT_POLICY_COUNT,
    PAYLOAD:sourceSwapPolicyCount::NUMBER   AS SOURCE_SWAP_POLICY_COUNT,
    SNAPSHOT_TS, SNAPSHOT_ID
FROM latest WHERE rn = 1;

CREATE OR REPLACE VIEW STG_TENANTS AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'tenant'
)
SELECT
    ORG_ID                                  AS PARENT_ORG_ID,
    OBJECT_ID                               AS TENANT_ORG_ID,
    COALESCE(PAYLOAD:name::STRING, PAYLOAD:organizationName::STRING) AS NAME,
    SNAPSHOT_TS, SNAPSHOT_ID, PAYLOAD
FROM latest WHERE rn = 1;

CREATE OR REPLACE VIEW STG_DEPLOYMENT_POLICIES AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'deployment_policy'
)
SELECT
    ORG_ID,
    OBJECT_ID                               AS DEPLOYMENT_POLICY_ID,
    PAYLOAD:name::STRING                    AS NAME,
    SNAPSHOT_TS, SNAPSHOT_ID, PAYLOAD
FROM latest WHERE rn = 1;

CREATE OR REPLACE VIEW STG_SOURCE_SWAP_POLICIES AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'source_swap_policy'
)
SELECT
    ORG_ID,
    OBJECT_ID                               AS SOURCE_SWAP_POLICY_ID,
    PAYLOAD:name::STRING                    AS NAME,
    SNAPSHOT_TS, SNAPSHOT_ID, PAYLOAD
FROM latest WHERE rn = 1;
