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
-- STG_WORKBOOK_LINEAGE  (one row per workbook -> source node; source-binding
-- truth for 9c8.4). Flattened from workbook_lineage.entries. SOURCE_TYPE is one
-- of table | dataset | data-model | customSQL | csv-upload | element (API enum);
-- 'element' rows (workbook-internal formula/chart nodes, no external binding)
-- are excluded here -- they carry no CONNECTION_ID and are noise for deployability.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_WORKBOOK_LINEAGE AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'workbook_lineage'
)
SELECT
    l.ORG_ID,
    l.OBJECT_ID                          AS WORKBOOK_ID,
    e.value:type::STRING                 AS SOURCE_TYPE,
    e.value:connectionId::STRING         AS CONNECTION_ID,
    e.value:name::STRING                 AS SOURCE_NAME,
    e.value:inodeId::STRING              AS SOURCE_INODE_ID,
    e.value:dataModelId::STRING          AS SOURCE_DATA_MODEL_ID,
    l.SNAPSHOT_TS, l.SNAPSHOT_ID
FROM latest l,
     LATERAL FLATTEN(input => COALESCE(l.PAYLOAD:entries, ARRAY_CONSTRUCT())) e
WHERE l.rn = 1
  AND e.value:type::STRING <> 'element';

-- ------------------------------------------------------------------------------
-- STG_DATAMODEL_LINEAGE  (one row per data model -> source node; mirrors
-- STG_WORKBOOK_LINEAGE for data models). Same SOURCE_TYPE enum and exclusion.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_DATAMODEL_LINEAGE AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'datamodel_lineage'
)
SELECT
    l.ORG_ID,
    l.OBJECT_ID                          AS DATA_MODEL_ID,
    e.value:type::STRING                 AS SOURCE_TYPE,
    e.value:connectionId::STRING         AS CONNECTION_ID,
    e.value:name::STRING                 AS SOURCE_NAME,
    e.value:inodeId::STRING              AS SOURCE_INODE_ID,
    e.value:dataModelId::STRING          AS SOURCE_DATA_MODEL_ID,
    l.SNAPSHOT_TS, l.SNAPSHOT_ID
FROM latest l,
     LATERAL FLATTEN(input => COALESCE(l.PAYLOAD:entries, ARRAY_CONSTRUCT())) e
WHERE l.rn = 1
  AND e.value:type::STRING <> 'element';

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
    -- memberType is Sigma's account-type / licence (Build, Build (No AI),
    -- view, analyze, admin, ...). The /v2/members payload has no `accountType`
    -- or `isArchived` key -- earlier mappings read those and always got NULL.
    PAYLOAD:memberType::STRING           AS MEMBER_TYPE,
    PAYLOAD:memberType::STRING           AS ACCOUNT_TYPE,  -- alias: memberType IS the account type
    PAYLOAD:userKind::STRING             AS USER_KIND,     -- internal | embed
    SNAPSHOT_TS, SNAPSHOT_ID
FROM latest WHERE rn = 1;
-- Note: /v2/members returns only active members, so an archived/deactivated
-- owner appears as OWNER_MISSING in V_INVENTORY (absent from STG_MEMBERS) --
-- there is no per-member archived flag to expose here.

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
    -- Each grant object identifies its grantee by memberId OR teamId (grantee
    -- type is implied by which is non-null); there are no granteeId/granteeType
    -- keys -- earlier mappings read those and always got NULL.
    COALESCE(g.value:memberId::STRING, g.value:teamId::STRING) AS GRANTEE_ID,
    IFF(g.value:memberId IS NOT NULL, 'member',
        IFF(g.value:teamId IS NOT NULL, 'team', NULL))         AS GRANTEE_TYPE,
    g.value:permission::STRING           AS PERMISSION,
    g.value:grantId::STRING              AS GRANT_ID,
    g.value:inodeType::STRING            AS INODE_TYPE,
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
-- STG_WRITEBACK_ACCESS
-- Flattened, deduped query-history access rows (OBJECT_TYPE='writeback_access',
-- landed by sigma_query_history_scan). One row per (query, object, access kind).
-- Because raw is append-only and the scan re-scans an overlap window each run,
-- the same (queryId, objectName, accessKind) can land more than once -- collapse
-- latest-wins here. Parses the object into DB/SCHEMA/SIGDS_TABLE (the table part
-- arrives quoted) and the Sigma workbook id + org slug out of the source URL, so
-- it joins to STG_WRITEBACK_TABLES / STG_WORKBOOKS. This is ground-truth lineage
-- that ENHANCES writeback governance; it never downgrades a table on its own.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_WRITEBACK_ACCESS AS
WITH latest AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY PAYLOAD:queryId::STRING,
                            PAYLOAD:objectName::STRING,
                            PAYLOAD:accessKind::STRING
               ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'writeback_access'
)
SELECT
    PAYLOAD:queryId::STRING                       AS QUERY_ID,
    PAYLOAD:startTime::TIMESTAMP_NTZ              AS ACCESS_AT,
    PAYLOAD:accessKind::STRING                    AS ACCESS_KIND,   -- READ | INSERT
    PAYLOAD:queryType::STRING                     AS QUERY_TYPE,
    PAYLOAD:userName::STRING                      AS DB_USER_NAME,
    PAYLOAD:roleName::STRING                      AS DB_ROLE_NAME,
    PAYLOAD:objectName::STRING                    AS OBJECT_NAME,
    UPPER(SPLIT_PART(PAYLOAD:objectName::STRING, '.', 1)) AS WB_DATABASE,
    UPPER(SPLIT_PART(PAYLOAD:objectName::STRING, '.', 2)) AS WB_SCHEMA,
    -- 3rd part arrives double-quoted (mixed-case SIGDS name); strip the quotes.
    REPLACE(SPLIT_PART(PAYLOAD:objectName::STRING, '.', 3), '"', '') AS SIGDS_TABLE,
    PAYLOAD:sourceUrl::STRING                     AS WORKBOOK_URL,
    -- workbook id = the last '-'-delimited token of the /workbook/<title>-<id> path
    -- segment (ids carry no dash); org slug = first path segment after the host.
    REGEXP_SUBSTR(
        SPLIT_PART(SPLIT_PART(PAYLOAD:sourceUrl::STRING, '/workbook/', 2), '?', 1),
        '[^-]+$')                                 AS WORKBOOK_ID,
    SPLIT_PART(SPLIT_PART(PAYLOAD:sourceUrl::STRING, 'sigmacomputing.com/', 2), '/', 1)
                                                  AS WORKBOOK_ORG_SLUG,
    PAYLOAD:sigmaUserEmail::STRING               AS SIGMA_USER_EMAIL,
    PAYLOAD:sigmaKind::STRING                     AS SIGMA_KIND,
    SNAPSHOT_TS, SNAPSHOT_ID
FROM latest WHERE rn = 1;

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
    PAYLOAD:roleSource::STRING              AS ROLE_SOURCE,        -- api | operator
    PAYLOAD:parentOrganizationId::STRING    AS PARENT_ORGANIZATION_ID,
    PAYLOAD:tenantCount::NUMBER             AS TENANT_COUNT,
    PAYLOAD:tenantsListError::STRING        AS TENANTS_LIST_ERROR,
    PAYLOAD:tenantSelfError::STRING         AS TENANT_SELF_ERROR,
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

-- ------------------------------------------------------------------------------
-- DATA ISOLATION -- user attributes (RLS backbone) + their bindings.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW STG_USER_ATTRIBUTES AS
WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'user_attribute'
)
SELECT
    ORG_ID,
    OBJECT_ID                            AS USER_ATTRIBUTE_ID,
    PAYLOAD:name::STRING                 AS NAME,
    PAYLOAD:description::STRING          AS DESCRIPTION,
    PAYLOAD:defaultValue:val::STRING     AS DEFAULT_VALUE,
    PAYLOAD:createdBy::STRING            AS CREATED_BY,
    SNAPSHOT_TS, SNAPSHOT_ID
FROM latest WHERE rn = 1;

-- One row per (attribute -> grantee) binding, flattened from the detail's
-- users/teams/tenants arrays. Grantee names resolved where a source exists.
CREATE OR REPLACE VIEW STG_USER_ATTRIBUTE_BINDINGS AS
WITH det AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ORG_ID, OBJECT_ID ORDER BY SNAPSHOT_TS DESC) AS rn
    FROM RAW_SIGMA_OBJECTS WHERE OBJECT_TYPE = 'user_attribute_detail'
),
flat AS (
    SELECT d.ORG_ID, d.OBJECT_ID AS USER_ATTRIBUTE_ID, d.PAYLOAD:name::STRING AS NAME,
           'user' AS GRANTEE_TYPE, b.value:userId::STRING AS GRANTEE_ID,
           b.value:value:val::STRING AS VALUE, d.SNAPSHOT_TS, d.SNAPSHOT_ID
    FROM det d, LATERAL FLATTEN(input => COALESCE(d.PAYLOAD:users, ARRAY_CONSTRUCT())) b
    WHERE d.rn = 1
    UNION ALL
    SELECT d.ORG_ID, d.OBJECT_ID, d.PAYLOAD:name::STRING,
           'team', b.value:teamId::STRING, b.value:value:val::STRING, d.SNAPSHOT_TS, d.SNAPSHOT_ID
    FROM det d, LATERAL FLATTEN(input => COALESCE(d.PAYLOAD:teams, ARRAY_CONSTRUCT())) b
    WHERE d.rn = 1
    UNION ALL
    SELECT d.ORG_ID, d.OBJECT_ID, d.PAYLOAD:name::STRING,
           'tenant', b.value:tenantOrganizationId::STRING, b.value:value:val::STRING, d.SNAPSHOT_TS, d.SNAPSHOT_ID
    FROM det d, LATERAL FLATTEN(input => COALESCE(d.PAYLOAD:tenants, ARRAY_CONSTRUCT())) b
    WHERE d.rn = 1
)
SELECT
    f.*,
    COALESCE(m.DISPLAY_NAME, t.NAME) AS GRANTEE_NAME
FROM flat f
LEFT JOIN STG_MEMBERS m ON m.ORG_ID = f.ORG_ID AND f.GRANTEE_TYPE = 'user' AND m.MEMBER_ID = f.GRANTEE_ID
LEFT JOIN STG_TEAMS   t ON t.ORG_ID = f.ORG_ID AND f.GRANTEE_TYPE = 'team' AND t.TEAM_ID   = f.GRANTEE_ID;
