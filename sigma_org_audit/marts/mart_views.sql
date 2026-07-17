-- ==============================================================================
-- mart_views.sql
--
-- Consumption layer for sigma_org_audit. Computed views built on the stage
-- views (and the SCD2 history tables for drift). These are what a Sigma
-- workbook / governance review reads.
--
-- Multi-org: every view carries ORG_ID through from the stage layer, so the
-- audit can be scoped to one org (WHERE ORG_ID = '...') or compared across orgs
-- (GROUP BY ORG_ID). All cross-object joins are org-scoped.
--
-- Run after stage_views.sql. Drift views additionally require the SCD2 history
-- tables produced by sigma_scd2_apply (scd2_history.sql).
--   USE DATABASE MY_DB; USE SCHEMA MY_SCHEMA;
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- V_OBJECT_LIFECYCLE -- deletion/tombstone detection from the append-only raw.
-- An object is DELETED if it was seen in an earlier snapshot but is absent from
-- its (org, object_type) cohort's LATEST snapshot. Because raw is append-only and
-- the stage views keep each object's last-seen row, a deleted object would
-- otherwise show forever as live -- this view is the authoritative current/deleted
-- signal. Needs >= 2 snapshots of a type to detect deletion (COHORT_SNAPSHOTS).
-- Caveat: a partial/under-paged latest extract can mark live objects deleted;
-- completeness hardening is tracked in 9c8.8.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_OBJECT_LIFECYCLE AS
WITH fc AS (
    SELECT ORG_ID, OBJECT_TYPE, OBJECT_ID, SNAPSHOT_TS
    FROM RAW_SIGMA_OBJECTS
    WHERE OBJECT_TYPE IN ('workbook','datamodel','dataset','connection','member','team','user_attribute')
),
cohort AS (
    SELECT ORG_ID, OBJECT_TYPE,
           MAX(SNAPSHOT_TS)            AS COHORT_LATEST_TS,
           COUNT(DISTINCT SNAPSHOT_TS) AS COHORT_SNAPSHOTS
    FROM fc GROUP BY 1, 2
),
obj AS (
    SELECT ORG_ID, OBJECT_TYPE, OBJECT_ID,
           MIN(SNAPSHOT_TS)            AS FIRST_SEEN,
           MAX(SNAPSHOT_TS)            AS LAST_SEEN,
           COUNT(DISTINCT SNAPSHOT_TS) AS SNAPSHOTS_SEEN
    FROM fc GROUP BY 1, 2, 3
)
SELECT
    o.ORG_ID, o.OBJECT_TYPE, o.OBJECT_ID,
    o.FIRST_SEEN, o.LAST_SEEN,
    c.COHORT_LATEST_TS, c.COHORT_SNAPSHOTS,
    (o.LAST_SEEN = c.COHORT_LATEST_TS) AS IS_CURRENT,
    (o.LAST_SEEN < c.COHORT_LATEST_TS) AS IS_DELETED,
    IFF(o.LAST_SEEN < c.COHORT_LATEST_TS, o.LAST_SEEN, NULL) AS DELETED_AFTER_TS
FROM obj o
JOIN cohort c ON c.ORG_ID = o.ORG_ID AND c.OBJECT_TYPE = o.OBJECT_TYPE;

-- ------------------------------------------------------------------------------
-- V_INVENTORY -- one row per first-class object, owner resolved, filterable.
-- Carries IS_CURRENT / IS_DELETED so deleted objects are flagged, not silently
-- presented as live. Filter `WHERE IS_CURRENT` for the live inventory.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_INVENTORY AS
WITH inv AS (
    SELECT w.ORG_ID, 'workbook'  AS OBJECT_TYPE, w.WORKBOOK_ID AS OBJECT_ID, w.NAME, w.PATH,
           w.OWNER_ID, m.DISPLAY_NAME AS OWNER_NAME, m.EMAIL AS OWNER_EMAIL,
           m.ACCOUNT_TYPE AS OWNER_ACCOUNT_TYPE,
           -- Archived/deactivated owners are absent from /v2/members, so they
           -- surface as OWNER_MISSING; there is no separate archived flag.
           (m.MEMBER_ID IS NULL) AS OWNER_MISSING,
           w.CREATED_AT, w.UPDATED_AT, w.SNAPSHOT_TS
    FROM STG_WORKBOOKS w LEFT JOIN STG_MEMBERS m ON m.ORG_ID = w.ORG_ID AND m.MEMBER_ID = w.OWNER_ID
    UNION ALL
    SELECT d.ORG_ID, 'datamodel', d.DATA_MODEL_ID, d.NAME, d.PATH,
           d.OWNER_ID, m.DISPLAY_NAME, m.EMAIL,
           m.ACCOUNT_TYPE,
           (m.MEMBER_ID IS NULL),
           d.CREATED_AT, d.UPDATED_AT, d.SNAPSHOT_TS
    FROM STG_DATAMODELS d LEFT JOIN STG_MEMBERS m ON m.ORG_ID = d.ORG_ID AND m.MEMBER_ID = d.OWNER_ID
    UNION ALL
    SELECT ds.ORG_ID, 'dataset', ds.DATASET_ID, ds.NAME, ds.PATH,
           ds.OWNER_ID, m.DISPLAY_NAME, m.EMAIL,
           m.ACCOUNT_TYPE,
           (m.MEMBER_ID IS NULL),
           ds.CREATED_AT, ds.UPDATED_AT, ds.SNAPSHOT_TS
    FROM STG_DATASETS ds LEFT JOIN STG_MEMBERS m ON m.ORG_ID = ds.ORG_ID AND m.MEMBER_ID = ds.OWNER_ID
)
SELECT
    inv.*,
    lc.LAST_SEEN,
    lc.DELETED_AFTER_TS,
    COALESCE(lc.IS_CURRENT, TRUE)  AS IS_CURRENT,
    COALESCE(lc.IS_DELETED, FALSE) AS IS_DELETED
FROM inv
LEFT JOIN V_OBJECT_LIFECYCLE lc
       ON lc.ORG_ID = inv.ORG_ID AND lc.OBJECT_TYPE = inv.OBJECT_TYPE AND lc.OBJECT_ID = inv.OBJECT_ID;

-- ------------------------------------------------------------------------------
-- V_MIGRATION_SCORE -- R/A/G migration readiness per dataset, with reasons.
-- Phase-1 heuristic: driven by the API migrationStatus plus workbook fan-out.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_MIGRATION_SCORE AS
WITH wb_fanout AS (
    -- how many workbooks reference each dataset (downstream blast radius)
    SELECT ws.ORG_ID AS ORG_ID,
           s.value:datasetId::STRING AS DATASET_ID,
           COUNT(DISTINCT ws.OBJECT_ID) AS WORKBOOK_COUNT
    FROM RAW_SIGMA_OBJECTS ws,
         LATERAL FLATTEN(input => ws.PAYLOAD:sources) s
    WHERE ws.OBJECT_TYPE = 'workbook_sources'
    GROUP BY 1, 2
)
SELECT
    ds.ORG_ID,
    ds.DATASET_ID,
    ds.NAME,
    ds.PATH,
    ds.MIGRATION_STATUS,
    COALESCE(f.WORKBOOK_COUNT, 0) AS DOWNSTREAM_WORKBOOK_COUNT,
    CASE
        WHEN ds.MIGRATION_STATUS IN ('migrated', 'not-required') THEN 'GREEN'
        WHEN ds.MIGRATION_STATUS = 'not-migrated' AND COALESCE(f.WORKBOOK_COUNT, 0) = 0 THEN 'AMBER'
        WHEN ds.MIGRATION_STATUS = 'not-migrated' THEN 'RED'
        ELSE 'AMBER'
    END AS RAG,
    CASE
        WHEN ds.MIGRATION_STATUS IN ('migrated', 'not-required')
            THEN 'Already migrated or migration not required'
        WHEN ds.MIGRATION_STATUS = 'not-migrated' AND COALESCE(f.WORKBOOK_COUNT, 0) = 0
            THEN 'Not migrated, but no workbooks depend on it -- low blast radius'
        WHEN ds.MIGRATION_STATUS = 'not-migrated'
            THEN 'Not migrated and referenced by ' || f.WORKBOOK_COUNT || ' workbook(s) -- migrate before deprecation'
        ELSE 'Unknown migration status -- review manually'
    END AS RAG_REASON,
    ds.SNAPSHOT_TS
FROM STG_DATASETS ds
LEFT JOIN wb_fanout f ON f.ORG_ID = ds.ORG_ID AND f.DATASET_ID = ds.DATASET_ID;

-- ------------------------------------------------------------------------------
-- V_WRITEBACK_GOVERNANCE -- status flags + archival score + reclaimable storage.
-- Ports the writeback_info_sf weighted archival model onto STG_WRITEBACK_TABLES.
--
-- Cross-org attribution: a writeback schema can be SHARED by several Sigma orgs.
-- The scan discovers every SIGDS table via one org's connection (ORG_ID =
-- discovering org), but the WAL workbook URL/ID reveals the TRUE owner. Each table
-- is classified so one org's report is not polluted by another org's tables:
--   OWNED        -- workbook_id maps to a live workbook (active; this/owning org)
--   ORPHANED     -- WAL says it is ours (slug matches) but no live workbook maps
--   CROSS_ORG    -- owned by a DIFFERENT org (matched elsewhere, or WAL slug differs)
--   UNATTRIBUTED -- no WAL url and no workbook match: owner cannot be determined
-- Only ORPHANED tables count toward this org's archival score / reclaimable bytes.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_WRITEBACK_GOVERNANCE AS
WITH org_slug AS (
    -- each extracted org's own slug, from its live workbook URLs
    SELECT ORG_ID,
           MAX(SPLIT_PART(SPLIT_PART(URL, 'sigmacomputing.com/', 2), '/', 1)) AS ORG_SLUG
    FROM STG_WORKBOOKS WHERE URL IS NOT NULL GROUP BY ORG_ID
),
-- Ground-truth access from query history (sigma_query_history_scan), aggregated
-- per writeback table. ACCESS_WORKBOOK_LIVE = at least one accessing workbook is
-- still live. This ENHANCES attribution/staleness; it only ever helps a table.
-- live workbooks keyed by their URL-safe id (the form embedded in query-comment
-- source URLs), since STG_WORKBOOKS.WORKBOOK_ID is the API UUID, a different form.
wb_url AS (
    SELECT WORKBOOK_ID,
           REGEXP_SUBSTR(SPLIT_PART(SPLIT_PART(URL, '/workbook/', 2), '?', 1),
                         '[^-/]+$') AS URL_ID
    FROM STG_WORKBOOKS WHERE URL IS NOT NULL
),
access AS (
    SELECT
        a.WB_DATABASE, a.WB_SCHEMA, a.SIGDS_TABLE,
        MAX(IFF(a.ACCESS_KIND = 'READ',   a.ACCESS_AT, NULL)) AS ACCESS_LAST_READ_AT,
        MAX(IFF(a.ACCESS_KIND = 'INSERT', a.ACCESS_AT, NULL)) AS ACCESS_LAST_WRITE_AT,
        COUNT_IF(a.ACCESS_KIND = 'READ')                      AS ACCESS_READ_COUNT,
        COUNT_IF(a.ACCESS_KIND = 'INSERT')                    AS ACCESS_WRITE_COUNT,
        COUNT(DISTINCT a.WORKBOOK_ID)                         AS ACCESS_DISTINCT_WORKBOOKS,
        COUNT(DISTINCT a.SIGMA_USER_EMAIL)                    AS ACCESS_DISTINCT_USERS,
        MAX(a.WORKBOOK_ID)                                    AS ACCESS_WORKBOOK_ID,
        BOOLOR_AGG(wb.WORKBOOK_ID IS NOT NULL)                AS ACCESS_WORKBOOK_LIVE
    FROM STG_WRITEBACK_ACCESS a
    LEFT JOIN wb_url wb ON wb.URL_ID = a.WORKBOOK_ID
    GROUP BY 1, 2, 3
),
base AS (
    SELECT
        wt.*,
        os.ORG_SLUG                                               AS DISCOVERED_BY_ORG_SLUG,
        DATEDIFF('day', wt.WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())  AS DAYS_SINCE_EDIT,
        DATEDIFF('day', wt.LAST_ALTERED,     CURRENT_TIMESTAMP())  AS DAYS_SINCE_ALTERED,
        ac.ACCESS_LAST_READ_AT,
        ac.ACCESS_LAST_WRITE_AT,
        COALESCE(ac.ACCESS_READ_COUNT, 0)                         AS ACCESS_READ_COUNT,
        COALESCE(ac.ACCESS_WRITE_COUNT, 0)                        AS ACCESS_WRITE_COUNT,
        COALESCE(ac.ACCESS_DISTINCT_WORKBOOKS, 0)                 AS ACCESS_DISTINCT_WORKBOOKS,
        COALESCE(ac.ACCESS_DISTINCT_USERS, 0)                     AS ACCESS_DISTINCT_USERS,
        ac.ACCESS_WORKBOOK_ID,
        COALESCE(ac.ACCESS_WORKBOOK_LIVE, FALSE)                  AS ACCESS_WORKBOOK_LIVE,
        -- most recent activity of any kind from query history
        GREATEST(COALESCE(ac.ACCESS_LAST_READ_AT,  '1900-01-01'::TIMESTAMP_NTZ),
                 COALESCE(ac.ACCESS_LAST_WRITE_AT, '1900-01-01'::TIMESTAMP_NTZ)) AS ACCESS_LAST_AT_RAW,
        (ac.WB_DATABASE IS NOT NULL)                              AS HAS_ANY_ACCESS,
        -- recent = within the same 90-day staleness horizon
        (GREATEST(COALESCE(ac.ACCESS_LAST_READ_AT,  '1900-01-01'::TIMESTAMP_NTZ),
                  COALESCE(ac.ACCESS_LAST_WRITE_AT, '1900-01-01'::TIMESTAMP_NTZ))
             >= DATEADD('day', -90, CURRENT_TIMESTAMP()))         AS HAS_RECENT_ACCESS,
        -- WAL-only staleness kept for provenance/back-compat
        (wt.WAL_LAST_EDIT_AT IS NULL
         OR DATEDIFF('day', wt.WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 90) AS WAL_IS_STALE,
        (COALESCE(wt.ROW_COUNT, 0) = 0)                            AS IS_EMPTY,
        CASE
            WHEN wt.OWNING_ORG_ID IS NOT NULL AND wt.OWNING_ORG_ID <> wt.ORG_ID THEN TRUE
            WHEN wt.OWNING_ORG_ID IS NULL AND wt.WAL_ORG_SLUG IS NOT NULL
                 AND wt.WAL_ORG_SLUG <> os.ORG_SLUG THEN TRUE
            ELSE FALSE
        END                                                       AS IS_FOREIGN
    FROM STG_WRITEBACK_TABLES wt
    LEFT JOIN org_slug os ON os.ORG_ID = wt.ORG_ID
    LEFT JOIN access ac
           ON ac.WB_DATABASE = UPPER(wt.WB_DATABASE)
          AND ac.WB_SCHEMA   = UPPER(wt.WB_SCHEMA)
          AND ac.SIGDS_TABLE = wt.SIGDS_TABLE
    WHERE COALESCE(wt.SCAN_REACHABLE, FALSE) = TRUE
),
enriched AS (
    SELECT base.*,
        -- staleness now factors query history: recent access is NOT stale, even
        -- with no WAL row (the su8 fix -- WAL absence no longer implies stale).
        (WAL_IS_STALE AND NOT HAS_RECENT_ACCESS)                  AS IS_STALE,
        -- provenance of the ownership signal
        CASE
            WHEN COALESCE(WORKBOOK_EXISTS, FALSE) AND ACCESS_WORKBOOK_LIVE THEN 'both'
            WHEN COALESCE(WORKBOOK_EXISTS, FALSE)                          THEN 'wal'
            WHEN ACCESS_WORKBOOK_LIVE                                      THEN 'access_history'
            ELSE 'none'
        END                                                       AS ATTRIBUTION_SOURCE
    FROM base
),
classified AS (
    SELECT enriched.*,
        CASE
            WHEN IS_FOREIGN                                        THEN 'CROSS_ORG'
            -- HIGH-WEIGHT marker: a live workbook actually reading/writing the
            -- table (query history) attributes ownership even when WAL is silent.
            WHEN COALESCE(WORKBOOK_EXISTS, FALSE)
              OR ACCESS_WORKBOOK_LIVE                              THEN 'OWNED'
            WHEN WAL_ORG_SLUG IS NOT NULL
                 AND WAL_ORG_SLUG = DISCOVERED_BY_ORG_SLUG         THEN 'ORPHANED'
            WHEN WAL_ORG_SLUG IS NULL                              THEN 'UNATTRIBUTED'
            ELSE 'ORPHANED'
        END AS ATTRIBUTION
    FROM enriched
)
SELECT
    classified.*,
    (ATTRIBUTION = 'ORPHANED') AS IS_ORPHANED,
    -- archival score only for tables orphaned to THIS org. Recent access (even if
    -- the workbook is gone) still lowers the score -- it may be in live use.
    IFF(ATTRIBUTION = 'ORPHANED',
        GREATEST(0,
            LEAST(100,
                  40
                + IFF(IS_STALE, 30, 0)
                + IFF(IS_EMPTY, 15, 0)
                + IFF(DAYS_SINCE_EDIT > 365, 15, IFF(DAYS_SINCE_EDIT > 180, 8, 0))
                - IFF(HAS_RECENT_ACCESS, 30, 0))),
        0) AS ARCHIVAL_SCORE,
    CASE ATTRIBUTION
        WHEN 'ORPHANED'     THEN IFF(IS_STALE, 'HIGH', 'MEDIUM')
        WHEN 'OWNED'        THEN 'LOW'
        WHEN 'CROSS_ORG'    THEN 'N/A-OTHER-ORG'
        ELSE                     'N/A-UNATTRIBUTED'
    END AS ARCHIVAL_CONFIDENCE,
    IFF(ATTRIBUTION = 'ORPHANED', COALESCE(BYTES, 0), 0) AS RECLAIMABLE_BYTES
FROM classified;

-- ------------------------------------------------------------------------------
-- V_WRITEBACK_SHARED_SCHEMAS -- surfaces writeback schemas used by more than one
-- Sigma org (the condition that inflates naive per-connection orphan counts).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_WRITEBACK_SHARED_SCHEMAS AS
SELECT
    ORG_ID                               AS DISCOVERED_BY_ORG_ID,
    CONNECTION_ID,
    CONNECTION_NAME,
    WB_DATABASE,
    WB_SCHEMA,
    COUNT(*)                             AS SIGDS_TABLES,
    COUNT(DISTINCT WAL_ORG_SLUG)         AS DISTINCT_OWNING_ORG_SLUGS,
    ARRAY_AGG(DISTINCT WAL_ORG_SLUG)     AS OWNING_ORG_SLUGS,
    (COUNT(DISTINCT WAL_ORG_SLUG) > 1)   AS IS_SHARED_ACROSS_ORGS
FROM STG_WRITEBACK_TABLES
GROUP BY 1, 2, 3, 4, 5;

-- ------------------------------------------------------------------------------
-- V_OWNERSHIP_CLEANUP -- objects whose owner is no longer an active member.
-- OWNER_MISSING covers both deleted and archived/deactivated owners, since
-- /v2/members returns only active members (an archived owner is simply absent).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_OWNERSHIP_CLEANUP AS
SELECT *
FROM V_INVENTORY
WHERE OWNER_MISSING = TRUE
  AND IS_CURRENT = TRUE;   -- don't chase ownership on already-deleted objects

-- ------------------------------------------------------------------------------
-- V_WORKBOOK_DRIFT -- changes to workbooks between snapshots, from SCD2 history.
-- Requires: CALL sigma_scd2_apply('STG_WORKBOOKS','SCD2_WORKBOOKS','WORKBOOK_ID');
-- A closed version (SCD_VALID_TO not null) means the object changed at that time.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_WORKBOOK_DRIFT AS
SELECT
    ORG_ID,
    WORKBOOK_ID,
    NAME,
    PATH,
    SCD_VALID_FROM AS CHANGED_FROM,
    SCD_VALID_TO   AS CHANGED_TO,
    SCD_IS_CURRENT,
    UPDATED_AT
FROM SCD2_WORKBOOKS
WHERE SCD_VALID_TO IS NOT NULL          -- only versions that were superseded
ORDER BY SCD_VALID_TO DESC;

-- ------------------------------------------------------------------------------
-- V_DATAMODEL_DRIFT -- changes to data models between snapshots, from SCD2 history.
-- Requires: CALL sigma_scd2_apply('STG_DATAMODELS','SCD2_DATAMODELS','DATA_MODEL_ID');
-- A closed version (SCD_VALID_TO not null) means the object changed at that time.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_DATAMODEL_DRIFT AS
SELECT
    ORG_ID,
    DATA_MODEL_ID,
    NAME,
    PATH,
    SCD_VALID_FROM AS CHANGED_FROM,
    SCD_VALID_TO   AS CHANGED_TO,
    SCD_IS_CURRENT,
    UPDATED_AT
FROM SCD2_DATAMODELS
WHERE SCD_VALID_TO IS NOT NULL          -- only versions that were superseded
ORDER BY SCD_VALID_TO DESC;

-- ------------------------------------------------------------------------------
-- V_TENANCY_TOPOLOGY -- per-org multi-tenant posture for migration planning.
-- ORG_ROLE: parent (can enumerate tenants and has them), standalone (reachable,
-- none), or unknown (tenant enumeration denied -- e.g. 403, not entitled/parent).
-- ------------------------------------------------------------------------------
-- Per-org tenancy summary (one row per org). PARENT_ORGANIZATION_ID is backfilled
-- from the tenant list of any parent that lists this org, for the common case where
-- a child's own self-lookup is denied (403) and can't return its parent.
CREATE OR REPLACE VIEW V_TENANCY_TOPOLOGY AS
SELECT
    o.ORG_ID,
    o.ORG_ROLE,
    o.ROLE_SOURCE,                                  -- api (confirmed) | operator (asserted)
    COALESCE(o.PARENT_ORGANIZATION_ID, tp.DERIVED_PARENT)  AS PARENT_ORGANIZATION_ID,
    CASE
        WHEN o.PARENT_ORGANIZATION_ID IS NOT NULL THEN 'api-self'      -- org self-reported it
        WHEN tp.DERIVED_PARENT        IS NOT NULL THEN 'tenant-list'   -- inferred from a parent's tenant list
        ELSE NULL
    END                                             AS PARENT_SOURCE,
    o.TENANT_COUNT,
    o.DEPLOYMENT_POLICY_COUNT,
    o.SOURCE_SWAP_POLICY_COUNT,
    (o.TENANTS_LIST_ERROR IS NOT NULL)              AS TENANT_ENUM_DENIED,
    o.TENANTS_LIST_ERROR,
    o.TENANT_SELF_ERROR,                            -- NULL for a benign 404 (not a tenant); 403 = denied
    CASE
        WHEN o.ORG_ROLE = 'parent'
            THEN 'Parent/host org with ' || o.TENANT_COUNT || ' tenant(s); assess deployment + source-swap policy coverage'
        WHEN o.ORG_ROLE = 'child'
            THEN 'Child tenant'
                 || COALESCE(' of parent ' || COALESCE(o.PARENT_ORGANIZATION_ID, tp.DERIVED_PARENT), '')
                 || CASE
                        WHEN o.PARENT_ORGANIZATION_ID IS NOT NULL THEN ' (API-confirmed)'
                        WHEN tp.DERIVED_PARENT        IS NOT NULL THEN ' (parent derived from a parent org''s tenant list)'
                        WHEN o.ROLE_SOURCE = 'operator'           THEN ' (operator-asserted; API self-lookup denied)'
                        ELSE ''
                    END
        WHEN o.ORG_ROLE = 'standalone'
            THEN 'Standalone org, no tenants/policies -- candidate to become a parent or be deployed as a tenant'
        ELSE 'Role indeterminate from the API -- tenant list + self-lookup both denied (403). May be a CHILD tenant or a non-parent / unentitled org; re-run with ORG_ROLE_OVERRIDE when the role is known out-of-band (see 9c8.12).'
    END                                             AS TOPOLOGY_NOTE,
    o.SNAPSHOT_TS
FROM STG_ORGANIZATION o
LEFT JOIN (
    SELECT TENANT_ORG_ID, ANY_VALUE(PARENT_ORG_ID) AS DERIVED_PARENT
    FROM STG_TENANTS GROUP BY TENANT_ORG_ID
) tp ON tp.TENANT_ORG_ID = o.ORG_ID;

-- ------------------------------------------------------------------------------
-- V_TENANT_RELATIONSHIPS -- the actual parent->tenant edges (the tenancy tree).
-- One row per tenant a parent enumerated via /v2/tenants, so a parent's tenants
-- are visible even when they weren't audited as their own org. TENANT_EXTRACTED
-- flags whether a full audit snapshot exists for that tenant org.
-- (V_TENANCY_TOPOLOGY is the per-org summary; this is the relationship grain.)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_TENANT_RELATIONSHIPS AS
SELECT
    t.PARENT_ORG_ID,
    po.ORG_ROLE                 AS PARENT_ROLE,
    t.TENANT_ORG_ID,
    t.NAME                      AS TENANT_NAME,
    (te.ORG_ID IS NOT NULL)     AS TENANT_EXTRACTED,       -- did we also audit this tenant org?
    te.ORG_ROLE                 AS TENANT_EXTRACTED_ROLE,
    t.SNAPSHOT_TS
FROM STG_TENANTS t
LEFT JOIN STG_ORGANIZATION po ON po.ORG_ID = t.PARENT_ORG_ID
LEFT JOIN STG_ORGANIZATION te ON te.ORG_ID = t.TENANT_ORG_ID;

-- ------------------------------------------------------------------------------
-- V_USER_ATTRIBUTE_USAGE -- per attribute: binding counts + whether any data
-- model references it (a heuristic RLS-in-use signal: the attribute id or name
-- appears in the model's detail spec). Provenance: REFERENCED_BY_MODELS is
-- inferred (string match), binding counts are observed.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_USER_ATTRIBUTE_USAGE AS
WITH binds AS (
    SELECT ORG_ID, USER_ATTRIBUTE_ID,
           COUNT_IF(GRANTEE_TYPE = 'user')   AS USER_BINDINGS,
           COUNT_IF(GRANTEE_TYPE = 'team')   AS TEAM_BINDINGS,
           COUNT_IF(GRANTEE_TYPE = 'tenant') AS TENANT_BINDINGS,
           COUNT(*)                          AS TOTAL_BINDINGS
    FROM STG_USER_ATTRIBUTE_BINDINGS GROUP BY 1, 2
),
refs AS (
    SELECT ua.ORG_ID, ua.USER_ATTRIBUTE_ID,
           COUNT(DISTINCT dm.DATA_MODEL_ID) AS REFERENCED_BY_MODELS
    FROM STG_USER_ATTRIBUTES ua
    JOIN STG_DATAMODELS dm
      ON dm.ORG_ID = ua.ORG_ID
     AND (CONTAINS(TO_VARCHAR(dm.DETAIL_PAYLOAD), ua.USER_ATTRIBUTE_ID)
          OR CONTAINS(TO_VARCHAR(dm.DETAIL_PAYLOAD), ua.NAME))
    GROUP BY 1, 2
)
SELECT
    ua.ORG_ID, ua.USER_ATTRIBUTE_ID, ua.NAME, ua.DESCRIPTION, ua.DEFAULT_VALUE,
    COALESCE(b.USER_BINDINGS, 0)   AS USER_BINDINGS,
    COALESCE(b.TEAM_BINDINGS, 0)   AS TEAM_BINDINGS,
    COALESCE(b.TENANT_BINDINGS, 0) AS TENANT_BINDINGS,
    COALESCE(b.TOTAL_BINDINGS, 0)  AS TOTAL_BINDINGS,
    COALESCE(r.REFERENCED_BY_MODELS, 0)        AS REFERENCED_BY_MODELS,
    (COALESCE(r.REFERENCED_BY_MODELS, 0) > 0)  AS USED_IN_DATA_MODEL,
    ua.SNAPSHOT_TS
FROM STG_USER_ATTRIBUTES ua
LEFT JOIN binds b ON b.ORG_ID = ua.ORG_ID AND b.USER_ATTRIBUTE_ID = ua.USER_ATTRIBUTE_ID
LEFT JOIN refs  r ON r.ORG_ID = ua.ORG_ID AND r.USER_ATTRIBUTE_ID = ua.USER_ATTRIBUTE_ID;

-- ------------------------------------------------------------------------------
-- V_DATA_ISOLATION -- per-org data-isolation posture for multi-tenant readiness.
-- One row per audited org (driven by STG_ORGANIZATION so the "no attributes"
-- case is still reported).
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_DATA_ISOLATION AS
WITH agg AS (
    SELECT ORG_ID,
           COUNT(*)                       AS USER_ATTRIBUTES,
           SUM(USER_BINDINGS)             AS USER_BINDINGS,
           SUM(TEAM_BINDINGS)             AS TEAM_BINDINGS,
           SUM(TENANT_BINDINGS)           AS TENANT_BINDINGS,
           COUNT_IF(USED_IN_DATA_MODEL)   AS ATTRS_USED_IN_MODELS
    FROM V_USER_ATTRIBUTE_USAGE GROUP BY 1
)
SELECT
    o.ORG_ID,
    o.ORG_ROLE,
    COALESCE(a.USER_ATTRIBUTES, 0)     AS USER_ATTRIBUTES,
    COALESCE(a.USER_BINDINGS, 0)       AS USER_BINDINGS,
    COALESCE(a.TEAM_BINDINGS, 0)       AS TEAM_BINDINGS,
    COALESCE(a.TENANT_BINDINGS, 0)     AS TENANT_BINDINGS,
    COALESCE(a.ATTRS_USED_IN_MODELS, 0) AS ATTRS_USED_IN_MODELS,
    CASE
        WHEN COALESCE(a.USER_ATTRIBUTES, 0) = 0
            THEN 'NONE -- no user attributes; no row-level data isolation configured'
        WHEN COALESCE(a.TENANT_BINDINGS, 0) > 0
            THEN 'TENANT-scoped isolation in use'
        WHEN COALESCE(a.USER_BINDINGS, 0) + COALESCE(a.TEAM_BINDINGS, 0) > 0
            THEN 'USER/TEAM-scoped isolation in use'
        ELSE 'ATTRIBUTES defined but no bindings -- isolation not active'
    END                                AS ISOLATION_POSTURE,
    o.SNAPSHOT_TS
FROM STG_ORGANIZATION o
LEFT JOIN agg a ON a.ORG_ID = o.ORG_ID;
