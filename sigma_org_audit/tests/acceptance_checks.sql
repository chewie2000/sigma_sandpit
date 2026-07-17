-- ==============================================================================
-- acceptance_checks.sql -- end-to-end health check for a deployed sigma_org_audit.
--
-- One result set: CHECK_NAME | DETAIL | STATUS (PASS/FAIL/INFO). Run after a
-- bootstrap/refresh. Set context first:
--   USE DATABASE SIGMA_ORG_AUDIT; USE SCHEMA AUDIT;  (or pass --database/--schema)
-- Thresholds are generic (>0) so it works on any org; INFO rows are expected-empty
-- cases (e.g. no datasets / no deletions) that are not failures.
-- ==============================================================================
WITH checks AS (

-- RAW layer ---------------------------------------------------------------------
SELECT 1 AS ORD, 'raw: rows present' AS CHECK_NAME,
       TO_VARCHAR(COUNT(*)) AS DETAIL,
       IFF(COUNT(*) > 0, 'PASS', 'FAIL') AS STATUS
FROM RAW_SIGMA_OBJECTS
UNION ALL
-- writeback_access rows are intentionally landed with NULL ORG_ID: their org is
-- resolved downstream (STG_WRITEBACK_ACCESS) from the workbook URL, and access
-- can be cross-org. Every other object type is org-stamped at land time.
SELECT 2, 'raw: ORG_ID stamped on every row (0 null)',
       TO_VARCHAR(COUNT_IF(ORG_ID IS NULL)),
       IFF(COUNT_IF(ORG_ID IS NULL) = 0, 'PASS', 'FAIL')
FROM RAW_SIGMA_OBJECTS
WHERE OBJECT_TYPE <> 'writeback_access'
UNION ALL
SELECT 3, 'raw: distinct orgs', TO_VARCHAR(COUNT(DISTINCT ORG_ID)),
       IFF(COUNT(DISTINCT ORG_ID) >= 1, 'PASS', 'FAIL')
FROM RAW_SIGMA_OBJECTS
UNION ALL
SELECT 4, 'raw: object-type coverage (>=12 types present)',
       TO_VARCHAR(COUNT(DISTINCT OBJECT_TYPE)),
       IFF(COUNT(DISTINCT OBJECT_TYPE) >= 12, 'PASS', 'FAIL')
FROM RAW_SIGMA_OBJECTS
UNION ALL
SELECT 5, 'raw: tenancy + user_attribute + writeback types present',
       LISTAGG(DISTINCT OBJECT_TYPE, ',') WITHIN GROUP (ORDER BY OBJECT_TYPE),
       IFF(COUNT_IF(OBJECT_TYPE='organization')>0
           AND COUNT_IF(OBJECT_TYPE='user_attribute')>0
           AND COUNT_IF(OBJECT_TYPE='writeback_table')>0, 'PASS','FAIL')
FROM RAW_SIGMA_OBJECTS
WHERE OBJECT_TYPE IN ('organization','user_attribute','writeback_table','writeback_wal','tenant')

-- STAGE layer -------------------------------------------------------------------
UNION ALL SELECT 10,'stage: STG_WORKBOOKS',         TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_WORKBOOKS
UNION ALL SELECT 11,'stage: STG_DATAMODELS',        TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_DATAMODELS
UNION ALL SELECT 12,'stage: STG_CONNECTIONS',       TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_CONNECTIONS
UNION ALL SELECT 13,'stage: STG_MEMBERS',           TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_MEMBERS
UNION ALL SELECT 14,'stage: STG_WRITEBACK_TABLES',  TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_WRITEBACK_TABLES
UNION ALL SELECT 15,'stage: STG_USER_ATTRIBUTES',   TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_USER_ATTRIBUTES
UNION ALL SELECT 16,'stage: STG_ORGANIZATION',      TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM STG_ORGANIZATION
-- STG_WRITEBACK_ACCESS is optional (needs IMPORTED PRIVILEGES + Enterprise
-- edition); 0 rows is INFO, not a failure -- the feature simply wasn't run.
UNION ALL SELECT 17,'stage: STG_WRITEBACK_ACCESS (optional)', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>=0,'PASS','FAIL') FROM STG_WRITEBACK_ACCESS

-- MART layer --------------------------------------------------------------------
UNION ALL SELECT 20,'mart: V_INVENTORY rows',       TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM V_INVENTORY
UNION ALL SELECT 21,'mart: V_INVENTORY current/deleted',
       'current=' || TO_VARCHAR(COUNT_IF(IS_CURRENT)) || ' deleted=' || TO_VARCHAR(COUNT_IF(IS_DELETED)),
       IFF(COUNT_IF(IS_CURRENT) > 0, 'PASS', 'FAIL') FROM V_INVENTORY
UNION ALL SELECT 22,'mart: V_OBJECT_LIFECYCLE rows', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM V_OBJECT_LIFECYCLE
UNION ALL SELECT 23,'mart: V_WRITEBACK_GOVERNANCE rows', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM V_WRITEBACK_GOVERNANCE
UNION ALL SELECT 24,'mart: writeback reclaimable only on ORPHANED',
       'bad=' || TO_VARCHAR(COUNT_IF(ATTRIBUTION <> 'ORPHANED' AND RECLAIMABLE_BYTES > 0)),
       IFF(COUNT_IF(ATTRIBUTION <> 'ORPHANED' AND RECLAIMABLE_BYTES > 0) = 0, 'PASS','FAIL')
FROM V_WRITEBACK_GOVERNANCE
-- invariant: access-history attribution can only ever land on OWNED tables
-- (it promotes ownership, never demotes) -- 0 violations expected.
UNION ALL SELECT 24.1,'mart: access-history attribution implies OWNED',
       'bad=' || TO_VARCHAR(COUNT_IF(ATTRIBUTION_SOURCE IN ('access_history','both') AND ATTRIBUTION <> 'OWNED')),
       IFF(COUNT_IF(ATTRIBUTION_SOURCE IN ('access_history','both') AND ATTRIBUTION <> 'OWNED') = 0, 'PASS','FAIL')
FROM V_WRITEBACK_GOVERNANCE
UNION ALL SELECT 25,'mart: V_WRITEBACK_SHARED_SCHEMAS rows', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>=0,'PASS','FAIL') FROM V_WRITEBACK_SHARED_SCHEMAS
UNION ALL SELECT 26,'mart: V_TENANCY_TOPOLOGY one row per org, role set',
       'rows=' || TO_VARCHAR(COUNT(*)) || ' nullrole=' || TO_VARCHAR(COUNT_IF(ORG_ROLE IS NULL)),
       IFF(COUNT(*) >= 1 AND COUNT_IF(ORG_ROLE IS NULL) = 0, 'PASS','FAIL') FROM V_TENANCY_TOPOLOGY
UNION ALL SELECT 27,'mart: V_DATA_ISOLATION one row per org, posture set',
       'rows=' || TO_VARCHAR(COUNT(*)) || ' nullposture=' || TO_VARCHAR(COUNT_IF(ISOLATION_POSTURE IS NULL)),
       IFF(COUNT(*) >= 1 AND COUNT_IF(ISOLATION_POSTURE IS NULL) = 0, 'PASS','FAIL') FROM V_DATA_ISOLATION
UNION ALL SELECT 28,'mart: V_USER_ATTRIBUTE_USAGE rows', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM V_USER_ATTRIBUTE_USAGE
UNION ALL SELECT 29,'mart: V_OWNERSHIP_CLEANUP only current (0 deleted)',
       'rows=' || TO_VARCHAR(COUNT(*)) || ' deleted=' || TO_VARCHAR(COALESCE(COUNT_IF(IS_DELETED), 0)),
       IFF(COALESCE(COUNT_IF(IS_DELETED), 0) = 0, 'PASS','FAIL') FROM V_OWNERSHIP_CLEANUP
UNION ALL SELECT 30,'mart: V_MIGRATION_SCORE runs (may be 0 = no datasets)',
       TO_VARCHAR(COUNT(*)), 'INFO' FROM V_MIGRATION_SCORE
UNION ALL SELECT 31,'mart: V_WORKBOOK_DRIFT runs (may be 0 = single snapshot)',
       TO_VARCHAR(COUNT(*)), 'INFO' FROM V_WORKBOOK_DRIFT
UNION ALL SELECT 32,'mart: V_TENANT_RELATIONSHIPS edges have non-null ids',
       'rows=' || TO_VARCHAR(COUNT(*)) || ' nullids=' ||
       TO_VARCHAR(COUNT_IF(PARENT_ORG_ID IS NULL OR TENANT_ORG_ID IS NULL)),
       IFF(COUNT_IF(PARENT_ORG_ID IS NULL OR TENANT_ORG_ID IS NULL) = 0, 'PASS','FAIL')
       FROM V_TENANT_RELATIONSHIPS

-- HISTORY layer -----------------------------------------------------------------
UNION ALL SELECT 40,'history: SCD2_WORKBOOKS rows',  TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM SCD2_WORKBOOKS
UNION ALL SELECT 41,'history: SCD2_CONNECTIONS rows', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM SCD2_CONNECTIONS
UNION ALL SELECT 42,'history: SCD2_WRITEBACK_TABLES rows', TO_VARCHAR(COUNT(*)), IFF(COUNT(*)>0,'PASS','FAIL') FROM SCD2_WRITEBACK_TABLES
)
SELECT CHECK_NAME, DETAIL, STATUS FROM checks ORDER BY ORD;
