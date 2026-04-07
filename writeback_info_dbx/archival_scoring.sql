-- =============================================================================
-- archival_scoring.sql
--
-- Weighted confidence scoring matrix for SIGDS_WORKBOOK_MAP.
-- Scores every record across multiple signals to surface candidates for
-- archival or cleanup, ranked from highest risk to lowest.
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> below.
--
-- =============================================================================
-- !! IMPORTANT — READ BEFORE ACTING ON THESE SCORES !!
-- =============================================================================
--
-- The confidence tiers and weights in this model are entirely subjective.
-- Appropriate thresholds will vary significantly from customer to customer
-- depending on usage patterns, business criticality, data retention policies,
-- and team workflows. Treat scores as a starting point for investigation,
-- not as a directive to delete.
--
-- Incorrectly removing a SIGDS table or its associated WAL table can cause
-- IRREPARABLE impact to the related Sigma content. Workbooks and input tables
-- that depend on these objects will break immediately. If the tables are
-- dropped rather than moved, recovery may not be possible.
--
-- Always follow the safe deletion process:
--   1. Move to a quarantine schema first — do NOT drop directly.
--   2. Monitor for a minimum of 30 days.
--   3. Confirm with the workbook owner before taking any action.
--   4. Only drop from quarantine once the safe period has passed with no issues.
--
-- =============================================================================
-- SCORING MODEL
-- =============================================================================
--
-- Signal                    Max     Description
-- ------------------------  ------  -------------------------------------------
-- API_IS_ARCHIVED           35      Workbook explicitly archived in Sigma
-- IS_ORPHANED               30      SIGDS data table no longer exists
-- IS_DELETED                30      WAL table no longer exists
-- Inactivity (edit age)     20      Days since last WAL edit (banded)
-- Low edit count            10      WAL_MAX_EDIT_NUM — barely used
-- Small / empty table       10      SIGDS_TABLE_SIZE_BYTES
-- IS_LEGACY_WAL              8      Old UUID-based WAL naming convention
-- Table age                  7      SIGDS_TABLE_CREATED_AT (banded)
--
-- Maximum possible score:  150
--
-- =============================================================================
-- CONFIDENCE TIERS
-- =============================================================================
--
-- Score     Tier         Recommendation
-- --------  -----------  ------------------------------------------------------
-- >= 70     CRITICAL     Multiple strong signals — prioritise for archival
-- >= 40     HIGH         Strong candidate — review and action
-- >= 20     MEDIUM       Worth monitoring — verify with owner before actioning
--  < 20     LOW          Active or recently used — retain
--
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;

WITH scored AS (

    SELECT
        -- Identity
        SIGDS_TABLE,
        WAL_TABLE_FQN,
        WORKBOOK_ID,
        WORKBOOK_NAME,
        WORKBOOK_PATH,
        OBJECT_TYPE,
        WAL_WORKBOOK_URL,
        WAL_INPUT_TABLE_NAME,
        TRIM(COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, ''))
                                                                AS OWNER,
        WAL_LAST_EDIT_BY,
        WAL_LAST_EDIT_AT,
        DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())   AS DAYS_SINCE_LAST_EDIT,
        SIGDS_TABLE_SIZE_BYTES,
        WAL_MAX_EDIT_NUM,
        SIGDS_TABLE_CREATED_AT,
        DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP())
                                                                AS TABLE_AGE_DAYS,

        -- Status flags
        API_IS_ARCHIVED,
        IS_ORPHANED,
        IS_DELETED,
        IS_LEGACY_WAL,
        IS_TAGGED_VERSION,
        VERSION_TAG_NAME,
        PARENT_WORKBOOK_ID,

        -- =================================================================
        -- SCORE COMPONENTS
        -- =================================================================

        -- 1. Workbook archived in Sigma (35)
        --    Strongest signal: Sigma itself considers the workbook retired.
        CASE WHEN API_IS_ARCHIVED = TRUE THEN 35 ELSE 0 END
            AS SCORE_ARCHIVED,

        -- 2. SIGDS data table no longer exists in Databricks (30)
        --    Table has already been dropped — WAL is orphaned.
        CASE WHEN IS_ORPHANED = TRUE THEN 30 ELSE 0 END
            AS SCORE_ORPHANED,

        -- 3. WAL table no longer exists in Databricks (30)
        --    WAL itself is gone — likely already cleaned up partially.
        CASE WHEN IS_DELETED = TRUE THEN 30 ELSE 0 END
            AS SCORE_DELETED,

        -- 4. Inactivity — days since last WAL edit (max 20)
        --    Long periods of no writeback activity suggest abandonment.
        CASE
            WHEN WAL_LAST_EDIT_AT IS NULL                                          THEN 20
            WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 365       THEN 20
            WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 180       THEN 15
            WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 90        THEN 8
            WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 30        THEN 3
            ELSE 0
        END AS SCORE_INACTIVITY,

        -- 5. Low edit count (max 10)
        --    Very few edits suggests the table was never actively used.
        CASE
            WHEN WAL_MAX_EDIT_NUM IS NULL OR WAL_MAX_EDIT_NUM = 0 THEN 10
            WHEN WAL_MAX_EDIT_NUM <= 5                            THEN 7
            WHEN WAL_MAX_EDIT_NUM <= 20                           THEN 3
            ELSE 0
        END AS SCORE_EDIT_COUNT,

        -- 6. Small or empty SIGDS table (max 10)
        --    Minimal data stored suggests low usage or a test table.
        CASE
            WHEN SIGDS_TABLE_SIZE_BYTES IS NULL OR SIGDS_TABLE_SIZE_BYTES = 0 THEN 10
            WHEN SIGDS_TABLE_SIZE_BYTES < 1048576                              THEN 7  -- < 1 MB
            WHEN SIGDS_TABLE_SIZE_BYTES < 10485760                             THEN 3  -- < 10 MB
            ELSE 0
        END AS SCORE_TABLE_SIZE,

        -- 7. Legacy WAL naming convention (8)
        --    Old sigds_wal_<uuid> tables predate the DS_ID convention and
        --    are more likely to be stale or unowned.
        CASE WHEN IS_LEGACY_WAL = TRUE THEN 8 ELSE 0 END
            AS SCORE_LEGACY_WAL,

        -- 8. Age of the SIGDS table (max 7)
        --    Long-lived tables that have seen no recent activity are lower
        --    priority to retain.
        CASE
            WHEN SIGDS_TABLE_CREATED_AT IS NULL                                          THEN 0
            WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 730       THEN 7  -- > 2 years
            WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 365       THEN 4  -- > 1 year
            ELSE 0
        END AS SCORE_AGE

    FROM SIGDS_WORKBOOK_MAP

),

totalled AS (

    SELECT
        *,
        SCORE_ARCHIVED
            + SCORE_ORPHANED
            + SCORE_DELETED
            + SCORE_INACTIVITY
            + SCORE_EDIT_COUNT
            + SCORE_TABLE_SIZE
            + SCORE_LEGACY_WAL
            + SCORE_AGE                                         AS TOTAL_SCORE
    FROM scored

)

SELECT
    -- Recommendation
    CASE
        WHEN TOTAL_SCORE >= 70 THEN 'CRITICAL — Prioritise for archival'
        WHEN TOTAL_SCORE >= 40 THEN 'HIGH — Strong candidate, review and action'
        WHEN TOTAL_SCORE >= 20 THEN 'MEDIUM — Monitor, verify with owner'
        ELSE                        'LOW — Active, retain'
    END                                                         AS ARCHIVAL_RECOMMENDATION,

    TOTAL_SCORE,

    -- Identity
    SIGDS_TABLE,
    WORKBOOK_NAME,
    OBJECT_TYPE,
    WAL_INPUT_TABLE_NAME,
    OWNER,
    WAL_LAST_EDIT_BY,
    WAL_LAST_EDIT_AT,
    DAYS_SINCE_LAST_EDIT,
    SIGDS_TABLE_SIZE_BYTES,
    WAL_MAX_EDIT_NUM,
    TABLE_AGE_DAYS,
    WAL_WORKBOOK_URL,

    -- Status flags
    API_IS_ARCHIVED,
    IS_ORPHANED,
    IS_DELETED,
    IS_LEGACY_WAL,
    IS_TAGGED_VERSION,
    VERSION_TAG_NAME,

    -- Score breakdown (for transparency / tuning)
    SCORE_ARCHIVED,
    SCORE_ORPHANED,
    SCORE_DELETED,
    SCORE_INACTIVITY,
    SCORE_EDIT_COUNT,
    SCORE_TABLE_SIZE,
    SCORE_LEGACY_WAL,
    SCORE_AGE

FROM totalled
ORDER BY
    TOTAL_SCORE DESC,
    DAYS_SINCE_LAST_EDIT DESC NULLS FIRST;


-- =============================================================================
-- SUMMARY ROLLUP — count and storage by recommendation tier
-- =============================================================================

WITH scored AS (
    SELECT
        CASE
            WHEN (
                CASE WHEN API_IS_ARCHIVED = TRUE THEN 35 ELSE 0 END +
                CASE WHEN IS_ORPHANED     = TRUE THEN 30 ELSE 0 END +
                CASE WHEN IS_DELETED      = TRUE THEN 30 ELSE 0 END +
                CASE
                    WHEN WAL_LAST_EDIT_AT IS NULL                                          THEN 20
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 365       THEN 20
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 180       THEN 15
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 90        THEN 8
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 30        THEN 3
                    ELSE 0
                END +
                CASE
                    WHEN WAL_MAX_EDIT_NUM IS NULL OR WAL_MAX_EDIT_NUM = 0 THEN 10
                    WHEN WAL_MAX_EDIT_NUM <= 5                            THEN 7
                    WHEN WAL_MAX_EDIT_NUM <= 20                           THEN 3
                    ELSE 0
                END +
                CASE
                    WHEN SIGDS_TABLE_SIZE_BYTES IS NULL OR SIGDS_TABLE_SIZE_BYTES = 0 THEN 10
                    WHEN SIGDS_TABLE_SIZE_BYTES < 1048576                              THEN 7
                    WHEN SIGDS_TABLE_SIZE_BYTES < 10485760                             THEN 3
                    ELSE 0
                END +
                CASE WHEN IS_LEGACY_WAL = TRUE THEN 8 ELSE 0 END +
                CASE
                    WHEN SIGDS_TABLE_CREATED_AT IS NULL                                          THEN 0
                    WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 730       THEN 7
                    WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 365       THEN 4
                    ELSE 0
                END
            ) >= 70 THEN 'CRITICAL'
            WHEN (
                CASE WHEN API_IS_ARCHIVED = TRUE THEN 35 ELSE 0 END +
                CASE WHEN IS_ORPHANED     = TRUE THEN 30 ELSE 0 END +
                CASE WHEN IS_DELETED      = TRUE THEN 30 ELSE 0 END +
                CASE
                    WHEN WAL_LAST_EDIT_AT IS NULL                                          THEN 20
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 365       THEN 20
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 180       THEN 15
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 90        THEN 8
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 30        THEN 3
                    ELSE 0
                END +
                CASE
                    WHEN WAL_MAX_EDIT_NUM IS NULL OR WAL_MAX_EDIT_NUM = 0 THEN 10
                    WHEN WAL_MAX_EDIT_NUM <= 5                            THEN 7
                    WHEN WAL_MAX_EDIT_NUM <= 20                           THEN 3
                    ELSE 0
                END +
                CASE
                    WHEN SIGDS_TABLE_SIZE_BYTES IS NULL OR SIGDS_TABLE_SIZE_BYTES = 0 THEN 10
                    WHEN SIGDS_TABLE_SIZE_BYTES < 1048576                              THEN 7
                    WHEN SIGDS_TABLE_SIZE_BYTES < 10485760                             THEN 3
                    ELSE 0
                END +
                CASE WHEN IS_LEGACY_WAL = TRUE THEN 8 ELSE 0 END +
                CASE
                    WHEN SIGDS_TABLE_CREATED_AT IS NULL                                          THEN 0
                    WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 730       THEN 7
                    WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 365       THEN 4
                    ELSE 0
                END
            ) >= 40 THEN 'HIGH'
            WHEN (
                CASE WHEN API_IS_ARCHIVED = TRUE THEN 35 ELSE 0 END +
                CASE WHEN IS_ORPHANED     = TRUE THEN 30 ELSE 0 END +
                CASE WHEN IS_DELETED      = TRUE THEN 30 ELSE 0 END +
                CASE
                    WHEN WAL_LAST_EDIT_AT IS NULL                                          THEN 20
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 365       THEN 20
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 180       THEN 15
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 90        THEN 8
                    WHEN DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()) > 30        THEN 3
                    ELSE 0
                END +
                CASE
                    WHEN WAL_MAX_EDIT_NUM IS NULL OR WAL_MAX_EDIT_NUM = 0 THEN 10
                    WHEN WAL_MAX_EDIT_NUM <= 5                            THEN 7
                    WHEN WAL_MAX_EDIT_NUM <= 20                           THEN 3
                    ELSE 0
                END +
                CASE
                    WHEN SIGDS_TABLE_SIZE_BYTES IS NULL OR SIGDS_TABLE_SIZE_BYTES = 0 THEN 10
                    WHEN SIGDS_TABLE_SIZE_BYTES < 1048576                              THEN 7
                    WHEN SIGDS_TABLE_SIZE_BYTES < 10485760                             THEN 3
                    ELSE 0
                END +
                CASE WHEN IS_LEGACY_WAL = TRUE THEN 8 ELSE 0 END +
                CASE
                    WHEN SIGDS_TABLE_CREATED_AT IS NULL                                          THEN 0
                    WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 730       THEN 7
                    WHEN DATEDIFF(DAY, SIGDS_TABLE_CREATED_AT, CURRENT_TIMESTAMP()) > 365       THEN 4
                    ELSE 0
                END
            ) >= 20 THEN 'MEDIUM'
            ELSE 'LOW'
        END                             AS TIER,
        SIGDS_TABLE_SIZE_BYTES
    FROM SIGDS_WORKBOOK_MAP
)

SELECT
    TIER,
    COUNT(*)                            AS RECORD_COUNT,
    SUM(SIGDS_TABLE_SIZE_BYTES)         AS TOTAL_SIZE_BYTES,
    ROUND(SUM(SIGDS_TABLE_SIZE_BYTES) / 1073741824.0, 2)
                                        AS TOTAL_SIZE_GB
FROM scored
GROUP BY TIER
ORDER BY
    CASE TIER
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH'     THEN 2
        WHEN 'MEDIUM'   THEN 3
        ELSE 4
    END;
