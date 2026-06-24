-- =============================================================================
-- archival_scoring.sql
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> below with the
-- Unity Catalog catalog and schema where SIGDS_WORKBOOK_MAP resides.
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;
--
-- SCORING MODEL OVERVIEW (total = 100 pts, higher = stronger archival candidate)
-- -----------------------------------------------------------------------------
--
-- Dimension                        Max    Description
-- -------------------------------  -----  -------------------------------------
-- 1. Archival / deletion status     30    IS_ORPHANED / IS_DELETED / API_IS_ARCHIVED
--                                         / workbook absent from Sigma API
-- 2. WAL edit recency               25    Days since last WAL write
-- 3. SIGDS table modification       15    Days since last write to the data table
--                                         NOTE: SIGDS_TABLE_LAST_MODIFIED reflects
--                                         ALL Delta writes including OPTIMIZE and
--                                         VACUUM, not just user writeback activity.
--                                         A recently-modified SIGDS table may not
--                                         indicate active use — cross-check with
--                                         WAL edit recency before drawing conclusions.
-- 4. Edit volume                    10    WAL_MAX_EDIT_NUM (low = likely unused)
--                                         NOTE: Thresholds are calibrated for
--                                         high-volume production environments.
--                                         In sandbox/test environments most tables
--                                         will score 8 pts (≤10 edits) regardless
--                                         of actual usage level. Tune ≤10 / ≤50
--                                         thresholds to your org's typical volumes.
-- 5. Legacy WAL flag                10    sigds_wal_<uuid> naming = pre-MultiWAL
-- 6. Storage reclamation value      10    Larger tables = more storage to reclaim
--
-- RISK PENALTY
-- -----------------------------------------------------------------------------
-- IS_TAGGED_VERSION = TRUE → subtract 15 pts (floor at 0)
-- Tagged versions (Prod, QA) are high-risk to archive — even a high raw score
-- should not automatically trigger quarantine for these records.
--
-- CONFIDENCE TIERS
-- -----------------------------------------------------------------------------
-- Score >= 75 → TIER 1 — Strong candidate (quarantine now)
-- Score 50–74 → TIER 2 — Likely candidate (review with owner)
-- Score 25–49 → TIER 3 — Monitor (check in 90 days)
-- Score  < 25 → TIER 4 — Keep (active or protected)
--
-- !! IMPORTANT — READ BEFORE ACTING ON THESE SCORES !!
-- -----------------------------------------------------------------------------
-- The confidence tiers and weights in this model are entirely subjective.
-- Appropriate thresholds will vary significantly from customer to customer
-- depending on usage patterns, business criticality, data retention policies,
-- and team workflows. Treat scores as a starting point for investigation,
-- NOT as a directive to delete.
--
-- Incorrectly removing a SIGDS table or its associated WAL table can cause
-- IRREPARABLE impact to the related Sigma content. Workbooks and input tables
-- that depend on these objects will break immediately and, if dropped rather
-- than moved, may not be recoverable.
--
-- SAFE DELETION PROCESS — always follow this sequence:
--   1. Move to a quarantine schema using ALTER TABLE RENAME TO
--      e.g. ALTER TABLE <CATALOG>.<SCHEMA>.<TABLE>
--               RENAME TO <CATALOG>.<SCHEMA>_quarantine.<TABLE>
--      Never DROP directly.
--   2. Monitor for a minimum of 30 days — confirm no workbook errors
--      and verify with the workbook owner before proceeding.
--   3. Only DROP from the quarantine schema once the safe period has passed
--      with no issues reported.
-- =============================================================================


-- =============================================================================
-- CTE — scored: inner subquery computes age/size fields; outer SELECT adds scores.
-- Databricks SQL does not support SELECT *, expr in CTEs, so derived fields are
-- computed in an inline subquery and referenced by name in the outer SELECT.
-- =============================================================================

WITH scored AS (

    SELECT
        -- Identity
        SIGDS_TABLE,
        SCAN_SCHEMA,
        WORKBOOK_NAME,
        API_OWNER_FIRST_NAME,
        API_OWNER_LAST_NAME,
        API_IS_ARCHIVED,
        IS_ORPHANED,
        IS_DELETED,
        IS_LEGACY_WAL,
        IS_TAGGED_VERSION,
        VERSION_TAG_NAME,
        WAL_MAX_EDIT_NUM,
        SIGDS_TABLE_SIZE_BYTES,
        WAL_WORKBOOK_URL,

        -- Derived age and size fields (computed in inner subquery)
        DAYS_SINCE_LAST_EDIT,
        DAYS_SINCE_SIGDS_MODIFIED,
        SIGDS_TABLE_SIZE_MB,

        -- -----------------------------------------------------------------
        -- 1. ARCHIVAL / DELETION STATUS (0–30 pts)
        --    Mutually exclusive — pick the highest applicable state.
        --    IS_ORPHANED is the strongest: the data table is already gone.
        --    The WORKBOOK_NAME IS NULL / API_IS_ARCHIVED IS NULL branch catches
        --    workbooks that were deleted from Sigma entirely (not just archived)
        --    and unsaved exploration sessions — in both cases there is no live
        --    Sigma content actively using this table.
        -- -----------------------------------------------------------------
        CASE
            WHEN IS_ORPHANED = TRUE
                THEN 30   -- SIGDS table no longer exists — WAL is a dead record
            WHEN IS_DELETED = TRUE AND IS_ORPHANED = FALSE
                THEN 25   -- WAL table gone but data table still present
            WHEN API_IS_ARCHIVED = TRUE
             AND IS_ORPHANED = FALSE
             AND IS_DELETED  = FALSE
                THEN 20   -- Workbook archived in Sigma; tables still exist
            WHEN WORKBOOK_NAME IS NULL
             AND API_IS_ARCHIVED IS NULL
             AND IS_ORPHANED = FALSE
             AND IS_DELETED  = FALSE
                THEN 15   -- Workbook absent from Sigma API — deleted or never saved
            ELSE 0
        END                                                         AS SCORE_STATUS,

        -- -----------------------------------------------------------------
        -- 2. WAL EDIT RECENCY (0–25 pts)
        --    Long inactivity in writeback = likely abandoned.
        --    NULL treated as > 365 days (no evidence of recent activity).
        -- -----------------------------------------------------------------
        CASE
            WHEN DAYS_SINCE_LAST_EDIT IS NULL
              OR DAYS_SINCE_LAST_EDIT > 365   THEN 25
            WHEN DAYS_SINCE_LAST_EDIT > 180   THEN 18
            WHEN DAYS_SINCE_LAST_EDIT > 90    THEN 10
            WHEN DAYS_SINCE_LAST_EDIT > 30    THEN 4
            ELSE 0
        END                                                         AS SCORE_WAL_RECENCY,

        -- -----------------------------------------------------------------
        -- 3. SIGDS TABLE MODIFICATION RECENCY (0–15 pts)
        --    Measures actual data-layer activity, independent of WAL edits.
        --    NULL treated as > 365 days (orphaned or never written to).
        -- -----------------------------------------------------------------
        CASE
            WHEN DAYS_SINCE_SIGDS_MODIFIED IS NULL
              OR DAYS_SINCE_SIGDS_MODIFIED > 365   THEN 15
            WHEN DAYS_SINCE_SIGDS_MODIFIED > 180   THEN 10
            WHEN DAYS_SINCE_SIGDS_MODIFIED > 90    THEN 5
            ELSE 0
        END                                                         AS SCORE_SIGDS_RECENCY,

        -- -----------------------------------------------------------------
        -- 4. EDIT VOLUME — WAL_MAX_EDIT_NUM (0–10 pts)
        --    Very low edit counts suggest exploratory or test tables that
        --    were never meaningfully used in production.
        -- -----------------------------------------------------------------
        CASE
            WHEN WAL_MAX_EDIT_NUM IS NULL
              OR WAL_MAX_EDIT_NUM = 0    THEN 10
            WHEN WAL_MAX_EDIT_NUM <= 10  THEN 8
            WHEN WAL_MAX_EDIT_NUM <= 50  THEN 5
            WHEN WAL_MAX_EDIT_NUM <= 200 THEN 2
            ELSE 0
        END                                                         AS SCORE_EDIT_VOLUME,

        -- -----------------------------------------------------------------
        -- 5. LEGACY WAL FLAG (0–10 pts)
        --    Old sigds_wal_<uuid> tables predate MultiWAL and are priority
        --    migration targets. Score higher if still actively used (urgent
        --    migration need) vs inactive (likely already abandoned).
        --    NULL DAYS_SINCE_LAST_EDIT = no known edit history — treat as
        --    inactive (5 pts), not as actively written (10 pts).
        -- -----------------------------------------------------------------
        CASE
            WHEN IS_LEGACY_WAL = TRUE
             AND DAYS_SINCE_LAST_EDIT IS NOT NULL
             AND DAYS_SINCE_LAST_EDIT < 180
                THEN 10   -- Legacy WAL still being actively written — migrate urgently
            WHEN IS_LEGACY_WAL = TRUE
                THEN 5    -- Legacy WAL, inactive or no edit history — lower urgency
            ELSE 0
        END                                                         AS SCORE_LEGACY_WAL,

        -- -----------------------------------------------------------------
        -- 6. STORAGE RECLAMATION VALUE (0–10 pts)
        --    Larger tables represent more storage to reclaim on cleanup.
        --    NULL or empty tables score 1 pt — still worth noting.
        -- -----------------------------------------------------------------
        CASE
            WHEN SIGDS_TABLE_SIZE_BYTES > 1073741824  THEN 10   -- > 1 GB
            WHEN SIGDS_TABLE_SIZE_BYTES > 104857600   THEN 7    -- > 100 MB
            WHEN SIGDS_TABLE_SIZE_BYTES > 10485760    THEN 4    -- > 10 MB
            ELSE 1                                               -- < 10 MB or NULL
        END                                                         AS SCORE_STORAGE,

        -- -----------------------------------------------------------------
        -- RISK PENALTY — IS_TAGGED_VERSION (0 or -15)
        --    Tagged versions (Prod, QA) are high-risk to archive.
        --    Subtract 15 pts to push them out of Tier 1/2 unless other
        --    signals are extremely strong.
        -- -----------------------------------------------------------------
        CASE
            WHEN IS_TAGGED_VERSION = TRUE THEN -15
            ELSE 0
        END                                                         AS PENALTY_TAGGED_VERSION

    FROM (
        -- Inner subquery: compute age and size fields so the outer SELECT
        -- can reference them by name in the scoring CASE expressions.
        SELECT
            SIGDS_TABLE,
            SCAN_SCHEMA,
            WORKBOOK_NAME,
            API_OWNER_FIRST_NAME,
            API_OWNER_LAST_NAME,
            API_IS_ARCHIVED,
            IS_ORPHANED,
            IS_DELETED,
            IS_LEGACY_WAL,
            IS_TAGGED_VERSION,
            VERSION_TAG_NAME,
            WAL_MAX_EDIT_NUM,
            SIGDS_TABLE_SIZE_BYTES,
            WAL_WORKBOOK_URL,
            CASE WHEN WAL_LAST_EDIT_AT IS NULL THEN NULL
                 ELSE DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())
            END                                                     AS DAYS_SINCE_LAST_EDIT,
            CASE WHEN SIGDS_TABLE_LAST_MODIFIED IS NULL THEN NULL
                 ELSE DATEDIFF(DAY, SIGDS_TABLE_LAST_MODIFIED, CURRENT_TIMESTAMP())
            END                                                     AS DAYS_SINCE_SIGDS_MODIFIED,
            ROUND(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0) / 1048576.0, 2)
                                                                    AS SIGDS_TABLE_SIZE_MB
        FROM SIGDS_WORKBOOK_MAP
    )

)


-- =============================================================================
-- FINAL SELECT — assemble output, compute TOTAL_SCORE and ARCHIVAL_TIER
-- =============================================================================

SELECT

    -- Identity
    SIGDS_TABLE,
    SCAN_SCHEMA,
    WORKBOOK_NAME,
    TRIM(
        COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')
    )                                                               AS OWNER_FULL_NAME,

    -- Status flags
    API_IS_ARCHIVED,
    IS_ORPHANED,
    IS_DELETED,
    IS_LEGACY_WAL,
    IS_TAGGED_VERSION,
    VERSION_TAG_NAME,

    -- Age and size metrics
    DAYS_SINCE_LAST_EDIT,
    DAYS_SINCE_SIGDS_MODIFIED,
    WAL_MAX_EDIT_NUM,
    SIGDS_TABLE_SIZE_MB,

    -- Component scores (exposed for transparency and threshold tuning)
    SCORE_STATUS,
    SCORE_WAL_RECENCY,
    SCORE_SIGDS_RECENCY,
    SCORE_EDIT_VOLUME,
    SCORE_LEGACY_WAL,
    SCORE_STORAGE,
    PENALTY_TAGGED_VERSION,

    -- Total score: sum all components + penalty, floor at 0, cap at 100
    GREATEST(0, LEAST(100,
          SCORE_STATUS
        + SCORE_WAL_RECENCY
        + SCORE_SIGDS_RECENCY
        + SCORE_EDIT_VOLUME
        + SCORE_LEGACY_WAL
        + SCORE_STORAGE
        + PENALTY_TAGGED_VERSION
    ))                                                              AS TOTAL_SCORE,

    -- Human-readable archival tier
    CASE
        WHEN GREATEST(0, LEAST(100,
              SCORE_STATUS + SCORE_WAL_RECENCY + SCORE_SIGDS_RECENCY
            + SCORE_EDIT_VOLUME + SCORE_LEGACY_WAL + SCORE_STORAGE
            + PENALTY_TAGGED_VERSION)) >= 75
            THEN 'TIER 1 — Strong candidate (quarantine now)'
        WHEN GREATEST(0, LEAST(100,
              SCORE_STATUS + SCORE_WAL_RECENCY + SCORE_SIGDS_RECENCY
            + SCORE_EDIT_VOLUME + SCORE_LEGACY_WAL + SCORE_STORAGE
            + PENALTY_TAGGED_VERSION)) >= 50
            THEN 'TIER 2 — Likely candidate (review with owner)'
        WHEN GREATEST(0, LEAST(100,
              SCORE_STATUS + SCORE_WAL_RECENCY + SCORE_SIGDS_RECENCY
            + SCORE_EDIT_VOLUME + SCORE_LEGACY_WAL + SCORE_STORAGE
            + PENALTY_TAGGED_VERSION)) >= 25
            THEN 'TIER 3 — Monitor (check in 90 days)'
        ELSE 'TIER 4 — Keep (active or protected)'
    END                                                             AS ARCHIVAL_TIER,

    -- Link for admin follow-up
    WAL_WORKBOOK_URL

FROM scored
ORDER BY
    TOTAL_SCORE DESC,
    SIGDS_TABLE_SIZE_MB DESC;


-- =============================================================================
-- TIER SUMMARY ROLLUP
-- Gives an at-a-glance view of the scale of the cleanup opportunity,
-- grouped by archival tier.
-- =============================================================================

WITH rollup AS (
    SELECT
        SIGDS_TABLE_SIZE_BYTES,
        DAYS_SINCE_LAST_EDIT,
        GREATEST(0, LEAST(100,
            -- Status
            CASE
                WHEN IS_ORPHANED = TRUE THEN 30
                WHEN IS_DELETED = TRUE AND IS_ORPHANED = FALSE THEN 25
                WHEN API_IS_ARCHIVED = TRUE AND IS_ORPHANED = FALSE AND IS_DELETED = FALSE THEN 20
                WHEN WORKBOOK_NAME IS NULL AND API_IS_ARCHIVED IS NULL
                 AND IS_ORPHANED = FALSE AND IS_DELETED = FALSE THEN 15
                ELSE 0
            END +
            -- WAL recency
            CASE
                WHEN DAYS_SINCE_LAST_EDIT IS NULL OR DAYS_SINCE_LAST_EDIT > 365 THEN 25
                WHEN DAYS_SINCE_LAST_EDIT > 180 THEN 18
                WHEN DAYS_SINCE_LAST_EDIT > 90  THEN 10
                WHEN DAYS_SINCE_LAST_EDIT > 30  THEN 4
                ELSE 0
            END +
            -- SIGDS recency
            CASE
                WHEN DAYS_SINCE_SIGDS_MODIFIED IS NULL OR DAYS_SINCE_SIGDS_MODIFIED > 365 THEN 15
                WHEN DAYS_SINCE_SIGDS_MODIFIED > 180 THEN 10
                WHEN DAYS_SINCE_SIGDS_MODIFIED > 90  THEN 5
                ELSE 0
            END +
            -- Edit volume
            CASE
                WHEN WAL_MAX_EDIT_NUM IS NULL OR WAL_MAX_EDIT_NUM = 0 THEN 10
                WHEN WAL_MAX_EDIT_NUM <= 10  THEN 8
                WHEN WAL_MAX_EDIT_NUM <= 50  THEN 5
                WHEN WAL_MAX_EDIT_NUM <= 200 THEN 2
                ELSE 0
            END +
            -- Legacy WAL
            CASE
                WHEN IS_LEGACY_WAL = TRUE AND DAYS_SINCE_LAST_EDIT IS NOT NULL
                 AND DAYS_SINCE_LAST_EDIT < 180 THEN 10
                WHEN IS_LEGACY_WAL = TRUE THEN 5
                ELSE 0
            END +
            -- Storage
            CASE
                WHEN SIGDS_TABLE_SIZE_BYTES > 1073741824 THEN 10
                WHEN SIGDS_TABLE_SIZE_BYTES > 104857600  THEN 7
                WHEN SIGDS_TABLE_SIZE_BYTES > 10485760   THEN 4
                ELSE 1
            END +
            -- Penalty
            CASE WHEN IS_TAGGED_VERSION = TRUE THEN -15 ELSE 0 END
        ))                                                          AS TOTAL_SCORE
    FROM (
        SELECT
            IS_ORPHANED,
            IS_DELETED,
            API_IS_ARCHIVED,
            IS_LEGACY_WAL,
            IS_TAGGED_VERSION,
            WAL_MAX_EDIT_NUM,
            SIGDS_TABLE_SIZE_BYTES,
            WORKBOOK_NAME,
            CASE WHEN WAL_LAST_EDIT_AT IS NULL THEN NULL
                 ELSE DATEDIFF(DAY, WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP())
            END AS DAYS_SINCE_LAST_EDIT,
            CASE WHEN SIGDS_TABLE_LAST_MODIFIED IS NULL THEN NULL
                 ELSE DATEDIFF(DAY, SIGDS_TABLE_LAST_MODIFIED, CURRENT_TIMESTAMP())
            END AS DAYS_SINCE_SIGDS_MODIFIED
        FROM SIGDS_WORKBOOK_MAP
    )
)

SELECT
    CASE
        WHEN TOTAL_SCORE >= 75 THEN 'TIER 1 — Strong candidate (quarantine now)'
        WHEN TOTAL_SCORE >= 50 THEN 'TIER 2 — Likely candidate (review with owner)'
        WHEN TOTAL_SCORE >= 25 THEN 'TIER 3 — Monitor (check in 90 days)'
        ELSE                        'TIER 4 — Keep (active or protected)'
    END                                                             AS ARCHIVAL_TIER,
    COUNT(*)                                                        AS RECORD_COUNT,
    ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3)
                                                                    AS TOTAL_SIZE_GB,
    ROUND(AVG(TOTAL_SCORE), 1)                                      AS AVG_SCORE,
    MIN(DAYS_SINCE_LAST_EDIT)                                       AS MIN_DAYS_SINCE_LAST_EDIT,
    MAX(DAYS_SINCE_LAST_EDIT)                                       AS MAX_DAYS_SINCE_LAST_EDIT
FROM rollup
GROUP BY
    CASE
        WHEN TOTAL_SCORE >= 75 THEN 'TIER 1 — Strong candidate (quarantine now)'
        WHEN TOTAL_SCORE >= 50 THEN 'TIER 2 — Likely candidate (review with owner)'
        WHEN TOTAL_SCORE >= 25 THEN 'TIER 3 — Monitor (check in 90 days)'
        ELSE                        'TIER 4 — Keep (active or protected)'
    END
ORDER BY
    CASE ARCHIVAL_TIER
        WHEN 'TIER 1 — Strong candidate (quarantine now)'    THEN 1
        WHEN 'TIER 2 — Likely candidate (review with owner)' THEN 2
        WHEN 'TIER 3 — Monitor (check in 90 days)'          THEN 3
        ELSE 4
    END;
