-- =============================================================================
-- archival_scoring.sql
--
-- Before running, replace <YOUR_CATALOG> and <YOUR_SCHEMA> below with the
-- Unity Catalog catalog and schema where SIGDS_WORKBOOK_MAP resides.
-- =============================================================================

USE CATALOG <YOUR_CATALOG>;
USE SCHEMA  <YOUR_SCHEMA>;

-- =============================================================================
-- WHAT THIS MODEL ANSWERS — and what it deliberately does NOT fold in
-- -----------------------------------------------------------------------------
-- A single score used to blend three different questions, which produced
-- counter-intuitive rankings. They are now separated:
--
--   1. ARCHIVABILITY_SCORE (0–100)  — "how confident are we this writeback is
--      dead and its leftover data table can be reclaimed?"  This is the score.
--   2. SIGDS_TABLE_SIZE_MB           — "how much would we reclaim?"  A
--      *priority* axis, shown as a column and used only as a sort tie-breaker.
--      Size does NOT make a table a better archival candidate, so it no longer
--      adds to the score (a large, actively-used table must not outrank a tiny
--      dead one).
--   3. MIGRATION_PRIORITY            — legacy (pre-MultiWAL) tables that should
--      be *migrated*, not archived.  Shown as a flag, not scored — an actively
--      used legacy table is not an archival candidate.
--
-- ORPHANED records (the SIGDS data table is already gone) are handled by a
-- SEPARATE query at the bottom: there is nothing left to quarantine, so they do
-- not belong in the archival tiers — the action is to remove the dangling WAL
-- table / stale map row.
--
-- ARCHIVABILITY_SCORE components (sum = 100, higher = stronger candidate)
-- -----------------------------------------------------------------------------
-- Status (0–40)
--   IS_DELETED = TRUE              → 40  WAL table gone but the data table still
--                                        exists = the clean leftover-to-reclaim
--                                        case, the strongest signal here.
--   API_IS_ARCHIVED = TRUE         → 15  Archived in Sigma. NOTE: archiving is
--                                        REVERSIBLE — a workbook can be
--                                        un-archived and will need its input
--                                        tables again. Deliberately low; retain
--                                        unless the workbook is permanently
--                                        deleted.
--   Workbook absent from API       → 10  WORKBOOK_ID present but not returned by
--                                        the API. LOW CONFIDENCE: this can also
--                                        be an enrichment gap (permissions, a
--                                        data-model id, a transient API miss, or
--                                        enrichment never having run), not a
--                                        real deletion. Verify before acting.
--                                        (A definitive fix needs an
--                                        "enrichment attempted" flag on the
--                                        populate side.)
-- WAL edit recency (0–45)   Days since last writeback. The primary, directional
--                           abandonment signal, so it carries the most weight.
--                           NULL treated as > 365 (no evidence of use).
-- SIGDS table recency (0–15) Days since last data-table write. SECONDARY and
--                           noisy: SIGDS_TABLE_LAST_MODIFIED moves on OPTIMIZE /
--                           VACUUM, so Delta maintenance can make a dead table
--                           look active and lower its score — cross-check WAL
--                           recency, do not trust this alone.
-- Risk penalty: IS_TAGGED_VERSION = TRUE → −15 (tagged Prod/QA versions are
--               high-risk to archive; floor the total at 0).
--
-- NOT scored (by design): edit volume. WAL_MAX_EDIT_NUM is a lifetime cumulative
-- counter, not a recency measure — a low count is "new & active" as often as
-- "never used", and a high count on a cold table would wrongly read as "keep".
-- It is shown as a context column for the human reviewer but does not affect the
-- score; abandonment is captured directionally by WAL edit recency instead.
--
-- CONFIDENCE TIERS  (>=75 TIER 1 quarantine now · 50–74 TIER 2 review w/ owner ·
--                    25–49 TIER 3 monitor · <25 TIER 4 keep)
--
-- !! IMPORTANT — these weights/tiers are subjective starting points for
-- investigation, NOT a directive to delete. Incorrectly removing a SIGDS or WAL
-- table breaks the related Sigma content and may be unrecoverable.
--
-- SAFE DELETION — always: (1) move to a *_quarantine schema with ALTER TABLE
-- RENAME TO (never DROP directly); (2) monitor >= 30 days and confirm with the
-- workbook owner; (3) only DROP from quarantine once the safe period passes.
-- =============================================================================


-- =============================================================================
-- VIEW — single source of truth for the scoring.
-- Both the candidate list and the tier rollup read this view, so the logic can
-- never drift between them (previously it was duplicated and could diverge).
-- Derived age/size fields are computed in an inner CTE because Databricks SQL
-- does not allow `SELECT *, <expr>` in a CTE; the scoring CASE expressions then
-- reference them by name.
-- =============================================================================

CREATE OR REPLACE VIEW SIGDS_ARCHIVAL_SCORED AS
WITH base AS (
    SELECT
        SIGDS_TABLE,
        SCAN_SCHEMA,
        WAL_TABLE_FQN,
        WORKBOOK_ID,
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
),
scored AS (
    SELECT
        SIGDS_TABLE,
        SCAN_SCHEMA,
        WAL_TABLE_FQN,
        WORKBOOK_ID,
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
        SIGDS_TABLE_SIZE_MB,
        WAL_WORKBOOK_URL,
        DAYS_SINCE_LAST_EDIT,
        DAYS_SINCE_SIGDS_MODIFIED,

        -- Status (0–40). Every branch is guarded with IS_ORPHANED = FALSE:
        -- IS_DELETED only means the WAL table vanished — it does NOT imply the
        -- data table still exists (if both are gone, IS_DELETED and IS_ORPHANED
        -- are BOTH true). Orphaned rows have nothing to reclaim and are reported
        -- by the separate dangling-cleanup query, so they must not earn a status
        -- score here. NULL flags are coalesced to FALSE so an unset flag never
        -- silently zeroes — or wrongly awards — a signal.
        CASE
            WHEN COALESCE(IS_DELETED,  FALSE) = TRUE
             AND COALESCE(IS_ORPHANED, FALSE) = FALSE
                THEN 40   -- WAL gone AND data table still present — clean leftover to reclaim
            WHEN COALESCE(API_IS_ARCHIVED, FALSE) = TRUE
             AND COALESCE(IS_DELETED,      FALSE) = FALSE
             AND COALESCE(IS_ORPHANED,     FALSE) = FALSE
                THEN 15   -- archived in Sigma (REVERSIBLE) — retain unless permanently deleted
            WHEN WORKBOOK_NAME   IS NULL
             AND API_IS_ARCHIVED IS NULL
             AND WORKBOOK_ID     IS NOT NULL
             AND COALESCE(IS_DELETED,  FALSE) = FALSE
             AND COALESCE(IS_ORPHANED, FALSE) = FALSE
                THEN 10   -- absent from API — LOW confidence (may be an enrichment gap)
            ELSE 0
        END                                                     AS SCORE_STATUS,

        -- WAL edit recency (0–45) — the primary, directional abandonment signal,
        -- so it carries the most weight. (Edit *volume* is deliberately NOT scored:
        -- WAL_MAX_EDIT_NUM is a lifetime counter, not a recency measure — a low
        -- count means "new & active" as often as "never used", and a high count
        -- on a cold table would wrongly read as "keep". It's kept as a context
        -- column only.)
        CASE
            WHEN DAYS_SINCE_LAST_EDIT IS NULL
              OR DAYS_SINCE_LAST_EDIT > 365   THEN 45
            WHEN DAYS_SINCE_LAST_EDIT > 180   THEN 32
            WHEN DAYS_SINCE_LAST_EDIT > 90    THEN 18
            WHEN DAYS_SINCE_LAST_EDIT > 30    THEN 6
            ELSE 0
        END                                                     AS SCORE_WAL_RECENCY,

        -- SIGDS table recency (0–15) — secondary, noisy (see header re OPTIMIZE/VACUUM).
        CASE
            WHEN DAYS_SINCE_SIGDS_MODIFIED IS NULL
              OR DAYS_SINCE_SIGDS_MODIFIED > 365   THEN 15
            WHEN DAYS_SINCE_SIGDS_MODIFIED > 180   THEN 10
            WHEN DAYS_SINCE_SIGDS_MODIFIED > 90    THEN 5
            ELSE 0
        END                                                     AS SCORE_SIGDS_RECENCY,

        -- Risk penalty (0 or −15).
        CASE
            WHEN COALESCE(IS_TAGGED_VERSION, FALSE) = TRUE THEN -15
            ELSE 0
        END                                                     AS PENALTY_TAGGED_VERSION,

        -- Migration priority — legacy (pre-MultiWAL) tables. A FLAG, not scored:
        -- an actively-used legacy table should be migrated, not archived.
        CASE
            WHEN COALESCE(IS_LEGACY_WAL, FALSE) = TRUE
             AND DAYS_SINCE_LAST_EDIT IS NOT NULL
             AND DAYS_SINCE_LAST_EDIT < 180
                THEN 'URGENT — active legacy WAL (migrate, do not archive)'
            WHEN COALESCE(IS_LEGACY_WAL, FALSE) = TRUE
                THEN 'LOW — inactive legacy WAL'
            ELSE NULL
        END                                                     AS MIGRATION_PRIORITY
    FROM base
)
SELECT
    SIGDS_TABLE,
    SCAN_SCHEMA,
    WAL_TABLE_FQN,
    WORKBOOK_ID,
    WORKBOOK_NAME,
    TRIM(COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, ''))
                                                                AS OWNER_FULL_NAME,
    API_IS_ARCHIVED,
    IS_ORPHANED,
    IS_DELETED,
    IS_LEGACY_WAL,
    IS_TAGGED_VERSION,
    VERSION_TAG_NAME,
    DAYS_SINCE_LAST_EDIT,
    DAYS_SINCE_SIGDS_MODIFIED,
    WAL_MAX_EDIT_NUM,
    SIGDS_TABLE_SIZE_BYTES,
    SIGDS_TABLE_SIZE_MB,
    WAL_WORKBOOK_URL,
    MIGRATION_PRIORITY,

    -- Component scores (exposed for transparency and threshold tuning)
    SCORE_STATUS,
    SCORE_WAL_RECENCY,
    SCORE_SIGDS_RECENCY,
    PENALTY_TAGGED_VERSION,

    GREATEST(0, LEAST(100,
          SCORE_STATUS
        + SCORE_WAL_RECENCY
        + SCORE_SIGDS_RECENCY
        + PENALTY_TAGGED_VERSION
    ))                                                          AS ARCHIVABILITY_SCORE,

    CASE
        WHEN GREATEST(0, LEAST(100, SCORE_STATUS + SCORE_WAL_RECENCY
            + SCORE_SIGDS_RECENCY + PENALTY_TAGGED_VERSION)) >= 75
            THEN 'TIER 1 — Strong candidate (quarantine now)'
        WHEN GREATEST(0, LEAST(100, SCORE_STATUS + SCORE_WAL_RECENCY
            + SCORE_SIGDS_RECENCY + PENALTY_TAGGED_VERSION)) >= 50
            THEN 'TIER 2 — Likely candidate (review with owner)'
        WHEN GREATEST(0, LEAST(100, SCORE_STATUS + SCORE_WAL_RECENCY
            + SCORE_SIGDS_RECENCY + PENALTY_TAGGED_VERSION)) >= 25
            THEN 'TIER 3 — Monitor (check in 90 days)'
        ELSE 'TIER 4 — Keep (active or protected)'
    END                                                         AS ARCHIVAL_TIER
FROM scored;


-- =============================================================================
-- 1. ARCHIVAL CANDIDATES — live records, ranked by archivability.
--    Orphaned records are excluded (handled by query 3). Storage is shown for
--    prioritisation and used only as a sort tie-breaker, not in the score.
-- =============================================================================

SELECT
    ARCHIVAL_TIER,
    ARCHIVABILITY_SCORE,
    SIGDS_TABLE,
    SCAN_SCHEMA,
    WORKBOOK_NAME,
    OWNER_FULL_NAME,
    API_IS_ARCHIVED,
    IS_DELETED,
    IS_TAGGED_VERSION,
    VERSION_TAG_NAME,
    MIGRATION_PRIORITY,
    DAYS_SINCE_LAST_EDIT,
    DAYS_SINCE_SIGDS_MODIFIED,
    WAL_MAX_EDIT_NUM,
    SIGDS_TABLE_SIZE_MB,
    SCORE_STATUS,
    SCORE_WAL_RECENCY,
    SCORE_SIGDS_RECENCY,
    PENALTY_TAGGED_VERSION,
    WAL_WORKBOOK_URL
FROM SIGDS_ARCHIVAL_SCORED
WHERE COALESCE(IS_ORPHANED, FALSE) = FALSE
ORDER BY
    ARCHIVABILITY_SCORE DESC,
    SIGDS_TABLE_SIZE_MB DESC;


-- =============================================================================
-- 2. TIER ROLLUP — scale of the cleanup opportunity (live records only).
--    Reads the same view, so counts always match query 1.
-- =============================================================================

SELECT
    ARCHIVAL_TIER,
    COUNT(*)                                                        AS RECORD_COUNT,
    ROUND(SUM(COALESCE(SIGDS_TABLE_SIZE_BYTES, 0)) / 1073741824.0, 3) AS RECLAIMABLE_GB,
    ROUND(AVG(ARCHIVABILITY_SCORE), 1)                              AS AVG_SCORE,
    MIN(DAYS_SINCE_LAST_EDIT)                                       AS MIN_DAYS_SINCE_LAST_EDIT,
    MAX(DAYS_SINCE_LAST_EDIT)                                       AS MAX_DAYS_SINCE_LAST_EDIT
FROM SIGDS_ARCHIVAL_SCORED
WHERE COALESCE(IS_ORPHANED, FALSE) = FALSE
GROUP BY ARCHIVAL_TIER
ORDER BY
    CASE ARCHIVAL_TIER
        WHEN 'TIER 1 — Strong candidate (quarantine now)'    THEN 1
        WHEN 'TIER 2 — Likely candidate (review with owner)' THEN 2
        WHEN 'TIER 3 — Monitor (check in 90 days)'           THEN 3
        ELSE 4
    END;


-- =============================================================================
-- 3. DANGLING WAL / STALE-RECORD CLEANUP — orphaned records.
--    The SIGDS data table is already gone, so there is nothing to quarantine and
--    no storage to reclaim; the action is to remove the leftover WAL table (if
--    still present) and the stale map row. These are NOT archival candidates and
--    are deliberately kept out of the scored tiers above.
-- =============================================================================

SELECT
    SIGDS_TABLE,
    SCAN_SCHEMA,
    WAL_TABLE_FQN,
    WORKBOOK_NAME,
    OWNER_FULL_NAME,
    DAYS_SINCE_LAST_EDIT,
    WAL_MAX_EDIT_NUM,
    WAL_WORKBOOK_URL
FROM SIGDS_ARCHIVAL_SCORED
WHERE COALESCE(IS_ORPHANED, FALSE) = TRUE
ORDER BY DAYS_SINCE_LAST_EDIT DESC NULLS LAST;
