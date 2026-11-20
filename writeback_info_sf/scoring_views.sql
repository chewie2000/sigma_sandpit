-- =============================================================================
-- scoring_views.sql (Snowflake)
--
-- Single source of truth for the archival scoring model. Replaces the two
-- hand-copied CASE blocks that used to live in archival_scoring.sql.
--
--   SIGDS_SCORING_CONFIG       one row of weights / thresholds -- tune with UPDATE
--   V_SIGDS_ACCESS_SUMMARY     SIGDS_ACCESS_LOG rolled up per table
--   V_SIGDS_ARCHIVAL_SCORE     one row per SIGDS table: components, score, tier
--   V_SIGDS_TIER_SUMMARY       tier roll-up: counts + reclaimable GB
--
-- Run create_sigds_workbook_map.sql first. Idempotent -- safe to re-run (the
-- config row is only inserted if missing, so your tuning survives a re-run).
--
-- Before running (or use `python populate_sigds_workbook_map.py --init`, which
-- substitutes these for you), replace <YOUR_DATABASE> and <YOUR_SCHEMA>.
-- =============================================================================

USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;


-- -----------------------------------------------------------------------------
-- Scoring configuration. Column defaults ARE the shipped model; to tune:
--   UPDATE SIGDS_SCORING_CONFIG SET WAL_DAYS_1 = 540 WHERE CONFIG_ID = 1;
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS SIGDS_SCORING_CONFIG (
  CONFIG_ID                NUMBER        DEFAULT 1,

  -- 1. Status (max 30): the strongest applicable state wins
  STATUS_PTS_ORPHANED      NUMBER        DEFAULT 30 COMMENT 'SIGDS data table no longer exists',
  STATUS_PTS_DELETED       NUMBER        DEFAULT 25 COMMENT 'WAL table gone, data table still present',
  STATUS_PTS_ARCHIVED      NUMBER        DEFAULT 20 COMMENT 'Workbook archived in Sigma',
  STATUS_PTS_NOT_IN_API    NUMBER        DEFAULT 15 COMMENT 'Workbook not returned by the Sigma API (deleted / unsaved / not visible)',

  -- 2. WAL edit recency (max 25): days since last WAL write; NULL counts as oldest band
  WAL_DAYS_1 NUMBER DEFAULT 365, WAL_PTS_1 NUMBER DEFAULT 25,
  WAL_DAYS_2 NUMBER DEFAULT 180, WAL_PTS_2 NUMBER DEFAULT 18,
  WAL_DAYS_3 NUMBER DEFAULT 90,  WAL_PTS_3 NUMBER DEFAULT 10,
  WAL_DAYS_4 NUMBER DEFAULT 30,  WAL_PTS_4 NUMBER DEFAULT 4,

  -- 3. SIGDS table LAST_ALTERED recency (max 15). NB: LAST_ALTERED tracks DDL, not
  --    user edits -- weak signal; lower these weights if schema changes are common.
  SIGDS_DAYS_1 NUMBER DEFAULT 365, SIGDS_PTS_1 NUMBER DEFAULT 15,
  SIGDS_DAYS_2 NUMBER DEFAULT 180, SIGDS_PTS_2 NUMBER DEFAULT 10,
  SIGDS_DAYS_3 NUMBER DEFAULT 90,  SIGDS_PTS_3 NUMBER DEFAULT 5,

  -- 4. Edit volume, WAL_MAX_EDIT_NUM (max 10). Calibrate to your org's volumes:
  --    in sandboxes nearly everything is <= 10 edits.
  EDITS_PTS_ZERO NUMBER DEFAULT 10,
  EDITS_LE_1 NUMBER DEFAULT 10,  EDITS_PTS_1 NUMBER DEFAULT 8,
  EDITS_LE_2 NUMBER DEFAULT 50,  EDITS_PTS_2 NUMBER DEFAULT 5,
  EDITS_LE_3 NUMBER DEFAULT 200, EDITS_PTS_3 NUMBER DEFAULT 2,

  -- 5. Legacy SIGDS_WAL_<uuid> naming (max 10)
  LEGACY_ACTIVE_DAYS       NUMBER        DEFAULT 180,
  LEGACY_PTS_ACTIVE        NUMBER        DEFAULT 10,
  LEGACY_PTS_INACTIVE      NUMBER        DEFAULT 5,

  -- 6. Storage worth reclaiming (max 10), on SIGDS + WAL (+ Time Travel/Fail-safe if known)
  STORAGE_BYTES_1 NUMBER DEFAULT 1073741824, STORAGE_PTS_1 NUMBER DEFAULT 10,
  STORAGE_BYTES_2 NUMBER DEFAULT 104857600,  STORAGE_PTS_2 NUMBER DEFAULT 7,
  STORAGE_BYTES_3 NUMBER DEFAULT 10485760,   STORAGE_PTS_3 NUMBER DEFAULT 4,
  STORAGE_PTS_ELSE NUMBER DEFAULT 1,

  -- Penalties (subtracted)
  PENALTY_TAGGED_VERSION   NUMBER        DEFAULT 15 COMMENT 'Prod/QA tagged versions are riskier to archive',
  ACCESS_RECENT_DAYS       NUMBER        DEFAULT 90,
  PENALTY_ACCESS_RECENT    NUMBER        DEFAULT 40 COMMENT 'A workbook read/wrote this table within ACCESS_RECENT_DAYS',
  ACCESS_OLDER_DAYS        NUMBER        DEFAULT 365,
  PENALTY_ACCESS_OLDER     NUMBER        DEFAULT 15 COMMENT 'Last access older than ACCESS_RECENT_DAYS but within ACCESS_OLDER_DAYS',

  -- Tier cut-offs (score >=)
  TIER1_MIN                NUMBER        DEFAULT 75,
  TIER2_MIN                NUMBER        DEFAULT 50,
  TIER3_MIN                NUMBER        DEFAULT 25,

  -- Safety cap: until the access log covers at least this many days, nothing can
  -- reach TIER 1 ("quarantine now") -- a write-only score cannot see readers.
  MIN_ACCESS_COVERAGE_DAYS NUMBER        DEFAULT 90
);

INSERT INTO SIGDS_SCORING_CONFIG (CONFIG_ID)
SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM SIGDS_SCORING_CONFIG WHERE CONFIG_ID = 1);


-- -----------------------------------------------------------------------------
-- Access log rolled up per writeback table. A WAL table's own accesses are
-- attributed to its SIGDS table only for DS_ID-named WALs (1:1); legacy
-- SIGDS_WAL_<uuid> tables are shared by several SIGDS tables, so attributing
-- their accesses would smear one table's activity across the others.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SIGDS_ACCESS_SUMMARY AS
WITH names AS (
    SELECT UPPER(SIGDS_TABLE) AS OBJ, UPPER(SCAN_SCHEMA) AS SCH, SCAN_SCHEMA, SIGDS_TABLE
    FROM SIGDS_WORKBOOK_MAP
    UNION ALL
    SELECT UPPER(REPLACE(SPLIT_PART(WAL_TABLE_FQN, '.', 3), '"', '')),
           UPPER(SCAN_SCHEMA), SCAN_SCHEMA, SIGDS_TABLE
    FROM SIGDS_WORKBOOK_MAP
    WHERE WAL_TABLE_FQN IS NOT NULL AND COALESCE(IS_LEGACY_WAL, FALSE) = FALSE
)
SELECT
    n.SCAN_SCHEMA,
    n.SIGDS_TABLE,
    MAX(IFF(l.ACCESS_KIND = 'READ',   l.START_TIME, NULL))   AS ACCESS_LAST_READ_AT,
    MAX(IFF(l.ACCESS_KIND = 'INSERT', l.START_TIME, NULL))   AS ACCESS_LAST_WRITE_AT,
    COUNT(DISTINCT IFF(l.ACCESS_KIND = 'READ',   l.QUERY_ID, NULL)) AS ACCESS_READ_COUNT,
    COUNT(DISTINCT IFF(l.ACCESS_KIND = 'INSERT', l.QUERY_ID, NULL)) AS ACCESS_WRITE_COUNT,
    COUNT(DISTINCT l.SIGMA_USER_EMAIL)                       AS ACCESS_DISTINCT_USERS,
    COUNT(DISTINCT l.SIGMA_SOURCE_URL)                       AS ACCESS_DISTINCT_SOURCES,
    MAX_BY(l.SIGMA_SOURCE_URL,  l.START_TIME)                AS ACCESS_LAST_SOURCE_URL,
    MAX_BY(l.SIGMA_USER_EMAIL,  l.START_TIME)                AS ACCESS_LAST_USER_EMAIL
FROM names n
JOIN SIGDS_ACCESS_LOG l
  ON l.OBJECT_NAME = n.OBJ AND l.OBJECT_SCHEMA = n.SCH
GROUP BY n.SCAN_SCHEMA, n.SIGDS_TABLE;


-- -----------------------------------------------------------------------------
-- The scoring model. Total 0-100, higher = stronger archival candidate.
--
-- Positive components (30 + 25 + 15 + 10 + 10 + 10 = 100) come from the map.
-- Penalties subtract: tagged versions, and -- the important one -- ground-truth
-- access from query history. Absence of access is NEVER scored as evidence of
-- abandonment on its own; it simply removes a protection. ACCESS_EVIDENCE says
-- which of those situations a row is in:
--   RECENT          read/written within ACCESS_RECENT_DAYS      (big penalty)
--   OLDER           last access within ACCESS_OLDER_DAYS         (small penalty)
--   NONE_IN_WINDOW  loaded, but nothing seen in that window      (no penalty)
--   NOT_LOADED      access log never loaded (ACCESS_HISTORY not readable, or
--                   loader not run) -- score reflects WRITES ONLY; treat with care
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SIGDS_ARCHIVAL_SCORE AS
WITH c AS (
    SELECT * FROM SIGDS_SCORING_CONFIG WHERE CONFIG_ID = 1
),
cov AS (
    -- Days the access log is trustworthy for = oldest successfully-loaded window.
    SELECT COALESCE(DATEDIFF('day', MIN(WINDOW_START),
                             CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ), 0)
               AS ACCESS_COVERAGE_DAYS
    FROM SIGDS_ACCESS_LOAD_RUNS WHERE STATUS = 'OK'
),
base AS (
    SELECT
        m.SIGDS_TABLE, m.SCAN_SCHEMA, m.WORKBOOK_ID, m.WORKBOOK_NAME, m.WORKBOOK_PATH, m.OBJECT_TYPE,
        m.API_OWNER_ID, m.API_OWNER_FIRST_NAME, m.API_OWNER_LAST_NAME,
        COALESCE(m.API_IS_ARCHIVED, FALSE)  AS API_IS_ARCHIVED,
        m.API_IS_ARCHIVED                   AS API_IS_ARCHIVED_RAW,
        COALESCE(m.API_NOT_FOUND, FALSE)    AS API_NOT_FOUND,
        COALESCE(m.IS_ORPHANED, FALSE)      AS IS_ORPHANED,
        COALESCE(m.IS_DELETED, FALSE)       AS IS_DELETED,
        COALESCE(m.IS_LEGACY_WAL, FALSE)    AS IS_LEGACY_WAL,
        COALESCE(m.IS_TAGGED_VERSION, FALSE) AS IS_TAGGED_VERSION,
        m.VERSION_TAG_NAME, m.WAL_MAX_EDIT_NUM, m.WAL_LAST_EDIT_AT, m.WAL_LAST_EDIT_BY,
        m.WAL_WORKBOOK_URL, m.WAL_TABLE_FQN,
        IFF(m.WAL_LAST_EDIT_AT IS NULL, NULL,
            DATEDIFF('day', m.WAL_LAST_EDIT_AT, CURRENT_TIMESTAMP()))         AS DAYS_SINCE_LAST_EDIT,
        IFF(m.SIGDS_TABLE_LAST_MODIFIED IS NULL, NULL,
            DATEDIFF('day', m.SIGDS_TABLE_LAST_MODIFIED, CURRENT_TIMESTAMP())) AS DAYS_SINCE_SIGDS_MODIFIED,
        COALESCE(m.SIGDS_TABLE_TOTAL_BYTES, m.SIGDS_TABLE_SIZE_BYTES, 0)
          + COALESCE(m.WAL_TABLE_SIZE_BYTES, 0)                               AS TOTAL_BYTES,
        a.ACCESS_LAST_READ_AT, a.ACCESS_LAST_WRITE_AT,
        COALESCE(a.ACCESS_READ_COUNT, 0)       AS ACCESS_READ_COUNT,
        COALESCE(a.ACCESS_WRITE_COUNT, 0)      AS ACCESS_WRITE_COUNT,
        COALESCE(a.ACCESS_DISTINCT_USERS, 0)   AS ACCESS_DISTINCT_USERS,
        a.ACCESS_LAST_SOURCE_URL, a.ACCESS_LAST_USER_EMAIL,
        IFF(a.ACCESS_LAST_READ_AT IS NULL AND a.ACCESS_LAST_WRITE_AT IS NULL, NULL,
            DATEDIFF('day',
                GREATEST(COALESCE(a.ACCESS_LAST_READ_AT,  '1900-01-01'::TIMESTAMP_NTZ),
                         COALESCE(a.ACCESS_LAST_WRITE_AT, '1900-01-01'::TIMESTAMP_NTZ)),
                CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ)) AS DAYS_SINCE_ACCESS,
        cov.ACCESS_COVERAGE_DAYS
    FROM SIGDS_WORKBOOK_MAP m
    LEFT JOIN V_SIGDS_ACCESS_SUMMARY a
           ON a.SCAN_SCHEMA = m.SCAN_SCHEMA AND a.SIGDS_TABLE = m.SIGDS_TABLE
    CROSS JOIN cov
),
scored AS (
    SELECT
        b.*,
        ROUND(b.TOTAL_BYTES / 1048576.0, 2) AS TOTAL_SIZE_MB,

        CASE
            WHEN b.IS_ORPHANED                                    THEN c.STATUS_PTS_ORPHANED
            WHEN b.IS_DELETED                                     THEN c.STATUS_PTS_DELETED
            WHEN b.API_IS_ARCHIVED                                THEN c.STATUS_PTS_ARCHIVED
            WHEN b.API_NOT_FOUND
              OR (b.WORKBOOK_NAME IS NULL AND b.API_IS_ARCHIVED_RAW IS NULL)
                                                                  THEN c.STATUS_PTS_NOT_IN_API
            ELSE 0
        END AS SCORE_STATUS,

        CASE
            WHEN b.DAYS_SINCE_LAST_EDIT IS NULL OR b.DAYS_SINCE_LAST_EDIT > c.WAL_DAYS_1 THEN c.WAL_PTS_1
            WHEN b.DAYS_SINCE_LAST_EDIT > c.WAL_DAYS_2 THEN c.WAL_PTS_2
            WHEN b.DAYS_SINCE_LAST_EDIT > c.WAL_DAYS_3 THEN c.WAL_PTS_3
            WHEN b.DAYS_SINCE_LAST_EDIT > c.WAL_DAYS_4 THEN c.WAL_PTS_4
            ELSE 0
        END AS SCORE_WAL_RECENCY,

        CASE
            WHEN b.DAYS_SINCE_SIGDS_MODIFIED IS NULL OR b.DAYS_SINCE_SIGDS_MODIFIED > c.SIGDS_DAYS_1 THEN c.SIGDS_PTS_1
            WHEN b.DAYS_SINCE_SIGDS_MODIFIED > c.SIGDS_DAYS_2 THEN c.SIGDS_PTS_2
            WHEN b.DAYS_SINCE_SIGDS_MODIFIED > c.SIGDS_DAYS_3 THEN c.SIGDS_PTS_3
            ELSE 0
        END AS SCORE_SIGDS_RECENCY,

        CASE
            WHEN b.WAL_MAX_EDIT_NUM IS NULL OR b.WAL_MAX_EDIT_NUM = 0 THEN c.EDITS_PTS_ZERO
            WHEN b.WAL_MAX_EDIT_NUM <= c.EDITS_LE_1 THEN c.EDITS_PTS_1
            WHEN b.WAL_MAX_EDIT_NUM <= c.EDITS_LE_2 THEN c.EDITS_PTS_2
            WHEN b.WAL_MAX_EDIT_NUM <= c.EDITS_LE_3 THEN c.EDITS_PTS_3
            ELSE 0
        END AS SCORE_EDIT_VOLUME,

        CASE
            WHEN b.IS_LEGACY_WAL AND b.DAYS_SINCE_LAST_EDIT IS NOT NULL
             AND b.DAYS_SINCE_LAST_EDIT < c.LEGACY_ACTIVE_DAYS THEN c.LEGACY_PTS_ACTIVE
            WHEN b.IS_LEGACY_WAL THEN c.LEGACY_PTS_INACTIVE
            ELSE 0
        END AS SCORE_LEGACY_WAL,

        CASE
            WHEN b.TOTAL_BYTES > c.STORAGE_BYTES_1 THEN c.STORAGE_PTS_1
            WHEN b.TOTAL_BYTES > c.STORAGE_BYTES_2 THEN c.STORAGE_PTS_2
            WHEN b.TOTAL_BYTES > c.STORAGE_BYTES_3 THEN c.STORAGE_PTS_3
            ELSE c.STORAGE_PTS_ELSE
        END AS SCORE_STORAGE,

        IFF(b.IS_TAGGED_VERSION, -c.PENALTY_TAGGED_VERSION, 0) AS PENALTY_TAGGED_VERSION,

        CASE
            WHEN b.ACCESS_COVERAGE_DAYS = 0                                   THEN 'NOT_LOADED'
            WHEN b.DAYS_SINCE_ACCESS IS NOT NULL
             AND b.DAYS_SINCE_ACCESS <= c.ACCESS_RECENT_DAYS                  THEN 'RECENT'
            WHEN b.DAYS_SINCE_ACCESS IS NOT NULL
             AND b.DAYS_SINCE_ACCESS <= c.ACCESS_OLDER_DAYS                   THEN 'OLDER'
            ELSE 'NONE_IN_WINDOW'
        END AS ACCESS_EVIDENCE
    FROM base b CROSS JOIN c
),
totaled AS (
    SELECT
        s.*,
        CASE s.ACCESS_EVIDENCE
            WHEN 'RECENT' THEN -c.PENALTY_ACCESS_RECENT
            WHEN 'OLDER'  THEN -c.PENALTY_ACCESS_OLDER
            ELSE 0
        END AS PENALTY_ACCESS,
        GREATEST(0, LEAST(100,
              s.SCORE_STATUS + s.SCORE_WAL_RECENCY + s.SCORE_SIGDS_RECENCY
            + s.SCORE_EDIT_VOLUME + s.SCORE_LEGACY_WAL + s.SCORE_STORAGE
            + s.PENALTY_TAGGED_VERSION
            + CASE s.ACCESS_EVIDENCE
                  WHEN 'RECENT' THEN -c.PENALTY_ACCESS_RECENT
                  WHEN 'OLDER'  THEN -c.PENALTY_ACCESS_OLDER
                  ELSE 0 END
        )) AS TOTAL_SCORE,
        c.TIER1_MIN, c.TIER2_MIN, c.TIER3_MIN, c.MIN_ACCESS_COVERAGE_DAYS
    FROM scored s CROSS JOIN c
),
tiered AS (
    SELECT
        t.*,
        CASE WHEN t.TOTAL_SCORE >= t.TIER1_MIN THEN 1
             WHEN t.TOTAL_SCORE >= t.TIER2_MIN THEN 2
             WHEN t.TOTAL_SCORE >= t.TIER3_MIN THEN 3
             ELSE 4 END AS RAW_TIER_CODE
    FROM totaled t
)
SELECT
    SIGDS_TABLE, SCAN_SCHEMA, WORKBOOK_ID, WORKBOOK_NAME, WORKBOOK_PATH, OBJECT_TYPE,
    API_OWNER_ID,
    TRIM(COALESCE(API_OWNER_FIRST_NAME, '') || ' ' || COALESCE(API_OWNER_LAST_NAME, '')) AS OWNER_FULL_NAME,

    -- Status flags
    API_IS_ARCHIVED, API_NOT_FOUND, IS_ORPHANED, IS_DELETED, IS_LEGACY_WAL,
    IS_TAGGED_VERSION, VERSION_TAG_NAME,

    -- Activity: writes (WAL) and ground-truth access (query history)
    DAYS_SINCE_LAST_EDIT, DAYS_SINCE_SIGDS_MODIFIED, WAL_MAX_EDIT_NUM, WAL_LAST_EDIT_BY,
    ACCESS_EVIDENCE, DAYS_SINCE_ACCESS, ACCESS_LAST_READ_AT, ACCESS_LAST_WRITE_AT,
    ACCESS_READ_COUNT, ACCESS_WRITE_COUNT, ACCESS_DISTINCT_USERS,
    ACCESS_LAST_USER_EMAIL, ACCESS_LAST_SOURCE_URL, ACCESS_COVERAGE_DAYS,

    TOTAL_BYTES, TOTAL_SIZE_MB,

    -- Component scores (exposed for transparency and threshold tuning)
    SCORE_STATUS, SCORE_WAL_RECENCY, SCORE_SIGDS_RECENCY, SCORE_EDIT_VOLUME,
    SCORE_LEGACY_WAL, SCORE_STORAGE, PENALTY_TAGGED_VERSION, PENALTY_ACCESS,
    TOTAL_SCORE,

    -- Tier. A would-be TIER 1 is held at TIER 2 until the access log has enough
    -- history to have seen readers -- except IS_ORPHANED rows, whose data table is
    -- already gone so there is nothing left to break.
    IFF(RAW_TIER_CODE = 1 AND NOT IS_ORPHANED AND ACCESS_COVERAGE_DAYS < MIN_ACCESS_COVERAGE_DAYS,
        2, RAW_TIER_CODE) AS TIER_CODE,
    (RAW_TIER_CODE = 1 AND NOT IS_ORPHANED AND ACCESS_COVERAGE_DAYS < MIN_ACCESS_COVERAGE_DAYS)
        AS TIER_CAPPED,
    CASE IFF(RAW_TIER_CODE = 1 AND NOT IS_ORPHANED AND ACCESS_COVERAGE_DAYS < MIN_ACCESS_COVERAGE_DAYS,
             2, RAW_TIER_CODE)
        WHEN 1 THEN 'TIER 1 — Strong candidate (quarantine now)'
        WHEN 2 THEN 'TIER 2 — Likely candidate (review with owner)'
        WHEN 3 THEN 'TIER 3 — Monitor (check in 90 days)'
        ELSE        'TIER 4 — Keep (active or protected)'
    END AS ARCHIVAL_TIER,
    CASE
        WHEN RAW_TIER_CODE = 1 AND NOT IS_ORPHANED AND ACCESS_COVERAGE_DAYS < MIN_ACCESS_COVERAGE_DAYS
            THEN 'Held at TIER 2: access history covers ' || ACCESS_COVERAGE_DAYS
                 || ' day(s), need ' || MIN_ACCESS_COVERAGE_DAYS || '. Readers may be unseen.'
        WHEN ACCESS_EVIDENCE = 'NOT_LOADED'
            THEN 'Score reflects writes only — access history not loaded.'
        WHEN ACCESS_EVIDENCE = 'RECENT'
            THEN 'In recent use per query history — do not archive.'
        ELSE NULL
    END AS SCORE_NOTE,

    WAL_WORKBOOK_URL, WAL_TABLE_FQN
FROM tiered;


-- -----------------------------------------------------------------------------
-- Tier roll-up: scale of the clean-up opportunity at a glance.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SIGDS_TIER_SUMMARY AS
SELECT
    TIER_CODE,
    ARCHIVAL_TIER,
    COUNT(*)                                        AS RECORD_COUNT,
    COUNT_IF(TIER_CAPPED)                           AS HELD_BY_COVERAGE_CAP,
    COUNT_IF(ACCESS_EVIDENCE = 'NOT_LOADED')        AS WRITE_ONLY_SCORED,
    ROUND(SUM(TOTAL_BYTES) / 1073741824.0, 3)       AS TOTAL_SIZE_GB,
    ROUND(AVG(TOTAL_SCORE), 1)                      AS AVG_SCORE,
    MIN(DAYS_SINCE_LAST_EDIT)                       AS MIN_DAYS_SINCE_LAST_EDIT,
    MAX(DAYS_SINCE_LAST_EDIT)                       AS MAX_DAYS_SINCE_LAST_EDIT
FROM V_SIGDS_ARCHIVAL_SCORE
GROUP BY TIER_CODE, ARCHIVAL_TIER;
