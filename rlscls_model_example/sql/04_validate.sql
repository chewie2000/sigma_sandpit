-- 04_validate.sql — the expected-results oracle.
--
-- Computes, in plain SQL, exactly what each persona SHOULD see, and asserts the
-- six acceptance criteria. This mirrors the Sigma formula
--     Combined RLS = [Office RLS] OR [Company RLS] OR [Site RLS]
-- where each dimension is
--     CurrentUserInTeam(<teams granted this value>) OR <my email in the granted emails>
--
-- Run with:  psql -v ON_ERROR_STOP=1 -v schema=<schema> -f 04_validate.sql

\set ON_ERROR_STOP on
SET search_path TO :"schema";

-- ---------------------------------------------------------------------------
-- The oracle. Takes a team list + an email — the same two inputs Sigma resolves
-- via CurrentUserInTeam() and CurrentUserEmail() — and returns visible rows.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rls_visible(p_teams text[], p_email text)
RETURNS TABLE (shipment_id bigint, company text, site text, office text, revenue_eur numeric)
LANGUAGE sql STABLE AS $$
    SELECT s.shipment_id, s.company, s.site, s.office, s.revenue_eur
    FROM rls_shipments s
    WHERE EXISTS (
        SELECT 1
        FROM rls_entity_grants g
        WHERE (   (g.principal_type = 'team' AND g.principal_id = ANY (p_teams))
               OR (g.principal_type = 'user' AND g.principal_id = p_email))
          AND (   (g.entity_type = 'office'  AND g.entity_value = s.office)
               OR (g.entity_type = 'company' AND g.entity_value = s.company)
               OR (g.entity_type = 'site'    AND g.entity_value = s.site))
    );
$$;

CREATE OR REPLACE FUNCTION rls_visible_count(p_teams text[], p_email text)
RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT count(*) FROM rls_visible(p_teams, p_email);
$$;

-- Resolve each persona's real inputs from the harness tables.
CREATE OR REPLACE VIEW rls_persona_inputs AS
SELECT p.persona,
       p.email,
       COALESCE(ARRAY(SELECT t.team_name
                        FROM rls_test_persona_teams t
                       WHERE t.persona = p.persona
                    ORDER BY t.team_name), ARRAY[]::text[]) AS teams
FROM rls_test_personas p;

-- Per-persona, per-row visibility with the branch that granted it.
CREATE OR REPLACE VIEW rls_expected_visibility AS
WITH persona_grant AS (
    SELECT DISTINCT i.persona, g.entity_type, g.entity_value
    FROM rls_persona_inputs i
    JOIN rls_entity_grants g
      ON (g.principal_type = 'user' AND g.principal_id = i.email)
      OR (g.principal_type = 'team' AND g.principal_id = ANY (i.teams))
)
SELECT
    i.persona,
    s.shipment_id,
    s.company, s.site, s.office, s.revenue_eur,
    EXISTS (SELECT 1 FROM persona_grant pg
             WHERE pg.persona = i.persona AND pg.entity_type = 'office'
               AND pg.entity_value = s.office)  AS office_rls,
    EXISTS (SELECT 1 FROM persona_grant pg
             WHERE pg.persona = i.persona AND pg.entity_type = 'company'
               AND pg.entity_value = s.company) AS company_rls,
    EXISTS (SELECT 1 FROM persona_grant pg
             WHERE pg.persona = i.persona AND pg.entity_type = 'site'
               AND pg.entity_value = s.site)    AS site_rls
FROM rls_persona_inputs i
CROSS JOIN rls_shipments s;

\echo ''
\echo '=== DATASET SHAPE ==='
SELECT (SELECT count(*) FROM rls_shipments)      AS shipments,
       (SELECT count(*) FROM rls_companies)      AS companies,
       (SELECT count(*) FROM rls_sites)          AS sites,
       (SELECT count(*) FROM rls_offices)        AS offices,
       (SELECT count(*) FROM rls_entity_grants)  AS grant_rows;

\echo ''
\echo '=== EXPECTED ROWS PER PERSONA ==='
SELECT persona,
       count(*) FILTER (WHERE office_rls OR company_rls OR site_rls) AS visible_rows,
       count(*)                                                      AS total_rows,
       ROUND(100.0 * count(*) FILTER (WHERE office_rls OR company_rls OR site_rls)
             / NULLIF(count(*), 0), 1)                               AS pct_visible,
       count(*) FILTER (WHERE office_rls)                            AS via_office,
       count(*) FILTER (WHERE company_rls)                           AS via_company,
       count(*) FILTER (WHERE site_rls)                              AS via_site
FROM rls_expected_visibility
GROUP BY persona
ORDER BY persona;

\echo ''
\echo '=== ACCEPTANCE CRITERIA ==='
WITH emea  AS (SELECT rls_visible_count(ARRAY['RLS Demo Ops EMEA'], '') AS n),
     apac  AS (SELECT rls_visible_count(ARRAY['RLS Demo Ops APAC'], '') AS n),
     keyac AS (SELECT rls_visible_count(ARRAY['RLS Demo Key Accounts'], '') AS n),
     u1    AS (SELECT rls_visible_count(
                   ARRAY['RLS Demo Ops EMEA','RLS Demo Ops APAC','RLS Demo Key Accounts'],
                   'mark.oldfield+user1@sigmacomputing.com') AS n),
     u1_teams_only AS (SELECT rls_visible_count(
                   ARRAY['RLS Demo Ops EMEA','RLS Demo Ops APAC','RLS Demo Key Accounts'],
                   '') AS n),
     u2    AS (SELECT rls_visible_count(ARRAY['RLS Demo Observers'],
                   'mark.oldfield+user2@sigmacomputing.com') AS n),
     u2_team_only AS (SELECT rls_visible_count(ARRAY['RLS Demo Observers'], '') AS n),
     nogr  AS (SELECT rls_visible_count(ARRAY[]::text[],
                   'mark.oldfield+user@sigmacomputing.com') AS n),
     london AS (SELECT count(*) AS n FROM rls_shipments WHERE office = 'London'),
     london_seen AS (SELECT count(*) AS n
                     FROM rls_visible(ARRAY['RLS Demo Ops EMEA','RLS Demo Ops APAC'], '')
                     WHERE office = 'London'),
     -- Rows no principal can reach on ANY dimension. Derived from the ledger, not
     -- a hardcoded value list, so it stays correct when the dimensions change.
     ungranted AS (SELECT count(*) AS n FROM rls_shipments s
                   WHERE NOT EXISTS (
                       SELECT 1 FROM rls_entity_grants g
                       WHERE (g.entity_type = 'office'  AND g.entity_value = s.office)
                          OR (g.entity_type = 'company' AND g.entity_value = s.company)
                          OR (g.entity_type = 'site'    AND g.entity_value = s.site))),
     total AS (SELECT count(*) AS n FROM rls_shipments)
SELECT * FROM (
    SELECT 1 AS ck, 'Union across two disjoint teams is strictly larger than either' AS criterion,
           format('EMEA=%s KeyAcc=%s both=%s', emea.n, keyac.n,
                  rls_visible_count(ARRAY['RLS Demo Ops EMEA','RLS Demo Key Accounts'], '')) AS detail,
           CASE WHEN rls_visible_count(ARRAY['RLS Demo Ops EMEA','RLS Demo Key Accounts'], '')
                     > GREATEST(emea.n, keyac.n) THEN 'PASS' ELSE 'FAIL' END AS result
    FROM emea, keyac
    UNION ALL
    SELECT 2, 'Team grant + personal grant combine additively for one person',
           format('teams only=%s, teams+personal=%s', u1_teams_only.n, u1.n),
           CASE WHEN u1.n > u1_teams_only.n THEN 'PASS' ELSE 'FAIL' END
    FROM u1, u1_teams_only
    UNION ALL
    SELECT 3, 'Personal grant alone grants visibility with zero team contribution',
           format('user2 team-only=%s, user2 total=%s', u2_team_only.n, u2.n),
           CASE WHEN u2_team_only.n = 0 AND u2.n > 0 THEN 'PASS' ELSE 'FAIL' END
    FROM u2, u2_team_only
    UNION ALL
    SELECT 4, 'True negatives: ungranted person sees 0, ungranted entities exist',
           format('nogrants persona=%s rows, never-visible shipments=%s', nogr.n, ungranted.n),
           CASE WHEN nogr.n = 0 AND ungranted.n > 0 THEN 'PASS' ELSE 'FAIL' END
    FROM nogr, ungranted
    UNION ALL
    SELECT 5, 'Overlapping team grants on same entity do not double-count',
           format('London rows=%s, seen by member of both teams=%s', london.n, london_seen.n),
           CASE WHEN london.n = london_seen.n THEN 'PASS' ELSE 'FAIL' END
    FROM london, london_seen
    UNION ALL
    SELECT 6, 'Scale: filtered result is an obvious strict subset',
           format('total=%s, user1 sees=%s (%s%%)', total.n, u1.n,
                  ROUND(100.0 * u1.n / NULLIF(total.n, 0), 1)),
           CASE WHEN u1.n > 200 AND u1.n < (total.n * 0.75) THEN 'PASS' ELSE 'FAIL' END
    FROM u1, total
) x ORDER BY ck;

\echo ''
\echo '=== ROWS VISIBLE ONLY VIA ONE BRANCH (isolates each mechanism) ==='
SELECT persona,
       count(*) FILTER (WHERE office_rls  AND NOT company_rls AND NOT site_rls) AS only_office,
       count(*) FILTER (WHERE company_rls AND NOT office_rls  AND NOT site_rls) AS only_company,
       count(*) FILTER (WHERE site_rls    AND NOT office_rls  AND NOT company_rls) AS only_site
FROM rls_expected_visibility
GROUP BY persona
ORDER BY persona;
