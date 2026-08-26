-- 02_seed_dimensions_and_grants.sql — reference data + the grants ledger.
--
-- Run with:  psql -v ON_ERROR_STOP=1 -v schema=<schema> -f 02_seed_dimensions_and_grants.sql

\set ON_ERROR_STOP on
SET search_path TO :"schema";

TRUNCATE rls_entity_grants, rls_test_persona_teams, rls_test_personas;
TRUNCATE rls_sites, rls_companies, rls_offices CASCADE;

-- ---------------------------------------------------------------------------
-- Offices (12). Only 4 are ever granted, so the filtered result is a visibly
-- small subset rather than "most of the table".
-- ---------------------------------------------------------------------------
INSERT INTO rls_offices (office, region) VALUES
    ('Paris',     'EMEA'),   -- granted (team EMEA)
    ('London',    'EMEA'),   -- granted (teams EMEA + APAC -> overlap case)
    ('Hamburg',   'EMEA'),   -- granted (user1 personally)
    ('Singapore', 'APAC'),   -- granted (team APAC)
    ('Amsterdam', 'EMEA'),   -- zero grants
    ('Barcelona', 'EMEA'),   -- zero grants
    ('Milan',     'EMEA'),   -- zero grants
    ('Dubai',     'MEA'),    -- zero grants
    ('Shanghai',  'APAC'),   -- zero grants
    ('Tokyo',     'APAC'),   -- zero grants
    ('New York',  'AMER'),   -- zero grants
    ('Chicago',   'AMER');   -- zero grants

-- ---------------------------------------------------------------------------
-- Companies (12). Only 3 are ever granted.
-- ---------------------------------------------------------------------------
INSERT INTO rls_companies (company, code, sector) VALUES
    ('Aurora Foods',   'AUR', 'Food & Beverage'),   -- granted (team Key Accounts)
    ('Baltic Steel',   'BAL', 'Metals'),            -- granted (team Key Accounts)
    ('Delta Textiles', 'DEL', 'Apparel'),           -- granted (user2 personally)
    ('Cobalt Pharma',  'COB', 'Pharmaceuticals'),   -- only site COB-2 is granted
    ('Everest Motors', 'EVE', 'Automotive'),        -- zero grants
    ('Fjord Marine',   'FJO', 'Marine'),            -- zero grants
    ('Granite Chem',   'GRA', 'Chemicals'),         -- zero grants
    ('Helios Optics',  'HEL', 'Electronics'),       -- zero grants
    ('Iberia Grain',   'IBE', 'Agriculture'),       -- zero grants
    ('Juniper Labs',   'JUN', 'Biotech'),           -- zero grants
    ('Kestrel Aero',   'KES', 'Aerospace'),         -- zero grants
    ('Lumen Glass',    'LUM', 'Materials');         -- zero grants

-- ---------------------------------------------------------------------------
-- Sites (24) — three per company.
-- ---------------------------------------------------------------------------
INSERT INTO rls_sites (site, company, city)
SELECT c.code || '-' || n,
       c.company,
       (ARRAY['Lyon','Rotterdam','Gdansk','Bilbao','Turin','Antwerp',
              'Malmo','Porto','Bristol','Katowice','Graz','Aarhus'])
           [1 + ((abs(hashtext(c.code || n::text)) % 12))]
FROM rls_companies c
CROSS JOIN generate_series(1, 3) AS n;

-- ---------------------------------------------------------------------------
-- THE GRANTS LEDGER
--
-- Team grants use the Sigma team NAME; user grants use the Sigma user EMAIL.
-- Nothing here encodes which user is in which team — that stays in Sigma.
--
--   RLS Demo Ops EMEA      -> offices Paris, London
--   RLS Demo Ops APAC      -> offices Singapore, London     <- London overlaps EMEA
--   RLS Demo Key Accounts  -> companies Aurora Foods, Baltic Steel
--   RLS Demo Observers     -> (deliberately absent: a team with zero grants)
--   user1 (personal)       -> office Hamburg, site COB-2
--   user2 (personal)       -> company Delta Textiles
-- ---------------------------------------------------------------------------
INSERT INTO rls_entity_grants (entity_type, entity_value, principal_type, principal_id) VALUES
    -- Team grants, office dimension
    ('office',  'Paris',          'team', 'RLS Demo Ops EMEA'),
    ('office',  'London',         'team', 'RLS Demo Ops EMEA'),
    ('office',  'Singapore',      'team', 'RLS Demo Ops APAC'),
    ('office',  'London',         'team', 'RLS Demo Ops APAC'),

    -- Team grants, company dimension
    ('company', 'Aurora Foods',   'team', 'RLS Demo Key Accounts'),
    ('company', 'Baltic Steel',   'team', 'RLS Demo Key Accounts'),

    -- Personal grants
    ('office',  'Hamburg',        'user', 'mark.oldfield+user1@sigmacomputing.com'),
    ('site',    'COB-2',          'user', 'mark.oldfield+user1@sigmacomputing.com'),
    ('company', 'Delta Textiles', 'user', 'mark.oldfield+user2@sigmacomputing.com');

-- ---------------------------------------------------------------------------
-- VALIDATION HARNESS ONLY — mirror of Sigma team membership.
-- ---------------------------------------------------------------------------
INSERT INTO rls_test_personas (persona, email, note) VALUES
    ('user1',    'mark.oldfield+user1@sigmacomputing.com',
     'Three teams (EMEA, APAC, Key Accounts) + two personal grants'),
    ('user2',    'mark.oldfield+user2@sigmacomputing.com',
     'One team with zero grants + one personal company grant'),
    ('nogrants', 'mark.oldfield+user@sigmacomputing.com',
     'No teams, no personal grants — must see zero rows');

INSERT INTO rls_test_persona_teams (persona, team_name) VALUES
    ('user1', 'RLS Demo Ops EMEA'),
    ('user1', 'RLS Demo Ops APAC'),
    ('user1', 'RLS Demo Key Accounts'),
    ('user2', 'RLS Demo Observers');
    -- 'nogrants' intentionally has no rows.
