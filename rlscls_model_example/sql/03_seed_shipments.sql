-- 03_seed_shipments.sql — deterministic synthetic fact table (8,000 shipments).
--
-- Determinism: every attribute is derived from md5(<salt> || shipment_id), so the
-- dataset is byte-identical on every rebuild and needs no seed file. Different
-- salts per attribute keep company / office / date / value mutually decorrelated
-- (a plain "id % n" would lock them into lockstep cycles).
--
-- Run with:  psql -v ON_ERROR_STOP=1 -v schema=<schema> -f 03_seed_shipments.sql

\set ON_ERROR_STOP on
SET search_path TO :"schema";

TRUNCATE rls_shipments;

-- Deterministic non-negative 31-bit integer from a salted id.
-- The & 2147483647 mask clears the sign bit; abs() would overflow on INT_MIN.
-- Returns int (not bigint): the mask guarantees the value fits, and int is what
-- date arithmetic, array subscripts and OFFSET all expect.
DROP FUNCTION IF EXISTS rls_hash(text, bigint);
CREATE FUNCTION rls_hash(salt text, id bigint)
RETURNS int LANGUAGE sql IMMUTABLE AS $$
    SELECT ((('x' || substr(md5(salt || id::text), 1, 8))::bit(32)::bigint) & 2147483647)::int;
$$;

-- Dimension members numbered 0..n-1 with their own cardinality attached, so the
-- modulo comes from the data rather than a hardcoded literal that silently skews
-- the spread the moment a dimension gains or loses a member.
INSERT INTO rls_shipments (shipment_id, shipped_date, company, site, office, mode, revenue_eur, teu)
WITH ids AS (
    SELECT gs AS shipment_id FROM generate_series(1, 8000) AS gs
),
company_ix AS (
    SELECT company, code,
           (row_number() OVER (ORDER BY company))::int - 1 AS ix,
           (count(*)    OVER ())::int                      AS n
    FROM rls_companies
),
office_ix AS (
    SELECT office,
           (row_number() OVER (ORDER BY office))::int - 1 AS ix,
           (count(*)    OVER ())::int                     AS n
    FROM rls_offices
)
SELECT
    i.shipment_id,
    (DATE '2024-09-01' + (rls_hash('date', i.shipment_id) % 730))          AS shipped_date,
    c.company,
    c.code || '-' || (1 + (rls_hash('site', i.shipment_id) % 3))           AS site,
    o.office,
    (ARRAY['Air','Sea','Road','Rail'])
        [1 + (rls_hash('mode', i.shipment_id) % 4)]                        AS mode,
    ROUND((800 + (rls_hash('rev', i.shipment_id) % 74200))::numeric, 2)    AS revenue_eur,
    ROUND(((10 + (rls_hash('teu', i.shipment_id) % 4790)) / 100.0)::numeric, 2) AS teu
FROM ids i
JOIN company_ix c ON c.ix = rls_hash('company', i.shipment_id) % c.n
JOIN office_ix  o ON o.ix = rls_hash('office',  i.shipment_id) % o.n;

DROP FUNCTION rls_hash(text, bigint);

ANALYZE rls_shipments;
