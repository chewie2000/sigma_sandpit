-- 01_schema.sql — additive RLS pattern: warehouse objects.
--
-- Target: Postgres (Snowflake) — the connection Sigma knows as "Postgres (Snowflake)".
-- Run with:  psql -v ON_ERROR_STOP=1 -v schema=<schema> -f 01_schema.sql
--
-- Design note: only GRANT data lives here. The team -> user relationship is NEVER
-- replicated into the warehouse; CurrentUserInTeam() resolves that live in Sigma.
-- The one exception is rls_test_personas / rls_test_persona_teams, which exist
-- SOLELY so the validation harness can compute expected results offline. They are
-- not part of the pattern and must not be joined into the data model.

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS :"schema";
SET search_path TO :"schema";

-- ---------------------------------------------------------------------------
-- Reference dimensions (readability only; the fact table is denormalised so
-- entity_value matching happens on plain text, exactly as the pattern needs).
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS rls_offices CASCADE;
CREATE TABLE rls_offices (
    office      text PRIMARY KEY,
    region      text NOT NULL
);

DROP TABLE IF EXISTS rls_companies CASCADE;
CREATE TABLE rls_companies (
    company     text PRIMARY KEY,
    code        text NOT NULL,
    sector      text NOT NULL
);

DROP TABLE IF EXISTS rls_sites CASCADE;
CREATE TABLE rls_sites (
    site        text PRIMARY KEY,
    company     text NOT NULL REFERENCES rls_companies(company),
    city        text NOT NULL
);

-- ---------------------------------------------------------------------------
-- The grants ledger. Additive by construction: no priority column, no ordering,
-- no uniqueness beyond the natural key. A row is a row; two rows granting the
-- same entity to two different teams simply both apply.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS rls_entity_grants CASCADE;
CREATE TABLE rls_entity_grants (
    entity_type    text NOT NULL,   -- 'company' | 'site' | 'office'
    entity_value   text NOT NULL,   -- e.g. 'Aurora Foods' | 'COB-2' | 'Paris'
    principal_type text NOT NULL,   -- 'team' | 'user'
    principal_id   text NOT NULL,   -- Sigma team NAME, or Sigma user email
    CONSTRAINT rls_entity_grants_type_ck
        CHECK (entity_type IN ('company', 'site', 'office')),
    CONSTRAINT rls_entity_grants_principal_ck
        CHECK (principal_type IN ('team', 'user')),
    -- Deliberately the FULL natural key: the same entity may be granted to many
    -- principals, and the same principal to many entities. Only exact dupes barred.
    CONSTRAINT rls_entity_grants_pk
        PRIMARY KEY (entity_type, entity_value, principal_type, principal_id)
);

-- ---------------------------------------------------------------------------
-- The fact table to secure. Grain: one shipment.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS rls_shipments CASCADE;
CREATE TABLE rls_shipments (
    shipment_id   bigint PRIMARY KEY,
    shipped_date  date          NOT NULL,
    company       text          NOT NULL,
    site          text          NOT NULL,
    office        text          NOT NULL,
    mode          text          NOT NULL,
    revenue_eur   numeric(12,2) NOT NULL,
    teu           numeric(8,2)  NOT NULL
);

CREATE INDEX rls_shipments_office_ix  ON rls_shipments (office);
CREATE INDEX rls_shipments_company_ix ON rls_shipments (company);
CREATE INDEX rls_shipments_site_ix    ON rls_shipments (site);

-- ---------------------------------------------------------------------------
-- VALIDATION HARNESS ONLY — mirrors Sigma team membership so expected results
-- can be computed offline. NOT part of the pattern. Do not add to the model.
-- ---------------------------------------------------------------------------

DROP TABLE IF EXISTS rls_test_persona_teams CASCADE;
DROP TABLE IF EXISTS rls_test_personas CASCADE;

CREATE TABLE rls_test_personas (
    persona     text PRIMARY KEY,
    email       text NOT NULL,
    note        text NOT NULL
);

CREATE TABLE rls_test_persona_teams (
    persona     text NOT NULL REFERENCES rls_test_personas(persona),
    team_name   text NOT NULL,
    PRIMARY KEY (persona, team_name)
);
