-- =============================================================================
-- setup_prerequisites.sql  (Snowflake)  -- ONE-TIME, run as ACCOUNTADMIN
--
-- Lets the sigds_scan stored procedure call the Sigma API from inside Snowflake,
-- and read the account-level history it needs. Creates:
--   1. A network rule allowing outbound HTTPS to YOUR Sigma API host only
--   2. Secrets holding the Sigma API URL + client credentials (kept out of code,
--      environment variables and procedure source)
--   3. An external access integration tying them together
--   4. Grants to the role that will create and run the procedure
--
-- Standalone: uses its own object names (sigds_*), so it neither depends on nor
-- clashes with sigma_org_audit's setup. If you already have an integration that
-- can reach Sigma and holds suitable secrets, you can skip this file and point
-- sigds_scan_procedure.sql at it instead (see "Reusing an existing integration"
-- at the bottom).
--
-- Find & replace these placeholders before running (Snowsight: Cmd/Ctrl+F, then
-- the replace option):
--   <YOUR_DATABASE>            database that will hold the map/log tables + procedure
--   <YOUR_SCHEMA>              schema that will hold them (must already exist)
--   <YOUR_ROLE>                role that will run create_*.sql, the procedure, and the task
--   <YOUR_SIGMA_API_HOST>      e.g. aws-api.sigmacomputing.com   (table below; no https://)
--   <YOUR_SIGMA_CLIENT_ID>     Sigma: Administration -> Developer Access -> Create New
--   <YOUR_SIGMA_CLIENT_SECRET> (shown once at creation). Use ADMIN scope: it is what
--                              allows an org-wide workbook list, so "workbook deleted"
--                              can be trusted.
--
-- SIGMA API HOST
--   AWS US (West)   aws-api.sigmacomputing.com
--   AWS US (East)   api.us-a.aws.sigmacomputing.com
--   AWS EU          api.eu.aws.sigmacomputing.com
--   AWS UK          api.uk.aws.sigmacomputing.com
--   AWS CA          api.ca.aws.sigmacomputing.com
--   AWS AU / APAC   api.au.aws.sigmacomputing.com
--   Azure US        api.us.azure.sigmacomputing.com
--   Azure EU        api.eu.azure.sigmacomputing.com
--   GCP US          api.sigmacomputing.com
-- See https://help.sigmacomputing.com/reference/get-started-sigma-api
--
-- Note: the CREATE SECRET statements below put the client secret into Snowflake's
-- query history (inherent to CREATE SECRET). Rotate it afterwards if that matters.
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE <YOUR_DATABASE>;
USE SCHEMA   <YOUR_SCHEMA>;

-- 1. Network rule -- egress to the Sigma API host only.
CREATE NETWORK RULE IF NOT EXISTS sigds_sigma_network_rule
  MODE       = EGRESS
  TYPE       = HOST_PORT
  VALUE_LIST = ('<YOUR_SIGMA_API_HOST>:443');

-- 2. Secrets.
CREATE SECRET IF NOT EXISTS sigds_sigma_base_url
  TYPE          = GENERIC_STRING
  SECRET_STRING = 'https://<YOUR_SIGMA_API_HOST>';

CREATE SECRET IF NOT EXISTS sigds_sigma_client_id
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_ID>';

CREATE SECRET IF NOT EXISTS sigds_sigma_client_secret
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_SECRET>';

-- 3. External access integration.
CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS sigds_sigma_access
  ALLOWED_NETWORK_RULES          = (sigds_sigma_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (sigds_sigma_base_url, sigds_sigma_client_id, sigds_sigma_client_secret)
  ENABLED = TRUE;

-- 4. Grants to the role that creates and runs the procedure.
--    (The procedure runs with the CALLER's rights (EXECUTE AS CALLER), so everything it
--    granted to the role that will CALL it.)
GRANT USAGE ON INTEGRATION sigds_sigma_access          TO ROLE <YOUR_ROLE>;
GRANT READ  ON SECRET sigds_sigma_base_url             TO ROLE <YOUR_ROLE>;
GRANT READ  ON SECRET sigds_sigma_client_id            TO ROLE <YOUR_ROLE>;
GRANT READ  ON SECRET sigds_sigma_client_secret        TO ROLE <YOUR_ROLE>;

-- Access evidence: QUERY_HISTORY / ACCESS_HISTORY / TABLE_STORAGE_METRICS live in
-- SNOWFLAKE.ACCOUNT_USAGE. ACCESS_HISTORY needs Enterprise Edition or higher; without
-- this grant (or on Standard edition) the scan still runs but scores are write-only
-- and TIER 1 is capped.
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <YOUR_ROLE>;

-- The role must be able to read the writeback schema(s) it will scan. Repeat per
-- schema (uncomment and edit):
-- GRANT USAGE  ON DATABASE <WRITEBACK_DB>                        TO ROLE <YOUR_ROLE>;
-- GRANT USAGE  ON SCHEMA   <WRITEBACK_DB>.<WRITEBACK_SCHEMA>     TO ROLE <YOUR_ROLE>;
-- GRANT SELECT ON ALL TABLES    IN SCHEMA <WRITEBACK_DB>.<WRITEBACK_SCHEMA> TO ROLE <YOUR_ROLE>;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA <WRITEBACK_DB>.<WRITEBACK_SCHEMA> TO ROLE <YOUR_ROLE>;

-- Optional: let the role schedule the scan with a TASK (see sigds_scan_procedure.sql).
-- GRANT EXECUTE TASK ON ACCOUNT TO ROLE <YOUR_ROLE>;

-- The role also needs, on <YOUR_DATABASE>.<YOUR_SCHEMA>: USAGE, CREATE TABLE,
-- CREATE VIEW and CREATE PROCEDURE (SYSADMIN has these if it owns the schema).
--
-- NOTE: the network rule and secrets above are created by ACCOUNTADMIN *in that schema*,
-- so your ACCOUNTADMIN session must be able to create objects there. That is automatic
-- if the schema is owned by SYSADMIN (ACCOUNTADMIN inherits it). If you use a dedicated
-- role instead, grant it up first:  GRANT ROLE <YOUR_ROLE> TO ROLE ACCOUNTADMIN;


-- =============================================================================
-- Reusing an existing integration (e.g. sigma_org_audit's `sigma_api_access`)
-- =============================================================================
-- Skip sections 1-3 above, keep the grants, and in sigds_scan_procedure.sql change
-- two lines in the CREATE PROCEDURE header:
--     EXTERNAL_ACCESS_INTEGRATIONS = (sigma_api_access)
--     SECRETS = ('sigma_base_url' = <db>.<schema>.sigma_base_url,
--                'sigma_client_id' = <db>.<schema>.sigma_client_id,
--                'sigma_client_secret' = <db>.<schema>.sigma_client_secret)
-- using the fully-qualified names of the secrets that integration already allows, and
-- grant your role USAGE on that integration and READ on those three secrets.
