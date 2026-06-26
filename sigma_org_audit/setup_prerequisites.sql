-- ==============================================================================
-- setup_prerequisites.sql
--
-- One-time setup script for the sigma_org_audit toolkit.
-- Run this ONCE as ACCOUNTADMIN before deploying the stored procedures.
--
-- This script creates:
--   1. A network rule allowing outbound HTTPS to the Sigma API host
--   2. Snowflake Secrets to store your Sigma API credentials securely
--   3. An external access integration referencing the network rule and secrets
--   4. Grants to the role that will run the stored procedures
--
-- Replace every placeholder marked <LIKE_THIS> before running.
--
-- SIGMA API HOST
-- --------------
-- The host depends on the cloud and region your Sigma organisation is on:
--
--   Cloud / Region        Host
--   --------------------  -------------------------------------------
--   AWS US (West)         aws-api.sigmacomputing.com
--   AWS US (East)         api.us-a.aws.sigmacomputing.com
--   AWS EU                api.eu.aws.sigmacomputing.com
--   AWS UK                api.uk.aws.sigmacomputing.com
--   AWS CA                api.ca.aws.sigmacomputing.com
--   AWS AU / APAC         api.au.aws.sigmacomputing.com
--   Azure US              api.us.azure.sigmacomputing.com
--   Azure EU              api.eu.azure.sigmacomputing.com
--   GCP US                api.sigmacomputing.com
--
-- See https://help.sigmacomputing.com/reference/get-started-sigma-api
--
-- SIGMA CLIENT CREDENTIALS
-- ------------------------
-- Generate from Sigma Administration -> Developer Access -> Create New.
-- Admin scope is required for skipPermissionCheck access (org-wide visibility).
-- The client secret is only shown once at creation -- copy it immediately.
-- See https://help.sigmacomputing.com/reference/generate-client-credentials
-- ==============================================================================

-- ==============================================================================
-- 0. Audit database + schema -- where everything below (and the procs/views) lives.
--    Created by the build role so it can deploy procs/views/tables there. The
--    network rule + secrets are schema-level objects, so this must exist first.
--    Replace <AUDIT_DB> / <AUDIT_SCHEMA> (e.g. SIGMA_ORG_AUDIT / AUDIT) and use
--    the SAME names everywhere downstream.
-- ==============================================================================

USE ROLE SYSADMIN;          -- the build/execution role (your <YOUR_ROLE>)
CREATE DATABASE IF NOT EXISTS <AUDIT_DB>;
CREATE SCHEMA   IF NOT EXISTS <AUDIT_DB>.<AUDIT_SCHEMA>;

USE ROLE ACCOUNTADMIN;
USE DATABASE <AUDIT_DB>;
USE SCHEMA   <AUDIT_SCHEMA>;

-- ==============================================================================
-- 1. Network rule -- allows outbound HTTPS to the Sigma API
-- ==============================================================================

-- Multi-org note: the egress list below includes EVERY Sigma API host, so one
-- deployment can audit orgs across any cloud/region without editing the rule.
-- Trim this to just your host(s) if your egress policy requires a tight list.
CREATE OR REPLACE NETWORK RULE sigma_api_network_rule
  MODE       = EGRESS
  TYPE       = HOST_PORT
  VALUE_LIST = (
    'aws-api.sigmacomputing.com:443',        -- AWS US (West)
    'api.us-a.aws.sigmacomputing.com:443',   -- AWS US (East)
    'api.eu.aws.sigmacomputing.com:443',     -- AWS EU
    'api.uk.aws.sigmacomputing.com:443',     -- AWS UK
    'api.ca.aws.sigmacomputing.com:443',     -- AWS CA
    'api.au.aws.sigmacomputing.com:443',     -- AWS AU / APAC
    'api.us.azure.sigmacomputing.com:443',   -- Azure US
    'api.eu.azure.sigmacomputing.com:443',   -- Azure EU
    'api.sigmacomputing.com:443'             -- GCP US
  );


-- ==============================================================================
-- 2. Secrets -- store all Sigma API configuration outside of procedure code
--    Values are never visible in procedure source, query history, or version
--    control. Use CREATE OR REPLACE only if you need to rotate a value.
--
--    sigma_base_url      -- the API base URL for your cloud/region (see host
--                           table above). Stored as a secret so it is defined in
--                           one place and stays in sync with the network rule.
--                           Example: https://api.eu.aws.sigmacomputing.com
--    sigma_client_id     -- OAuth client ID from Administration -> Developer Access
--    sigma_client_secret -- OAuth client secret (shown once at creation only)
-- ==============================================================================

CREATE SECRET IF NOT EXISTS sigma_base_url
  TYPE          = GENERIC_STRING
  SECRET_STRING = 'https://<YOUR_SIGMA_API_HOST>';
  -- Example: SECRET_STRING = 'https://api.eu.aws.sigmacomputing.com';

CREATE SECRET IF NOT EXISTS sigma_client_id
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_ID>';

CREATE SECRET IF NOT EXISTS sigma_client_secret
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_SECRET>';


-- ==============================================================================
-- 3. External access integration
--    References the network rule and all three secrets so procedures can
--    make outbound API calls and read configuration at runtime.
-- ==============================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sigma_api_access
  ALLOWED_NETWORK_RULES          = (sigma_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (sigma_base_url, sigma_client_id, sigma_client_secret)
  ENABLED = TRUE;


-- ==============================================================================
-- 4. Grants -- allow the procedure-execution role to use the integration
--    and read all secrets.
--    Replace <YOUR_ROLE> with the role used to CREATE and CALL the procedures
--    (e.g. SYSADMIN or a custom role).
-- ==============================================================================

GRANT USAGE ON INTEGRATION sigma_api_access TO ROLE <YOUR_ROLE>;
GRANT READ   ON SECRET sigma_base_url       TO ROLE <YOUR_ROLE>;
GRANT READ   ON SECRET sigma_client_id      TO ROLE <YOUR_ROLE>;
GRANT READ   ON SECRET sigma_client_secret  TO ROLE <YOUR_ROLE>;


-- ==============================================================================
-- 5. (Optional) Multi-org / multi-tenant registry (for sigma_org_extract_all)
--    To refresh many orgs (a parent + tenants, unrelated orgs, or any mix) from
--    one trigger, create a single registry secret holding each org's credentials
--    and bind it to the integration. sigma_org_extract_all loops over it.
--    See README "Refresh many orgs from one trigger". Sketch:
--
--    CREATE OR REPLACE SECRET sigma_tenant_registry TYPE = GENERIC_STRING
--      SECRET_STRING = '[{"label":"acme","baseUrl":"https://<host>",
--                         "clientId":"...","clientSecret":"...",
--                         "role":"child","enabled":true}]';
--    ALTER EXTERNAL ACCESS INTEGRATION sigma_api_access
--      SET ALLOWED_AUTHENTICATION_SECRETS =
--        (sigma_base_url, sigma_client_id, sigma_client_secret, sigma_tenant_registry);
--    GRANT READ ON SECRET sigma_tenant_registry TO ROLE <YOUR_ROLE>;
-- ==============================================================================

-- ==============================================================================
-- 6. Writeback scan reachability (for sigma_writeback_scan)
--    The writeback scan reads INFORMATION_SCHEMA metadata and the input-table
--    write-ahead-log (WAL) tables in whatever databases/schemas your Sigma
--    connections write back to. The procedure-execution role must be able to
--    read those locations, otherwise the connection is inventoried from the API
--    but flagged SCAN_REACHABLE = FALSE (its SIGDS/WAL contents are skipped).
--
--    Grant the execution role USAGE + SELECT on each writeback database/schema.
--    Replace the example identifiers with the writeback locations reported by
--    STG_CONNECTIONS after the first sigma_org_extract run.
-- ==============================================================================

-- GRANT USAGE  ON DATABASE <WRITEBACK_DB>                TO ROLE <YOUR_ROLE>;
-- GRANT USAGE  ON SCHEMA   <WRITEBACK_DB>.<WRITEBACK_SCHEMA> TO ROLE <YOUR_ROLE>;
-- GRANT SELECT ON ALL TABLES    IN SCHEMA <WRITEBACK_DB>.<WRITEBACK_SCHEMA> TO ROLE <YOUR_ROLE>;
-- GRANT SELECT ON FUTURE TABLES IN SCHEMA <WRITEBACK_DB>.<WRITEBACK_SCHEMA> TO ROLE <YOUR_ROLE>;
