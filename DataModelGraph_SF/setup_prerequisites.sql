-- ==============================================================================
-- setup_prerequisites.sql
--
-- One-time setup script for the DataModelGraph_SF toolkit.
-- Run this ONCE as ACCOUNTADMIN before deploying the stored procedure.
--
-- This script creates:
--   1. A network rule allowing outbound HTTPS to the Sigma API host
--   2. Snowflake Secrets to store your Sigma API credentials securely
--   3. An external access integration referencing the network rule and secrets
--   4. Grants to the role that will run the stored procedure
--
-- Replace every placeholder marked <LIKE_THIS> before running.
--
-- SIGMA API HOST
-- --------------
-- The host depends on the cloud and region your Sigma organisation is on:
--
--   Cloud / Region        Host
--   --------------------  -------------------------------------------
--   AWS US                aws-api.sigmacomputing.com
--   AWS EU                api.eu.aws.sigmacomputing.com
--   Azure US              api.us.azure.sigmacomputing.com
--   GCP US                api.us.gcp.sigmacomputing.com
--
-- See https://help.sigmacomputing.com/reference/get-started-sigma-api
--
-- SIGMA CLIENT CREDENTIALS
-- ------------------------
-- Generate from Sigma Administration → Developer Access → Create New.
-- Admin scope is recommended so skipPermissionCheck returns org-wide results.
-- The client secret is only shown once at creation — copy it immediately.
-- See https://help.sigmacomputing.com/reference/generate-client-credentials
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- ==============================================================================
-- 1. Network rule — allows outbound HTTPS to the Sigma API
-- ==============================================================================

CREATE OR REPLACE NETWORK RULE sigma_api_network_rule
  MODE       = EGRESS
  TYPE       = HOST_PORT
  VALUE_LIST = ('<YOUR_SIGMA_API_HOST>:443');
  -- Example: VALUE_LIST = ('aws-api.sigmacomputing.com:443');


-- ==============================================================================
-- 2. Secrets — store all Sigma API configuration outside of procedure code
--    Values are never visible in procedure source, query history, or version
--    control. Use CREATE OR REPLACE only if you need to rotate a value.
--
--    sigma_base_url      — the API base URL for your cloud/region (see host
--                          table above). Stored as a secret so it is defined
--                          in one place and stays in sync with the network rule.
--                          Example: https://aws-api.sigmacomputing.com
--    sigma_client_id     — OAuth client ID from Sigma Administration → Developer Access
--    sigma_client_secret — OAuth client secret (shown once at creation only)
-- ==============================================================================

CREATE SECRET IF NOT EXISTS sigma_base_url
  TYPE          = GENERIC_STRING
  SECRET_STRING = 'https://<YOUR_SIGMA_API_HOST>';
  -- Example: SECRET_STRING = 'https://aws-api.sigmacomputing.com';

CREATE SECRET IF NOT EXISTS sigma_client_id
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_ID>';

CREATE SECRET IF NOT EXISTS sigma_client_secret
  TYPE          = GENERIC_STRING
  SECRET_STRING = '<YOUR_SIGMA_CLIENT_SECRET>';


-- ==============================================================================
-- 3. External access integration
--    References the network rule and all three secrets so the procedure can
--    make outbound API calls and read configuration at runtime.
-- ==============================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sigma_api_access
  ALLOWED_NETWORK_RULES          = (sigma_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (sigma_base_url, sigma_client_id, sigma_client_secret)
  ENABLED = TRUE;


-- ==============================================================================
-- 4. Grants — allow the procedure-execution role to use the integration
--    and read all secrets.
--    Replace <YOUR_ROLE> with the role used to CREATE and CALL the procedure
--    (e.g. SYSADMIN or a custom role).
-- ==============================================================================

GRANT USAGE ON INTEGRATION sigma_api_access TO ROLE <YOUR_ROLE>;
GRANT READ   ON SECRET sigma_base_url       TO ROLE <YOUR_ROLE>;
GRANT READ   ON SECRET sigma_client_id      TO ROLE <YOUR_ROLE>;
GRANT READ   ON SECRET sigma_client_secret  TO ROLE <YOUR_ROLE>;
