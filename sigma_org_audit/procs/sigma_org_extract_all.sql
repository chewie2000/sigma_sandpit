-- ==============================================================================
-- sigma_org_extract_all.sql
--
-- Multi-tenant / multi-org fan-out wrapper for sigma_org_extract.
--
-- Reads a single Snowflake SECRET (sigma_tenant_registry) holding a JSON array of
-- the orgs to audit, and runs sigma_org_extract once per org -- so ONE deployment
-- and ONE trigger can refresh many orgs (a parent + its tenants, unrelated orgs,
-- or any mix). Each org's credentials are passed to sigma_org_extract via its
-- override parameters, so they are bound as call arguments rather than embedded
-- in logged SQL.
--
-- Why a single registry secret? A stored procedure can only read secrets that are
-- statically declared in its SECRETS clause -- it cannot resolve a secret by a
-- name computed at runtime. One registry secret (bound once to the integration)
-- therefore scales to any number of orgs with no proc/integration change.
--
-- Registry secret shape (sigma_tenant_registry, TYPE = GENERIC_STRING):
--   [
--     {"label":"acme",   "baseUrl":"https://aws-api.sigmacomputing.com",
--      "clientId":"...", "clientSecret":"...", "role":"child",  "enabled":true},
--     {"label":"globex", "baseUrl":"https://api.eu.aws.sigmacomputing.com",
--      "clientId":"...", "clientSecret":"...", "role":"parent", "enabled":true}
--   ]
--   - label        human-friendly handle (ORG_ID from /v2/whoami is the real key)
--   - role         recorded via ORG_ROLE_OVERRIDE (parent|child|standalone); the
--                  tenants API cannot self-identify a child from inside, so this
--                  asserts the known role (tagged roleSource=operator).
--   - enabled      skip when false
--
-- Parameters
--   TARGET_DATABASE / TARGET_SCHEMA / TARGET_TABLE  -- where RAW_SIGMA_OBJECTS lives
--   TENANT_LABEL  -- optional: run ONLY the org with this label (default NULL = all
--                    enabled orgs). This is the "refresh one tenant" path.
--   INCLUDE_GRANTS / MAX_WORKERS  -- passed through to sigma_org_extract
--
-- Example
--   CALL sigma_org_extract_all('SIGMA_ORG_AUDIT','AUDIT');              -- all enabled
--   CALL sigma_org_extract_all('SIGMA_ORG_AUDIT','AUDIT',
--        'RAW_SIGMA_OBJECTS', 'acme');                                  -- just 'acme'
--
-- Prerequisites
--   - sigma_org_extract deployed (this wrapper calls it).
--   - SECRET sigma_tenant_registry created, added to sigma_api_access's
--     ALLOWED_AUTHENTICATION_SECRETS, and READ-granted to the executing role.
-- ==============================================================================

CREATE OR REPLACE PROCEDURE sigma_org_extract_all(
    TARGET_DATABASE STRING,
    TARGET_SCHEMA   STRING,
    TARGET_TABLE    STRING  DEFAULT 'RAW_SIGMA_OBJECTS',
    TENANT_LABEL    STRING  DEFAULT NULL,
    INCLUDE_GRANTS  BOOLEAN DEFAULT TRUE,
    MAX_WORKERS     NUMBER  DEFAULT 10
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
EXTERNAL_ACCESS_INTEGRATIONS = (sigma_api_access)
SECRETS = ('sigma_tenant_registry' = sigma_tenant_registry)
HANDLER = 'main'
AS
$$
import _snowflake
import json


def main(session, target_database, target_schema, target_table,
         tenant_label, include_grants, max_workers):

    try:
        registry = json.loads(_snowflake.get_generic_secret_string('sigma_tenant_registry'))
    except Exception as e:
        return json.dumps({"error": f"could not read/parse sigma_tenant_registry: {e}"})
    if not isinstance(registry, list):
        return json.dumps({"error": "sigma_tenant_registry must be a JSON array of org objects"})

    # Select orgs: all enabled, or just the requested label.
    selected = [o for o in registry
                if o.get("enabled", True)
                and (tenant_label is None or o.get("label") == tenant_label)]
    if tenant_label is not None and not selected:
        return json.dumps({"error": f"no enabled org with label '{tenant_label}' in registry"})

    results = []
    ok = 0
    for o in selected:
        label = o.get("label")
        base_url, client_id, client_secret = o.get("baseUrl"), o.get("clientId"), o.get("clientSecret")
        if not (base_url and client_id and client_secret):
            results.append({"label": label, "error": "registry entry missing baseUrl/clientId/clientSecret"})
            continue
        try:
            # Delegate to the validated single-org proc. Credentials are passed as
            # bound call args (not interpolated into SQL); role is asserted via the
            # override so the per-org role is recorded.
            res = session.call(
                "SIGMA_ORG_EXTRACT",
                target_database, target_schema, target_table,
                include_grants, max_workers,
                base_url, client_id, client_secret, o.get("role"),
            )
            results.append({"label": label,
                            "result": json.loads(res) if isinstance(res, str) else res})
            ok += 1
        except Exception as e:
            results.append({"label": label, "error": str(e)})

    return json.dumps({
        "mode": "single" if tenant_label is not None else "all",
        "orgs_selected": len(selected),
        "orgs_succeeded": ok,
        "results": results,
    })
$$;
