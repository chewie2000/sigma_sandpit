#!/usr/bin/env bash
# ==============================================================================
# deploy.sh -- one-command deploy / refresh for sigma_org_audit.
#
# Wraps the Snowflake CLI (`snow`) so you don't run the SQL files by hand in the
# right order with the right context. Idempotent: object deploys use
# CREATE OR REPLACE; the bootstrap encodes the first-time dependency order
# (procs -> extract -> stage -> writeback -> history -> marts, because the drift
# mart view needs the SCD2 tables to exist first).
#
# The ordered file lists below (PROC_FILES / VIEW_FILES) are the single source of
# deploy order -- they are also the manifest a future Snowflake Git-integration
# deploy would run via EXECUTE IMMEDIATE FROM (see DEPLOY.md, "Path to option 2").
#
# Usage:
#   ./deploy.sh <command> [flags]
#
# Commands:
#   setup          (ACCOUNTADMIN) audit db/schema + network rule + secrets +
#                  integration + grants
#   registry       (ACCOUNTADMIN) set the org registry secret. Modes:
#                    registry                          -> 1 org from env vars
#                    registry --label acme --org-role child   -> name/role that org
#                    registry --file orgs.json         -> many orgs from a JSON file
#                  (orgs.json = JSON array; see orgs.example.json)
#   deploy-procs   (re)create the stored procedures
#   deploy-views   (re)create stage + mart views (needs SCD2 tables to exist)
#   bootstrap      full first-time install: procs -> extract -> stage -> writeback
#                  -> history -> marts
#   refresh [lbl]  data refresh only: extract(all|lbl) + writeback + history
#   reset          drop procs/views/SCD2 tables (keeps secrets + raw), then redeploy
#   reset --hard   reset + also drop RAW_SIGMA_OBJECTS + SIGMA_EXTRACT_LOG, for a
#                  from-scratch rebuild reusing setup/secrets/registry (then bootstrap)
#   reset --data   data-only: drop raw/log/SCD2 but KEEP procs + views, for a clean
#                  dataset without redeploying code (then refresh)
#   teardown       (ACCOUNTADMIN) DROP the audit DB + integration -- full clean-down.
#                  Destructive; prompts to confirm unless --yes. Re-run setup after.
#   help
#
# Flags (defaults shown):
#   --conn <snow CLI default connection>   (omitted unless you pass --conn NAME)
#   --db SIGMA_ORG_AUDIT  --schema AUDIT
#   --role SYSADMIN  --warehouse COMPUTE_WH  --admin-role ACCOUNTADMIN
#
# Output: file deploys print just ">> <file> ... ok" (the compiled SQL/Python is
# hidden on success). Set DEPLOY_VERBOSE=1 to echo full snow output; failures are
# always shown in full.
#
# Connection: if --conn is not given, the Snowflake CLI's own default connection
# is used (set one with `snow connection set-default <name>`). A connection name
# is local to ~/.snowflake/connections.toml, so there is no portable hardcoded
# default -- pass --conn NAME to target a specific profile.
#
# Secrets are read from the environment (SIGMA_BASE_URL / SIGMA_CLIENT_ID /
# SIGMA_CLIENT_SECRET) and injected via a 0600 temp file that is deleted after --
# never passed on the command line.
# ==============================================================================
set -euo pipefail

CONN=""   # empty => use the Snowflake CLI's default connection (no -c passed)
DB=SIGMA_ORG_AUDIT
SCHEMA=AUDIT
ROLE=SYSADMIN
WH=COMPUTE_WH
ADMIN_ROLE=ACCOUNTADMIN
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PROC_FILES=(
  "procs/sigma_org_extract.sql"
  "procs/sigma_writeback_scan.sql"
  "procs/sigma_org_extract_all.sql"
  "marts/scd2_history.sql"
)
STAGE_FILES=("stage/stage_views.sql")
MART_FILES=("marts/mart_views.sql")

# Current proc signatures -- dropped before (re)create so an argument-count change
# never hits Snowflake's "ambiguous overload" error. Keep in sync with the procs.
PROC_SIGNATURES=(
  "SIGMA_ORG_EXTRACT(VARCHAR,VARCHAR,VARCHAR,BOOLEAN,NUMBER,VARCHAR,VARCHAR,VARCHAR,VARCHAR)"
  "SIGMA_WRITEBACK_SCAN(VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR)"
  "SIGMA_ORG_EXTRACT_ALL(VARCHAR,VARCHAR,VARCHAR,VARCHAR,BOOLEAN,NUMBER)"
  "SIGMA_SCD2_APPLY(VARCHAR,VARCHAR,VARCHAR)"
)

# ---- arg parsing -------------------------------------------------------------
CMD="${1:-help}"; shift || true
LABEL=""              # org label: positional for `refresh`, or --label for `registry`
ORG_ROLE_VAL="child"  # role recorded for the single-org `registry` seed (--org-role)
REG_FILE=""           # registry: load a JSON file of orgs instead of env (--file)
RESET_HARD=0          # reset --hard: also drop RAW_SIGMA_OBJECTS + SIGMA_EXTRACT_LOG
RESET_DATA=0          # reset --data: drop data only (raw/log/SCD2), keep procs + views
ASSUME_YES=0          # teardown: skip the interactive confirmation (--yes)
# capture an optional positional label for `refresh`
if [[ "${CMD}" == "refresh" && "${1:-}" != "" && "${1:-}" != --* ]]; then LABEL="$1"; shift || true; fi
while [[ $# -gt 0 ]]; do
  case "$1" in
    --conn) CONN="$2"; shift 2;;
    --db) DB="$2"; shift 2;;
    --schema) SCHEMA="$2"; shift 2;;
    --role) ROLE="$2"; shift 2;;
    --warehouse) WH="$2"; shift 2;;
    --admin-role) ADMIN_ROLE="$2"; shift 2;;
    --label) LABEL="$2"; shift 2;;
    --org-role) ORG_ROLE_VAL="$2"; shift 2;;
    --file) REG_FILE="$2"; shift 2;;
    --hard) RESET_HARD=1; shift;;
    --data) RESET_DATA=1; shift;;
    --yes|-y) ASSUME_YES=1; shift;;
    *) echo "unknown flag: $1" >&2; exit 2;;
  esac
done

# Only pass -c when a connection was given; otherwise snow uses its default.
# (Guarded expansion so an empty array is safe under `set -u` on bash 3.2.)
CONN_ARGS=(); [[ -n "$CONN" ]] && CONN_ARGS=(-c "$CONN")

_filter() { grep -v -iE "bad owner|too wide|config_manager|UserWarning|chown|chmod|skip this|^ *warn\(|SF_SKIP" || true; }
sf()  { snow sql ${CONN_ARGS[@]+"${CONN_ARGS[@]}"} --role "$ROLE"       --database "$DB" --schema "$SCHEMA" --warehouse "$WH" "$@" 2>&1 | _filter; }
sfa() { snow sql ${CONN_ARGS[@]+"${CONN_ARGS[@]}"} --role "$ADMIN_ROLE" --database "$DB" --schema "$SCHEMA" --warehouse "$WH" "$@" 2>&1 | _filter; }

# Quiet by default: print ">> <file> ... ok" per file rather than the large
# SQL / Python bodies snow echoes while compiling. On failure, the full snow
# output is shown and the run aborts. Set DEPLOY_VERBOSE=1 to see everything.
run_files() {
  local f out
  for f in "$@"; do
    printf '>> %s ... ' "$f"
    if out=$(sf -f "$HERE/$f"); then
      echo "ok"
      if [[ "${DEPLOY_VERBOSE:-0}" == 1 ]]; then printf '%s\n' "$out"; fi
    else
      echo "FAILED"
      printf '%s\n' "$out" >&2
      return 1
    fi
  done
}

drop_procs() {
  local sig
  for sig in "${PROC_SIGNATURES[@]}"; do
    echo ">> DROP PROCEDURE IF EXISTS $sig"
    sf -q "DROP PROCEDURE IF EXISTS $sig;" >/dev/null || true
  done
}

cmd_deploy_procs() { drop_procs; run_files "${PROC_FILES[@]}"; }
cmd_deploy_views() { run_files "${STAGE_FILES[@]}" "${MART_FILES[@]}"; }

cmd_extract() {
  if [[ -n "$LABEL" ]]; then
    echo ">> CALL sigma_org_extract_all (label=$LABEL)"
    sf -q "CALL sigma_org_extract_all('$DB','$SCHEMA','RAW_SIGMA_OBJECTS','$LABEL');"
  else
    echo ">> CALL sigma_org_extract_all (all enabled orgs)"
    sf -q "CALL sigma_org_extract_all('$DB','$SCHEMA');"
  fi
}
cmd_writeback() { echo ">> CALL sigma_writeback_scan"; sf -q "CALL sigma_writeback_scan('$DB','$SCHEMA');"; }
cmd_history() {
  echo ">> CALL sigma_scd2_apply x4"
  sf -q "
    CALL sigma_scd2_apply('STG_WORKBOOKS','SCD2_WORKBOOKS','WORKBOOK_ID');
    CALL sigma_scd2_apply('STG_DATASETS','SCD2_DATASETS','DATASET_ID');
    CALL sigma_scd2_apply('STG_CONNECTIONS','SCD2_CONNECTIONS','CONNECTION_ID');
    CALL sigma_scd2_apply('STG_WRITEBACK_TABLES','SCD2_WRITEBACK_TABLES','SIGDS_TABLE');"
}

cmd_bootstrap() {
  cmd_deploy_procs
  cmd_extract
  run_files "${STAGE_FILES[@]}"
  cmd_writeback
  cmd_history
  run_files "${MART_FILES[@]}"
  echo "== bootstrap complete =="
}

cmd_refresh() { cmd_extract; cmd_writeback; cmd_history; echo "== refresh complete =="; }

cmd_reset() {
  if [[ "$RESET_DATA" == 1 && "$RESET_HARD" == 1 ]]; then
    echo "use either --data or --hard, not both." >&2; exit 2
  fi
  if [[ "$RESET_DATA" == 1 ]]; then
    # Data-only: wipe snapshots + progress log + SCD2 history, but KEEP the
    # deployed procs and views. Lets you re-`refresh` for a clean dataset
    # without redeploying code (no ACCOUNTADMIN). Views over RAW_SIGMA_OBJECTS
    # are briefly invalid until refresh repopulates it.
    sf -q "
      DROP TABLE IF EXISTS RAW_SIGMA_OBJECTS; DROP TABLE IF EXISTS SIGMA_EXTRACT_LOG;
      DROP TABLE IF EXISTS SCD2_WORKBOOKS; DROP TABLE IF EXISTS SCD2_DATASETS;
      DROP TABLE IF EXISTS SCD2_CONNECTIONS; DROP TABLE IF EXISTS SCD2_WRITEBACK_TABLES;" >/dev/null || true
    echo "== reset --data done (dropped raw/log/SCD2; procs + views + secrets kept). Run refresh (or bootstrap). =="
    return
  fi
  drop_procs
  sf -q "
    DROP VIEW IF EXISTS V_TENANCY_TOPOLOGY; DROP VIEW IF EXISTS V_WRITEBACK_SHARED_SCHEMAS;
    DROP VIEW IF EXISTS V_WORKBOOK_DRIFT; DROP VIEW IF EXISTS V_OWNERSHIP_CLEANUP;
    DROP VIEW IF EXISTS V_WRITEBACK_GOVERNANCE; DROP VIEW IF EXISTS V_MIGRATION_SCORE;
    DROP VIEW IF EXISTS V_INVENTORY;
    DROP TABLE IF EXISTS SCD2_WORKBOOKS; DROP TABLE IF EXISTS SCD2_DATASETS;
    DROP TABLE IF EXISTS SCD2_CONNECTIONS; DROP TABLE IF EXISTS SCD2_WRITEBACK_TABLES;" >/dev/null || true
  if [[ "$RESET_HARD" == 1 ]]; then
    # --hard also clears the raw snapshots + progress log, for a from-scratch
    # rebuild that reuses the existing setup/secrets/registry (no ACCOUNTADMIN).
    sf -q "DROP TABLE IF EXISTS RAW_SIGMA_OBJECTS; DROP TABLE IF EXISTS SIGMA_EXTRACT_LOG;" >/dev/null || true
    echo "== reset --hard done (also dropped RAW_SIGMA_OBJECTS + SIGMA_EXTRACT_LOG; secrets/registry kept). Run bootstrap. =="
  else
    echo "== reset done (secrets + RAW_SIGMA_OBJECTS preserved); run bootstrap or deploy-procs/deploy-views =="
  fi
}

cmd_teardown() {
  # Full clean-down: DROP the audit database (cascades schema, tables, views,
  # procs, network rule + all secrets) and the account-level external-access
  # integration -- i.e. everything `setup` created. Destructive + irreversible;
  # requires interactive confirmation unless --yes is passed. Re-provision with
  # setup -> registry -> bootstrap.
  if [[ "$ASSUME_YES" != 1 ]]; then
    if [[ ! -t 0 ]]; then
      echo "teardown needs confirmation; re-run with --yes for non-interactive use." >&2
      exit 2
    fi
    printf 'This DROPS DATABASE %s and INTEGRATION sigma_api_access (irreversible).\nType the database name to confirm: ' "$DB"
    read -r ans
    [[ "$ans" == "$DB" ]] || { echo "confirmation did not match '$DB'; aborted." >&2; exit 2; }
  fi
  # No --database/--schema (we are dropping it); admin role for the integration.
  snow sql ${CONN_ARGS[@]+"${CONN_ARGS[@]}"} --role "$ADMIN_ROLE" --warehouse "$WH" \
    -q "DROP DATABASE IF EXISTS \"$DB\"; DROP INTEGRATION IF EXISTS sigma_api_access;" 2>&1 | _filter || true
  echo "== teardown complete: dropped database $DB + integration sigma_api_access. Re-provision: setup -> registry -> bootstrap. =="
}

# ---- privileged: setup + registry (env-injected via 0600 temp file) ----------
# Redact the two secret values from any streamed output.
_redact() {
  python3 -c "import sys,os
red=[v for v in (os.environ.get('SIGMA_CLIENT_SECRET',''), os.environ.get('SIGMA_CLIENT_ID','')) if v]
for line in sys.stdin:
    for r in red: line=line.replace(r,'***')
    sys.stdout.write(line)"
}

cmd_setup() {
  : "${SIGMA_BASE_URL:?}"; : "${SIGMA_CLIENT_ID:?}"; : "${SIGMA_CLIENT_SECRET:?}"
  local tmp; tmp="$(mktemp -t soa_setup.XXXXXX.sql)"; chmod 600 "$tmp"
  SOA_DB="$DB" SOA_SCHEMA="$SCHEMA" SOA_GRANT_ROLE="$ROLE" SOA_ADMIN_ROLE="$ADMIN_ROLE" \
  python3 - "$tmp" <<'PY'
import os, sys
base, cid, sec = os.environ["SIGMA_BASE_URL"], os.environ["SIGMA_CLIENT_ID"], os.environ["SIGMA_CLIENT_SECRET"]
db, schema = os.environ["SOA_DB"], os.environ["SOA_SCHEMA"]
grant_role, admin_role = os.environ["SOA_GRANT_ROLE"], os.environ["SOA_ADMIN_ROLE"]
hosts = ["aws-api.sigmacomputing.com","api.us-a.aws.sigmacomputing.com","api.eu.aws.sigmacomputing.com",
         "api.uk.aws.sigmacomputing.com","api.ca.aws.sigmacomputing.com","api.au.aws.sigmacomputing.com",
         "api.us.azure.sigmacomputing.com","api.eu.azure.sigmacomputing.com","api.sigmacomputing.com"]
vlist = ",\n    ".join(f"'{h}:443'" for h in hosts)
open(sys.argv[1],"w").write(f"""
-- build role owns the audit DB/schema so it can deploy procs/views/tables
USE ROLE {grant_role};
CREATE DATABASE IF NOT EXISTS {db};
CREATE SCHEMA   IF NOT EXISTS {db}.{schema};
-- account-level objects (network rule, secrets, integration) as the admin role
USE ROLE {admin_role};
USE DATABASE {db};
USE SCHEMA {schema};
CREATE OR REPLACE NETWORK RULE sigma_api_network_rule MODE=EGRESS TYPE=HOST_PORT
  VALUE_LIST = (\n    {vlist}\n  );
CREATE OR REPLACE SECRET sigma_base_url      TYPE=GENERIC_STRING SECRET_STRING='{base}';
CREATE OR REPLACE SECRET sigma_client_id     TYPE=GENERIC_STRING SECRET_STRING='{cid}';
CREATE OR REPLACE SECRET sigma_client_secret TYPE=GENERIC_STRING SECRET_STRING='{sec}';
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION sigma_api_access
  ALLOWED_NETWORK_RULES=(sigma_api_network_rule)
  ALLOWED_AUTHENTICATION_SECRETS=(sigma_base_url, sigma_client_id, sigma_client_secret)
  ENABLED=TRUE;
GRANT USAGE ON INTEGRATION sigma_api_access TO ROLE {grant_role};
GRANT READ ON SECRET sigma_base_url      TO ROLE {grant_role};
GRANT READ ON SECRET sigma_client_id     TO ROLE {grant_role};
GRANT READ ON SECRET sigma_client_secret TO ROLE {grant_role};
""")
PY
  # No --database/--schema here: the script CREATEs and USEs them (they may not exist yet).
  snow sql ${CONN_ARGS[@]+"${CONN_ARGS[@]}"} --warehouse "$WH" -f "$tmp" 2>&1 | _filter | _redact
  rm -f "$tmp"; echo "== setup complete (db=$DB schema=$SCHEMA; temp file deleted) =="
}

# Seed/replace the org registry secret. Two modes:
#   --file orgs.json   -> load a JSON array of orgs (the multi-org path)
#   (default)          -> one org from env vars, label via --label, role --org-role
cmd_registry() {
  local tmp; tmp="$(mktemp -t soa_reg.XXXXXX.sql)"; chmod 600 "$tmp"
  if [[ -n "$REG_FILE" ]]; then
    [[ -f "$REG_FILE" ]] || { echo "registry file not found: $REG_FILE" >&2; exit 2; }
    REG_FILE="$REG_FILE" ROLE_FOR_GRANTS="$ROLE" python3 - "$tmp" <<'PY'
import os, sys, json
orgs = json.load(open(os.environ["REG_FILE"]))
assert isinstance(orgs, list) and orgs, "registry file must be a non-empty JSON array"
for o in orgs:
    for k in ("label","baseUrl","clientId","clientSecret"):
        assert o.get(k), f"org {o.get('label','?')} missing '{k}'"
    o.setdefault("role","standalone"); o.setdefault("enabled", True)
j = json.dumps(orgs).replace("'","''")
open(sys.argv[1],"w").write(f"""
CREATE OR REPLACE SECRET sigma_tenant_registry TYPE=GENERIC_STRING SECRET_STRING='{j}';
ALTER EXTERNAL ACCESS INTEGRATION sigma_api_access
  SET ALLOWED_AUTHENTICATION_SECRETS=(sigma_base_url, sigma_client_id, sigma_client_secret, sigma_tenant_registry);
GRANT READ ON SECRET sigma_tenant_registry TO ROLE {os.environ['ROLE_FOR_GRANTS']};
""")
print(f"loaded {len(orgs)} org(s) from {os.environ['REG_FILE']}: " + ", ".join(o['label'] for o in orgs), file=sys.stderr)
PY
  else
    : "${SIGMA_BASE_URL:?}"; : "${SIGMA_CLIENT_ID:?}"; : "${SIGMA_CLIENT_SECRET:?}"
    ROLE_FOR_GRANTS="$ROLE" LABEL="${LABEL:-primary}" ORG_ROLE_VAL="$ORG_ROLE_VAL" python3 - "$tmp" <<'PY'
import os, sys, json
reg=[{"label":os.environ["LABEL"],"baseUrl":os.environ["SIGMA_BASE_URL"],
      "clientId":os.environ["SIGMA_CLIENT_ID"],"clientSecret":os.environ["SIGMA_CLIENT_SECRET"],
      "role":os.environ["ORG_ROLE_VAL"],"enabled":True}]
j=json.dumps(reg).replace("'","''")
open(sys.argv[1],"w").write(f"""
CREATE OR REPLACE SECRET sigma_tenant_registry TYPE=GENERIC_STRING SECRET_STRING='{j}';
ALTER EXTERNAL ACCESS INTEGRATION sigma_api_access
  SET ALLOWED_AUTHENTICATION_SECRETS=(sigma_base_url, sigma_client_id, sigma_client_secret, sigma_tenant_registry);
GRANT READ ON SECRET sigma_tenant_registry TO ROLE {os.environ['ROLE_FOR_GRANTS']};
""")
print(f"seeded 1 org from env: {os.environ['LABEL']} (role={os.environ['ORG_ROLE_VAL']})", file=sys.stderr)
PY
  fi
  # Drop the secret-bearing CREATE line from echoed output; redact env creds elsewhere.
  sfa -f "$tmp" 2>&1 | _filter | grep -v -i "SECRET_STRING" | _redact
  rm -f "$tmp"
  echo "== registry set (temp file deleted). Re-run with --file to manage many orgs. =="
}

case "$CMD" in
  setup)        cmd_setup;;
  registry)     cmd_registry;;
  deploy-procs) cmd_deploy_procs;;
  deploy-views) cmd_deploy_views;;
  bootstrap|all) cmd_bootstrap;;
  refresh)      cmd_refresh;;
  reset)        cmd_reset;;
  teardown)     cmd_teardown;;
  help|*)       sed -n '2,/^# ===/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//';;
esac
