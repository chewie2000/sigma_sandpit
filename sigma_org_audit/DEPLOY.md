# Deploying sigma_org_audit

`deploy.sh` wraps the Snowflake CLI (`snow`) so you install and refresh the whole
pipeline with one command instead of running the SQL files by hand. It encodes the
dependency order and is idempotent.

## Prerequisites
- `snow` (Snowflake CLI) configured with a connection. If you don't pass `--conn`,
  the CLI's **default** connection is used — set one with
  `snow connection set-default <name>`, or pass `--conn <name>` each time.
- `SIGMA_BASE_URL`, `SIGMA_CLIENT_ID`, `SIGMA_CLIENT_SECRET` in the environment
  (used only by the privileged `setup` / `registry` commands; injected via a 0600
  temp file that is deleted after — never on the command line).

## One-time install
```bash
./deploy.sh setup       # ACCOUNTADMIN: network rule, secrets, integration, grants
./deploy.sh registry    # ACCOUNTADMIN: seed the tenant registry secret (1 org from env)
./deploy.sh bootstrap   # SYSADMIN: procs -> extract -> stage -> writeback -> history -> marts
```

## Day-to-day
```bash
./deploy.sh refresh            # re-extract all registered orgs + writeback + history
./deploy.sh refresh acme       # refresh just the org labelled 'acme'
./deploy.sh deploy-procs       # redeploy procedures after a code change
./deploy.sh deploy-views       # redeploy stage + mart views
./deploy.sh reset              # drop procs/views/SCD2 (keeps secrets + RAW), then rebuild
```

Flags (defaults): `--conn` *(your snow CLI default connection)* `--db SIGMA_ORG_AUDIT
--schema AUDIT --role SYSADMIN --warehouse COMPUTE_WH --admin-role ACCOUNTADMIN`.
A connection name is local to `~/.snowflake/connections.toml`, so `--conn` has no
portable hardcoded default; omit it to use the CLI default, or pass `--conn <name>`.

## Why the order matters
`bootstrap` runs procs → extract → **stage views** → writeback → **history**
(SCD2 tables) → **mart views**, because the drift mart (`V_WORKBOOK_DRIFT`)
references the `SCD2_*` tables and won't compile until `sigma_scd2_apply` has
created them. `deploy.sh` also `DROP`s the current proc signatures before
recreating, so an argument-count change never trips Snowflake's ambiguous-overload
error.

The ordered file lists in `deploy.sh` (`PROC_FILES`, `STAGE_FILES`, `MART_FILES`)
are the single source of deploy order. Everything else below reuses them.

---

## Path to option 2 — deploy from Git, inside Snowflake (no laptop/CLI)
Once the SQL lives in a git remote, Snowflake can deploy it natively, which makes
the tool trivially shareable and schedulable (no external runner):

```sql
-- one-time
CREATE OR REPLACE API INTEGRATION sigma_org_audit_git
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/<org>/')
  ENABLED = TRUE;
CREATE OR REPLACE GIT REPOSITORY sigma_org_audit_repo
  API_INTEGRATION = sigma_org_audit_git
  ORIGIN = 'https://github.com/<org>/<repo>.git';

-- deploy / update
ALTER GIT REPOSITORY sigma_org_audit_repo FETCH;
EXECUTE IMMEDIATE FROM
  @sigma_org_audit_repo/branches/main/sigma_org_audit/procs/sigma_org_extract.sql;
-- ...one EXECUTE IMMEDIATE FROM per file, in the deploy.sh order...
```

To make this one statement, add a thin `deploy.sql` that `EXECUTE IMMEDIATE FROM`s
each file in the `PROC_FILES`→`STAGE_FILES`→`MART_FILES` order (a Snowflake
Scripting block). Because the per-file artifacts are unchanged, option 2 is purely
additive — it reuses the same files and order this script already defines. The
privileged `setup`/`registry` steps still run once (they carry secrets).

## Path to option 3 — change management (only if needed)
- **schemachange** (Snowflake-Labs): put the files under `versioned/` (`V1.1__...`)
  and let it track what's been applied — useful when you want an applied-change
  ledger across environments.
- **dbt**: the `STG_*`/`V_*` views are a natural dbt project (models + `ref()` DAG
  + tests + lineage). The extraction procs stay outside dbt. Worth it only if you
  want view lineage/testing; overkill for the current view count.
