# sigma_org_audit

> **Proof of concept.** This is a reference implementation shared to demonstrate an approach and give others something to extrapolate from — not a finished, supported, or authoritative tool. Take the ideas, adapt the patterns, build your own.

> **Disclaimer:** This project contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A Snowflake-native toolkit that builds a **replayable audit of a Sigma
organisation** from the Sigma REST API, for internal governance checks and
migration-readiness assessment (single-org or multi-tenant). It covers the core
content, people, writeback, tenancy, and data-isolation surface today — see
[**Scope**](#scope--whats-covered-vs-not-yet) for exactly what is and isn't captured.

## The idea — a replayable three-layer pipeline

```
Sigma REST API  ─┐
                 ├─►  RAW  ──►  STAGE  ──►  MARTS  ──►  audit_queries / Sigma workbook
Writeback schemas┘   (VARIANT  (typed     (SCD2 history
                      snapshots) latest    + scoring + drift
                      append-only) state)   + governance)
```

- **Raw** — append-only `VARIANT` snapshots of every API object, one row per
  object per snapshot. Nothing is transformed; a new API field is absorbed
  without code changes. Everything downstream is rebuildable from here.
- **Stage** — views only, zero business logic, latest-state per object.
- **Marts** — SCD2 history (derived from raw snapshots, so disposable and
  replayable), plus scoring / drift / governance views built for consumption.

This shape is the distinguishing choice: a bug in transform logic, or a need to
backfill a new computed column, never requires re-hitting the API — you rebuild
stage/marts from raw.

## Scope — what's covered vs. not yet

**Captured & analysed today**

- **Content:** workbooks (+ sources), data models (+ detail), datasets
  (deprecated), connections (+ writeback locations).
- **People & access:** members, teams, artifact grants (inode-level for
  workbooks / datasets / data models).
- **Tenancy & deployment:** org role (parent / child / standalone), tenants,
  deployment policies, source-swap policies.
- **Data isolation:** user attributes + their user/team/tenant bindings, with a
  heuristic "used in a data model" (RLS) signal.
- **Writeback:** SIGDS input tables + WAL activity, archival scoring, and
  cross-org attribution for shared writeback schemas.
- **Derived:** inventory, ownership cleanup, object lifecycle (deletion
  detection), R/A/G migration scoring (dataset→model), SCD2 history + drift,
  tenancy topology, data-isolation posture.

**Not captured yet** (on the roadmap)

- Workspaces (+ grants); the real folder/file tree (only path *strings* today);
  connection-level grants.
- Account types & permissions; API credentials / connectors; SAML/SSO config.
- Pixel-perfect **reports**; tags / version tags; templates.
- Schedules (exports), materialization schedules, embeds, cross-org shares,
  bookmarks.
- Workbook **internals** — elements / pages / controls / queries / lineage
  (needed for source-binding & complexity / t-shirt sizing); per-tenant
  deployment detail (what's deployed into each tenant).

**Known limitations**

- **Admin/org settings aren't API-readable** — `/v2/organizations/settings` is
  PATCH-only (no GET). Auth (SAML), account types, and user attributes are
  readable via their own endpoints; the monolithic settings object is not.
- **Visibility scoping is uneven** — workbooks & datasets are fetched org-wide
  (`skipPermissionCheck`); data models, connections, members, teams are not, so
  those lists may reflect only what the token can see. No completeness flag yet.
- **Scale unproven** — validated against one small org; list pagination and the
  per-object detail fan-out haven't been exercised at large scale.
- **Child org-role is operator-asserted** — a child can't self-identify via the
  tenants API (403 from inside), so `role` comes from the registry, not the API.
- **Snowflake-only writeback** — connections to other accounts or Databricks are
  inventoried but their writeback contents are skipped (`SCAN_REACHABLE = FALSE`).
- **RLS signal is heuristic** — a string match of the attribute in a model spec,
  not a parsed security policy.

This is a proof-of-concept under active development; the lists above change as the
coverage epic progresses.

## Files

| File | Purpose |
|---|---|
| `deploy.sh` / `DEPLOY.md` | One-command deploy/refresh wrapper around `snow`, and its guide. |
| `setup_prerequisites.sql` | One-time ACCOUNTADMIN setup: network rule, secrets, external access integration, grants (+ optional tenant registry). |
| `api_flow.md` | Endpoint catalog + what each call lands in `RAW_SIGMA_OBJECTS`. |
| `procs/sigma_org_extract.sql` | Raw extraction proc — lands every API object type (incl. tenancy + user attributes) as VARIANT snapshots; org-role detection. |
| `procs/sigma_org_extract_all.sql` | Multi-org fan-out — reads the tenant-registry secret and runs the extract per org (refresh all, or one by label). |
| `procs/sigma_writeback_scan.sql` | Writeback audit — discovers writeback/WAL schemas from connections, scans SIGDS tables + WAL activity. |
| `stage/stage_views.sql` | `STG_*` views: typed latest-state flatten (workbooks, models, connections, writeback, members/teams, tenancy, user attributes). |
| `marts/scd2_history.sql` | `sigma_scd2_apply` — generic type-2 history builder for any stage view. |
| `marts/mart_views.sql` | Inventory + lifecycle (deletion), R/A/G migration scoring, writeback governance (cross-org attribution), ownership, drift, tenancy topology, data isolation. |
| `audit_queries.sql` | Ready-to-run governance & migration-readiness queries. |

A companion **`sigma-org-audit` Claude Code skill** (in `../sigma_skills/`) drives
this pipeline and interprets the results, optionally cross-checking against live
data via the **`sigma-cli` sub-skill**.

## Install

### Prerequisites
- The **Snowflake CLI** (`snow`) configured with a connection (a profile in
  `~/.snowflake/connections.toml`). Either set it as your default
  (`snow connection set-default <name>`) or pass `--conn <name>` to every command.
- One Sigma org's **admin** API credentials exported as environment variables —
  `SIGMA_BASE_URL`, `SIGMA_CLIENT_ID`, `SIGMA_CLIENT_SECRET`. Generate the
  client id/secret at *Administration → Developer Access*; the host comes from the
  table in `setup_prerequisites.sql`. Set them in your shell:
  ```bash
  export SIGMA_BASE_URL=https://aws-api.sigmacomputing.com
  export SIGMA_CLIENT_ID=...
  export SIGMA_CLIENT_SECRET=...
  ```
  For a persistent setup put those lines in your shell profile (`~/.zshenv`,
  `~/.zshrc`, or `~/.bashrc`) so every session has them. They are read **only** by
  `./deploy.sh setup` and `./deploy.sh registry` (to seed the one env-based org);
  the multi-org `registry --file orgs.json` path carries each org's creds in the
  file instead, so the env vars aren't needed for it.
- Snowflake rights: **ACCOUNTADMIN** for the one-time setup; a build role
  (e.g. **SYSADMIN**) for everything else.

### Quick start (recommended)
```bash
cd sigma_org_audit
./deploy.sh setup       # 1. once, ACCOUNTADMIN — network rule, secrets, integration, grants
./deploy.sh registry    # 2. once, ACCOUNTADMIN — seed the org registry from your env vars
./deploy.sh bootstrap   # 3. build + first load: procs -> extract -> views -> history -> marts
```
Then verify and explore:
```bash
# Acceptance checks — expect all PASS / INFO
snow sql -c <conn> --role SYSADMIN --database SIGMA_ORG_AUDIT --schema AUDIT \
  --warehouse <wh> -f tests/acceptance_checks.sql
# The report queries
snow sql -c <conn> --role SYSADMIN --database SIGMA_ORG_AUDIT --schema AUDIT \
  --warehouse <wh> -f audit_queries.sql
```
Re-pull data anytime with `./deploy.sh refresh` (all orgs) or
`./deploy.sh refresh <label>` (one org).

**Where does it install?** Into a database + schema chosen by the `--db` / `--schema`
flags (defaults `SIGMA_ORG_AUDIT.AUDIT`). `setup` **creates** them; every later
command creates its objects there. See the command + flag tables below.

> The three steps must run **in order** — `bootstrap` reads the registry created
> by `registry`, which uses the integration created by `setup`.

### `deploy.sh` commands

| Command | What it does |
|---|---|
| `setup` | (ACCOUNTADMIN) create the audit DB + schema, network rule, secrets, integration, grants |
| `registry` | (ACCOUNTADMIN) seed the org-registry secret from your env vars (one org) |
| `bootstrap` | full build + first load: procs → extract → stage → writeback → history → marts |
| `refresh [label]` | re-pull data: all enabled orgs, or just `label` |
| `deploy-procs` | (re)create the stored procedures only |
| `deploy-views` | (re)create the stage + mart views only |
| `reset` | drop procs/views/SCD2 (keeps secrets + raw), ready to rebuild |
| `help` | usage |

### `deploy.sh` flags

Append to any command (e.g. `./deploy.sh bootstrap --db MY_DB --schema MY_SCHEMA`):

| Flag | Default | Meaning |
|---|---|---|
| `--conn` | *your snow CLI default connection* | Snowflake CLI connection name |
| `--db` | `SIGMA_ORG_AUDIT` | target database (created by `setup`) |
| `--schema` | `AUDIT` | target schema (created by `setup`) |
| `--role` | `SYSADMIN` | build/execution role (owns the DB/schema, runs procs) |
| `--warehouse` | `COMPUTE_WH` | warehouse for compute |
| `--admin-role` | `ACCOUNTADMIN` | role for the privileged `setup` step |

**Connection:** a connection name is local to your `~/.snowflake/connections.toml`,
so there is no portable hardcoded default. If you don't pass `--conn`, the script
omits `-c` and the **Snowflake CLI's own default connection** is used. Set one once:
```bash
snow connection set-default <your-connection-name>
```
or pass `--conn <name>` on every command to target a specific profile.

> Pass the **same** `--conn/--db/--schema` to every command so all objects land in
> one place. The non-connection defaults (`SIGMA_ORG_AUDIT.AUDIT`, `SYSADMIN`,
> `COMPUTE_WH`) match the reference sandbox; override for your own.

### Manual install (no CLI / running SQL in Snowsight)
`deploy.sh` just runs the SQL files in dependency order. To do it by hand:

1. **Setup (ACCOUNTADMIN):** fill the placeholders in `setup_prerequisites.sql` and run it.
2. **Procedures:** run `procs/sigma_org_extract.sql`, `procs/sigma_writeback_scan.sql`,
   `procs/sigma_org_extract_all.sql`, `marts/scd2_history.sql`.
3. **Extract** (set `USE DATABASE`/`USE SCHEMA` first):
   `CALL sigma_org_extract('DB','SCHEMA');` then `CALL sigma_writeback_scan('DB','SCHEMA');`
4. **Stage views:** run `stage/stage_views.sql`.
5. **History:** the four `CALL sigma_scd2_apply(...)` statements (see `audit_queries.sql`
   / the worked example below).
6. **Mart views:** run `marts/mart_views.sql`.
7. **Query:** `audit_queries.sql`.

> **Order matters** (the reason `deploy.sh` exists): procs → extract → writeback →
> stage → **history → marts**. `marts/mart_views.sql` creates `V_WORKBOOK_DRIFT`,
> which needs the `SCD2_*` tables, so history must run first. Also: if a procedure's
> **parameter count** changes between versions, `DROP PROCEDURE <name>(<types>)`
> before re-creating (Snowflake rejects the ambiguous overload).

## Multiple orgs and tenants

Multi-org by default: every extract calls `GET /v2/whoami` and stamps **`ORG_ID`**
on each row of `RAW_SIGMA_OBJECTS`, so one warehouse holds many orgs side by side —
a parent + its tenants, unrelated orgs, or any mix. Every `STG_*`/`V_*` view carries
`ORG_ID`, so you scope a report with `WHERE ORG_ID = '<uuid>'` or compare orgs with
`GROUP BY ORG_ID`. (Query 0 in `audit_queries.sql` lists the orgs present.)

**Almost everything is built once and shared** — the setup objects, the audit
database/schema, all procedures, the `STG_*`/`V_*` views, and the org-tagged
`RAW_SIGMA_OBJECTS` table. Only the *extract* runs per org, and
`sigma_writeback_scan` / `sigma_scd2_apply` are a single call each covering every
org in the raw table. You never rebuild views per org.

### The org registry (how `deploy.sh` does multi-org)

The registry is **one Snowflake secret** (`sigma_tenant_registry`) holding a JSON
array of orgs; `sigma_org_extract_all` loops over it. (A stored proc can only read
statically-declared secrets, so one registry secret — bound once — scales to any
number of orgs with no proc/integration change.) `./deploy.sh registry` writes that
secret for you; you don't hand-write SQL.

**Where do multiple orgs' credentials live?** The `SIGMA_*` env vars hold exactly
**one** org. For several orgs you keep an **`orgs.json` file** as your source of
truth — one object per org — and apply it with `./deploy.sh registry --file
orgs.json`. That file is what you edit and re-apply whenever an org is added,
removed, rotated, or enabled/disabled; keep it local (0600, git-ignored). The env
vars are still needed **once** for `./deploy.sh setup` (which creates the base
secrets + integration), but the per-org audit credentials for a fleet live in
`orgs.json`, not the environment.

| You have… | Keep credentials in | Apply with |
|---|---|---|
| one org | `SIGMA_*` env vars (`~/.zshenv`) | `./deploy.sh registry` |
| many orgs / tenants | `orgs.json` (one object per org) | `./deploy.sh registry --file orgs.json` |

**Each org needs:** `label` (your handle), `baseUrl` (the org's API host — see the
table in `setup_prerequisites.sql`), `clientId` + `clientSecret` (generate in *that
org's* Administration → Developer Access), `role` (`parent` / `child` / `standalone`
— asserted, since a child can't self-identify via the tenants API), and `enabled`.

**One org (from your env vars)** — the quick-start default:
```bash
./deploy.sh registry                              # label "primary", role "child"
./deploy.sh registry --label acme --org-role parent
```

**Many orgs (from a JSON file)** — the multi-tenant path:
```bash
cp orgs.example.json orgs.json     # then edit: one object per org
chmod 600 orgs.json                # it holds plaintext secrets
./deploy.sh registry --file orgs.json
```
`orgs.json` is git-ignored. The file shape is in
[`orgs.example.json`](orgs.example.json). Re-running `registry` (either mode)
replaces the whole registry — to add/remove an org, edit `orgs.json` and re-run.

**Then run the audit** for all orgs, or one by label:
```bash
./deploy.sh refresh            # all enabled orgs
./deploy.sh refresh acme       # just 'acme'
```
`sigma_org_extract_all` returns a per-org summary; one org's failure (e.g. a 403)
doesn't abort the batch.

- **Trigger from Sigma:** a *Refresh all* button → the no-label call; a *Refresh
  this org* button → with a label. Via a Call API action to the Snowflake SQL API,
  or a scheduled Task. Creds pass as bound call args, not logged SQL.
- **Security:** registry secrets live in Snowflake's secret store; the `CREATE
  SECRET` statement does land in Snowflake query history (inherent). Keep
  `orgs.json` local (0600, git-ignored).

> **Alternative store:** keep the org list (and secrets) in a **table** instead of
> the registry secret — easier to manage, can even be a Sigma input table for
> self-service onboarding — at the cost of holding secrets in a column.

### Without the registry (single org)

If you're auditing just one org, you don't need the registry — the base
`sigma_base_url` / `sigma_client_id` / `sigma_client_secret` secrets created by
setup are enough: `CALL sigma_org_extract('SIGMA_ORG_AUDIT','AUDIT');`. To point at
a different single org, either rotate those three secrets, or pass per-call
override params (`... , baseUrl, clientId, clientSecret` — convenient, but the
secret then appears in query history). The network rule already allows egress to
every Sigma API host, so any cloud/region is reachable without re-running setup.

## Writeback scan — discovered, not configured

`sigma_writeback_scan` does **not** take a schema parameter. It reads the
writeback schema locations straight from each connection's detail payload
(`STG_CONNECTIONS`), so it self-discovers every writeback schema in the org and
attributes each `SIGDS_*` table back to its owning connection. This is why
`sigma_org_extract` must run first.

**WAL location:** by Sigma's design the write-access destination schema is
reserved for *all* internal write-back objects, so the `SIGDS_WAL_*` edit-log
tables live in the **writeback schema(s)** alongside the `SIGDS_*` data tables —
not necessarily in a separate audit-log schema. The scan therefore looks for WAL
tables in each writeback location first, and only also uses the connection's
`inputTableAuditLogSchema` if it is set and points somewhere different. (The API's
`inputTableAuditLogSchema` field is frequently null even when WAL tables exist, so
it is not relied on as the primary source.)

**Reachability:** the connections API reveals *where* every writeback schema is,
but the scan can only read locations the executing Snowflake role has access to.
Connections pointing at another account/warehouse (or Databricks) are still
inventoried from the API, but their table/WAL contents are skipped and the
connection is flagged `SCAN_REACHABLE = FALSE` — a Phase-2 (cross-account
runner) concern.

**Shared writeback schemas / cross-org attribution:** a single warehouse schema
can be the writeback destination for *several* Sigma orgs. A `SIGDS_*` table is
physically discovered via whichever org's connection points at the schema, but
its true owner is read from the WAL workbook URL/ID. `V_WRITEBACK_GOVERNANCE`
therefore classifies every table by `ATTRIBUTION` — `OWNED`, `ORPHANED`,
`CROSS_ORG`, or `UNATTRIBUTED` — and only `ORPHANED` tables (confidently this
org's, with no live workbook) count toward its archival score and reclaimable
storage. Without this, one org's orphan/storage numbers would be inflated by
every other org's tables sharing the schema. `V_WRITEBACK_SHARED_SCHEMAS` flags
which writeback schemas are shared across orgs.

## Out of scope (Phase 2)

- The consuming Sigma workbook spec (code representation).
- The Call-API action / remediation layer (buttons that transfer ownership,
  retag, swap sources, archive — writing to an audit trail).
- Cross-account / Databricks writeback deep scans.

## Lineage / prior art

Reuses proven patterns from sibling projects in this repo: the
external-network-access + token/backoff/secrets stored-proc pattern from
`DataSetMigrateHelper_SF`, and the SIGDS/WAL writeback inventory + archival
scoring from `writeback_info_sf`.
