# Sigma API flow — sigma_org_audit

How the extraction procedures talk to the Sigma REST API, what each call returns,
and where it lands in `RAW_SIGMA_OBJECTS`. Endpoint shapes were cross-checked
against the Sigma API reference (see the `Sigma_Docs` MCP / the official docs).

All calls are authenticated with an OAuth 2.0 client-credentials bearer token
(`POST /v2/auth/token`), refreshed transparently 60 s before expiry by
`SigmaTokenManager`. Every GET goes through `_get_with_backoff` (exponential
backoff on 429 / 5xx, 4 attempts). List endpoints page on `hasMore`/`nextPage`
or `nextPageToken`, `limit=500`.

## Authentication

| Method | Path | Purpose |
|---|---|---|
| POST | `/v2/auth/token` | Exchange `client_id` + `client_secret` for a bearer token. |

`grant_type=client_credentials`, form-encoded. Admin-scoped credentials are
required for `skipPermissionCheck=true` (org-wide visibility).

## Org identity

| Method | Path | Purpose |
|---|---|---|
| GET | `/v2/whoami` | Returns `{userId, organizationId}`. Called once at the start of every extract to resolve the **`ORG_ID`** stamped on every landed row (multi-org tagging). Not stored as its own OBJECT_TYPE. |

## Tenancy + deployment topology (multi-tenant migration)

Captured per extract; each is Beta/entitled, so a 403 is recorded in the result
`errors` (and the org role becomes `unknown`), never fatal.

| OBJECT_TYPE | Method | Path | Notes |
|---|---|---|---|
| `tenant` | GET | `/v2/tenants` | Tenant (child) orgs of a parent/host org. Non-empty => caller is a PARENT. 403 if caller lacks tenant-scope (e.g. a child's own creds). |
| `tenant_self` | GET | `/v2/tenants/{ownOrgId}` | Self-lookup: a populated `parentOrganizationId` confirms the caller is a CHILD. Needs tenant-scoped access; a child's own creds are typically 403. |
| `deployment_policy` | GET | `/v2/deploymentPolicies` | Beta. Governs deploying content to tenants. |
| `deployment_policy_detail` | GET | `/v2/deploymentPolicies/{id}/tenants` + `/files` | Per-policy target tenants + documents. |
| `source_swap_policy` | GET | `/v2/sourceSwapPolicies` | Beta. Per-tenant source remapping. |
| `organization` | (derived) | — | Synthetic row: `{organizationId, role, roleSource, parentOrganizationId, tenantCount, tenantsListError, tenantSelfError, deploymentPolicyCount, sourceSwapPolicyCount}`. |

**Role classification:** non-empty `/v2/tenants` => `parent`; `parentOrganizationId`
from the self-lookup => `child`; reachable-but-empty list => `standalone`;
both calls denied (403) => `indeterminate`. Because a child org's own credentials
are usually denied on the tenants API, a child **cannot** self-identify from
inside — pass the `ORG_ROLE_OVERRIDE` parameter (e.g. `'child'`) to record the
role authoritatively when known out-of-band; it is tagged `roleSource = operator`
vs `api`.

## `sigma_org_extract` — API object types

Each row in `RAW_SIGMA_OBJECTS` is `(SNAPSHOT_ID, SNAPSHOT_TS, ORG_ID,
OBJECT_TYPE, OBJECT_ID, PAYLOAD, EXTRACTED_AT)`. PAYLOAD is the unparsed JSON
object; `ORG_ID` is the `organizationId` from `/v2/whoami`, so one raw table can
hold many orgs.

| OBJECT_TYPE | Method | Path | Notes |
|---|---|---|---|
| `workbook` | GET | `/v2/workbooks?skipPermissionCheck=true&limit=500` | Org-wide list. Admin. Paged. |
| `workbook_sources` | GET | `/v2/workbooks/{id}/sources` | One call per workbook (thread pool). Lands `{workbookId, sources[]}`. |
| `datamodel` | GET | `/v2/dataModels?limit=500` | List. Paged. |
| `datamodel_detail` | GET | `/v2/dataModels/{id}` | One call per data model (thread pool). Full metadata/spec. |
| `dataset` | GET | `/v2/datasets?skipPermissionCheck=true&limit=500` | Org-wide list. Admin. Exposes `migrationStatus`. Paged. |
| `connection` | GET | `/v2/connections?limit=500` | List (inventory). Paged. |
| `connection_detail` | GET | `/v2/connections/{id}` | One call per connection (thread pool). **Only place** `writebackSchemas[]`, `writebacks[]`, and `inputTableAuditLogSchema` are returned — drives the writeback scan. |
| `member` | GET | `/v2/members?limit=500` | List. Paged. Display-name resolution. |
| `team` | GET | `/v2/teams?limit=500` | List. Paged. |
| `grant` | GET | `/v2/grants?inodeId={id}` | Optional (`INCLUDE_GRANTS`). One call per workbook/dataset/datamodel inode. Lands `{inodeId, artifactType, grants[]}`. |

### Connection detail payload — the writeback locations

`GET /v2/connections/{connectionId}` returns (relevant fields):

```jsonc
{
  "connectionId": "...",
  "type": "snowflake",            // warehouse type
  "account": "...", "warehouse": "...",
  "writebackSchemas": [           // OAuth connections: write-back output location
    { "database": "DB", "schema": "SCH", "writebackSchemaId": "..." }
  ],
  "writebacks": [                 // non-OAuth connections: same shape
    { "database": "DB", "schema": "SCH", "writebackSchemaId": "..." }
  ],
  "inputTableAuditLogSchema": {   // the input-table write-ahead-log (WAL) schema
    "database": "DB", "schema": "SCH", "writebackSchemaId": "..."
  }
}
```

`STG_CONNECTIONS` flattens these into typed columns; `sigma_writeback_scan`
reads them to know which schemas to scan.

## `sigma_writeback_scan` — writeback object types

Targets are **discovered** from `connection_detail` rows (no manual schema
param). For each connection's writeback / WAL location the role can read:

| OBJECT_TYPE | Source | Notes |
|---|---|---|
| `writeback_table` | `INFORMATION_SCHEMA.TABLES` in each discovered writeback DB/schema | One row per input/writeback **data** table (arbitrary names; `SIGDS_WAL_*` excluded): row count, bytes, created/last-altered, tagged with `connectionId`. |
| `writeback_wal` | the `SIGDS_WAL_*` tables in the connection's `inputTableAuditLogSchema` | Watermark per WAL table: `MAX(EDIT_NUM)`, edit count, last edit ts, and the data table name + workbook/element/user metadata extracted from the WAL `METADATA`/`EDIT` JSON. |

Connections whose writeback location is in another account/warehouse (or on
Databricks) are inventoried from the API but flagged `SCAN_REACHABLE = FALSE`;
their table/WAL contents are a Phase-2 (cross-account runner) concern.

## Pagination dialects

Two are observed across the API; `list_paginated` handles both:

- `{ "entries": [...], "hasMore": true, "nextPage": "<token>" }`
- `{ "entries": [...], "nextPageToken": "<token>" }`

## Rate limiting

- `_get_with_backoff`: retry on 429 / 5xx, wait `2^(attempt+1)` s, max 4 attempts.
- Per-object detail fan-out (connections, workbook sources, data models, grants)
  runs through a `ThreadPoolExecutor` (`MAX_WORKERS`, default 10) sharing one
  lock-protected token manager.
