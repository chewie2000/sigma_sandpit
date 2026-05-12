# DataSetMigrateHelper_SF — API Flow Reference

This document describes which Sigma API endpoints each stored procedure calls, in what order, and how the data returned by each call ends up in the output tables. All three procedures share the same authentication pattern and credential setup.

---

## Authentication (all procedures)

Every procedure starts by calling the Sigma token endpoint using credentials stored in Snowflake Secrets:

```
POST /v2/auth/token
  grant_type=client_credentials
  client_id=<from secret>
  client_secret=<from secret>
```

Returns a bearer token used for all subsequent API calls. The token is cached in memory and transparently refreshed before expiry (60-second safety margin). In `sigma_artifact_grants`, a threading lock protects the token manager so concurrent workers share a single token without race conditions.

---

## Procedure 1 — `sigma_dataset_dependencies`

**Output table:** `SIGMA_DATASET_DEPENDENCIES`

### API call sequence

```
1. POST /v2/auth/token
        │
        ▼
2. GET /v2/members?limit=500          [paginated]
        │
        │  Builds members_by_id dict: memberId → display name
        │  Used to resolve DATASET_CREATED_BY_NAME, DATASET_OWNER_NAME, MIGRATED_BY_NAME
        ▼
3. GET /v2/connections?limit=500      [paginated]
        │
        │  Builds connections_by_id dict: connectionId → {name, type}
        │  Used later when resolving table sources to a connection
        ▼
4. GET /v2/datamodels?limit=500       [paginated]
        │
        │  Returns data models owned by the API credential user.
        │  Note: skipPermissionCheck is not supported by this endpoint —
        │  models owned by other users are absent from this list but may
        │  still appear as migration targets.
        ▼
5. GET /v2/datasets?skipPermissionCheck=true&limit=500   [paginated]
        │
        │  Returns all datasets org-wide with migrationStatus:
        │    "not-migrated" | "migrated" | "not-required"
        │  Also carries migrationToDataModel.dataModelId for migrated datasets.
        │
        │  Any dataModelId referenced by a migrated dataset that was absent
        │  from step 4 is queued for individual fetch (step 6).
        ▼
6. GET /v2/datamodels/{dataModelId}   [one per missing data model]
        │
        │  Fetches data models owned by other users that appeared as
        │  migration targets in step 5 but were not in the list response.
        ▼
7. GET /v2/datasets/{datasetId}/sources   [one per dataset, sequential with 0.1s delay]
        │
        │  Returns source list for each dataset.
        │  Source type "dataset" → records a parent→child edge in the dependency graph.
        │  Source type "table"   → records the inodeId of the first table source
        │                          for connection resolution in step 8.
        ▼
8. GET /v2/connections/paths/{inodeId}    [one per unique table inode]
        │
        │  Maps a table-type inodeId to its connectionId.
        │  Combined with connections_by_id from step 3 to resolve
        │  CONNECTION_ID, CONNECTION_NAME, CONNECTION_TYPE.
```

### How API data maps to output columns

| Output column | Source |
|---|---|
| `DATASET_ID` | `/v2/datasets` → `datasetId` |
| `DATASET_NAME` | `/v2/datasets` → `name` |
| `DATASET_PATH` | `/v2/datasets` → `path` |
| `DATASET_URL` | `/v2/datasets` → `url` |
| `DATASET_CREATED_BY` | `/v2/datasets` → `createdBy` (raw member UID) |
| `DATASET_CREATED_BY_NAME` | Resolved via `/v2/members` lookup |
| `DATASET_OWNER` | `/v2/datasets` → `owner` (raw member UID) |
| `DATASET_OWNER_NAME` | Resolved via `/v2/members` lookup |
| `DATASET_MIGRATION_STATUS` | `/v2/datasets` → `migrationStatus` |
| `MIGRATED_AT` | `/v2/datasets` → `migrationToDataModel.migratedAt` |
| `MIGRATED_BY` | `/v2/datasets` → `migrationToDataModel.migratedBy` (raw member UID) |
| `MIGRATED_BY_NAME` | Resolved via `/v2/members` lookup |
| `DATA_MODEL_ID` | `/v2/datasets` → `migrationToDataModel.dataModelId` |
| `DATA_MODEL_NAME / PATH / URL` | `/v2/datamodels` or `/v2/datamodels/{id}` |
| `DATA_MODEL_CREATED_AT / UPDATED_AT` | `/v2/datamodels` → `createdAt / updatedAt` |
| `RELATION_TYPE` | Computed from the dependency graph (ROOT / INTERNAL / LEAF) |
| `PARENT_ID / PARENT_NAME` | Graph edge from `/v2/datasets/{id}/sources` |
| `UPSTREAM_PARENT_COUNT` | Count of direct parent datasets per node |
| `DOWNSTREAM_CHILD_COUNT` | Count of datasets that directly source from this node |
| `CONNECTION_ID / NAME / TYPE` | `/v2/connections/paths/{inodeId}` + `/v2/connections` |

### Performance note

Steps 1–6 are sequential. Step 7 is sequential with a 0.1-second delay between calls to avoid rate limiting. Step 8 runs sequentially over unique inodes only (deduped across all datasets).

---

## Procedure 2 — `sigma_workbook_source_map`

**Output tables:** `SIGMA_WORKBOOK_MIGRATION_SUMMARY` + `SIGMA_WORKBOOK_SOURCE_DETAILS`

**Prerequisite:** `sigma_dataset_dependencies` must have run first — this procedure reads `SIGMA_DATASET_DEPENDENCIES` from Snowflake to enrich workbook source rows. It does not re-call the dataset or data model APIs.

### API call sequence

```
1. POST /v2/auth/token
        │
        ▼
2. GET /v2/members?limit=500          [paginated]
        │
        │  Builds members_by_id dict for MIGRATED_BY_NAME resolution
        │  in SIGMA_WORKBOOK_SOURCE_DETAILS.
        ▼
3. [Snowflake] SELECT from SIGMA_DATASET_DEPENDENCIES (MAX RUN_ID)
        │
        │  Loads datasets_by_id and data_models_by_id dicts.
        │  These are used to determine whether a workbook source is
        │  within migration scope, and to enrich detail rows.
        ▼
4. GET /v2/workbooks?skipPermissionCheck=true&limit=500   [paginated]
        │
        │  Returns all workbooks org-wide including owner name/email
        │  (already a resolved object — no UID lookup needed here).
        ▼
5. GET /v2/workbooks/{workbookId}/sources   [one per workbook, concurrent — 10 workers]
        │
        │  Returns source list for each workbook.
        │  Source type "dataset"    → matched against datasets_by_id
        │  Source type "data-model" → matched against data_models_by_id
        │  Source type "table"      → counted but not in migration scope
        │
        │  Workbooks with no migration-scope sources are excluded from output.
```

### How API data maps to output columns

**SIGMA_WORKBOOK_MIGRATION_SUMMARY**

| Output column | Source |
|---|---|
| `WORKBOOK_ID` | `/v2/workbooks` → `workbookId` |
| `WORKBOOK_NAME / URL / PATH` | `/v2/workbooks` |
| `OWNER_NAME` | `/v2/workbooks` → `owner.name` (already resolved by API) |
| `OWNER_EMAIL` | `/v2/workbooks` → `owner.email` (already resolved by API) |
| `WORKBOOK_CREATED_AT / UPDATED_AT` | `/v2/workbooks` → `createdAt / updatedAt` |
| `TOTAL_SOURCES` | Count of in-scope sources from `/v2/workbooks/{id}/sources` |
| `DATASET_SOURCE_COUNT` | Count of in-scope "dataset" type sources |
| `DATA_MODEL_SOURCE_COUNT` | Count of in-scope "data-model" type sources |
| `TABLE_SOURCE_COUNT` | Count of "table" type sources (informational) |
| `MIGRATION_STATUS` | Computed: FULLY / PARTIALLY / NOT MIGRATED |

**SIGMA_WORKBOOK_SOURCE_DETAILS** (one row per in-scope source per workbook)

| Output column | Source |
|---|---|
| `WORKBOOK_ID / NAME / PATH` | `/v2/workbooks` |
| `SOURCE_INODE_ID` | `/v2/workbooks/{id}/sources` → `inodeId` |
| `SOURCE_DATA_MODEL_ID` | `/v2/workbooks/{id}/sources` → `dataModelId` |
| `SOURCE_TYPE` | `/v2/workbooks/{id}/sources` → `type` |
| `IN_MIGRATION_SCOPE` | Whether source matched a known dataset or data model |
| `DATASET_ID / NAME / PATH / URL` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |
| `DATASET_MIGRATION_STATUS` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |
| `DATASET_RELATION_TYPE` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |
| `MIGRATED_AT` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |
| `MIGRATED_BY` | Enriched from `SIGMA_DATASET_DEPENDENCIES` (raw UID) |
| `MIGRATED_BY_NAME` | Resolved via `/v2/members` lookup |
| `DATA_MODEL_ID / NAME / URL / PATH` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |
| `UPSTREAM_PARENT_COUNT` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |
| `DOWNSTREAM_CHILD_COUNT` | Enriched from `SIGMA_DATASET_DEPENDENCIES` |

### Performance note

Step 5 runs concurrently with a thread pool (default 10 workers). All other steps are sequential. The concurrency means 429 rate-limit backoff is possible — the `_get_with_backoff` helper handles this automatically with exponential backoff.

---

## Procedure 3 — `sigma_artifact_grants`

**Output table:** `SIGMA_ARTIFACT_GRANTS`

**Prerequisites:** Both `sigma_dataset_dependencies` and `sigma_workbook_source_map` must have run first — this procedure reads artifact IDs from both output tables rather than re-listing them from the API.

### API call sequence

```
1. POST /v2/auth/token
        │
        ▼
2. GET /v2/teams?limit=500            [paginated]
        │
        │  Builds teams_by_id dict: teamId → {name}
        │  Used to hydrate grantee display names for team grants.
        ▼
3. GET /v2/members?limit=500          [paginated]
        │
        │  Builds members_by_id dict: memberId → {firstName, lastName, email}
        │  Used to hydrate grantee display names for member grants.
        ▼
4. [Snowflake] SELECT DISTINCT DATASET_ID from SIGMA_DATASET_DEPENDENCIES
              SELECT DISTINCT DATA_MODEL_ID from SIGMA_DATASET_DEPENDENCIES
              SELECT DISTINCT WORKBOOK_ID from SIGMA_WORKBOOK_MIGRATION_SUMMARY
        │
        │  Builds the artifact list: (type, id, name, path, url)
        │  Three artifact types: dataset / datamodel / workbook
        ▼
5. GET /v2/grants?inodeId={id}&limit=1000         [per artifact, concurrent — 10 workers]
        │
        │  Returns ALL effective grants on the artifact including
        │  those inherited from parent folders and workspaces.
        │  Each grant has either teamId or memberId/userId.
        ▼
6. GET /v2/grants?inodeId={id}&directGrantsOnly=true&limit=1000   [per artifact, concurrent]
        │
        │  Returns only grants set explicitly on this artifact
        │  (not inherited). Used to populate IS_DIRECT_GRANT —
        │  the grantee IDs from this response form the direct_ids set.
        │  Rows from step 5 are flagged IS_DIRECT_GRANT = TRUE
        │  if their grantee ID appears in this set.
```

Steps 5 and 6 are submitted as pairs per artifact into the same thread pool — two futures per artifact, 10 workers total.

### How API data maps to output columns

| Output column | Source |
|---|---|
| `ARTIFACT_TYPE` | Determined by which source table the ID came from |
| `ARTIFACT_ID` | From `SIGMA_DATASET_DEPENDENCIES` or `SIGMA_WORKBOOK_MIGRATION_SUMMARY` |
| `ARTIFACT_NAME / PATH / URL` | From the same source tables |
| `GRANTEE_TYPE` | Inferred from which ID field is populated (`teamId` → "team", `memberId`/`userId` → "member") |
| `GRANTEE_ID` | `/v2/grants` → `teamId` or `memberId` or `userId` |
| `GRANTEE_NAME` | Resolved via teams_by_id or members_by_id from steps 2–3 |
| `PERMISSION_LEVEL` | `/v2/grants` → `permission` / `access` / `level` / `role` (defensive lookup) |
| `IS_DIRECT_GRANT` | TRUE if grantee ID appears in the `directGrantsOnly=true` response for this artifact |

### Performance note

Steps 5 and 6 run concurrently (10 workers). Each artifact generates two API calls. For large orgs with many artifacts this is the most API-intensive procedure — reduce `MAX_WORKERS` in the proc if you hit sustained 429 errors.

---

## Execution order and data dependencies

```
sigma_dataset_dependencies
        │
        │  writes SIGMA_DATASET_DEPENDENCIES
        │    ├── dataset IDs, names, paths, URLs
        │    ├── migration status and migrationToDataModel fields
        │    └── data model IDs, names, paths
        ▼
sigma_workbook_source_map
        │   reads SIGMA_DATASET_DEPENDENCIES for scope + enrichment
        │
        │  writes SIGMA_WORKBOOK_MIGRATION_SUMMARY
        │    └── workbook IDs, names, paths, URLs, owner
        │
        │  writes SIGMA_WORKBOOK_SOURCE_DETAILS
        │    └── per-source enriched rows
        ▼
sigma_artifact_grants
        │   reads SIGMA_DATASET_DEPENDENCIES   for dataset + data model artifact IDs
        │   reads SIGMA_WORKBOOK_MIGRATION_SUMMARY for workbook artifact IDs
        │
        │  writes SIGMA_ARTIFACT_GRANTS
        │    └── one row per (artifact, grantee) with IS_DIRECT_GRANT flag
```

Running any procedure out of order will cause it to fail or produce incomplete results because it reads from the output of the prior procedure. Always run in the sequence above.

---

## API credential requirements

All procedures require **Admin** credentials. Two endpoints are critical:

| Endpoint | Why Admin is needed |
|---|---|
| `GET /v2/datasets?skipPermissionCheck=true` | Without admin, only datasets owned by the credential user are returned — the dependency graph will be incomplete |
| `GET /v2/workbooks?skipPermissionCheck=true` | Same — non-admin credentials return only workbooks the user can see |
| `GET /v2/datamodels` | Does not support `skipPermissionCheck` at all — data models owned by other users are fetched individually as they are encountered as migration targets |
