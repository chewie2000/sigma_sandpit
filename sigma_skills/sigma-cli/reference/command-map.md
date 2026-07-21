# sigcli command map

`sigcli` maps dynamically onto the Sigma v2 REST API. **Always confirm against
the live binary** — this map is a starting point, not a contract (private beta).

## Discovery recipe (do this first)

```bash
export PATH="$HOME/.sigcli/bin:$PATH"   # non-interactive shells

sigcli api list-prefixes                # all resources: command, aliases, pathPrefix, op count
sigcli api schema /v2/connections       # operations + parameter shapes for a resource
```

`list-prefixes` returns objects like:

```json
{ "command": "connections", "aliases": ["conn"],
  "description": "Manage data connections",
  "matchedOperations": 18, "pathPrefix": "/v2/connections" }
```

Use `command` as `<resource>`; derive `<action>` (e.g. `list`, `get`) and its
params from `schema`.

## Resources observed (v0.0.12)

| Resource (`command`) | pathPrefix | Notes |
|---|---|---|
| `connections` (alias `conn`) | `/v2/connections` | 18 ops. **`get`** returns writeback/WAL schema locations — the audit's discovery source. |
| `data-models` | `/v2/dataModels` | 15 ops. |
| `datasets` | `/v2/datasets` | 9 ops. |
| `account-types` | `/v2/accountTypes` | 4 ops. |
| `deployment-policies` | `/v2/deploymentPolicies` | 12 ops. |
| `auth` | `/v2/auth` | token endpoint. |
| `credentials` | `/v2/credentials` | CLI-side credential management. |

> Not exhaustive — `list-prefixes` is authoritative for the installed version
> (workbooks, members, teams, grants, etc. are also expected; confirm live).

## Example calls

```bash
# List connections, then fetch one connection's detail (writeback + WAL schemas)
sigcli api connections list -f json
sigcli api connections get --params '{"connectionId":"<id>"}' -f json

# Workbooks / members / data models (confirm exact action names via schema)
sigcli api workbooks list -f json
sigcli api members list -f json
sigcli api data-models get --params '{"dataModelId":"<id>"}' -f json
```

## Always-available (CLI-side)

```bash
sigcli auth login          # establish a session from env credentials
sigcli credentials ...     # manage stored API credentials
sigcli --version           # version check (confirm beta drift)
```

## Pagination

Operations that page expose a next-page token in their response; follow it until
absent, mirroring the API's `nextPage`/`nextPageToken` dialects. Confirm the
field name from `schema`.
