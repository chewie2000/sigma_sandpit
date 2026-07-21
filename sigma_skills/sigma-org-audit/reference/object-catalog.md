# Object catalog — what the audit captures

Each object type lands in `RAW_SIGMA_OBJECTS` (raw VARIANT snapshots), is
flattened in a `STG_*` view, and feeds one or more mart views. Live equivalents
are fetched via `sigma-cli` (`sigcli api <resource> <action>`).

| Object | OBJECT_TYPE(s) | Source | Stage view | Governance question it answers |
|---|---|---|---|---|
| Workbooks | `workbook`, `workbook_sources` | `GET /v2/workbooks`, `/{id}/sources` | `STG_WORKBOOKS` | What exists, who owns it, what does it depend on, downstream blast radius. |
| Data models | `datamodel`, `datamodel_detail` | `GET /v2/dataModels`, `/{id}` | `STG_DATAMODELS` | Migration targets; model inventory. |
| Datasets | `dataset` | `GET /v2/datasets` (admin) | `STG_DATASETS` | Migration readiness (`migrationStatus`). |
| Connections | `connection`, `connection_detail` | `GET /v2/connections`, `/{id}` | `STG_CONNECTIONS`, `STG_CONNECTION_WRITEBACKS` | Warehouse wiring; **writeback + WAL schema locations** (drives the writeback scan). |
| Members | `member` | `GET /v2/members` | `STG_MEMBERS` | Owner resolution; archived/missing owners. |
| Teams | `team` | `GET /v2/teams` | `STG_TEAMS` | Team inventory; grant resolution. |
| Grants | `grant` | `GET /v2/grants?inodeId=` | `STG_GRANTS` | Who can access what (per-artifact). |
| Writeback tables | `writeback_table` | `INFORMATION_SCHEMA.TABLES` in discovered writeback schemas | `STG_WRITEBACK_TABLES` | SIGDS input-table inventory: size, age, reachability. |
| Writeback WAL | `writeback_wal` | input-table WAL tables in `inputTableAuditLogSchema` | `STG_WRITEBACK_TABLES` | Last edit / activity / editor per SIGDS table; orphan + staleness signals. |

## Notes

- **Writeback discovery is automatic.** The writeback scan reads the writeback
  and WAL `database`/`schema` from each `connection_detail` payload — there is no
  manual schema list. A connection's writeback location that the executing role
  cannot read is flagged `SCAN_REACHABLE = FALSE` (inventoried from the API,
  contents skipped).
- **`workbook_sources`** holds the dataset/model fan-out used to compute a
  dataset's downstream blast radius in migration scoring.
- Live (`sigcli`) coverage uses the same v2 resources; discover them with
  `sigcli api list-prefixes` and inspect shapes with `sigcli api schema <path>`.
