# Report template

Render the audit in this shape. Keep it scannable. Every data row carries a
**Source** column (`sigcli` | `marts` | `inferred`). Scope sections to `--focus`
when set.

---

```
# Sigma Org Audit — <org / connection base URL>

Source mode: <marts | sigcli | both>   Snapshot: <latest SNAPSHOT_TS>
Live verification: <enabled / DISABLED — reason>   Generated: <ts>
```

If live verification was requested but unavailable, say so on its own line at the
top (degrade loudly).

## 1. Inventory

| Object type | Count | Owner missing | Owner archived | Source |
|---|---|---|---|---|
| workbook | … | … | … | marts |

## 2. Migration readiness (R/A/G)

Summary counts, then the top RED datasets to act on first.

| RAG | Datasets | Downstream workbooks | Source |
|---|---|---|---|
| RED | … | … | inferred |

| Dataset | Path | Downstream WBs | Reason | Source |
|---|---|---|---|---|
| … | … | … | <cite rubric §1> | inferred |

## 3. Ownership cleanup

| Object | Type | Owner email | Missing? | Archived? | Source |
|---|---|---|---|---|---|

## 4. Writeback governance

Storage reclamation summary, then orphaned/stale tables.

| Connection | SIGDS tables | Reclaimable GB | Confidence | Source |
|---|---|---|---|---|
| … | … | … | <cite rubric §2> | inferred |

| SIGDS table | Connection | Rows | Bytes | Days since edit | Orphaned | Stale | Score | Source |
|---|---|---|---|---|---|---|---|---|

Note any connections flagged `SCAN_REACHABLE = FALSE` (inventoried, not deep-scanned).

## 5. Drift

| Object | Change window | Note | Source |
|---|---|---|---|
| <workbook> | <from> → <to> | superseded version | marts |
| <field> | marts=<x> live=<y> | live ahead of snapshot | sigcli |

## 6. Recommendations

Prioritised, actionable, read-only (Phase 1). For each: what, why (cite rubric),
and the Phase-2 action that would remediate it (transfer ownership / retag /
swap sources / quarantine+archive).
```

---

**Provenance reminder:** scores (R/A/G, archival) are always `inferred`.
Warehouse-read facts are `marts`. Anything confirmed live this run is `sigcli`,
and on conflict the `sigcli` value is the one shown.
