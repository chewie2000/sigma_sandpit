# Scoring rubric — the auditable source of truth

Every score / classification in the report must cite a section here. The mart
views implement these rules; this doc is where the reasoning lives. If you find a
real case the rules don't cover, flag it rather than inventing a score.

All scores are `source: inferred` — they are computed heuristics, not observed
facts. Mark them as such in the report.

---

## 1. Migration readiness (R/A/G) — `V_MIGRATION_SCORE`

Per dataset, driven by the API `migrationStatus` and downstream workbook fan-out
(`DOWNSTREAM_WORKBOOK_COUNT`).

| RAG | Condition | Meaning |
|---|---|---|
| **GREEN** | `migrationStatus` in (`migrated`, `not-required`) | Done or N/A. |
| **AMBER** | `not-migrated` AND no workbooks depend on it | Low blast radius; migrate at leisure. |
| **RED** | `not-migrated` AND ≥1 workbook depends on it | Migrate before the dataset is deprecated. |
| AMBER | unknown / null status | Review manually. |

**Reason string** must state the status and the downstream count. Priority order
for remediation: RED by descending `DOWNSTREAM_WORKBOOK_COUNT`.

> **Phase-1 limitation.** Fan-out is workbook references only; full dataset->dataset
> dependency chains (as in `DataSetMigrateHelper_SF`) are not yet folded in. A
> dataset feeding other datasets may be under-scored. Note this where relevant.

---

## 2. Writeback archival score — `V_WRITEBACK_GOVERNANCE`

Per `SIGDS_*` table (reachable scans only). Status flags first:

- `IS_ORPHANED` — no live workbook maps to the table (`WORKBOOK_EXISTS = FALSE`).
- `IS_STALE` — no WAL edit in > 90 days, or no WAL activity ever observed.
- `IS_EMPTY` — `ROW_COUNT = 0`.

**Weighted score (0–100; higher = stronger archive/cleanup candidate):**

| Signal | Points |
|---|---|
| `IS_ORPHANED` | 40 |
| `IS_STALE` | 30 |
| `IS_EMPTY` | 15 |
| no edit in > 365 days | 15 |
| no edit in 180–365 days | 8 |

**Confidence:** `HIGH` if orphaned AND stale; `MEDIUM` if orphaned OR stale;
else `LOW`. **Reclaimable bytes** counted only when orphaned or stale.

> Cleanup guidance: never drop directly. Recommend quarantine (rename/move) then
> drop after a hold period — mirror the safe-deletion process from
> `writeback_info_sf`.

---

## 3. Ownership cleanup — `V_OWNERSHIP_CLEANUP`

An object is a cleanup candidate when its owner is:

- `OWNER_MISSING` — `ownerId` resolves to no member (deleted account), or
- `OWNER_ARCHIVED` — owner member `IS_ARCHIVED = TRUE`.

Recommend reassigning ownership before the member record is purged.

---

## 4. Drift — `V_WORKBOOK_DRIFT` (and other SCD2 tables)

A superseded SCD2 version (`SCD_VALID_TO` not null) marks an observed change to
the object between snapshots. Report the changed window (`CHANGED_FROM` ->
`CHANGED_TO`). Under `--source both`, a marts-vs-SIGCLI disagreement is *also*
drift: live has moved ahead of the last snapshot.
