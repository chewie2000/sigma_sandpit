# Sigma data model materialization — best-practice reference

A reference for choosing what to materialize, where, how often, and what to avoid. Synthesises Sigma's public docs with internal engineering and field guidance. Written to be both human-readable and structured enough to drive a static-analysis tool.

---

## Where materialization belongs

Materialization is for **expensive, reusable, relatively stable upstream logic**, ideally inside a data model, not a workbook. Internal sentiment is consistent: workbook-level materialization is a worse fit than data-model materialization — broad workbook materialization has been called "not the right approach", and allowing workbook-element schedules "has not been a great experience."

Default position: if you can move the expensive logic into a data model and materialize it there, do so. Workbook materialization remains valid for terminal-stage workbook-specific aggregations, but is the exception, not the rule.

---

## Sigma's materialization model in brief

A materialization is bound to a query digest — a hash of the query shape the materialized element resolves to. When a workbook or data model element evaluates, Sigma checks whether a materialization exists with a matching digest. If it matches, Sigma reads the materialized table. If anything has shifted the query shape — formulas, filters, joins, controls, security context, source structure — the digest no longer matches and Sigma falls back to a live warehouse query until a fresh materialization is built.

Two operational consequences fall out of this:

- Anything that changes query shape silently invalidates downstream materializations. The materialization isn't deleted; it just stops being used.
- Materialization is not a generic "closest ancestor" cache across lineage. Sigma uses the materialization tied to the element being evaluated, with one exception (see "multi-level grouping" below): "child table is going to be a different query, we don't reuse results from parent to derive child."

---

## Positive patterns — when to materialize

These are the use cases where materialization earns its keep.

### Heavy joins and unions

Public guidance: materialize elements built on expensive joins (3+ sources, fact-to-fact, or anything where the warehouse plan is non-trivial). Joins are the canonical materialization win. Lookups are weaker — "the materialization will be used by a join, if that works for you, but not by lookup."

If you have a lookup, the workaround is to create a child element containing only the looked-up columns and materialize that child instead.

### Aggregated grouping levels (not the leaf grain)

For data model or workbook elements with grouping levels, Sigma materializes the chosen grouping plus all less-granular levels above it. **Do not materialize the most granular ("all source columns") level** unless you genuinely need the leaf grain accessible from a materialization. Public docs explicitly call this out as "often unnecessary and potentially costly," and internal planning notes ("avoid costly base-level materializations") agree.

The internal rule of thumb: materialize from the level you care about upwards. For example, "materialize the daily grouping level, but don't materialize the base level."

### Multi-level / hierarchical chains

Sigma supports materialising several levels of a chain (parent → child → grandchild). The engine builds higher levels on top of lower ones — `t_mat_<id>_1` is built on top of `t_mat_<id>_0`.

Three rules apply when you do this:

1. **Schedule downstream after upstream.** Public docs are explicit: a child schedule should start after its parent completes. Use the "After selected parent schedule finishes" option for dependent schedules rather than guessing with cron offsets.
2. **Expect cascading invalidation.** A change that invalidates the parent's digest will invalidate all descendants until each is rematerialized. Public docs improved this for version-tagged data models — older materializations continue to be used until the new one is built — but the principle still applies.
3. **Materialise each layer that earns it independently.** Don't assume materialising the parent helps if the child is joined to other live elements; partial plans frequently fall back to live. Materialise each layer that is itself expensive and reused.

### Custom SQL elements

Custom SQL elements re-run their SQL verbatim every time. If the SQL is expensive and the upstream is stable, materialising the custom SQL element pays back quickly. Caveat: custom SQL using the `sigma_element()` syntax to reference another Sigma element **cannot be materialised** — that's a documented limit.

### Cross-workbook reuse via data models

If multiple workbooks read the same expensive transformation, materialise it once in a data model and let every workbook benefit. Sigma's own performance docs and internal voices both point this out as a defining advantage of data model materialization over per-workbook duplication.

### Joined output of already-materialised elements

Materialising the output of a join between two materialised tables is allowed and often useful — but it is its own materialization problem and the joined output's digest must still match for it to be used. Order of operations matters: do the join first, then apply filters/controls downstream on a child element. Public community guidance confirms this — joining after filtering reduces materialization reusability.

---

## Hard exclusions — do not materialise these

These are not "soft penalties." Materializing them produces output that Sigma will silently bypass at query time, or worse, produces incorrect results.

### Elements targeted by a control

If a workbook page control targets a materialised element, changing the control value queries live data, not the materialised table. The fix is to materialise the parent and target the control at a child element. Public docs are explicit.

### Elements containing user-scoped system functions

`CurrentUserAttribute()`, `CurrentUserEmail()`, and other per-user functions cannot be correctly materialised because the result varies per user. Per-user RLS / CLS in lineage will similarly cause bypass or invalidation — internal modelling guidance flags this directly.

### Elements with dynamic time functions

`Now()`, `Today()`, and related dynamic time functions get hardcoded at publish time, producing stale or incorrect downstream behaviour. Don't materialise anything whose formulas reference these.

### Elements with relative date filters

A "last 7 days" style filter on the element forces Sigma to perform a full refresh rather than incremental, and disables dynamic table incremental refresh. Public docs call this out explicitly. If you need recent-window analytics, materialise the full underlying table and apply the relative window in a downstream child.

### Elements sourced directly from Input Tables (current architecture)

This is the most nuanced one. The current Input Tables architecture uses a log-replay / sequence-number model. Every edit changes the query that returns the latest row version, which changes the digest, which invalidates downstream materializations until they're rebuilt.

This is intended behaviour today — the engine philosophy is the reason: "row version" is really a sequence number that can include schema changes, so the engine treats every change conservatively.

The standard pattern: **materialise a child of the Input Table, not the Input Table itself**, and accept that frequent edits will still cause some re-materialization churn. This becomes a non-issue with Sigma Tables (see below).

### Custom SQL elements using `sigma_element()`

Documented as unsupported for materialization.

### Elements in a write-ahead-log-based dependency chain

WAL-based architectures are no longer recommended or supported. Don't build new materialisation work that depends on WAL semantics.

---

## Soft penalties — possible but risky

These don't disqualify materialization but lower its expected value or reliability.

| Pattern | Risk |
|---|---|
| Joining a materialised parent to a non-materialised live element downstream | Partial plans often fall back to live; you lose most of the win |
| Materialising very small tables | Alpha Query and browser cache likely already serve them efficiently |
| Materialising elements in lineage of column-level-secured Snowflake views | Per-user variation may cause per-user bypass |
| Materialising elements in lineage of a Snowflake semantic view | Semantic views in Sigma still have limits (no joins/unions/transpose, controls can't target them, derived metrics unavailable) |
| Materialising elements with expand/collapse interactions | Some grouped expand/collapse states are not yet supported by the materialization engine |
| Adding, editing, or deleting metrics on a materialised data model | Public docs: invalidates use until a new materialization job runs |

---

## Dynamic tables vs static materialized tables (Snowflake)

Sigma's default behaviour on Snowflake connections is to use **Snowflake dynamic tables** for materialization, falling back to static tables when dynamic tables aren't supported for the query shape.

**Prefer dynamic tables when:**
- Source data changes regularly
- The result is large or expensive to rebuild
- Incremental refresh is actually available for the query shape
- The connection role has dynamic table privileges
- Change tracking is enabled with non-zero time travel retention on every source table that may be queried to build the dynamic table

**Prefer (or fall back to) static tables when:**
- The warehouse isn't Snowflake (dynamic tables are Snowflake-only)
- The element is a dataset, not a data model or workbook element (dynamic tables only support data models and workbooks)
- The query shape prevents incremental refresh — e.g. relative date filters, references to `Today()`, `Now()`
- A dependency on a non-dynamic materialization is in the chain (Sigma avoids creating dynamic tables for sheets with non-dynamic dependencies, because the DT would have to rebuild every time the dependency refreshes)
- The source is a complex or secure Snowflake view (dynamic tables can fail against these)

**Skip behaviour:** when Sigma can detect underlying data hasn't changed since the last successful materialization, it skips the run and shows a "Skipped" status. This is Snowflake-specific.

**Databricks:** no equivalent dynamic table or skip-on-no-change behaviour in Sigma today. Treat Databricks materialization as static tables only.

---

## Scheduling and cadence

### Frequency

Pick the **slowest cadence that still meets freshness expectations**, and run after upstream ETL or dbt loads. Public docs put it succinctly: more complex queries should usually run less frequently to reduce computational burden.

### Windows

Off-hours is the default. The internal admin guide says materialization should typically run in the middle of the night, early morning, or after batch warehouse loads complete.

### Dependent schedules

Use the "After selected parent schedule finishes" option rather than approximating with cron offsets. Behaviour:

- The child runs only after the parent successfully completes
- If the parent fails or is paused, dependent children fail or are paused
- Parents and children appear grouped together in the schedule UI
- Parents can be multiple levels upstream and can live in a different workbook or data model

### Schedule pitfalls

- **Don't over-schedule.** If a run takes longer than the cadence, you create overlap and "materialization in progress" errors rather than freshness.
- **Don't assume tags share schedules.** Each version tag on a data model needs its own materialization schedule. Materialization schedules do not propagate across tags or tenants automatically.
- **Cross-data-model dependent schedules** have had UI/visibility bugs historically — verify the UI shows the dependency you configured.

### Inactivity

Sigma pauses materializations that go unused for 60 days or fail five times consecutively.

---

## Failure modes and observability

### Common reasons materializations fail or get skipped

| Reason | What it looks like |
|---|---|
| Digest mismatch | App trace shows `rewriteError ... digest does not match ...`; element falls back to live |
| Upstream input table edit (current architecture) | Downstream mats marked skipped after every IT change |
| Query timeout | Materialization uses warehouse-level timeout, not Sigma connection timeout |
| Dynamic table unsupported / fallback failure | Common with complex / secure views in lineage |
| Permissions / writeback schema mismatch | Especially across version tags |
| Parent dependency not complete | Child sits waiting; not a failure, just delay |
| Schedule overlap | New run errors with "materialization in progress" |

### Debug tooling (internal)

- **Element menu → View materialization info** — per-element status
- **Workbook or data model lineage view** — materialization status and dependency graph
- **Admin → Materializations** — org-wide status and logs
- Internal materialization debugger and Materialization Debugger workbook — retrigger and inspect
- Application Trace workbook — request and task traces
- Internal tables: `TASK_STATUS`, `MATERIALIZATIONS`, `SHEET_LEVEL_MATERIALIZATIONS`

---

## Feature interactions

### Embedding

Embed contexts themselves don't disable materialization. The interaction point is **version tagging**:

- Tagged workbook or report using a materialised **data model** → materialization is used
- Tagged workbook or report using a materialised **dataset** → materialization is **not** used; a copy is taken
- Tagged workbooks cannot be materialised; only published workbooks, plus published or tagged data models

For embedding plus SDLC, materialised **data models** are the supported path. Materialised datasets and workbook materializations are footguns.

Embed connection swapping (`eval_connection_id`) can break materialization use because the alternate connection may not have the relevant writeback schema.

### Multi-tenancy

Don't expect materialization to transfer across tenants automatically. That would create materialization sprawl in the warehouse. Tenancy moves content; materialization schedules need to be set up per-tenant explicitly.

### Version tagging

- Each tag can have its own materialization schedule
- Tagged data models work cleanly with materialization; tagged workbooks do not
- Tags are not global links — "you need to carefully control how you connect your workbook to the proper version tagged DM, nothing is done for you"

### Cortex, Sigma Assistant, and Sigma Agents

Materialised data itself is a fine substrate for Sigma Assistant, Sigma Agents, and Cortex Agent.

Caveats are about semantic views and metrics, not materialization:

- Ask Sigma / Sigma Assistant historically ignored some pre-defined metrics on Snowflake semantic views (resolved early May 2026)
- Semantic view limitations in Sigma: no joins/unions/transpose, controls can't target them, derived metrics unavailable, metrics only on directly sourced element
- Cortex Agent passthrough path for AI access is unaffected

### Sigma data type / element-kind specifics

- Dataset materialization supports one grouping level only
- Data model and workbook element materialization supports multiple grouping levels per element

---

## Sigma Tables transition

Sigma Tables (Snowflake private beta live; Databricks beta following) is the next evolution of Input Tables. The relevant materialization changes:

- **Input-table digest invalidation goes away.** "Sigma tables always have the latest version of the table as the one stored on disk, including any schema changes etc. Because we don't need to do a log replay like we do for current input tables, a materialization doesn't need to include any concept of a sequence number." So the hard-skip rule "don't materialise off Input Tables" relaxes when the source is a Sigma Table.
- **Cross-workbook reads/writes** become natural, which strengthens the data model materialization story.
- **Leaf-grain economics don't change.** Sigma Tables alter writeback storage, not the cost of materialising the deepest grouping. Still avoid leaf-grain materialization.
- **Dependent schedule semantics don't change.**

Until a model is fully on Sigma Tables, the current Input Tables rules apply.

---

## Warehouse-specific differences

### Snowflake

- Dynamic tables for incremental materialization when supported
- Skip-on-no-change behaviour reduces unnecessary runs
- Transient tables used for some materialization paths
- Secure or complex views in lineage can break dynamic table use

### Databricks

- No dynamic table equivalent in Sigma today
- No skip-on-no-change behaviour documented
- Treat as static table materialization only
- The update-metadata path supporting skip behaviour is implemented only for Snowflake currently

---

## Quick-reference: signals that earn materialization

Useful inputs for any static-analysis recommender built on top of this document.

**Positive signals**

- Element joins three or more sources
- Element formulas include `OVER(...)`, `RANK(...)`, `LAG(...)`, or other window functions
- Element is a custom SQL element (not using `sigma_element()`)
- Element has heavy aggregation at a non-leaf grain
- Element is at a non-leaf grouping level
- Element has high downstream fan-out — many dependent elements or workbooks
- Element is sourced only from warehouse tables (or, in future, Sigma Tables)
- Element is in a data model, not a workbook

**Negative signals — hard skip**

- Element is targeted by a control
- Formulas reference `CurrentUserAttribute()`, `CurrentUserEmail()`, or other user-scoped system functions
- Formulas reference `Now()`, `Today()`, or other dynamic time functions
- Element has a relative date filter applied
- Element is sourced directly from an Input Table (current architecture)
- Element is a custom SQL element using `sigma_element()`
- Element depends on WAL-based behaviour

**Negative signals — penalty, not skip**

- Element is at the most granular grouping level
- Element already has a materialization schedule (don't double-recommend)
- Element is joined downstream to non-materialised live elements
- Element row count / data volume is expected to be tiny
- Element is in a Snowflake semantic view chain with controls or unsupported features
- Element formulas use lookups rather than joins (lookups don't reuse materialization)
- Element is in lineage of column-level-secured warehouse views

**Cadence band heuristic**

- Hourly — high downstream fan-out, warehouse-only sources, small expected output, fast upstream changes
- Daily — default for most candidates
- Weekly — low fan-out, very expensive joins, slow-changing upstream
- Manual / on-demand only — freshness matters less than cost

---

## Sources

Public:

- [About materialization](https://help.sigmacomputing.com/docs/materialization)
- [Schedule materialization for a data model or workbook](https://help.sigmacomputing.com/docs/schedule-materialization-for-a-data-model-or-workbook)
- [Schedule materialization for a version-tagged data model](https://help.sigmacomputing.com/docs/schedule-materialization-for-a-version-tagged-data-model)
- [Manage materializations](https://help.sigmacomputing.com/docs/manage-materializations)
- [Best practices for improved document performance](https://help.sigmacomputing.com/docs/best-practices-for-improved-performance)
- [Connect to Snowflake](https://help.sigmacomputing.com/docs/connect-to-snowflake)
- [Write custom SQL](https://help.sigmacomputing.com/docs/write-custom-sql)
- [Query and extend Snowflake semantic views in Sigma](https://help.sigmacomputing.com/docs/query-and-extend-snowflake-semantic-views-in-sigma)
- [Add version tags to workbooks, data models, and reports](https://help.sigmacomputing.com/docs/add-version-tags-to-workbooks-and-data-models)
- [How to use Materialization to Boost Performance? — Sigma Community](https://community.sigmacomputing.com/t/how-to-use-materialization-to-boost-performance/6756)
- [Workbook Performance Tuning / Optimization (Comprehensive Compendium) — Sigma Community](https://community.sigmacomputing.com/t/workbook-performance-tuning-optimization-comprehensive-compendium/3344)

Internal references and threads have been stripped from this shipped copy; the public sources above carry the same guidance.
