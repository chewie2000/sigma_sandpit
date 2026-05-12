# CLAUDE.md — sigma_sandpit

This is the root workspace for Sigma Computing technical work. Beads (`bd`) is
initialised here and is the source of truth for all task tracking and persistent
memory across sessions.

---

## Session Start (always do this first)

At the beginning of every session, before doing anything else:

```bash
bd ready --json
```

This shows all unblocked tasks. Pick the highest-priority item and claim it:

```bash
bd update bd-xxxx --status in_progress
```

If the user gives you a specific task, check whether it already exists in Beads
before creating a duplicate:

```bash
bd list --json | grep -i "keyword"
```

---

## Workstream Structure

Issues should be labelled by workstream so they can be filtered cleanly.
Use these labels consistently:

| Folder                     | Label                  |
|----------------------------|------------------------|
| `DataModelGraph_SF`        | `data-model-graph`     |
| `DataSetMigrateHelper_SF`  | `dataset-migrate`      |
| `python`                   | `python`               |
| `sql`                      | `sql`                  |
| `writeback_info_sf`        | `writeback-sf`         |
| `writeback_info_dbx`       | `writeback-dbx`        |
| Cross-cutting / general    | `sigma-sandpit`        |

To filter by workstream:

```bash
bd list --label dataset-migrate
bd ready --label writeback-sf
```

---

## Creating Issues

Create issues proactively — do not wait to be asked. If you discover a bug,
a follow-up task, a blocker, or something worth remembering across sessions,
create a Beads issue immediately and tell the user what you created.

```bash
# Standard task
bd create "description" -p 2 -t task --label <workstream-label>

# Bug
bd create "description" -p 1 -t bug --label <workstream-label>

# Something blocking current work
bd create "description" -p 1 -t bug --label <workstream-label>
bd dep add bd-blocked bd-blocker
```

Priority scale: `1` = critical, `2` = high, `3` = medium, `4` = low

---

## During a Session

- When you start work on a task: `bd update bd-xxxx --status in_progress`
- When you finish a task: `bd close bd-xxxx "brief summary of what was done"`
- When you discover related work: create a new issue immediately, link if relevant:
  `bd dep add bd-new bd-existing`
- When a task is blocked: create the blocker as an issue and add the dependency

---

## Session End (always do this last)

Before ending any session:

1. Close any completed tasks with a meaningful summary
2. Create issues for anything discovered but not yet done
3. Force sync to persist state:

```bash
bd sync
```

Never end a session without running `bd sync`.

---

## Snowflake vs Databricks Context

Several workstreams have both SF (Snowflake) and DBX (Databricks) variants.
When creating issues, be explicit in the label and description — do not use
generic labels when a specific `writeback-sf` or `writeback-dbx` label applies.

---

## Useful Commands Reference

```bash
bd ready                          # What's unblocked and ready to work on
bd list                           # All open issues
bd list --label <label>           # Filter by workstream
bd show bd-xxxx                   # Full detail on a specific issue
bd create "desc" -p 1 -t task     # Create an issue
bd update bd-xxxx --status in_progress  # Claim a task
bd close bd-xxxx "what was done"  # Close a completed task
bd dep add bd-a bd-b              # bd-a is blocked by bd-b
bd sync                           # Force persist to git
bd list --status closed           # Review completed work
```
