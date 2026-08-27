# An additive access model for row-level security

*Sigma Computing — validated end to end against a live Sigma workbook · August 2026*

A working, validated approach for letting a person's visible data be the union of every team and personal grant they hold, without the priority collapsing that user attributes run into today.

---

## The problem: two permission layers, only one of them additive

This is a common gap: workspace access is additive across every team a person belongs to, but row-level scope driven by user attributes is not — it resolves to one value, decided by team priority.

| Layer | Behaviour today |
|---|---|
| Access to content (workspaces, workbooks) | **Additive** — grants from every team a person belongs to are combined automatically |
| Access to rows (user attributes) | **Not additive** — a person resolves to one value per attribute, decided by team priority |

Anyone who legitimately belongs to more than one scoping group hits this: the second grant does not add to the first, it competes with it and silently loses. There is no error and no empty dashboard — just a quietly under-counted one.

---

## The fix: a native Sigma function already solves this

The limitation above is specific to **user attributes**, not to Sigma's security model as a whole. Sigma has a built-in function for exactly this case:

```
CurrentUserInTeam(team)
```

This returns true if the current user is a member of the given team — a live membership check, not an attribute lookup. There is no priority contest the way there is with user attributes.

> **One correction worth flagging directly:** this function takes a *single* team name, not a list. Concatenating several team names into one string and passing that in looks reasonable but fails silently — it was measured directly during build, and the fix (see "The correction, in practice" below) is straightforward once caught. Team membership resolves per team, one at a time, then the individual results are combined.

The equivalent for individual exceptions is:

```
CurrentUserEmail()
```

Both functions resolve directly against Sigma's own team and user data every time they run, so the underlying membership relationship never needs to be replicated anywhere else — it stays exactly where it already lives.

---

## The pattern: one grants ledger, resolved one row at a time

Team grants and personal grants are modeled as the same kind of fact — *this principal is entitled to this entity value* — sitting in one table, differing only in one column. Nothing about the table treats one kind as more important than the other, because there is no priority column.

| entity_type | entity_value | principal_type | principal_id |
|---|---|---|---|
| office | Paris | team | RLS Demo Ops EMEA |
| office | London | team | RLS Demo Ops EMEA |
| office | London | team | RLS Demo Ops APAC |
| office | Hamburg | user | …+user1@sigmacomputing.com |

*(Real rows from the validated build — London genuinely is granted to two different teams, which is exactly the case additive RLS exists to handle correctly.)*

Because `CurrentUserInTeam()` only takes one team name, each grant row is checked individually — never aggregated into a string first:

```
Grant Applies = If([principal_type] = "team",
    Coalesce(CurrentUserInTeam([principal_id]), false),
    [principal_id] = CurrentUserEmail())
```

This one column handles both branches — team or personal — because `principal_id` is always a single value on a single row. The individual results are then combined per entity value with `Max()` (see below), which is where the union across multiple grants actually happens.

Adding a new kind of grant later — a third entity type, a temporary exception — means adding rows to the same table, not building a new mechanism.

---

## The build: how this fits together inside a Sigma data model

The ledger has multiple rows per entity value (one per team or user granted), so a plain join against the fact table would fan out rows. The fix is twofold: resolve membership one grant row at a time (above), then use Sigma's `Lookup` function with an aggregate formula to bring the result onto the fact table without expanding it.

```
Entity Grants
  + Grant Applies — evaluated per row, per viewer
        │
        │  Lookup(Max(...), entity_type, entity_value)
        │  — compound key, no child tables
        ▼
Shipments
  + Office / Company / Site RLS
        │
        ▼
Combined RLS filter
  Office RLS OR Company RLS OR Site RLS
        │
        ▼
Only rows the current person is entitled to remain visible
```

### In three steps

1. On the ledger element (`Entity Grants`), add `Grant Applies` (above) and a numeric mirror of it, so `Max()` can aggregate it.
2. On the fact element (`Shipments`), per dimension: `Office RLS = Lookup(Max([Entity Grants/Grant Applies Num]), …) = 1`, matching on entity type and entity value together in a single compound-key lookup — no per-dimension child tables needed.
3. OR the per-dimension results together, filter on the combined result, and hide every helper column from explorers — 14 helper columns stay hidden in the shipped model, leaving 8 business columns visible.

> **Confirmed during build:** a compound-key lookup (matching on entity type and entity value together, against the single unfiltered ledger) works correctly. An earlier design assumed this needed one filtered child table per scoping dimension — that turned out to be unnecessary once tested side by side against the compound-key version on the same data, with identical results.

---

## The correction, in practice: team and personal grants resolve the same way, row by row

Because membership is checked one grant row at a time (above), a team grant and a personal grant are symmetric all the way through — both produce a per-row true/false for the current viewer, and `Max()` combines however many rows exist for an entity into one answer. London is a real case of this in the validated build: it's granted to two different teams on two separate rows.

| Grant row (London) | Member of<br>RLS Demo Ops EMEA | Member of<br>RLS Demo Ops APAC | RLS Demo<br>Observers (no grants) |
|---|---|---|---|
| team · RLS Demo Ops EMEA | **true** | false | false |
| team · RLS Demo Ops APAC | false | **true** | false |
| **Max (this office)** | **1** | **1** | **0** |
| **Row visible?** | **Yes — via EMEA** | **Yes — via APAC** | **No** |

`Max()` is what makes this additive: one matching grant out of any number is enough, and because it's idempotent, someone who happens to belong to *both* EMEA and APAC still sees London's 670 rows exactly once — confirmed directly, not just reasoned about. The personal-grant branch runs through the identical mechanism: Hamburg is granted only to one individual by email, with no team row at all, and `Grant Applies`/`Max()` resolves it correctly with a single row to aggregate. Team and personal grants aren't two mechanisms stitched together — they're the same column, evaluated on different rows.

> **Two related gotchas worth knowing about:** `CurrentUserInTeam()` returns `NULL`, not `false`, when given an empty value, and `OR`/`Max()` over a `NULL` can silently produce `NULL` across an entire table rather than `false` — this hit all 8,000 rows before it was caught. Both this and the case where `Lookup()` finds no matching grant at all need to be wrapped in `Coalesce(…, false)` — caught during build precisely because it failed closed (nobody saw rows they shouldn't) rather than throwing a visible error.

---

## Worked example: one person, two sources of access, one resolution — with real numbers

This is the exact composition tested in the build, not a hypothetical: the persona `user1` belongs to three teams and holds two personal grants on top. Every branch resolves through the same `Grant Applies`/`Max()` mechanism, with no precedence question between them.

```
                        user1
                       /      \
     3 team memberships        2 personal grants
   EMEA · APAC · Key Accounts    office Hamburg · site COB-2
            │                          │
   Team-derived scope             Personal additions
       2,984 rows                    +675 rows
            \                          /
             \                        /
             user1's effective scope
        3,659 rows — matches the independent
             SQL check exactly
```

The team memberships alone would produce 2,984 visible rows; the two personal grants (an office no team covers, and one specific site) add 675 more, entirely independent of team membership. The combined total, 3,659 rows, was checked against an independent SQL calculation of what `user1` should see and matched exactly.

---

## Evidence: validated end to end, not just designed

This pattern has been built and tested against a real Sigma workbook and a real warehouse table — 8,000 synthetic freight shipment records, three test personas, and an independent SQL calculation of what each persona should see, checked against what Sigma actually returned.

| Check | Result |
|---|---|
| Union across two disjoint teams | EMEA alone: 1,322 rows. Key Accounts alone: 1,296 rows. Both together: 2,422 — strictly more than either alone |
| Team grant + personal grant, same person | `user1`: 2,984 rows from teams, 3,659 once personal grants are added |
| Personal grant with no team involvement | `user2` (on the zero-grant Observers team): 646 rows, entirely from one personal grant |
| Zero grants → zero rows | A no-grants persona: 0 rows, confirmed live in Sigma, not just in the SQL check |
| Two teams granted the same entity | London: 670 rows, granted to both EMEA and APAC — a member of both sees exactly 670, not double |
| No fan-out from the ledger | Fact table returns exactly 8,000 rows with all lookup columns attached |

Out of 8,000 rows: `user1` sees 45.7%, `user2` sees 8.1%, and 3,913 rows (49%) are visible to nobody — a believable, obviously-filtered result rather than something that could pass by coincidence on a small table.

---

## Worth knowing before extending this: two limitations, found while building

### The ledger can only widen access, never narrow it

Every grant added to the ledger can only increase what someone sees — there's no way to express "this company, but only shipments through Paris." If every access rule in your org reads as *the union of things you're entitled to*, this pattern fits as-is. If any rule needs to read as *X, but only Y*, the ledger needs an additional pair of columns to carry that condition — worth confirming which kind of rule you actually need before building further.

### Hierarchy between dimensions is emergent, not modeled

The row filter treats company, site, and office as three independent checks joined by `OR` — nothing in the ledger records that a site belongs to a company. In the validated data, this still works out correctly: a grant on one company covers all 682 of that company's rows across its three sites, because every fact row carries the company value directly. But a grant on one site (221 rows) does not cover that company's other sites, and office cuts across the whole structure entirely — a single office (Paris, 652 rows) touches all 12 companies and all 36 sites, since the office reflects who handled the shipment, not whose cargo it is. The cascade only works because the fact table is denormalized to carry company, site, and office on every row; if site were resolved by joining out to a separate table instead, a company-level grant would silently stop reaching it. Worth keeping in mind for however your own hierarchy ends up structured on the fact table.

---

## Common questions

**Can a user's row-level scope ever be the union of their teams?**
Yes, natively, via `CurrentUserInTeam()`. This was already possible in Sigma — it simply needs a different mechanism than team-scoped user attributes, which is what carries the priority limitation.

**If not natively, what pattern do you recommend to get there?**
The grants ledger above: one table, resolved one grant row at a time with `CurrentUserInTeam()` or `CurrentUserEmail()`, then combined per entity value with `Max()`. Validated against a real Sigma workbook with 8,000 test rows — see Evidence above.

**Can user attributes be set per user through the API at scale, refreshed on a schedule?**
This pattern doesn't use user attributes at all, so the question doesn't apply to it. Team membership stays inside Sigma's normal team management; the only scheduled job needed is a small warehouse update to the grants table whenever an entitlement changes.

---

## Next steps: where we go from here

1. **Confirm the union-only limitation is acceptable.** Check whether any of your real access rules need "X, but only Y" — if so, the ledger needs qualifier columns before going further (see Limitations above).
2. **Map the real hierarchy.** Confirm how office, country, and group actually relate on your fact tables, since a company-level grant only cascades to its sites because of how the fact table is denormalized today — not because the ledger models the hierarchy itself.
3. **Apply it to your real data.** Build the ledger and data model against your actual teams and entities, following the validated pattern above.

`Row & column security` · `Input tables & page visibility — separate session`

---

*Prepared by Sigma Computing for discussion purposes. Table and column names shown are illustrative and will be finalized during implementation.*
