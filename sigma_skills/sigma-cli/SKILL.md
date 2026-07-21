---
name: sigma-cli
description: >-
  Fetch authoritative LIVE data from a Sigma organisation by driving the `sigcli`
  command-line interface (Sigma's CLI, private beta). A reusable sub-skill —
  other skills (e.g. sigma-org-audit) call it when they need current, observed
  state rather than warehouse snapshots or inference. It runs `sigcli api
  <resource> <action>` with JSON output, discovers the available resources and
  operation shapes at runtime (so it stays correct as the beta API evolves),
  parses results, and maps exit codes to actionable messages. Use whenever a
  task needs live Sigma API data via the CLI on this machine, or a caller asks
  to "verify live", "check against the live org", or "use sigcli". Auth is via
  the SIGMA_CLIENT_ID / SIGMA_CLIENT_SECRET / SIGMA_BASE_URL environment
  variables; this skill never prints secrets.
---

# sigma-cli

Drive the `sigcli` binary to pull live data from the Sigma REST API. This is the
CLI analogue of delegating to a `sigma-api` token skill: callers hand off here,
get back parsed JSON, and stay out of the auth / pagination / error weeds.

> **Private beta.** `sigcli` is a private-beta tool (observed v0.0.12). Its
> command surface can change. This skill **discovers** resources/operations at
> runtime rather than hard-coding them, so it tolerates churn — but verify
> availability before relying on a specific operation.

## When to use

- A skill or user needs **live, current** Sigma state (not a warehouse snapshot).
- A caller asks to verify findings "against the live org" or to "use sigcli".
- Live enrichment of fields the warehouse/inference can't confirm.

**Do NOT use** for: anything that writes/mutates the org unless the caller has
explicitly authorised it (default to read/list/get operations); bulk historical
analysis (that's the `sigma_org_audit` warehouse pipeline); secret management.

## The binary

- Path: `~/.sigcli/bin/sigcli` (interactive shells get it on PATH via
  `~/.zshrc` sourcing `~/.sigcli/bin/env`).
- **Non-interactive invocations must put it on PATH first** — prepend
  `~/.sigcli/bin` or source the env file:
  ```bash
  export PATH="$HOME/.sigcli/bin:$PATH"   # or: . "$HOME/.sigcli/bin/env"
  ```

## Invocation

```bash
sigcli api <resource> <action> [--params '{"key":"value"}'] [-f json]
```

- `-f json` (default) is the format to parse. `table`/`yaml`/`csv` also exist.
- Path/query parameters go in `--params` as a JSON object, e.g.
  `--params '{"connectionId":"abc"}'`.

### Discover, don't guess

The backend maps dynamically onto the Sigma v2 API. Enumerate rather than
assume:

```bash
sigcli api list-prefixes              # list resources (command, pathPrefix, op count)
sigcli api schema /v2/connections     # inspect a resource's operations + param shapes
```

Resolve the right `<resource> <action>` from `list-prefixes`, confirm the
parameters with `schema`, then call.

## Auth

- Reads `SIGMA_CLIENT_ID`, `SIGMA_CLIENT_SECRET`, `SIGMA_BASE_URL` from the
  environment. `sigcli auth login` establishes a session; credentials are cached
  encrypted at `~/.sigcli/credentials.enc`.
- **Never echo or log secret values.** If auth is needed, run `sigcli auth login`
  and report success/failure, not the credentials.

## Exit codes → caller signals

| Code | Meaning | This skill's response |
|---|---|---|
| 0 | Success | Parse JSON, return it. |
| 1 | API error (Sigma returned an error) | Report the API message; do not retry blindly. |
| 2 | Auth error (missing/invalid creds) | Tell the caller auth failed; suggest `sigcli auth login` / checking env. Callers should degrade to warehouse/inference. |
| 3 | Validation (bad args/input) | Re-derive the operation/params via `schema`; fix and retry once. |
| 4 | HTTP / transport | Transient — one bounded retry, then report. Callers should degrade. |

## Workflow

1. Ensure `~/.sigcli/bin` is on PATH (non-interactive).
2. If needed, confirm the operation: `list-prefixes` → `schema <pathPrefix>`.
3. Run `sigcli api <resource> <action> --params '<json>' -f json`.
4. On non-zero exit, map per the table above (retry on 3/4 once; surface 1/2).
5. Return parsed JSON to the caller. For paginated resources, follow the next
   page token the operation exposes until exhausted.

## Behavioral rules

- **Discover at runtime**; don't hard-code resource/action names that may drift.
- **Read by default.** Treat create/update/delete operations as requiring
  explicit caller authorisation.
- **Never print secrets.**
- **Report exit codes honestly** so callers can apply their own fallback (e.g.
  `sigma-org-audit` degrades to marts on exit 2/4).
- Output is data for the caller, not a user-facing message — return the JSON.
