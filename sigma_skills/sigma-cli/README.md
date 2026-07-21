# sigma-cli

> **Proof of concept.** This skill is a reference implementation shared to demonstrate an approach and give others something to extrapolate from — not a finished, supported, or authoritative tool. Take the ideas, adapt the patterns, build your own.

> **Disclaimer:** This skill (and the repository it lives in) contains personal scripts and tools written independently by the author. Although the author is employed by Sigma Computing, this work is not created, endorsed, tested, or supported by Sigma Computing in any capacity. These scripts are provided as-is, with no warranty or guarantee of fitness for any purpose. Use at your own risk. For official Sigma Computing documentation, support, and tooling, refer to [Sigma's official documentation](https://help.sigmacomputing.com).

A reusable Claude Code sub-skill that drives the **`sigcli`** command-line
interface to fetch **live** data from a Sigma organisation. Other skills delegate
to it when they need current, observed state instead of warehouse snapshots or
inference (e.g. `sigma-org-audit --source both`).

> **Private beta.** `sigcli` is a private-beta tool (observed v0.0.12); its
> command surface may change. This skill discovers resources and operation shapes
> at runtime (`sigcli api list-prefixes`, `sigcli api schema <path>`) rather than
> hard-coding them, so it tolerates churn.

## What it encapsulates

- **Invocation:** `sigcli api <resource> <action> [--params '{...}'] [-f json]`,
  JSON by default.
- **Discovery:** `list-prefixes` to enumerate resources, `schema <pathPrefix>`
  to inspect operations and parameters.
- **Auth:** reads `SIGMA_CLIENT_ID` / `SIGMA_CLIENT_SECRET` / `SIGMA_BASE_URL`
  from the environment; `sigcli auth login`; encrypted cache at
  `~/.sigcli/credentials.enc`. Never prints secrets.
- **PATH:** binary at `~/.sigcli/bin/sigcli`; non-interactive shells must source
  `~/.sigcli/bin/env` or prepend `~/.sigcli/bin` to PATH.
- **Exit codes:** 0 ok · 1 API error · 2 auth · 3 validation · 4 HTTP — mapped to
  caller signals so dependents can degrade gracefully.

## Files

| File | Purpose |
|---|---|
| `SKILL.md` | Workflow, invocation, auth, exit-code handling, behavioral rules. |
| `reference/command-map.md` | Resource → command cheatsheet + runtime discovery recipe. |

## Related

- `sigma-org-audit` — the primary caller; uses this for live verification with
  SIGCLI authoritative on conflict.
