#!/usr/bin/env python3
"""Per-persona expected-vs-actual validation, run through Sigma's query engine.

The API credentials authenticate as the operator, so CurrentUserEmail() always
resolves to them. To measure what a persona actually sees, this script briefly
MIRRORS each persona onto the operator account — same team memberships, and the
persona's personal grants re-pointed at the operator's email — exports the
secured workbook, then restores the original state.

That exercises the real mechanism (CurrentUserInTeam + CurrentUserEmail through
Sigma SQL generation); the only substitution is which email string appears in the
ledger. The email branch is independently confirmed, so the substitution is sound.

Cleanup runs in a finally block: an aborted run still restores team membership
and deletes the mirrored grant rows.

  source ~/.zshrc && python3 validate_personas.py
"""

import csv
import io
import json
import os
import subprocess
import sys
import time

from rls_common import get_token as token, load_config

_cfg = load_config()
if not _cfg.get("secureWorkbookId"):
    raise SystemExit("config.json has no secureWorkbookId yet — build_workbook.py --kind secured --build first.")
for p in _cfg["personas"]:
    if not p.get("id"):
        raise SystemExit(f"persona {p['name']!r} has no id in config.json yet — run setup_personas.py first.")

OPERATOR_ID = _cfg["operatorId"]
OPERATOR_EMAIL = _cfg["operatorEmail"]
WORKBOOK_ID = _cfg["secureWorkbookId"]
SCHEMA = _cfg["schema"]
ELEMENT_ID = "secured"
BASE = os.environ.get("SIGMA_BASE_URL", "https://aws-api.sigmacomputing.com")

# team name -> team id, all four demo teams
TEAMS = _cfg["teams"]

# persona -> teams to join, and the personal grants to mirror onto the operator
PERSONAS = [{"name": p["name"], "email": p["email"], "teams": p["teams"],
             "grants": [tuple(g) for g in p["grants"]]}
            for p in _cfg["personas"]]


def sh(cmd, **kw):
    return subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True,
                          text=True, **kw)


def psql(sql):
    r = sh(f'source .env && psql -tAq -c "{sql}"', executable="/bin/bash")
    if r.returncode:
        raise RuntimeError(f"psql failed: {r.stderr}")
    return r.stdout.strip()


def team_patch(tok, team_id, action, member_id):
    sh(["curl", "-sS", "-X", "PATCH", f"{BASE}/v2/teams/{team_id}/members",
        "-H", f"Authorization: Bearer {tok}", "-H", "Content-Type: application/json",
        "-d", json.dumps({action: [member_id]})])


def in_team(tok, team_id, member_id):
    r = sh(["curl", "-sS", f"{BASE}/v2/teams/{team_id}/members",
            "-H", f"Authorization: Bearer {tok}"])
    try:
        return any(e.get("userId") == member_id
                   for e in json.loads(r.stdout).get("entries", []))
    except Exception:
        return False


def export_rowcount(tok):
    """Export the secured element and return how many rows came back."""
    r = sh(["curl", "-sS", "-X", "POST", f"{BASE}/v2/workbooks/{WORKBOOK_ID}/export",
            "-H", f"Authorization: Bearer {tok}", "-H", "Content-Type: application/json",
            "-d", json.dumps({"elementId": ELEMENT_ID, "format": {"type": "csv"}})])
    qid = json.loads(r.stdout)["queryId"]
    for _ in range(60):
        time.sleep(5)
        d = sh(["curl", "-sS", "-w", "\n%{http_code}", f"{BASE}/v2/query/{qid}/download",
                "-H", f"Authorization: Bearer {tok}"])
        body, _, code = d.stdout.rpartition("\n")
        if code == "200":
            return sum(1 for _ in csv.DictReader(io.StringIO(body)))
    raise RuntimeError("export never completed")


def oracle_count(persona):
    teams = ",".join(f"'{t}'" for t in persona["teams"]) or ""
    arr = f"ARRAY[{teams}]::text[]" if teams else "ARRAY[]::text[]"
    return int(psql(f"set search_path to {SCHEMA}; "
                    f"select {SCHEMA}.rls_visible_count({arr}, '{persona['email']}')"
                    ).splitlines()[-1])


def main():
    tok = token()

    # Snapshot the operator's real membership of the demo teams, and restore it
    # exactly at the end. Any demo team the operator belongs to but the persona
    # does not would otherwise leak its grants into every measurement — a
    # persona expected to see 0 rows would report the operator's own scope.
    original = {t: in_team(tok, tid, OPERATOR_ID) for t, tid in TEAMS.items()}
    if any(original.values()):
        print("operator is currently in: "
              f"{', '.join(t for t, v in original.items() if v)} "
              "— temporarily leaving these, will restore at the end\n")

    results = []
    try:
        results = run_personas(tok)
    finally:
        for t, was_member in original.items():
            if was_member != in_team(tok, TEAMS[t], OPERATOR_ID):
                team_patch(tok, TEAMS[t], "add" if was_member else "remove", OPERATOR_ID)
        restored = {t: in_team(tok, TEAMS[t], OPERATOR_ID) for t in TEAMS}
        print("\nteam membership restored:",
              "OK" if restored == original else f"*** MISMATCH {restored} vs {original}")

    print("\n| persona | email | expected rows | actual rows | result |")
    print("|---|---|---|---|---|")
    for n, e, exp, act in results:
        print(f"| {n} | `{e}` | {exp} | {act} | {'PASS' if exp == act else 'FAIL'} |")
    if any(exp != act for _, _, exp, act in results):
        sys.exit(1)


def run_personas(tok):
    results = []
    for p in PERSONAS:
        expected = oracle_count(p)
        want = set(p["teams"])
        granted = []
        try:
            # Set demo-team membership to EXACTLY the persona's set.
            for t, tid in TEAMS.items():
                member = in_team(tok, tid, OPERATOR_ID)
                if t in want and not member:
                    team_patch(tok, tid, "add", OPERATOR_ID)
                elif t not in want and member:
                    team_patch(tok, tid, "remove", OPERATOR_ID)
            for etype, evalue in p["grants"]:
                psql(f"insert into {SCHEMA}.rls_entity_grants values "
                     f"('{etype}','{evalue}','user','{OPERATOR_EMAIL}') "
                     f"on conflict do nothing")
                granted.append((etype, evalue))
            time.sleep(3)   # let membership settle before querying
            actual = export_rowcount(tok)
        finally:
            # Team membership is reconciled by the caller's snapshot restore;
            # only the mirrored grant rows need undoing here.
            if granted:
                psql(f"delete from {SCHEMA}.rls_entity_grants "
                     f"where principal_id = '{OPERATOR_EMAIL}'")
        results.append((p["name"], p["email"], expected, actual))
        print(f"  {p['name']:9} expected={expected:5}  actual={actual:5}  "
              f"{'PASS' if expected == actual else 'FAIL'}")

    left = psql(f"select count(*) from {SCHEMA}.rls_entity_grants "
                f"where principal_id='{OPERATOR_EMAIL}'").splitlines()[-1]
    print(f"\ncleanup: operator grant rows remaining = {left} (want 0)")
    return results


if __name__ == "__main__":
    main()
