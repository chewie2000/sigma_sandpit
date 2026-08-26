#!/usr/bin/env python3
"""One-time bootstrap: resolve the operator, demo teams, and test personas
that build_model.py / build_workbook.py / validate_personas.py / make_embed_urls.py
all read out of config.json.

Idempotent — safe to re-run. Existing teams/personas are found by lookup and
left alone; only missing ones are created. Resolved IDs are written back into
config.json so this only has to run once per installing org.

  cp config.example.json config.json   # then fill in the placeholders
  source ~/.zshrc && python3 setup_personas.py
"""

import sys
from urllib.parse import quote

from rls_common import api, get_token, load_config, save_config


def find_member_by_email(tok, email):
    # A literal "+" (common in persona aliases like x+user1@domain) is read
    # server-side as a space unless percent-encoded — confirmed live: an
    # unencoded search for "a+b@x.com" matched nothing even though the member
    # existed, so this quote() is not optional.
    res = api("GET", f"/v2/members?search={quote(email, safe='')}", tok)
    for m in res.get("entries", []):
        if m.get("email", "").lower() == email.lower():
            return m
    return None


def find_team_by_name(tok, name):
    res = api("GET", f"/v2/teams?search={quote(name, safe='')}", tok)
    for t in res.get("entries", []):
        if t.get("name") == name:
            return t
    return None


def resolve_operator(tok, cfg):
    if cfg.get("operatorId"):
        print(f"operator: using cached id {cfg['operatorId']}")
        return
    email = cfg["operatorEmail"]
    member = find_member_by_email(tok, email)
    if not member:
        raise SystemExit(
            f"could not find a Sigma member with email {email!r} — "
            "operatorEmail must be the email you log into Sigma with."
        )
    cfg["operatorId"] = member["memberId"]
    print(f"operator: resolved {email} -> {cfg['operatorId']}")


def resolve_teams(tok, cfg):
    for name in list(cfg["teams"]):
        if cfg["teams"][name]:
            print(f"team {name!r}: using cached id {cfg['teams'][name]}")
            continue
        team = find_team_by_name(tok, name)
        if team:
            cfg["teams"][name] = team["teamId"]
            print(f"team {name!r}: found existing id {team['teamId']}")
            continue
        created = api("POST", "/v2/teams", tok, {"name": name})
        team_id = created.get("teamId") or created.get("id")
        if not team_id:
            raise SystemExit(f"team creation for {name!r} did not return an id: {created}")
        cfg["teams"][name] = team_id
        print(f"team {name!r}: created {team_id}")


def resolve_personas(tok, cfg):
    for p in cfg["personas"]:
        if p.get("id"):
            print(f"persona {p['name']!r}: using cached id {p['id']}")
            continue
        member = find_member_by_email(tok, p["email"])
        if member:
            p["id"] = member["memberId"]
            print(f"persona {p['name']!r}: found existing member {p['id']}")
            continue
        print(f"persona {p['name']!r}: no member found for {p['email']!r}, inviting...")
        try:
            created = api("POST", "/v2/members", tok,
                          {"email": p["email"], "memberType": "analyze",
                           "firstName": p["name"], "lastName": "(RLS demo persona)"})
            p["id"] = created.get("memberId")
        except SystemExit:
            raise SystemExit(
                f"could not invite {p['email']!r} automatically (see error above). "
                "Invite this test user manually in Sigma (Admin > Members) with the "
                "'analyze' role, then re-run this script — it will find them by email."
            )
        print(f"persona {p['name']!r}: invited, id={p['id']}")


def validate_prerequisites(tok, cfg):
    conn = api("GET", f"/v2/connections/{cfg['connectionId']}", tok, ok404=True)
    if conn is None:
        raise SystemExit(
            f"connectionId {cfg['connectionId']!r} not found — create the warehouse "
            "connection in Sigma first (Admin > Connections) and put its id in config.json."
        )
    folder = api("GET", f"/v2/files/{cfg['folderId']}", tok, ok404=True)
    if folder is None:
        raise SystemExit(
            f"folderId {cfg['folderId']!r} not found — create the target folder in "
            "Sigma first and put its id in config.json."
        )
    print(f"connection {cfg['connectionId']} and folder {cfg['folderId']} both exist")


def main():
    cfg = load_config()
    tok = get_token()
    validate_prerequisites(tok, cfg)
    resolve_operator(tok, cfg)
    resolve_teams(tok, cfg)
    resolve_personas(tok, cfg)
    save_config(cfg)
    print("\nconfig.json updated. Next: ./run_sql.sh, then build_model.py.")


if __name__ == "__main__":
    sys.exit(main())
