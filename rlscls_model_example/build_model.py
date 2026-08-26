#!/usr/bin/env python3
"""Build the additive-RLS Sigma data model spec and POST it.

Shipped shape: a compound-key Lookup straight against the unfiltered grants
ledger, matching (entity_type, entity_value) together, per scoping dimension.
No per-dimension child tables — a constant "<Dim> Key" column supplies the
entity_type half of the key pair.

An earlier per-dimension-child-table shape (Approach A) was built alongside
this one and shown to agree on every row before being deleted. Pass
--compare-shapes to rebuild that comparison (adds the child tables, the " B"
suffix on this shape's names, and an agreement column per dimension).

connectionId / folderId / schema come from config.json (see config.example.json
and setup_personas.py). --update defaults to config.json's dataModelId once one
exists, so a create-then-update flow needs no id passed by hand after the first run.

Usage:
  python3 build_model.py --dry-run     # write spec JSON, don't call the API
  python3 build_model.py               # create on first run, update thereafter
  python3 build_model.py --update <id> # override the id instead of using config.json's
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

from rls_common import load_config, save_config

_cfg = load_config()
CONNECTION_ID = _cfg["connectionId"]

# Postgres connections index as TWO path segments — [schema, table] — not the
# three ([database, schema, table]) that Snowflake connections use. Confirmed
# against GET /v2/connections/paths?connectionId=... after a sync.
SCHEMA = _cfg["schema"]
GRANTS_TBL, SHIPMENTS_TBL = "rls_entity_grants", "rls_shipments"

FOLDER_ID = _cfg["folderId"]

MODEL_NAME = "RLS Additive Grants Example"
MODEL_DESC = (
    "Additive row-level security: a user's scope is the UNION of every team grant "
    "and personal grant they hold. CurrentUserInTeam() is resolved per grant ROW "
    "(never against a concatenated team-name string - it silently fails for "
    "multi-team entities) and aggregated with Max(); CurrentUserEmail() covers "
    "personal grants. A compound-key Lookup() matches (entity_type, entity_value) "
    "against one grants ledger - no per-dimension child tables, no user "
    "attributes, no team-to-user mapping replicated into the warehouse."
)

# The three scoping dimensions: (slug, entity_type value, shipment column display name)
DIMENSIONS = [
    ("office",  "office",  "Office"),
    ("company", "company", "Company"),
    ("site",    "site",    "Site"),
]


def grants_element():
    """The grants ledger, plus the two null-producing helper columns.

    If() with no else-branch returns null, and ListAgg skips nulls — that is what
    lets one ledger carry both team and user principals without them colliding
    when they are aggregated separately.
    """
    return {
        "id": "grants",
        "kind": "table",
        "name": "Entity Grants",
        "description": "The grants ledger. Additive by construction: no priority column.",
        "source": {
            "kind": "warehouse-table",
            "connectionId": CONNECTION_ID,
            "path": [SCHEMA, GRANTS_TBL],
        },
        "columns": [
            {"id": "g_etype",  "formula": f"[{GRANTS_TBL}/entity_type]",    "name": "Entity Type"},
            {"id": "g_evalue", "formula": f"[{GRANTS_TBL}/entity_value]",   "name": "Entity Value"},
            {"id": "g_ptype",  "formula": f"[{GRANTS_TBL}/principal_type]", "name": "Principal Type"},
            {"id": "g_pid",    "formula": f"[{GRANTS_TBL}/principal_id]",   "name": "Principal ID"},
            {"id": "g_team",   "formula": 'If([Principal Type] = "team", [Principal ID])',
             "name": "Team Principal",
             "description": "Diagnostic only: null for user grants, so ListAgg yields teams."},
            {"id": "g_user",   "formula": 'If([Principal Type] = "user", [Principal ID])',
             "name": "User Principal",
             "description": "Diagnostic only: null for team grants, so ListAgg yields emails."},

            # ---- The heart of the pattern. ----------------------------------
            # Membership is resolved HERE, one grant row at a time, where
            # [Principal ID] is always a SINGLE team name.
            #
            # CurrentUserInTeam() does NOT parse a comma-separated string — it
            # compares the whole string to one team name. Aggregating team names
            # with ListAgg and passing the result in therefore silently returns
            # false for any entity granted to more than one team, which is exactly
            # the case additive RLS exists to support. Verified: an entity granted
            # to "Team A" alone matched, the same user against "Team A,Team B"
            # did not.
            {"id": "g_applies", "formula": (
                'If([Principal Type] = "team", '
                'Coalesce(CurrentUserInTeam([Principal ID]), False), '
                '[Principal ID] = CurrentUserEmail())'),
             "name": "Grant Applies",
             "description": "Does THIS grant row apply to the current user?"},
            {"id": "g_applies_n", "formula": 'If([Grant Applies] = True, 1, 0)',
             "name": "Grant Applies Num",
             "description": "Numeric form so Max() can aggregate it additively: "
                            "Max = 1 means at least one grant matched."},
        ],
    }


def dimension_child_element(slug, entity_type, _col):
    """Approach A: one child table per dimension, filtered to that entity_type.

    Filtering first means a single-key Lookup on entity_value can never match a
    row belonging to a different dimension.
    """
    name = f"{slug.capitalize()} Grants"
    return {
        "id": f"grants_{slug}",
        "kind": "table",
        "name": name,
        "description": f"Approach A: ledger filtered to entity_type = '{entity_type}'.",
        "source": {"kind": "table", "elementId": "grants"},
        "columns": [
            {"id": "c_etype",  "formula": "[Entity Grants/Entity Type]",     "name": "Entity Type"},
            {"id": "c_evalue", "formula": "[Entity Grants/Entity Value]",    "name": "Entity Value"},
            {"id": "c_team",   "formula": "[Entity Grants/Team Principal]",  "name": "Team Principal"},
            {"id": "c_user",   "formula": "[Entity Grants/User Principal]",  "name": "User Principal"},
            {"id": "c_applies_n", "formula": "[Entity Grants/Grant Applies Num]",
             "name": "Grant Applies Num"},
        ],
        "filters": [{
            "id": f"f_{slug}",
            "columnId": "c_etype",
            "kind": "text-match",
            "mode": "equals",
            "value": entity_type,
            "case": "insensitive",
        }],
    }


def shipments_element(hide_helpers=True, apply_filter=True, compare_shapes=False):
    """Build the secured fact table.

    hide_helpers   — hide the lookup/RLS scaffolding from downstream users. Turned
                     off while validating so a workbook can read those columns.
    apply_filter   — filter the element to the current user's visible rows. Turned
                     off while validating so expected-vs-actual covers all rows.
    compare_shapes — also emit the Approach-A columns and the A-vs-B agreement
                     columns. Only needed to reproduce the original comparison;
                     the shipped model is Approach B alone.
    """
    H = hide_helpers
    cols = [
        {"id": "s_id",      "formula": f"[{SHIPMENTS_TBL}/shipment_id]",  "name": "Shipment ID"},
        {"id": "s_date",    "formula": f"[{SHIPMENTS_TBL}/shipped_date]", "name": "Shipped Date"},
        {"id": "s_company", "formula": f"[{SHIPMENTS_TBL}/company]",      "name": "Company"},
        {"id": "s_site",    "formula": f"[{SHIPMENTS_TBL}/site]",         "name": "Site"},
        {"id": "s_office",  "formula": f"[{SHIPMENTS_TBL}/office]",       "name": "Office"},
        {"id": "s_mode",    "formula": f"[{SHIPMENTS_TBL}/mode]",         "name": "Mode"},
        {"id": "s_revenue", "formula": f"[{SHIPMENTS_TBL}/revenue_eur]",  "name": "Revenue EUR"},
        {"id": "s_teu",     "formula": f"[{SHIPMENTS_TBL}/teu]",          "name": "TEU"},
    ]

    rls_terms, legacy_terms = [], []

    for slug, entity_type, col in DIMENSIONS:
        Cap = slug.capitalize()
        sfx = " B" if compare_shapes else ""      # keep the old names when comparing

        # Compound-key Lookup against the UNFILTERED ledger, matching entity_type
        # AND entity_value together. A constant column supplies the literal side of
        # the entity_type key pair. This is why no per-dimension child table is
        # needed: the compound key does the discrimination a filter used to do.
        cols += [
            {"id": f"b_{slug}_key", "hidden": H, "name": f"{Cap} Key",
             "formula": f'"{entity_type}"'},

            # Diagnostics only — nothing depends on these. They answer "why can
            # this person see this row?" without re-deriving it by hand. Note they
            # must NOT be fed to CurrentUserInTeam(): see Grant Applies on the
            # ledger element for why.
            {"id": f"b_{slug}_teams", "hidden": H, "name": f"{Cap} Team Names{sfx}",
             "formula": (
                 f'Lookup(ListAgg([Entity Grants/Team Principal], ","), '
                 f'[{Cap} Key], [Entity Grants/Entity Type], '
                 f'[{col}], [Entity Grants/Entity Value])'
             )},
            {"id": f"b_{slug}_users", "hidden": H, "name": f"{Cap} User Emails{sfx}",
             "formula": (
                 f'Lookup(ListAgg([Entity Grants/User Principal], ","), '
                 f'[{Cap} Key], [Entity Grants/Entity Type], '
                 f'[{col}], [Entity Grants/Entity Value])'
             )},

            # The actual verdict. Max() over the per-grant-row booleans is the
            # additive union; idempotent, so overlapping grants cannot double-count.
            {"id": f"b_{slug}_rls", "hidden": H, "name": f"{Cap} RLS{sfx}",
             "formula": (
                 f'Coalesce(Lookup(Max([Entity Grants/Grant Applies Num]), '
                 f'[{Cap} Key], [Entity Grants/Entity Type], '
                 f'[{col}], [Entity Grants/Entity Value]), 0) = 1'
             )},
        ]
        rls_terms.append(f"[{Cap} RLS{sfx}]")

        if compare_shapes:
            child = f"{Cap} Grants"
            cols += [
                {"id": f"a_{slug}_teams", "hidden": H, "name": f"{Cap} Team Names",
                 "formula": f'Lookup(ListAgg([{child}/Team Principal], ","), [{col}], [{child}/Entity Value])'},
                {"id": f"a_{slug}_users", "hidden": H, "name": f"{Cap} User Emails",
                 "formula": f'Lookup(ListAgg([{child}/User Principal], ","), [{col}], [{child}/Entity Value])'},
                {"id": f"a_{slug}_rls", "hidden": H, "name": f"{Cap} RLS",
                 "formula": (
                     f'Coalesce(Lookup(Max([{child}/Grant Applies Num]), '
                     f'[{col}], [{child}/Entity Value]), 0) = 1'
                 )},
                {"id": f"agree_{slug}", "hidden": H, "name": f"{Cap} Lookup Agree",
                 "formula": (
                     f'(Coalesce([{Cap} Team Names], "") = Coalesce([{Cap} Team Names B], "")) And '
                     f'(Coalesce([{Cap} User Emails], "") = Coalesce([{Cap} User Emails B], ""))'
                 )},
            ]
            legacy_terms.append(f"[{Cap} RLS]")

    combined_name = "Combined RLS B" if compare_shapes else "Combined RLS"
    cols.append(
        {"id": "combined_b", "hidden": H, "name": combined_name,
         "formula": " Or ".join(rls_terms),
         "description": "Visible if ANY dimension grants this row — the additive union."})

    if compare_shapes:
        cols += [
            {"id": "combined_a", "hidden": H, "name": "Combined RLS",
             "formula": " Or ".join(legacy_terms),
             "description": "Approach A verdict (filtered child tables)."},
            {"id": "ab_agree", "hidden": H, "name": "A equals B",
             "formula": "[Combined RLS] = [Combined RLS B]"},
            {"id": "lookups_agree", "hidden": H, "name": "Lookups Agree",
             "formula": " And ".join(f"[{c.capitalize()} Lookup Agree]" for c, _, _ in DIMENSIONS)},
        ]

    # Numeric mirror of the verdict, purely so the element filter can be a
    # number-range. A `list` filter over a BOOLEAN column is accepted by the API
    # and enforced correctly in generated SQL, but Sigma's filter editor renders
    # it as "Invalid filter" — alarming on a security control.
    cols.append(
        {"id": "combined_num", "hidden": H, "name": "Combined RLS Num",
         "formula": f'If([{combined_name}] = True, 1, 0)',
         "description": "Filter target: 1 = visible to the current user."})

    el = {
        "id": "shipments",
        "kind": "table",
        "name": "Shipments",
        "description": "Fact table secured by the additive grants pattern.",
        "source": {
            "kind": "warehouse-table",
            "connectionId": CONNECTION_ID,
            "path": [SCHEMA, SHIPMENTS_TBL],
        },
        "columns": cols,
    }
    if apply_filter:
        # Applied on the ELEMENT, never on a control — a control is
        # viewer-changeable, which would make the security bypassable.
        el["filters"] = [{
            "id": "f_rls",
            "columnId": "combined_num",
            "kind": "number-range",
            "min": 1,
            "max": 1,
            "includeNulls": "never",
        }]
    return el


def build_spec(hide_helpers=True, apply_filter=True, compare_shapes=False):
    # Element order matters: referenced elements must precede their referrers.
    elements = [grants_element()]
    if compare_shapes:
        elements += [dimension_child_element(*d) for d in DIMENSIONS]
    elements.append(shipments_element(hide_helpers, apply_filter, compare_shapes))
    return {
        "name": MODEL_NAME,
        "description": MODEL_DESC,
        "schemaVersion": 1,   # required by POST /v2/dataModels/spec
        "folderId": FOLDER_ID,
        "pages": [{"id": "page1", "name": "Model", "elements": elements}],
    }


def get_token():
    out = subprocess.run(
        ["bash", "-lc",
         'source ~/.zshrc >/dev/null 2>&1; '
         'bash /Users/mark.oldfield/Skills/sigma-api/scripts/get-token.sh'],
        capture_output=True, text=True, check=True).stdout
    for line in out.splitlines():
        if "SIGMA_API_TOKEN=" in line:
            return line.split("=", 1)[1].strip().strip("'\"")
    raise SystemExit("could not obtain SIGMA_API_TOKEN")


def call_api(method, path, token, payload=None):
    """Shell out to curl — this Python has no CA bundle configured, so urllib
    fails SSL verification against the Sigma API."""
    base = os.environ.get("SIGMA_BASE_URL", "https://aws-api.sigmacomputing.com")
    cmd = ["curl", "-sS", "-X", method, base + path,
           "-H", f"Authorization: Bearer {token}",
           "-H", "Content-Type: application/json",
           "-w", "\n%{http_code}"]
    if payload is not None:
        cmd += ["--data-binary", "@-"]
    r = subprocess.run(cmd, input=json.dumps(payload) if payload is not None else None,
                       capture_output=True, text=True)
    body, _, code = r.stdout.rpartition("\n")
    if not code.isdigit() or int(code) >= 400:
        print(f"HTTP {code}\n{body}\n{r.stderr}", file=sys.stderr)
        raise SystemExit(1)
    return json.loads(body or "{}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--update", metavar="DATA_MODEL_ID",
                    help="defaults to config.json's dataModelId once one has been created")
    ap.add_argument("--compare-shapes", action="store_true",
                    help="also build Approach A (filtered child tables) alongside "
                         "Approach B, with agreement columns, to reproduce the "
                         "original equivalence comparison")
    ap.add_argument("--validation-mode", action="store_true",
                    help="expose helper columns and skip the RLS filter, so the "
                         "validation workbook can compare expected vs actual")
    args = ap.parse_args()

    spec = build_spec(hide_helpers=not args.validation_mode,
                      apply_filter=not args.validation_mode,
                      compare_shapes=args.compare_shapes)
    here = os.path.dirname(os.path.abspath(__file__))
    out = os.path.join(here, "model", "rls_model.spec.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(spec, f, indent=2)
    n_cols = sum(len(e["columns"]) for e in spec["pages"][0]["elements"])
    print(f"wrote {out}  ({len(spec['pages'][0]['elements'])} elements, {n_cols} columns)")
    if args.dry_run:
        return

    token = get_token()
    update_id = args.update or _cfg.get("dataModelId")
    if update_id:
        res = call_api("PUT", f"/v2/dataModels/{update_id}/spec", token, spec)
        print(f"updated {update_id}")
        # PUT .../spec does NOT propagate the top-level "description" field (name
        # and description are file metadata, not part of the structural spec) —
        # confirmed by reading it back unchanged after a PUT that changed it.
        # PATCH /v2/files/{id} is the endpoint that actually updates it.
        call_api("PATCH", f"/v2/files/{update_id}", token, {"description": spec["description"]})
        print("  description synced via PATCH /v2/files")
    else:
        res = call_api("POST", "/v2/dataModels/spec", token, spec)
        print("created:", json.dumps(
            {k: res.get(k) for k in ("dataModelId", "name", "url")}, indent=2))
        if not res.get("dataModelId"):
            # Do NOT continue silently: config.json's dataModelId stays null, so
            # the next run would see no cached id and POST again, creating a
            # second, duplicate model instead of updating this one.
            raise SystemExit(
                "created a model but the response above has no 'dataModelId' — "
                "copy the real id into config.json's dataModelId field by hand "
                "before running again, or this will create a duplicate."
            )
        _cfg["dataModelId"] = res["dataModelId"]
        save_config(_cfg)
        print(f"  saved dataModelId to config.json — future runs update it automatically")


if __name__ == "__main__":
    main()
