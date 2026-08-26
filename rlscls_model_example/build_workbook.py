#!/usr/bin/env python3
"""Build the two workbooks over the RLS data model, and export the validation one to CSV.

--kind validation (default): a single flat table exposing the fact columns, the
shipped model's per-dimension lookup/RLS helper columns, and Combined RLS. All
aggregation happens locally against the exported CSV — that keeps the workbook
trivial and avoids depending on workbook-side grouping semantics while validating.

--kind secured: the consumer-facing workbook — business columns only, no
scaffolding, relying entirely on the model's own RLS filter.

  python3 build_workbook.py --kind validation --build
  python3 build_workbook.py --kind secured    --build --grant-personas
  python3 build_workbook.py --export <workbookId> --out out/admin.csv

--build creates on first run and updates on every run after, using the id
cached in config.json (dataModelId/secureWorkbookId/validationWorkbookId) —
pass --update <id> to override that instead.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time

from rls_common import load_config, save_config

_cfg = load_config()
if not _cfg.get("dataModelId"):
    raise SystemExit("config.json has no dataModelId yet — run build_model.py first.")

DATA_MODEL_ID = _cfg["dataModelId"]
MODEL_ELEMENT = "shipments"
FOLDER_ID = _cfg["folderId"]
SRC = "Shipments"          # the model element's NAME — the formula prefix

BASE = os.environ.get("SIGMA_BASE_URL", "https://aws-api.sigmacomputing.com")


def columns():
    cols = [
        ("c_id",      "Shipment ID"),
        ("c_office",  "Office"),
        ("c_company", "Company"),
        ("c_site",    "Site"),
        ("c_revenue", "Revenue EUR"),
    ]
    out = [{"id": i, "formula": f"[{SRC}/{n}]", "name": n} for i, n in cols]
    # Mirrors the shipped (Approach B) model. If the model is rebuilt with
    # --compare-shapes, add the " B" / "Lookup Agree" / "A equals B" columns back.
    for cap in ("Office", "Company", "Site"):
        for suffix in ("Team Names", "User Emails", "RLS"):
            name = f"{cap} {suffix}"
            out.append({"id": f"c_{name.lower().replace(' ', '_')}",
                        "formula": f"[{SRC}/{name}]", "name": name})
    for n in ("Combined RLS", "Combined RLS Num"):
        out.append({"id": f"c_{n.lower().replace(' ', '_')}",
                    "formula": f"[{SRC}/{n}]", "name": n})
    return out


def spec():
    return {
        "name": "RLS Additive Grants — Validation",
        "folderId": FOLDER_ID,
        "description": "DIAGNOSTIC. Flat export used to verify the additive RLS "
                       "pattern: row-count preservation and per-user filtering. "
                       "Exposes the model's hidden helper columns (Team Names / "
                       "User Emails / RLS per dimension, Combined RLS). NOTE: "
                       "while the data model has its RLS filter applied, this "
                       "shows only the viewer's own visible rows — rebuild the "
                       "model with --validation-mode to inspect all 8,000 rows. "
                       "Not for end users.",
        "document": {
            "schemaVersion": 1,
            "kind": "workbook",
            "elements": [{
                "id": "detail",
                "kind": "table",
                "name": "RLS Detail",
                "source": {"kind": "data-model",
                           "dataModelId": DATA_MODEL_ID,
                           "elementId": MODEL_ELEMENT},
                "columns": columns(),
            }],
            "pages": [{"id": "page-main", "name": "Validation"}],
            "layout": (
                '<?xml version="1.0" encoding="utf-8"?>\n'
                '<Page type="grid" gridTemplateColumns="repeat(24, 1fr)" '
                'gridTemplateRows="auto" id="page-main">\n'
                '  <Element elementId="detail" gridColumn="1 / 25" gridRow="1 / 25"/>\n'
                '</Page>'
            ),
        },
    }


def secured_spec():
    """The consumer-facing workbook: business columns only, no scaffolding.

    Row-level security is enforced on the data model element, not here — this
    workbook has no filters of its own and cannot weaken them.
    """
    business = ["Shipment ID", "Shipped Date", "Company", "Site", "Office",
                "Mode", "Revenue EUR"]
    return {
        "name": "RLS Additive Grants — Secured View",
        "folderId": FOLDER_ID,
        "description": "Consumer-facing view of the secured shipments table. Every "
                       "row shown is one the viewer is entitled to via the additive "
                       "grants ledger. Row-level security is enforced on the data "
                       "model element, not here.",
        "document": {
            "schemaVersion": 1,
            "kind": "workbook",
            "elements": [{
                "id": "secured",
                "kind": "table",
                "name": "Shipments (secured)",
                "source": {"kind": "data-model",
                           "dataModelId": DATA_MODEL_ID,
                           "elementId": MODEL_ELEMENT},
                "columns": [{"id": f"w_{n.lower().replace(' ', '_')}",
                             "formula": f"[{SRC}/{n}]", "name": n} for n in business],
            }],
            "pages": [{"id": "page-main", "name": "Secured"}],
            "layout": (
                '<?xml version="1.0" encoding="utf-8"?>\n'
                '<Page type="grid" gridTemplateColumns="repeat(24, 1fr)" '
                'gridTemplateRows="auto" id="page-main">\n'
                '  <Element elementId="secured" gridColumn="1 / 25" gridRow="1 / 25"/>\n'
                '</Page>'
            ),
        },
    }


def token():
    out = subprocess.run(
        ["bash", "-lc",
         'source ~/.zshrc >/dev/null 2>&1; '
         'bash /Users/mark.oldfield/Skills/sigma-api/scripts/get-token.sh'],
        capture_output=True, text=True, check=True).stdout
    for line in out.splitlines():
        if "SIGMA_API_TOKEN=" in line:
            return line.split("=", 1)[1].strip().strip("'\"")
    raise SystemExit("no token")


def api(method, path, tok, payload=None, raw=False):
    cmd = ["curl", "-sS", "-X", method, BASE + path,
           "-H", f"Authorization: Bearer {tok}",
           "-H", "Content-Type: application/json", "-w", "\n%{http_code}"]
    if payload is not None:
        cmd += ["--data-binary", "@-"]
    r = subprocess.run(cmd, input=json.dumps(payload) if payload is not None else None,
                       capture_output=True, text=True)
    body, _, code = r.stdout.rpartition("\n")
    if not code.isdigit() or int(code) >= 400:
        print(f"HTTP {code}\n{body}\n{r.stderr}", file=sys.stderr)
        raise SystemExit(1)
    return body if raw else json.loads(body or "{}")


def do_export(tok, workbook_id, out_path):
    """Kick off an async CSV export of the detail element and poll for the file."""
    q = api("POST", f"/v2/workbooks/{workbook_id}/export", tok,
            {"elementId": "detail", "format": {"type": "csv"}})
    qid = q.get("queryId") or q.get("exportId")
    print("export queued:", qid)
    for _ in range(60):
        time.sleep(5)
        cmd = ["curl", "-sS", "-D", "-", f"{BASE}/v2/query/{qid}/download",
               "-H", f"Authorization: Bearer {tok}"]
        r = subprocess.run(cmd, capture_output=True, text=True)
        head, _, body = r.stdout.partition("\r\n\r\n")
        status = head.split()[1] if head.split() else "?"
        if status == "200" and body.strip():
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            with open(out_path, "w") as f:
                f.write(body)
            print(f"wrote {out_path} ({len(body.splitlines())} lines)")
            return
        print(f"  status {status}, waiting…")
    raise SystemExit("export did not complete")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=("validation", "secured"), default="validation",
                    help="which workbook to build")
    ap.add_argument("--build", action="store_true",
                    help="write and push the spec — creates if config.json has no "
                         "cached id for this --kind yet, updates otherwise")
    ap.add_argument("--update", metavar="WORKBOOK_ID",
                    help="override the workbook id instead of using config.json's cached one")
    ap.add_argument("--grant-personas", action="store_true",
                    help="grant view access on this workbook to every persona in config.json")
    ap.add_argument("--export", metavar="WORKBOOK_ID")
    ap.add_argument("--out", default="out/admin.csv")
    args = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    tok = token()
    kind_field = "secureWorkbookId" if args.kind == "secured" else "validationWorkbookId"

    if args.build:
        update_id = args.update or _cfg.get(kind_field)
        s = secured_spec() if args.kind == "secured" else spec()
        fname = f"rls_{args.kind}.workbook.json"
        p = os.path.join(here, "model", fname)
        with open(p, "w") as f:
            json.dump(s, f, indent=2)
        print(f"wrote {p} ({len(s['document']['elements'][0]['columns'])} columns)")

        # Always dry-run the spec first: verify writes nothing and reports {"valid": true}.
        v = api("POST", "/v2/workbooks/spec/verify", tok, s, raw=True)
        print("verify:", v.strip())

        path = (f"/v2/workbooks/{update_id}/spec" if update_id
                else "/v2/workbooks/spec")
        # NB: this endpoint answers in YAML, not JSON, so take the body raw.
        result = api("PUT" if update_id else "POST", path, tok, s, raw=True)
        print(result.strip())

        if not update_id:
            m = re.search(r'^workbookId:\s*"?([^"\n]+)"?', result, re.M)
            if m:
                new_id = m.group(1).strip()
                _cfg[kind_field] = new_id
                save_config(_cfg)
                print(f"  saved {kind_field}={new_id} to config.json — "
                      "future --build runs will update it automatically")
            else:
                # Do NOT continue silently: config.json's *WorkbookId stays null,
                # so the next --build run would see no cached id and POST again,
                # creating a second, duplicate workbook instead of updating this one.
                raise SystemExit(
                    f"created a workbook but couldn't parse its id out of the "
                    f"response above (expected a 'workbookId:' line in the YAML — "
                    f"the raw response was printed above this error). Copy the real "
                    f"id into config.json's {kind_field!r} field by hand before "
                    f"running --build again, or this will create a duplicate."
                )

    if args.grant_personas:
        wb_id = args.update or _cfg.get(kind_field)
        if not wb_id:
            raise SystemExit(f"no {kind_field} in config.json yet — run --build first")
        grants = [{"grantee": {"memberId": p["id"]}, "permission": "view"}
                  for p in _cfg["personas"] if p.get("id")]
        if not grants:
            raise SystemExit("no persona ids in config.json yet — run setup_personas.py first")
        api("POST", f"/v2/workbooks/{wb_id}/grants", tok, {"grants": grants})
        print(f"granted view on {wb_id} to {len(grants)} personas")

    if args.export:
        do_export(tok, args.export, os.path.join(here, args.out))


if __name__ == "__main__":
    main()
