"""Shared config/API helpers for the additive-RLS example.

config.json (gitignored) holds everything specific to the installing org:
connection/folder IDs, the operator's own identity, the demo teams, and the
test personas. Copy config.example.json to config.json and fill it in before
running setup_personas.py.

  source ~/.zshrc && python3 setup_personas.py
"""

import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "config.json")
EXAMPLE_PATH = os.path.join(HERE, "config.example.json")
BASE = os.environ.get("SIGMA_BASE_URL", "https://aws-api.sigmacomputing.com")

PLACEHOLDER_PREFIX = "REPLACE_WITH_"


def load_config():
    if not os.path.exists(CONFIG_PATH):
        raise SystemExit(
            f"{CONFIG_PATH} not found. Copy config.example.json to config.json "
            "and fill in connectionId / folderId / schema / operatorEmail / "
            "persona emails first."
        )
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    unfilled = _find_placeholders(cfg)
    if unfilled:
        raise SystemExit(
            "config.json still has placeholder values that need filling in:\n  "
            + "\n  ".join(unfilled)
        )
    return cfg


def save_config(cfg):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")


def _find_placeholders(obj, path=""):
    found = []
    if isinstance(obj, str) and obj.startswith(PLACEHOLDER_PREFIX):
        found.append(path or "<root>")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            found += _find_placeholders(v, f"{path}.{k}" if path else k)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            found += _find_placeholders(v, f"{path}[{i}]")
    return found


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


def api(method, path, token, payload=None, raw=False, ok404=False):
    """Shell out to curl — matches the rest of this project (no CA bundle
    configured for urllib against the Sigma API)."""
    cmd = ["curl", "-sS", "-X", method, BASE + path,
           "-H", f"Authorization: Bearer {token}",
           "-H", "Content-Type: application/json",
           "-w", "\n%{http_code}"]
    if payload is not None:
        cmd += ["--data-binary", "@-"]
    r = subprocess.run(cmd, input=json.dumps(payload) if payload is not None else None,
                       capture_output=True, text=True)
    body, _, code = r.stdout.rpartition("\n")
    if code == "404" and ok404:
        return None
    if not code.isdigit() or int(code) >= 400:
        print(f"HTTP {code} on {method} {path}\n{body}\n{r.stderr}", file=sys.stderr)
        raise SystemExit(1)
    if raw:
        return body
    return json.loads(body or "{}")
