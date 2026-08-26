#!/usr/bin/env python3
"""Generate JWT-signed Sigma embed URLs, one per test persona.

Each URL opens the secured workbook AS that persona, so CurrentUserEmail() and
CurrentUserInTeam() resolve to them rather than to whoever generated the link.
That is what makes per-user row-level filtering observable without knowing each
test account's password.

Signing uses SIGMA_CLIENT_ID / SIGMA_CLIENT_SECRET. Those must belong to a
Developer Access credential with embedding enabled; a REST-only credential is
rejected at load time with an invalid-token error.

  source .env 2>/dev/null; source ~/.zshrc; python3 make_embed_urls.py
"""

import base64
import hashlib
import hmac
import json
import os
import sys
import time
import uuid

from rls_common import api, get_token, load_config

_cfg = load_config()
if not _cfg.get("secureWorkbookId"):
    raise SystemExit("config.json has no secureWorkbookId yet — build_workbook.py --kind secured --build first.")

PERSONAS = _cfg["personas"]  # each has name/email/id/teams/grants
SESSION_SECONDS = 3600


def workbook_url():
    """The browser URL isn't derivable from the workbook id alone — Sigma URLs
    use a separate short slug — so fetch it from the workbook itself."""
    wb = api("GET", f"/v2/workbooks/{_cfg['secureWorkbookId']}", get_token())
    url = wb.get("url")
    if not url:
        raise SystemExit(f"GET /v2/workbooks/{_cfg['secureWorkbookId']} returned no url field: {wb}")
    return url


def b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def sign(payload: dict, client_id: str, secret: str) -> str:
    header = {"alg": "HS256", "typ": "JWT", "kid": client_id}
    signing_input = f"{b64(json.dumps(header).encode())}.{b64(json.dumps(payload).encode())}"
    sig = hmac.new(secret.encode(), signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{b64(sig)}"


def main():
    cid = os.environ.get("SIGMA_CLIENT_ID")
    sec = os.environ.get("SIGMA_CLIENT_SECRET")
    if not cid or not sec:
        sys.exit("SIGMA_CLIENT_ID / SIGMA_CLIENT_SECRET not set — source ~/.zshrc first")

    url = workbook_url()
    now = int(time.time())
    for p in PERSONAS:
        payload = {
            "sub": p["email"],
            "iss": cid,
            "jti": str(uuid.uuid4()),
            "iat": now,
            "exp": now + SESSION_SECONDS,
            "aud": "sigmacomputing",
            "scope": "embed_read",
        }
        token = sign(payload, cid, sec)
        print(f"\n### {p['name']}  ({p['email']})")
        print(f"{url}?:embed=true&:jwt={token}")


if __name__ == "__main__":
    main()
