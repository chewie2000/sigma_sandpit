#!/usr/bin/env bash
# Build (or rebuild) the additive-RLS demo objects in Postgres (Snowflake).
#
#   ./run_sql.sh            # full rebuild: schema -> seed -> shipments -> validate
#   ./run_sql.sh 04         # run just one step by numeric prefix
#
# Credentials come from ./.env (gitignored). Fill PGPASSWORD and PGDATABASE there.

set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -f .env ]]; then
  echo "error: .env not found — copy the template and fill in PGPASSWORD/PGDATABASE" >&2
  exit 1
fi
# shellcheck disable=SC1091
source .env

for v in PGHOST PGUSER PGDATABASE PGPASSWORD; do
  if [[ -z "${!v:-}" ]]; then
    echo "error: $v is empty in .env" >&2
    exit 1
  fi
done

SCHEMA="${RLS_SCHEMA:-mark_o}"
FILTER="${1:-}"

for f in sql/*.sql; do
  base="$(basename "$f")"
  [[ -n "$FILTER" && "$base" != "$FILTER"* ]] && continue
  echo "── $base ─────────────────────────────────────────────"
  psql -v ON_ERROR_STOP=1 -v "schema=$SCHEMA" -f "$f"
done
