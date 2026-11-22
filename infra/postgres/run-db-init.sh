#!/usr/bin/env bash
# Runs every /db-init/*.sql (infra/db-init/, owner C) against Postgres as the superuser.
# Owner: D. Started by the db-init compose service on EVERY `up`, so the SQL must be
# idempotent (see infra/db-init/README.md).
#
# Passwords: every environment variable named <NAME>_DB_PASSWORD (from infra/.env) is passed
# to psql as the variable <name>_db_password, e.g. TRACKER_DB_PASSWORD -> :'tracker_db_password'.
# Values are never printed. A .sql file that uses a variable that is not set fails.
set -euo pipefail

DIR="${DB_INIT_DIR:-/db-init}"

for i in $(seq 1 30); do
  pg_isready -q && break
  [[ "$i" == 30 ]] && { echo "db-init: postgres not ready" >&2; exit 1; }
  sleep 2
done

vars=()
names=()
for name in $(compgen -e); do
  if [[ "$name" =~ ^[A-Z][A-Z0-9_]*_DB_PASSWORD$ ]]; then
    vars+=(-v "${name,,}=${!name}")
    names+=("${name,,}")
  fi
done

shopt -s nullglob
files=("$DIR"/*.sql)
if [[ ${#files[@]} -eq 0 ]]; then
  echo "db-init: no .sql files in infra/db-init/; nothing to do"
  exit 0
fi
echo "db-init: password variables available: ${names[*]:-(none)}"

# Keep failing statements (which may contain a password) out of the server log.
export PGOPTIONS="-c log_min_error_statement=panic -c log_statement=none"

for file in $(printf '%s\n' "${files[@]}" | sort); do
  echo "db-init: running $(basename "$file")"
  # -X: ignore ~/.psqlrc. -o /dev/null: hide query results (errors and \echo still show).
  # VERBOSITY=terse: no statement context in errors, which could echo a password.
  tr -d '\r' < "$file" | psql -X -q -o /dev/null -v ON_ERROR_STOP=1 -v VERBOSITY=terse \
      "${vars[@]}" -f - \
    || { echo "db-init: FAILED in $(basename "$file")" >&2; exit 1; }
done
echo "db-init: ok (${#files[@]} file(s))"
