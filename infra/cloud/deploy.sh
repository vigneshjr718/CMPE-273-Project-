#!/usr/bin/env bash
# Deploy or update PartStream on the cloud VM. Owner: D. Run on the VM from anywhere.
#
#   infra/cloud/deploy.sh                    # all services in the "apps" profile
#   infra/cloud/deploy.sh tracker coordinator   # only these (plus their dependencies)
#
#   DEPLOY_BRANCH=<branch>   branch to deploy (default: the branch currently checked out)
#   SKIP_DATA_DISK_CHECK=1   skip the encrypted-disk checks (never on the real VM)
#
# Uses ONLY infra/docker-compose.yml. The dev override (which publishes Postgres/Kafka/API
# ports) is never loaded, so only the frontend/proxy port is exposed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
BRANCH="${DEPLOY_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
[[ "$BRANCH" != "HEAD" ]] || { echo "deploy: ERROR: detached HEAD; set DEPLOY_BRANCH" >&2; exit 1; }
COMPOSE=(docker compose -f infra/docker-compose.yml --profile apps)

die() { echo "deploy: ERROR: $*" >&2; exit 1; }

# --- preflight -------------------------------------------------------------------------
[[ -f infra/.env ]] || die "infra/.env is missing (see infra/cloud/provision.md section 6)"
if grep -q 'local_only' infra/.env; then
  die "infra/.env still contains local_only placeholder passwords"
fi
# Every :'<name>_db_password' used by infra/db-init/*.sql needs <NAME>_DB_PASSWORD in infra/.env,
# otherwise db-init fails and no application starts.
missing=()
for var in $(cat infra/db-init/*.sql 2>/dev/null | grep -oE ":'[a-z0-9_]+_db_password'" | tr -d ":'" | sort -u); do
  upper="${var^^}"
  grep -qE "^${upper}=.+" infra/.env || missing+=("$upper")
done
[[ ${#missing[@]} -eq 0 ]] || die "infra/.env is missing: ${missing[*]} (create with: openssl rand -hex 16)"
if grep -qE '^[A-Z_]+_DATABASE_URL=' infra/.env; then
  echo "deploy: WARNING: *_DATABASE_URL lines in infra/.env are ignored; compose builds the URLs" \
       "from *_DB_PASSWORD. Remove them to avoid confusion." >&2
fi
if [[ "${SKIP_DATA_DISK_CHECK:-0}" != "1" ]]; then
  mountpoint -q /mnt/data || die "/mnt/data is not mounted (did the LUKS unlock fail?)"
  root_dir="$(docker info --format '{{.DockerRootDir}}')"
  [[ "$root_dir" == "/mnt/data/docker" ]] || die "Docker data-root is $root_dir, expected /mnt/data/docker"
fi
[[ -z "$(git status --porcelain --untracked-files=no)" ]] \
  || die "uncommitted changes in $ROOT; commit or discard them before deploying"

# --- update code -----------------------------------------------------------------------
git fetch --prune origin
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"
echo "deploy: code at $(git rev-parse --short HEAD) on $BRANCH"

# --- build and start -------------------------------------------------------------------
# Apps wait for healthy Postgres, a successful db-init (roles/databases from infra/db-init/)
# and a successful kafka-init (topics). --wait fails if a service does not become healthy.
if ! "${COMPOSE[@]}" up -d --build --wait --remove-orphans "$@"; then
  echo "deploy: ERROR: startup failed. db-init / kafka-init output:" >&2
  "${COMPOSE[@]}" logs --no-log-prefix --tail=20 db-init kafka-init >&2 || true
  exit 1
fi
"${COMPOSE[@]}" ps
echo "deploy: done"
