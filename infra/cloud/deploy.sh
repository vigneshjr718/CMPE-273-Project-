#!/usr/bin/env bash
# Deploy or update PartStream on the cloud VM. Owner: D. Run on the VM from anywhere.
#
#   infra/cloud/deploy.sh                       # every app service that can be built (see below)
#   infra/cloud/deploy.sh tracker coordinator   # exactly these (plus their dependencies)
#   infra/cloud/deploy.sh --list                # only print what the default would deploy/skip
#
# Default (no service names): deploys every service in the "apps" profile whose build context
# has its Dockerfile, and skips the rest. Placeholders, whose code doesn't exist yet, can't be
# built. A service that depends on a skipped one is skipped too. Skipped services are printed.
# Explicit service names are passed to compose unchanged.
#
#   DEPLOY_BRANCH=<branch>   branch to deploy (default: the branch currently checked out)
#   SKIP_DATA_DISK_CHECK=1   skip the encrypted-disk checks (never on the real VM)
#
# Uses ONLY infra/docker-compose.yml. The dev override (which publishes Postgres/Kafka/API
# ports) is never loaded, so only the frontend/proxy port is exposed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f infra/docker-compose.yml --profile apps)
PYTHON="${PYTHON:-python3}"

die() { echo "deploy: ERROR: $*" >&2; exit 1; }

# Prints "DEPLOY <service>" / "SKIP <service> <reason>" for every buildable app service.
plan_default_services() {
  local config
  config="$("${COMPOSE[@]}" config --format json)" || die "docker compose config failed"
  COMPOSE_CONFIG="$config" "$PYTHON" - <<'PY'
import json, os

services = json.loads(os.environ["COMPOSE_CONFIG"])["services"]
skipped = {}

def missing_dockerfile(name):
    build = services[name].get("build")
    if not build:
        return None                       # image-only (postgres, kafka, ...): always available
    context = build.get("context", "")
    dockerfile = os.path.join(context, build.get("dockerfile") or "Dockerfile")
    if not os.path.isdir(context):
        return f"no code yet ({os.path.relpath(context)} does not exist)"
    if not os.path.isfile(dockerfile):
        return f"no Dockerfile in {os.path.relpath(context)}"
    return None

def deployable(name, path=()):
    if name in skipped:
        return False
    reason = missing_dockerfile(name)
    if reason:
        skipped[name] = reason
        return False
    for dep in services[name].get("depends_on") or {}:
        if dep not in path and not deployable(dep, path + (name,)):
            skipped[name] = f"depends on skipped service {dep}"
            return False
    return True

for name in sorted(services):
    if services[name].get("build") and deployable(name):
        print("DEPLOY", name)
for name in sorted(skipped):
    print("SKIP", name, skipped[name])
PY
}

# Turns the plan into the service list; prints what is skipped.
select_default_services() {
  local plan kind name reason
  plan="$(plan_default_services)" || die "could not work out which services can be built"
  SELECTED=()
  while read -r kind name reason; do
    case "$kind" in
      DEPLOY) SELECTED+=("$name") ;;
      SKIP)   echo "deploy: skipping $name: $reason" ;;
    esac
  done <<< "$plan"
}

if [[ "${1:-}" == "--list" ]]; then
  select_default_services
  echo "deploy: would deploy: ${SELECTED[*]:-(nothing)}"
  exit 0
fi

BRANCH="${DEPLOY_BRANCH:-$(git rev-parse --abbrev-ref HEAD)}"
[[ "$BRANCH" != "HEAD" ]] || die "detached HEAD; set DEPLOY_BRANCH"

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

# --- choose services (after the pull, so newly added service code counts) ---------------
if [[ $# -eq 0 ]]; then
  select_default_services
  [[ ${#SELECTED[@]} -gt 0 ]] || die "no app service can be built yet"
  set -- "${SELECTED[@]}"
fi
echo "deploy: services: $*"

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
