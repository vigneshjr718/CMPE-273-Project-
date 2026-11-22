#!/usr/bin/env bash
# Restore PartStream PostgreSQL databases from a scripts/backup.sh directory. Owner: D.
#
#   scripts/restore.sh --yes <backup-dir>                 # every database in the backup
#   scripts/restore.sh --yes <backup-dir> tracker_db      # only the named databases
#   scripts/restore.sh --yes --replay-tracker <dir> tracker_db
#
# DESTRUCTIVE: each restored database is DROPPED and recreated from the dump. Roles from
# globals.sql are recreated/updated (existing roles keep working; passwords are set to the
# backed-up values).
#
# Stop the application services first; the script refuses while any of them is running:
#   docker compose -f infra/docker-compose.yml --profile apps stop
#
# --replay-tracker  after restoring tracker_db, rewind the tracker's Kafka consumer group to
#                   the earliest offset. The restored DB is older than the committed offsets,
#                   so without this the tracker would never see the events in between.
#                   Replay is safe: already-applied events are deduplicated by event_id.
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: don't rewrite container paths

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f infra/docker-compose.yml)   # relative to ROOT (cd below)
INFRA_SERVICES="postgres kafka kafka-init"
TRACKER_GROUP="${TRACKER_CONSUMER_GROUP:-tracker-v1}"

die() { echo "restore: ERROR: $*" >&2; exit 1; }
usage() { sed -n '2,8p' "$0" >&2; exit 2; }

YES=0; REPLAY_TRACKER=0
while [[ $# -gt 0 && "$1" == --* ]]; do
  case "$1" in
    --yes) YES=1 ;;
    --replay-tracker) REPLAY_TRACKER=1 ;;
    *) usage ;;
  esac
  shift
done
[[ $# -ge 1 ]] || usage
[[ -d "$1" ]] || die "$1 is not a directory"
SRC="$(cd "$1" && pwd)"; shift
cd "$ROOT"
[[ -f "$SRC/SHA256SUMS" && -f "$SRC/globals.sql" ]] || die "$SRC is not a backup directory"
[[ "$YES" == 1 ]] || die "this drops and recreates databases; re-run with --yes"

if [[ $# -gt 0 ]]; then DATABASES="$*"
else DATABASES="$(cd "$SRC" && ls -1 ./*.dump | sed 's|^\./||; s|\.dump$||')"; fi
for db in $DATABASES; do
  [[ "$db" =~ ^[a-z0-9_]+$ ]] || die "invalid database name: $db"
  [[ -f "$SRC/$db.dump" ]] || die "no $db.dump in $SRC"
done

(cd "$SRC" && sha256sum --quiet -c SHA256SUMS) || die "checksum mismatch in $SRC"

running="$("${COMPOSE[@]}" --profile apps ps --status running --services)"
for svc in $running; do
  [[ " $INFRA_SERVICES " == *" $svc "* ]] || die "application service '$svc' is running; stop the apps first"
done

pg() { "${COMPOSE[@]}" exec -T postgres "$@"; }

echo "restore: roles from globals.sql"
# Existing roles report "already exists"; that is expected and not fatal.
pg psql -U postgres -d postgres -q -v ON_ERROR_STOP=0 < "$SRC/globals.sql" > /dev/null 2>&1 || true

for db in $DATABASES; do
  echo "restore: $db"
  pg psql -U postgres -d postgres -q -v ON_ERROR_STOP=1 -c \
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$db' AND pid <> pg_backend_pid();" > /dev/null
  pg pg_restore -U postgres -d postgres --clean --if-exists --create --exit-on-error < "$SRC/$db.dump"
done

if [[ " $DATABASES " == *" tracker_db "* ]]; then
  if [[ "$REPLAY_TRACKER" == 1 ]]; then
    echo "restore: rewinding Kafka consumer group $TRACKER_GROUP to earliest"
    "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
      --bootstrap-server localhost:9092 --group "$TRACKER_GROUP" \
      --reset-offsets --to-earliest --all-topics --execute
  else
    echo "restore: NOTE tracker_db is now older than the tracker's Kafka offsets."
    echo "         Re-run with --replay-tracker (or see docs/runbook.md) before starting the tracker."
  fi
fi
echo "restore: OK. Start the apps again and have each owner verify their data."
