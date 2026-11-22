#!/usr/bin/env bash
# Logical backup of every PartStream PostgreSQL database. Owner: D.
#
#   scripts/backup.sh
#
# Writes <BACKUP_DIR>/<UTC timestamp>/:
#   globals.sql      roles and their password hashes (pg_dumpall --globals-only)
#   <db>.dump        one pg_dump custom-format file per database
#   SHA256SUMS       checksums, verified by restore.sh
#
#   BACKUP_DIR    default: /mnt/data/backups if /mnt/data is mounted (cloud VM),
#                 otherwise <repo>/backups (git-ignored)
#   KEEP_BACKUPS  if set (e.g. 14), delete all but the newest N backups afterwards
#
# Each database dump is internally consistent. Different databases are dumped one after
# another, NOT as one snapshot. A backup on the same VM is not off-site: copy it elsewhere
# (see docs/runbook.md).
set -euo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash on Windows: don't rewrite container paths

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
COMPOSE=(docker compose -f infra/docker-compose.yml)   # relative: works in Git Bash too

if [[ -z "${BACKUP_DIR:-}" ]]; then
  if mountpoint -q /mnt/data 2>/dev/null; then BACKUP_DIR=/mnt/data/backups
  else BACKUP_DIR="$ROOT/backups"; fi
fi
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="$BACKUP_DIR/$STAMP"
WORK="$DEST.partial"

umask 077   # dumps contain all data and role password hashes
mkdir -p "$WORK"
trap 'echo "backup: FAILED, leaving $WORK for inspection" >&2' ERR

pg() { "${COMPOSE[@]}" exec -T postgres "$@"; }

DATABASES="$(pg psql -U postgres -d postgres -Atc \
  "SELECT datname FROM pg_database WHERE NOT datistemplate AND datname <> 'postgres' ORDER BY 1")"
[[ -n "$DATABASES" ]] || { echo "backup: no application databases found" >&2; exit 1; }

pg pg_dumpall -U postgres --globals-only > "$WORK/globals.sql"
for db in $DATABASES; do
  pg pg_dump -U postgres --format=custom "$db" > "$WORK/$db.dump"
  pg pg_restore --list < "$WORK/$db.dump" > /dev/null   # readable archive?
  echo "backup: $db ($(wc -c < "$WORK/$db.dump") bytes)"
done
(cd "$WORK" && sha256sum globals.sql ./*.dump > SHA256SUMS)
mv "$WORK" "$DEST"
trap - ERR
echo "backup: OK -> $DEST"

if [[ -n "${KEEP_BACKUPS:-}" ]]; then
  [[ "$KEEP_BACKUPS" =~ ^[1-9][0-9]*$ ]] || { echo "backup: KEEP_BACKUPS must be >= 1" >&2; exit 1; }
  ls -1d "$BACKUP_DIR"/[0-9]*T[0-9]*Z 2>/dev/null | sort | head -n "-$KEEP_BACKUPS" | while read -r old; do
    rm -rf -- "$old" && echo "backup: pruned $old"
  done
fi
