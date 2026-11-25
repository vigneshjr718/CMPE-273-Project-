#!/usr/bin/env bash
# DB-level inventory benchmark for the seller schema (no HTTP, no Python).
# Measures the cost and correctness of the stock-changing transactions themselves.
#
# Usage (needs a Postgres role that can CREATE DATABASE; uses standard PG* env vars):
#   PGHOST=localhost PGUSER=postgres PGPASSWORD=... ./run_pgbench.sh
#   CLIENTS="1 8 32 64" DURATION=10 REPS=3 ./run_pgbench.sh
#
# Writes benchmarks/results/inventory_pgbench_<timestamp>.json
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
MIG="$ROOT/services/seller/migrations"
OUT_DIR="$ROOT/benchmarks/results"
BENCH_DB="${BENCH_DB:-bench_seller}"
CLIENTS="${CLIENTS:-1 8 32 64}"
DURATION="${DURATION:-10}"
REPS="${REPS:-3}"
WORKLOADS="${WORKLOADS:-reserve_uniform reserve_hot mixed}"
PGBENCH="${PGBENCH:-$(command -v pgbench || echo /usr/lib/postgresql/16/bin/pgbench)}"
[[ "$BENCH_DB" == bench_* ]] || { echo "BENCH_DB must start with bench_ (it is dropped!)"; exit 1; }
mkdir -p "$OUT_DIR"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$OUT_DIR/inventory_pgbench_$STAMP.json"

reset_db() {
  psql -qX -d postgres -c "DROP DATABASE IF EXISTS $BENCH_DB" -c "CREATE DATABASE $BENCH_DB" >/dev/null
  psql -qX -d "$BENCH_DB" -v ON_ERROR_STOP=1 -f "$MIG/001_init.sql" -f "$MIG/002_functions.sql" >/dev/null
  # 40 parts with effectively unlimited stock, so throughput isn't cut short by sell-outs
  psql -qX -d "$BENCH_DB" -c "INSERT INTO inventory (part_id, description, unit_price_cents, total_quantity, available_quantity)
    SELECT 'BENCH-' || lpad(g::text, 4, '0'), 'bench', 100, 1000000000, 1000000000 FROM generate_series(1, 40) g" >/dev/null
}

hw_cpu="$(nproc 2>/dev/null || sysctl -n hw.ncpu)"
hw_mem="$(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo 2>/dev/null || echo unknown)"
pg_ver="$(psql -qXAt -d postgres -c 'show server_version')"
echo "{" > "$OUT"
echo "  \"benchmark\": \"inventory_pgbench\", \"timestamp\": \"$STAMP\"," >> "$OUT"
echo "  \"hardware\": {\"cpus\": $hw_cpu, \"memory\": \"$hw_mem\", \"host\": \"$(uname -srm)\", \"postgres\": \"$pg_ver\"}," >> "$OUT"
echo "  \"duration_s\": $DURATION, \"repetitions\": $REPS, \"runs\": [" >> "$OUT"
first=1
for w in $WORKLOADS; do
  for c in $CLIENTS; do
    for rep in $(seq 1 "$REPS"); do
      reset_db
      j=$(( c < 8 ? c : 8 ))
      log="$(mktemp)"
      "$PGBENCH" -n -d "$BENCH_DB" -c "$c" -j "$j" -T "$DURATION" -f "$HERE/$w.sql" -r >"$log" 2>&1 || { cat "$log"; exit 1; }
      tps=$(awk '/^tps/ {print $3; exit}' "$log")
      lat=$(awk '/latency average/ {print $4; exit}' "$log")
      std=$(awk '/latency stddev/ {print $4; exit}' "$log"); std=${std:-null}
      txns=$(awk '/number of transactions actually processed/ {print $NF; exit}' "$log")
      failed=$(awk '/number of failed transactions/ {print $5; exit}' "$log"); failed=${failed:-0}
      inv="$(psql -qXAt -F, -v ON_ERROR_STOP=1 -d "$BENCH_DB" -f "$HERE/invariants.sql")" \
        || { echo "invariant check failed to run"; exit 1; }
      [[ "$(wc -l <<<"$inv")" -ge 5 ]] || { echo "invariant check returned too few rows"; exit 1; }
      bad=$(awk -F, '{s+=$2} END {print s+0}' <<<"$inv")
      rm -f "$log"
      [[ $first == 1 ]] || echo "," >> "$OUT"; first=0
      printf '    {"workload": "%s", "clients": %s, "rep": %s, "transactions": %s, "tps": %s, "latency_avg_ms": %s, "latency_stddev_ms": %s, "failed": %s, "invariant_violations": %s}' \
        "$w" "$c" "$rep" "$txns" "$tps" "$lat" "$std" "$failed" "$bad" >> "$OUT"
      printf '%-16s clients=%-3s rep=%s  tps=%-10s avg=%-7sms failed=%s violations=%s\n' "$w" "$c" "$rep" "$tps" "$lat" "$failed" "$bad"
    done
  done
done
printf '\n  ]\n}\n' >> "$OUT"
psql -qX -d postgres -c "DROP DATABASE IF EXISTS $BENCH_DB" >/dev/null
echo "results -> $OUT"
