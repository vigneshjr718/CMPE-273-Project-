#!/usr/bin/env bash
# DB-level race tests for the seller stock functions (no Python needed).
# Uses parallel psql sessions against a THROWAWAY database (dropped/recreated).
#
#   PGHOST=localhost PGPORT=5433 PGUSER=postgres PGPASSWORD=test \
#     services/seller/tests/sql/run_db_race_tests.sh
#
# Exit code 0 = all assertions passed.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
MIG="$HERE/../../migrations"
DB="${RACE_DB:-race_seller_test}"
[[ "$DB" == *test* ]] || { echo "RACE_DB must contain 'test' (it is dropped)"; exit 2; }
set +m
FAILS=0
q()  { psql -X -qAt -d "$DB" -c "$1"; }
ok() { if [[ "$2" == "$3" ]]; then echo "  PASS  $1"; else echo "  FAIL  $1: expected [$3] got [$2]"; FAILS=$((FAILS+1)); fi; }
ETA="now() + interval '2 day'"

psql -X -q -d postgres -c "DROP DATABASE IF EXISTS $DB" -c "CREATE DATABASE $DB" >/dev/null 2>&1
psql -X -q -d "$DB" -v ON_ERROR_STOP=1 -f "$MIG/001_init.sql" -f "$MIG/002_functions.sql" >/dev/null 2>&1 \
  || { echo "migration failed"; exit 1; }
q "INSERT INTO inventory (part_id, description, unit_price_cents, total_quantity, available_quantity)
   VALUES ('LAST', 'last unit', 100, 1, 1), ('BULK', 'bulk', 100, 100000, 100000)"
TMP="$(mktemp -d)"

echo "1. 50 parallel reserves, distinct keys, 1 unit"
for i in $(seq 1 50); do
  q "SELECT result FROM seller_reserve('race-$i','h','{}','o','l','LAST',1,NULL,$ETA,gen_random_uuid())" > "$TMP/a.$i" &
done; wait
ok "exactly one RESERVED" "$(cat "$TMP"/a.* | grep -c '^RESERVED$')" 1
ok "49 REJECTED"          "$(cat "$TMP"/a.* | grep -c '^REJECTED$')" 49
ok "stock 0 available / 1 reserved" "$(q "SELECT available_quantity||'/'||reserved_quantity FROM inventory WHERE part_id='LAST'")" "0/1"

echo "2. 30 parallel reserves, SAME key"
for i in $(seq 1 30); do
  q "SELECT result||':'||reservation_id FROM seller_reserve('same','h','{}','o','l','BULK',5,NULL,$ETA,gen_random_uuid())" > "$TMP/b.$i" &
done; wait
ok "one RESERVED, 29 REPLAYED" "$(cut -d: -f1 "$TMP"/b.* | sort | uniq -c | awk '{print $2"="$1}' | paste -sd,)" "REPLAYED=29,RESERVED=1"
ok "one reservation id"        "$(cut -d: -f2 "$TMP"/b.* | sort -u | wc -l | tr -d ' ')" 1
ok "stock taken once"          "$(q "SELECT available_quantity FROM inventory WHERE part_id='BULK'")" 99995

echo "3. 30 parallel cancels of that key (different cancellation keys)"
for i in $(seq 1 30); do
  q "SELECT stock_restored||':'||receipt FROM seller_cancel_operation('same','cancel-$i','x','o','l','BULK',5,NULL)" > "$TMP/c.$i" &
done; wait
ok "stock restored exactly once" "$(cut -d: -f1 "$TMP"/c.* | paste -sd+ | bc)" 5
ok "one receipt for everyone"    "$(cut -d: -f2 "$TMP"/c.* | sort -u | wc -l | tr -d ' ')" 1
ok "stock back"                  "$(q "SELECT available_quantity FROM inventory WHERE part_id='BULK'")" 100000

echo "4. reserve in flight (commits after 2s) vs fence arriving at 0.5s"
( psql -X -qAt -d "$DB" -c "BEGIN" -c "SELECT result FROM seller_reserve('inflight-1','h','{}','o','l','BULK',7,NULL,$ETA,gen_random_uuid())" \
       -c "SELECT pg_sleep(2)" -c "COMMIT" | grep -v '^$' > "$TMP/d.a" ) &
sleep 0.5
ok "fence waits, then cancels the reservation" "$(q "SELECT status||':'||stock_restored FROM seller_cancel_operation('inflight-1','ck','x','o','l','BULK',7,NULL)")" "CANCELLED:7"
wait
ok "reserve itself had succeeded" "$(grep -c RESERVED "$TMP/d.a")" 1
ok "net stock unchanged" "$(q "SELECT available_quantity FROM inventory WHERE part_id='BULK'")" 100000

echo "5. fence in flight (commits after 2s) vs late reserve arriving at 0.5s"
( psql -X -qAt -d "$DB" -c "BEGIN" -c "SELECT status FROM seller_cancel_operation('inflight-2','ck','x','o','l','BULK',7,NULL)" \
       -c "SELECT pg_sleep(2)" -c "COMMIT" >/dev/null ) &
sleep 0.5
ok "late reserve rejected" "$(q "SELECT result FROM seller_reserve('inflight-2','h','{}','o','l','BULK',7,NULL,$ETA,gen_random_uuid())")" "OPERATION_CANCELLED"
wait
ok "stock untouched" "$(q "SELECT available_quantity FROM inventory WHERE part_id='BULK'")" 100000

echo "6. reserve in flight then ROLLBACK (crash); retry with same key at 0.5s"
( psql -X -qAt -d "$DB" -c "BEGIN" -c "SELECT result FROM seller_reserve('inflight-3','h','{}','o','l','BULK',2,NULL,$ETA,gen_random_uuid())" \
       -c "SELECT pg_sleep(2)" -c "ROLLBACK" >/dev/null ) &
sleep 0.5
ok "retry reserves after rollback" "$(q "SELECT result FROM seller_reserve('inflight-3','h','{}','o','l','BULK',2,NULL,$ETA,gen_random_uuid())")" "RESERVED"
wait
ok "exactly one effect" "$(q "SELECT available_quantity FROM inventory WHERE part_id='BULK'")" 99998

echo "7. cancel vs deliver racing on the same reservation, 20 rounds (deadlock check)"
for i in $(seq 1 20); do
  q "SELECT 1 FROM seller_reserve('cd-$i','h','{}','o','l','BULK',1,NULL,$ETA,gen_random_uuid())" >/dev/null
  rid="$(q "SELECT reservation_id FROM reservations WHERE operation_key='cd-$i'")"
  q "SELECT http_status FROM seller_set_delivery('$rid','DELIVERED',NULL)" > "$TMP/e.d.$i" 2>&1 &
  q "SELECT status FROM seller_cancel_operation('cd-$i','c','x','o','l','BULK',1,NULL)" > "$TMP/e.c.$i" 2>&1 &
  wait
done
ok "no errors/deadlocks" "$(cat "$TMP"/e.* | grep -ci 'error\|deadlock')" 0
ok "every pair has exactly one winner" "$(q "SELECT count(*) FROM reservations WHERE operation_key LIKE 'cd-%' AND status IN ('CANCELLED','DELIVERED')")" 20

echo "8. invariants after everything"
BAD="$(q "SELECT count(*) FROM inventory i
  WHERE i.reserved_quantity  <> (SELECT coalesce(sum(quantity),0) FROM reservations r WHERE r.part_id=i.part_id AND r.status='RESERVED')
     OR i.delivered_quantity <> (SELECT coalesce(sum(quantity),0) FROM reservations r WHERE r.part_id=i.part_id AND r.status='DELIVERED')
     OR i.available_quantity < 0")"
ok "inventory matches reservations" "$BAD" 0
ok "no fenced key holds stock" "$(q "SELECT count(*) FROM idempotency_records ir JOIN reservations r USING (operation_key) WHERE ir.outcome='FENCED'")" 0
ok "every cancelled reservation has a receipt" "$(q "SELECT count(*) FROM reservations r JOIN idempotency_records ir USING (operation_key) WHERE r.status='CANCELLED' AND ir.cancellation_receipt IS NULL")" 0

rm -rf "$TMP"
psql -X -q -d postgres -c "DROP DATABASE IF EXISTS $DB" >/dev/null 2>&1
echo
if [[ $FAILS -eq 0 ]]; then echo "ALL PASSED"; else echo "$FAILS FAILED"; exit 1; fi
