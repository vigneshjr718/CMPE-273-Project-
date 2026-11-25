-- Realistic coordinator mix over 3,000 operation keys / 40 parts:
--   65% reserve (repeats of an existing key = idempotent replays)
--   10% reserve with a conflicting payload (409 path)
--   15% cancel/fence the operation (some keys never reserved -> tombstones)
--   10% delivery update (ETA push or mark delivered)
\set k random(1, 3000)
\set part :k % 40 + 1
\set qty :k % 3 + 1
\set op random(1, 20)
\if :op <= 13
SELECT result FROM seller_reserve('mx-' || :k, 'h-' || :qty, '{}', 'o', 'l', 'BENCH-' || lpad(:part::text, 4, '0'), :qty, NULL, now() + interval '2 day', gen_random_uuid());
\elif :op <= 15
SELECT result FROM seller_reserve('mx-' || :k, 'WRONG', '{}', 'o', 'l', 'BENCH-' || lpad(:part::text, 4, '0'), 9, NULL, now() + interval '2 day', gen_random_uuid());
\elif :op <= 18
SELECT status FROM seller_cancel_operation('mx-' || :k, 'c-' || :k, 'bench', 'o', 'l', 'BENCH-' || lpad(:part::text, 4, '0'), :qty, NULL);
\elif :op <= 19
SELECT d.http_status FROM reservations r, LATERAL seller_set_delivery(r.reservation_id, NULL, now() + interval '5 day') d WHERE r.operation_key = 'mx-' || :k;
\else
SELECT d.http_status FROM reservations r, LATERAL seller_set_delivery(r.reservation_id, 'DELIVERED', NULL) d WHERE r.operation_key = 'mx-' || :k;
\endif
