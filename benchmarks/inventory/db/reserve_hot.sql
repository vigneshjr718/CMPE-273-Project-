-- Every client hammers ONE part (worst-case row-lock contention). Unique key per call.
SELECT result FROM seller_reserve(gen_random_uuid()::text, 'h', '{}', 'o', 'l',
       'BENCH-0001', 1, NULL, now() + interval '2 day', gen_random_uuid());
