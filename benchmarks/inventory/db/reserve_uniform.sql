-- Reserve 1 unit of a random part out of 40 (low contention). Unique key per call.
\set part random(1, 40)
SELECT result FROM seller_reserve(gen_random_uuid()::text, 'h', '{}', 'o', 'l',
       'BENCH-' || lpad(:part::text, 4, '0'), 1, NULL, now() + interval '2 day', gen_random_uuid());
