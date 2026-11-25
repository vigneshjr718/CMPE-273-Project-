# Seller service (Package C)

One codebase, five instances (`seller-1` … `seller-5`). Each instance owns one
Postgres database and differs only by env (`config/seller-N.env`).

| Instance | Port (in compose and on localhost) | DB / role | Delivery | Price × | Character |
|---|---|---|---|---|---|
| seller-1 | 8101 | seller1_db / seller1 | 24 h | 1.15 | fast, pricey |
| seller-2 | 8102 | seller2_db / seller2 | 72 h | 0.90 | budget |
| seller-3 | 8103 | seller3_db / seller3 | 48 h | 1.00 | regional |
| seller-4 | 8104 | seller4_db / seller4 | 120 h | 0.80 | bulk, slow |
| seller-5 | 8105 | seller5_db / seller5 | 216 h | 0.70 | cheapest, often too late |

Contract: [`contracts/openapi/seller-api.yaml`](../../contracts/openapi/seller-api.yaml).
Its wire shapes are exactly what B's coordinator adapters parse
(`services/coordinator/app/clients/`). `tests/b_contract.py` holds verbatim copies of
B's reply models, and every DB test validates the seller's replies with them.

## Run

```bash
# whole stack (D's compose; the seller blocks are in compose.sellers.yml)
cp infra/.env.example infra/.env        # once
docker compose -f infra/docker-compose.yml -f infra/docker-compose.dev.yml --profile apps \
  up -d --build seller-1 seller-2 seller-3 seller-4 seller-5
curl localhost:8101/health

# one seller outside Docker
cd services/seller
pip install -r requirements-dev.txt
cp .env.example .env            # set DB_PASSWORD (and ADMIN_TOKEN for faults)
uvicorn app.main:app --port 8101
```

Startup applies `migrations/*.sql` (once each, advisory-locked) and inserts seed
inventory **only if missing**, so a restart never resets stock.

```bash
python -m seed.generate_seed --seller-id seller-3 --print   # inspect seed data (no DB)
python -m seed.generate_seed --seller-id seller-3 --sql     # SQL form
```

Every seller carries 33 of the 40 catalog parts (`PART-0001`…`PART-0040`). Each
part is stocked by exactly 4 of the 5 sellers, so there is always a replacement
option. `PART-0999` has exactly 1 unit everywhere, for last-unit demos.

## Tests

```bash
pytest                                        # unit tests only (no DB)
TEST_DATABASE_URL=postgresql://postgres:test@localhost:5433/seller_test pytest   # + 32 DB/API tests
PGHOST=localhost PGPORT=5433 PGUSER=postgres PGPASSWORD=test tests/sql/run_db_race_tests.sh  # 21 race assertions
```
The DB/API tests cover:
- reserve, replay and conflicting-payload 409, and stored rejections;
- lookup-by-key, fencing, cancel receipts and `status_version`;
- shipped → `PENDING` and delivered → `DELIVERED` on cancel;
- injected errors, and lost replies on reserve and cancel;
- restart persistence;
- real-thread races: last unit, same key, parallel cancels, and a random mix.

`tests/sql/run_db_race_tests.sh` drives the DB functions with parallel `psql`
sessions. It covers in-flight reserve vs. fence in both orders, crash-before-commit,
and cancel vs. deliver.

E2E against running sellers: `tests/e2e/test_last_unit_race.py`,
`tests/e2e/test_failure_recovery_seller_side.py`. Their docstrings give the env vars.

## For the coordinator (B): the semantics in one place

| You do | Seller guarantees |
|---|---|
| Persist `operation_key`, then `POST /reservations` (`Idempotency-Key: <operation_key>`) | ≤ 1 stock effect per key, ever (DB-enforced, survives restart). `201 RESERVED` |
| Retry the same key + same body | `200` same `RESERVED` reply, or the same `409 REJECTED` |
| Not enough stock / unknown part | `409 {status: REJECTED, reason: INSUFFICIENT_STOCK \| PART_NOT_FOUND}`: definitive for that key; try another seller with a new key |
| Same key, different body | `409` error envelope `IDEMPOTENCY_KEY_REUSED`: a caller bug, not a REJECTED reply |
| Timeout / 5xx | Outcome UNKNOWN → `GET /reservations/by-key/{key}` |
| Lookup → `404 KEY_NOT_FOUND` | No record *yet*. Retry with the **same** key, or fence it |
| `POST /reservation-operations/{key}/cancel` (`Idempotency-Key: <cancellation_key>`, body order/line/part/quantity/reservation_id-or-null) | `200 CANCELLED`, `fenced: true`, durable `cancellation_receipt`: no delivery happened and none can, including a late or replayed reserve with that key (it gets `409 REJECTED reason OPERATION_CANCELLED`). Works for never-seen keys (tombstone). Stock restored exactly once; repeats return the same receipt and version |
| …but the reservation was shipped | `202 PENDING` (`fenced: false`, no receipt): retry later |
| …but it was delivered | `200 DELIVERED` with `delivered_at`: don't replace |
| …but part/quantity/reservation_id don't match the recorded operation | `409 OPERATION_MISMATCH`, nothing changed |
| Poll `GET /reservations/{id}/delivery` | `IN_TRANSIT` (+ETA) or `DELIVERED` (+`delivered_at`), `status_version` per operation. A cancelled reservation gives `409 RESERVATION_CANCELLED` (never delivery proof) |

`status_version` is a single counter per original operation key, shared by
delivery and cancellation replies. It is bumped in the same transaction as every
observable change (ETA moved, shipped, delivered, cancelled or fenced), stays the
same on identical re-reads, and never resets.
`needed_by` is part of the reserve fingerprint, so send the same value on retries.

## Fault injection (team only, `X-Admin-Token`)

```bash
T="X-Admin-Token: $SELLER_ADMIN_TOKEN"
# slow seller: every request +3 s
curl -XPOST localhost:8102/admin/faults -H "$T" -H 'content-type: application/json' -d '{"endpoint":"*","delay_ms":3000}'
# dead-ish seller: 100% 503 before any change
curl -XPOST localhost:8102/admin/faults -H "$T" -H 'content-type: application/json' -d '{"endpoint":"*","error_rate":1}'
# committed-but-reply-lost on the NEXT reserve only (commits, hangs 30 s, then 504)
curl -XPOST localhost:8101/admin/faults -H "$T" -H 'content-type: application/json' \
     -d '{"endpoint":"reserve","lost_reply_rate":1,"lost_reply_hang_ms":30000,"remaining":1}'
curl localhost:8101/admin/faults -H "$T"            # list
curl -XDELETE localhost:8101/admin/faults -H "$T"   # clear all
```
Endpoints: `*`, `quotes`, `reserve`, `cancel`, `lookup`, `delivery`. Rules live in
memory and are cleared by a restart (stopping the container is the "dead seller" test).

Demo delivery controls (each real change bumps `status_version`):
```bash
curl -XPOST localhost:8101/admin/reservations/$RID/delivery -H "$T" -H 'content-type: application/json' -d '{"delay_hours":240}'           # make it late
curl -XPOST localhost:8101/admin/reservations/$RID/delivery -H "$T" -H 'content-type: application/json' -d '{"delivery_status":"SHIPPED"}'  # cancel -> PENDING
curl -XPOST localhost:8101/admin/reservations/$RID/delivery -H "$T" -H 'content-type: application/json' -d '{"delivery_status":"DELIVERED"}'
curl localhost:8101/reservations/$RID                                                                     # full internal view (is_late, receipt, ...)
curl localhost:8101/admin/inventory -H "$T"                                                              # stock
curl -XPOST localhost:8101/admin/inventory -H "$T" -H 'content-type: application/json' -d '{"part_id":"PART-0999","add_quantity":1}'  # restock
```

## Layout

```
app/main.py                  app factory, startup (migrate + seed), request logging
app/api/                     health, quotes, reservations (+ by-key, + /reservation-operations cancel), delivery, admin_faults
app/services/inventory.py    quotes, reserve -> seller_reserve()
app/services/idempotency.py  key validation, request fingerprint, lookup-by-key
app/services/cancellation.py fencing -> seller_cancel_operation()
app/services/delivery.py     versioned delivery replies + demo updates -> seller_set_delivery()
app/faults.py                in-memory fault rules (pure, unit-tested)
migrations/001_init.sql      inventory, reservations, idempotency_records (+ conservation CHECK)
migrations/002_functions.sql all stock-changing logic as single-transaction DB functions
seed/generate_seed.py        reproducible inventory
tests/b_contract.py          verbatim copies of B's reply models (contract check)
tests/sql/run_db_race_tests.sh parallel-psql race tests for the DB functions
```
