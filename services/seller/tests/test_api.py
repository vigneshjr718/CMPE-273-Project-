"""Seller behaviour against a real Postgres (TEST_DATABASE_URL). See conftest.py.

Every reply B's coordinator would parse is also validated with B's own models
(tests/b_contract.py), so a contract break fails here before integration.
"""
from __future__ import annotations

import random
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import pytest

from tests import b_contract as B
from tests.conftest import ADMIN

pytestmark = pytest.mark.db


def now():
    return datetime.now(timezone.utc)


# Fixed reference time: a retry must resend a byte-identical body (same needed_by),
# exactly like the coordinator does, or the seller rightly sees a different payload.
T0 = now().replace(microsecond=0)


def iso(hours: float) -> str:
    return (T0 + timedelta(hours=hours)).isoformat()


def body(part="PART-T1", qty=1, line="l1", order="o1", needed_by_hours=240):
    b = {"order_id": order, "line_id": line, "part_id": part, "quantity": qty}
    if needed_by_hours is not None:
        b["needed_by"] = iso(needed_by_hours)
    return b


def reserve(client, key, **kw):
    return client.post("/reservations", json=body(**kw), headers={"Idempotency-Key": key})


def cancel(client, key, *, part="PART-T1", qty=1, rid=None, ckey="cancel-1", line="l1"):
    return client.post(f"/reservation-operations/{key}/cancel", headers={"Idempotency-Key": ckey},
                       json={"order_id": "o1", "line_id": line, "part_id": part, "quantity": qty,
                             "reservation_id": rid})


def as_b(model, response):
    """Parse a reply exactly as B's coordinator would."""
    return model.model_validate(response.json())


# ---- basics -------------------------------------------------------------------------
def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["seller_id"] == "seller-1"


def test_quote_matches_b(client, stock):
    stock("PART-Q", 5)
    r = client.post("/quotes", json={"part_id": "PART-Q", "quantity": 2, "needed_by": iso(100)})
    assert r.status_code == 200
    q = as_b(B.Quote, r)
    assert (q.seller_id, q.part_id, q.available_quantity) == ("seller-1", "PART-Q", 5)
    eta_hours = (q.estimated_delivery_at - now()).total_seconds() / 3600
    assert 47 < eta_hours < 49                                     # DELIVERY_HOURS=48
    unknown = as_b(B.Quote, client.post("/quotes", json={"part_id": "NOPE", "quantity": 1, "needed_by": iso(5)}))
    assert unknown.available_quantity == 0
    assert stock("PART-Q")["available_quantity"] == 5              # quoting holds nothing


def test_quote_rejects_naive_datetime(client):
    r = client.post("/quotes", json={"part_id": "P", "quantity": 1, "needed_by": "2030-01-01T00:00:00"})
    assert r.status_code == 422 and r.json()["error"]["code"] == "VALIDATION_ERROR"


def test_missing_or_bad_key_is_400(client, stock):
    stock("PART-T1", 5)
    r = client.post("/reservations", json=body())
    assert r.status_code == 400 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REQUIRED"
    r = client.post("/reservations", json=body(), headers={"Idempotency-Key": "bad key"})
    assert r.status_code == 400
    assert stock("PART-T1")["available_quantity"] == 5


# ---- reserve / idempotency ------------------------------------------------------------
def test_reserve_then_replay(client, stock):
    stock("PART-T1", 5)
    r1, r2 = reserve(client, "k1", qty=2), reserve(client, "k1", qty=2)
    assert (r1.status_code, r2.status_code) == (201, 200)
    assert r2.headers["Idempotent-Replayed"] == "true"
    a, b = as_b(B.ReservationReply, r1), as_b(B.ReservationReply, r2)
    assert a.status == "RESERVED" and a.reservation_id == b.reservation_id
    assert (a.seller_id, a.operation_key, a.part_id, a.quantity) == ("seller-1", "k1", "PART-T1", 2)
    s = stock("PART-T1")
    assert (s["available_quantity"], s["reserved_quantity"]) == (3, 2)


def test_same_key_different_payload_is_409_error(client, stock):
    stock("PART-T1", 5)
    assert reserve(client, "k1", qty=2).status_code == 201
    r = reserve(client, "k1", qty=3)
    assert r.status_code == 409 and r.json()["error"]["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert stock("PART-T1")["available_quantity"] == 3


def test_insufficient_stock_is_rejected_and_definitive(client, stock):
    stock("PART-T1", 1)
    r = reserve(client, "k1", qty=2)
    rej = as_b(B.ReservationReply, r)
    assert r.status_code == 409 and rej.status == "REJECTED" and rej.reason == "INSUFFICIENT_STOCK"
    assert rej.reservation_id is None and rej.quantity == 2
    stock("PART-T1", 10)                                   # stock arrives later...
    r = reserve(client, "k1", qty=2)                        # ...same key still replays the rejection
    assert r.status_code == 409 and r.json()["reason"] == "INSUFFICIENT_STOCK"
    assert reserve(client, "k2", qty=2).status_code == 201  # a NEW attempt (new key) succeeds


def test_unknown_part_is_rejected_409(client):
    r = reserve(client, "k1", part="NOPE")
    assert r.status_code == 409 and as_b(B.ReservationReply, r).reason == "PART_NOT_FOUND"


# ---- lookup-by-key ------------------------------------------------------------------------
def test_lookup_by_key_lifecycle(client, stock):
    stock("PART-T1", 5)
    r = client.get("/reservations/by-key/k1")
    assert r.status_code == 404 and r.json()["error"]["code"] == "KEY_NOT_FOUND"

    rid = reserve(client, "k1").json()["reservation_id"]
    look = as_b(B.ReservationReply, client.get("/reservations/by-key/k1"))
    assert look.status == "RESERVED" and look.reservation_id == rid

    reserve(client, "k-rej", qty=999)
    rej = as_b(B.ReservationReply, client.get("/reservations/by-key/k-rej"))
    assert rej.status == "REJECTED" and rej.reason == "INSUFFICIENT_STOCK"

    cancel(client, "k1", rid=rid)
    r = client.get("/reservations/by-key/k1")              # reserved-then-cancelled: not a ReservationReply
    assert r.status_code == 409 and r.json()["error"]["code"] == "OPERATION_CANCELLED"


def test_key_named_like_a_route_segment(client, stock):
    stock("PART-T1", 5)
    for key in ("cancel", "delivery"):
        assert reserve(client, key).status_code == 201
        assert client.get(f"/reservations/by-key/{key}").json()["status"] == "RESERVED"


# ---- cancellation / fencing ------------------------------------------------------------------
def test_cancel_restores_once_with_stable_receipt(client, stock):
    stock("PART-T1", 5)
    rid = reserve(client, "k1", qty=3).json()["reservation_id"]
    r1 = cancel(client, "k1", qty=3, rid=rid, ckey="c-1")
    c1 = as_b(B.CancellationReply, r1)
    assert r1.status_code == 200 and c1.status == "CANCELLED" and c1.fenced and c1.cancellation_receipt
    assert (c1.operation_key, c1.cancellation_key, c1.reservation_id, c1.quantity) == ("k1", "c-1", rid, 3)
    c2 = as_b(B.CancellationReply, cancel(client, "k1", qty=3, rid=rid, ckey="c-1"))
    c3 = as_b(B.CancellationReply, cancel(client, "k1", qty=3, ckey="c-2"))   # another cancel key, no rid
    assert c2.cancellation_receipt == c1.cancellation_receipt == c3.cancellation_receipt
    assert c2.status_version == c1.status_version == c3.status_version      # identical observation
    assert c3.cancellation_key == "c-2"
    s = stock("PART-T1")
    assert (s["available_quantity"], s["reserved_quantity"]) == (5, 0)


def test_late_reserve_after_cancel_is_rejected(client, stock):
    stock("PART-T1", 5)
    reserve(client, "k1")
    cancel(client, "k1")
    r = reserve(client, "k1")
    rej = as_b(B.ReservationReply, r)
    assert r.status_code == 409 and rej.reason == "OPERATION_CANCELLED" and rej.reservation_id is None
    assert stock("PART-T1")["available_quantity"] == 5


def test_late_reserve_after_cancel_with_different_body_is_still_cancelled(client, stock):
    stock("PART-T1", 5)
    reserve(client, "k1", qty=1)
    cancel(client, "k1", qty=1)
    r = reserve(client, "k1", qty=2, needed_by_hours=99)      # different body, cancelled key
    assert r.status_code == 409 and as_b(B.ReservationReply, r).reason == "OPERATION_CANCELLED"
    assert stock("PART-T1")["available_quantity"] == 5


def test_fence_unseen_key_then_late_reserve(client, stock):
    stock("PART-T1", 5)
    r = cancel(client, "k-late", qty=2, ckey="c-late")
    f = as_b(B.CancellationReply, r)
    assert r.status_code == 200 and f.status == "CANCELLED" and f.fenced and f.reservation_id is None
    assert (f.part_id, f.quantity, f.status_version) == ("PART-T1", 2, 1)
    late = reserve(client, "k-late", qty=2)
    assert late.status_code == 409 and late.json()["reason"] == "OPERATION_CANCELLED"
    assert as_b(B.ReservationReply, client.get("/reservations/by-key/k-late")).status == "REJECTED"
    assert stock("PART-T1")["available_quantity"] == 5


def test_cancel_rejected_key_gets_receipt(client, stock):
    stock("PART-T1", 1)
    reserve(client, "k1", qty=5)                               # rejected
    c = as_b(B.CancellationReply, cancel(client, "k1", qty=5))
    assert c.status == "CANCELLED" and c.fenced and c.status_version == 2


def test_cancel_identity_mismatch_409(client, stock):
    stock("PART-T1", 5)
    rid = reserve(client, "k1", qty=2).json()["reservation_id"]
    assert cancel(client, "k1", qty=3).json()["error"]["code"] == "OPERATION_MISMATCH"
    assert cancel(client, "k1", qty=2, rid=str(uuid.uuid4())).status_code == 409
    assert cancel(client, "k1", qty=2, rid="not-a-uuid").status_code == 409
    assert stock("PART-T1")["reserved_quantity"] == 2          # nothing cancelled
    assert cancel(client, "k1", qty=2, rid=rid).status_code == 200


def test_cancel_requires_cancellation_key(client):
    r = client.post("/reservation-operations/k1/cancel",
                    json={"order_id": "o1", "line_id": "l1", "part_id": "PART-T1", "quantity": 1})
    assert r.status_code == 400


def test_shipped_cancel_is_pending_then_delivered(client, stock):
    stock("PART-T1", 5)
    rid = reserve(client, "k1", qty=2).json()["reservation_id"]
    client.post(f"/admin/reservations/{rid}/delivery", json={"delivery_status": "SHIPPED"}, headers=ADMIN)
    r = cancel(client, "k1", qty=2)
    p = as_b(B.CancellationReply, r)
    assert r.status_code == 202 and p.status == "PENDING" and not p.fenced and p.cancellation_receipt is None
    client.post(f"/admin/reservations/{rid}/delivery", json={"delivery_status": "DELIVERED"}, headers=ADMIN)
    r = cancel(client, "k1", qty=2)
    d = as_b(B.CancellationReply, r)
    assert r.status_code == 200 and d.status == "DELIVERED" and d.delivered_at and d.reservation_id == rid
    assert d.status_version > p.status_version


# ---- delivery -------------------------------------------------------------------------------
def test_delivery_versions_and_states(client, stock):
    stock("PART-T1", 5)
    rid = reserve(client, "k1", qty=2).json()["reservation_id"]
    d1 = as_b(B.DeliveryReply, client.get(f"/reservations/{rid}/delivery"))
    assert d1.status == "IN_TRANSIT" and d1.status_version == 1 and d1.delivered_at is None
    assert (d1.operation_key, d1.reservation_id, d1.part_id, d1.quantity) == ("k1", rid, "PART-T1", 2)
    again = as_b(B.DeliveryReply, client.get(f"/reservations/{rid}/delivery"))
    assert again == d1                                                     # stable on re-read

    client.post(f"/admin/reservations/{rid}/delivery", json={"delay_hours": 300}, headers=ADMIN)
    d2 = as_b(B.DeliveryReply, client.get(f"/reservations/{rid}/delivery"))
    assert d2.status_version == 2 and d2.estimated_delivery_at > d1.estimated_delivery_at
    view = client.get(f"/reservations/{rid}").json()
    assert view["is_late"] is True                                         # needed_by was +240h

    client.post(f"/admin/reservations/{rid}/delivery", json={"delivery_status": "SHIPPED"}, headers=ADMIN)
    d3 = as_b(B.DeliveryReply, client.get(f"/reservations/{rid}/delivery"))
    assert d3.status == "IN_TRANSIT" and d3.status_version == 3

    client.post(f"/admin/reservations/{rid}/delivery", json={"delivery_status": "DELIVERED"}, headers=ADMIN)
    d4 = as_b(B.DeliveryReply, client.get(f"/reservations/{rid}/delivery"))
    assert d4.status == "DELIVERED" and d4.delivered_at and d4.status_version == 4
    s = stock("PART-T1")
    assert (s["available_quantity"], s["reserved_quantity"], s["delivered_quantity"]) == (3, 0, 2)


def test_delivery_of_cancelled_reservation_is_409(client, stock):
    stock("PART-T1", 5)
    rid = reserve(client, "k1").json()["reservation_id"]
    cancel(client, "k1", rid=rid)
    r = client.get(f"/reservations/{rid}/delivery")
    assert r.status_code == 409 and r.json()["error"]["code"] == "RESERVATION_CANCELLED"
    r = client.post(f"/admin/reservations/{rid}/delivery", json={"delivery_status": "DELIVERED"}, headers=ADMIN)
    assert r.status_code == 409


def test_delivery_unknown_404(client):
    assert client.get(f"/reservations/{uuid.uuid4()}/delivery").status_code == 404
    assert client.get("/reservations/not-a-uuid/delivery").status_code == 404


# ---- faults / admin ---------------------------------------------------------------------
def test_admin_requires_token(client):
    assert client.get("/admin/faults").status_code == 403
    assert client.get("/admin/faults", headers={"X-Admin-Token": "wrong"}).status_code == 403
    assert client.get("/admin/faults", headers=ADMIN).status_code == 200


def test_injected_error_changes_nothing(client, stock):
    stock("PART-T1", 5)
    client.post("/admin/faults", json={"endpoint": "reserve", "error_rate": 1, "remaining": 1}, headers=ADMIN)
    r = reserve(client, "k1")
    assert r.status_code == 503 and r.json()["error"]["code"] == "SELLER_UNAVAILABLE"
    assert stock("PART-T1")["available_quantity"] == 5
    assert client.get("/reservations/by-key/k1").status_code == 404   # never processed -> safe to retry
    assert reserve(client, "k1").status_code == 201


def test_lost_reply_commits_and_is_reconcilable(client, stock):
    stock("PART-T1", 5)
    client.post("/admin/faults", json={"endpoint": "reserve", "lost_reply_rate": 1, "lost_reply_hang_ms": 10,
                                       "remaining": 1}, headers=ADMIN)
    r = reserve(client, "k1", qty=2)
    assert r.status_code == 504 and r.json()["error"]["code"] == "REPLY_LOST"
    assert stock("PART-T1")["available_quantity"] == 3                # it DID commit
    look = as_b(B.ReservationReply, client.get("/reservations/by-key/k1"))
    assert look.status == "RESERVED"
    r = reserve(client, "k1", qty=2)                                   # retry with the same key
    assert r.status_code == 200 and r.json()["reservation_id"] == look.reservation_id
    assert stock("PART-T1")["available_quantity"] == 3


def test_lost_reply_on_cancel_then_retry_gets_same_receipt(client, stock):
    stock("PART-T1", 5)
    reserve(client, "k1", qty=2)
    client.post("/admin/faults", json={"endpoint": "cancel", "lost_reply_rate": 1, "lost_reply_hang_ms": 10,
                                       "remaining": 1}, headers=ADMIN)
    assert cancel(client, "k1", qty=2, ckey="ck").status_code == 504
    again = as_b(B.CancellationReply, cancel(client, "k1", qty=2, ckey="ck"))
    assert again.status == "CANCELLED" and again.cancellation_receipt
    assert stock("PART-T1")["available_quantity"] == 5


def test_restock_and_inventory_admin(client):
    r = client.post("/admin/inventory", json={"part_id": "PART-NEW", "add_quantity": 4}, headers=ADMIN)
    assert r.status_code == 200 and r.json()["available_quantity"] == 4
    items = client.get("/admin/inventory", params={"part_id": "PART-NEW"}, headers=ADMIN).json()["items"]
    assert items[0]["total_quantity"] == 4


# ---- restart ------------------------------------------------------------------------------
def test_state_survives_restart(make_client, stock):
    stock("PART-T1", 5)
    first = make_client()
    rid = reserve(first, "k1", qty=2).json()["reservation_id"]
    receipt = cancel(first, "k-fenced", qty=1).json()["cancellation_receipt"]

    restarted = make_client()            # new app, new engine, new in-memory state
    r = reserve(restarted, "k1", qty=2)
    assert r.status_code == 200 and r.json()["reservation_id"] == rid
    assert reserve(restarted, "k1", qty=3).status_code == 409                       # payload check survives
    assert reserve(restarted, "k-fenced").json()["reason"] == "OPERATION_CANCELLED"  # tombstone survives
    assert cancel(restarted, "k-fenced", qty=1).json()["cancellation_receipt"] == receipt
    assert stock("PART-T1")["available_quantity"] == 3


# ---- concurrency (service layer, real threads, real DB) --------------------------------------
def _reserve_direct(settings, engine, key, part, qty=1):
    from app.services import inventory
    return inventory.reserve(engine, settings, key, {"order_id": "o", "line_id": "l", "part_id": part,
                                                      "quantity": qty, "needed_by": None})


def _cancel_direct(settings, engine, key, part, qty, ckey):
    from app.services import cancellation
    return cancellation.cancel_operation(engine, settings, key, ckey, {
        "order_id": "o", "line_id": "l", "part_id": part, "quantity": qty, "reservation_id": None})


def test_last_unit_race_only_one_wins(settings, db, stock):
    stock("PART-LAST", 1)
    with ThreadPoolExecutor(max_workers=25) as pool:
        outs = list(pool.map(lambda i: _reserve_direct(settings, db, f"race-{i}", "PART-LAST"), range(25)))
    results = [o.result for o in outs]
    assert results.count("RESERVED") == 1 and results.count("REJECTED") == 24
    s = stock("PART-LAST")
    assert (s["available_quantity"], s["reserved_quantity"]) == (0, 1)


def test_same_key_concurrent_requests_one_effect(settings, db, stock):
    stock("PART-T1", 100)
    with ThreadPoolExecutor(max_workers=20) as pool:
        outs = list(pool.map(lambda _: _reserve_direct(settings, db, "same", "PART-T1", 5), range(20)))
    assert [o.result for o in outs].count("RESERVED") == 1
    assert len({o.row["reservation_id"] for o in outs}) == 1
    assert stock("PART-T1")["available_quantity"] == 95


def test_concurrent_cancels_restore_once(settings, db, stock):
    stock("PART-T1", 10)
    _reserve_direct(settings, db, "k1", "PART-T1", 4)
    with ThreadPoolExecutor(max_workers=20) as pool:
        outs = list(pool.map(lambda i: _cancel_direct(settings, db, "k1", "PART-T1", 4, f"c{i}"), range(20)))
    assert sum(o.committed_change for o in outs) == 1
    assert len({o.reply["cancellation_receipt"] for o in outs}) == 1
    assert stock("PART-T1")["available_quantity"] == 10


def test_random_mix_preserves_conservation(settings, db, stock):
    from sqlalchemy import text
    for p in ("PART-A", "PART-B", "PART-C"):
        stock(p, 20)
    rng = random.Random(7)
    ops = [(rng.choice(["reserve", "reserve", "reserve", "cancel", "replay"]), rng.randint(1, 60)) for _ in range(400)]

    def run(op):
        kind, k = op
        key, part, qty = f"mix-{k}", f"PART-{'ABC'[k % 3]}", k % 3 + 1
        if kind == "cancel":
            return _cancel_direct(settings, db, key, part, qty, f"c-{k}")
        return _reserve_direct(settings, db, key, part, qty)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(run, ops))

    with db.connect() as conn:
        bad = conn.execute(text(
            "SELECT count(*) FROM inventory i WHERE i.reserved_quantity <> "
            "(SELECT coalesce(sum(quantity),0) FROM reservations r WHERE r.part_id=i.part_id AND r.status='RESERVED')"
            " OR i.available_quantity < 0")).scalar()
        fenced_with_stock = conn.execute(text(
            "SELECT count(*) FROM idempotency_records ir JOIN reservations r USING (operation_key)"
            " WHERE ir.outcome='FENCED'")).scalar()
        no_receipt = conn.execute(text(
            "SELECT count(*) FROM reservations r JOIN idempotency_records ir USING (operation_key)"
            " WHERE r.status='CANCELLED' AND ir.cancellation_receipt IS NULL")).scalar()
    assert (bad, fenced_with_stock, no_receipt) == (0, 0, 0)
