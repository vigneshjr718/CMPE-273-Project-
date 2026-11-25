"""E2E: two (or many) requests competing for the last unit -> exactly one wins.

Runs against live sellers over HTTP (docker compose up, or the cloud deployment).

    SELLER_URLS=http://localhost:8101,http://localhost:8102 \
    SELLER_ADMIN_TOKEN=... \
    pytest tests/e2e/test_last_unit_race.py -v

Each test creates its own fresh part (PART-E2E-<uuid>) via the admin restock
endpoint, so it never disturbs seed data and can run repeatedly.
"""
from __future__ import annotations

import os
import threading
import uuid

import pytest

httpx = pytest.importorskip("httpx")

URLS = [u.strip().rstrip("/") for u in os.environ.get("SELLER_URLS", "").split(",") if u.strip()]
TOKEN = os.environ.get("SELLER_ADMIN_TOKEN", "")
ADMIN = {"X-Admin-Token": TOKEN}

pytestmark = pytest.mark.skipif(not URLS or not TOKEN, reason="set SELLER_URLS and SELLER_ADMIN_TOKEN")


def _new_part(base: str, qty: int) -> str:
    part = f"PART-E2E-{uuid.uuid4().hex[:10]}"
    r = httpx.post(f"{base}/admin/inventory", json={"part_id": part, "add_quantity": qty}, headers=ADMIN, timeout=10)
    r.raise_for_status()
    return part


def _stock(base: str, part: str) -> dict:
    r = httpx.get(f"{base}/admin/inventory", params={"part_id": part}, headers=ADMIN, timeout=10)
    r.raise_for_status()
    return r.json()["items"][0]


def _race(base: str, part: str, n: int, qty: int = 1) -> list[httpx.Response]:
    """Fire n reserves with distinct keys, released together by a barrier."""
    barrier = threading.Barrier(n)
    results: list[httpx.Response | None] = [None] * n

    def worker(i: int) -> None:
        with httpx.Client(timeout=30) as c:
            barrier.wait()
            results[i] = c.post(f"{base}/reservations",
                                json={"order_id": f"o-{i}", "line_id": "l1", "part_id": part, "quantity": qty},
                                headers={"Idempotency-Key": f"e2e-{part}-{i}"})

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results  # type: ignore[return-value]


@pytest.mark.parametrize("base", URLS)
def test_two_requests_for_last_unit(base):
    part = _new_part(base, 1)
    res = _race(base, part, 2)
    codes = sorted(r.status_code for r in res)
    assert codes == [201, 409], codes
    loser = next(r for r in res if r.status_code == 409).json()
    assert loser["status"] == "REJECTED" and loser["reason"] == "INSUFFICIENT_STOCK"
    s = _stock(base, part)
    assert (s["available_quantity"], s["reserved_quantity"]) == (0, 1)


@pytest.mark.parametrize("base", URLS)
def test_fifty_requests_for_three_units(base):
    part = _new_part(base, 3)
    res = _race(base, part, 50)
    assert sum(r.status_code == 201 for r in res) == 3
    assert sum(r.status_code == 409 for r in res) == 47
    s = _stock(base, part)
    assert (s["available_quantity"], s["reserved_quantity"]) == (0, 3)


@pytest.mark.parametrize("base", URLS)
def test_cancel_frees_the_unit_for_the_next_buyer(base):
    part = _new_part(base, 1)
    res = _race(base, part, 5)
    winner_idx = next(i for i, r in enumerate(res) if r.status_code == 201)
    key = f"e2e-{part}-{winner_idx}"
    receipts = set()
    for i in range(3):  # repeated cancel (even with different cancellation keys) -> one inventory effect
        r = httpx.post(f"{base}/reservation-operations/{key}/cancel", timeout=10,
                       headers={"Idempotency-Key": f"cancel-{key}-{i}"},
                       json={"order_id": f"o-{winner_idx}", "line_id": "l1", "part_id": part, "quantity": 1})
        assert r.status_code == 200 and r.json()["status"] == "CANCELLED"
        receipts.add(r.json()["cancellation_receipt"])
    assert len(receipts) == 1
    assert _stock(base, part)["available_quantity"] == 1
    res = _race(base, part, 5)
    assert sum(r.status_code == 201 for r in res) == 1
