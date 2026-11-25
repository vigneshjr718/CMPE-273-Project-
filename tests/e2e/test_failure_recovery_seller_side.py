"""E2E, seller half of tests/e2e/test_failure_recovery.py (shared with B).

These drive a live seller the way the coordinator must: through timeouts,
lost replies and restarts. Merge into B's test_failure_recovery.py, or keep as a
separate file — they don't depend on the coordinator.

    SELLER_URLS=http://localhost:8101 SELLER_ADMIN_TOKEN=... pytest tests/e2e/test_failure_recovery_seller_side.py -v

Optional restart test: also set SELLER_RESTART_CMD, e.g.
    SELLER_RESTART_CMD="docker compose -f infra/docker-compose.yml restart seller-1"
"""
from __future__ import annotations

import os
import subprocess
import time
import uuid

import pytest

httpx = pytest.importorskip("httpx")

URLS = [u.strip().rstrip("/") for u in os.environ.get("SELLER_URLS", "").split(",") if u.strip()]
BASE = URLS[0] if URLS else ""
TOKEN = os.environ.get("SELLER_ADMIN_TOKEN", "")
ADMIN = {"X-Admin-Token": TOKEN}
RESTART_CMD = os.environ.get("SELLER_RESTART_CMD")

pytestmark = pytest.mark.skipif(not BASE or not TOKEN, reason="set SELLER_URLS and SELLER_ADMIN_TOKEN")


@pytest.fixture(autouse=True)
def _clear_faults():
    httpx.delete(f"{BASE}/admin/faults", headers=ADMIN, timeout=10)
    yield
    httpx.delete(f"{BASE}/admin/faults", headers=ADMIN, timeout=10)


def _part(qty: int) -> str:
    part = f"PART-E2E-{uuid.uuid4().hex[:10]}"
    httpx.post(f"{BASE}/admin/inventory", json={"part_id": part, "add_quantity": qty}, headers=ADMIN,
               timeout=10).raise_for_status()
    return part


def _available(part: str) -> int:
    r = httpx.get(f"{BASE}/admin/inventory", params={"part_id": part}, headers=ADMIN, timeout=10)
    return r.json()["items"][0]["available_quantity"]


def _reserve(key: str, part: str, qty: int = 1, timeout: float = 10, needed_by: str | None = None):
    body = {"order_id": "o-e2e", "line_id": "l1", "part_id": part, "quantity": qty}
    if needed_by:
        body["needed_by"] = needed_by
    return httpx.post(f"{BASE}/reservations", timeout=timeout, headers={"Idempotency-Key": key}, json=body)


def _cancel(key: str, part: str, qty: int = 1, ckey: str | None = None):
    return httpx.post(f"{BASE}/reservation-operations/{key}/cancel", timeout=10,
                      headers={"Idempotency-Key": ckey or f"cancel-{key}"},
                      json={"order_id": "o-e2e", "line_id": "l1", "part_id": part, "quantity": qty})


def test_timeout_then_reconcile_then_retry_no_duplicate():
    """Committed-but-reply-lost: client times out (UNKNOWN), lookup shows RESERVED, retry replays."""
    part, key = _part(5), f"op-{uuid.uuid4().hex}"
    httpx.post(f"{BASE}/admin/faults", headers=ADMIN, timeout=10, json={
        "endpoint": "reserve", "lost_reply_rate": 1, "lost_reply_hang_ms": 5000, "remaining": 1})
    with pytest.raises(httpx.TimeoutException):
        _reserve(key, part, 2, timeout=1.0)                       # caller gives up: outcome UNKNOWN

    look = httpx.get(f"{BASE}/reservations/by-key/{key}", timeout=10).json()
    assert look["status"] == "RESERVED"
    r = _reserve(key, part, 2)
    assert r.status_code == 200 and r.json()["reservation_id"] == look["reservation_id"]
    assert _available(part) == 3                                   # exactly one effect


def test_timeout_then_fence_then_late_reserve_rejected():
    """Coordinator gives up on a key: fence it. The delayed original must not take stock."""
    part, key = _part(5), f"op-{uuid.uuid4().hex}"
    fence = _cancel(key, part).json()
    assert fence["status"] == "CANCELLED" and fence["fenced"] is True and fence["reservation_id"] is None
    r = _reserve(key, part)
    assert r.status_code == 409 and r.json()["reason"] == "OPERATION_CANCELLED"
    assert _available(part) == 5


def test_error_before_commit_is_safe_to_retry():
    part, key = _part(5), f"op-{uuid.uuid4().hex}"
    httpx.post(f"{BASE}/admin/faults", headers=ADMIN, timeout=10,
               json={"endpoint": "reserve", "error_rate": 1, "remaining": 1})
    assert _reserve(key, part).status_code == 503
    assert httpx.get(f"{BASE}/reservations/by-key/{key}", timeout=10).status_code == 404
    assert _reserve(key, part).status_code == 201
    assert _available(part) == 4


def test_slow_seller_delay():
    httpx.post(f"{BASE}/admin/faults", headers=ADMIN, timeout=10,
               json={"endpoint": "quotes", "delay_ms": 1500, "remaining": 1})
    t = time.perf_counter()
    with pytest.raises(httpx.TimeoutException):
        httpx.post(f"{BASE}/quotes", timeout=0.5,
                   json={"part_id": "PART-0001", "quantity": 1, "needed_by": "2099-01-01T00:00:00Z"})
    assert time.perf_counter() - t < 1.4


def test_late_delivery_then_cancel_confirmed():
    import datetime as dt
    part, key = _part(5), f"op-{uuid.uuid4().hex}"
    needed_by = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=20)).isoformat()
    rid = _reserve(key, part, needed_by=needed_by).json()["reservation_id"]
    d1 = httpx.get(f"{BASE}/reservations/{rid}/delivery", timeout=10).json()
    assert d1["status"] == "IN_TRANSIT"
    httpx.post(f"{BASE}/admin/reservations/{rid}/delivery", json={"delay_hours": 30 * 24}, headers=ADMIN, timeout=10)
    d2 = httpx.get(f"{BASE}/reservations/{rid}/delivery", timeout=10).json()
    assert d2["status_version"] > d1["status_version"] and d2["estimated_delivery_at"] > needed_by
    c = _cancel(key, part).json()
    assert c["status"] == "CANCELLED" and c["fenced"] is True and c["cancellation_receipt"]
    assert _available(part) == 5


@pytest.mark.skipif(not RESTART_CMD, reason="set SELLER_RESTART_CMD to test restart")
def test_state_survives_seller_restart():
    part, key, fenced = _part(5), f"op-{uuid.uuid4().hex}", f"op-{uuid.uuid4().hex}"
    rid = _reserve(key, part, 2).json()["reservation_id"]
    receipt = _cancel(fenced, part).json()["cancellation_receipt"]

    subprocess.run(RESTART_CMD, shell=True, check=True)
    for _ in range(60):
        try:
            if httpx.get(f"{BASE}/health", timeout=2).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(1)

    r = _reserve(key, part, 2)
    assert r.status_code == 200 and r.json()["reservation_id"] == rid
    assert _reserve(key, part, 3).status_code == 409
    assert _reserve(fenced, part).json()["reason"] == "OPERATION_CANCELLED"
    assert _cancel(fenced, part).json()["cancellation_receipt"] == receipt
    assert _available(part) == 3
