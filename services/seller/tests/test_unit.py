"""Pure unit tests — no database needed."""
from __future__ import annotations

import random
from datetime import datetime, timezone

import pytest

from app.faults import FaultRegistry, FaultRule
from app.services.idempotency import is_valid_key, lookup_kind, request_hash
from seed.generate_seed import DEMO_PART, SELLERS, seller_rows, to_sql


# ---- operation keys / fingerprints -------------------------------------------------
@pytest.mark.parametrize("key", ["a", "ord-1:line-2:attempt-3", "x" * 200, "A.B_c-9:z"])
def test_valid_keys(key):
    assert is_valid_key(key)


@pytest.mark.parametrize("key", [None, "", "x" * 201, "has space", "semi;colon", "slash/x", "ünï"])
def test_invalid_keys(key):
    assert not is_valid_key(key)


def test_request_hash_ignores_key_order_but_not_values():
    a = {"order_id": "o1", "line_id": "l1", "part_id": "PART-0001", "quantity": 2, "needed_by": "2026-12-01"}
    b = dict(reversed(list(a.items())))
    assert request_hash(a) == request_hash(b)
    assert request_hash(a) != request_hash({**a, "quantity": 3})
    assert request_hash(a) != request_hash({**a, "needed_by": None})


@pytest.mark.parametrize("record,status,expected", [
    ("RESERVED", "RESERVED", "RESERVED"),
    ("RESERVED", "DELIVERED", "RESERVED"),     # a delivered key still "holds" its reservation
    ("RESERVED", "CANCELLED", "CANCELLED"),
    ("REJECTED", None, "REJECTED"),
    ("FENCED", None, "REJECTED"),              # fenced before any reserve: no stock ever taken
])
def test_lookup_kind(record, status, expected):
    assert lookup_kind(record, status) == expected


# ---- faults ----------------------------------------------------------------------------
def test_fault_rule_validation():
    with pytest.raises(ValueError):
        FaultRule(endpoint="nope")
    with pytest.raises(ValueError):
        FaultRule(endpoint="reserve", error_rate=1.5)
    with pytest.raises(ValueError):
        FaultRule(endpoint="reserve", remaining=0)


def test_fault_matching_and_remaining():
    reg = FaultRegistry(random.Random(1))
    reg.add(FaultRule(endpoint="reserve", error_rate=1.0, remaining=2))
    assert not reg.decide("quotes").any
    assert reg.decide("reserve").error
    assert reg.decide("reserve").error
    assert not reg.decide("reserve").any          # consumed
    assert reg.list() == []


def test_wildcard_delay_applies_everywhere():
    reg = FaultRegistry(random.Random(1))
    reg.add(FaultRule(endpoint="*", delay_ms=250))
    for ep in ("quotes", "reserve", "cancel", "lookup", "delivery"):
        assert reg.decide(ep).delay_ms == 250


def test_error_and_lost_reply_are_exclusive():
    reg = FaultRegistry(random.Random(1))
    reg.add(FaultRule(endpoint="reserve", error_rate=1.0))
    reg.add(FaultRule(endpoint="reserve", lost_reply_rate=1.0, lost_reply_hang_ms=5))
    d = reg.decide("reserve")
    assert d.error and not d.lost_reply


def test_lost_reply_rate_is_roughly_respected():
    reg = FaultRegistry(random.Random(42))
    reg.add(FaultRule(endpoint="reserve", lost_reply_rate=0.3, lost_reply_hang_ms=1))
    hits = sum(reg.decide("reserve").lost_reply for _ in range(2000))
    assert 450 < hits < 750


def test_clear():
    reg = FaultRegistry()
    reg.add(FaultRule(endpoint="*", delay_ms=1))
    reg.clear()
    assert not reg.decide("reserve").any


# ---- seed ------------------------------------------------------------------------------
def test_seed_is_reproducible_and_sellers_differ():
    assert seller_rows(7, 2, 1.0) == seller_rows(7, 2, 1.0)
    assert seller_rows(7, 2, 1.0) != seller_rows(7, 3, 1.0)
    assert seller_rows(7, 2, 1.0) != seller_rows(8, 2, 1.0)


def test_every_catalog_part_has_alternative_sellers():
    carriers: dict[str, int] = {}
    for n in range(1, SELLERS + 1):
        for r in seller_rows(20261124, n, 1.0):
            carriers[r.part_id] = carriers.get(r.part_id, 0) + 1
    assert all(c >= 4 for c in carriers.values())
    assert carriers[DEMO_PART] == SELLERS


def test_demo_part_has_exactly_one_unit_everywhere():
    for n in range(1, SELLERS + 1):
        demo = [r for r in seller_rows(1, n, 1.0) if r.part_id == DEMO_PART]
        assert len(demo) == 1 and demo[0].quantity == 1


def test_seed_sql_is_insert_if_missing():
    sql = to_sql(seller_rows(1, 1, 1.0))
    assert "ON CONFLICT (part_id) DO NOTHING" in sql
    assert sql.count("PART-") == len(seller_rows(1, 1, 1.0))


# ---- lateness (needs sqlalchemy installed, not a DB) -------------------------------------
def test_is_late():
    inv = pytest.importorskip("app.services.inventory")
    def d(day):
        return datetime(2026, 12, day, 12, tzinfo=timezone.utc)
    assert inv.compute_is_late("RESERVED", d(5), d(1))
    assert not inv.compute_is_late("RESERVED", d(1), d(1))
    assert not inv.compute_is_late("RESERVED", d(5), None)
    assert not inv.compute_is_late("CANCELLED", d(5), d(1))
    assert not inv.compute_is_late("DELIVERED", d(5), d(1))


def test_config_url_forms():
    cfg = pytest.importorskip("app.config")
    s = cfg.Settings(database_url="postgres://u:p@h:5432/d")
    assert s.sqlalchemy_url == "postgresql+psycopg://u:p@h:5432/d"
    s = cfg.Settings(database_url=None, db_user="seller2_svc", db_password="p@ss/word", db_host="pg", db_name="seller2_db")
    assert s.sqlalchemy_url == "postgresql+psycopg://seller2_svc:p%40ss%2Fword@pg:5432/seller2_db"
    assert cfg.Settings(seller_id="seller-4").seller_number == 4
    assert cfg.Settings().db_user == "seller1"            # D's role naming (no _svc suffix)
