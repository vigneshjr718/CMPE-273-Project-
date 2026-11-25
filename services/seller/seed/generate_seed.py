"""Reproducible synthetic inventory for seller-1 … seller-5.

Same SEED_BASE + same seller id  ->  byte-identical inventory, every time.

Design (so the coordinator has interesting choices):
  * A shared catalog of 40 parts, PART-0001 … PART-0040.
  * Every part is carried by exactly 4 of the 5 sellers (one seller skips it),
    so there is always an alternative seller to replace with.
  * Stock per (seller, part): ~10% zero, ~15% exactly one unit, rest 2-60.
  * Price = catalog base price x seller PRICE_MULTIPLIER x small per-seller jitter.
  * PART-0999 "last-unit demo": exactly 1 unit at every seller.

Usage:
    python -m seed.generate_seed --seller-id seller-2 --print     # show rows
    python -m seed.generate_seed --seller-id seller-2 --sql       # emit SQL
    python -m seed.generate_seed --apply                          # insert using app settings (idempotent)

Applying is "insert if missing" (ON CONFLICT DO NOTHING): restarting a seller
never resets stock that has already been reserved.
"""
from __future__ import annotations

import argparse
import random
from dataclasses import dataclass

CATALOG_SIZE = 40
SELLERS = 5
DEMO_PART = "PART-0999"

_NOUNS = ["Bolt", "Bearing", "Gasket", "Valve", "Sensor", "Bracket", "Pump", "Filter",
          "Relay", "Hose", "Spring", "Coupling", "Seal", "Gear", "Fuse", "Clamp"]
_ADJ = ["M6", "M8", "Hex", "Steel", "Brass", "High-temp", "Micro", "Heavy-duty", "Sealed", "24V"]


@dataclass(frozen=True)
class SeedRow:
    part_id: str
    description: str
    unit_price_cents: int
    quantity: int


def catalog(seed_base: int) -> list[tuple[str, str, int]]:
    """(part_id, description, base_price_cents) shared by all sellers."""
    rng = random.Random(seed_base)
    out = []
    for i in range(1, CATALOG_SIZE + 1):
        desc = f"{rng.choice(_ADJ)} {rng.choice(_NOUNS)}"
        base = rng.randint(150, 25_000)
        out.append((f"PART-{i:04d}", desc, base))
    return out


def seller_rows(seed_base: int, seller_number: int, price_multiplier: float = 1.0) -> list[SeedRow]:
    if not 1 <= seller_number <= SELLERS:
        raise ValueError("seller_number must be 1..5")
    rng = random.Random(seed_base * 1000 + seller_number)
    rows: list[SeedRow] = []
    for idx, (part_id, desc, base) in enumerate(catalog(seed_base), start=1):
        if idx % SELLERS == seller_number % SELLERS:   # each part skipped by exactly one seller
            continue
        roll = rng.random()
        qty = 0 if roll < 0.10 else 1 if roll < 0.25 else rng.randint(2, 60)
        price = max(1, round(base * price_multiplier * rng.uniform(0.9, 1.1)))
        rows.append(SeedRow(part_id, desc, price, qty))
    rows.append(SeedRow(DEMO_PART, "Last-unit demo part", max(1, round(9_999 * price_multiplier)), 1))
    return rows


def to_sql(rows: list[SeedRow]) -> str:
    def lit(s: str) -> str:
        return "'" + s.replace("'", "''") + "'"
    values = ",\n".join(
        f"  ({lit(r.part_id)}, {lit(r.description)}, {r.unit_price_cents}, {r.quantity}, {r.quantity})" for r in rows
    )
    return ("INSERT INTO inventory (part_id, description, unit_price_cents, total_quantity, available_quantity)\n"
            f"VALUES\n{values}\nON CONFLICT (part_id) DO NOTHING;\n")


def apply(engine, seed_base: int, seller_number: int, price_multiplier: float) -> int:
    """Insert missing rows. Returns number of rows inserted (0 on a restarted seller)."""
    from sqlalchemy import text

    rows = seller_rows(seed_base, seller_number, price_multiplier)
    stmt = text(
        "INSERT INTO inventory (part_id, description, unit_price_cents, total_quantity, available_quantity)"
        " VALUES (:part_id, :description, :price, :qty, :qty) ON CONFLICT (part_id) DO NOTHING"
    )
    inserted = 0
    with engine.begin() as conn:
        for r in rows:
            inserted += conn.execute(stmt, {"part_id": r.part_id, "description": r.description,
                                            "price": r.unit_price_cents, "qty": r.quantity}).rowcount
    return inserted


def _main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seller-id", help="e.g. seller-3 (default: SELLER_ID from env)")
    p.add_argument("--seed-base", type=int, help="default: SEED_BASE from env")
    p.add_argument("--price-multiplier", type=float, help="default: PRICE_MULTIPLIER from env")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--print", action="store_true", help="print a table (default)")
    mode.add_argument("--sql", action="store_true", help="print INSERT SQL")
    mode.add_argument("--apply", action="store_true", help="insert into the seller DB (uses app settings)")
    a = p.parse_args()

    if a.apply or not (a.seller_id and a.seed_base is not None and a.price_multiplier is not None):
        from app.config import get_settings
        s = get_settings()
        seller_id = a.seller_id or s.seller_id
        seed_base = a.seed_base if a.seed_base is not None else s.seed_base
        mult = a.price_multiplier if a.price_multiplier is not None else s.price_multiplier
    else:
        s = None
        seller_id, seed_base, mult = a.seller_id, a.seed_base, a.price_multiplier
    number = int("".join(c for c in seller_id if c.isdigit()) or 1)

    if a.apply:
        from app.db import make_engine
        assert s is not None  # --apply always loads settings above
        n = apply(make_engine(s), seed_base, number, mult)
        print(f"{seller_id}: inserted {n} new inventory rows")
        return
    rows = seller_rows(seed_base, number, mult)
    if a.sql:
        print(to_sql(rows), end="")
        return
    print(f"{seller_id} (seed_base={seed_base}, price_multiplier={mult}): {len(rows)} parts, "
          f"{sum(r.quantity for r in rows)} units")
    for r in rows:
        print(f"  {r.part_id}  {r.quantity:>3} units  {r.unit_price_cents/100:>9.2f}  {r.description}")


if __name__ == "__main__":
    _main()
