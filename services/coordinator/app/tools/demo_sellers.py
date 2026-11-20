"""Controlled in-process test fixture. This is NOT Package C's seller service."""
from datetime import timedelta
from uuid import NAMESPACE_URL, uuid5

from app.clients.reservations import ReservationReply
from app.schemas import Quote, utc_now


class DemoSellers:
    async def quote(self, seller_id, line):
        return Quote(
            seller_id=seller_id, part_id=line.part_id, available_quantity=100,
            estimated_delivery_at=utc_now() + timedelta(hours=int(seller_id.rsplit("-", 1)[-1])),
        )

    async def reserve(self, seller_id, order_id, line, operation_key):
        # Deterministic response only; no real stock ledger or C-owned DB exists.
        return ReservationReply(
            seller_id=seller_id, operation_key=operation_key, part_id=line.part_id,
            quantity=line.quantity, status="RESERVED",
            reservation_id=str(uuid5(NAMESPACE_URL, operation_key)),
            estimated_delivery_at=utc_now() + timedelta(hours=1),
        )
