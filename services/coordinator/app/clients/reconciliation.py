"""Provisional lookup-by-key adapter. A 404 is absence of evidence, not failure proof."""
from typing import Protocol
from urllib.parse import quote

from app.clients.reservations import (
    HttpReservationClient, ReservationClient, ReservationReply,
)


class ReconciliationClient(ReservationClient, Protocol):
    async def lookup(self, seller_id: str, operation_key: str) -> ReservationReply | None: ...


class HttpReconciliationClient(HttpReservationClient):
    async def lookup(self, seller_id: str, operation_key: str) -> ReservationReply | None:
        response = await self.client.get(
            self.seller_urls[seller_id].rstrip("/")
            + "/reservations/by-key/" + quote(operation_key, safe="")
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError("lookup has not returned a terminal reservation result")
        return ReservationReply.model_validate(response.json())
