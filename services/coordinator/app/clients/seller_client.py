"""Quote-only HTTP adapter for the locally proposed seller contract."""
from typing import Protocol

import httpx

from app.schemas import OrderLine, Quote


class QuoteClient(Protocol):
    async def quote(self, seller_id: str, line: OrderLine) -> Quote: ...


class HttpQuoteClient:
    def __init__(self, client: httpx.AsyncClient, seller_urls: dict[str, str]):
        self.client = client
        self.seller_urls = seller_urls

    async def quote(self, seller_id: str, line: OrderLine) -> Quote:
        url = self.seller_urls[seller_id].rstrip("/") + "/quotes"
        response = await self.client.post(url, json={
            "part_id": line.part_id,
            "quantity": line.quantity,
            "needed_by": line.needed_by.isoformat(),
        })
        response.raise_for_status()
        return Quote.model_validate(response.json())
