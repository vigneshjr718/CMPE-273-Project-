import asyncio
from datetime import timedelta

import httpx
import pytest

from app.clients.seller_client import HttpQuoteClient
from app.schemas import Quote
from app.saga.selection import collect_quotes


class Sellers:
    def __init__(self, now):
        self.now = now
        self.active = 0
        self.max_active = 0

    async def quote(self, seller_id, line):
        self.active += 1
        self.max_active = max(self.active, self.max_active)
        try:
            if seller_id == "slow":
                await asyncio.Event().wait()
            if seller_id == "down":
                raise httpx.ConnectError("seller down")
            await asyncio.sleep(0.005)
            return Quote(
                seller_id=seller_id, part_id=line.part_id,
                available_quantity=0 if seller_id == "empty" else 10,
                estimated_delivery_at=self.now + timedelta(
                    days=3 if seller_id == "late" else 1),
            )
        finally:
            self.active -= 1


async def test_queries_overlap_and_are_bounded(line, now):
    client = Sellers(now)
    result = await collect_quotes(client, ["s3", "s2", "s1"], line,
                                  now=now, concurrency=2)
    assert client.max_active == 2
    assert [q.seller_id for q in result.eligible] == ["s1", "s2", "s3"]
    assert result.failures == ()


async def test_slow_or_failed_seller_preserves_other_quotes(line, now):
    client = Sellers(now)
    result = await collect_quotes(
        client, ["slow", "down", "good", "empty", "late"], line,
        now=now, timeout_seconds=0.03,
    )
    assert result.best.seller_id == "good"
    assert {f.seller_id for f in result.failures} == {"slow", "down"}
    assert client.active == 0


async def test_no_eligible_quote_preserves_uncertainty(line, now):
    result = await collect_quotes(Sellers(now), ["down", "empty"], line, now=now)
    assert result.best is None
    assert result.failures[0].seller_id == "down"


async def test_expired_line_does_not_call_sellers(line, now):
    class Unexpected:
        async def quote(self, *_):
            pytest.fail("expired line should not query sellers")
    result = await collect_quotes(
        Unexpected(), ["s1"], line, now=now + timedelta(days=3))
    assert result.best is None


async def test_quote_cannot_impersonate_another_seller(line, now):
    class Wrong:
        async def quote(self, *_):
            return Quote(seller_id="other", part_id=line.part_id,
                         available_quantity=9, estimated_delivery_at=now)
    result = await collect_quotes(Wrong(), ["s1"], line, now=now)
    assert result.best is None
    assert result.failures[0].reason == "invalid_quote_identity"


async def test_http_adapter_uses_proposed_wire_contract(line, now):
    def respond(request):
        assert request.method == "POST"
        assert str(request.url) == "http://seller-1:8000/quotes"
        assert b'"quantity":2' in request.content
        return httpx.Response(200, json={
            "seller_id": "s1", "part_id": "bumper", "available_quantity": 4,
            "estimated_delivery_at": now.isoformat(),
        })
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        client = HttpQuoteClient(http, {"s1": "http://seller-1:8000"})
        quote = await client.quote("s1", line)
        assert quote.available_quantity == 4


async def test_cancellation_propagates_to_pending_quote_tasks(line, now):
    client = Sellers(now)
    task = asyncio.create_task(collect_quotes(client, ["slow"], line, now=now))
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.active == 0
