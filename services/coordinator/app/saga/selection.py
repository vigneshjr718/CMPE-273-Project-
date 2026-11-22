import asyncio
from dataclasses import dataclass
from datetime import datetime

import httpx
from pydantic import ValidationError

from app.clients.seller_client import QuoteClient
from app.schemas import OrderLine, Quote


@dataclass(frozen=True)
class QuoteFailure:
    seller_id: str
    reason: str


@dataclass(frozen=True)
class QuoteResult:
    eligible: tuple[Quote, ...]
    failures: tuple[QuoteFailure, ...]

    @property
    def best(self) -> Quote | None:
        return self.eligible[0] if self.eligible else None


async def collect_quotes(
    client: QuoteClient, seller_ids: list[str], line: OrderLine, *,
    now: datetime, timeout_seconds: float = 2.0, concurrency: int = 5,
) -> QuoteResult:
    """Concurrent reads only. No reservation, cancellation or retry side effects."""
    if now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if timeout_seconds <= 0 or concurrency < 1:
        raise ValueError("timeout and concurrency must be positive")
    if len(seller_ids) != len(set(seller_ids)):
        raise ValueError("seller IDs must be unique")
    if line.needed_by < now:
        return QuoteResult((), ())
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch(seller_id: str):
        async with semaphore:
            try:
                quote = await asyncio.wait_for(
                    client.quote(seller_id, line), timeout=timeout_seconds
                )
                if quote.seller_id != seller_id or quote.part_id != line.part_id:
                    return QuoteFailure(seller_id, "invalid_quote_identity")
                return quote
            except TimeoutError:
                return QuoteFailure(seller_id, "quote_timeout")
            except (httpx.HTTPError, ValidationError, ValueError):
                return QuoteFailure(seller_id, "quote_unavailable_or_invalid")

    results = await asyncio.gather(*(fetch(sid) for sid in seller_ids))
    eligible = [
        r for r in results if isinstance(r, Quote)
        and r.available_quantity >= line.quantity
        and now <= r.estimated_delivery_at <= line.needed_by
    ]
    eligible.sort(key=lambda q: (q.estimated_delivery_at, q.seller_id))
    failures = tuple(r for r in results if isinstance(r, QuoteFailure))
    # Failures are preserved: no eligible quote is not necessarily proof of no stock.
    return QuoteResult(tuple(eligible), failures)
