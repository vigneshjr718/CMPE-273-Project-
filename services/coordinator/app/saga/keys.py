import hashlib
import json


def reservation_key(order_id: str, line_id: str, seller_id: str, attempt_number: int) -> str:
    """Persist this key before sending; retries reuse the saved key."""
    if attempt_number < 1:
        raise ValueError("attempt_number starts at one")
    raw = json.dumps(
        [order_id, line_id, seller_id, attempt_number],
        separators=(",", ":"), ensure_ascii=True,
    ).encode()
    return "reserve-" + hashlib.sha256(raw).hexdigest()
