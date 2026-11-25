"""Shared request helpers: app state access, fault hooks, admin auth."""
from __future__ import annotations

import hmac
import logging
import time

from fastapi import Header, Request

from app.errors import ApiError
from app.faults import FaultDecision

log = logging.getLogger("seller.faults")


def before(request: Request, endpoint: str) -> FaultDecision:
    """Apply injected delay/error BEFORE any DB work. Returns the decision for after()."""
    decision: FaultDecision = request.app.state.faults.decide(endpoint)
    if decision.any:
        log.warning("fault injected", extra={"endpoint": endpoint, "delay_ms": decision.delay_ms,
                                             "error": decision.error, "lost_reply": decision.lost_reply,
                                             "rules": list(decision.rule_ids)})
    if decision.delay_ms:
        time.sleep(decision.delay_ms / 1000)
    if decision.error:
        raise ApiError(503, "SELLER_UNAVAILABLE", "Injected fault: failed before any change was made",
                       {"injected": True})
    return decision


def after_commit(decision: FaultDecision, committed_change: bool, endpoint: str) -> None:
    """'Committed but reply lost': the change is durable, but the caller never hears back in time."""
    if decision.lost_reply and committed_change:
        log.warning("withholding reply after commit", extra={"endpoint": endpoint,
                                                             "hang_ms": decision.lost_reply_hang_ms})
        time.sleep(decision.lost_reply_hang_ms / 1000)
        raise ApiError(504, "REPLY_LOST", "Injected fault: the change WAS committed but the reply was withheld",
                       {"injected": True})


def require_admin(request: Request, x_admin_token: str | None = Header(default=None)) -> None:
    expected = request.app.state.settings.admin_token
    if not expected or not x_admin_token or not hmac.compare_digest(expected, x_admin_token):
        raise ApiError(403, "ADMIN_FORBIDDEN", "Admin token missing/invalid, or admin endpoints are disabled")
