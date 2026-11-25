"""In-memory fault injection (per instance, cleared on restart).

Pure logic, no web framework imports, so it is unit-testable on its own.

Fault kinds, evaluated once per request (`FaultRegistry.decide`):
  delay_ms        sleep before doing anything
  error           503 BEFORE any DB change      -> caller may safely retry
  lost_reply      DB change IS committed, then the reply is withheld for
                  lost_reply_hang_ms and finally a 504 is returned
                  -> caller times out with an UNKNOWN outcome
"""
from __future__ import annotations

import random
import threading
import uuid
from dataclasses import asdict, dataclass, field

ENDPOINTS = ("*", "quotes", "reserve", "cancel", "lookup", "delivery")


@dataclass
class FaultRule:
    endpoint: str
    delay_ms: int = 0
    error_rate: float = 0.0
    lost_reply_rate: float = 0.0
    lost_reply_hang_ms: int = 30_000
    remaining: int | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    fired: int = 0

    def __post_init__(self) -> None:
        if self.endpoint not in ENDPOINTS:
            raise ValueError(f"endpoint must be one of {ENDPOINTS}")
        if not 0 <= self.error_rate <= 1 or not 0 <= self.lost_reply_rate <= 1:
            raise ValueError("rates must be within [0, 1]")
        if self.delay_ms < 0 or self.lost_reply_hang_ms < 0:
            raise ValueError("durations must be >= 0")
        if self.remaining is not None and self.remaining < 1:
            raise ValueError("remaining must be >= 1 or null")

    def matches(self, endpoint: str) -> bool:
        return self.endpoint in ("*", endpoint)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FaultDecision:
    delay_ms: int = 0
    error: bool = False
    lost_reply: bool = False
    lost_reply_hang_ms: int = 0
    rule_ids: tuple[str, ...] = ()

    @property
    def any(self) -> bool:
        return bool(self.delay_ms or self.error or self.lost_reply)


class FaultRegistry:
    def __init__(self, rng: random.Random | None = None) -> None:
        self._rules: list[FaultRule] = []
        self._lock = threading.Lock()
        self._rng = rng or random.Random()

    def add(self, rule: FaultRule) -> FaultRule:
        with self._lock:
            self._rules.append(rule)
        return rule

    def clear(self) -> None:
        with self._lock:
            self._rules.clear()

    def list(self) -> list[FaultRule]:
        with self._lock:
            return list(self._rules)

    def decide(self, endpoint: str) -> FaultDecision:
        """Roll the dice for one request. A rule with `remaining` is consumed only when it fires."""
        d = FaultDecision()
        fired: list[str] = []
        with self._lock:
            for rule in list(self._rules):
                if not rule.matches(endpoint):
                    continue
                hit = False
                if rule.delay_ms:
                    d.delay_ms = max(d.delay_ms, rule.delay_ms)
                    hit = True
                if rule.error_rate and self._rng.random() < rule.error_rate:
                    d.error = True
                    hit = True
                elif rule.lost_reply_rate and self._rng.random() < rule.lost_reply_rate:
                    d.lost_reply = True
                    d.lost_reply_hang_ms = max(d.lost_reply_hang_ms, rule.lost_reply_hang_ms)
                    hit = True
                if hit:
                    rule.fired += 1
                    fired.append(rule.id)
                    if rule.remaining is not None:
                        rule.remaining -= 1
                        if rule.remaining <= 0:
                            self._rules.remove(rule)
        if d.error:  # an error happens before any DB work, so it cannot also lose a committed reply
            d.lost_reply = False
        d.rule_ids = tuple(fired)
        return d
