"""Draft line state machine; all transitions still need durable orchestration."""
from enum import StrEnum


class LineState(StrEnum):
    PENDING = "PENDING"
    QUOTING = "QUOTING"
    RESERVING = "RESERVING"
    RESERVED = "RESERVED"
    UNKNOWN = "UNKNOWN"
    RECONCILING = "RECONCILING"
    CANCELLATION_PENDING = "CANCELLATION_PENDING"
    REPLACING = "REPLACING"
    DELIVERED = "DELIVERED"
    UNFULFILLED = "UNFULFILLED"
    RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"


TRANSITIONS = {
    LineState.PENDING: {LineState.QUOTING},
    LineState.QUOTING: {
        LineState.RESERVING, LineState.UNFULFILLED,
        LineState.RECONCILIATION_REQUIRED,
    },
    LineState.RESERVING: {
        LineState.RESERVED, LineState.UNKNOWN, LineState.QUOTING,
    },
    LineState.RESERVED: {LineState.CANCELLATION_PENDING, LineState.DELIVERED},
    LineState.UNKNOWN: {LineState.RECONCILING, LineState.CANCELLATION_PENDING},
    LineState.RECONCILING: {
        LineState.RESERVED, LineState.CANCELLATION_PENDING,
        LineState.RECONCILIATION_REQUIRED, LineState.UNKNOWN, LineState.QUOTING,
    },
    LineState.CANCELLATION_PENDING: {
        LineState.REPLACING, LineState.DELIVERED,
        LineState.RECONCILIATION_REQUIRED,
    },
    LineState.REPLACING: {LineState.QUOTING},
    LineState.RECONCILIATION_REQUIRED: {
        LineState.RECONCILING, LineState.CANCELLATION_PENDING, LineState.QUOTING,
    },
    LineState.DELIVERED: set(),
    LineState.UNFULFILLED: set(),
}


def require_transition(current: LineState, target: LineState) -> None:
    if target not in TRANSITIONS[current]:
        raise ValueError(f"illegal line transition: {current} -> {target}")
