import pytest

from app.saga.states import LineState as S, require_transition


@pytest.mark.parametrize("target", [S.UNFULFILLED, S.REPLACING, S.DELIVERED])
def test_unknown_cannot_skip_reconciliation(target):
    with pytest.raises(ValueError):
        require_transition(S.UNKNOWN, target)


def test_late_reservation_requires_cancellation_stage():
    with pytest.raises(ValueError):
        require_transition(S.RESERVED, S.REPLACING)
    require_transition(S.RESERVED, S.CANCELLATION_PENDING)
    require_transition(S.CANCELLATION_PENDING, S.REPLACING)


@pytest.mark.parametrize("state", [S.DELIVERED, S.UNFULFILLED])
def test_terminal_states_do_not_restart(state):
    with pytest.raises(ValueError):
        require_transition(state, S.QUOTING)


def test_manual_reconciliation_can_resume():
    require_transition(S.RECONCILIATION_REQUIRED, S.RECONCILING)
