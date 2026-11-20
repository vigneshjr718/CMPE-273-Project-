from datetime import timedelta

import pytest
from pydantic import ValidationError

from app.schemas import OrderCreatedEvent, OrderCreatedPayload, OrderLine
from app.saga.keys import reservation_key


@pytest.mark.parametrize("quantity", [0, -1, True, "2", 1.5])
def test_invalid_quantities_rejected(line, quantity):
    with pytest.raises(ValidationError):
        OrderLine.model_validate({**line.model_dump(), "quantity": quantity})


def test_duplicate_line_ids_rejected(line):
    with pytest.raises(ValidationError, match="unique"):
        OrderCreatedPayload(order_id="o-1", lines=[line, line])


def test_naive_deadline_rejected(line):
    with pytest.raises(ValidationError):
        OrderLine.model_validate({
            **line.model_dump(), "needed_by": line.needed_by.replace(tzinfo=None),
        })


def test_replayed_past_deadline_remains_parseable(line):
    replay = OrderLine.model_validate({
        **line.model_dump(), "needed_by": "2020-01-01T00:00:00Z",
    })
    assert replay.needed_by.year == 2020


def test_event_strict_version_and_extra_fields(line, now):
    event = {
        "event_id": "b114e320-a162-4b3b-9124-e541ccdcf960",
        "event_type": "order.created", "schema_version": 1,
        "producer": "order", "occurred_at": now,
        "payload": {"order_id": "o-1", "lines": [line.model_dump()]},
    }
    assert OrderCreatedEvent.model_validate(event).payload.order_id == "o-1"
    with pytest.raises(ValidationError):
        OrderCreatedEvent.model_validate({**event, "schema_version": 2})
    with pytest.raises(ValidationError):
        OrderCreatedEvent.model_validate({**event, "unexpected": "field"})


def test_operation_keys_stable_but_attempt_specific():
    first = reservation_key("o", "line", "s-1", 1)
    assert first == reservation_key("o", "line", "s-1", 1)
    assert first != reservation_key("o", "line", "s-1", 2)
    assert first != reservation_key("o", "line", "s-2", 1)
    assert reservation_key("a:b", "c", "d", 1) != reservation_key("a", "b:c", "d", 1)
    with pytest.raises(ValueError):
        reservation_key("o", "line", "s", 0)
