from datetime import datetime, timedelta, timezone

import pytest

from app.schemas import OrderLine

@pytest.fixture
def now():
    return datetime(2030, 1, 1, tzinfo=timezone.utc)

@pytest.fixture
def line(now):
    return OrderLine(
        line_id="line-1", part_id="bumper", quantity=2,
        needed_by=now + timedelta(days=2),
    )
