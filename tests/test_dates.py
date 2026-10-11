from datetime import datetime
import pytest
from src.common.dates import validate_ymd, window_bounds, hour_starts


@pytest.mark.parametrize("value", ["20260230", "20261301", "2026011", "2026-01-01", "20260101;echo bad"])
def test_invalid_dates_rejected(value):
    with pytest.raises(ValueError):
        validate_ymd(value)


@pytest.mark.parametrize("end,expected", [
    ("2026-01-31 23:45:00", ["2026-01-31 23:00"]),
    ("2026-02-01 00:00:00", ["2026-01-31 23:00"]),
    ("2026-02-01 00:05:00", ["2026-01-31 23:00", "2026-02-01 00:00"]),
    ("2026-02-01 00:15:00", ["2026-02-01 00:00"]),
])
def test_half_open_windows_cross_month_and_midnight(end, expected):
    start, finish = window_bounds(datetime.fromisoformat(end))
    assert [h.strftime("%Y-%m-%d %H:%M") for h in hour_starts(start, finish)] == expected


def test_window_end_is_deterministic():
    _, end = window_bounds(datetime(2026, 1, 1, 10, 7, 59))
    assert end == datetime(2026, 1, 1, 10, 5)
