"""Validated KST processing boundaries, independent of scheduler wall clock."""
from __future__ import annotations
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
KST = ZoneInfo("Asia/Seoul")


def validate_ymd(value: str) -> str:
    if len(value) != 8 or not value.isascii() or not value.isdigit():
        raise ValueError("target_ymd must be YYYYMMDD")
    datetime.strptime(value, "%Y%m%d")
    return value


def date_parts(value: str) -> tuple[str, str, str]:
    validate_ymd(value)
    return value[:4], value[4:6], value[6:8]


def parse_as_of(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def now_kst() -> datetime:
    return datetime.now(KST).replace(tzinfo=None)


def window_bounds(as_of: datetime, minutes: int = 15) -> tuple[datetime, datetime]:
    if minutes <= 0:
        raise ValueError("window minutes must be positive")
    end = as_of.replace(minute=(as_of.minute // 5) * 5, second=0, microsecond=0)
    return end - timedelta(minutes=minutes), end


def hour_starts(start: datetime, end: datetime) -> list[datetime]:
    """Snapshot partitions intersecting the half-open interval [start, end)."""
    if end <= start:
        raise ValueError("end must be after start")
    result = []
    cursor = start.replace(minute=0, second=0, microsecond=0)
    while cursor < end:
        result.append(cursor)
        cursor += timedelta(hours=1)
    return result
