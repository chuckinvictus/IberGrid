from __future__ import annotations

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo


MADRID = ZoneInfo("Europe/Madrid")


def ensure_madrid(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=MADRID)
    return dt.astimezone(MADRID)


def start_of_day(day: date) -> datetime:
    return datetime.combine(day, datetime.min.time(), tzinfo=MADRID)


def end_of_day(day: date) -> datetime:
    return start_of_day(day) + timedelta(days=1) - timedelta(minutes=1)


def isoformat_minutes(dt: datetime) -> str:
    return ensure_madrid(dt).isoformat(timespec="minutes")

