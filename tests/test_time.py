from datetime import date, datetime

from ibergrid_ml.time import MADRID, end_of_day, ensure_madrid, start_of_day


def test_start_and_end_of_day_use_madrid_timezone() -> None:
    day = date(2026, 3, 29)
    start = start_of_day(day)
    end = end_of_day(day)

    assert start.tzinfo == MADRID
    assert end.tzinfo == MADRID
    assert start.hour == 0
    assert end.hour == 23


def test_ensure_madrid_keeps_aware_datetimes() -> None:
    naive = datetime(2026, 4, 1, 12, 30)
    aware = ensure_madrid(naive)

    assert aware.tzinfo == MADRID
    assert aware.hour == 12

