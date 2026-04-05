from datetime import datetime, timedelta

import polars as pl

from ibergrid_ml.evaluation.metrics import cheapest_window_hit_rate
from ibergrid_ml.models.heuristics import HeuristicQuantileForecaster
from ibergrid_ml.time import MADRID


def _feature_frame() -> pl.DataFrame:
    start = datetime(2026, 3, 1, tzinfo=MADRID)
    rows = []
    for index in range(24 * 14):
        timestamp = start + timedelta(hours=index)
        rows.append(
            {
                "timestamp": timestamp,
                "pvpc_eur_mwh": 90 + (index % 24) * 1.1,
                "spot_eur_mwh": 60 + (index % 24) * 0.8,
                "demand_actual_mw": 25000 + (index % 24) * 90,
                "shortwave_radiation_wm2": 400 if 8 <= timestamp.hour <= 18 else 0,
                "wind_speed_kmh": 14 + (index % 5),
                "pvpc_lag_24h": 85 + (index % 24) * 1.1,
                "pvpc_lag_168h": 82 + (index % 24) * 1.0,
            }
        )
    return pl.DataFrame(rows)


def test_heuristic_forecaster_returns_hourly_quantiles() -> None:
    frame = _feature_frame()
    model = HeuristicQuantileForecaster()

    forecast = model.forecast(frame, frame["timestamp"].max() + timedelta(hours=1), horizon_hours=24)

    assert forecast.shape == (24, 5)
    assert all(forecast["p10"] < forecast["p50"])
    assert all(forecast["p50"] < forecast["p90"])


def test_cheapest_window_hit_rate_is_bounded() -> None:
    frame = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 4, 1, hour, tzinfo=MADRID) for hour in range(6)],
            "actual": [10, 20, 30, 40, 50, 60],
            "predicted": [11, 21, 31, 59, 49, 39],
        }
    )

    score = cheapest_window_hit_rate(frame, "actual", "predicted", top_k=2)

    assert 0.0 <= score <= 1.0
