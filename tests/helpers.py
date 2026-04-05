from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.time import MADRID


def clear_runtime_caches() -> None:
    from ibergrid_api.config import get_settings as get_api_settings
    from ibergrid_api.dependencies import get_forecast_service
    from ibergrid_ml.persistence import get_engine, get_session_factory

    get_engine.cache_clear()
    get_session_factory.cache_clear()
    get_api_settings.cache_clear()
    get_forecast_service.cache_clear()


def configure_test_environment(monkeypatch, tmp_path: Path) -> ForecastSettings:
    data_root = tmp_path / "data"
    artifacts_root = tmp_path / "artifacts"
    mlruns_root = tmp_path / "mlruns"
    monkeypatch.setenv("IBERGRID_DATA_ROOT", str(data_root))
    monkeypatch.setenv("IBERGRID_ARTIFACTS_ROOT", str(artifacts_root))
    monkeypatch.setenv("IBERGRID_DATABASE_URL", f"sqlite:///{tmp_path / 'ibergrid-test.db'}")
    monkeypatch.setenv("IBERGRID_MLFLOW_TRACKING_URI", f"sqlite:///{tmp_path / 'mlflow-test.db'}")
    clear_runtime_caches()
    return ForecastSettings()


def build_training_frame(days: int = 96) -> pl.DataFrame:
    start = datetime(2025, 12, 1, tzinfo=MADRID)
    rows: list[dict[str, object]] = []
    for index in range(days * 24):
        timestamp = start + timedelta(hours=index)
        hour = timestamp.hour
        solar = max(0.0, 950.0 - abs(hour - 13) * 110.0)
        wind = 1250.0 + ((index * 7) % 380)
        total_generation = 7600.0 + solar + wind
        demand_actual = 23150.0 + hour * 92.0 + (0 if timestamp.weekday() < 5 else -620.0)
        demand_forecast = demand_actual + ((index % 6) - 3) * 35.0
        temperature = 10.0 + hour * 0.55
        spot = 41.0 + hour * 0.7 + demand_actual / 1800.0 - solar / 280.0 - wind / 450.0
        pvpc = spot + 12.0 + (4.5 if hour in {8, 9, 20, 21} else 0.0)
        rows.append(
            {
                "timestamp": timestamp,
                "pvpc_eur_mwh": pvpc,
                "spot_eur_mwh": spot,
                "demand_actual_mw": demand_actual,
                "demand_forecast_mw": demand_forecast,
                "temperature_c": temperature,
                "relative_humidity_pct": 58.0 - min(hour, 12) * 0.9,
                "wind_speed_kmh": 12.5 + ((index * 3) % 8),
                "shortwave_radiation_wm2": solar,
                "wind_generation_mwh": wind,
                "solar_generation_mwh": solar,
                "total_generation_mwh": total_generation,
            }
        )

    return (
        pl.DataFrame(rows)
        .sort("timestamp")
        .with_columns(
            pl.col("timestamp").dt.hour().alias("hour_of_day"),
            pl.col("timestamp").dt.weekday().alias("day_of_week"),
            pl.col("timestamp").dt.month().alias("month_of_year"),
            pl.col("timestamp").dt.week().alias("week_of_year"),
            (pl.col("timestamp").dt.weekday() >= 5).alias("is_weekend"),
            pl.lit(False).alias("is_holiday"),
            (pl.col("wind_generation_mwh") / pl.col("total_generation_mwh")).alias("wind_share"),
            (pl.col("solar_generation_mwh") / pl.col("total_generation_mwh")).alias("solar_share"),
            pl.col("pvpc_eur_mwh").shift(24).alias("pvpc_lag_24h"),
            pl.col("pvpc_eur_mwh").shift(24 * 7).alias("pvpc_lag_168h"),
            pl.col("spot_eur_mwh").shift(24).alias("spot_lag_24h"),
            pl.col("demand_actual_mw").shift(24).alias("demand_lag_24h"),
            pl.col("demand_forecast_mw").shift(24).alias("demand_forecast_lag_24h"),
            pl.col("temperature_c").shift(24).alias("temperature_lag_24h"),
            pl.col("wind_speed_kmh").shift(24).alias("wind_speed_lag_24h"),
            pl.col("shortwave_radiation_wm2").shift(24).alias("solar_radiation_lag_24h"),
            pl.col("pvpc_eur_mwh").rolling_mean(window_size=24, min_samples=6).alias("pvpc_rolling_mean_24h"),
            pl.col("pvpc_eur_mwh").rolling_std(window_size=24, min_samples=6).alias("pvpc_rolling_std_24h"),
            pl.col("demand_actual_mw").rolling_mean(window_size=24, min_samples=6).alias("demand_rolling_mean_24h"),
            pl.col("demand_actual_mw").rolling_std(window_size=24, min_samples=6).alias("demand_rolling_std_24h"),
        )
        .with_columns(
            pl.when(pl.col("pvpc_rolling_std_24h") >= 25.0)
            .then(pl.lit("high-volatility"))
            .when(pl.col("pvpc_rolling_mean_24h") >= 100.0)
            .then(pl.lit("elevated"))
            .otherwise(pl.lit("normal"))
            .alias("price_regime")
        )
        .drop_nulls(
            subset=[
                "pvpc_lag_24h",
                "pvpc_lag_168h",
                "spot_lag_24h",
                "pvpc_rolling_mean_24h",
                "pvpc_rolling_std_24h",
            ]
        )
    )


def build_source_health_frame(observed_at: datetime) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "source_name": "redata_pvpc",
                "observed_at": observed_at,
                "status": "healthy",
                "freshness_hours": 1.0,
                "row_count": 1000,
                "null_rate": 0.0,
                "detail": "synthetic healthy source",
                "metrics_json": {},
            },
            {
                "source_name": "redata_spot",
                "observed_at": observed_at,
                "status": "healthy",
                "freshness_hours": 1.0,
                "row_count": 1000,
                "null_rate": 0.0,
                "detail": "synthetic healthy source",
                "metrics_json": {},
            },
            {
                "source_name": "redata_demand_actual",
                "observed_at": observed_at,
                "status": "healthy",
                "freshness_hours": 1.0,
                "row_count": 1000,
                "null_rate": 0.0,
                "detail": "synthetic healthy source",
                "metrics_json": {},
            },
            {
                "source_name": "redata_demand_forecast",
                "observed_at": observed_at,
                "status": "healthy",
                "freshness_hours": 1.0,
                "row_count": 1000,
                "null_rate": 0.0,
                "detail": "synthetic healthy source",
                "metrics_json": {},
            },
            {
                "source_name": "open_meteo_weather",
                "observed_at": observed_at,
                "status": "healthy",
                "freshness_hours": 3.0,
                "row_count": 1000,
                "null_rate": 0.0,
                "detail": "synthetic healthy source",
                "metrics_json": {},
            },
            {
                "source_name": "omie_reconciliation",
                "observed_at": observed_at,
                "status": "healthy",
                "freshness_hours": 6.0,
                "row_count": 336,
                "null_rate": 0.0,
                "detail": "synthetic healthy source",
                "metrics_json": {"latest_absolute_delta_eur_mwh": 0.2},
            },
        ]
    )


def build_serving_snapshot(
    training_frame: pl.DataFrame,
    target_start: datetime,
    horizon_hours: int,
) -> pl.DataFrame:
    history = training_frame.tail(24 * 21).select(
        "timestamp",
        "pvpc_eur_mwh",
        "spot_eur_mwh",
        "demand_actual_mw",
        "demand_forecast_mw",
        "temperature_c",
        "relative_humidity_pct",
        "wind_speed_kmh",
        "shortwave_radiation_wm2",
        "hour_of_day",
        "day_of_week",
        "month_of_year",
        "week_of_year",
        "is_weekend",
        "is_holiday",
        "pvpc_lag_24h",
        "pvpc_lag_168h",
        "spot_lag_24h",
        "demand_lag_24h",
        "demand_forecast_lag_24h",
        "temperature_lag_24h",
        "wind_speed_lag_24h",
        "solar_radiation_lag_24h",
        "pvpc_rolling_mean_24h",
        "pvpc_rolling_std_24h",
        "demand_rolling_mean_24h",
        "demand_rolling_std_24h",
    )
    rows: list[dict[str, object]] = []
    for offset in range(horizon_hours):
        timestamp = target_start + timedelta(hours=offset)
        rows.append(
            {
                "timestamp": timestamp,
                "pvpc_eur_mwh": None,
                "spot_eur_mwh": None,
                "demand_actual_mw": None,
                "demand_forecast_mw": 24700.0 + timestamp.hour * 84.0,
                "temperature_c": 11.0 + timestamp.hour * 0.4,
                "relative_humidity_pct": 55.0,
                "wind_speed_kmh": 15.0 + (offset % 6),
                "shortwave_radiation_wm2": max(0.0, 920.0 - abs(timestamp.hour - 13) * 105.0),
                "hour_of_day": timestamp.hour,
                "day_of_week": timestamp.weekday(),
                "month_of_year": timestamp.month,
                "week_of_year": int(timestamp.strftime("%V")),
                "is_weekend": timestamp.weekday() >= 5,
                "is_holiday": False,
                "pvpc_lag_24h": float(training_frame.tail(24)["pvpc_eur_mwh"][offset % 24]),
                "pvpc_lag_168h": float(training_frame.tail(24 * 7)["pvpc_eur_mwh"][offset % (24 * 7)]),
                "spot_lag_24h": float(training_frame.tail(24)["spot_eur_mwh"][offset % 24]),
                "demand_lag_24h": float(training_frame.tail(24)["demand_actual_mw"][offset % 24]),
                "demand_forecast_lag_24h": float(training_frame.tail(24)["demand_forecast_mw"][offset % 24]),
                "temperature_lag_24h": float(training_frame.tail(24)["temperature_c"][offset % 24]),
                "wind_speed_lag_24h": float(training_frame.tail(24)["wind_speed_kmh"][offset % 24]),
                "solar_radiation_lag_24h": float(training_frame.tail(24)["shortwave_radiation_wm2"][offset % 24]),
                "pvpc_rolling_mean_24h": float(training_frame.tail(24)["pvpc_eur_mwh"].mean()),
                "pvpc_rolling_std_24h": float(training_frame.tail(24)["pvpc_eur_mwh"].std()),
                "demand_rolling_mean_24h": float(training_frame.tail(24)["demand_actual_mw"].mean()),
                "demand_rolling_std_24h": float(training_frame.tail(24)["demand_actual_mw"].std()),
            }
        )
    return pl.concat([history, pl.DataFrame(rows)], how="diagonal_relaxed").sort("timestamp")
