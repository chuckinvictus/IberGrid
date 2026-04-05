from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from math import ceil

import duckdb
import holidays
import polars as pl

from ibergrid_ml.clients.open_meteo import OpenMeteoClient
from ibergrid_ml.clients.omie import OMIEClient
from ibergrid_ml.clients.redata import REDataClient
from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.data.store import LakehouseStore
from ibergrid_ml.schemas import DatasetName
from ibergrid_ml.time import MADRID, end_of_day, ensure_madrid, start_of_day


def _datetime_range(start_at: datetime, end_at: datetime) -> list[datetime]:
    rows: list[datetime] = []
    cursor = ensure_madrid(start_at).replace(minute=0, second=0, microsecond=0)
    end_at = ensure_madrid(end_at).replace(minute=0, second=0, microsecond=0)
    while cursor <= end_at:
        rows.append(cursor)
        cursor = cursor + timedelta(hours=1)
    return rows


def _safe_numeric(frame: pl.DataFrame, column: str) -> pl.Expr:
    if column not in frame.columns:
        return pl.lit(0.0).alias(column)
    return pl.col(column).fill_null(0.0)


@dataclass(slots=True)
class FeatureBuilder:
    settings: ForecastSettings
    store: LakehouseStore
    redata: REDataClient
    weather: OpenMeteoClient
    omie: OMIEClient

    @classmethod
    def from_settings(cls, settings: ForecastSettings) -> "FeatureBuilder":
        store = LakehouseStore(settings)
        return cls(
            settings=settings,
            store=store,
            redata=REDataClient(settings),
            weather=OpenMeteoClient(settings),
            omie=OMIEClient(settings),
        )

    def refresh_recent(self, days: int = 120) -> None:
        today = datetime.now(MADRID).date()
        self.backfill_range(today - timedelta(days=days), today)

    def backfill_range(self, start_day: date, end_day: date, chunk_days: int = 14) -> None:
        current = start_day
        while current <= end_day:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end_day)
            prices = self.redata.fetch_market_prices(start_of_day(current), end_of_day(chunk_end))
            demand = self.redata.fetch_demand(start_of_day(current), end_of_day(chunk_end))
            generation = self.redata.fetch_generation_mix_daily(start_of_day(current), end_of_day(chunk_end))
            weather = self.weather.fetch_archive(current, chunk_end)
            self.store.merge_write(prices, "bronze", "redata_market_prices", ["timestamp", "metric"])
            self.store.merge_write(demand, "bronze", "redata_demand", ["timestamp", "metric"])
            self.store.merge_write(generation, "bronze", "redata_generation_daily", ["day", "technology"])
            self.store.merge_write(weather, "bronze", "open_meteo_weather", ["timestamp", "hub"])
            current = chunk_end + timedelta(days=1)

        self._materialize_silver()
        self._materialize_gold()
        self._materialize_spot_reconciliation(start_day, end_day)

    def build_serving_snapshot(self, start_at: datetime, horizon_hours: int | None = None) -> pl.DataFrame:
        horizon_hours = horizon_hours or self.settings.horizon_hours
        history = self.store.read("gold", DatasetName.TRAINING_DATASET).sort("timestamp")
        if history.is_empty():
            history = self.store.read("gold", DatasetName.FEATURE_SNAPSHOT_HOURLY).sort("timestamp")
        if history.is_empty():
            return history

        start_at = ensure_madrid(start_at).replace(minute=0, second=0, microsecond=0)
        end_at = start_at + timedelta(hours=horizon_hours - 1)
        horizon_days = max(ceil(horizon_hours / 24), 1)
        weather_future = self.weather.fetch_forecast(start_at.date(), horizon_days=max(horizon_days, 7))
        demand_future = self.redata.fetch_demand(start_at, end_at)

        demand_wide = (
            demand_future.pivot(index="timestamp", on="metric", values="value_mw", aggregate_function="first")
            .sort("timestamp")
            if not demand_future.is_empty()
            else pl.DataFrame(schema={"timestamp": pl.Datetime(time_zone="Europe/Madrid")})
        )
        weather_hourly = (
            weather_future.group_by("timestamp")
            .agg(
                pl.col("temperature_c").mean(),
                pl.col("relative_humidity_pct").mean(),
                pl.col("wind_speed_kmh").mean(),
                pl.col("shortwave_radiation_wm2").mean(),
            )
            .sort("timestamp")
            if not weather_future.is_empty()
            else pl.DataFrame(schema={"timestamp": pl.Datetime(time_zone="Europe/Madrid")})
        )

        future_frame = pl.DataFrame({"timestamp": _datetime_range(start_at, end_at)}).join(
            demand_wide, on="timestamp", how="left"
        ).join(weather_hourly, on="timestamp", how="left")

        combined = pl.concat(
            [
                history.select(
                    "timestamp",
                    "pvpc_eur_mwh",
                    "spot_eur_mwh",
                    "demand_actual_mw",
                    "demand_forecast_mw",
                    "temperature_c",
                    "relative_humidity_pct",
                    "wind_speed_kmh",
                    "shortwave_radiation_wm2",
                    "wind_generation_mwh",
                    "solar_generation_mwh",
                    "total_generation_mwh",
                ),
                future_frame.with_columns(
                    pl.lit(None, dtype=pl.Float64).alias("pvpc_eur_mwh"),
                    pl.lit(None, dtype=pl.Float64).alias("spot_eur_mwh"),
                    pl.lit(None, dtype=pl.Float64).alias("demand_actual_mw"),
                    pl.lit(None, dtype=pl.Float64).alias("wind_generation_mwh"),
                    pl.lit(None, dtype=pl.Float64).alias("solar_generation_mwh"),
                    pl.lit(None, dtype=pl.Float64).alias("total_generation_mwh"),
                ).select(
                    "timestamp",
                    "pvpc_eur_mwh",
                    "spot_eur_mwh",
                    "demand_actual_mw",
                    "demand_forecast_mw",
                    "temperature_c",
                    "relative_humidity_pct",
                    "wind_speed_kmh",
                    "shortwave_radiation_wm2",
                    "wind_generation_mwh",
                    "solar_generation_mwh",
                    "total_generation_mwh",
                ),
            ],
            how="diagonal_relaxed",
        ).unique(subset=["timestamp"], keep="last").sort("timestamp")

        enriched = self._enrich_feature_frame(combined, drop_training_nulls=False)
        self.store.write(enriched, "gold", DatasetName.SERVING_SNAPSHOT)
        return enriched

    def build_reference_snapshot(self, day: date) -> pl.DataFrame:
        reference = self.omie.fetch_day_ahead_reference(day)
        self.store.write(reference, "bronze", "omie_spot_reference_latest")
        return reference

    def refresh_spot_reconciliation(self, start_day: date, end_day: date) -> pl.DataFrame:
        self._materialize_spot_reconciliation(start_day, end_day)
        return self.store.read("gold", DatasetName.SPOT_RECONCILIATION)

    def _materialize_silver(self) -> None:
        prices = self.store.read("bronze", "redata_market_prices")
        demand = self.store.read("bronze", "redata_demand")
        generation = self.store.read("bronze", "redata_generation_daily")
        weather = self.store.read("bronze", "open_meteo_weather")

        pvpc_hourly = (
            prices.filter(pl.col("metric") == "pvpc")
            .with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
            .group_by("hour")
            .agg(pl.col("price_eur_mwh").mean().alias("price_eur_mwh"))
            .rename({"hour": "timestamp"})
            .sort("timestamp")
        )
        spot_hourly = (
            prices.filter(pl.col("metric") == "spot")
            .with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
            .group_by("hour")
            .agg(pl.col("price_eur_mwh").mean().alias("price_eur_mwh"))
            .rename({"hour": "timestamp"})
            .sort("timestamp")
        )
        self.store.write(pvpc_hourly, "silver", DatasetName.PVPC_HOURLY)
        self.store.write(spot_hourly, "silver", DatasetName.SPOT_HOURLY)

        demand_wide = (
            demand.pivot(index="timestamp", on="metric", values="value_mw", aggregate_function="first")
            .sort("timestamp")
        )
        if "demand_actual_mw" in demand_wide.columns:
            self.store.write(
                demand_wide.select("timestamp", pl.col("demand_actual_mw")),
                "silver",
                DatasetName.DEMAND_ACTUAL,
            )
        if "demand_forecast_mw" in demand_wide.columns:
            self.store.write(
                demand_wide.select("timestamp", pl.col("demand_forecast_mw")),
                "silver",
                DatasetName.DEMAND_FORECAST,
            )

        self.store.write(generation, "silver", DatasetName.GENERATION_MIX_DAILY)

        aggregated_weather = (
            weather.group_by("timestamp")
            .agg(
                pl.col("temperature_c").mean(),
                pl.col("relative_humidity_pct").mean(),
                pl.col("wind_speed_kmh").mean(),
                pl.col("shortwave_radiation_wm2").mean(),
            )
            .sort("timestamp")
        )
        self.store.write(aggregated_weather, "silver", DatasetName.WEATHER_HOURLY)

    def _materialize_gold(self) -> None:
        pvpc_path = self.store.path("silver", DatasetName.PVPC_HOURLY)
        spot_path = self.store.path("silver", DatasetName.SPOT_HOURLY)
        demand_actual_path = self.store.path("silver", DatasetName.DEMAND_ACTUAL)
        demand_forecast_path = self.store.path("silver", DatasetName.DEMAND_FORECAST)
        weather_path = self.store.path("silver", DatasetName.WEATHER_HOURLY)

        generation = self.store.read("silver", DatasetName.GENERATION_MIX_DAILY)
        generation_pivot = generation.pivot(index="day", on="technology", values="generation_mwh", aggregate_function="first")
        generation_pivot = generation_pivot.with_columns(pl.col("day").cast(pl.Date))
        generation_pivot = generation_pivot.with_columns(
            _safe_numeric(generation_pivot, "Eólica").alias("wind_generation_mwh"),
            _safe_numeric(generation_pivot, "Solar fotovoltaica").alias("solar_generation_mwh"),
            _safe_numeric(generation_pivot, "Generación total").alias("total_generation_mwh"),
        )
        self.store.write(generation_pivot, "gold", DatasetName.GENERATION_MIX_PIVOT_DAILY)

        generation_daily_path = self.store.path("gold", DatasetName.GENERATION_MIX_PIVOT_DAILY)
        query = f"""
            WITH base AS (
                SELECT
                    pvpc.timestamp,
                    pvpc.price_eur_mwh AS pvpc_eur_mwh,
                    spot.price_eur_mwh AS spot_eur_mwh,
                    da.demand_actual_mw,
                    df.demand_forecast_mw,
                    weather.temperature_c,
                    weather.relative_humidity_pct,
                    weather.wind_speed_kmh,
                    weather.shortwave_radiation_wm2,
                    CAST(pvpc.timestamp AS DATE) AS day
                FROM read_parquet('{pvpc_path}') AS pvpc
                LEFT JOIN read_parquet('{spot_path}') AS spot USING(timestamp)
                LEFT JOIN read_parquet('{demand_actual_path}') AS da USING(timestamp)
                LEFT JOIN read_parquet('{demand_forecast_path}') AS df USING(timestamp)
                LEFT JOIN read_parquet('{weather_path}') AS weather USING(timestamp)
            )
            SELECT
                base.*,
                EXTRACT(HOUR FROM timestamp) AS hour_of_day,
                EXTRACT(DOW FROM timestamp) AS day_of_week,
                EXTRACT(MONTH FROM timestamp) AS month_of_year,
                EXTRACT(WEEK FROM timestamp) AS week_of_year,
                CASE WHEN EXTRACT(DOW FROM timestamp) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
                gen.wind_generation_mwh,
                gen.solar_generation_mwh,
                gen.total_generation_mwh
            FROM base
            LEFT JOIN read_parquet('{generation_daily_path}') AS gen
              ON base.day = gen.day
            ORDER BY timestamp
        """
        feature_snapshot = pl.from_arrow(duckdb.sql(query).arrow())
        feature_snapshot = self._enrich_feature_frame(feature_snapshot, drop_training_nulls=True)
        self.store.write(feature_snapshot, "gold", DatasetName.FEATURE_SNAPSHOT_HOURLY)
        self.store.write(feature_snapshot, "gold", DatasetName.TRAINING_DATASET)

    def _enrich_feature_frame(self, frame: pl.DataFrame, drop_training_nulls: bool) -> pl.DataFrame:
        if frame.is_empty():
            return frame

        spanish_holidays = holidays.country_holidays("ES")
        holiday_dates = [day for day in spanish_holidays if frame["timestamp"].min().date() <= day <= frame["timestamp"].max().date()]
        holiday_frame = pl.DataFrame({"day": holiday_dates, "is_holiday": [True] * len(holiday_dates)}) if holiday_dates else pl.DataFrame({"day": [], "is_holiday": []}, schema={"day": pl.Date, "is_holiday": pl.Boolean})

        enriched = (
            frame.sort("timestamp")
            .with_columns(
                pl.col("timestamp").dt.date().alias("day"),
                pl.col("timestamp").dt.hour().cast(pl.Int16).alias("hour_of_day"),
                pl.col("timestamp").dt.strftime("%w").cast(pl.Int16).alias("day_of_week"),
                pl.col("timestamp").dt.month().cast(pl.Int16).alias("month_of_year"),
                pl.col("timestamp").dt.week().cast(pl.Int16).alias("week_of_year"),
                pl.col("timestamp").dt.strftime("%w").cast(pl.Int16).is_in([0, 6]).alias("is_weekend"),
                pl.when(pl.col("wind_generation_mwh").is_not_null() & pl.col("total_generation_mwh").is_not_null())
                .then(pl.col("wind_generation_mwh") / pl.col("total_generation_mwh"))
                .otherwise(None)
                .alias("wind_share"),
                pl.when(pl.col("solar_generation_mwh").is_not_null() & pl.col("total_generation_mwh").is_not_null())
                .then(pl.col("solar_generation_mwh") / pl.col("total_generation_mwh"))
                .otherwise(None)
                .alias("solar_share"),
            )
            .join(holiday_frame, on="day", how="left")
            .with_columns(
                pl.col("is_holiday").fill_null(False),
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
                .alias("price_regime"),
            )
        )
        if drop_training_nulls:
            enriched = enriched.drop_nulls(
                subset=[
                    "pvpc_eur_mwh",
                    "pvpc_lag_24h",
                    "pvpc_lag_168h",
                    "spot_lag_24h",
                    "pvpc_rolling_mean_24h",
                    "pvpc_rolling_std_24h",
                ]
            )
        return enriched

    def _materialize_spot_reconciliation(self, start_day: date, end_day: date) -> None:
        spot = self.store.read("silver", DatasetName.SPOT_HOURLY)
        if spot.is_empty():
            self.store.write(pl.DataFrame(), "gold", DatasetName.SPOT_RECONCILIATION)
            return

        candidate_days = []
        cursor = max(start_day, end_day - timedelta(days=13))
        while cursor <= end_day:
            candidate_days.append(cursor)
            cursor = cursor + timedelta(days=1)

        references: list[pl.DataFrame] = []
        for day_value in candidate_days:
            try:
                references.append(self.omie.fetch_day_ahead_reference(day_value))
            except Exception:
                continue

        if not references:
            self.store.write(pl.DataFrame(), "gold", DatasetName.SPOT_RECONCILIATION)
            return

        omie_reference = pl.concat(references, how="diagonal_relaxed").sort("timestamp")
        merged = (
            spot.join(omie_reference, on="timestamp", how="inner")
            .with_columns(
                (pl.col("price_eur_mwh") - pl.col("omie_spot_eur_mwh")).alias("delta_eur_mwh"),
                (pl.col("price_eur_mwh") - pl.col("omie_spot_eur_mwh")).abs().alias("absolute_delta_eur_mwh"),
            )
            .sort("timestamp")
        )
        self.store.write(merged, "gold", DatasetName.SPOT_RECONCILIATION)
