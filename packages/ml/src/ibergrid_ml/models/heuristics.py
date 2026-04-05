from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import numpy as np
import polars as pl

from ibergrid_ml.time import ensure_madrid


@dataclass(slots=True)
class HeuristicQuantileForecaster:
    calibration_window_hours: int = 24 * 45

    def forecast(self, feature_frame: pl.DataFrame, start_at: datetime, horizon_hours: int = 168) -> pl.DataFrame:
        start_at = ensure_madrid(start_at).replace(minute=0, second=0, microsecond=0)
        history = feature_frame.sort("timestamp")
        caches = {
            column: {
                ensure_madrid(timestamp).isoformat(): float(value)
                for timestamp, value in zip(history["timestamp"].to_list(), history[column].to_list(), strict=False)
                if value is not None
            }
            for column in (
                "pvpc_eur_mwh",
                "demand_actual_mw",
                "shortwave_radiation_wm2",
                "wind_speed_kmh",
            )
        }
        rows: list[dict[str, object]] = []
        hourly_band = self._hourly_residual_band(history)
        for step in range(horizon_hours):
            timestamp = start_at + timedelta(hours=step)
            lag_24 = self._lookup(history, caches, timestamp - timedelta(hours=24), "pvpc_eur_mwh")
            lag_168 = self._lookup(history, caches, timestamp - timedelta(hours=168), "pvpc_eur_mwh")
            demand_ref = self._lookup(history, caches, timestamp - timedelta(hours=24), "demand_actual_mw")
            solar_ref = self._lookup(history, caches, timestamp - timedelta(hours=24), "shortwave_radiation_wm2")
            wind_ref = self._lookup(history, caches, timestamp - timedelta(hours=24), "wind_speed_kmh")

            base = np.average([lag_24, lag_168], weights=[0.62, 0.38])
            demand_adjustment = 0.0012 * (demand_ref - history["demand_actual_mw"].tail(24 * 14).mean())
            solar_adjustment = -0.008 * max((solar_ref or 0) - 180, 0)
            wind_adjustment = -0.018 * max((wind_ref or 0) - 18, 0)
            median = float(base + demand_adjustment + solar_adjustment + wind_adjustment)
            hour_band = hourly_band.get(timestamp.hour, 18.0)
            weekly_multiplier = 1.0 + (0.18 if step >= 24 else 0.0) + (0.24 if step >= 72 else 0.0)
            spread = hour_band * weekly_multiplier
            rows.append(
                {
                    "timestamp": timestamp,
                    "p10": round(median - 0.9 * spread, 2),
                    "p50": round(median, 2),
                    "p90": round(median + 1.15 * spread, 2),
                    "risk_level": self._risk_label(spread),
                }
            )
        return pl.DataFrame(rows)

    def _lookup(
        self,
        feature_frame: pl.DataFrame,
        caches: dict[str, dict[str, float]],
        timestamp: datetime,
        column: str,
    ) -> float:
        fallback = float(feature_frame[column].drop_nulls().tail(24 * 7).mean())
        return caches.get(column, {}).get(ensure_madrid(timestamp).isoformat(), fallback)

    def _hourly_residual_band(self, feature_frame: pl.DataFrame) -> dict[int, float]:
        recent = feature_frame.tail(self.calibration_window_hours).with_columns(
            (pl.col("pvpc_eur_mwh") - (0.6 * pl.col("pvpc_lag_24h") + 0.4 * pl.col("pvpc_lag_168h"))).alias("residual")
        )
        stats = recent.group_by(pl.col("timestamp").dt.hour().alias("hour")).agg(pl.col("residual").std().alias("std"))
        return {row["hour"]: max(float(row["std"] or 10.0), 8.0) for row in stats.iter_rows(named=True)}

    @staticmethod
    def _risk_label(spread: float) -> str:
        if spread < 18:
            return "low"
        if spread < 30:
            return "medium"
        return "high"
