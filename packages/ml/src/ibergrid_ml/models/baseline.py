from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import polars as pl


@dataclass(slots=True)
class SeasonalNaiveForecaster:
    seasonal_hours: int
    label: str

    def predict(self, feature_frame: pl.DataFrame, horizon: int) -> pl.DataFrame:
        future = feature_frame.tail(horizon).select("timestamp", "pvpc_eur_mwh").rename({"pvpc_eur_mwh": "actual"})
        source = feature_frame.select("timestamp", "pvpc_eur_mwh")
        predictions: list[dict[str, float | object]] = []
        series = {row["timestamp"]: row["pvpc_eur_mwh"] for row in source.iter_rows(named=True)}
        for timestamp in future["timestamp"]:
            anchor = timestamp - timedelta(hours=self.seasonal_hours)
            value = series.get(anchor)
            predictions.append({"timestamp": timestamp, "prediction": value})
        return pl.DataFrame(predictions)


def seasonal_blend(feature_frame: pl.DataFrame, horizon: int) -> pl.DataFrame:
    target = feature_frame.tail(horizon).select("timestamp", "pvpc_eur_mwh")
    blended = target.with_columns(
        (0.6 * pl.col("pvpc_eur_mwh").shift(24) + 0.4 * pl.col("pvpc_eur_mwh").shift(168)).alias("prediction")
    )
    return blended.select("timestamp", "prediction")
