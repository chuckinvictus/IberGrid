from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import polars as pl


def mae(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.mean(np.abs(actual - predicted)))


def rmse(actual: np.ndarray, predicted: np.ndarray) -> float:
    return float(np.sqrt(np.mean((actual - predicted) ** 2)))


def smape(actual: np.ndarray, predicted: np.ndarray) -> float:
    denominator = np.abs(actual) + np.abs(predicted)
    denominator = np.where(denominator == 0, 1.0, denominator)
    return float(100.0 * np.mean(2.0 * np.abs(predicted - actual) / denominator))


def quantile_loss(actual: np.ndarray, predicted: np.ndarray, quantile: float) -> float:
    errors = actual - predicted
    return float(np.mean(np.maximum(quantile * errors, (quantile - 1) * errors)))


def interval_coverage(actual: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> float:
    within = (actual >= lower) & (actual <= upper)
    return float(np.mean(within))


def cheapest_window_hit_rate(frame: pl.DataFrame, actual_col: str, predicted_col: str, top_k: int = 3) -> float:
    hits: list[float] = []
    with_day = frame.with_columns(pl.col("timestamp").dt.date().alias("day"))
    for _, daily in with_day.group_by("day"):
        actual_hours = set(daily.sort(actual_col).head(top_k)["timestamp"].dt.hour().to_list())
        predicted_hours = set(daily.sort(predicted_col).head(top_k)["timestamp"].dt.hour().to_list())
        hits.append(len(actual_hours & predicted_hours) / top_k)
    return float(np.mean(hits)) if hits else 0.0


@dataclass(slots=True)
class BenchmarkScore:
    name: str
    mae: float
    rmse: float
    smape: float
    quantile_loss_p10: float | None = None
    quantile_loss_p50: float | None = None
    quantile_loss_p90: float | None = None
    cheapest_window_hit_rate: float | None = None
