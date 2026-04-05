from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class DatasetName(StrEnum):
    PVPC_HOURLY = "pvpc_hourly"
    SPOT_HOURLY = "spot_hourly"
    DEMAND_ACTUAL = "demand_actual"
    DEMAND_FORECAST = "demand_forecast"
    GENERATION_MIX_DAILY = "generation_mix_daily"
    WEATHER_HOURLY = "weather_hourly"
    FEATURE_SNAPSHOT_HOURLY = "feature_snapshot_hourly"
    TRAINING_DATASET = "training_dataset"
    SERVING_SNAPSHOT = "serving_snapshot"
    PUBLISHED_FORECAST_SNAPSHOT = "published_forecast_snapshot"
    GENERATION_MIX_PIVOT_DAILY = "generation_mix_pivot_daily"
    FORECAST_EXPLANATIONS = "forecast_explanations"
    BACKTEST_SUMMARY = "backtest_summary"
    SOURCE_HEALTH_SNAPSHOT = "source_health_snapshot"
    SPOT_RECONCILIATION = "spot_reconciliation"


@dataclass(slots=True)
class SourceFreshness:
    name: str
    last_observed_at: datetime | None
    status: str
    detail: str | None = None
    freshness_hours: float | None = None
    row_count: int | None = None
    null_rate: float | None = None
    metrics_json: dict | None = None


@dataclass(slots=True)
class DriverImpact:
    name: str
    score: float
    direction: str
