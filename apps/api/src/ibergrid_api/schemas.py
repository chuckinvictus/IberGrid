from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field


class DriverImpactResponse(BaseModel):
    name: str
    score: float
    direction: str


class LocalExplanationResponse(BaseModel):
    confidence: str | None = None
    positive_drivers: list[DriverImpactResponse] | list[dict[str, Any]] = Field(default_factory=list)
    negative_drivers: list[DriverImpactResponse] | list[dict[str, Any]] = Field(default_factory=list)


class SourceHealthResponse(BaseModel):
    name: str
    last_observed_at: datetime | None = None
    status: str
    detail: str | None = None
    freshness_hours: float | None = None
    row_count: int | None = None
    null_rate: float | None = None
    metrics_json: dict[str, Any] | None = None


class HourlyForecastPoint(BaseModel):
    timestamp: datetime
    p10: float
    p50: float
    p90: float
    risk_level: str
    relative_cheapness_score: float
    savings_vs_daily_mean: float
    utility: dict[str, Any] | None = None
    local_explanations: dict[str, Any] | LocalExplanationResponse | None = None


class DayAheadResponse(BaseModel):
    forecast_run_id: int
    forecast: list[HourlyForecastPoint]
    history: list[dict[str, Any]]
    best_hours: list[int]
    worst_hours: list[int]
    metadata: dict[str, Any]


class WeekAheadBand(BaseModel):
    day: date
    mean_p10: float
    mean_p50: float
    mean_p90: float
    min_p50: float
    max_p50: float
    risk_level: str
    relative_cheapness_score: float
    aggregate_savings_signal: float


class WeekAheadResponse(BaseModel):
    daily_bands: list[WeekAheadBand]
    cheapest_windows: list[dict[str, Any]]
    weekly_explanations: list[dict[str, Any]]
    metadata: dict[str, Any]


class MarketContextResponse(BaseModel):
    hourly: list[dict[str, Any]]
    generation_mix_daily: list[dict[str, Any]]
    source_health: list[dict[str, Any]] | list[SourceHealthResponse]


class PerformanceResponse(BaseModel):
    benchmarks: list[dict[str, Any]]
    calibration: dict[str, float]
    latest_backtest_curve: list[dict[str, Any]]
    champion_model: dict[str, Any]
    last_promotion_decision: str
    source_health: list[dict[str, Any]] | list[SourceHealthResponse]


class StatusResponse(BaseModel):
    latest_ingestion: dict[str, Any] | None = None
    latest_training: dict[str, Any] | None = None
    latest_forecast: dict[str, Any] | None = None
    latest_model: dict[str, Any]
    source_health: list[dict[str, Any]] | list[SourceHealthResponse]
    serving_mode: str
