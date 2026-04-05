from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ibergrid_ml.persistence import Base


class IngestionRun(Base):
    __tablename__ = "ingestion_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32))
    start_day: Mapped[date] = mapped_column(Date)
    end_day: Mapped[date] = mapped_column(Date)
    source_summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    detail_json: Mapped[dict] = mapped_column(JSON, default=dict)

    source_snapshots: Mapped[list["SourceHealthSnapshot"]] = relationship(back_populates="ingestion_run")


class TrainingRun(Base):
    __tablename__ = "training_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32))
    train_start: Mapped[date] = mapped_column(Date)
    train_end: Mapped[date] = mapped_column(Date)
    validation_start: Mapped[date] = mapped_column(Date)
    validation_end: Mapped[date] = mapped_column(Date)
    test_start: Mapped[date] = mapped_column(Date)
    test_end: Mapped[date] = mapped_column(Date)
    mlflow_run_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    champion_decision: Mapped[str] = mapped_column(String(32), default="pending")
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)

    model_versions: Mapped[list["ModelVersion"]] = relationship(back_populates="training_run")


class ModelVersion(Base):
    __tablename__ = "model_version"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    training_run_id: Mapped[int | None] = mapped_column(ForeignKey("training_run.id"), nullable=True)
    version: Mapped[str] = mapped_column(String(64), unique=True)
    model_type: Mapped[str] = mapped_column(String(64))
    artifact_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict] = mapped_column(JSON, default=dict)
    explanation_json: Mapped[dict] = mapped_column(JSON, default=dict)
    promotion_summary_json: Mapped[dict] = mapped_column(JSON, default=dict)
    is_promoted: Mapped[bool] = mapped_column(Boolean, default=False)
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    training_run: Mapped["TrainingRun | None"] = relationship(back_populates="model_versions")
    forecast_runs: Mapped[list["ForecastRun"]] = relationship(back_populates="model_version")


class ForecastRun(Base):
    __tablename__ = "forecast_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    model_version_id: Mapped[int | None] = mapped_column(ForeignKey("model_version.id"), nullable=True)
    serving_mode: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    target_start: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    target_end: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    publish_day: Mapped[date] = mapped_column(Date)
    fallback_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)

    model_version: Mapped["ModelVersion | None"] = relationship(back_populates="forecast_runs")
    points: Mapped[list["ForecastPoint"]] = relationship(back_populates="forecast_run")
    explanations: Mapped[list["ForecastExplanation"]] = relationship(back_populates="forecast_run")


class ForecastPoint(Base):
    __tablename__ = "forecast_point"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    forecast_run_id: Mapped[int] = mapped_column(ForeignKey("forecast_run.id"))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    p10: Mapped[float] = mapped_column(Float)
    p50: Mapped[float] = mapped_column(Float)
    p90: Mapped[float] = mapped_column(Float)
    risk_level: Mapped[str] = mapped_column(String(32))
    relative_cheapness_score: Mapped[float] = mapped_column(Float)
    savings_vs_daily_mean: Mapped[float] = mapped_column(Float)
    utility_json: Mapped[dict] = mapped_column(JSON, default=dict)

    forecast_run: Mapped["ForecastRun"] = relationship(back_populates="points")


class ForecastExplanation(Base):
    __tablename__ = "forecast_explanation"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    forecast_run_id: Mapped[int] = mapped_column(ForeignKey("forecast_run.id"))
    timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    horizon_bucket: Mapped[str] = mapped_column(String(64))
    explanation_scope: Mapped[str] = mapped_column(String(32))
    confidence: Mapped[str] = mapped_column(String(32))
    positive_drivers_json: Mapped[list] = mapped_column(JSON, default=list)
    negative_drivers_json: Mapped[list] = mapped_column(JSON, default=list)

    forecast_run: Mapped["ForecastRun"] = relationship(back_populates="explanations")


class BacktestResult(Base):
    __tablename__ = "backtest_result"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    training_run_id: Mapped[int] = mapped_column(ForeignKey("training_run.id"))
    model_name: Mapped[str] = mapped_column(String(64))
    slice_name: Mapped[str] = mapped_column(String(64))
    mae: Mapped[float] = mapped_column(Float)
    rmse: Mapped[float] = mapped_column(Float)
    smape: Mapped[float] = mapped_column(Float)
    quantile_loss_p10: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantile_loss_p50: Mapped[float | None] = mapped_column(Float, nullable=True)
    quantile_loss_p90: Mapped[float | None] = mapped_column(Float, nullable=True)
    coverage_p10_p90: Mapped[float | None] = mapped_column(Float, nullable=True)
    cheapest_window_hit_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    summary_json: Mapped[dict] = mapped_column(JSON, default=dict)


class SourceHealthSnapshot(Base):
    __tablename__ = "source_health_snapshot"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ingestion_run_id: Mapped[int | None] = mapped_column(ForeignKey("ingestion_run.id"), nullable=True)
    source_name: Mapped[str] = mapped_column(String(64))
    observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(String(32))
    freshness_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    row_count: Mapped[int] = mapped_column(Integer, default=0)
    null_rate: Mapped[float] = mapped_column(Float, default=0.0)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[dict] = mapped_column(JSON, default=dict)

    ingestion_run: Mapped["IngestionRun | None"] = relationship(back_populates="source_snapshots")

