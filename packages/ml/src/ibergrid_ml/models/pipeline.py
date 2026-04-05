from __future__ import annotations

import json
import pickle
from contextlib import nullcontext
from dataclasses import asdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from sklearn.linear_model import Ridge

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.data.feature_builder import FeatureBuilder
from ibergrid_ml.data.store import LakehouseStore
from ibergrid_ml.db_models import BacktestResult, ForecastExplanation, ForecastPoint, SourceHealthSnapshot
from ibergrid_ml.evaluation.metrics import (
    cheapest_window_hit_rate,
    interval_coverage,
    mae,
    quantile_loss,
    rmse,
    smape,
)
from ibergrid_ml.logging import configure_logging, get_logger
from ibergrid_ml.models.heuristics import HeuristicQuantileForecaster
from ibergrid_ml.models.tft import TFTTrainer
from ibergrid_ml.persistence import Base, get_engine, session_scope
from ibergrid_ml.repositories import Repository
from ibergrid_ml.schemas import DatasetName, DriverImpact, SourceFreshness
from ibergrid_ml.time import MADRID, ensure_madrid, start_of_day


FEATURE_COLUMNS = [
    "hour_of_day",
    "day_of_week",
    "month_of_year",
    "week_of_year",
    "is_weekend",
    "is_holiday",
    "demand_forecast_mw",
    "temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "shortwave_radiation_wm2",
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
]

LOCAL_EXPLANATION_WEIGHTS = {
    "pvpc_lag_24h": 1.0,
    "pvpc_lag_168h": 0.6,
    "spot_lag_24h": 0.8,
    "demand_forecast_mw": 0.75,
    "temperature_c": 0.25,
    "wind_speed_kmh": -0.55,
    "shortwave_radiation_wm2": -0.65,
    "pvpc_rolling_std_24h": 0.5,
}


@dataclass(slots=True)
class ModelBundle:
    name: str
    p10: np.ndarray
    p50: np.ndarray
    p90: np.ndarray
    curve: list[dict[str, Any]]
    actual: np.ndarray | None = None
    aligned_frame: pl.DataFrame | None = None


@dataclass(slots=True)
class ProductionPipeline:
    settings: ForecastSettings
    builder: FeatureBuilder
    store: LakehouseStore
    heuristic: HeuristicQuantileForecaster
    lightgbm_models: dict[float, Any] | None = None

    @classmethod
    def from_settings(cls, settings: ForecastSettings) -> "ProductionPipeline":
        configure_logging()
        builder = FeatureBuilder.from_settings(settings)
        return cls(
            settings=settings,
            builder=builder,
            store=builder.store,
            heuristic=HeuristicQuantileForecaster(),
        )

    def ensure_schema(self) -> None:
        Base.metadata.create_all(bind=get_engine())

    def ensure_training_history(self) -> None:
        training = self.store.read("gold", DatasetName.TRAINING_DATASET)
        if training.is_empty():
            end_day = datetime.now(MADRID).date()
            start_day = end_day - timedelta(days=self.settings.training_lookback_days)
            self.run_ingestion(start_day, end_day)
            return

        observed_days = (training["timestamp"].max().date() - training["timestamp"].min().date()).days
        if observed_days < min(self.settings.training_lookback_days, 80):
            end_day = datetime.now(MADRID).date()
            start_day = end_day - timedelta(days=self.settings.training_lookback_days)
            self.run_ingestion(start_day, end_day)

    def run_ingestion(self, start_day: date, end_day: date) -> dict[str, Any]:
        self.ensure_schema()
        logger = get_logger("ibergrid.ingestion")
        with session_scope() as session:
            repo = Repository(session)
            run = repo.create_ingestion_run(start_day, end_day)
            try:
                logger.info("ingestion_started", extra={"run_id": run.id, "start_day": start_day.isoformat(), "end_day": end_day.isoformat()})
                self.builder.backfill_range(start_day, end_day)
                snapshots = self._source_health_rows()
                repo.replace_source_snapshots(run.id, snapshots)
                summary = {row.source_name: row.status for row in snapshots}
                detail = {
                    row.source_name: {
                        "freshness_hours": row.freshness_hours,
                        "row_count": row.row_count,
                        "null_rate": row.null_rate,
                    }
                    for row in snapshots
                }
                repo.finish_ingestion_run(run, "success", summary, detail)
                self.store.write(
                    pl.DataFrame(
                        [
                            {
                                "source_name": row.source_name,
                                "observed_at": row.observed_at,
                                "status": row.status,
                                "freshness_hours": row.freshness_hours,
                                "row_count": row.row_count,
                                "null_rate": row.null_rate,
                                "detail": row.detail,
                            }
                            for row in snapshots
                        ]
                    ),
                    "gold",
                    DatasetName.SOURCE_HEALTH_SNAPSHOT,
                )
                return {"status": "success", "ingestion_run_id": run.id, "source_health": summary}
            except Exception as exc:
                repo.finish_ingestion_run(run, "failed", {"error": "ingestion_failed"}, {"message": str(exc)})
                logger.exception("ingestion_failed", extra={"run_id": run.id})
                raise

    def run_omie_reconciliation(self, start_day: date, end_day: date) -> dict[str, Any]:
        logger = get_logger("ibergrid.reconciliation")
        logger.info(
            "omie_reconciliation_started",
            extra={"start_day": start_day.isoformat(), "end_day": end_day.isoformat()},
        )
        frame = self.builder.refresh_spot_reconciliation(start_day, end_day)
        summary = {
            "status": "success" if not frame.is_empty() else "missing",
            "rows": frame.height,
            "mean_absolute_delta_eur_mwh": (
                float(frame["absolute_delta_eur_mwh"].mean()) if "absolute_delta_eur_mwh" in frame.columns and frame.height > 0 else None
            ),
        }
        logger.info("omie_reconciliation_completed", extra=summary)
        return summary

    def train_and_promote(self) -> dict[str, Any]:
        self.ensure_schema()
        self.ensure_training_history()
        logger = get_logger("ibergrid.training")
        frame = self.store.read("gold", DatasetName.TRAINING_DATASET).sort("timestamp")
        train_frame, validation_frame, test_frame = self._split_frame(frame)

        with session_scope() as session:
            repo = Repository(session)
            training_run = repo.create_training_run(
                train_start=train_frame["timestamp"].min().date(),
                train_end=train_frame["timestamp"].max().date(),
                validation_start=validation_frame["timestamp"].min().date(),
                validation_end=validation_frame["timestamp"].max().date(),
                test_start=test_frame["timestamp"].min().date(),
                test_end=test_frame["timestamp"].max().date(),
            )
            logger.info("training_started", extra={"run_id": training_run.id})
            mlflow_run_id: str | None = None
            try:
                with self._mlflow_run(training_run.id) as active_run:
                    if active_run is not None:
                        mlflow_run_id = active_run.info.run_id
                    benchmark_rows, summary, model_version_payload = self._fit_and_evaluate_models(
                        training_run.id, train_frame, validation_frame, test_frame, active_run
                    )
                    repo.replace_backtest_results(training_run.id, benchmark_rows)
                    self.store.write(
                        pl.DataFrame([row.summary_json | {"model_name": row.model_name, "slice_name": row.slice_name} for row in benchmark_rows]),
                        "gold",
                        DatasetName.BACKTEST_SUMMARY,
                    )
                    champion_decision = "rejected"
                    if model_version_payload is not None:
                        model_version = repo.create_model_version(
                            version=model_version_payload["version"],
                            model_type=model_version_payload["model_type"],
                            artifact_path=model_version_payload["artifact_path"],
                            metrics_json=model_version_payload["metrics_json"],
                            explanation_json=model_version_payload["explanation_json"],
                            promotion_summary_json=model_version_payload["promotion_summary_json"],
                            training_run_id=training_run.id,
                            is_promoted=model_version_payload["is_promoted"],
                        )
                        if model_version_payload["is_promoted"]:
                            repo.promote_model_version(model_version.id)
                            champion_decision = "promoted"
                        else:
                            champion_decision = "held_out"
                    repo.finish_training_run(
                        training_run,
                        status="success",
                        champion_decision=champion_decision,
                        summary=summary,
                        mlflow_run_id=mlflow_run_id,
                    )
                    logger.info("training_completed", extra={"run_id": training_run.id, "decision": champion_decision})
                    return {
                        "status": "success",
                        "training_run_id": training_run.id,
                        "champion_decision": champion_decision,
                        "summary": summary,
                    }
            except Exception as exc:
                repo.finish_training_run(
                    training_run,
                    status="failed",
                    champion_decision="failed",
                    summary={"message": str(exc)},
                    mlflow_run_id=mlflow_run_id,
                )
                logger.exception("training_failed", extra={"run_id": training_run.id})
                raise

    def publish_forecast(self, publish_day: date | None = None) -> dict[str, Any]:
        self.ensure_schema()
        self.ensure_training_history()
        logger = get_logger("ibergrid.publication")
        publish_day = publish_day or datetime.now(MADRID).date()
        target_day = publish_day + timedelta(days=self.settings.forecast_origin_offset_days)
        target_start = start_of_day(target_day)
        target_end = target_start + timedelta(hours=self.settings.horizon_hours - 1)
        serving_snapshot = self.builder.build_serving_snapshot(target_start, self.settings.horizon_hours)
        source_health = self.source_health()
        if any(item.status in {"missing", "stale"} for item in source_health if item.name.startswith("redata")):
            raise RuntimeError("REData is unavailable; publication is blocked.")
        explanation_confidence = self._forecast_explanation_confidence(source_health)

        with session_scope() as session:
            repo = Repository(session)
            promoted = repo.get_promoted_model()
            if promoted is None:
                forecast = self.heuristic.forecast(
                    self.store.read("gold", DatasetName.TRAINING_DATASET).sort("timestamp"),
                    target_start,
                    horizon_hours=self.settings.horizon_hours,
                )
                serving_mode = "heuristic-fallback"
                fallback_reason = "No promoted TFT model is available."
                model_version_id = None
                global_importance = {}
            else:
                if promoted.model_type != "tft":
                    raise RuntimeError(f"Unsupported promoted model type: {promoted.model_type}")
                forecast = self._predict_tft(serving_snapshot, Path(promoted.artifact_path or ""))
                serving_mode = "persisted-tft"
                fallback_reason = None
                model_version_id = promoted.id
                global_importance = promoted.explanation_json.get("global_importance", {})

            utility = self._utility_metrics(forecast)
            explanations = self._build_forecast_explanations(
                serving_snapshot=serving_snapshot,
                forecast=utility,
                confidence=explanation_confidence,
                global_importance=global_importance,
            )
            with_model = utility.with_columns(
                pl.col("timestamp").dt.date().alias("day"),
            )
            metadata = {
                "forecast_day": target_day.isoformat(),
                "generated_at": datetime.now(MADRID).isoformat(),
                "source_health": [asdict(item) for item in source_health],
                "source_health_state": self._overall_source_status(source_health),
                "degradation_notes": self._degradation_notes(source_health),
                "explanation_confidence": explanation_confidence,
            }
            forecast_run = repo.create_forecast_run(
                publish_day=publish_day,
                target_start=target_start,
                target_end=target_end,
                serving_mode=serving_mode,
                status="published",
                metadata_json=self._json_ready(metadata),
                model_version_id=model_version_id,
                fallback_reason=fallback_reason,
            )
            points = [
                ForecastPoint(
                    forecast_run_id=forecast_run.id,
                    timestamp=row["timestamp"],
                    p10=float(row["p10"]),
                    p50=float(row["p50"]),
                    p90=float(row["p90"]),
                    risk_level=str(row["risk_level"]),
                    relative_cheapness_score=float(row["relative_cheapness_score"]),
                    savings_vs_daily_mean=float(row["savings_vs_daily_mean"]),
                    utility_json={
                        "is_cheapest_2h_candidate": bool(row["is_cheapest_2h_candidate"]),
                        "is_cheapest_4h_candidate": bool(row["is_cheapest_4h_candidate"]),
                        "is_peak_risk": bool(row["is_peak_risk"]),
                    },
                )
                for row in with_model.to_dicts()
            ]
            repo.replace_forecast_contents(forecast_run.id, points, explanations)
            self.store.write(with_model, "gold", DatasetName.PUBLISHED_FORECAST_SNAPSHOT)
            self.store.write(
                pl.DataFrame(
                    [
                        {
                            "timestamp": item.timestamp,
                            "horizon_bucket": item.horizon_bucket,
                            "scope": item.explanation_scope,
                            "confidence": item.confidence,
                            "positive_drivers_json": item.positive_drivers_json,
                            "negative_drivers_json": item.negative_drivers_json,
                        }
                        for item in explanations
                    ]
                ),
                "gold",
                DatasetName.FORECAST_EXPLANATIONS,
            )
            logger.info("forecast_published", extra={"forecast_run_id": forecast_run.id, "serving_mode": serving_mode})
            return {
                "status": "success",
                "forecast_run_id": forecast_run.id,
                "serving_mode": serving_mode,
                "target_day": target_day.isoformat(),
            }

    def day_ahead_payload(self, target_day: date) -> dict[str, Any]:
        with session_scope() as session:
            repo = Repository(session)
            run = repo.forecast_for_day(target_day)
            if run is None:
                raise RuntimeError("No published forecast covers the requested day.")
            points = [
                point
                for point in repo.list_forecast_points(run.id)
                if ensure_madrid(point.timestamp).date() == target_day
            ]
            explanations = [
                explanation
                for explanation in repo.list_forecast_explanations(run.id)
                if explanation.timestamp is not None and ensure_madrid(explanation.timestamp).date() == target_day
            ]
            model_version = repo.get_model_version(run.model_version_id) if run.model_version_id is not None else None

        history_start = start_of_day(target_day) - timedelta(hours=24)
        history = self.store.read("gold", DatasetName.TRAINING_DATASET).filter(
            (pl.col("timestamp") >= history_start) & (pl.col("timestamp") < start_of_day(target_day))
        )
        if not points:
            raise RuntimeError("Published run does not contain hourly points for the requested day.")

        frame = pl.DataFrame(
            [
                {
                    "timestamp": point.timestamp,
                    "p10": point.p10,
                    "p50": point.p50,
                    "p90": point.p90,
                    "risk_level": point.risk_level,
                    "relative_cheapness_score": point.relative_cheapness_score,
                    "savings_vs_daily_mean": point.savings_vs_daily_mean,
                    "utility": point.utility_json,
                }
                for point in points
            ]
        ).sort("timestamp")
        best_hours = frame.sort("p50").head(3)["timestamp"].dt.hour().to_list()
        worst_hours = frame.sort("p50", descending=True).head(3)["timestamp"].dt.hour().to_list()
        explanation_map = {
            ensure_madrid(item.timestamp).isoformat(): {
                "confidence": item.confidence,
                "positive_drivers": item.positive_drivers_json,
                "negative_drivers": item.negative_drivers_json,
            }
            for item in explanations
            if item.timestamp is not None
        }
        return {
            "forecast_run_id": run.id,
            "forecast": [
                row | {"local_explanations": explanation_map.get(ensure_madrid(row["timestamp"]).isoformat(), {})}
                for row in frame.to_dicts()
            ],
            "history": history.select("timestamp", "pvpc_eur_mwh", "spot_eur_mwh").to_dicts(),
            "best_hours": best_hours,
            "worst_hours": worst_hours,
            "metadata": {
                "generated_at": ensure_madrid(run.generated_at).isoformat(),
                "serving_mode": run.serving_mode,
                "fallback_reason": run.fallback_reason,
                "model_version": model_version.version if model_version is not None else "heuristic-fallback",
                "source_health": [asdict(item) for item in self.source_health()],
                "freshness": self.status_snapshot(),
            },
        }

    def week_ahead_payload(self, from_day: date) -> dict[str, Any]:
        with session_scope() as session:
            repo = Repository(session)
            run = repo.forecast_for_day(from_day)
            if run is None:
                raise RuntimeError("No published forecast covers the requested week.")
            points = repo.list_forecast_points(run.id)
            explanations = repo.list_forecast_explanations(run.id)
            model_version = repo.get_model_version(run.model_version_id) if run.model_version_id is not None else None

        frame = pl.DataFrame(
            [
                {
                    "timestamp": point.timestamp,
                    "p10": point.p10,
                    "p50": point.p50,
                    "p90": point.p90,
                    "risk_level": point.risk_level,
                    "relative_cheapness_score": point.relative_cheapness_score,
                    "savings_vs_daily_mean": point.savings_vs_daily_mean,
                    **point.utility_json,
                }
                for point in points
            ]
        ).filter(pl.col("timestamp").dt.date() >= from_day).sort("timestamp")

        daily = (
            frame.with_columns(pl.col("timestamp").dt.date().alias("day"))
            .group_by("day")
            .agg(
                pl.col("p10").mean().alias("mean_p10"),
                pl.col("p50").mean().alias("mean_p50"),
                pl.col("p90").mean().alias("mean_p90"),
                pl.col("p50").min().alias("min_p50"),
                pl.col("p50").max().alias("max_p50"),
                pl.col("risk_level").mode().first().alias("risk_level"),
                pl.col("relative_cheapness_score").mean().alias("relative_cheapness_score"),
                pl.col("savings_vs_daily_mean").sum().alias("aggregate_savings_signal"),
            )
            .sort("day")
        )

        cheapest_windows = []
        for day_value in daily["day"].to_list():
            daily_hours = frame.filter(pl.col("timestamp").dt.date() == day_value).sort("p50")
            two_hour = daily_hours.head(2)
            four_hour = daily_hours.head(4)
            cheapest_windows.append(
                {
                    "day": day_value.isoformat(),
                    "best_two_hour_window": two_hour["timestamp"].dt.hour().to_list(),
                    "best_four_hour_window": four_hour["timestamp"].dt.hour().to_list(),
                    "avg_two_hour_price": round(two_hour["p50"].mean(), 2),
                    "avg_four_hour_price": round(four_hour["p50"].mean(), 2),
                    "peak_risk_hours": daily_hours.sort("p50", descending=True).head(2)["timestamp"].dt.hour().to_list(),
                }
            )

        weekly_explanations = [
            {
                "horizon_bucket": item.horizon_bucket,
                "confidence": item.confidence,
                "positive_drivers": item.positive_drivers_json,
                "negative_drivers": item.negative_drivers_json,
            }
            for item in explanations
            if item.explanation_scope == "weekly_bucket"
        ]
        return {
            "daily_bands": daily.to_dicts(),
            "cheapest_windows": cheapest_windows,
            "weekly_explanations": weekly_explanations,
            "metadata": {
                "generated_at": ensure_madrid(run.generated_at).isoformat(),
                "serving_mode": run.serving_mode,
                "model_version": model_version.version if model_version is not None else "heuristic-fallback",
                "source_health": [asdict(item) for item in self.source_health()],
            },
        }

    def market_context_payload(self, start_at: datetime, end_at: datetime) -> dict[str, Any]:
        feature_frame = self.store.read("gold", DatasetName.TRAINING_DATASET).sort("timestamp")
        filtered = feature_frame.filter((pl.col("timestamp") >= start_at) & (pl.col("timestamp") <= end_at))
        generation_frame = self.store.read("silver", DatasetName.GENERATION_MIX_DAILY)
        if generation_frame.is_empty() or "day" not in generation_frame.columns:
            generation = pl.DataFrame()
        else:
            generation = generation_frame.filter((pl.col("day") >= start_at.date()) & (pl.col("day") <= end_at.date()))
        return {
            "hourly": filtered.select(
                "timestamp",
                "pvpc_eur_mwh",
                "spot_eur_mwh",
                "demand_actual_mw",
                "demand_forecast_mw",
                "temperature_c",
                "wind_speed_kmh",
                "shortwave_radiation_wm2",
            ).to_dicts(),
            "generation_mix_daily": generation.to_dicts(),
            "source_health": [asdict(item) for item in self.source_health()],
        }

    def performance_payload(self) -> dict[str, Any]:
        with session_scope() as session:
            repo = Repository(session)
            latest_training = repo.get_latest_training_run()
            latest_model = repo.get_promoted_model() or repo.get_latest_model_version()
            if latest_training is None:
                raise RuntimeError("No training run has been recorded yet.")
            backtests = repo.list_backtest_results(latest_training.id)
        benchmarks = [
            {
                "name": row.model_name,
                "slice_name": row.slice_name,
                "mae": row.mae,
                "rmse": row.rmse,
                "smape": row.smape,
                "quantile_loss_p10": row.quantile_loss_p10,
                "quantile_loss_p50": row.quantile_loss_p50,
                "quantile_loss_p90": row.quantile_loss_p90,
                "coverage_p10_p90": row.coverage_p10_p90,
                "cheapest_window_hit_rate": row.cheapest_window_hit_rate,
            }
            for row in backtests
        ]
        summary = latest_training.summary_json
        calibration = summary.get("calibration", {})
        return {
            "benchmarks": benchmarks,
            "calibration": calibration,
            "latest_backtest_curve": summary.get("latest_backtest_curve", []),
            "champion_model": {
                "version": latest_model.version if latest_model is not None else None,
                "model_type": latest_model.model_type if latest_model is not None else "heuristic-fallback",
                "promotion_summary": latest_model.promotion_summary_json if latest_model is not None else {},
            },
            "last_promotion_decision": latest_training.champion_decision,
            "source_health": [asdict(item) for item in self.source_health()],
        }

    def status_snapshot(self) -> dict[str, Any]:
        with session_scope() as session:
            repo = Repository(session)
            latest_ingestion = repo.get_latest_ingestion_run()
            latest_training = repo.get_latest_training_run()
            latest_forecast = repo.latest_forecast_run()
            latest_model = repo.get_promoted_model() or repo.get_latest_model_version()
        return {
            "latest_ingestion": self._run_summary(latest_ingestion),
            "latest_training": self._run_summary(latest_training),
            "latest_forecast": self._run_summary(latest_forecast),
            "latest_model": {
                "version": latest_model.version if latest_model is not None else None,
                "model_type": latest_model.model_type if latest_model is not None else "heuristic-fallback",
                "promoted_at": ensure_madrid(latest_model.promoted_at).isoformat() if latest_model is not None and latest_model.promoted_at is not None else None,
            },
            "source_health": [asdict(item) for item in self.source_health()],
            "serving_mode": latest_forecast.serving_mode if latest_forecast is not None else "unavailable",
        }

    def source_health(self) -> list[SourceFreshness]:
        source_rows = self.store.read("gold", DatasetName.SOURCE_HEALTH_SNAPSHOT)
        if source_rows.is_empty():
            return []
        return [
            SourceFreshness(
                name=row["source_name"],
                last_observed_at=row.get("observed_at"),
                status=row["status"],
                detail=row.get("detail"),
                freshness_hours=row.get("freshness_hours"),
                row_count=row.get("row_count"),
                null_rate=row.get("null_rate"),
                metrics_json=row.get("metrics_json"),
            )
            for row in source_rows.to_dicts()
        ]

    def _fit_and_evaluate_models(
        self,
        training_run_id: int,
        train_frame: pl.DataFrame,
        validation_frame: pl.DataFrame,
        test_frame: pl.DataFrame,
        active_mlflow_run: Any,
    ) -> tuple[list[BacktestResult], dict[str, Any], dict[str, Any] | None]:
        X_train, y_train = self._matrix(train_frame)
        X_test, y_test = self._matrix(test_frame)

        ridge_model = Ridge(alpha=1.0)
        ridge_model.fit(X_train, y_train)
        ridge_p50 = ridge_model.predict(X_test)
        residual = y_train - ridge_model.predict(X_train)
        ridge_sigma = float(np.nanstd(residual) or 8.0)
        ridge_bundle = ModelBundle(
            name="ridge",
            p10=ridge_p50 - 1.1 * ridge_sigma,
            p50=ridge_p50,
            p90=ridge_p50 + 1.1 * ridge_sigma,
            curve=self._curve_payload(test_frame, ridge_p50),
        )

        lightgbm_bundle = self._train_lightgbm(train_frame, test_frame)
        d1_bundle = ModelBundle(
            name="naive_d1",
            p10=test_frame["pvpc_lag_24h"].to_numpy() - 10.0,
            p50=test_frame["pvpc_lag_24h"].to_numpy(),
            p90=test_frame["pvpc_lag_24h"].to_numpy() + 10.0,
            curve=self._curve_payload(test_frame, test_frame["pvpc_lag_24h"].to_numpy()),
        )
        d7_bundle = ModelBundle(
            name="naive_d7",
            p10=test_frame["pvpc_lag_168h"].to_numpy() - 12.0,
            p50=test_frame["pvpc_lag_168h"].to_numpy(),
            p90=test_frame["pvpc_lag_168h"].to_numpy() + 12.0,
            curve=self._curve_payload(test_frame, test_frame["pvpc_lag_168h"].to_numpy()),
        )

        tft_bundle: ModelBundle | None = None
        tft_global_importance: dict[str, float] = {}
        tft_artifact_path: str | None = None
        if TFTTrainer.available():
            artifact_dir = self.settings.models_dir / f"training-run-{training_run_id}" / "tft"
            trainer = TFTTrainer(
                artifact_dir=artifact_dir,
                encoder_hours=self.settings.encoder_hours,
                horizon_hours=self.settings.horizon_hours,
                batch_size=self.settings.retrain_batch_size,
                num_workers=self.settings.retrain_num_workers,
                max_epochs=self.settings.retrain_epochs,
            )
            feature_frame = pl.concat([train_frame, validation_frame], how="diagonal_relaxed")
            artifact, tft_global_importance = trainer.train(feature_frame)
            tft_artifact_path = str(artifact_dir)
            tft_bundle = self._backtest_tft(trainer, pl.concat([feature_frame, test_frame], how="diagonal_relaxed"), test_frame)

        benchmark_rows = []
        bundles = [d1_bundle, d7_bundle, ridge_bundle, lightgbm_bundle]
        benchmark_frame = test_frame
        if tft_bundle is not None and tft_bundle.aligned_frame is not None:
            bundles = [self._align_bundle_to_frame(bundle, test_frame, tft_bundle.aligned_frame) for bundle in bundles]
            bundles.append(tft_bundle)
            benchmark_frame = tft_bundle.aligned_frame
        elif tft_bundle is not None:
            bundles.append(tft_bundle)
        for bundle in bundles:
            benchmark_rows.append(self._benchmark_row(training_run_id, bundle, y_test, benchmark_frame))

        calibration_bundle = tft_bundle or lightgbm_bundle
        calibration_actual = calibration_bundle.actual if calibration_bundle.actual is not None else y_test
        calibration = {
            "below_p10": float(np.mean(calibration_actual < calibration_bundle.p10)),
            "within_band": interval_coverage(calibration_actual, calibration_bundle.p10, calibration_bundle.p90),
            "above_p90": float(np.mean(calibration_actual > calibration_bundle.p90)),
        }
        latest_curve = (tft_bundle or lightgbm_bundle).curve[-24:]
        summary = {
            "calibration": calibration,
            "latest_backtest_curve": latest_curve,
            "benchmarks": [
                {
                    "name": row.model_name,
                    "mae": row.mae,
                    "rmse": row.rmse,
                    "smape": row.smape,
                    "coverage_p10_p90": row.coverage_p10_p90,
                    "cheapest_window_hit_rate": row.cheapest_window_hit_rate,
                }
                for row in benchmark_rows
            ],
        }
        if active_mlflow_run is not None:
            self._log_mlflow(summary, benchmark_rows)

        promoted_payload = None
        if tft_bundle is not None and tft_artifact_path is not None:
            tft_row = next(row for row in benchmark_rows if row.model_name == "tft")
            d1_row = next(row for row in benchmark_rows if row.model_name == "naive_d1")
            d7_row = next(row for row in benchmark_rows if row.model_name == "naive_d7")
            improvement_d1 = (d1_row.mae - tft_row.mae) / d1_row.mae
            improvement_d7 = (d7_row.mae - tft_row.mae) / d7_row.mae
            smape_improvement_d1 = (d1_row.smape - tft_row.smape) / d1_row.smape
            smape_improvement_d7 = (d7_row.smape - tft_row.smape) / d7_row.smape
            promoted = (
                improvement_d1 >= self.settings.promotion_day_ahead_mae_improvement
                and improvement_d7 >= self.settings.promotion_day_ahead_mae_improvement
                and smape_improvement_d1 >= self.settings.promotion_smape_improvement
                and smape_improvement_d7 >= self.settings.promotion_smape_improvement
                and self.settings.promotion_coverage_min <= (tft_row.coverage_p10_p90 or 0.0) <= self.settings.promotion_coverage_max
            )
            promoted_payload = {
                "version": f"tft-{training_run_id}",
                "model_type": "tft",
                "artifact_path": tft_artifact_path,
                "metrics_json": {
                    "mae": tft_row.mae,
                    "rmse": tft_row.rmse,
                    "smape": tft_row.smape,
                    "coverage_p10_p90": tft_row.coverage_p10_p90,
                },
                "explanation_json": {
                    "global_importance": tft_global_importance,
                    "feature_columns": FEATURE_COLUMNS,
                },
                "promotion_summary_json": {
                    "improvement_vs_d1_mae": improvement_d1,
                    "improvement_vs_d7_mae": improvement_d7,
                    "improvement_vs_d1_smape": smape_improvement_d1,
                    "improvement_vs_d7_smape": smape_improvement_d7,
                    "coverage_p10_p90": tft_row.coverage_p10_p90,
                },
                "is_promoted": promoted,
            }

        self._persist_auxiliary_models(training_run_id, ridge_model, lightgbm_bundle)
        return benchmark_rows, summary, promoted_payload

    def _train_lightgbm(self, train_frame: pl.DataFrame, test_frame: pl.DataFrame) -> ModelBundle:
        try:
            from lightgbm import LGBMRegressor
        except ImportError as exc:
            raise RuntimeError("lightgbm is required for the production benchmark suite.") from exc

        X_train, y_train = self._matrix(train_frame)
        X_test, _ = self._matrix(test_frame)
        params = {
            "n_estimators": 240,
            "learning_rate": 0.05,
            "num_leaves": 31,
            "subsample": 0.9,
            "colsample_bytree": 0.9,
            "min_child_samples": 16,
            "verbosity": -1,
            "random_state": 42,
        }
        models: dict[float, Any] = {}
        predictions: dict[float, np.ndarray] = {}
        for quantile in (0.1, 0.5, 0.9):
            model = LGBMRegressor(objective="quantile", alpha=quantile, **params)
            model.fit(X_train, y_train)
            models[quantile] = model
            predictions[quantile] = model.predict(X_test)
        self.lightgbm_models = models
        return ModelBundle(
            name="lightgbm_quantile",
            p10=predictions[0.1],
            p50=predictions[0.5],
            p90=predictions[0.9],
            curve=self._curve_payload(test_frame, predictions[0.5]),
        )

    def _backtest_tft(self, trainer: TFTTrainer, full_frame: pl.DataFrame, test_frame: pl.DataFrame) -> ModelBundle:
        origins = test_frame.select(pl.col("timestamp").dt.date().alias("day")).unique().sort("day")["day"].to_list()
        frames: list[pl.DataFrame] = []
        for origin_day in origins:
            origin = start_of_day(origin_day)
            history = full_frame.filter(pl.col("timestamp") < origin)
            future = (
                full_frame.filter(
                    (pl.col("timestamp") >= origin)
                    & (pl.col("timestamp") < origin + timedelta(hours=self.settings.horizon_hours))
                )
                .with_columns(
                    pl.lit(None, dtype=pl.Float64).alias("pvpc_eur_mwh"),
                    pl.lit(None, dtype=pl.Float64).alias("spot_eur_mwh"),
                    pl.lit(None, dtype=pl.Float64).alias("demand_actual_mw"),
                )
            )
            if history.is_empty() or future.height < self.settings.horizon_hours:
                continue
            combined = pl.concat([history, future], how="diagonal_relaxed").sort("timestamp")
            frames.append(trainer.predict(combined).head(24))
        if not frames:
            raise RuntimeError("TFT backtest could not generate any rolling-origin predictions.")
        prediction_frame = pl.concat(frames, how="diagonal_relaxed").sort("timestamp")
        actual = test_frame.join(prediction_frame, on="timestamp", how="inner").sort("timestamp")
        return ModelBundle(
            name="tft",
            p10=actual["p10"].to_numpy(),
            p50=actual["p50"].to_numpy(),
            p90=actual["p90"].to_numpy(),
            curve=self._curve_payload(actual, actual["p50"].to_numpy(), actual_col="pvpc_eur_mwh"),
            actual=actual["pvpc_eur_mwh"].to_numpy(),
            aligned_frame=actual.select("timestamp", "pvpc_eur_mwh"),
        )

    def _align_bundle_to_frame(
        self, bundle: ModelBundle, source_frame: pl.DataFrame, target_frame: pl.DataFrame | None
    ) -> ModelBundle:
        if target_frame is None:
            return bundle
        base_frame = (
            bundle.aligned_frame
            if bundle.aligned_frame is not None
            else source_frame.select("timestamp", "pvpc_eur_mwh").with_columns(
                pl.Series("p10", bundle.p10),
                pl.Series("p50", bundle.p50),
                pl.Series("p90", bundle.p90),
            )
        )
        aligned = (
            base_frame.join(target_frame.select("timestamp"), on="timestamp", how="inner")
            .sort("timestamp")
        )
        if aligned.is_empty():
            raise RuntimeError(f"Could not align benchmark bundle '{bundle.name}' to the TFT evaluation window.")
        p10 = aligned["p10"].to_numpy() if "p10" in aligned.columns else bundle.p10
        p50 = aligned["p50"].to_numpy() if "p50" in aligned.columns else bundle.p50
        p90 = aligned["p90"].to_numpy() if "p90" in aligned.columns else bundle.p90
        actual = aligned["pvpc_eur_mwh"].to_numpy()
        return ModelBundle(
            name=bundle.name,
            p10=p10,
            p50=p50,
            p90=p90,
            curve=self._curve_payload(aligned, p50, actual_col="pvpc_eur_mwh"),
            actual=actual,
            aligned_frame=aligned.select("timestamp", "pvpc_eur_mwh"),
        )

    def _predict_tft(self, serving_snapshot: pl.DataFrame, artifact_dir: Path) -> pl.DataFrame:
        trainer = TFTTrainer(
            artifact_dir=artifact_dir,
            encoder_hours=self.settings.encoder_hours,
            horizon_hours=self.settings.horizon_hours,
            batch_size=self.settings.retrain_batch_size,
            num_workers=self.settings.retrain_num_workers,
            max_epochs=self.settings.retrain_epochs,
        )
        scored = trainer.predict(serving_snapshot).with_columns(
            pl.when((pl.col("p90") - pl.col("p10")) < 18)
            .then(pl.lit("low"))
            .when((pl.col("p90") - pl.col("p10")) < 34)
            .then(pl.lit("medium"))
            .otherwise(pl.lit("high"))
            .alias("risk_level")
        )
        return scored

    def _persist_auxiliary_models(self, training_run_id: int, ridge_model: Ridge, lightgbm_bundle: ModelBundle) -> None:
        artifact_dir = self.settings.models_dir / f"training-run-{training_run_id}" / "auxiliary"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        with (artifact_dir / "ridge.pkl").open("wb") as handle:
            pickle.dump(ridge_model, handle)
        if self.lightgbm_models is not None:
            for quantile, model in self.lightgbm_models.items():
                model.booster_.save_model(str(artifact_dir / f"lightgbm_q{int(quantile * 100)}.txt"))
        (artifact_dir / "manifest.json").write_text(
            json.dumps({"features": FEATURE_COLUMNS, "lightgbm_name": lightgbm_bundle.name}, indent=2)
        )

    def _utility_metrics(self, forecast: pl.DataFrame) -> pl.DataFrame:
        enriched = forecast.with_columns(pl.col("timestamp").dt.date().alias("day"))
        daily_stats = enriched.group_by("day").agg(
            pl.col("p50").mean().alias("daily_mean_p50"),
            pl.col("p50").min().alias("daily_min_p50"),
            pl.col("p50").max().alias("daily_max_p50"),
        )
        enriched = enriched.join(daily_stats, on="day", how="left").with_columns(
            (
                100
                * (
                    (pl.col("daily_max_p50") - pl.col("p50"))
                    / (pl.col("daily_max_p50") - pl.col("daily_min_p50") + 1e-6)
                )
            ).alias("relative_cheapness_score"),
            (pl.col("daily_mean_p50") - pl.col("p50")).alias("savings_vs_daily_mean"),
        )

        flags: list[dict[str, Any]] = []
        for day_value in enriched["day"].unique().sort().to_list():
            day_frame = enriched.filter(pl.col("day") == day_value).sort("p50")
            cheapest_2h = set(day_frame.head(2)["timestamp"].to_list())
            cheapest_4h = set(day_frame.head(4)["timestamp"].to_list())
            riskiest = set(day_frame.sort("p50", descending=True).head(2)["timestamp"].to_list())
            for row in day_frame.to_dicts():
                flags.append(
                    {
                        "timestamp": row["timestamp"],
                        "is_cheapest_2h_candidate": row["timestamp"] in cheapest_2h,
                        "is_cheapest_4h_candidate": row["timestamp"] in cheapest_4h,
                        "is_peak_risk": row["timestamp"] in riskiest,
                    }
                )

        return enriched.join(pl.DataFrame(flags), on="timestamp", how="left").drop(
            "daily_mean_p50", "daily_min_p50", "daily_max_p50"
        )

    def _build_forecast_explanations(
        self,
        serving_snapshot: pl.DataFrame,
        forecast: pl.DataFrame,
        confidence: str,
        global_importance: dict[str, float],
    ) -> list[ForecastExplanation]:
        history = serving_snapshot.filter(pl.col("pvpc_eur_mwh").is_not_null()).tail(24 * 21)
        future = serving_snapshot.filter(pl.col("pvpc_eur_mwh").is_null()).head(self.settings.horizon_hours)
        baseline_stats = history.select(
            [pl.col(column).mean().alias(f"{column}_mean") for column in LOCAL_EXPLANATION_WEIGHTS]
        ).to_dicts()[0]
        forecast_rows = forecast.sort("timestamp").to_dicts()

        rows: list[ForecastExplanation] = []
        for row in future.to_dicts():
            positive: list[DriverImpact] = []
            negative: list[DriverImpact] = []
            for feature_name, weight in LOCAL_EXPLANATION_WEIGHTS.items():
                baseline_value = baseline_stats.get(f"{feature_name}_mean") or 0.0
                raw_value = float(row.get(feature_name) or baseline_value)
                score = (raw_value - baseline_value) * weight
                if score >= 0:
                    positive.append(DriverImpact(name=feature_name, score=score, direction="up"))
                else:
                    negative.append(DriverImpact(name=feature_name, score=abs(score), direction="down"))
            positive = sorted(positive, key=lambda item: item.score, reverse=True)[:3]
            negative = sorted(negative, key=lambda item: item.score, reverse=True)[:3]
            rows.append(
                ForecastExplanation(
                    forecast_run_id=0,
                    timestamp=row["timestamp"],
                    horizon_bucket=f"H+{int((ensure_madrid(row['timestamp']) - ensure_madrid(forecast_rows[0]['timestamp'])).total_seconds() // 3600):03d}",
                    explanation_scope="hourly",
                    confidence=confidence,
                    positive_drivers_json=[asdict(item) for item in positive],
                    negative_drivers_json=[asdict(item) for item in negative],
                )
            )

        if global_importance:
            ordered = sorted(global_importance.items(), key=lambda item: item[1], reverse=True)
            buckets = [
                ("day_1", 0, 24),
                ("days_2_3", 24, 72),
                ("days_4_7", 72, self.settings.horizon_hours),
            ]
            for name, lower, upper in buckets:
                rows.append(
                    ForecastExplanation(
                        forecast_run_id=0,
                        timestamp=None,
                        horizon_bucket=name,
                        explanation_scope="weekly_bucket",
                        confidence=confidence,
                        positive_drivers_json=[
                            {"name": key, "score": value, "direction": "up"}
                            for key, value in ordered[:3]
                        ],
                        negative_drivers_json=[
                            {"name": key, "score": value, "direction": "down"}
                            for key, value in ordered[-3:]
                        ],
                    )
                )
        return rows

    def _benchmark_row(
        self, training_run_id: int, bundle: ModelBundle, actual: np.ndarray, frame: pl.DataFrame
    ) -> BacktestResult:
        benchmark_actual = bundle.actual if bundle.actual is not None else actual
        benchmark_frame = bundle.aligned_frame if bundle.aligned_frame is not None else frame.select("timestamp", "pvpc_eur_mwh")
        predicted_frame = benchmark_frame.with_columns(
            pl.Series("predicted", bundle.p50)
        )
        return BacktestResult(
            training_run_id=training_run_id,
            model_name=bundle.name,
            slice_name="day_ahead_locked",
            mae=mae(benchmark_actual, bundle.p50),
            rmse=rmse(benchmark_actual, bundle.p50),
            smape=smape(benchmark_actual, bundle.p50),
            quantile_loss_p10=quantile_loss(benchmark_actual, bundle.p10, 0.1),
            quantile_loss_p50=quantile_loss(benchmark_actual, bundle.p50, 0.5),
            quantile_loss_p90=quantile_loss(benchmark_actual, bundle.p90, 0.9),
            coverage_p10_p90=interval_coverage(benchmark_actual, bundle.p10, bundle.p90),
            cheapest_window_hit_rate=cheapest_window_hit_rate(
                predicted_frame, "pvpc_eur_mwh", "predicted", top_k=3
            ),
            summary_json={
                "curve": bundle.curve,
            },
        )

    def _matrix(self, frame: pl.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        matrix = (
            frame.select(
                [
                    pl.col(column).cast(pl.Float64).fill_null(strategy="forward").fill_null(0.0).alias(column)
                    if frame.schema[column] != pl.Boolean
                    else pl.col(column).cast(pl.Int8).cast(pl.Float64).fill_null(0.0).alias(column)
                    for column in FEATURE_COLUMNS
                ]
            )
            .to_numpy()
        )
        target = frame["pvpc_eur_mwh"].to_numpy()
        return matrix, target

    def _curve_payload(
        self, frame: pl.DataFrame, prediction: np.ndarray, actual_col: str = "pvpc_eur_mwh"
    ) -> list[dict[str, Any]]:
        timestamps = frame["timestamp"].to_list()
        actual = frame[actual_col].to_list()
        return [
            {
                "timestamp": ensure_madrid(timestamp).isoformat() if isinstance(timestamp, datetime) else timestamp,
                "actual": actual_value,
                "predicted": float(predicted_value),
            }
            for timestamp, actual_value, predicted_value in zip(timestamps, actual, prediction, strict=False)
        ]

    def _split_frame(self, frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        if frame.height < 24 * 60:
            raise RuntimeError("Training dataset is too small for the locked split policy.")
        max_date = frame["timestamp"].max().date()
        test_start = max_date - timedelta(days=20)
        validation_start = test_start - timedelta(days=14)
        train_frame = frame.filter(pl.col("timestamp").dt.date() < validation_start)
        validation_frame = frame.filter(
            (pl.col("timestamp").dt.date() >= validation_start)
            & (pl.col("timestamp").dt.date() < test_start)
        )
        test_frame = frame.filter(pl.col("timestamp").dt.date() >= test_start)
        return train_frame, validation_frame, test_frame

    def _source_health_rows(self) -> list[SourceHealthSnapshot]:
        checks = [
            ("redata_pvpc", self.store.read("silver", DatasetName.PVPC_HOURLY)),
            ("redata_spot", self.store.read("silver", DatasetName.SPOT_HOURLY)),
            ("redata_demand_actual", self.store.read("silver", DatasetName.DEMAND_ACTUAL)),
            ("redata_demand_forecast", self.store.read("silver", DatasetName.DEMAND_FORECAST)),
            ("open_meteo_weather", self.store.read("silver", DatasetName.WEATHER_HOURLY)),
            ("omie_reconciliation", self.store.read("gold", DatasetName.SPOT_RECONCILIATION)),
        ]
        rows: list[SourceHealthSnapshot] = []
        now = datetime.now(MADRID)
        for source_name, frame in checks:
            if frame.is_empty():
                rows.append(
                    SourceHealthSnapshot(
                        ingestion_run_id=None,
                        source_name=source_name,
                        observed_at=None,
                        status="missing",
                        freshness_hours=None,
                        row_count=0,
                        null_rate=1.0,
                        detail="No rows are available.",
                        metrics_json={},
                    )
                )
                continue
            timestamp_column = "timestamp" if "timestamp" in frame.columns else None
            observed_at = ensure_madrid(frame[timestamp_column].max()) if timestamp_column is not None else None
            freshness_hours = (
                round(max((now - observed_at).total_seconds() / 3600, 0.0), 2) if observed_at is not None else None
            )
            null_rate = float(
                sum(frame.select(pl.all().null_count()).row(0)) / max(frame.height * max(len(frame.columns), 1), 1)
            )
            status = "healthy"
            if source_name == "open_meteo_weather" and null_rate > 0.1:
                status = "degraded"
            elif source_name == "omie_reconciliation":
                delta = (
                    float(frame["absolute_delta_eur_mwh"].mean())
                    if "absolute_delta_eur_mwh" in frame.columns and frame.height > 0
                    else 0.0
                )
                status = "healthy" if delta < 2.0 else "degraded"
            elif freshness_hours is not None and freshness_hours > 24:
                status = "stale"
            rows.append(
                SourceHealthSnapshot(
                    ingestion_run_id=None,
                    source_name=source_name,
                    observed_at=observed_at,
                    status=status,
                    freshness_hours=freshness_hours,
                    row_count=frame.height,
                    null_rate=null_rate,
                    detail=f"{frame.height} rows",
                    metrics_json={
                        "latest_absolute_delta_eur_mwh": float(frame["absolute_delta_eur_mwh"].mean())
                        if "absolute_delta_eur_mwh" in frame.columns and frame.height > 0
                        else None
                    },
                )
            )
        return rows

    def _run_summary(self, run: Any) -> dict[str, Any] | None:
        if run is None:
            return None
        payload = {"id": run.id}
        for field in ("status", "serving_mode", "publish_day", "started_at", "completed_at", "generated_at"):
            if hasattr(run, field):
                value = getattr(run, field)
                payload[field] = ensure_madrid(value).isoformat() if isinstance(value, datetime) else value
        return payload

    def _mlflow_run(self, training_run_id: int) -> Any:
        try:
            import mlflow
        except ImportError:
            return nullcontext(None)

        tracking_uri = self.settings.mlflow_tracking_uri
        if tracking_uri.startswith("file:./"):
            tracking_uri = f"file:{(Path.cwd() / tracking_uri[7:]).resolve()}"
        elif tracking_uri.startswith("sqlite:///./"):
            relative_path = tracking_uri.removeprefix("sqlite:///./")
            tracking_uri = f"sqlite:///{(Path.cwd() / relative_path).resolve()}"
        mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(self.settings.mlflow_experiment_name)
        return mlflow.start_run(run_name=f"training-run-{training_run_id}")

    def _log_mlflow(self, summary: dict[str, Any], benchmark_rows: list[BacktestResult]) -> None:
        import mlflow

        for row in benchmark_rows:
            prefix = row.model_name.replace("-", "_")
            mlflow.log_metric(f"{prefix}_mae", row.mae)
            mlflow.log_metric(f"{prefix}_rmse", row.rmse)
            mlflow.log_metric(f"{prefix}_smape", row.smape)
            if row.coverage_p10_p90 is not None:
                mlflow.log_metric(f"{prefix}_coverage_p10_p90", row.coverage_p10_p90)
        mlflow.log_dict(summary, "training_summary.json")

    def _json_ready(self, value: Any) -> Any:
        if isinstance(value, datetime):
            return ensure_madrid(value).isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dict):
            return {key: self._json_ready(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self._json_ready(item) for item in value]
        return value

    def _forecast_explanation_confidence(self, source_health: list[SourceFreshness]) -> str:
        weather = next((item for item in source_health if item.name == "open_meteo_weather"), None)
        if weather is None or weather.status == "missing":
            return "low"
        if weather.status in {"degraded", "stale"}:
            return "medium"
        return "high"

    def _overall_source_status(self, source_health: list[SourceFreshness]) -> str:
        statuses = {item.status for item in source_health}
        if "missing" in statuses:
            return "degraded"
        if "stale" in statuses or "degraded" in statuses:
            return "caution"
        return "healthy"

    def _degradation_notes(self, source_health: list[SourceFreshness]) -> list[str]:
        notes: list[str] = []
        for item in source_health:
            if item.status == "healthy":
                continue
            notes.append(f"{item.name} is {item.status}")
        return notes
