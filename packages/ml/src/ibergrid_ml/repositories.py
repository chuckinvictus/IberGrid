from __future__ import annotations

from datetime import UTC, date, datetime

from sqlalchemy import delete, desc, select, update
from sqlalchemy.orm import Session

from ibergrid_ml.db_models import (
    BacktestResult,
    ForecastExplanation,
    ForecastPoint,
    ForecastRun,
    IngestionRun,
    ModelVersion,
    SourceHealthSnapshot,
    TrainingRun,
)
from ibergrid_ml.time import MADRID


class Repository:
    def __init__(self, session: Session):
        self.session = session

    def create_ingestion_run(self, start_day: date, end_day: date) -> IngestionRun:
        run = IngestionRun(
            started_at=datetime.now(UTC),
            completed_at=None,
            status="running",
            start_day=start_day,
            end_day=end_day,
            source_summary_json={},
            detail_json={},
        )
        self.session.add(run)
        self.session.flush()
        return run

    def finish_ingestion_run(self, run: IngestionRun, status: str, summary: dict, detail: dict) -> IngestionRun:
        run.completed_at = datetime.now(UTC)
        run.status = status
        run.source_summary_json = summary
        run.detail_json = detail
        self.session.flush()
        return run

    def replace_source_snapshots(self, ingestion_run_id: int, rows: list[SourceHealthSnapshot]) -> None:
        self.session.execute(
            delete(SourceHealthSnapshot).where(SourceHealthSnapshot.ingestion_run_id == ingestion_run_id)
        )
        for row in rows:
            row.ingestion_run_id = ingestion_run_id
            self.session.add(row)
        self.session.flush()

    def create_training_run(
        self,
        train_start: date,
        train_end: date,
        validation_start: date,
        validation_end: date,
        test_start: date,
        test_end: date,
    ) -> TrainingRun:
        run = TrainingRun(
            started_at=datetime.now(UTC),
            completed_at=None,
            status="running",
            train_start=train_start,
            train_end=train_end,
            validation_start=validation_start,
            validation_end=validation_end,
            test_start=test_start,
            test_end=test_end,
            mlflow_run_id=None,
            champion_decision="pending",
            summary_json={},
        )
        self.session.add(run)
        self.session.flush()
        return run

    def finish_training_run(
        self,
        run: TrainingRun,
        status: str,
        champion_decision: str,
        summary: dict,
        mlflow_run_id: str | None,
    ) -> TrainingRun:
        run.completed_at = datetime.now(UTC)
        run.status = status
        run.champion_decision = champion_decision
        run.summary_json = summary
        run.mlflow_run_id = mlflow_run_id
        self.session.flush()
        return run

    def replace_backtest_results(self, training_run_id: int, rows: list[BacktestResult]) -> None:
        self.session.execute(delete(BacktestResult).where(BacktestResult.training_run_id == training_run_id))
        for row in rows:
            row.training_run_id = training_run_id
            self.session.add(row)
        self.session.flush()

    def create_model_version(
        self,
        version: str,
        model_type: str,
        artifact_path: str | None,
        metrics_json: dict,
        explanation_json: dict,
        promotion_summary_json: dict,
        training_run_id: int | None,
        is_promoted: bool,
    ) -> ModelVersion:
        model_version = ModelVersion(
            training_run_id=training_run_id,
            version=version,
            model_type=model_type,
            artifact_path=artifact_path,
            metrics_json=metrics_json,
            explanation_json=explanation_json,
            promotion_summary_json=promotion_summary_json,
            is_promoted=is_promoted,
            promoted_at=datetime.now(UTC) if is_promoted else None,
            created_at=datetime.now(UTC),
        )
        self.session.add(model_version)
        self.session.flush()
        return model_version

    def promote_model_version(self, model_version_id: int) -> None:
        self.session.execute(update(ModelVersion).values(is_promoted=False, promoted_at=None))
        self.session.execute(
            update(ModelVersion)
            .where(ModelVersion.id == model_version_id)
            .values(is_promoted=True, promoted_at=datetime.now(UTC))
        )
        self.session.flush()

    def get_promoted_model(self) -> ModelVersion | None:
        stmt = select(ModelVersion).where(ModelVersion.is_promoted.is_(True)).order_by(desc(ModelVersion.promoted_at))
        return self.session.execute(stmt).scalar_one_or_none()

    def get_model_version(self, model_version_id: int) -> ModelVersion | None:
        stmt = select(ModelVersion).where(ModelVersion.id == model_version_id)
        return self.session.execute(stmt).scalar_one_or_none()

    def get_latest_model_version(self) -> ModelVersion | None:
        stmt = select(ModelVersion).order_by(desc(ModelVersion.created_at))
        return self.session.execute(stmt).scalars().first()

    def get_latest_training_run(self) -> TrainingRun | None:
        stmt = select(TrainingRun).order_by(desc(TrainingRun.started_at))
        return self.session.execute(stmt).scalars().first()

    def get_latest_ingestion_run(self) -> IngestionRun | None:
        stmt = select(IngestionRun).order_by(desc(IngestionRun.started_at))
        return self.session.execute(stmt).scalars().first()

    def create_forecast_run(
        self,
        publish_day: date,
        target_start: datetime,
        target_end: datetime,
        serving_mode: str,
        status: str,
        metadata_json: dict,
        model_version_id: int | None = None,
        fallback_reason: str | None = None,
    ) -> ForecastRun:
        run = ForecastRun(
            model_version_id=model_version_id,
            serving_mode=serving_mode,
            status=status,
            target_start=target_start,
            target_end=target_end,
            generated_at=datetime.now(UTC),
            publish_day=publish_day,
            fallback_reason=fallback_reason,
            metadata_json=metadata_json,
        )
        self.session.add(run)
        self.session.flush()
        return run

    def replace_forecast_contents(
        self,
        forecast_run_id: int,
        points: list[ForecastPoint],
        explanations: list[ForecastExplanation],
    ) -> None:
        self.session.execute(delete(ForecastPoint).where(ForecastPoint.forecast_run_id == forecast_run_id))
        self.session.execute(delete(ForecastExplanation).where(ForecastExplanation.forecast_run_id == forecast_run_id))
        for point in points:
            point.forecast_run_id = forecast_run_id
            self.session.add(point)
        for explanation in explanations:
            explanation.forecast_run_id = forecast_run_id
            self.session.add(explanation)
        self.session.flush()

    def latest_forecast_run(self) -> ForecastRun | None:
        stmt = select(ForecastRun).order_by(desc(ForecastRun.generated_at))
        return self.session.execute(stmt).scalars().first()

    def forecast_for_day(self, target_day: date) -> ForecastRun | None:
        day_start = datetime.combine(target_day, datetime.min.time(), tzinfo=MADRID)
        stmt = (
            select(ForecastRun)
            .where(ForecastRun.target_start <= day_start)
            .where(ForecastRun.target_end >= day_start)
            .order_by(desc(ForecastRun.generated_at))
        )
        run = self.session.execute(stmt).scalars().first()
        if run is None:
            return None
        return run

    def list_forecast_points(self, forecast_run_id: int) -> list[ForecastPoint]:
        stmt = select(ForecastPoint).where(ForecastPoint.forecast_run_id == forecast_run_id).order_by(ForecastPoint.timestamp)
        return list(self.session.execute(stmt).scalars())

    def list_forecast_explanations(self, forecast_run_id: int) -> list[ForecastExplanation]:
        stmt = (
            select(ForecastExplanation)
            .where(ForecastExplanation.forecast_run_id == forecast_run_id)
            .order_by(ForecastExplanation.timestamp)
        )
        return list(self.session.execute(stmt).scalars())

    def list_backtest_results(self, training_run_id: int) -> list[BacktestResult]:
        stmt = select(BacktestResult).where(BacktestResult.training_run_id == training_run_id)
        return list(self.session.execute(stmt).scalars())

    def list_source_snapshots(self, ingestion_run_id: int) -> list[SourceHealthSnapshot]:
        stmt = select(SourceHealthSnapshot).where(SourceHealthSnapshot.ingestion_run_id == ingestion_run_id)
        return list(self.session.execute(stmt).scalars())
