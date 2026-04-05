from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.data.feature_builder import FeatureBuilder
from ibergrid_ml.data.store import LakehouseStore
from ibergrid_ml.models.heuristics import HeuristicQuantileForecaster
from ibergrid_ml.models.pipeline import ProductionPipeline
from ibergrid_ml.time import MADRID


@dataclass(slots=True)
class ForecastService:
    settings: ForecastSettings
    builder: FeatureBuilder
    store: LakehouseStore
    forecaster: HeuristicQuantileForecaster
    pipeline: ProductionPipeline

    @classmethod
    def from_settings(cls, settings: ForecastSettings) -> "ForecastService":
        pipeline = ProductionPipeline.from_settings(settings)
        return cls(
            settings=settings,
            builder=pipeline.builder,
            store=pipeline.store,
            forecaster=pipeline.heuristic,
            pipeline=pipeline,
        )

    def refresh_recent(self, days: int) -> dict[str, object]:
        end_day = datetime.now(MADRID).date()
        start_day = end_day - timedelta(days=days)
        return self.pipeline.run_ingestion(start_day, end_day)

    def backfill(self, years: int) -> dict[str, object]:
        end_day = datetime.now(MADRID).date()
        start_day = end_day - timedelta(days=365 * years)
        return self.pipeline.run_ingestion(start_day, end_day)

    def train_and_promote(self) -> dict[str, object]:
        return self.pipeline.train_and_promote()

    def publish(self, publish_day: date | None = None) -> dict[str, object]:
        return self.pipeline.publish_forecast(publish_day)

    def day_ahead(self, target_day: date) -> dict[str, object]:
        return self.pipeline.day_ahead_payload(target_day)

    def week_ahead(self, start_day: date) -> dict[str, object]:
        return self.pipeline.week_ahead_payload(start_day)

    def market_context(self, start_at: datetime, end_at: datetime) -> dict[str, object]:
        return self.pipeline.market_context_payload(start_at, end_at)

    def performance_snapshot(self) -> dict[str, object]:
        return self.pipeline.performance_payload()

    def source_health(self):
        return self.pipeline.source_health()

    def status_snapshot(self) -> dict[str, object]:
        return self.pipeline.status_snapshot()

    def reconcile_recent(self, days: int) -> dict[str, object]:
        end_day = datetime.now(MADRID).date()
        start_day = end_day - timedelta(days=days)
        return self.pipeline.run_omie_reconciliation(start_day, end_day)
