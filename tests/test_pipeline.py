from __future__ import annotations

from datetime import date
from pathlib import Path

from ibergrid_ml.models.pipeline import ModelBundle
from ibergrid_ml.models.service import ForecastService
from ibergrid_ml.models.tft import TFTArtifact, TFTTrainer
from ibergrid_ml.persistence import session_scope
from ibergrid_ml.repositories import Repository
from ibergrid_ml.schemas import DatasetName
from tests.helpers import (
    build_serving_snapshot,
    build_source_health_frame,
    build_training_frame,
    configure_test_environment,
)


def test_publish_forecast_persists_outputs(tmp_path, monkeypatch) -> None:
    settings = configure_test_environment(monkeypatch, tmp_path)
    service = ForecastService.from_settings(settings)
    service.pipeline.ensure_schema()

    training_frame = build_training_frame()
    service.store.write(training_frame, "gold", DatasetName.TRAINING_DATASET)
    service.store.write(
        build_source_health_frame(training_frame["timestamp"].max()),
        "gold",
        DatasetName.SOURCE_HEALTH_SNAPSHOT,
    )
    monkeypatch.setattr(
        service.pipeline.builder.__class__,
        "build_serving_snapshot",
        lambda _self, start_at, horizon_hours: build_serving_snapshot(training_frame, start_at, horizon_hours),
    )

    result = service.publish(date(2026, 4, 3))

    assert result["status"] == "success"
    assert result["serving_mode"] == "heuristic-fallback"
    day_ahead = service.day_ahead(date(2026, 4, 4))
    assert len(day_ahead["forecast"]) == 24
    assert day_ahead["metadata"]["serving_mode"] == "heuristic-fallback"

    published_frame = service.store.read("gold", DatasetName.PUBLISHED_FORECAST_SNAPSHOT)
    assert published_frame.height == settings.horizon_hours

    with session_scope() as session:
        repo = Repository(session)
        run = repo.latest_forecast_run()
        assert run is not None
        assert len(repo.list_forecast_points(run.id)) == settings.horizon_hours
        assert repo.list_forecast_explanations(run.id)


def test_train_and_promote_promotes_tft_when_thresholds_are_met(tmp_path, monkeypatch) -> None:
    settings = configure_test_environment(monkeypatch, tmp_path)
    service = ForecastService.from_settings(settings)
    service.pipeline.ensure_schema()
    service.store.write(build_training_frame(), "gold", DatasetName.TRAINING_DATASET)

    monkeypatch.setattr(TFTTrainer, "available", staticmethod(lambda: True))

    def fake_train(self, feature_frame):
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact = TFTArtifact(
            checkpoint_path=self.artifact_dir / "tft.ckpt",
            dataset_parameters_path=self.artifact_dir / "dataset_parameters.pkl",
            interpretation_path=self.artifact_dir / "global_importance.pkl",
        )
        for path in (artifact.checkpoint_path, artifact.dataset_parameters_path, artifact.interpretation_path):
            path.write_bytes(b"synthetic")
        return artifact, {"pvpc_lag_24h": 0.42, "spot_lag_24h": 0.24, "demand_forecast_mw": 0.17}

    def fake_backtest(self, trainer, full_frame, test_frame):
        actual = test_frame["pvpc_eur_mwh"].to_numpy()
        coverage_cutoff = max(int(len(actual) * 0.2), 1)
        p10 = actual - 1.0
        p90 = actual + 1.0
        p10[:coverage_cutoff] = actual[:coverage_cutoff] + 2.0
        p90[:coverage_cutoff] = actual[:coverage_cutoff] + 4.0
        return ModelBundle(
            name="tft",
            p10=p10,
            p50=actual,
            p90=p90,
            curve=self._curve_payload(test_frame, actual),
            actual=actual,
            aligned_frame=test_frame.select("timestamp", "pvpc_eur_mwh"),
        )

    monkeypatch.setattr(TFTTrainer, "train", fake_train)
    monkeypatch.setattr(service.pipeline.__class__, "_backtest_tft", fake_backtest)

    result = service.train_and_promote()

    assert result["status"] == "success"
    assert result["champion_decision"] == "promoted"
    with session_scope() as session:
        promoted = Repository(session).get_promoted_model()
        assert promoted is not None
        assert promoted.model_type == "tft"


def test_train_and_promote_holds_out_tft_when_thresholds_are_not_met(tmp_path, monkeypatch) -> None:
    settings = configure_test_environment(monkeypatch, tmp_path)
    service = ForecastService.from_settings(settings)
    service.pipeline.ensure_schema()
    training_frame = build_training_frame()
    service.store.write(training_frame, "gold", DatasetName.TRAINING_DATASET)

    monkeypatch.setattr(TFTTrainer, "available", staticmethod(lambda: True))

    def fake_train(self, feature_frame):
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact = TFTArtifact(
            checkpoint_path=self.artifact_dir / "tft.ckpt",
            dataset_parameters_path=self.artifact_dir / "dataset_parameters.pkl",
            interpretation_path=self.artifact_dir / "global_importance.pkl",
        )
        for path in (artifact.checkpoint_path, artifact.dataset_parameters_path, artifact.interpretation_path):
            path.write_bytes(b"synthetic")
        return artifact, {"pvpc_lag_24h": 0.4}

    def weak_backtest(self, trainer, full_frame, test_frame):
        baseline = test_frame["pvpc_lag_24h"].to_numpy()
        return ModelBundle(
            name="tft",
            p10=baseline - 10.0,
            p50=baseline,
            p90=baseline + 10.0,
            curve=self._curve_payload(test_frame, baseline),
        )

    monkeypatch.setattr(TFTTrainer, "train", fake_train)
    monkeypatch.setattr(service.pipeline.__class__, "_backtest_tft", weak_backtest)

    result = service.train_and_promote()

    assert result["status"] == "success"
    assert result["champion_decision"] == "held_out"
    with session_scope() as session:
        repo = Repository(session)
        assert repo.get_promoted_model() is None
        latest_model = repo.get_latest_model_version()
        assert latest_model is not None
        assert latest_model.is_promoted is False


def test_tft_trainer_resolves_num_workers_from_override_and_cpu_count(monkeypatch) -> None:
    explicit = TFTTrainer(
        artifact_dir=Path("/tmp/explicit"),
        encoder_hours=336,
        horizon_hours=168,
        batch_size=64,
        num_workers=0,
        max_epochs=8,
    )
    assert explicit._resolve_num_workers() == 0

    auto = TFTTrainer(
        artifact_dir=Path("/tmp/auto"),
        encoder_hours=336,
        horizon_hours=168,
        batch_size=64,
        num_workers=None,
        max_epochs=8,
    )
    monkeypatch.setattr("ibergrid_ml.models.tft.os.cpu_count", lambda: 12)
    assert auto._resolve_num_workers() == 4
