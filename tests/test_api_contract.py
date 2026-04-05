from __future__ import annotations

from datetime import date, datetime, timedelta

from fastapi.testclient import TestClient

from ibergrid_ml.models.service import ForecastService
from ibergrid_ml.models.tft import TFTTrainer
from ibergrid_ml.schemas import DatasetName
from ibergrid_ml.time import MADRID
from tests.helpers import (
    build_serving_snapshot,
    build_source_health_frame,
    build_training_frame,
    clear_runtime_caches,
    configure_test_environment,
)


def test_api_contract_serves_persisted_outputs(tmp_path, monkeypatch) -> None:
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
    monkeypatch.setattr(TFTTrainer, "available", staticmethod(lambda: False))
    service.train_and_promote()
    service.publish(date(2026, 4, 3))
    clear_runtime_caches()

    from ibergrid_api.main import app

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/ready").status_code == 200

        day_response = client.get("/api/v1/forecast/day-ahead", params={"date": "2026-04-04"})
        week_response = client.get("/api/v1/forecast/week-ahead", params={"from": "2026-04-04"})
        context_response = client.get(
            "/api/v1/context/market",
            params={
                "from": datetime(2026, 4, 2, tzinfo=MADRID).isoformat(),
                "to": (datetime(2026, 4, 2, tzinfo=MADRID) + timedelta(days=1)).isoformat(),
            },
        )
        performance_response = client.get("/api/v1/model/performance/latest")
        status_response = client.get("/api/v1/status/latest")

        assert day_response.status_code == 200
        assert len(day_response.json()["forecast"]) == 24
        assert week_response.status_code == 200
        assert len(week_response.json()["daily_bands"]) == 7
        assert context_response.status_code == 200
        assert performance_response.status_code == 200
        assert status_response.status_code == 200
        assert status_response.json()["serving_mode"] == "heuristic-fallback"
