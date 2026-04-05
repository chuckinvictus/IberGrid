from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
import polars as pl

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.time import ensure_madrid


@dataclass(slots=True)
class REDataClient:
    settings: ForecastSettings

    @property
    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url="https://apidatos.ree.es",
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=self.settings.api_timeout_seconds,
        )

    def _fetch(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        with self._client as client:
            response = client.get(path, params=params)
            response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            detail = payload["errors"][0].get("detail", "Unknown REData error")
            raise RuntimeError(detail)
        return payload

    def fetch_market_prices(self, start: datetime, end: datetime) -> pl.DataFrame:
        payload = self._fetch(
            "/es/datos/mercados/precios-mercados-tiempo-real",
            {
                "start_date": ensure_madrid(start).isoformat(timespec="minutes"),
                "end_date": ensure_madrid(end).isoformat(timespec="minutes"),
                "time_trunc": "hour",
            },
        )
        rows: list[dict[str, Any]] = []
        for entry in payload.get("included", []):
            title = entry["attributes"]["title"]
            metric = "pvpc" if title == "PVPC" else "spot"
            for value in entry["attributes"]["values"]:
                rows.append(
                    {
                        "timestamp": ensure_madrid(datetime.fromisoformat(value["datetime"])),
                        "metric": metric,
                        "price_eur_mwh": float(value["value"]),
                    }
                )
        return pl.DataFrame(rows).sort("timestamp")

    def fetch_demand(self, start: datetime, end: datetime) -> pl.DataFrame:
        payload = self._fetch(
            "/es/datos/demanda/demanda-tiempo-real",
            {
                "start_date": ensure_madrid(start).isoformat(timespec="minutes"),
                "end_date": ensure_madrid(end).isoformat(timespec="minutes"),
                "time_trunc": "hour",
            },
        )
        raw_rows: list[dict[str, Any]] = []
        for entry in payload.get("included", []):
            title = entry["attributes"]["title"]
            metric = {
                "Real": "demand_actual_mw",
                "Prevista": "demand_forecast_mw",
                "Programada": "demand_programmed_mw",
                "Programada total": "demand_programmed_total_mw",
            }.get(title)
            if metric is None:
                continue
            for value in entry["attributes"]["values"]:
                raw_rows.append(
                    {
                        "timestamp": ensure_madrid(datetime.fromisoformat(value["datetime"])),
                        "metric": metric,
                        "value_mw": float(value["value"]),
                    }
                )
        frame = pl.DataFrame(raw_rows)
        return (
            frame.with_columns(pl.col("timestamp").dt.truncate("1h").alias("hour"))
            .group_by(["hour", "metric"])
            .agg(pl.col("value_mw").mean().alias("value_mw"))
            .rename({"hour": "timestamp"})
            .sort("timestamp")
        )

    def fetch_generation_mix_daily(self, start: datetime, end: datetime) -> pl.DataFrame:
        payload = self._fetch(
            "/es/datos/generacion/estructura-generacion",
            {
                "start_date": ensure_madrid(start).isoformat(timespec="minutes"),
                "end_date": ensure_madrid(end).isoformat(timespec="minutes"),
                "time_trunc": "day",
            },
        )
        rows: list[dict[str, Any]] = []
        for entry in payload.get("included", []):
            title = entry["attributes"]["title"]
            generation_type = entry["attributes"]["type"]
            for value in entry["attributes"]["values"]:
                rows.append(
                    {
                        "day": ensure_madrid(datetime.fromisoformat(value["datetime"])).date(),
                        "technology": title,
                        "generation_type": generation_type,
                        "generation_mwh": float(value["value"]),
                        "share": float(value["percentage"]),
                    }
                )
        return pl.DataFrame(rows).sort(["day", "technology"])

