from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo
from warnings import warn

import httpx
import polars as pl

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.time import MADRID


WEATHER_HUBS: dict[str, tuple[float, float]] = {
    "madrid": (40.4168, -3.7038),
    "barcelona": (41.3874, 2.1686),
    "valencia": (39.4699, -0.3763),
    "bilbao": (43.2630, -2.9350),
    "sevilla": (37.3891, -5.9845),
    "zaragoza": (41.6488, -0.8891),
}

WEATHER_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,shortwave_radiation"
UTC = ZoneInfo("UTC")
WEATHER_SCHEMA = {
    "timestamp": pl.Datetime(time_zone="Europe/Madrid"),
    "hub": pl.String,
    "temperature_c": pl.Float64,
    "relative_humidity_pct": pl.Float64,
    "wind_speed_kmh": pl.Float64,
    "shortwave_radiation_wm2": pl.Float64,
}


@dataclass(slots=True)
class OpenMeteoClient:
    settings: ForecastSettings

    @property
    def _timeout(self) -> float:
        return self.settings.api_timeout_seconds

    def _fetch(self, base_url: str, params: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(base_url, params=params)
            response.raise_for_status()
        return response.json()

    def _frame_from_payload(self, hub: str, payload: dict[str, Any]) -> pl.DataFrame:
        hourly = payload["hourly"]
        rows = []
        for idx, raw_time in enumerate(hourly["time"]):
            utc_timestamp = datetime.fromisoformat(raw_time).replace(tzinfo=UTC)
            rows.append(
                {
                    "timestamp": utc_timestamp.astimezone(MADRID),
                    "hub": hub,
                    "temperature_c": hourly["temperature_2m"][idx],
                    "relative_humidity_pct": hourly["relative_humidity_2m"][idx],
                    "wind_speed_kmh": hourly["wind_speed_10m"][idx],
                    "shortwave_radiation_wm2": hourly["shortwave_radiation"][idx],
                }
            )
        return pl.DataFrame(rows)

    def fetch_archive(self, start_day: date, end_day: date) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for hub in self.settings.weather_hubs:
            lat, lon = WEATHER_HUBS[hub]
            try:
                payload = self._fetch(
                    "https://archive-api.open-meteo.com/v1/archive",
                    {
                        "latitude": lat,
                        "longitude": lon,
                        "start_date": start_day.isoformat(),
                        "end_date": end_day.isoformat(),
                        "hourly": WEATHER_VARS,
                        "timezone": "GMT",
                    },
                )
                frames.append(self._frame_from_payload(hub, payload))
            except httpx.HTTPError as exc:
                warn(f"Skipping weather archive for {hub}: {exc}", RuntimeWarning, stacklevel=2)
        if not frames:
            return pl.DataFrame(schema=WEATHER_SCHEMA)
        return pl.concat(frames).sort(["timestamp", "hub"])

    def fetch_forecast(self, start_day: date, horizon_days: int = 7) -> pl.DataFrame:
        frames: list[pl.DataFrame] = []
        for hub in self.settings.weather_hubs:
            lat, lon = WEATHER_HUBS[hub]
            try:
                payload = self._fetch(
                    "https://api.open-meteo.com/v1/forecast",
                    {
                        "latitude": lat,
                        "longitude": lon,
                        "hourly": WEATHER_VARS,
                        "forecast_days": horizon_days,
                        "timezone": "GMT",
                    },
                )
                frame = self._frame_from_payload(hub, payload).filter(pl.col("timestamp").dt.date() >= start_day)
                frames.append(frame)
            except httpx.HTTPError as exc:
                warn(f"Skipping weather forecast for {hub}: {exc}", RuntimeWarning, stacklevel=2)
        if not frames:
            return pl.DataFrame(schema=WEATHER_SCHEMA)
        return pl.concat(frames).sort(["timestamp", "hub"])
