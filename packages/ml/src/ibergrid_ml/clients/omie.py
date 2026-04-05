from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from io import StringIO

import httpx
import polars as pl

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.time import MADRID


@dataclass(slots=True)
class OMIEClient:
    settings: ForecastSettings

    def fetch_day_ahead_reference(self, day: date) -> pl.DataFrame:
        filename = f"marginalpdbc_{day.strftime('%Y%m%d')}.1"
        url = f"https://www.omie.es/es/file-download?parents=marginalpdbc&filename={filename}"
        with httpx.Client(timeout=self.settings.api_timeout_seconds) as client:
            response = client.get(url)
            response.raise_for_status()
        lines = [
            line
            for line in response.text.splitlines()
            if line and not line.startswith("MARGINALPDBC") and not line.startswith("*")
        ]
        buffer = StringIO("\n".join(lines))
        frame = pl.read_csv(
            buffer,
            separator=";",
            has_header=False,
            new_columns=["year", "month", "day", "period", "price_pt", "price_es", "discard"],
        ).drop("discard")
        hourly = (
            frame.select(
                ((pl.col("period").cast(pl.Int32) - 1) // 4).alias("hour_index"),
                pl.col("price_es").cast(pl.Float64).alias("omie_spot_eur_mwh"),
            )
            .group_by("hour_index")
            .agg(pl.col("omie_spot_eur_mwh").mean())
            .sort("hour_index")
        )
        local_midnight = datetime.combine(day, datetime.min.time(), tzinfo=MADRID)
        utc_midnight = local_midnight.astimezone(UTC)
        timestamps = [
            (utc_midnight + timedelta(hours=int(hour_index))).astimezone(MADRID)
            for hour_index in hourly["hour_index"].to_list()
        ]
        return hourly.select(
            pl.Series("timestamp", timestamps),
            pl.col("omie_spot_eur_mwh"),
        )
