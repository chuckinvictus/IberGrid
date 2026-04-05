from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ibergrid_ml.config import ForecastSettings


@dataclass(slots=True)
class LakehouseStore:
    settings: ForecastSettings

    def path(self, layer: str, name: str) -> Path:
        root = {
            "bronze": self.settings.bronze_dir,
            "silver": self.settings.silver_dir,
            "gold": self.settings.gold_dir,
        }[layer]
        return root / f"{name}.parquet"

    def read(self, layer: str, name: str) -> pl.DataFrame:
        path = self.path(layer, name)
        if not path.exists():
            return pl.DataFrame()
        return pl.read_parquet(path)

    def write(self, frame: pl.DataFrame, layer: str, name: str) -> Path:
        path = self.path(layer, name)
        frame.write_parquet(path)
        return path

    def merge_write(self, frame: pl.DataFrame, layer: str, name: str, subset: list[str]) -> Path:
        current = self.read(layer, name)
        if current.is_empty():
            merged = frame
        else:
            merged = pl.concat([current, frame], how="diagonal_relaxed").unique(subset=subset, keep="last").sort(subset)
        return self.write(merged, layer, name)

