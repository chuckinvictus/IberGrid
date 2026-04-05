from __future__ import annotations

import os
import pickle
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

from ibergrid_ml.logging import get_logger


QUANTILES = [0.1, 0.5, 0.9]
KNOWN_REAL_FEATURES = [
    "hour_of_day",
    "day_of_week",
    "month_of_year",
    "week_of_year",
    "demand_forecast_mw",
    "temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "shortwave_radiation_wm2",
]
KNOWN_CATEGORICAL_FEATURES = ["is_weekend", "is_holiday"]
UNKNOWN_REAL_FEATURES = ["pvpc_eur_mwh", "spot_eur_mwh", "demand_actual_mw"]


@dataclass(slots=True)
class TFTArtifact:
    checkpoint_path: Path
    dataset_parameters_path: Path
    interpretation_path: Path


@dataclass(slots=True)
class TFTTrainer:
    artifact_dir: Path
    encoder_hours: int
    horizon_hours: int
    batch_size: int
    num_workers: int | None
    max_epochs: int
    _dataset_parameters: dict[str, Any] | None = field(default=None, init=False, repr=False)
    _prediction_model: Any | None = field(default=None, init=False, repr=False)

    @staticmethod
    def available() -> bool:
        try:
            import lightning  # noqa: F401
            import pytorch_forecasting  # noqa: F401
            import torch  # noqa: F401
        except ImportError:
            return False
        return True

    def train(self, feature_frame: pl.DataFrame) -> tuple[TFTArtifact, dict[str, float]]:
        if not self.available():
            raise RuntimeError("Install the optional 'torch' extra to train the TFT model.")

        import pandas as pd
        from lightning.pytorch import Trainer
        from lightning.pytorch.callbacks import Callback
        from lightning.pytorch.callbacks import EarlyStopping
        from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet
        from pytorch_forecasting.data import GroupNormalizer
        from pytorch_forecasting.metrics import QuantileLoss

        class TrainingProgressCallback(Callback):
            def __init__(self, max_epochs: int, batch_size: int, num_workers: int, train_batches: int, val_batches: int) -> None:
                self.max_epochs = max_epochs
                self.batch_size = batch_size
                self.num_workers = num_workers
                self.train_batches = train_batches
                self.val_batches = val_batches
                self.logger = get_logger("ibergrid.training.tft")
                self.fit_started_at = 0.0
                self.epoch_started_at = 0.0

            def on_fit_start(self, trainer, pl_module) -> None:  # type: ignore[no-untyped-def]
                self.fit_started_at = time.perf_counter()
                self.logger.info(
                    "TFT training started: epochs=%s train_batches=%s val_batches=%s batch_size=%s num_workers=%s",
                    self.max_epochs,
                    self.train_batches,
                    self.val_batches,
                    self.batch_size,
                    self.num_workers,
                )

            def on_train_epoch_start(self, trainer, pl_module) -> None:  # type: ignore[no-untyped-def]
                self.epoch_started_at = time.perf_counter()
                self.logger.info("TFT epoch %s/%s started", trainer.current_epoch + 1, self.max_epochs)

            def on_train_epoch_end(self, trainer, pl_module) -> None:  # type: ignore[no-untyped-def]
                current_epoch = trainer.current_epoch + 1
                elapsed = time.perf_counter() - self.fit_started_at
                epoch_seconds = time.perf_counter() - self.epoch_started_at
                remaining_epochs = max(self.max_epochs - current_epoch, 0)
                avg_epoch_seconds = elapsed / current_epoch if current_epoch else 0.0
                eta_seconds = avg_epoch_seconds * remaining_epochs
                metrics = trainer.callback_metrics
                train_metric = metrics.get("train_loss_epoch")
                if train_metric is None:
                    train_metric = metrics.get("train_loss")
                train_loss = self._metric(train_metric)
                val_loss = self._metric(metrics.get("val_loss"))
                self.logger.info(
                    "TFT epoch %s/%s completed: train_loss=%s val_loss=%s epoch_s=%.1f elapsed_s=%.1f eta_s=%.1f",
                    current_epoch,
                    self.max_epochs,
                    train_loss,
                    val_loss,
                    epoch_seconds,
                    elapsed,
                    eta_seconds,
                )

            def on_fit_end(self, trainer, pl_module) -> None:  # type: ignore[no-untyped-def]
                total_seconds = time.perf_counter() - self.fit_started_at
                completed_epochs = min(trainer.current_epoch, self.max_epochs)
                metrics = trainer.callback_metrics
                val_loss = self._metric(metrics.get("val_loss"))
                self.logger.info(
                    "TFT training finished: completed_epochs=%s/%s total_s=%.1f final_val_loss=%s",
                    completed_epochs,
                    self.max_epochs,
                    total_seconds,
                    val_loss,
                )

            @staticmethod
            def _metric(value: Any) -> str:
                if value is None:
                    return "n/a"
                if hasattr(value, "item"):
                    value = value.item()
                try:
                    return f"{float(value):.4f}"
                except (TypeError, ValueError):
                    return str(value)

        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        frame = self._prepare_frame(feature_frame)
        num_workers = self._resolve_num_workers()
        training_cutoff = int(frame["time_idx"].max()) - self.horizon_hours
        training = TimeSeriesDataSet(
            frame[frame["time_idx"] <= training_cutoff],
            time_idx="time_idx",
            target="pvpc_eur_mwh",
            group_ids=["series"],
            min_encoder_length=self.encoder_hours,
            max_encoder_length=self.encoder_hours,
            min_prediction_length=self.horizon_hours,
            max_prediction_length=self.horizon_hours,
            static_categoricals=["series"],
            time_varying_known_categoricals=KNOWN_CATEGORICAL_FEATURES,
            time_varying_known_reals=KNOWN_REAL_FEATURES,
            time_varying_unknown_reals=UNKNOWN_REAL_FEATURES,
            target_normalizer=GroupNormalizer(groups=["series"]),
            add_relative_time_idx=True,
            add_target_scales=True,
            add_encoder_length=True,
            allow_missing_timesteps=True,
        )
        validation = TimeSeriesDataSet.from_dataset(
            training,
            frame,
            min_prediction_idx=training_cutoff + 1,
            stop_randomization=True,
            predict=False,
        )
        dataloader_kwargs: dict[str, Any] = {
            "batch_size": self.batch_size,
            "num_workers": num_workers,
        }
        if num_workers > 0:
            dataloader_kwargs["persistent_workers"] = True
        train_loader = training.to_dataloader(train=True, **dataloader_kwargs)
        val_loader = validation.to_dataloader(train=False, **dataloader_kwargs)
        train_batches = len(train_loader)
        val_batches = len(val_loader)
        model = TemporalFusionTransformer.from_dataset(
            training,
            learning_rate=0.01,
            hidden_size=32,
            attention_head_size=4,
            dropout=0.12,
            hidden_continuous_size=16,
            output_size=len(QUANTILES),
            loss=QuantileLoss(quantiles=QUANTILES),
            log_interval=-1,
            reduce_on_plateau_patience=2,
        )
        trainer = Trainer(
            max_epochs=self.max_epochs,
            accelerator="cpu",
            gradient_clip_val=0.1,
            enable_checkpointing=False,
            enable_model_summary=False,
            enable_progress_bar=False,
            logger=False,
            callbacks=[
                EarlyStopping(monitor="val_loss", patience=3, mode="min"),
                TrainingProgressCallback(
                    max_epochs=self.max_epochs,
                    batch_size=self.batch_size,
                    num_workers=num_workers,
                    train_batches=train_batches,
                    val_batches=val_batches,
                ),
            ],
        )
        trainer.fit(model, train_loader, val_loader)

        checkpoint_path = self.artifact_dir / "tft.ckpt"
        trainer.save_checkpoint(checkpoint_path)
        dataset_parameters_path = self.artifact_dir / "dataset_parameters.pkl"
        with dataset_parameters_path.open("wb") as handle:
            parameters = training.get_parameters()
            pickle.dump(parameters, handle)
            self._dataset_parameters = parameters

        interpretation = self._extract_global_importance(model, val_loader)
        interpretation_path = self.artifact_dir / "global_importance.pkl"
        with interpretation_path.open("wb") as handle:
            pickle.dump(interpretation, handle)

        return TFTArtifact(
            checkpoint_path=checkpoint_path,
            dataset_parameters_path=dataset_parameters_path,
            interpretation_path=interpretation_path,
        ), interpretation

    def predict(self, combined_frame: pl.DataFrame) -> pl.DataFrame:
        if not self.available():
            raise RuntimeError("Install the optional 'torch' extra to score the TFT model.")

        from pytorch_forecasting import TemporalFusionTransformer, TimeSeriesDataSet

        ordered = combined_frame.sort("timestamp")
        future_rows = ordered.filter(pl.col("pvpc_eur_mwh").is_null()).sort("timestamp").head(self.horizon_hours)
        if future_rows.is_empty():
            raise RuntimeError("TFT prediction requires future rows with null `pvpc_eur_mwh` values.")

        future_indices = (
            ordered.with_row_index("row_idx")
            .filter(pl.col("pvpc_eur_mwh").is_null())
            .head(self.horizon_hours)["row_idx"]
            .to_list()
        )
        window_start = max(0, int(future_indices[0]) - self.encoder_hours)
        window_length = int(future_indices[-1]) - window_start + 1
        prediction_window = ordered.slice(window_start, window_length)

        frame = self._prepare_frame(prediction_window)
        frame["time_idx"] = np.arange(len(frame))
        if self._dataset_parameters is None:
            with (self.artifact_dir / "dataset_parameters.pkl").open("rb") as handle:
                self._dataset_parameters = pickle.load(handle)
        parameters = self._dataset_parameters

        dataset = TimeSeriesDataSet.from_parameters(parameters, frame, predict=True, stop_randomization=True)
        if self._prediction_model is None:
            self._prediction_model = TemporalFusionTransformer.load_from_checkpoint(self.artifact_dir / "tft.ckpt")
            self._prediction_model.eval()

        prediction = self._prediction_model.predict(
            dataset,
            mode="quantiles",
            batch_size=1,
            num_workers=0,
            trainer_kwargs={"logger": False, "enable_progress_bar": False, "enable_model_summary": False},
        )
        quantile_tensor = prediction.output if hasattr(prediction, "output") else prediction
        quantile_array = quantile_tensor.detach().cpu().numpy() if hasattr(quantile_tensor, "detach") else np.asarray(quantile_tensor)
        if quantile_array.ndim == 3:
            quantile_array = quantile_array[0]
        if quantile_array.shape[0] != future_rows.height:
            raise RuntimeError(
                f"TFT prediction horizon mismatch: expected {future_rows.height} rows, received {quantile_array.shape[0]}."
            )
        return future_rows.select("timestamp").with_columns(
            pl.Series("p10", quantile_array[:, 0]),
            pl.Series("p50", quantile_array[:, 1]),
            pl.Series("p90", quantile_array[:, 2]),
        )

    def load_global_importance(self) -> dict[str, float]:
        interpretation_path = self.artifact_dir / "global_importance.pkl"
        if not interpretation_path.exists():
            return {}
        with interpretation_path.open("rb") as handle:
            return pickle.load(handle)

    def _resolve_num_workers(self) -> int:
        if self.num_workers is not None:
            return max(self.num_workers, 0)
        cpu_count = os.cpu_count() or 1
        return min(max(cpu_count - 1, 1), 4)

    def _prepare_frame(self, feature_frame: pl.DataFrame) -> Any:
        import pandas as pd

        columns = [
            "timestamp",
            "pvpc_eur_mwh",
            "spot_eur_mwh",
            "demand_actual_mw",
            "demand_forecast_mw",
            "pvpc_lag_24h",
            "spot_lag_24h",
            "demand_lag_24h",
            "demand_forecast_lag_24h",
            "temperature_c",
            "temperature_lag_24h",
            "relative_humidity_pct",
            "wind_speed_kmh",
            "wind_speed_lag_24h",
            "shortwave_radiation_wm2",
            "solar_radiation_lag_24h",
            "hour_of_day",
            "day_of_week",
            "month_of_year",
            "week_of_year",
            "is_weekend",
            "is_holiday",
        ]
        frame = feature_frame.select(columns).sort("timestamp").to_pandas()
        frame["pvpc_eur_mwh"] = frame["pvpc_eur_mwh"].fillna(frame["pvpc_lag_24h"]).ffill().bfill()
        frame["spot_eur_mwh"] = frame["spot_eur_mwh"].fillna(frame["spot_lag_24h"]).ffill().bfill()
        frame["demand_actual_mw"] = (
            frame["demand_actual_mw"].fillna(frame["demand_lag_24h"]).fillna(frame["demand_forecast_mw"]).ffill().bfill()
        )
        frame["demand_forecast_mw"] = (
            frame["demand_forecast_mw"]
            .fillna(frame["demand_forecast_lag_24h"])
            .fillna(frame["demand_actual_mw"])
            .ffill()
            .bfill()
        )
        frame["temperature_c"] = frame["temperature_c"].fillna(frame["temperature_lag_24h"]).ffill().bfill()
        frame["relative_humidity_pct"] = frame["relative_humidity_pct"].ffill().bfill()
        frame["wind_speed_kmh"] = frame["wind_speed_kmh"].fillna(frame["wind_speed_lag_24h"]).ffill().bfill()
        frame["shortwave_radiation_wm2"] = frame["shortwave_radiation_wm2"].fillna(frame["solar_radiation_lag_24h"]).ffill().bfill()
        frame["series"] = "peninsular_spain"
        frame["time_idx"] = np.arange(len(frame))
        frame["is_weekend"] = frame["is_weekend"].fillna(False).astype(int).astype(str)
        frame["is_holiday"] = frame["is_holiday"].fillna(False).astype(int).astype(str)
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        return frame

    def _extract_global_importance(self, model: Any, dataloader: Any) -> dict[str, float]:
        try:
            raw_predictions, x = model.predict(
                dataloader,
                mode="raw",
                return_x=True,
                trainer_kwargs={"logger": False, "enable_progress_bar": False, "enable_model_summary": False},
            )
            interpretation = model.interpret_output(raw_predictions.output, reduction="sum")
            encoder = interpretation.get("encoder_variables")
            decoder = interpretation.get("decoder_variables")
            variable_names = KNOWN_REAL_FEATURES + KNOWN_CATEGORICAL_FEATURES + UNKNOWN_REAL_FEATURES
            scores: dict[str, float] = {}
            for tensor in (encoder, decoder):
                if tensor is None:
                    continue
                values = tensor.detach().cpu().numpy() if hasattr(tensor, "detach") else np.asarray(tensor)
                flattened = values.mean(axis=0) if values.ndim > 1 else values
                for idx, score in enumerate(flattened[: len(variable_names)]):
                    scores[variable_names[idx]] = scores.get(variable_names[idx], 0.0) + float(score)
            total = sum(scores.values()) or 1.0
            return {name: value / total for name, value in sorted(scores.items(), key=lambda item: item[1], reverse=True)}
        except Exception:
            return {}
