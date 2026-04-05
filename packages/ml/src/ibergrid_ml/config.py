from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ForecastSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="IBERGRID_", env_file=".env", extra="ignore")

    app_name: str = "IberGrid PVPC Intelligence Platform"
    timezone: str = "Europe/Madrid"
    data_root: Path = Field(default=Path("data"))
    artifacts_root: Path = Field(default=Path("artifacts"))
    bronze_dir: Path | None = None
    silver_dir: Path | None = None
    gold_dir: Path | None = None
    models_dir: Path | None = None
    reports_dir: Path | None = None
    api_timeout_seconds: float = 12.0
    database_url: str = "sqlite:///./data/ibergrid.db"
    mlflow_tracking_uri: str = "sqlite:///./data/mlflow.db"
    mlflow_experiment_name: str = "ibergrid-production"
    horizon_hours: int = 168
    encoder_hours: int = 24 * 14
    training_lookback_days: int = 365 * 2
    forecast_origin_offset_days: int = 1
    allow_demo_fallback: bool = False
    production_mode: bool = False
    retrain_batch_size: int = 64
    retrain_num_workers: int | None = None
    retrain_epochs: int = 8
    promotion_day_ahead_mae_improvement: float = 0.08
    promotion_smape_improvement: float = 0.05
    promotion_coverage_min: float = 0.75
    promotion_coverage_max: float = 0.85
    worker_schedule_enabled: bool = True
    worker_refresh_days: int = 30
    worker_backfill_years: int = 2
    worker_reconciliation_days: int = 14
    worker_daily_job_hour: int = 7
    worker_daily_job_minute: int = 10
    worker_weekly_job_day_of_week: str = "sun"
    worker_weekly_job_hour: int = 5
    worker_weekly_job_minute: int = 30
    worker_reconciliation_job_hour: int = 6
    worker_reconciliation_job_minute: int = 45
    weather_hubs: tuple[str, ...] = (
        "madrid",
        "barcelona",
        "valencia",
        "bilbao",
        "sevilla",
        "zaragoza",
    )

    def model_post_init(self, __context: object) -> None:
        self.bronze_dir = self.data_root / "bronze"
        self.silver_dir = self.data_root / "silver"
        self.gold_dir = self.data_root / "gold"
        self.models_dir = self.artifacts_root / "models"
        self.reports_dir = self.artifacts_root / "reports"
        for path in (self.bronze_dir, self.silver_dir, self.gold_dir, self.models_dir, self.reports_dir):
            path.mkdir(parents=True, exist_ok=True)
