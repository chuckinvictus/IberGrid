from __future__ import annotations

from datetime import datetime, timedelta

import typer
from rich.console import Console

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.models.service import ForecastService
from ibergrid_ml.time import MADRID


app = typer.Typer(help="IberGrid operational CLI.")
console = Console()


def _service() -> ForecastService:
    return ForecastService.from_settings(ForecastSettings())


@app.command()
def refresh(days: int = 120) -> None:
    result = _service().refresh_recent(days)
    console.print(result)


@app.command()
def backfill(years: int = 2) -> None:
    result = _service().backfill(years)
    console.print(result)


@app.command()
def train() -> None:
    result = _service().train_and_promote()
    console.print(result)


@app.command()
def publish() -> None:
    result = _service().publish()
    console.print(result)


@app.command()
def reconcile(days: int = 14) -> None:
    result = _service().reconcile_recent(days)
    console.print(result)


@app.command("daily-job")
def daily_job(days: int = 30) -> None:
    service = _service()
    service.refresh_recent(days)
    result = service.publish()
    console.print(result)


@app.command("weekly-job")
def weekly_job(years: int = 2) -> None:
    service = _service()
    service.backfill(years)
    result = service.train_and_promote()
    console.print(result)


@app.command("status")
def status() -> None:
    console.print(_service().status_snapshot())


@app.command()
def preview() -> None:
    service = _service()
    today = datetime.now(MADRID).date()
    preview = service.day_ahead(today + timedelta(days=1))
    console.print(preview["metadata"])
    console.print(preview["forecast"][:5])


if __name__ == "__main__":
    app()
