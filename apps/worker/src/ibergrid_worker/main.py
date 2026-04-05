from __future__ import annotations

from datetime import datetime
from typing import Any

import typer
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from rich.console import Console

from ibergrid_ml.config import ForecastSettings
from ibergrid_ml.logging import configure_logging, get_logger
from ibergrid_ml.models.service import ForecastService
from ibergrid_ml.time import MADRID


app = typer.Typer(help="IberGrid scheduled worker.")
console = Console()


def _service() -> ForecastService:
    return ForecastService.from_settings(ForecastSettings())


def _run_job(job_name: str, callback: Any) -> dict[str, Any]:
    logger = get_logger("ibergrid.worker")
    logger.info("worker_job_started", extra={"job_name": job_name})
    result = callback()
    logger.info("worker_job_completed", extra={"job_name": job_name, "result": result})
    return result


def _daily_callback() -> dict[str, Any]:
    service = _service()
    service.refresh_recent(service.settings.worker_refresh_days)
    return service.publish()


def _weekly_callback() -> dict[str, Any]:
    service = _service()
    service.backfill(service.settings.worker_backfill_years)
    return service.train_and_promote()


def _reconciliation_callback() -> dict[str, Any]:
    service = _service()
    return service.reconcile_recent(service.settings.worker_reconciliation_days)


def _run_daily(service: ForecastService, refresh_days: int) -> dict[str, Any]:
    service.refresh_recent(refresh_days)
    return service.publish()


def _run_weekly(service: ForecastService, backfill_years: int) -> dict[str, Any]:
    service.backfill(backfill_years)
    return service.train_and_promote()


@app.command("daily-job")
def daily_job(days: int | None = None) -> None:
    service = _service()
    refresh_days = days if days is not None else service.settings.worker_refresh_days
    console.print(_run_job("daily-job", lambda: _run_daily(service, refresh_days)))


@app.command("weekly-job")
def weekly_job(years: int | None = None) -> None:
    service = _service()
    backfill_years = years if years is not None else service.settings.worker_backfill_years
    console.print(_run_job("weekly-job", lambda: _run_weekly(service, backfill_years)))


@app.command("reconciliation-job")
def reconciliation_job(days: int | None = None) -> None:
    service = _service()
    reconciliation_days = days if days is not None else service.settings.worker_reconciliation_days
    console.print(
        _run_job(
            "reconciliation-job",
            lambda: service.reconcile_recent(reconciliation_days),
        )
    )


@app.command()
def status() -> None:
    console.print(_service().status_snapshot())


@app.command()
def serve() -> None:
    configure_logging()
    settings = ForecastSettings()
    logger = get_logger("ibergrid.worker")
    if not settings.worker_schedule_enabled:
        raise typer.Exit(code=0)

    scheduler = BlockingScheduler(timezone=settings.timezone)
    scheduler.add_job(
        lambda: _run_job("daily-job", _daily_callback),
        CronTrigger(hour=settings.worker_daily_job_hour, minute=settings.worker_daily_job_minute),
        id="daily-job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        lambda: _run_job("weekly-job", _weekly_callback),
        CronTrigger(
            day_of_week=settings.worker_weekly_job_day_of_week,
            hour=settings.worker_weekly_job_hour,
            minute=settings.worker_weekly_job_minute,
        ),
        id="weekly-job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        lambda: _run_job("reconciliation-job", _reconciliation_callback),
        CronTrigger(
            hour=settings.worker_reconciliation_job_hour,
            minute=settings.worker_reconciliation_job_minute,
        ),
        id="reconciliation-job",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    logger.info(
        "worker_started",
        extra={
            "timezone": settings.timezone,
            "started_at": datetime.now(MADRID).isoformat(),
            "daily_job": f"{settings.worker_daily_job_hour:02d}:{settings.worker_daily_job_minute:02d}",
            "weekly_job": f"{settings.worker_weekly_job_day_of_week} {settings.worker_weekly_job_hour:02d}:{settings.worker_weekly_job_minute:02d}",
            "reconciliation_job": f"{settings.worker_reconciliation_job_hour:02d}:{settings.worker_reconciliation_job_minute:02d}",
        },
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("worker_stopped")


if __name__ == "__main__":
    app()
