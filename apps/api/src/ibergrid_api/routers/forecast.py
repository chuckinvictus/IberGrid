from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from ibergrid_api.dependencies import get_forecast_service
from ibergrid_api.schemas import DayAheadResponse, WeekAheadResponse
from ibergrid_ml.models.service import ForecastService


router = APIRouter(prefix="/api/v1/forecast", tags=["forecast"])


@router.get("/day-ahead", response_model=DayAheadResponse)
def day_ahead(
    date_value: date = Query(alias="date"),
    service: ForecastService = Depends(get_forecast_service),
) -> DayAheadResponse:
    try:
        return DayAheadResponse.model_validate(service.day_ahead(date_value))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/week-ahead", response_model=WeekAheadResponse)
def week_ahead(
    from_date: date = Query(alias="from"),
    service: ForecastService = Depends(get_forecast_service),
) -> WeekAheadResponse:
    try:
        return WeekAheadResponse.model_validate(service.week_ahead(from_date))
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
