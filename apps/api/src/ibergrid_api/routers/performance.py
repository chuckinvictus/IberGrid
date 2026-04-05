from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ibergrid_api.dependencies import get_forecast_service
from ibergrid_api.schemas import PerformanceResponse
from ibergrid_ml.models.service import ForecastService


router = APIRouter(prefix="/api/v1/model", tags=["model"])


@router.get("/performance/latest", response_model=PerformanceResponse)
def latest_performance(
    service: ForecastService = Depends(get_forecast_service),
) -> PerformanceResponse:
    try:
        return PerformanceResponse.model_validate(service.performance_snapshot())
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
