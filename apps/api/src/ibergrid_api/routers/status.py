from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ibergrid_api.dependencies import get_forecast_service
from ibergrid_api.schemas import StatusResponse
from ibergrid_ml.models.service import ForecastService


router = APIRouter(prefix="/api/v1/status", tags=["status"])


@router.get("/latest", response_model=StatusResponse)
def latest_status(
    service: ForecastService = Depends(get_forecast_service),
) -> StatusResponse:
    try:
        return StatusResponse.model_validate(service.status_snapshot())
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
