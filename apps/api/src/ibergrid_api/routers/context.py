from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from ibergrid_api.dependencies import get_forecast_service
from ibergrid_api.schemas import MarketContextResponse
from ibergrid_ml.models.service import ForecastService
from ibergrid_ml.time import ensure_madrid


router = APIRouter(prefix="/api/v1/context", tags=["context"])


@router.get("/market", response_model=MarketContextResponse)
def market_context(
    from_date: datetime = Query(alias="from"),
    to_date: datetime = Query(alias="to"),
    service: ForecastService = Depends(get_forecast_service),
) -> MarketContextResponse:
    try:
        payload = service.market_context(ensure_madrid(from_date), ensure_madrid(to_date))
        return MarketContextResponse.model_validate(payload)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
