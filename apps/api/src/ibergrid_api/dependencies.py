from functools import lru_cache

from ibergrid_ml.models.service import ForecastService

from ibergrid_api.config import get_settings


@lru_cache(maxsize=1)
def get_forecast_service() -> ForecastService:
    return ForecastService.from_settings(get_settings())

