from functools import lru_cache

from ibergrid_ml.config import ForecastSettings


@lru_cache(maxsize=1)
def get_settings() -> ForecastSettings:
    return ForecastSettings()

