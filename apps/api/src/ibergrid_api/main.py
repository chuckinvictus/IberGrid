from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ibergrid_api.config import get_settings
from ibergrid_api.db import Base, get_engine
from ibergrid_api.routers.context import router as context_router
from ibergrid_api.routers.forecast import router as forecast_router
from ibergrid_api.routers.performance import router as performance_router
from ibergrid_api.routers.status import router as status_router
from ibergrid_api.dependencies import get_forecast_service
from ibergrid_ml.logging import configure_logging


settings = get_settings()
configure_logging()


@asynccontextmanager
async def lifespan(_: FastAPI):
    Base.metadata.create_all(bind=get_engine())
    get_forecast_service().pipeline.ensure_schema()
    yield


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def readiness() -> dict[str, str]:
    status = get_forecast_service().status_snapshot()
    return {"status": "ready" if status["latest_forecast"] is not None else "warming"}


app.include_router(forecast_router)
app.include_router(context_router)
app.include_router(performance_router)
app.include_router(status_router)
