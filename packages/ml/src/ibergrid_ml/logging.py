from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import orjson


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if hasattr(record, "event"):
            payload["event"] = record.event
        for key in ("run_id", "model_version", "source", "status"):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value
        return orjson.dumps(payload).decode("utf-8")


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    for logger_name in (
        "httpx",
        "lightning",
        "lightning.pytorch",
        "lightning.pytorch.utilities.rank_zero",
        "lightning.fabric",
        "lightning.fabric.utilities.rank_zero",
        "pytorch_lightning",
        "pytorch_lightning.utilities.rank_zero",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
