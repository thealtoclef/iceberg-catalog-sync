"""Standard logging setup driven by config."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

from iceberg_catalog_sync.config import LogConfig


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)


def setup_logging(config: LogConfig) -> logging.Logger:
    logger = logging.getLogger("iceberg_catalog_sync")
    logger.setLevel(getattr(logging, config.level))
    logger.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    if config.format == "json":
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger
