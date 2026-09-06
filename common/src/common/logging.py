"""One logging setup for every service: level from LOG_LEVEL, timestamps in UTC."""

from __future__ import annotations

import logging
import os
import time


def setup_logging(service: str) -> logging.Logger:
    logging.Formatter.converter = time.gmtime
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format=f"%(asctime)sZ %(levelname)-7s {service} %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return logging.getLogger(service)
