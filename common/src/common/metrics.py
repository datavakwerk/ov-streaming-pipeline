"""Prometheus exposition helper. Each service exposes /metrics on its own port (PRD F8)."""

from __future__ import annotations

import logging

from prometheus_client import start_http_server

log = logging.getLogger(__name__)


def start_metrics_server(port: int) -> None:
    start_http_server(port)
    log.info("metrics on :%d/metrics", port)
