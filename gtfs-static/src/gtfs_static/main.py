"""gtfs-static sidecar: load once at startup, then refresh daily.

A plain ``while True: load(); sleep(24h)`` loop — a cron sidecar, not an orchestrator (Airflow
lives in another portfolio project). The zip is cached on a volume and re-downloaded only when
the server's Last-Modified changes, so restarts are cheap.
"""

from __future__ import annotations

import os
import signal
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
import psycopg
from prometheus_client import Counter, Gauge

from common.config import env, env_int, postgres_dsn
from common.logging import setup_logging
from common.metrics import start_metrics_server
from gtfs_static.loader import load_zip

log = setup_logging("gtfs-static")

LOADS = Counter("gtfs_static_loads_total", "Reference data loads", ["outcome"])
ROWS = Gauge("gtfs_static_rows", "Rows in reference tables after the last load", ["table"])
LAST_LOAD = Gauge("gtfs_static_last_load_timestamp", "Unix time of the last successful load")
LOAD_SECONDS = Gauge("gtfs_static_load_seconds", "Duration of the last load")


def _meta_get(conn: psycopg.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM ref_meta WHERE key = %s", (key,)).fetchone()
    return row[0] if row else None


def _meta_set(conn: psycopg.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO ref_meta (key, value, updated_at) VALUES (%s, %s, now()) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
        (key, value),
    )
    conn.commit()


def download(url: str, dest: Path, if_modified_since: str | None) -> str | None:
    """Stream the zip to ``dest``. Returns the new Last-Modified, or None if not modified."""
    headers = {"User-Agent": "ov-streaming-pipeline/0.1 (gtfs-static loader)"}
    if if_modified_since and dest.exists():
        headers["If-Modified-Since"] = if_modified_since
    with (
        httpx.Client(timeout=httpx.Timeout(60, read=600), follow_redirects=True) as client,
        client.stream("GET", url, headers=headers) as response,
    ):
        if response.status_code == 304:
            return None
        response.raise_for_status()
        last_modified = response.headers.get("Last-Modified", datetime.now(tz=UTC).isoformat())
        if if_modified_since == last_modified and dest.exists():
            return None  # server ignored the validator but nothing changed
        with tempfile.NamedTemporaryFile("wb", dir=dest.parent, delete=False) as fh:
            for chunk in response.iter_bytes(1 << 20):
                fh.write(chunk)
            tmp = Path(fh.name)
        tmp.replace(dest)
        log.info("downloaded %s (%.0f MB)", dest, dest.stat().st_size / 1e6)
        return last_modified


def load_once(conn: psycopg.Connection, url: str, cache: Path, force: bool) -> bool:
    started = time.monotonic()
    previous = None if force else _meta_get(conn, "gtfs_last_modified")
    last_modified = download(url, cache, previous)
    if last_modified is None and _meta_get(conn, "gtfs_loaded_at"):
        log.info("static GTFS unchanged since %s; nothing to do", previous)
        LOADS.labels("unchanged").inc()
        return False
    counts = load_zip(conn, str(cache))
    for table, n in counts.items():
        ROWS.labels(table).set(n)
    _meta_set(conn, "gtfs_last_modified", last_modified or previous or "")
    _meta_set(conn, "gtfs_loaded_at", datetime.now(tz=UTC).isoformat())
    elapsed = time.monotonic() - started
    LOAD_SECONDS.set(elapsed)
    LAST_LOAD.set(time.time())
    LOADS.labels("loaded").inc()
    log.info("reference data loaded in %.0fs: %s", elapsed, counts)
    return True


def main() -> None:
    start_metrics_server(env_int("METRICS_PORT", 9104))
    url = env("GTFS_STATIC_URL", "https://gtfs.ovapi.nl/nl/gtfs-nl.zip")
    cache = Path(env("GTFS_CACHE_DIR", "/cache")) / "gtfs-nl.zip"
    cache.parent.mkdir(parents=True, exist_ok=True)
    interval = env_int("GTFS_REFRESH_INTERVAL_S", 86_400)
    once = os.environ.get("GTFS_RUN_ONCE", "").lower() in {"1", "true"}
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())

    while not stop.is_set():
        try:
            with psycopg.connect(postgres_dsn()) as conn:
                load_once(conn, url, cache, force=False)
        except Exception:
            LOADS.labels("failed").inc()
            log.exception("load failed; retrying in 10 minutes")
            if once:
                raise
            stop.wait(600)
            continue
        if once:
            return
        stop.wait(interval)


if __name__ == "__main__":
    main()
