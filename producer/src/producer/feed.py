"""Polite HTTP client for the OVapi GTFS-RT endpoints.

Conditional GETs (ETag / If-Modified-Since) mean an unchanged feed costs a 304 and no bytes,
which matters at a 10 s poll interval against a public, free service.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import httpx

log = logging.getLogger(__name__)

POSITIONS_PATH = "vehiclePositions.pb"
TRIP_UPDATES_PATH = "tripUpdates.pb"


@dataclass(slots=True)
class FeedResponse:
    body: bytes | None  # None when the server answered 304 Not Modified
    status: int
    elapsed_s: float
    etag: str | None = None


@dataclass(slots=True)
class FeedClient:
    base_url: str
    timeout_s: float = 20.0
    user_agent: str = "ov-streaming-pipeline/0.1 (+portfolio project; polite poller)"
    _validators: dict[str, dict[str, str]] = field(default_factory=dict, init=False)
    _client: httpx.Client | None = field(default=None, init=False)

    def _http(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url.rstrip("/") + "/",
                timeout=self.timeout_s,
                headers={"User-Agent": self.user_agent, "Accept-Encoding": "gzip"},
                follow_redirects=True,
            )
        return self._client

    def fetch(self, path: str) -> FeedResponse:
        headers = dict(self._validators.get(path, {}))
        response = self._http().get(path, headers=headers)
        elapsed = response.elapsed.total_seconds()
        if response.status_code == 304:
            return FeedResponse(body=None, status=304, elapsed_s=elapsed)
        response.raise_for_status()
        validators: dict[str, str] = {}
        if etag := response.headers.get("ETag"):
            validators["If-None-Match"] = etag
        if last_modified := response.headers.get("Last-Modified"):
            validators["If-Modified-Since"] = last_modified
        self._validators[path] = validators
        return FeedResponse(
            body=response.content,
            status=response.status_code,
            elapsed_s=elapsed,
            etag=response.headers.get("ETag"),
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
