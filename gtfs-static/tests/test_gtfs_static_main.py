"""load_once: download failures fall back to the cached zip; unchanged feeds are skipped."""

from pathlib import Path

import httpx
import pytest

from gtfs_static import main as sidecar

FIXTURE = Path(__file__).parent / "fixtures" / "gtfs-mini.zip"


class FakeConn:
    def __init__(self, meta=None):
        self.meta = dict(meta or {})


def _patch(monkeypatch, conn, download, loaded):
    monkeypatch.setattr(sidecar, "_meta_get", lambda c, k: conn.meta.get(k))
    monkeypatch.setattr(sidecar, "_meta_set", lambda c, k, v: conn.meta.__setitem__(k, v))
    monkeypatch.setattr(sidecar, "download", download)
    monkeypatch.setattr(
        sidecar, "load_zip", lambda c, path: loaded.append(path) or {"ref_routes": 5}
    )


def test_download_failure_falls_back_to_cached_zip(monkeypatch, tmp_path):
    cache = tmp_path / "gtfs-nl.zip"
    cache.write_bytes(FIXTURE.read_bytes())
    conn, loaded = FakeConn(), []

    def failing_download(url, dest, ims):
        raise httpx.HTTPStatusError(
            "429", request=httpx.Request("GET", url), response=httpx.Response(429)
        )

    _patch(monkeypatch, conn, failing_download, loaded)
    assert sidecar.load_once(conn, "http://feed/gtfs.zip", cache, force=False) is True
    assert loaded == [str(cache)]
    assert "gtfs_loaded_at" in conn.meta


def test_download_failure_without_cache_raises(monkeypatch, tmp_path):
    conn, loaded = FakeConn(), []

    def failing_download(url, dest, ims):
        raise httpx.ConnectError("no network")

    _patch(monkeypatch, conn, failing_download, loaded)
    with pytest.raises(httpx.ConnectError):
        sidecar.load_once(conn, "http://feed/gtfs.zip", tmp_path / "missing.zip", force=False)
    assert loaded == []


def test_unchanged_feed_is_not_reloaded(monkeypatch, tmp_path):
    cache = tmp_path / "gtfs-nl.zip"
    cache.write_bytes(FIXTURE.read_bytes())
    conn = FakeConn({"gtfs_last_modified": "Sun, 07 Sep 2026 09:03:00 GMT", "gtfs_loaded_at": "x"})
    loaded = []
    _patch(monkeypatch, conn, lambda url, dest, ims: None, loaded)
    assert sidecar.load_once(conn, "http://feed/gtfs.zip", cache, force=False) is False
    assert loaded == []
