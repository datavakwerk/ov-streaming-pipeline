"""Pipeline health strip: a few instant queries against Prometheus (optional, degrades to '—')."""

from __future__ import annotations

import httpx

QUERIES = {
    "msg_in_per_s": "sum(rate(producer_published_total[2m]))",
    "msg_processed_per_s": "sum(rate(processor_messages_total[2m]))",
    "consumer_lag": "sum(processor_consumer_lag)",
    # `or vector(0)`: a counter with no samples yet must read 0, not make the whole sum empty.
    "dlq_1h": "(sum(increase(processor_dlq_total[1h])) or vector(0)) "
    "+ (sum(increase(producer_dlq_total[1h])) or vector(0))",
    "late_1h": "sum(increase(processor_late_events_total[1h])) or vector(0)",
    "windows_1h": "sum(increase(processor_windows_finalized_total[1h]))",
    "watermark_lag_s": "processor_watermark_lag_seconds",
    "archive_objects_1h": "sum(increase(archiver_objects_total[1h]))",
}


def pipeline_stats(prometheus_url: str, timeout_s: float = 2.0) -> dict[str, float | None]:
    out: dict[str, float | None] = dict.fromkeys(QUERIES)
    try:
        with httpx.Client(base_url=prometheus_url, timeout=timeout_s) as client:
            for key, expr in QUERIES.items():
                r = client.get("/api/v1/query", params={"query": expr})
                result = r.json().get("data", {}).get("result", [])
                if result:
                    out[key] = float(result[0]["value"][1])
    except (httpx.HTTPError, ValueError, KeyError):
        pass
    return out
