"""Pull the numbers the README quotes straight from Prometheus, so they are never hand-typed.

make throughput            # last hour
RANGE=24h make throughput  # the 24-hour run
"""

from __future__ import annotations

import os

import httpx

PROM = os.environ.get("PROMETHEUS_URL", "http://localhost:9090")
RANGE = os.environ.get("RANGE", "1h")

QUERIES = [
    ("Messages published / s (avg)", f"sum(rate(producer_published_total[{RANGE}]))", "{:.1f}"),
    (
        "Messages published / s (peak, 1m)",
        f"max_over_time(sum(rate(producer_published_total[1m]))[{RANGE}:1m])",
        "{:.1f}",
    ),
    ("Messages processed / s (avg)", f"sum(rate(processor_messages_total[{RANGE}]))", "{:.1f}"),
    (
        "Feed entities seen / poll (positions)",
        f'avg_over_time(sum(increase(producer_entities_total{{feed="positions"}}[1m]))[{RANGE}:1m]) / 3',
        "{:.0f}",
    ),
    (
        "Suppressed by differ (share)",
        f'sum(increase(producer_entities_total{{outcome="unchanged"}}[{RANGE}])) / sum(increase(producer_entities_total{{outcome=~"unchanged|event"}}[{RANGE}]))',
        "{:.1%}",
    ),
    (
        "Windows finalized / 5 min (avg)",
        f"sum(increase(processor_windows_finalized_total[{RANGE}])) / ({RANGE.rstrip('h') if RANGE.endswith('h') else 1} * 12)",
        "{:.0f}",
    ),
    ("Consumer lag (max)", f"max_over_time(sum(processor_consumer_lag)[{RANGE}:15s])", "{:.0f}"),
    (
        "Consumer lag (p50)",
        f"quantile_over_time(0.5, sum(processor_consumer_lag)[{RANGE}:15s])",
        "{:.0f}",
    ),
    (
        "Watermark lag s (p50)",
        f"quantile_over_time(0.5, processor_watermark_lag_seconds[{RANGE}])",
        "{:.0f}",
    ),
    (
        "Batch processing p95 s",
        f"histogram_quantile(0.95, sum by (le) (rate(processor_batch_seconds_bucket[{RANGE}])))",
        "{:.3f}",
    ),
    (
        "Window upsert p95 s",
        f"histogram_quantile(0.95, sum by (le) (rate(processor_upsert_seconds_bucket[{RANGE}])))",
        "{:.3f}",
    ),
    ("Late events dropped", f"sum(increase(processor_late_events_total[{RANGE}]))", "{:.0f}"),
    (
        "Dead letters (producer + processor)",
        f"sum(increase(producer_dlq_total[{RANGE}])) + sum(increase(processor_dlq_total[{RANGE}]))",
        "{:.0f}",
    ),
    ("Archive objects written", f"sum(increase(archiver_objects_total[{RANGE}]))", "{:.0f}"),
    ("Archive MB written", f"increase(archiver_bytes_total[{RANGE}]) / 1e6", "{:.1f}"),
]


def main() -> None:
    with httpx.Client(base_url=PROM, timeout=10) as client:
        print(f"| Metric (last {RANGE}) | Value |\n|---|---|")
        for label, expr, fmt in QUERIES:
            result = client.get("/api/v1/query", params={"query": expr}).json()["data"]["result"]
            value = fmt.format(float(result[0]["value"][1])) if result else "n/a"
            print(f"| {label} | {value} |")


if __name__ == "__main__":
    main()
