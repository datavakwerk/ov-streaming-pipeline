"""Prometheus metrics for the processor (scraped on :9102). These are the Grafana panels."""

from prometheus_client import Counter, Gauge, Histogram

CONSUMED = Counter("processor_messages_total", "Messages consumed", ["topic"])
DECODE_ERRORS = Counter("processor_decode_errors_total", "Messages that failed decoding", ["topic"])
DLQ = Counter("processor_dlq_total", "Messages sent to the DLQ", ["source_topic"])
OBSERVATIONS = Counter(
    "processor_observations_total", "Observations routed into windows", ["dimension"]
)
LATE = Counter("processor_late_events_total", "Events dropped as late (behind the watermark)")
DUPLICATES = Counter("processor_duplicate_events_total", "Events deduped inside an open window")
WINDOWS_FINALIZED = Counter("processor_windows_finalized_total", "Windows finalized", ["dimension"])
UPSERTS = Counter("processor_upsert_rows_total", "Rows upserted", ["table"])
BATCH_SECONDS = Histogram(
    "processor_batch_seconds",
    "Processing time of one poll batch",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
UPSERT_SECONDS = Histogram(
    "processor_upsert_seconds",
    "Time to upsert one batch of finalized windows",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5),
)
WATERMARK_LAG = Gauge("processor_watermark_lag_seconds", "now - event-time watermark")
OPEN_WINDOWS = Gauge("processor_open_windows", "Windows currently open")
OPEN_OBSERVATIONS = Gauge("processor_open_observations", "Observations held in open windows")
CONSUMER_LAG = Gauge(
    "processor_consumer_lag", "Messages behind the partition high watermark", ["topic", "partition"]
)
COMMITS = Counter("processor_commits_total", "Offset commits")
UNCOMMITTED = Gauge("processor_uncommitted_messages", "Consumed but not yet committed messages")
ACTIVE_SOURCES = Gauge("processor_active_sources", "Partitions contributing to the watermark")
