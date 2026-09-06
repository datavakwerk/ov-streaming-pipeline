"""Prometheus metrics for the producer (scraped on :9101, PRD F8)."""

from prometheus_client import Counter, Gauge, Histogram

POLLS = Counter("producer_polls_total", "Feed polls by outcome", ["feed", "status"])
POLL_SECONDS = Histogram(
    "producer_poll_seconds",
    "Wall time of one poll (fetch + parse + publish)",
    ["feed"],
    buckets=(0.1, 0.25, 0.5, 1, 2, 5, 10, 30),
)
ENTITIES = Counter(
    "producer_entities_total",
    "Feed entities by outcome: event, error, skipped, unchanged",
    ["feed", "outcome"],
)
PUBLISHED = Counter("producer_published_total", "Messages handed to librdkafka", ["topic"])
DELIVERED = Counter("producer_delivered_total", "Messages acknowledged by the broker", ["topic"])
DELIVERY_ERRORS = Counter("producer_delivery_errors_total", "Broker delivery failures", ["topic"])
DLQ = Counter("producer_dlq_total", "Entities sent to the DLQ at parse time", ["topic"])
FEED_AGE = Gauge(
    "producer_feed_age_seconds", "now - feed header timestamp at the last poll", ["feed"]
)
LAST_POLL = Gauge("producer_last_poll_timestamp", "Unix time of the last poll", ["feed"])
DIFFER_KEYS = Gauge("producer_differ_keys", "Keys tracked by the snapshot differ", ["feed"])
PAYLOAD_BYTES = Histogram(
    "producer_payload_bytes",
    "Size of a fetched feed body",
    ["feed"],
    buckets=(10_000, 100_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000),
)
