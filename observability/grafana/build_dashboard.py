"""Generate dashboards/pipeline.json. Hand-editing Grafana JSON is misery; this is not.

    uv run python observability/grafana/build_dashboard.py

The dashboard answers PRD question 4 for ops: consumer lag, throughput (msg/s), DLQ rate,
late-event rate, window processing time — the panels the 24-hour-run screenshot is taken from.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).parent / "dashboards" / "pipeline.json"
PROM = {"type": "prometheus", "uid": "prometheus"}

_id = 0


def next_id() -> int:
    global _id
    _id += 1
    return _id


def target(expr: str, legend: str = "") -> dict:
    return {
        "datasource": PROM,
        "expr": expr,
        "legendFormat": legend,
        "refId": chr(64 + next_id() % 26 + 1),
    }


def timeseries(
    title: str,
    targets: list[dict],
    *,
    x: int,
    y: int,
    w: int = 8,
    h: int = 7,
    unit: str = "short",
    stack: bool = False,
    thresholds: list | None = None,
    fill: int = 12,
    desc: str = "",
) -> dict:
    custom = {
        "lineWidth": 2,
        "fillOpacity": fill,
        "showPoints": "never",
        "spanNulls": True,
        "gradientMode": "opacity",
    }
    if stack:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    panel = {
        "id": next_id(),
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": targets,
        "fieldConfig": {
            "defaults": {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}},
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "calcs": ["mean", "max"]},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }
    if thresholds:
        panel["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
        panel["fieldConfig"]["defaults"]["custom"]["thresholdsStyle"] = {"mode": "line"}
    return panel


def stat(
    title: str,
    expr: str,
    *,
    x: int,
    y: int,
    w: int = 4,
    h: int = 4,
    unit: str = "short",
    decimals: int = 0,
    thresholds: list | None = None,
    desc: str = "",
    color_mode: str = "value",
) -> dict:
    steps = thresholds or [{"color": "green", "value": None}]
    return {
        "id": next_id(),
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": PROM,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [target(expr)],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": decimals,
                "thresholds": {"mode": "absolute", "steps": steps},
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": color_mode,
            "graphMode": "area",
            "textMode": "value",
            "justifyMode": "center",
        },
    }


def row(title: str, y: int) -> dict:
    return {
        "id": next_id(),
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def green_amber_red(amber: float, red: float) -> list[dict]:
    return [
        {"color": "green", "value": None},
        {"color": "orange", "value": amber},
        {"color": "red", "value": red},
    ]


panels = [
    # ------------------------------------------------------------- headline stats
    stat(
        "Messages in / s",
        "sum(rate(producer_published_total[2m]))",
        x=0,
        y=0,
        unit="reqps",
        decimals=1,
        desc="Events published by the producer (after snapshot diffing).",
    ),
    stat(
        "Messages processed / s",
        "sum(rate(processor_messages_total[2m]))",
        x=4,
        y=0,
        unit="reqps",
        decimals=1,
    ),
    stat(
        "Consumer lag",
        "sum(processor_consumer_lag)",
        x=8,
        y=0,
        thresholds=green_amber_red(2000, 10000),
        desc="Messages between the processor's position and the partition high watermarks, summed.",
    ),
    stat(
        "Watermark lag",
        "processor_watermark_lag_seconds",
        x=12,
        y=0,
        unit="s",
        thresholds=green_amber_red(300, 600),
        desc="now - event-time watermark. Steady state ≈ allowed lateness (120 s) + feed cadence (60 s).",
    ),
    stat(
        "DLQ / h",
        "sum(increase(processor_dlq_total[1h])) + sum(increase(producer_dlq_total[1h]))",
        x=16,
        y=0,
        thresholds=green_amber_red(50, 500),
        color_mode="background",
    ),
    stat(
        "Late events / h",
        "sum(increase(processor_late_events_total[1h]))",
        x=20,
        y=0,
        thresholds=green_amber_red(1000, 10000),
    ),
    # ------------------------------------------------------------- throughput
    row("Throughput", 4),
    timeseries(
        "Published per second, by topic",
        [target("sum by (topic) (rate(producer_published_total[1m]))", "{{topic}}")],
        x=0,
        y=5,
        unit="reqps",
        stack=True,
    ),
    timeseries(
        "Consumed per second, by consumer",
        [
            target("sum by (topic) (rate(processor_messages_total[1m]))", "processor {{topic}}"),
            target("sum by (topic) (rate(archiver_messages_total[1m]))", "archiver {{topic}}"),
        ],
        x=8,
        y=5,
        unit="reqps",
    ),
    timeseries(
        "Feed entities per poll, by outcome",
        [
            target(
                "sum by (feed, outcome) (increase(producer_entities_total[1m]))",
                "{{feed}} {{outcome}}",
            )
        ],
        x=16,
        y=5,
        stack=True,
        desc="event = published, unchanged = suppressed by the snapshot differ, skipped = filtered (inactive trips, ghost vehicles), error = dead-lettered.",
    ),
    # ------------------------------------------------------------- lag & latency
    row("Lag & latency", 12),
    timeseries(
        "Consumer lag per partition",
        [target("processor_consumer_lag", "{{topic}}[{{partition}}]")],
        x=0,
        y=13,
        thresholds=[{"color": "transparent", "value": None}, {"color": "red", "value": 5000}],
        desc="PRD N2: must stay bounded, never grow monotonically.",
    ),
    timeseries(
        "Event-time watermark lag",
        [
            target("processor_watermark_lag_seconds", "watermark lag"),
            target("producer_feed_age_seconds", "feed age {{feed}}"),
        ],
        x=8,
        y=13,
        unit="s",
    ),
    timeseries(
        "Processing time (p95)",
        [
            target(
                "histogram_quantile(0.95, sum by (le) (rate(processor_batch_seconds_bucket[5m])))",
                "batch p95",
            ),
            target(
                "histogram_quantile(0.95, sum by (le) (rate(processor_upsert_seconds_bucket[5m])))",
                "window upsert p95",
            ),
            target(
                "histogram_quantile(0.95, sum by (le, feed) (rate(producer_poll_seconds_bucket[5m])))",
                "poll p95 {{feed}}",
            ),
        ],
        x=16,
        y=13,
        unit="s",
    ),
    # ------------------------------------------------------------- correctness
    row("Correctness: DLQ, lateness, windows", 20),
    timeseries(
        "Dead-letter rate",
        [
            target(
                "sum by (source_topic) (rate(processor_dlq_total[5m]))",
                "processor {{source_topic}}",
            ),
            target("sum by (topic) (rate(producer_dlq_total[5m]))", "producer {{topic}}"),
        ],
        x=0,
        y=21,
        unit="reqps",
        fill=30,
    ),
    timeseries(
        "Late & duplicate events",
        [
            target("rate(processor_late_events_total[5m])", "late (dropped)"),
            target("rate(processor_duplicate_events_total[5m])", "duplicates (deduped)"),
        ],
        x=8,
        y=21,
        unit="reqps",
    ),
    timeseries(
        "Windows finalized per 5 minutes",
        [
            target(
                "sum by (dimension) (increase(processor_windows_finalized_total[5m]))",
                "{{dimension}}",
            )
        ],
        x=16,
        y=21,
        stack=True,
    ),
    # ------------------------------------------------------------- state
    row("State & archive", 28),
    timeseries(
        "Open windows / observations held",
        [
            target("processor_open_windows", "open windows"),
            target("processor_open_observations", "observations in open windows"),
            target("processor_uncommitted_messages", "uncommitted messages"),
        ],
        x=0,
        y=29,
    ),
    timeseries(
        "Snapshot differ keys (fleet size seen)",
        [target("producer_differ_keys", "{{feed}}")],
        x=8,
        y=29,
    ),
    timeseries(
        "Archive: objects and bytes per hour",
        [
            target("sum(increase(archiver_objects_total[1h]))", "objects / h"),
            target("increase(archiver_bytes_total[1h])", "bytes / h"),
        ],
        x=16,
        y=29,
    ),
    # ------------------------------------------------------------- broker
    row("Redpanda", 36),
    timeseries(
        "Broker requests / s",
        [
            target(
                "sum by (redpanda_request) (rate(redpanda_kafka_request_latency_seconds_count[1m]))",
                "{{redpanda_request}}",
            )
        ],
        x=0,
        y=37,
        unit="reqps",
    ),
    timeseries(
        "Broker memory",
        [
            target("redpanda_memory_allocated_memory", "allocated"),
            target("redpanda_memory_available_memory", "available"),
        ],
        x=8,
        y=37,
        unit="bytes",
    ),
    timeseries(
        "Partition high watermarks",
        [target("sum by (redpanda_topic) (redpanda_kafka_max_offset)", "{{redpanda_topic}}")],
        x=16,
        y=37,
    ),
]

dashboard = {
    "uid": "ov-pipeline",
    "title": "OV streaming pipeline",
    "tags": ["ov", "kafka", "streaming"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "10s",
    "time": {"from": "now-1h", "to": "now"},
    "editable": True,
    "graphTooltip": 1,
    "panels": panels,
    "templating": {"list": []},
    "annotations": {"list": []},
}

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(dashboard, indent=2) + "\n")
print(f"wrote {OUT} ({len(panels)} panels)")
