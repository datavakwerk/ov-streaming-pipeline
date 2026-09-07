"""Producer service: poll OVapi -> diff -> publish, with graceful shutdown."""

from __future__ import annotations

import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx

from common.config import env, env_int, env_list
from common.events import TOPIC_ARRIVAL_PREDICTIONS, TOPIC_VEHICLE_POSITIONS
from common.logging import setup_logging
from common.metrics import start_metrics_server
from producer import metrics
from producer.differ import SnapshotDiffer
from producer.feed import POSITIONS_PATH, TRIP_UPDATES_PATH, FeedClient, RateLimited
from producer.parse import FeedError, ParseResult, parse_trip_updates, parse_vehicle_positions
from producer.publisher import KafkaPublisher

log = setup_logging("producer")


@dataclass(frozen=True, slots=True)
class Config:
    base_url: str
    bootstrap: str
    operator_filter: frozenset[str]
    positions_interval_s: int
    trip_updates_interval_s: int
    prediction_horizon_s: int
    heartbeat_s: int
    max_position_age_s: int
    metrics_port: int

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            base_url=env("OVAPI_BASE_URL", "https://gtfs.ovapi.nl/nl"),
            bootstrap=env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
            operator_filter=frozenset(op.upper() for op in env_list("OPERATOR_FILTER")),
            positions_interval_s=env_int("POSITIONS_POLL_INTERVAL_S", 20),
            trip_updates_interval_s=env_int("TRIP_UPDATES_POLL_INTERVAL_S", 60),
            prediction_horizon_s=env_int("PREDICTION_HORIZON_S", 900),
            heartbeat_s=env_int("HEARTBEAT_S", 60),
            max_position_age_s=env_int("MAX_POSITION_AGE_S", 600),
            metrics_port=env_int("METRICS_PORT", 9101),
        )


@dataclass(slots=True)
class Feed:
    name: str
    path: str
    topic: str
    interval_s: int
    differ: SnapshotDiffer
    # Parse errors repeat on every snapshot too; one DLQ message per distinct (entity, reason).
    error_differ: SnapshotDiffer = field(
        default_factory=lambda: SnapshotDiffer(
            key=lambda e: e.entity_id, fingerprint=lambda e: (e.reason, e.raw), max_age_s=None
        )
    )
    next_due: float = 0.0
    backoff_s: float = 0.0


class Producer:
    def __init__(self, cfg: Config, client: FeedClient, publisher: KafkaPublisher):
        self.cfg = cfg
        self.client = client
        self.publisher = publisher
        self.feeds = [
            Feed(
                "positions",
                POSITIONS_PATH,
                TOPIC_VEHICLE_POSITIONS,
                cfg.positions_interval_s,
                # A vehicle that did not report again has the same timestamp: nothing new.
                SnapshotDiffer(
                    key=lambda e: e.vehicle_id, fingerprint=lambda e: e.timestamp, max_age_s=None
                ),
            ),
            Feed(
                "trip_updates",
                TRIP_UPDATES_PATH,
                TOPIC_ARRIVAL_PREDICTIONS,
                cfg.trip_updates_interval_s,
                SnapshotDiffer(
                    key=lambda e: e.trip_id,
                    fingerprint=lambda e: (e.stop_id, e.delay_seconds),
                    max_age_s=cfg.heartbeat_s,
                ),
            ),
        ]

    def _parse(self, feed: Feed, body: bytes) -> ParseResult:
        if feed.name == "positions":
            return parse_vehicle_positions(body, operator_filter=self.cfg.operator_filter)
        return parse_trip_updates(
            body, operator_filter=self.cfg.operator_filter, horizon_s=self.cfg.prediction_horizon_s
        )

    def poll_once(self, feed: Feed) -> None:
        started = time.monotonic()
        try:
            response = self.client.fetch(feed.path)
        except RateLimited as exc:
            # OVapi is a free public service and does rate-limit (HTTP 429). Back off for the
            # advertised Retry-After, or exponentially up to 5 minutes, and say so loudly.
            feed.backoff_s = min(exc.retry_after_s or max(feed.backoff_s * 2, 30), 300)
            feed.next_due = time.monotonic() + feed.backoff_s
            metrics.POLLS.labels(feed.name, "429").inc()
            log.warning("%s: rate limited (429); backing off %ss", feed.name, feed.backoff_s)
            return
        except httpx.HTTPError as exc:
            metrics.POLLS.labels(feed.name, "error").inc()
            log.warning("%s: fetch failed: %s", feed.name, exc)
            return
        feed.backoff_s = 0
        metrics.LAST_POLL.labels(feed.name).set(time.time())
        if response.body is None:
            metrics.POLLS.labels(feed.name, "304").inc()
            log.debug("%s: not modified", feed.name)
            return
        metrics.POLLS.labels(feed.name, str(response.status)).inc()
        metrics.PAYLOAD_BYTES.labels(feed.name).observe(len(response.body))

        try:
            result = self._parse(feed, response.body)
        except FeedError as exc:
            # The whole payload is garbage: park it in the DLQ and carry on.
            self.publisher.publish_dead_letter(
                feed.topic, key="feed", raw=response.body, reason=str(exc)
            )
            log.error("%s: unparseable feed sent to DLQ: %s", feed.name, exc)
            return

        feed_age = (datetime.now(tz=UTC) - result.feed_timestamp).total_seconds()
        metrics.FEED_AGE.labels(feed.name).set(feed_age)

        # Parse errors repeat on every snapshot too: one DLQ message per distinct entity/reason.
        new_errors, _ = feed.error_differ.diff(result.errors)
        for err in new_errors:
            self.publisher.publish_dead_letter(
                feed.topic,
                key=err.entity_id,
                raw=err.raw,
                reason=err.reason,
                entity_id=err.entity_id,
            )

        events = result.events
        if feed.name == "positions":
            # Ghost vehicles: OVapi keeps reporting some positions that are hours or days old
            # (seen: a bus "STOPPED_AT" the same spot for eleven days). Keep them off the map.
            # Also seen: a vehicle clock 29 minutes *ahead* of the feed. Anything more than a
            # minute in the future is a broken clock, not a prediction.
            feed_ts = result.feed_timestamp.timestamp()
            fresh_after = feed_ts - self.cfg.max_position_age_s
            events = [e for e in events if fresh_after <= e.timestamp.timestamp() <= feed_ts + 60]
            result.skipped += len(result.events) - len(events)

        changed, stats = feed.differ.diff(events)
        for event in changed:
            self.publisher.publish(event)

        metrics.ENTITIES.labels(feed.name, "event").inc(len(changed))
        metrics.ENTITIES.labels(feed.name, "unchanged").inc(stats.unchanged)
        metrics.ENTITIES.labels(feed.name, "error").inc(len(new_errors))
        metrics.ENTITIES.labels(feed.name, "skipped").inc(result.skipped)
        metrics.DIFFER_KEYS.labels(feed.name).set(len(feed.differ))
        elapsed = time.monotonic() - started
        metrics.POLL_SECONDS.labels(feed.name).observe(elapsed)
        log.info(
            "%s: %d entities -> %d published (%d heartbeats), %d unchanged, %d skipped, "
            "%d errors (%d new -> dlq), feed age %.0fs, %.2fs",
            feed.name,
            len(events) + len(result.errors) + result.skipped,
            len(changed),
            stats.heartbeats,
            stats.unchanged,
            result.skipped,
            len(result.errors),
            len(new_errors),
            feed_age,
            elapsed,
        )

    def run(self, stop: threading.Event) -> None:
        log.info(
            "polling %s every %ss/%ss, operators=%s, bootstrap=%s",
            self.cfg.base_url,
            self.cfg.positions_interval_s,
            self.cfg.trip_updates_interval_s,
            sorted(self.cfg.operator_filter) or "ALL",
            self.cfg.bootstrap,
        )
        while not stop.is_set():
            now = time.monotonic()
            for feed in self.feeds:
                if now >= feed.next_due:
                    feed.next_due = now + feed.interval_s
                    self.poll_once(feed)
            sleep_for = max(0.0, min(f.next_due for f in self.feeds) - time.monotonic())
            stop.wait(min(sleep_for, 1.0))
        log.info("shutting down: flushing producer")
        self.publisher.flush()
        self.client.close()


def main() -> None:
    cfg = Config.from_env()
    start_metrics_server(cfg.metrics_port)
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    Producer(cfg, FeedClient(cfg.base_url), KafkaPublisher.connect(cfg.bootstrap)).run(stop)


if __name__ == "__main__":
    main()
