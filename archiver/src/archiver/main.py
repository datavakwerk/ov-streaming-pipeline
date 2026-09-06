"""Archiver service: own consumer group, raw bytes -> MinIO, commit only after a successful put.

At-least-once here too: a crash between put and commit re-archives the same offset range, which
overwrites the same object (offset ranges are in the key). Duplicate lines are impossible for
that reason, and replay dedups by (topic, partition, offset) regardless.
"""

from __future__ import annotations

import signal
import threading

from confluent_kafka import Consumer, KafkaError, TopicPartition

from archiver import metrics
from archiver.sink import ArchiveSink, RawMessage, S3Store
from common.config import env, env_float, env_int
from common.events import TOPICS
from common.logging import setup_logging
from common.metrics import start_metrics_server

log = setup_logging("archiver")


def consumer_config(bootstrap: str) -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": "archiver",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 30_000,
        "client.id": "ov-archiver",
    }


def to_raw(msg) -> RawMessage:
    return RawMessage(
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        timestamp_ms=msg.timestamp()[1],
        key=msg.key(),
        headers=list(msg.headers() or []),
        payload=msg.value() or b"",
    )


class Archiver:
    def __init__(self, consumer: Consumer, sink: ArchiveSink):
        self.consumer = consumer
        self.sink = sink
        # Highest offset seen per partition that is *not yet* durable in the archive.
        self._uncommitted: dict[tuple[str, int], int] = {}
        self._bytes_seen = 0

    def _commit_flushed(self, flushed_keys: list[str]) -> None:
        """After a flush, commit offsets for the partitions whose buffers are now empty."""
        if not flushed_keys:
            return
        offsets = []
        for (topic, partition), offset in list(self._uncommitted.items()):
            if any(f"/{topic}/" in k and f"/p{partition}-" in k for k in flushed_keys):
                offsets.append(TopicPartition(topic, partition, offset + 1))
                del self._uncommitted[(topic, partition)]
        if offsets:
            self.consumer.commit(offsets=offsets, asynchronous=False)
            metrics.COMMITS.inc()

    def run(self, stop: threading.Event) -> None:
        self.consumer.subscribe(list(TOPICS))
        log.info("archiving %s", TOPICS)
        while not stop.is_set():
            msg = self.consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error("consumer error: %s", msg.error())
                    continue
                raw = to_raw(msg)
                metrics.CONSUMED.labels(raw.topic).inc()
                self._uncommitted[(raw.topic, raw.partition)] = raw.offset
                self._after_flush(self.sink.add(raw))
            self._after_flush(self.sink.flush_due())
            metrics.PENDING.set(self.sink.pending())
        log.info("shutting down: flushing %d buffered messages", self.sink.pending())
        self._after_flush(self.sink.flush_all())
        self.consumer.close()

    def _after_flush(self, flushed: list[str]) -> None:
        if not flushed:
            return
        for key in flushed:
            metrics.OBJECTS.labels(key.split("/")[1]).inc()
        metrics.BYTES.inc(self.sink.bytes_written - self._bytes_seen)
        self._bytes_seen = self.sink.bytes_written
        self._commit_flushed(flushed)


def main() -> None:
    start_metrics_server(env_int("METRICS_PORT", 9103))
    store = S3Store(
        endpoint=env("MINIO_ENDPOINT", "http://localhost:9010"),
        access_key=env("MINIO_ROOT_USER", "minioadmin"),
        secret_key=env("MINIO_ROOT_PASSWORD", "minioadmin"),
        bucket=env("MINIO_BUCKET", "raw-events"),
    )
    sink = ArchiveSink(
        store,
        prefix=env("ARCHIVE_PREFIX", "raw-events"),
        max_messages=env_int("ARCHIVE_BATCH_MESSAGES", 1000),
        max_age_s=env_float("ARCHIVE_BATCH_SECONDS", 60),
    )
    consumer = Consumer(consumer_config(env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")))
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    Archiver(consumer, sink).run(stop)


if __name__ == "__main__":
    main()
