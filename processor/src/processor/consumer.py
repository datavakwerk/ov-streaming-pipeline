"""Manual-commit Kafka consumer loop around the Pipeline.

Offset policy (COMMITS.md commit 9): offsets advance only past messages whose windows are
finalized *and* upserted. Per partition we keep the consumed messages' (offset, window_end) in
arrival order; after each finalization we pop the prefix whose window_end <= watermark and
commit one past the last popped offset. Everything still in an open window is redelivered after
a crash and rebuilds that window from scratch. Because the windower's watermark is seeded from
the newest window already in Postgres, redelivered messages of *finalized* windows are treated
as late and never overwrite a complete row with a partial recomputation.

Positions (no windows) are committed once their batch has been upserted into vehicle_latest.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from confluent_kafka import Consumer, KafkaError, TopicPartition

from common.events import TOPIC_ARRIVAL_PREDICTIONS, TOPIC_VEHICLE_POSITIONS
from common.serde import SchemaError
from processor import metrics
from processor.dlq import DeadLetterPublisher
from processor.pipeline import Pipeline
from processor.windows import window_start_for

log = logging.getLogger(__name__)


def consumer_config(bootstrap: str, group_id: str = "processor") -> dict:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 600_000,
        "session.timeout.ms": 45_000,
        "partition.assignment.strategy": "cooperative-sticky",
        "client.id": "ov-processor",
    }


@dataclass
class _PartitionLedger:
    """Uncommitted messages of one partition: (offset, window_end) in consumption order."""

    pending: deque = field(default_factory=deque)
    committable: int | None = None  # offset to commit (one past the last durable message)

    def add(self, offset: int, window_end: datetime | None) -> None:
        self.pending.append((offset, window_end))

    def release(self, watermark: datetime | None, positions_flushed: bool) -> None:
        while self.pending:
            offset, window_end = self.pending[0]
            if window_end is None:  # a position: durable once the state flush happened
                if not positions_flushed:
                    break
            elif watermark is None or window_end > watermark:
                break
            self.pending.popleft()
            self.committable = offset + 1


class ProcessorLoop:
    def __init__(
        self,
        consumer: Consumer,
        pipeline: Pipeline,
        dlq: DeadLetterPublisher,
        *,
        idle_finalize_s: float = 90.0,
        state_flush_s: float = 5.0,
        lag_report_s: float = 15.0,
        purge_s: float = 300.0,
    ):
        self.consumer = consumer
        self.pipeline = pipeline
        self.dlq = dlq
        self.idle_finalize_s = idle_finalize_s
        self.state_flush_s = state_flush_s
        self.lag_report_s = lag_report_s
        self.purge_s = purge_s
        self._ledgers: dict[tuple[str, int], _PartitionLedger] = {}
        self._last_event_at = time.monotonic()
        self._last_flush = time.monotonic()
        self._last_lag = 0.0
        self._last_purge = time.monotonic()

    # --------------------------------------------------------------- handlers
    def handle(self, msg) -> None:
        topic = msg.topic()
        metrics.CONSUMED.labels(topic).inc()
        ledger = self._ledgers.setdefault((topic, msg.partition()), _PartitionLedger())
        try:
            event = self.pipeline.decode(msg.value())
        except SchemaError as exc:
            metrics.DECODE_ERRORS.labels(topic).inc()
            self.dlq.send(msg, str(exc))
            ledger.add(msg.offset(), None)  # dead-lettered = handled; commit like a position
            return
        self.pipeline.ingest(event, source=(topic, msg.partition()))
        window_end = None
        if topic == TOPIC_ARRIVAL_PREDICTIONS:
            window_end = window_start_for(
                event.timestamp, self.pipeline.windower.window_s
            ) + timedelta(seconds=self.pipeline.windower.window_s)
        ledger.add(msg.offset(), window_end)
        self._last_event_at = time.monotonic()

    def tick(self) -> None:
        """Periodic work: flush state tables, finalize windows, commit offsets, report lag."""
        now_mono = time.monotonic()
        flushed = False
        if now_mono - self._last_flush >= self.state_flush_s:
            self.pipeline.flush_state()
            self._last_flush = now_mono
            flushed = True
        idle = now_mono - self._last_event_at
        if idle >= self.idle_finalize_s:
            self.pipeline.finalize(now=datetime.now(tz=UTC), idle_s=idle)
        else:
            self.pipeline.finalize()
        if flushed:
            self._commit()
        if now_mono - self._last_lag >= self.lag_report_s:
            self._report_lag()
            self._last_lag = now_mono
        if now_mono - self._last_purge >= self.purge_s:
            self.pipeline.sink.purge_stale_state()
            self._last_purge = now_mono

    # ---------------------------------------------------------------- commits
    def _commit(self) -> None:
        watermark = self.pipeline.windower.watermark
        offsets = []
        for (topic, partition), ledger in self._ledgers.items():
            ledger.release(watermark, positions_flushed=True)
            if ledger.committable is not None:
                offsets.append(TopicPartition(topic, partition, ledger.committable))
                ledger.committable = None
        if offsets:
            try:
                self.consumer.commit(offsets=offsets, asynchronous=False)
                metrics.COMMITS.inc()
            except Exception as exc:
                log.warning("commit failed (will retry next tick): %s", exc)
        metrics.UNCOMMITTED.set(sum(len(ledger.pending) for ledger in self._ledgers.values()))

    def _report_lag(self) -> None:
        try:
            assignment = self.consumer.assignment()
            if not assignment:
                return
            positions = {
                (tp.topic, tp.partition): tp.offset for tp in self.consumer.position(assignment)
            }
            for tp in assignment:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=2.0, cached=True)
                pos = positions.get((tp.topic, tp.partition))
                if pos is not None and pos >= 0 and high >= 0:
                    metrics.CONSUMER_LAG.labels(tp.topic, str(tp.partition)).set(max(0, high - pos))
        except Exception as exc:
            log.debug("lag report failed: %s", exc)

    # ------------------------------------------------------------------- run
    def run(self, stop) -> None:
        self.consumer.subscribe([TOPIC_VEHICLE_POSITIONS, TOPIC_ARRIVAL_PREDICTIONS])
        log.info("consuming %s", [TOPIC_VEHICLE_POSITIONS, TOPIC_ARRIVAL_PREDICTIONS])
        while not stop.is_set():
            batch = self.consumer.consume(num_messages=2000, timeout=1.0)
            with metrics.BATCH_SECONDS.time():
                for msg in batch:
                    if msg.error():
                        if msg.error().code() != KafkaError._PARTITION_EOF:
                            log.error("consumer error: %s", msg.error())
                        continue
                    self.handle(msg)
                self.tick()
        log.info("shutting down: flushing state, finalizing what the watermark allows")
        self.pipeline.flush_state()
        self.pipeline.finalize()
        self._commit()
        self.dlq.flush()
        self.consumer.close()
