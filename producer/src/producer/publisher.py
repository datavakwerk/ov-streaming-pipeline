"""Thin wrapper around confluent-kafka's Producer with delivery accounting.

Keying (interview question, PRD golden rule): positions are keyed by ``vehicle_id`` and
predictions by ``trip_id``. A stable key means a stable partition, so all updates for one
vehicle/trip are totally ordered on a single partition — which is what lets the processor's
"latest state" upserts and per-key dedupe reason about ordering at all.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from common.events import TOPIC_FOR_EVENT, Event, dlq_topic
from common.serde import encode, headers_for
from producer import metrics

log = logging.getLogger(__name__)


class _RawProducer(Protocol):
    """The part of confluent_kafka.Producer we use — so tests can inject a fake."""

    def produce(
        self, topic: str, value: bytes, key: bytes, headers: list, on_delivery: Callable
    ) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


def default_config(bootstrap: str, client_id: str = "ov-producer") -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        # Idempotent producer: retries never reorder or duplicate within a partition.
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 50,
        "batch.num.messages": 10_000,
        "compression.type": "lz4",
        "message.timeout.ms": 60_000,
    }


class KafkaPublisher:
    def __init__(self, raw: _RawProducer):
        self._raw = raw
        self.delivery_errors = 0

    @classmethod
    def connect(cls, bootstrap: str) -> KafkaPublisher:
        from confluent_kafka import Producer

        return cls(Producer(default_config(bootstrap)))

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.delivery_errors += 1
            metrics.DELIVERY_ERRORS.labels(msg.topic()).inc()
            log.error("delivery failed topic=%s key=%s: %s", msg.topic(), msg.key(), err)
        else:
            metrics.DELIVERED.labels(msg.topic()).inc()

    def publish(self, event: Event) -> str:
        topic = TOPIC_FOR_EVENT[type(event)]
        self._raw.produce(
            topic,
            value=encode(event),
            key=event.key.encode("utf-8"),
            headers=headers_for(event),
            on_delivery=self._on_delivery,
        )
        metrics.PUBLISHED.labels(topic).inc()
        self._raw.poll(0)  # serve delivery callbacks without blocking
        return topic

    def publish_dead_letter(
        self, source_topic: str, key: str, raw: bytes, reason: str, **extra: str
    ) -> str:
        """Parse-time DLQ: the original entity bytes plus error metadata headers (PRD F5)."""
        topic = dlq_topic(source_topic)
        headers = [
            ("stage", b"parse"),
            ("source_topic", source_topic.encode()),
            ("error", reason.encode("utf-8")[:1024]),
            ("failed_at", datetime.now(tz=UTC).isoformat(timespec="seconds").encode()),
            *((k, v.encode("utf-8")) for k, v in extra.items()),
        ]
        self._raw.produce(
            topic,
            value=raw,
            key=key.encode("utf-8"),
            headers=headers,
            on_delivery=self._on_delivery,
        )
        metrics.DLQ.labels(source_topic).inc()
        self._raw.poll(0)
        return topic

    def flush(self, timeout_s: float = 30.0) -> int:
        remaining = self._raw.flush(timeout_s)
        if remaining:
            log.warning("%d messages still undelivered after flush", remaining)
        return remaining
