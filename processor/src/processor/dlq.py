"""Dead-letter publishing for the processor: undecodable messages -> dlq.<source_topic>.

The DLQ message carries the *original bytes* as its value and the failure context in headers,
so ``ov-replay dlq`` can re-decode with a fixed parser and re-inject into the source topic.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Protocol

from common.events import dlq_topic
from processor import metrics

log = logging.getLogger(__name__)


class _RawProducer(Protocol):
    def produce(self, topic: str, value: bytes, key, headers: list, on_delivery=None) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


def dead_letter_headers(
    source_topic: str,
    partition: int,
    offset: int,
    error: str,
    original_headers: list[tuple[str, bytes]] | None,
    stage: str = "decode",
) -> list[tuple[str, bytes]]:
    original = dict(original_headers or [])
    return [
        ("stage", stage.encode()),
        ("source_topic", source_topic.encode()),
        ("source_partition", str(partition).encode()),
        ("source_offset", str(offset).encode()),
        ("error", error.encode("utf-8")[:1024]),
        ("failed_at", datetime.now(tz=UTC).isoformat(timespec="seconds").encode()),
        ("schema_version", original.get("schema_version", b"?")),
        ("event_type", original.get("event_type", b"?")),
    ]


class DeadLetterPublisher:
    def __init__(self, raw: _RawProducer):
        self._raw = raw

    @classmethod
    def connect(cls, bootstrap: str) -> DeadLetterPublisher:
        from confluent_kafka import Producer

        return cls(
            Producer(
                {
                    "bootstrap.servers": bootstrap,
                    "client.id": "ov-processor-dlq",
                    "enable.idempotence": True,
                    "acks": "all",
                }
            )
        )

    def send(self, msg, error: str) -> str:
        topic = dlq_topic(msg.topic())
        self._raw.produce(
            topic,
            value=msg.value() or b"",
            key=msg.key(),
            headers=dead_letter_headers(
                msg.topic(), msg.partition(), msg.offset(), error, msg.headers()
            ),
        )
        self._raw.poll(0)
        metrics.DLQ.labels(msg.topic()).inc()
        log.warning(
            "dlq: %s[%d]@%d -> %s (%s)", msg.topic(), msg.partition(), msg.offset(), topic, error
        )
        return topic

    def flush(self) -> None:
        self._raw.flush(30)
