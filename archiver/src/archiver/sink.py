"""Raw messages -> gzipped JSON-lines objects in S3-compatible storage.

Deliberately dumb: bytes in, bytes out, no parsing (PRD F2). One JSON line per Kafka message:

    {"topic", "partition", "offset", "timestamp_ms", "key": base64, "headers": {name: base64},
     "payload": base64}

Object key layout::

    raw-events/<topic>/date=YYYY-MM-DD/hour=HH/p<partition>-<first_offset>-<last_offset>.jsonl.gz

Batches are per (topic, partition) so the offset range in the file name is exact and
contiguous, which makes replay idempotent: re-archiving the same range overwrites the same
object. Buffers flush on size *or* age so the overnight lull still produces files.
"""

from __future__ import annotations

import base64
import gzip
import io
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Protocol

log = logging.getLogger(__name__)


class ObjectStore(Protocol):
    def put(self, key: str, body: bytes, content_type: str) -> None: ...


@dataclass(frozen=True, slots=True)
class RawMessage:
    topic: str
    partition: int
    offset: int
    timestamp_ms: int
    key: bytes | None
    headers: list[tuple[str, bytes]]
    payload: bytes

    def to_line(self) -> bytes:
        record = {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "timestamp_ms": self.timestamp_ms,
            "key": _b64(self.key),
            "headers": {k: _b64(v) for k, v in self.headers},
            "payload": _b64(self.payload),
        }
        return json.dumps(record, separators=(",", ":")).encode("utf-8") + b"\n"

    @classmethod
    def from_line(cls, line: bytes | str) -> RawMessage:
        record = json.loads(line)
        return cls(
            topic=record["topic"],
            partition=record["partition"],
            offset=record["offset"],
            timestamp_ms=record["timestamp_ms"],
            key=_unb64(record["key"]),
            headers=[(k, _unb64(v) or b"") for k, v in record.get("headers", {}).items()],
            payload=_unb64(record["payload"]) or b"",
        )


def _b64(value: bytes | None) -> str | None:
    return None if value is None else base64.b64encode(value).decode("ascii")


def _unb64(value: str | None) -> bytes | None:
    return None if value is None else base64.b64decode(value)


def object_key(
    prefix: str, topic: str, partition: int, first_offset: int, last_offset: int, at: datetime
) -> str:
    at = at.astimezone(UTC)
    return (
        f"{prefix}/{topic}/date={at:%Y-%m-%d}/hour={at:%H}/"
        f"p{partition}-{first_offset:012d}-{last_offset:012d}.jsonl.gz"
    )


@dataclass(slots=True)
class _Buffer:
    messages: list[RawMessage] = field(default_factory=list)
    opened_at: float = field(default_factory=time.monotonic)


@dataclass
class ArchiveSink:
    store: ObjectStore
    prefix: str = "raw-events"
    max_messages: int = 1000
    max_age_s: float = 60.0
    clock: type(time.monotonic) = time.monotonic
    _buffers: dict[tuple[str, int], _Buffer] = field(default_factory=dict, init=False, repr=False)
    objects_written: int = field(default=0, init=False)
    bytes_written: int = field(default=0, init=False)

    def add(self, message: RawMessage) -> list[str]:
        """Buffer one message; returns object keys written if this triggered a flush."""
        buf = self._buffers.setdefault(
            (message.topic, message.partition), _Buffer(opened_at=self.clock())
        )
        buf.messages.append(message)
        if len(buf.messages) >= self.max_messages:
            return [self._flush_one((message.topic, message.partition))]
        return []

    def flush_due(self) -> list[str]:
        """Flush buffers older than ``max_age_s`` (call this every loop iteration)."""
        now = self.clock()
        due = [
            k
            for k, b in self._buffers.items()
            if b.messages and now - b.opened_at >= self.max_age_s
        ]
        return [self._flush_one(k) for k in due]

    def flush_all(self) -> list[str]:
        return [self._flush_one(k) for k in list(self._buffers) if self._buffers[k].messages]

    def pending(self) -> int:
        return sum(len(b.messages) for b in self._buffers.values())

    def _flush_one(self, key: tuple[str, int]) -> str:
        buf = self._buffers.pop(key)
        topic, partition = key
        first, last = buf.messages[0], buf.messages[-1]
        # Partition by the *broker* timestamp of the first message: what actually happened when.
        at = datetime.fromtimestamp(first.timestamp_ms / 1000, tz=UTC)
        name = object_key(self.prefix, topic, partition, first.offset, last.offset, at)
        raw = io.BytesIO()
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as gz:
            for message in buf.messages:
                gz.write(message.to_line())
        body = raw.getvalue()
        self.store.put(name, body, "application/gzip")
        self.objects_written += 1
        self.bytes_written += len(body)
        log.info("archived %s (%d msgs, %d bytes)", name, len(buf.messages), len(body))
        return name


class S3Store:
    """boto3-backed ObjectStore for MinIO (or any S3)."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket: str):
        import boto3
        from botocore.config import Config

        self.bucket = bucket
        self._s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def put(self, key: str, body: bytes, content_type: str) -> None:
        self._s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType=content_type)

    def list_keys(self, prefix: str):
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                yield item["Key"]

    def get(self, key: str) -> bytes:
        return self._s3.get_object(Bucket=self.bucket, Key=key)["Body"].read()
