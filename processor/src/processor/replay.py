"""Replay tooling (PRD F10 + the DLQ reprocessing path, PRD F5).

ov-replay archive --from 2026-09-06T20:00 --to 2026-09-06T22:00 [--topic arrival_predictions]
    Re-run the processing pipeline over raw messages from the MinIO archive for a time range.
    Same code path as the live consumer (processor.pipeline), no Kafka involved, and the
    upsert key makes overwriting existing windows safe. Deduplicates by (topic, partition,
    offset) so overlapping archive files cannot double count.

ov-replay dlq [--source-topic arrival_predictions] [--dry-run]
    Read every message on dlq.<topic>, try to decode it with the *current* parser/serde and
    publish the ones that now succeed back onto the source topic (header
    replayed_from=dlq). Ones that still fail stay where they are and are reported. The
    consumer group `dlq-replay` remembers what was handled, so running it twice is safe.
"""

from __future__ import annotations

import argparse
import gzip
import sys
from datetime import UTC, datetime, timedelta

from common.config import env, postgres_dsn
from common.events import TOPICS, dlq_topic
from common.logging import setup_logging
from common.serde import SchemaError, decode

log = setup_logging("replay")


# --------------------------------------------------------------------------- archive replay
def _hours(start: datetime, end: datetime):
    t = start.replace(minute=0, second=0, microsecond=0)
    while t <= end:
        yield t
        t += timedelta(hours=1)


def replay_archive(
    start: datetime, end: datetime, topics: list[str], window_s: int, lateness_s: int
) -> dict:
    from archiver.sink import RawMessage, S3Store
    from processor.pipeline import Pipeline
    from processor.sink import PostgresSink
    from processor.windows import TumblingWindower

    store = S3Store(
        endpoint=env("MINIO_ENDPOINT", "http://localhost:9010"),
        access_key=env("MINIO_ROOT_USER", "minioadmin"),
        secret_key=env("MINIO_ROOT_PASSWORD", "minioadmin"),
        bucket=env("MINIO_BUCKET", "raw-events"),
    )
    prefix = env("ARCHIVE_PREFIX", "raw-events")
    sink = PostgresSink.connect(postgres_dsn())
    # Generous lateness: archive files are read partition by partition, so event times interleave
    # far more than in the live stream. Per-source watermarks handle it; lateness is the safety net.
    pipeline = Pipeline(
        TumblingWindower(
            window_s=window_s, allowed_lateness_s=max(lateness_s, 600), idle_source_s=1e12
        ),
        sink,
    )

    stats = {"objects": 0, "messages": 0, "duplicates": 0, "decode_errors": 0, "windows": 0}
    seen: set[tuple[str, int, int]] = set()
    for topic in topics:
        for hour in _hours(start, end):
            hour_prefix = f"{prefix}/{topic}/date={hour:%Y-%m-%d}/hour={hour:%H}/"
            for key in store.list_keys(hour_prefix):
                stats["objects"] += 1
                for line in gzip.decompress(store.get(key)).splitlines():
                    raw = RawMessage.from_line(line)
                    at = datetime.fromtimestamp(raw.timestamp_ms / 1000, tz=UTC)
                    if not (start <= at <= end):
                        continue
                    ident = (raw.topic, raw.partition, raw.offset)
                    if ident in seen:
                        stats["duplicates"] += 1
                        continue
                    seen.add(ident)
                    stats["messages"] += 1
                    try:
                        event = decode(raw.payload)
                    except SchemaError:
                        stats["decode_errors"] += 1
                        continue
                    pipeline.ingest(event, source=(raw.topic, raw.partition))
                log.info("%s: %d messages so far", key.rsplit("/", 1)[-1], stats["messages"])
    pipeline.flush_state()
    pipeline.finalize()
    # Close everything that is still open: the range is finite, nothing more is coming.
    pipeline.windower.advance_to(end + timedelta(seconds=lateness_s + window_s + 1))
    stats["windows"] += len(pipeline.finalize())
    stats["windows"] += pipeline.windower.stats.finalized - stats["windows"]
    sink.close()
    return stats


# --------------------------------------------------------------------------- DLQ replay
def replay_dlq(source_topics: list[str], dry_run: bool, bootstrap: str) -> dict:
    from confluent_kafka import Consumer, Producer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "dlq-replay",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    stats = {"read": 0, "republished": 0, "still_failing": 0, "skipped": 0}
    failing: dict[str, int] = {}

    for source in source_topics:
        topic = dlq_topic(source)
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        end_offsets = {
            tp.partition: consumer.get_watermark_offsets(tp, timeout=10)[1] for tp in tps
        }
        done = {p for p, hi in end_offsets.items() if hi == 0}
        while len(done) < len(tps):
            msg = consumer.poll(2.0)
            if msg is None:
                break
            if msg.error():
                continue
            stats["read"] += 1
            headers = dict(msg.headers() or [])
            stage = headers.get("stage", b"decode").decode()
            payload = msg.value() or b""
            ok, reason = _retry(stage, source, payload)
            if ok:
                stats["republished"] += 1
                if not dry_run:
                    producer.produce(
                        source,
                        value=ok,
                        key=msg.key(),
                        headers=[
                            ("replayed_from", topic.encode()),
                            ("schema_version", headers.get("schema_version", b"1")),
                        ],
                    )
                    producer.poll(0)
            else:
                stats["still_failing"] += 1
                failing[reason] = failing.get(reason, 0) + 1
            if msg.offset() + 1 >= end_offsets[msg.partition()]:
                done.add(msg.partition())
            if not dry_run:
                consumer.store_offsets(msg)
        if not dry_run:
            producer.flush(30)
            try:
                consumer.commit(asynchronous=False)
            except Exception as exc:
                log.debug("nothing to commit: %s", exc)
    consumer.close()
    stats["still_failing_reasons"] = failing
    return stats


def _retry(stage: str, source: str, payload: bytes) -> tuple[bytes | None, str]:
    """Re-run the step that failed. Returns (bytes to republish, reason if still failing)."""
    if stage == "parse":
        # Producer-side failure: the payload is a GTFS-RT entity (or a whole feed). Wrap it in a
        # FeedMessage so the parser can retry it.
        from google.transit import gtfs_realtime_pb2 as gtfs

        from common.serde import encode
        from producer.parse import parse_trip_updates, parse_vehicle_positions

        feed = gtfs.FeedMessage()
        try:
            feed.ParseFromString(payload)
            if not feed.entity:
                raise ValueError("no entities")
        except Exception:
            entity = gtfs.FeedEntity()
            try:
                entity.ParseFromString(payload)
            except Exception as exc:
                return None, f"undecodable protobuf: {exc}"
            feed = gtfs.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            feed.header.timestamp = int(datetime.now(tz=UTC).timestamp())
            feed.entity.add().CopyFrom(entity)
        parse = parse_vehicle_positions if source == "vehicle_positions" else parse_trip_updates
        result = parse(
            feed.SerializeToString(),
            **({} if source == "vehicle_positions" else {"horizon_s": None}),
        )
        if result.events:
            return encode(result.events[0]), ""
        return None, result.errors[0].reason if result.errors else "no event produced"
    # Processor-side failure: bytes that did not decode under the schema at the time.
    try:
        decode(payload)
    except SchemaError as exc:
        return None, str(exc)
    return payload, ""


# --------------------------------------------------------------------------- CLI
def _parse_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts.replace(tzinfo=UTC) if ts.tzinfo is None else ts.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="ov-replay", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("archive", help="recompute windows from the MinIO archive")
    a.add_argument("--from", dest="start", required=True, type=_parse_ts, help="UTC ISO timestamp")
    a.add_argument("--to", dest="end", required=True, type=_parse_ts, help="UTC ISO timestamp")
    a.add_argument("--topic", action="append", choices=TOPICS, help="default: both topics")
    a.add_argument("--window-seconds", type=int, default=int(env("WINDOW_SECONDS", "300")))
    a.add_argument(
        "--lateness-seconds", type=int, default=int(env("ALLOWED_LATENESS_SECONDS", "120"))
    )
    d = sub.add_parser("dlq", help="re-decode dead letters and re-inject the fixable ones")
    d.add_argument("--source-topic", action="append", choices=TOPICS, help="default: both topics")
    d.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.cmd == "archive":
        stats = replay_archive(
            args.start,
            args.end,
            args.topic or list(TOPICS),
            args.window_seconds,
            args.lateness_seconds,
        )
    else:
        stats = replay_dlq(
            args.source_topic or list(TOPICS),
            args.dry_run,
            env("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
        )
    log.info("done: %s", stats)
    print(stats)
    return 0


if __name__ == "__main__":
    sys.exit(main())
