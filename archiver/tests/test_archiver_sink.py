import gzip
import json
from datetime import UTC, datetime

from archiver.sink import ArchiveSink, RawMessage, object_key


class MemoryStore:
    def __init__(self):
        self.objects = {}

    def put(self, key, body, content_type):
        self.objects[key] = body


class Clock:
    t = 100.0

    def __call__(self):
        return self.t


def msg(offset, partition=0, topic="vehicle_positions", ts_ms=1788728782000):
    return RawMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp_ms=ts_ms,
        key=b"GVB:13:1",
        headers=[("schema_version", b"1")],
        payload=b'{"type":"vehicle_position"}',
    )


def test_object_key_layout():
    at = datetime(2026, 9, 6, 7, 5, tzinfo=UTC)
    assert (
        object_key("raw-events", "vehicle_positions", 3, 10, 1009, at)
        == "raw-events/vehicle_positions/date=2026-09-06/hour=07/"
        "p3-000000000010-000000001009.jsonl.gz"
    )


def test_line_roundtrip_preserves_bytes_and_headers():
    original = msg(7)
    restored = RawMessage.from_line(original.to_line())
    assert restored == original


def test_flush_on_size_threshold_per_partition():
    store, clock = MemoryStore(), Clock()
    sink = ArchiveSink(store, max_messages=3, max_age_s=60, clock=clock)
    assert sink.add(msg(0)) == [] and sink.add(msg(1, partition=1)) == []
    assert sink.add(msg(1)) == []
    written = sink.add(msg(2))  # third message on partition 0 -> flush partition 0 only
    assert written == [
        "raw-events/vehicle_positions/date=2026-09-06/hour=21/p0-000000000000-000000000002.jsonl.gz"
    ]
    assert sink.pending() == 1  # partition 1 still buffered

    lines = gzip.decompress(store.objects[written[0]]).decode().splitlines()
    assert [json.loads(line)["offset"] for line in lines] == [0, 1, 2]


def test_flush_on_age_so_the_overnight_lull_still_produces_files():
    store, clock = MemoryStore(), Clock()
    sink = ArchiveSink(store, max_messages=1000, max_age_s=60, clock=clock)
    sink.add(msg(0))
    clock.t += 59
    assert sink.flush_due() == []
    clock.t += 2
    assert len(sink.flush_due()) == 1 and sink.pending() == 0


def test_rearchiving_same_range_overwrites_same_object():
    store, clock = MemoryStore(), Clock()
    sink = ArchiveSink(store, max_messages=2, clock=clock)
    sink.add(msg(0)), sink.add(msg(1))
    sink.add(msg(0)), sink.add(msg(1))  # redelivery after a crash between put and commit
    assert len(store.objects) == 1 and sink.objects_written == 2


def test_flush_all_on_shutdown():
    store = MemoryStore()
    sink = ArchiveSink(store, clock=Clock())
    sink.add(msg(0)), sink.add(msg(5, topic="arrival_predictions"))
    keys = sink.flush_all()
    assert len(keys) == 2 and sink.pending() == 0
    assert any("/arrival_predictions/" in k for k in keys)
