"""Pipeline + offset ledger behaviour with fakes (no Kafka, no Postgres)."""

from datetime import UTC, datetime, timedelta

import pytest

from common.events import ArrivalPrediction, VehiclePosition
from common.serde import SchemaError, encode
from processor.consumer import _PartitionLedger
from processor.pipeline import Pipeline
from processor.windows import TumblingWindower

T0 = datetime(2026, 9, 6, 12, 0, tzinfo=UTC)


class FakeSink:
    def __init__(self):
        self.aggregates, self.vehicles, self.trip_delays = [], [], []

    def upsert_aggregates(self, aggs):
        self.aggregates.extend(aggs)
        return len(aggs)

    def upsert_vehicles(self, events):
        self.vehicles.extend(events)
        return len(events)

    def upsert_trip_delays(self, events):
        self.trip_delays.extend(events)
        return len(events)


def pred(trip, route, stop, delay, seconds):
    return ArrivalPrediction(trip, route, "OP", stop, delay, T0 + timedelta(seconds=seconds))


def test_one_prediction_feeds_line_and_station_windows():
    sink = FakeSink()
    p = Pipeline(TumblingWindower(300, 120), sink)
    p.ingest(pred("t1", "L1", "S1", 60, 10))
    p.ingest(pred("t2", "L1", "S2", 120, 20))
    p.ingest(pred("t3", "L2", "S1", 0, 430))  # closes the 12:00 window
    written = p.finalize()
    by_key = {(a.key.dimension, a.key.dimension_id): a for a in written}
    assert by_key[("line", "L1")].count == 2 and by_key[("line", "L1")].mean == 90
    assert by_key[("station", "S1")].count == 1 and by_key[("station", "S2")].count == 1
    assert by_key[("operator", "OP")].count == 2 and by_key[("network", "NL")].count == 2
    assert sink.aggregates == written


def test_positions_and_predictions_flush_to_state_tables():
    sink = FakeSink()
    p = Pipeline(TumblingWindower(), sink)
    p.ingest(VehiclePosition("v1", "t1", "L1", "OP", 52.0, 4.9, T0))
    p.ingest(pred("t1", "L1", "S1", 30, 0))
    p.flush_state()
    assert len(sink.vehicles) == 1 and len(sink.trip_delays) == 1
    p.flush_state()
    assert len(sink.vehicles) == 1  # buffers cleared after a flush


def test_redelivered_prediction_is_deduped_within_the_open_window():
    sink = FakeSink()
    p = Pipeline(TumblingWindower(300, 120), sink)
    same = pred("t1", "L1", "S1", 60, 10)
    p.ingest(same)
    p.ingest(same)
    p.ingest(pred("t9", "L9", "S9", 0, 500))
    [line] = [a for a in p.finalize() if a.key.dimension_id == "L1"]
    assert line.count == 1


def test_decode_errors_bubble_as_schema_error():
    p = Pipeline(TumblingWindower(), FakeSink())
    with pytest.raises(SchemaError):
        p.decode(b"nope")
    assert p.decode(encode(pred("t", "r", "s", 1, 0))).trip_id == "t"


def test_idle_finalize_closes_windows_without_new_events():
    sink = FakeSink()
    p = Pipeline(TumblingWindower(300, 120), sink)
    p.ingest(pred("t1", "L1", "S1", 60, 10))
    assert p.finalize() == []
    assert len(p.finalize(now=T0 + timedelta(minutes=7), idle_s=90)) == 4


def test_ledger_commits_only_past_finalized_windows_in_order():
    ledger = _PartitionLedger()
    w_end = T0 + timedelta(minutes=5)
    ledger.add(100, w_end)  # prediction in [12:00, 12:05)
    ledger.add(101, None)  # a position
    ledger.add(102, w_end + timedelta(minutes=5))  # prediction in [12:05, 12:10)
    ledger.add(103, None)

    ledger.release(watermark=T0 + timedelta(minutes=4), positions_flushed=True)
    assert ledger.committable is None  # the head is still in an open window: nothing commits

    ledger.release(watermark=w_end, positions_flushed=True)
    assert ledger.committable == 102  # 100 and 101 are durable; 102 still open blocks 103
    assert [o for o, _ in ledger.pending] == [102, 103]

    ledger.release(watermark=w_end + timedelta(minutes=5), positions_flushed=True)
    assert ledger.committable == 104 and not ledger.pending


def test_ledger_holds_positions_until_state_flush():
    ledger = _PartitionLedger()
    ledger.add(7, None)
    ledger.release(watermark=None, positions_flushed=False)
    assert ledger.committable is None
    ledger.release(watermark=None, positions_flushed=True)
    assert ledger.committable == 8
