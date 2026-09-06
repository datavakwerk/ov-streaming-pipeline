"""Sink idempotency: the same finalized window applied twice yields one identical row.

Two flavours (COMMITS.md commit 9): a fake connection that records SQL (runs in CI) and a real
Postgres (marked integration, run with `make test-integration` against the compose stack).
"""

from datetime import UTC, datetime, timedelta

import pytest

from common.events import ArrivalPrediction, VehiclePosition
from processor.aggregates import Aggregate, WindowKey
from processor.sink import (
    UPSERT_AGGREGATES,
    PostgresSink,
    aggregate_row,
    latest_by_key,
    vehicle_row,
)

T0 = datetime(2026, 9, 6, 8, 15, tzinfo=UTC)
AGG = Aggregate(
    key=WindowKey(T0, T0 + timedelta(minutes=5), "line", "NS:IC800"),
    operator="NS",
    count=10,
    mean=144.0,
    p90=300,
    max=600,
    min=0,
)


class FakeCursor:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def executemany(self, query, rows):
        self.log.append((query, list(rows)))


class FakeConn:
    def __init__(self):
        self.log = []
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.log)

    def commit(self):
        self.commits += 1


def test_upsert_statement_conflicts_on_the_window_key_and_replaces_not_adds():
    assert "ON CONFLICT (window_start, dimension, dimension_id) DO UPDATE" in UPSERT_AGGREGATES
    assert "event_count  = EXCLUDED.event_count" in UPSERT_AGGREGATES
    assert "+" not in UPSERT_AGGREGATES.split("DO UPDATE")[1]  # never accumulate on redelivery


def test_applying_the_same_window_twice_sends_identical_rows():
    conn = FakeConn()
    sink = PostgresSink(conn)
    sink.upsert_aggregates([AGG])
    sink.upsert_aggregates([AGG])  # redelivery after a crash between upsert and commit
    (q1, rows1), (q2, rows2) = conn.log
    assert q1 == q2 == UPSERT_AGGREGATES
    assert rows1 == rows2 == [aggregate_row(AGG)]
    assert conn.commits == 2


def test_aggregate_row_layout():
    assert aggregate_row(AGG) == (
        T0,
        T0 + timedelta(minutes=5),
        "line",
        "NS:IC800",
        "NS",
        10,
        144.0,
        300,
        600,
        0,
    )


def test_latest_by_key_collapses_a_batch_to_the_newest_row_per_key():
    older = VehiclePosition("v1", "t", "r", "OP", 52.0, 4.9, T0)
    newer = VehiclePosition("v1", "t", "r", "OP", 52.1, 4.9, T0 + timedelta(seconds=30))
    other = VehiclePosition("v2", "t", "r", "OP", 52.2, 4.9, T0)
    rows = latest_by_key(
        (vehicle_row(e) for e in (newer, older, other)), key_index=0, time_index=10
    )
    assert sorted(r[0] for r in rows) == ["v1", "v2"]
    assert next(r for r in rows if r[0] == "v1")[7] == 52.1  # the newer lat won


def test_upsert_vehicles_and_trip_delays_go_through_and_commit():
    conn = FakeConn()
    sink = PostgresSink(conn)
    assert sink.upsert_vehicles([VehiclePosition("v1", "t", "r", "OP", 52.0, 4.9, T0)]) == 1
    assert sink.upsert_trip_delays([ArrivalPrediction("t1", "r", "OP", "s", 42, T0)]) == 1
    assert sink.upsert_vehicles([]) == 0
    assert conn.commits == 2


@pytest.mark.integration
def test_duplicate_delivery_does_not_double_count_in_real_postgres():
    from common.config import postgres_dsn

    sink = PostgresSink.connect(postgres_dsn())
    key = AGG.key
    try:
        sink.conn.execute(
            "DELETE FROM delay_aggregates "
            "WHERE window_start = %s AND dimension = %s AND dimension_id = %s",
            (key.window_start, key.dimension, key.dimension_id),
        )
        sink.conn.commit()
        sink.upsert_aggregates([AGG])
        sink.upsert_aggregates([AGG])
        rows = sink.conn.execute(
            "SELECT event_count, mean_delay_s, p90_delay_s FROM delay_aggregates "
            "WHERE window_start = %s AND dimension = %s AND dimension_id = %s",
            (key.window_start, key.dimension, key.dimension_id),
        ).fetchall()
        assert rows == [(10, 144.0, 300.0)]  # one row, not two; count 10, not 20
    finally:
        sink.close()
