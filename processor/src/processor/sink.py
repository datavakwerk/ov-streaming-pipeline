"""Idempotent Postgres sink.

Why at-least-once is safe here (PRD §5.2, quoted verbatim in the README)
----------------------------------------------------------------------
The consumer commits Kafka offsets only *after* a finalized window has been upserted. If the
process dies between the upsert and the commit, the broker redelivers the messages; the
processor recomputes the window and upserts it again. The upsert key is
``(window_start, dimension, dimension_id)`` — the primary key of ``delay_aggregates`` — so the
second write replaces the row with identical values instead of adding to it. Redelivery can
therefore never double-count a finalized window. The "latest state" tables are guarded by
``observed_at`` so an older redelivered position can never overwrite a newer one.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import datetime

import psycopg
from psycopg import sql
from psycopg.rows import tuple_row

from common.events import ArrivalPrediction, VehiclePosition
from processor import metrics
from processor.aggregates import Aggregate

log = logging.getLogger(__name__)

UPSERT_AGGREGATES = """
INSERT INTO delay_aggregates
    (window_start, window_end, dimension, dimension_id, operator,
     event_count, mean_delay_s, p90_delay_s, max_delay_s, min_delay_s, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, dimension, dimension_id) DO UPDATE SET
    window_end   = EXCLUDED.window_end,
    operator     = COALESCE(EXCLUDED.operator, delay_aggregates.operator),
    event_count  = EXCLUDED.event_count,
    mean_delay_s = EXCLUDED.mean_delay_s,
    p90_delay_s  = EXCLUDED.p90_delay_s,
    max_delay_s  = EXCLUDED.max_delay_s,
    min_delay_s  = EXCLUDED.min_delay_s,
    computed_at  = now()
"""

UPSERT_VEHICLE = """
INSERT INTO vehicle_latest
    (vehicle_id, trip_id, route_id, operator, stop_id, stop_sequence, status,
     lat, lon, bearing, observed_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (vehicle_id) DO UPDATE SET
    trip_id = EXCLUDED.trip_id, route_id = EXCLUDED.route_id, operator = EXCLUDED.operator,
    stop_id = EXCLUDED.stop_id, stop_sequence = EXCLUDED.stop_sequence,
    status = EXCLUDED.status, lat = EXCLUDED.lat, lon = EXCLUDED.lon,
    bearing = EXCLUDED.bearing, observed_at = EXCLUDED.observed_at, updated_at = now()
WHERE EXCLUDED.observed_at >= vehicle_latest.observed_at
"""

UPSERT_TRIP_DELAY = """
INSERT INTO trip_delay_latest
    (trip_id, route_id, operator, stop_id, stop_sequence, delay_s, predicted_time,
     observed_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (trip_id) DO UPDATE SET
    route_id = EXCLUDED.route_id, operator = EXCLUDED.operator, stop_id = EXCLUDED.stop_id,
    stop_sequence = EXCLUDED.stop_sequence, delay_s = EXCLUDED.delay_s,
    predicted_time = EXCLUDED.predicted_time, observed_at = EXCLUDED.observed_at,
    updated_at = now()
WHERE EXCLUDED.observed_at >= trip_delay_latest.observed_at
"""


def aggregate_row(agg: Aggregate) -> tuple:
    k = agg.key
    return (
        k.window_start,
        k.window_end,
        k.dimension,
        k.dimension_id,
        agg.operator,
        agg.count,
        agg.mean,
        agg.p90,
        agg.max,
        agg.min,
    )


def vehicle_row(e: VehiclePosition) -> tuple:
    return (
        e.vehicle_id,
        e.trip_id,
        e.route_id,
        e.operator,
        e.stop_id,
        e.stop_sequence,
        e.status,
        e.lat,
        e.lon,
        e.bearing,
        e.timestamp,
    )


def trip_delay_row(e: ArrivalPrediction) -> tuple:
    return (
        e.trip_id,
        e.route_id,
        e.operator,
        e.stop_id,
        e.stop_sequence,
        e.delay_seconds,
        e.predicted_time,
        e.timestamp,
    )


def latest_by_key(rows: Iterable[tuple], key_index: int, time_index: int) -> list[tuple]:
    """Collapse a batch to the newest row per key so one executemany never races itself."""
    latest: dict = {}
    for row in rows:
        current = latest.get(row[key_index])
        if current is None or row[time_index] >= current[time_index]:
            latest[row[key_index]] = row
    return list(latest.values())


class PostgresSink:
    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    @classmethod
    def connect(cls, dsn: str) -> PostgresSink:
        conn = psycopg.connect(dsn, autocommit=False, row_factory=tuple_row)
        conn.execute("SET application_name = 'ov-processor'")
        conn.commit()
        return cls(conn)

    def upsert_aggregates(self, aggregates: Sequence[Aggregate]) -> int:
        if not aggregates:
            return 0
        with metrics.UPSERT_SECONDS.time(), self.conn.cursor() as cur:
            cur.executemany(UPSERT_AGGREGATES, [aggregate_row(a) for a in aggregates])
        self.conn.commit()
        metrics.UPSERTS.labels("delay_aggregates").inc(len(aggregates))
        return len(aggregates)

    def upsert_vehicles(self, events: Sequence[VehiclePosition]) -> int:
        rows = latest_by_key((vehicle_row(e) for e in events), key_index=0, time_index=10)
        if rows:
            with self.conn.cursor() as cur:
                cur.executemany(UPSERT_VEHICLE, rows)
            self.conn.commit()
            metrics.UPSERTS.labels("vehicle_latest").inc(len(rows))
        return len(rows)

    def upsert_trip_delays(self, events: Sequence[ArrivalPrediction]) -> int:
        rows = latest_by_key((trip_delay_row(e) for e in events), key_index=0, time_index=7)
        if rows:
            with self.conn.cursor() as cur:
                cur.executemany(UPSERT_TRIP_DELAY, rows)
            self.conn.commit()
            metrics.UPSERTS.labels("trip_delay_latest").inc(len(rows))
        return len(rows)

    def newest_window_end(self) -> datetime | None:
        row = self.conn.execute("SELECT max(window_end) FROM delay_aggregates").fetchone()
        self.conn.commit()
        return row[0] if row else None

    def purge_stale_state(self, older_than_s: int = 3600) -> None:
        """Vehicles that stopped reporting drop out of the 'right now' tables after an hour."""
        for table in ("vehicle_latest", "trip_delay_latest"):
            self.conn.execute(
                sql.SQL(
                    "DELETE FROM {} WHERE observed_at < now() - %s * interval '1 second'"
                ).format(sql.Identifier(table)),
                (older_than_s,),
            )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()
