"""All SQL for the dashboard, one function per panel. Timestamps stay UTC here."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import psycopg

FRESH_POSITIONS = "3 minutes"  # a vehicle not seen for longer leaves the map


def _df(conn: psycopg.Connection, sql: str, params: tuple = ()) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [d.name for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def latest_window(conn: psycopg.Connection) -> tuple[datetime | None, datetime | None]:
    row = conn.execute(
        "SELECT window_start, window_end FROM delay_aggregates "
        "WHERE dimension = 'network' ORDER BY window_start DESC LIMIT 1"
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


def freshness(conn: psycopg.Connection) -> dict:
    row = conn.execute(
        "SELECT extract(epoch FROM now() - max(observed_at)), count(*) FILTER "
        f"(WHERE observed_at > now() - interval '{FRESH_POSITIONS}') FROM vehicle_latest"
    ).fetchone()
    return {"positions_age_s": row[0], "vehicles_live": row[1]}


def right_now(
    conn: psycopg.Connection, operators: list[str] | None, route_types: list[int] | None
) -> dict:
    """Current network state from the per-trip latest delays (exact, not re-aggregated)."""
    sql = f"""
        SELECT count(*)                                              AS trips,
               avg(t.delay_s)                                        AS mean_s,
               percentile_cont(0.9) WITHIN GROUP (ORDER BY t.delay_s) AS p90_s,
               avg((t.delay_s < 60)::int)                            AS on_time_share,
               avg((t.delay_s >= 300)::int)                          AS severe_share
        FROM trip_delay_latest t
        LEFT JOIN ref_routes r ON r.route_id = t.route_id
        WHERE t.observed_at > now() - interval '{FRESH_POSITIONS}'
          AND (%(ops)s::text[] IS NULL OR t.operator = ANY(%(ops)s))
          AND (%(types)s::int[] IS NULL OR r.route_type = ANY(%(types)s))
    """
    row = conn.execute(sql, {"ops": operators or None, "types": route_types or None}).fetchone()
    keys = ("trips", "mean_s", "p90_s", "on_time_share", "severe_share")
    return dict(zip(keys, row, strict=True))


def vehicles(
    conn: psycopg.Connection,
    operators: list[str] | None,
    route_types: list[int] | None,
    limit: int = 8000,
) -> pd.DataFrame:
    sql = f"""
        SELECT v.vehicle_id, v.operator, v.lat, v.lon, v.bearing, v.status, v.observed_at,
               t.delay_s,
               r.short_name AS line, r.long_name, r.route_type, r.color AS route_color,
               a.name AS agency, tr.headsign,
               s.name AS next_stop
        FROM vehicle_latest v
        LEFT JOIN trip_delay_latest t ON t.trip_id = v.trip_id
        LEFT JOIN ref_routes r       ON r.route_id = v.route_id
        LEFT JOIN ref_agencies a     ON a.agency_id = r.agency_id
        LEFT JOIN ref_trips tr       ON tr.trip_id = v.trip_id
        LEFT JOIN ref_stops s        ON s.stop_id = v.stop_id
        WHERE v.observed_at > now() - interval '{FRESH_POSITIONS}'
          AND (%(ops)s::text[] IS NULL OR v.operator = ANY(%(ops)s))
          AND (%(types)s::int[] IS NULL OR r.route_type = ANY(%(types)s))
        ORDER BY v.observed_at DESC
        LIMIT %(limit)s
    """
    return _df(conn, sql, {"ops": operators or None, "types": route_types or None, "limit": limit})


def worst_lines(
    conn: psycopg.Connection,
    window_start: datetime,
    operators: list[str] | None,
    route_types: list[int] | None,
    min_count: int = 5,
    limit: int = 12,
) -> pd.DataFrame:
    sql = """
        SELECT d.dimension_id AS route_id, r.short_name AS line, r.long_name, r.route_type,
               r.color AS route_color, COALESCE(a.name, d.operator) AS agency, d.operator,
               d.event_count AS n, d.mean_delay_s AS mean_s, d.p90_delay_s AS p90_s,
               d.max_delay_s AS max_s
        FROM delay_aggregates d
        LEFT JOIN ref_routes r   ON r.route_id = d.dimension_id
        LEFT JOIN ref_agencies a ON a.agency_id = r.agency_id
        WHERE d.dimension = 'line' AND d.window_start = %(ws)s AND d.event_count >= %(min)s
          AND (%(ops)s::text[] IS NULL OR d.operator = ANY(%(ops)s))
          AND (%(types)s::int[] IS NULL OR r.route_type = ANY(%(types)s))
        ORDER BY d.mean_delay_s DESC
        LIMIT %(limit)s
    """
    return _df(
        conn,
        sql,
        {
            "ws": window_start,
            "min": min_count,
            "ops": operators or None,
            "types": route_types or None,
            "limit": limit,
        },
    )


def worst_stations(
    conn: psycopg.Connection,
    window_start: datetime,
    operators: list[str] | None,
    min_count: int = 5,
    limit: int = 12,
) -> pd.DataFrame:
    sql = """
        SELECT d.dimension_id AS stop_id, COALESCE(s.name, d.dimension_id) AS station,
               s.lat, s.lon, d.operator,
               d.event_count AS n, d.mean_delay_s AS mean_s, d.p90_delay_s AS p90_s,
               d.max_delay_s AS max_s
        FROM delay_aggregates d
        LEFT JOIN ref_stops s ON s.stop_id = d.dimension_id
        WHERE d.dimension = 'station' AND d.window_start = %(ws)s AND d.event_count >= %(min)s
          AND (%(ops)s::text[] IS NULL OR d.operator = ANY(%(ops)s))
        ORDER BY d.mean_delay_s DESC
        LIMIT %(limit)s
    """
    return _df(
        conn, sql, {"ws": window_start, "min": min_count, "ops": operators or None, "limit": limit}
    )


def operators_last_window(conn: psycopg.Connection, window_start: datetime) -> pd.DataFrame:
    sql = """
        SELECT d.dimension_id AS operator, COALESCE(a.name, d.dimension_id) AS agency,
               d.event_count AS n, d.mean_delay_s AS mean_s, d.p90_delay_s AS p90_s
        FROM delay_aggregates d
        LEFT JOIN ref_agencies a ON a.agency_id = d.dimension_id
        WHERE d.dimension = 'operator' AND d.window_start = %s AND d.event_count >= 20
        ORDER BY d.mean_delay_s DESC
    """
    return _df(conn, sql, (window_start,))


def network_trend(conn: psycopg.Connection, hours: int = 3) -> pd.DataFrame:
    sql = """
        SELECT window_start, window_end, event_count AS n, mean_delay_s AS mean_s,
               p90_delay_s AS p90_s, max_delay_s AS max_s
        FROM delay_aggregates
        WHERE dimension = 'network' AND window_start > now() - %s * interval '1 hour'
        ORDER BY window_start
    """
    return _df(conn, sql, (hours,))


def operator_choices(conn: psycopg.Connection) -> pd.DataFrame:
    sql = f"""
        SELECT v.operator, COALESCE(min(a.name), v.operator) AS agency, count(*) AS vehicles
        FROM vehicle_latest v
        LEFT JOIN ref_agencies a ON a.agency_id = v.operator
        WHERE v.observed_at > now() - interval '{FRESH_POSITIONS}'
        GROUP BY v.operator ORDER BY vehicles DESC
    """
    return _df(conn, sql)


def delay_histogram(conn: psycopg.Connection, operators: list[str] | None) -> pd.DataFrame:
    sql = f"""
        SELECT width_bucket(delay_s, -120, 900, 17) AS b, count(*) AS n
        FROM trip_delay_latest
        WHERE observed_at > now() - interval '{FRESH_POSITIONS}'
          AND (%(ops)s::text[] IS NULL OR operator = ANY(%(ops)s))
        GROUP BY b ORDER BY b
    """
    return _df(conn, sql, {"ops": operators or None})
