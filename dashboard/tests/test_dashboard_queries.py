"""Query functions against a real Postgres (integration) plus SQL sanity checks (unit)."""

import pytest

from dashboard import queries


def test_every_state_query_filters_on_freshness():
    import inspect

    for fn in (queries.freshness, queries.right_now, queries.vehicles, queries.operator_choices):
        assert "observed_at > now() - interval" in inspect.getsource(fn), fn.__name__


def test_worst_tables_use_the_last_window_and_a_min_count():
    import inspect

    for fn in (queries.worst_lines, queries.worst_stations):
        src = inspect.getsource(fn)
        assert "window_start = %(ws)s" in src and "event_count >= %(min)s" in src


@pytest.mark.integration
def test_queries_run_against_the_stack():
    import psycopg

    from common.config import postgres_dsn

    with psycopg.connect(postgres_dsn()) as conn:
        ws, _ = queries.latest_window(conn)
        assert queries.freshness(conn)["vehicles_live"] >= 0
        assert set(queries.right_now(conn, None, None)) == {
            "trips",
            "mean_s",
            "p90_s",
            "on_time_share",
            "severe_share",
        }
        assert list(queries.vehicles(conn, None, None, limit=5).columns)[:4] == [
            "vehicle_id",
            "operator",
            "lat",
            "lon",
        ]
        if ws is not None:
            assert not queries.worst_lines(conn, ws, None, None, min_count=1).empty
            assert "station" in queries.worst_stations(conn, ws, None, min_count=1).columns
        assert "n" in queries.network_trend(conn).columns
