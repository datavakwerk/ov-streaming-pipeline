"""Row parsing against a tiny fixture zip; the COPY/swap path is an integration test."""

import io
import zipfile
from pathlib import Path

import pytest

from gtfs_static.loader import SPECS, _rows

FIXTURE = Path(__file__).parent / "fixtures" / "gtfs-mini.zip"


def _spec(table):
    return next(s for s in SPECS if s.table == table)


def test_fixture_zip_has_the_four_files_we_load():
    with zipfile.ZipFile(FIXTURE) as zf:
        assert {s.file for s in SPECS} <= set(zf.namelist())


def test_routes_rows_are_typed():
    with zipfile.ZipFile(FIXTURE) as zf:
        rows = list(_rows(zf, _spec("ref_routes")))
    assert rows[0] == (
        "134842",
        "ALLGO",
        "326",
        "Lijn 326 (R-net) Almere Stad - Blaricum P+R",
        3,
        "e00713",
        "ffffff",
    )
    assert all(isinstance(r[4], int) for r in rows)


def test_stops_rows_have_float_coordinates_and_optional_columns_as_none():
    with zipfile.ZipFile(FIXTURE) as zf:
        rows = list(_rows(zf, _spec("ref_stops")))
    stop = dict(zip(_spec("ref_stops").columns, rows[0], strict=True))
    assert stop["name"] == "Koln-Ehrenfeld"
    assert stop["lat"] == pytest.approx(50.9515817) and stop["lon"] == pytest.approx(6.9174579)
    assert stop["code"] is None and stop["platform_code"] is None


def test_rows_missing_required_columns_are_skipped():
    spec = _spec("ref_trips")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(
            "trips.txt",
            "route_id,service_id,trip_id,realtime_trip_id,trip_headsign,trip_short_name,"
            "trip_long_name,direction_id,block_id,shape_id,wheelchair_accessible,bikes_allowed\n"
            "1,1,,IFF:1,Utrecht,1,IC,0,,,0,1\n"
            "1,1,42,IFF:2,Utrecht,2,IC,1,,,0,1\n",
        )
    with zipfile.ZipFile(io.BytesIO(buf.getvalue())) as zf:
        rows = list(_rows(zf, spec))
    assert rows == [("42", "1", "1", "IFF:2", "Utrecht", "2", 1)]


def test_missing_gtfs_column_is_an_error():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("agency.txt", "agency_id,agency_name\nNS,NS\n")
    with (
        zipfile.ZipFile(io.BytesIO(buf.getvalue())) as zf,
        pytest.raises(ValueError, match="agency_url"),
    ):
        list(_rows(zf, _spec("ref_agencies")))


@pytest.mark.integration
def test_load_zip_swaps_tables_atomically_against_real_postgres():
    """Loads into scratch copies of the ref_* tables: the live reference data is never touched."""
    import dataclasses

    import psycopg

    from common.config import postgres_dsn
    from gtfs_static.loader import load_zip

    scratch = tuple(dataclasses.replace(spec, table=f"itest_{spec.table}") for spec in SPECS)
    with psycopg.connect(postgres_dsn()) as conn:
        for spec, live in zip(scratch, SPECS, strict=True):
            conn.execute(f"DROP TABLE IF EXISTS {spec.table}")
            conn.execute(f"CREATE TABLE {spec.table} (LIKE {live.table} INCLUDING ALL)")
        conn.commit()
        try:
            counts = load_zip(conn, str(FIXTURE), specs=scratch)
            assert counts["itest_ref_routes"] == 5 and counts["itest_ref_stops"] == 5
            assert conn.execute("SELECT count(*) FROM itest_ref_routes").fetchone()[0] == 5
            counts_again = load_zip(conn, str(FIXTURE), specs=scratch)  # idempotent daily refresh
            assert counts_again == counts
        finally:
            for spec in scratch:
                conn.execute(f"DROP TABLE IF EXISTS {spec.table}")
            conn.commit()
