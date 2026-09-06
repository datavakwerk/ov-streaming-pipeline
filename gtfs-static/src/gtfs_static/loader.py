"""Static GTFS zip -> ref_* tables, via staging tables and an atomic swap.

The NL feed (gtfs.ovapi.nl/nl/gtfs-nl.zip) is ~220 MB zipped, 1.3 GB unzipped; stop_times.txt
alone is 1 GB. We only read the four files the pipeline needs (agency, routes, stops, trips —
PRD F7) and stream them straight from the zip into ``COPY``, never materializing a file in
memory. Load goes into ``<table>__staging``; then one transaction renames staging over live, so
the dashboard never reads a half-loaded reference set.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import psycopg

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class TableSpec:
    table: str
    file: str
    columns: tuple[str, ...]  # target column order
    sources: tuple[str, ...]  # GTFS column per target column
    ints: frozenset[str] = frozenset()
    floats: frozenset[str] = frozenset()
    required: tuple[str, ...] = ()  # rows missing these are skipped


SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        "ref_agencies",
        "agency.txt",
        ("agency_id", "name", "url"),
        ("agency_id", "agency_name", "agency_url"),
        required=("agency_id",),
    ),
    TableSpec(
        "ref_routes",
        "routes.txt",
        ("route_id", "agency_id", "short_name", "long_name", "route_type", "color", "text_color"),
        (
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_type",
            "route_color",
            "route_text_color",
        ),
        ints=frozenset({"route_type"}),
        required=("route_id",),
    ),
    TableSpec(
        "ref_stops",
        "stops.txt",
        (
            "stop_id",
            "code",
            "name",
            "lat",
            "lon",
            "location_type",
            "parent_station",
            "platform_code",
        ),
        (
            "stop_id",
            "stop_code",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "location_type",
            "parent_station",
            "platform_code",
        ),
        ints=frozenset({"location_type"}),
        floats=frozenset({"lat", "lon"}),
        required=("stop_id", "stop_name"),
    ),
    TableSpec(
        "ref_trips",
        "trips.txt",
        (
            "trip_id",
            "route_id",
            "service_id",
            "realtime_trip_id",
            "headsign",
            "short_name",
            "direction_id",
        ),
        (
            "trip_id",
            "route_id",
            "service_id",
            "realtime_trip_id",
            "trip_headsign",
            "trip_short_name",
            "direction_id",
        ),
        ints=frozenset({"direction_id"}),
        required=("trip_id",),
    ),
)


def _rows(zf: zipfile.ZipFile, spec: TableSpec) -> Iterator[tuple]:
    with zf.open(spec.file) as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        missing = [c for c in spec.sources if c not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(f"{spec.file}: missing columns {missing}")
        for record in reader:
            values = []
            skip = False
            for target, source in zip(spec.columns, spec.sources, strict=True):
                value = (record.get(source) or "").strip()
                if value == "":
                    if target in spec.required:
                        skip = True
                        break
                    values.append(None)
                    continue
                try:
                    if target in spec.ints:
                        values.append(int(float(value)))
                    elif target in spec.floats:
                        values.append(float(value))
                    else:
                        values.append(value)
                except ValueError:
                    values.append(None)
            if not skip:
                yield tuple(values)


def _copy(cur: psycopg.Cursor, table: str, columns: Iterable[str], rows: Iterator[tuple]) -> int:
    cols = ", ".join(columns)
    count = 0
    with cur.copy(f"COPY {table} ({cols}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(row)
            count += 1
    return count


def load_zip(
    conn: psycopg.Connection, zip_path: str, specs: tuple[TableSpec, ...] = SPECS
) -> dict[str, int]:
    """Load the given GTFS zip into the ref_* tables. Returns row counts per table."""
    counts: dict[str, int] = {}
    with zipfile.ZipFile(zip_path) as zf, conn.cursor() as cur:
        for spec in specs:
            staging = f"{spec.table}__staging"
            cur.execute(f"DROP TABLE IF EXISTS {staging}")
            cur.execute(f"CREATE TABLE {staging} (LIKE {spec.table} INCLUDING ALL)")
            # Trips/stops may repeat ids across agencies in messy feeds: dedupe on load by
            # copying into a temp heap first, then INSERT ... ON CONFLICT DO NOTHING.
            cur.execute(f"CREATE TEMP TABLE {staging}_raw (LIKE {spec.table}) ON COMMIT DROP")
            n = _copy(cur, f"{staging}_raw", spec.columns, _rows(zf, spec))
            cols = ", ".join(spec.columns)
            cur.execute(
                f"INSERT INTO {staging} ({cols}) SELECT {cols} FROM {staging}_raw "
                f"ON CONFLICT DO NOTHING"
            )
            counts[spec.table] = cur.rowcount
            log.info("%s: %d rows read, %d loaded into staging", spec.file, n, cur.rowcount)
        # Atomic swap: readers see either the old set or the new set, never a mix.
        for spec in specs:
            cur.execute(f"DROP TABLE IF EXISTS {spec.table}__old")
            cur.execute(f"ALTER TABLE {spec.table} RENAME TO {spec.table}__old")
            cur.execute(f"ALTER TABLE {spec.table}__staging RENAME TO {spec.table}")
            cur.execute(f"DROP TABLE {spec.table}__old")
        conn.commit()
    return counts
