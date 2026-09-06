-- Schema for the NL OV streaming pipeline. Applied once by the Postgres image on first start
-- (docker-entrypoint-initdb.d). Idempotent so it can also be re-applied by hand.
--
-- All timestamps are UTC (timestamptz); the dashboard converts to Europe/Amsterdam (PRD N5).

CREATE TABLE IF NOT EXISTS schema_migrations (
    version     text PRIMARY KEY,
    applied_at  timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Windowed aggregates: the processor's output. The primary key IS the idempotency key:
-- redelivering a finalized window upserts the identical row (PRD §5.2, commit 9).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS delay_aggregates (
    window_start   timestamptz NOT NULL,
    window_end     timestamptz NOT NULL,
    dimension      text        NOT NULL CHECK (dimension IN ('line', 'station', 'operator', 'network')),
    dimension_id   text        NOT NULL,
    operator       text,
    event_count    integer     NOT NULL,
    mean_delay_s   real        NOT NULL,
    p90_delay_s    real        NOT NULL,
    max_delay_s    real        NOT NULL,
    min_delay_s    real        NOT NULL,
    computed_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, dimension, dimension_id)
);
CREATE INDEX IF NOT EXISTS delay_aggregates_dim_window_idx
    ON delay_aggregates (dimension, window_start DESC);

-- ---------------------------------------------------------------------------
-- "Right now" state for the map: last known position per vehicle and last known delay per
-- trip. Both are last-write-wins upserts guarded by observed_at, so out-of-order redelivery
-- can never move a vehicle backwards.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vehicle_latest (
    vehicle_id     text PRIMARY KEY,
    trip_id        text,
    route_id       text,
    operator       text,
    stop_id        text,
    stop_sequence  integer,
    status         text,
    lat            double precision NOT NULL,
    lon            double precision NOT NULL,
    bearing        real,
    observed_at    timestamptz NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS vehicle_latest_observed_idx ON vehicle_latest (observed_at DESC);

CREATE TABLE IF NOT EXISTS trip_delay_latest (
    trip_id         text PRIMARY KEY,
    route_id        text,
    operator        text,
    stop_id         text,
    stop_sequence   integer,
    delay_s         integer NOT NULL,
    predicted_time  timestamptz,
    observed_at     timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Static GTFS reference data (PRD F7). Loaded by the gtfs-static sidecar into staging tables
-- and swapped in atomically, so readers never see a half-loaded set.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ref_agencies (
    agency_id   text PRIMARY KEY,
    name        text NOT NULL,
    url         text
);

CREATE TABLE IF NOT EXISTS ref_routes (
    route_id     text PRIMARY KEY,
    agency_id    text,
    short_name   text,
    long_name    text,
    route_type   integer,          -- GTFS: 0 tram, 1 metro, 2 rail, 3 bus, 4 ferry
    color        text,
    text_color   text
);

CREATE TABLE IF NOT EXISTS ref_stops (
    stop_id         text PRIMARY KEY,
    code            text,
    name            text NOT NULL,
    lat             double precision,
    lon             double precision,
    location_type   integer,
    parent_station  text,
    platform_code   text
);

CREATE TABLE IF NOT EXISTS ref_trips (
    trip_id            text PRIMARY KEY,
    route_id           text,
    service_id         text,
    realtime_trip_id   text,
    headsign           text,
    short_name         text,
    direction_id       integer
);

CREATE TABLE IF NOT EXISTS ref_meta (
    key         text PRIMARY KEY,
    value       text,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO schema_migrations (version) VALUES ('001_initial')
ON CONFLICT (version) DO NOTHING;
