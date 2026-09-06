"""Parser tests against recorded OVapi responses (producer/tests/fixtures/gtfsrt/record.sh)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from common.events import ArrivalPrediction, VehiclePosition
from producer.parse import FeedError, parse_trip_updates, parse_vehicle_positions

FIXTURES = Path(__file__).parent / "fixtures" / "gtfsrt"
FEED_TIME = datetime.fromtimestamp(1788728782, tz=UTC)


@pytest.fixture(scope="module")
def positions_raw() -> bytes:
    return (FIXTURES / "vehicle_positions.pb").read_bytes()


@pytest.fixture(scope="module")
def trip_updates_raw() -> bytes:
    return (FIXTURES / "trip_updates.pb").read_bytes()


# --------------------------------------------------------------------------- positions


def test_positions_fixture_parses_cleanly(positions_raw):
    result = parse_vehicle_positions(positions_raw)
    assert result.feed_timestamp == FEED_TIME
    assert len(result.events) + len(result.errors) == 200
    assert len(result.events) >= 190  # a handful of real entities lack a position
    assert all(isinstance(e, VehiclePosition) for e in result.events)
    assert all(err.reason == "vehicle without position" for err in result.errors)


def test_first_position_matches_recorded_values(positions_raw):
    first = parse_vehicle_positions(positions_raw).events[0]
    assert first.vehicle_id == "ARR:6166:7058"
    assert first.operator == "ARR"
    assert first.trip_id == "381646875"
    assert first.route_id == "159889"
    assert first.stop_id == "4133173"
    assert first.stop_sequence == 1
    assert first.status == "IN_TRANSIT_TO"
    assert (first.lat, first.lon) == pytest.approx((51.6958771, 5.30144501))
    assert first.timestamp == datetime.fromtimestamp(1788728771, tz=UTC)
    assert first.timestamp.tzinfo is UTC


def test_positions_are_within_the_netherlands_ish(positions_raw):
    for event in parse_vehicle_positions(positions_raw).events:
        assert 50.5 <= event.lat <= 53.7, event
        assert 3.0 <= event.lon <= 7.3, event


def test_operator_filter(positions_raw):
    result = parse_vehicle_positions(positions_raw, operator_filter=frozenset({"GVB", "HTM"}))
    assert {e.operator for e in result.events} == {"GVB", "HTM"}
    assert len(result.events) == 46
    assert result.skipped > 100


def test_malformed_entities_are_isolated_not_fatal():
    result = parse_vehicle_positions((FIXTURES / "vehicle_positions_mixed.pb").read_bytes())
    assert len(result.events) == 1
    reasons = {err.entity_id: err.reason for err in result.errors}
    assert reasons == {
        "2026-09-06:BAD:1:1": "vehicle without position",
        "2026-09-06:BAD:2:2": "vehicle without trip_id",
        "2026-09-06:BAD:3:3": "implausible coordinates (95.0, 5.301445007324219)",
    }
    # The DLQ payload is the original entity bytes, re-parseable after a fix.
    assert all(err.raw for err in result.errors)


def test_garbage_payload_is_a_feed_error():
    with pytest.raises(FeedError, match="not a GTFS-RT FeedMessage"):
        parse_vehicle_positions((FIXTURES / "garbage.pb").read_bytes())


# --------------------------------------------------------------------------- predictions


def test_trip_updates_fixture_parses(trip_updates_raw):
    result = parse_trip_updates(trip_updates_raw, horizon_s=None)
    assert result.errors == []
    assert all(isinstance(e, ArrivalPrediction) for e in result.events)
    # 41 of the 300 recorded updates are CANCELED (all stops SKIPPED): no prediction.
    assert result.skipped == 41
    assert len(result.events) == 259


def test_prediction_takes_first_non_skipped_stop_with_delay(trip_updates_raw):
    events = {e.trip_id: e for e in parse_trip_updates(trip_updates_raw, horizon_s=None).events}
    gvb13 = events["380807322"]
    assert gvb13.operator == "GVB"
    assert gvb13.route_id == "152330"
    assert gvb13.stop_id == "3981133"
    assert gvb13.stop_sequence == 1
    assert gvb13.delay_seconds == 18  # departure delay wins over arrival delay (-36)
    assert gvb13.predicted_time == datetime.fromtimestamp(1788727578, tz=UTC)
    assert gvb13.timestamp == FEED_TIME  # event time = when the prediction was issued


def test_event_time_is_feed_time_for_all_predictions(trip_updates_raw):
    assert {e.timestamp for e in parse_trip_updates(trip_updates_raw).events} == {FEED_TIME}


def test_horizon_drops_trips_that_start_much_later(trip_updates_raw):
    everything = parse_trip_updates(trip_updates_raw, horizon_s=None)
    active = parse_trip_updates(trip_updates_raw, horizon_s=900)
    assert len(active.events) < len(everything.events)
    for event in active.events:
        assert (event.predicted_time - FEED_TIME).total_seconds() <= 900


def test_prediction_operator_filter(trip_updates_raw):
    result = parse_trip_updates(trip_updates_raw, operator_filter=frozenset({"EBS"}))
    assert result.events and {e.operator for e in result.events} == {"EBS"}
