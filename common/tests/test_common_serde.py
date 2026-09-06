from datetime import UTC, datetime

import pytest

from common.events import SCHEMA_VERSION, ArrivalPrediction, VehiclePosition
from common.serde import (
    HEADER_EVENT_TYPE,
    HEADER_SCHEMA_VERSION,
    SUPPORTED_VERSIONS,
    SchemaError,
    decode,
    encode,
    headers_for,
)

T = datetime(2026, 9, 6, 12, 0, 5, tzinfo=UTC)

POSITION = VehiclePosition(
    vehicle_id="GVB:13:13605",
    trip_id="380807322",
    route_id="152330",
    operator="GVB",
    lat=52.3714218,
    lon=4.83810043,
    timestamp=T,
    stop_id="3980974",
    stop_sequence=15,
    status="IN_TRANSIT_TO",
)
PREDICTION = ArrivalPrediction(
    trip_id="380807322",
    route_id="152330",
    operator="GVB",
    stop_id="3981133",
    delay_seconds=18,
    timestamp=T,
    stop_sequence=16,
    predicted_time=T,
)


@pytest.mark.parametrize("event", [POSITION, PREDICTION], ids=["position", "prediction"])
def test_roundtrip(event):
    raw = encode(event)
    assert decode(raw) == event
    assert raw.startswith(b'{"type":"')  # type + schema_version lead the body


def test_body_carries_schema_version_and_type():
    import json

    body = json.loads(encode(POSITION))
    assert body["schema_version"] == SCHEMA_VERSION
    assert body["type"] == "vehicle_position"
    assert body["timestamp"] == "2026-09-06T12:00:05+00:00"


def test_headers():
    headers = dict(headers_for(PREDICTION, produced_at=T))
    assert headers[HEADER_SCHEMA_VERSION] == str(SCHEMA_VERSION).encode()
    assert headers[HEADER_EVENT_TYPE] == b"arrival_prediction"


def test_current_version_is_supported():
    assert SCHEMA_VERSION in SUPPORTED_VERSIONS


@pytest.mark.parametrize(
    "raw, fragment",
    [
        (b"\xff\xfe not json", "not valid UTF-8 JSON"),
        (b"[1,2,3]", "not a JSON object"),
        (b'{"type":"vehicle_position"}', "schema_version missing"),
        (b'{"type":"vehicle_position","schema_version":99}', "unsupported schema_version 99"),
        (b'{"type":"teleport","schema_version":1}', "unknown event type"),
        (b'{"type":"arrival_prediction","schema_version":1,"trip_id":"1"}', "missing required"),
    ],
)
def test_decode_failures_are_schema_errors(raw, fragment):
    with pytest.raises(SchemaError, match=fragment):
        decode(raw)


def test_wrong_field_type_is_schema_error():
    raw = encode(PREDICTION).replace(b'"delay_seconds":18', b'"delay_seconds":"soon"')
    with pytest.raises(SchemaError, match="delay_seconds"):
        decode(raw)


def test_naive_timestamp_rejected():
    raw = encode(POSITION).replace(b"+00:00", b"")
    with pytest.raises(SchemaError, match="without timezone"):
        decode(raw)


def test_optional_fields_survive_as_none():
    raw = encode(ArrivalPrediction("t", "r", "OP", "s", 0, T))
    event = decode(raw)
    assert event.stop_sequence is None and event.predicted_time is None
