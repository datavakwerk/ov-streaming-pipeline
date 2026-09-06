"""The versioned event contract.

Everything downstream of the producer speaks *this* contract, never GTFS-RT. The archiver is the
one exception: it stores raw bytes so a future parser can re-derive events from the source of
truth (PRD §4, F2).

Schema evolution policy (README "Schema evolution" quotes this):

* ``SCHEMA_VERSION`` is stamped into every message body *and* into a Kafka header, so consumers
  can route on version without parsing the body.
* Backward-compatible changes (adding an optional field with a default) do **not** bump the
  version. Removing/renaming a field or changing a type does.
* ``common.serde`` keeps a decoder per supported version; the processor accepts the current and
  the previous version, so producers and consumers can be rolled independently.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from datetime import UTC, datetime
from typing import Literal

SCHEMA_VERSION = 1

TOPIC_VEHICLE_POSITIONS = "vehicle_positions"
TOPIC_ARRIVAL_PREDICTIONS = "arrival_predictions"
TOPICS = (TOPIC_VEHICLE_POSITIONS, TOPIC_ARRIVAL_PREDICTIONS)


def dlq_topic(source_topic: str) -> str:
    """DLQ naming policy: ``dlq.<source_topic>`` (PRD §5.2)."""
    return f"dlq.{source_topic}"


VehicleStatus = Literal["INCOMING_AT", "STOPPED_AT", "IN_TRANSIT_TO"]


@dataclass(frozen=True, slots=True)
class VehiclePosition:
    """Where a vehicle is right now. Keyed on ``vehicle_id`` in Kafka (commit 5)."""

    vehicle_id: str  # "<operator>:<line>:<journey>" — the GTFS-RT entity id minus its date prefix
    trip_id: str
    route_id: str
    operator: str
    lat: float
    lon: float
    timestamp: datetime  # UTC, when the vehicle reported this position
    stop_id: str | None = None  # next (or current) stop
    stop_sequence: int | None = None
    status: VehicleStatus | None = None
    bearing: float | None = None
    schema_version: int = SCHEMA_VERSION

    @property
    def key(self) -> str:
        return self.vehicle_id


@dataclass(frozen=True, slots=True)
class ArrivalPrediction:
    """The current delay of a trip, as predicted for its next stop. Keyed on ``trip_id``.

    ``delay_seconds`` is the feed-provided value (PRD OQ4, decision 2 in COMMITS.md): departure
    delay when present, arrival delay otherwise. Positive = late, negative = early.
    """

    trip_id: str
    route_id: str
    operator: str
    stop_id: str
    delay_seconds: int
    timestamp: datetime  # UTC, when the prediction was issued (feed header time) = event time
    stop_sequence: int | None = None
    predicted_time: datetime | None = None
    schema_version: int = SCHEMA_VERSION

    @property
    def key(self) -> str:
        return self.trip_id


Event = VehiclePosition | ArrivalPrediction

EVENT_TYPE_NAMES: dict[type, str] = {
    VehiclePosition: "vehicle_position",
    ArrivalPrediction: "arrival_prediction",
}
EVENT_TYPES_BY_NAME: dict[str, type] = {v: k for k, v in EVENT_TYPE_NAMES.items()}
TOPIC_FOR_EVENT: dict[type, str] = {
    VehiclePosition: TOPIC_VEHICLE_POSITIONS,
    ArrivalPrediction: TOPIC_ARRIVAL_PREDICTIONS,
}


def utc(ts: int | float) -> datetime:
    """Epoch seconds -> aware UTC datetime (PRD N5: UTC everywhere except the dashboard)."""
    return datetime.fromtimestamp(ts, tz=UTC)


def event_type_name(event: Event) -> str:
    return EVENT_TYPE_NAMES[type(event)]


def to_dict(event: Event) -> dict:
    return asdict(event)


def field_names(cls: type) -> tuple[str, ...]:
    return tuple(f.name for f in fields(cls))


__all__ = [
    "EVENT_TYPES_BY_NAME",
    "EVENT_TYPE_NAMES",
    "SCHEMA_VERSION",
    "TOPICS",
    "TOPIC_ARRIVAL_PREDICTIONS",
    "TOPIC_FOR_EVENT",
    "TOPIC_VEHICLE_POSITIONS",
    "ArrivalPrediction",
    "Event",
    "VehiclePosition",
    "dlq_topic",
    "event_type_name",
    "field",
    "to_dict",
    "utc",
]
