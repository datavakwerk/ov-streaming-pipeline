"""GTFS-RT protobuf -> typed events, entity-by-entity fault isolated.

This is the *only* module in the repo that knows GTFS-RT exists (COMMITS.md commit 4: the
protocol risk is contained here). Parsing is fault isolated per entity: one malformed entity
yields one :class:`ParseError` (the DLQ payload), never a crashed batch.

Observed OVapi feed facts that shaped this module (recorded 2026-09-06, see fixtures):

* Entity ids look like ``2026-09-06:GVB:13:13605`` = ``<date>:<operator>:<line>:<journey>``.
  The operator code is only available here, so we take it from the id.
* ``VehiclePosition`` carries no delay; delay lives in ``TripUpdate.stop_time_update[].
  arrival/departure.delay``. Both feeds are full snapshots (``incrementality = FULL_DATASET``).
* ~18% of trip updates are CANCELED or consist solely of SKIPPED stops: no prediction there.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2 as gtfs

from common.events import ArrivalPrediction, VehiclePosition, utc

_ENTITY_ID = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2}):(?P<operator>[A-Za-z0-9_-]+):(?P<rest>.+)$")

_STATUS = {
    gtfs.VehiclePosition.INCOMING_AT: "INCOMING_AT",
    gtfs.VehiclePosition.STOPPED_AT: "STOPPED_AT",
    gtfs.VehiclePosition.IN_TRANSIT_TO: "IN_TRANSIT_TO",
}
_SKIPPED = gtfs.TripUpdate.StopTimeUpdate.SKIPPED
_CANCELED = gtfs.TripDescriptor.CANCELED


class FeedError(ValueError):
    """The whole payload is unusable (not a FeedMessage). The caller sends it to the DLQ."""


@dataclass(frozen=True, slots=True)
class ParseError:
    """One entity that could not become an event. Becomes a DLQ message with these headers."""

    entity_id: str
    reason: str
    raw: bytes  # the serialized entity, so the DLQ holds the original bytes


@dataclass(slots=True)
class ParseResult:
    feed_timestamp: datetime
    events: list = field(default_factory=list)
    errors: list[ParseError] = field(default_factory=list)
    skipped: int = 0  # entities deliberately not turned into events (canceled, inactive, ...)


def _split_entity_id(entity_id: str) -> tuple[str, str]:
    """``2026-09-06:GVB:13:13605`` -> ``("GVB", "GVB:13:13605")``."""
    match = _ENTITY_ID.match(entity_id)
    if not match:
        raise ValueError(f"entity id {entity_id!r} does not look like <date>:<operator>:<rest>")
    operator = match["operator"].upper()
    return operator, f"{operator}:{match['rest']}"


def _parse_feed(raw: bytes) -> gtfs.FeedMessage:
    feed = gtfs.FeedMessage()
    try:
        feed.ParseFromString(bytes(raw))
    except DecodeError as exc:
        raise FeedError(f"not a GTFS-RT FeedMessage: {exc}") from exc
    if not feed.HasField("header") or not feed.header.timestamp:
        raise FeedError("FeedMessage without header timestamp")
    return feed


def _keep_operator(operator: str, operator_filter: frozenset[str] | None) -> bool:
    return not operator_filter or operator in operator_filter


# --------------------------------------------------------------------------- positions


def _vehicle_position(entity: gtfs.FeedEntity) -> VehiclePosition:
    if not entity.HasField("vehicle"):
        raise ValueError("entity has no vehicle")
    vehicle = entity.vehicle
    operator, vehicle_id = _split_entity_id(entity.id)
    if not vehicle.trip.trip_id:
        raise ValueError("vehicle without trip_id")
    if not vehicle.HasField("position"):
        raise ValueError("vehicle without position")
    lat, lon = vehicle.position.latitude, vehicle.position.longitude
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0) or (lat == 0.0 and lon == 0.0):
        raise ValueError(f"implausible coordinates ({lat}, {lon})")
    if not vehicle.timestamp:
        raise ValueError("vehicle without timestamp")
    return VehiclePosition(
        vehicle_id=vehicle_id,
        trip_id=vehicle.trip.trip_id,
        route_id=vehicle.trip.route_id,
        operator=operator,
        lat=lat,
        lon=lon,
        timestamp=utc(vehicle.timestamp),
        stop_id=vehicle.stop_id or None,
        stop_sequence=vehicle.current_stop_sequence
        if vehicle.HasField("current_stop_sequence")
        else None,
        status=_STATUS.get(vehicle.current_status) if vehicle.HasField("current_status") else None,
        bearing=vehicle.position.bearing if vehicle.position.HasField("bearing") else None,
    )


def parse_vehicle_positions(
    raw: bytes, *, operator_filter: frozenset[str] | None = None
) -> ParseResult:
    feed = _parse_feed(raw)
    result = ParseResult(feed_timestamp=utc(feed.header.timestamp))
    for entity in feed.entity:
        try:
            event = _vehicle_position(entity)
        except ValueError as exc:
            result.errors.append(ParseError(entity.id, str(exc), entity.SerializeToString()))
            continue
        if _keep_operator(event.operator, operator_filter):
            result.events.append(event)
        else:
            result.skipped += 1
    return result


# --------------------------------------------------------------------------- predictions


def _next_stop_update(update: gtfs.TripUpdate) -> gtfs.TripUpdate.StopTimeUpdate | None:
    """The first non-skipped stop_time_update that carries a delay: the trip's *current* delay."""
    for stu in update.stop_time_update:
        if stu.schedule_relationship == _SKIPPED:
            continue
        if (stu.HasField("departure") and stu.departure.HasField("delay")) or (
            stu.HasField("arrival") and stu.arrival.HasField("delay")
        ):
            return stu
    return None


def _arrival_prediction(entity: gtfs.FeedEntity, feed_time: datetime) -> ArrivalPrediction | None:
    if not entity.HasField("trip_update"):
        raise ValueError("entity has no trip_update")
    update = entity.trip_update
    operator, _ = _split_entity_id(entity.id)
    if not update.trip.trip_id:
        raise ValueError("trip_update without trip_id")
    if update.trip.schedule_relationship == _CANCELED:
        return None
    stu = _next_stop_update(update)
    if stu is None:
        return None
    if not stu.stop_id:
        raise ValueError("stop_time_update without stop_id")
    # Departure delay is what a passenger on the platform experiences (PRD question 2);
    # arrival delay is the fallback for terminal stops.
    if stu.HasField("departure") and stu.departure.HasField("delay"):
        delay, when = stu.departure.delay, stu.departure.time
    else:
        delay, when = stu.arrival.delay, stu.arrival.time
    return ArrivalPrediction(
        trip_id=update.trip.trip_id,
        route_id=update.trip.route_id,
        operator=operator,
        stop_id=stu.stop_id,
        delay_seconds=int(delay),
        timestamp=feed_time,
        stop_sequence=stu.stop_sequence if stu.HasField("stop_sequence") else None,
        predicted_time=utc(when) if when else None,
    )


def parse_trip_updates(
    raw: bytes,
    *,
    operator_filter: frozenset[str] | None = None,
    horizon_s: int | None = 900,
) -> ParseResult:
    """Parse trip updates into one ``ArrivalPrediction`` per *active* trip.

    ``horizon_s`` drops trips whose next stop is more than that far in the future (tomorrow
    morning's 06:45 departure is in the feed all day, "on time"). ``None`` disables the filter
    (used by tests and by replay).
    """
    feed = _parse_feed(raw)
    feed_time = utc(feed.header.timestamp)
    result = ParseResult(feed_timestamp=feed_time)
    for entity in feed.entity:
        try:
            event = _arrival_prediction(entity, feed_time)
        except ValueError as exc:
            result.errors.append(ParseError(entity.id, str(exc), entity.SerializeToString()))
            continue
        if event is None or not _keep_operator(event.operator, operator_filter):
            result.skipped += 1
            continue
        if (
            horizon_s is not None
            and event.predicted_time is not None
            and (event.predicted_time - feed_time).total_seconds() > horizon_s
        ):
            result.skipped += 1
            continue
        result.events.append(event)
    return result


__all__ = [
    "FeedError",
    "ParseError",
    "ParseResult",
    "parse_trip_updates",
    "parse_vehicle_positions",
]
