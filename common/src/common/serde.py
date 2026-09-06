"""Event <-> Kafka message bytes.

Wire format: UTF-8 JSON, one object per message, with ``type`` and ``schema_version`` as the
first two keys. Datetimes are ISO-8601 with an explicit ``+00:00`` offset.

A decoder exists per supported schema version. Decoding failures raise :class:`SchemaError`,
which the processor turns into a DLQ message (PRD F5) — never a crash, never a silent drop.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from common.events import (
    EVENT_TYPES_BY_NAME,
    SCHEMA_VERSION,
    ArrivalPrediction,
    Event,
    VehiclePosition,
    event_type_name,
)

HEADER_SCHEMA_VERSION = "schema_version"
HEADER_EVENT_TYPE = "event_type"
HEADER_PRODUCED_AT = "produced_at"


class SchemaError(ValueError):
    """The bytes are not a valid event under any supported schema version."""


def _dt_out(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise SchemaError("naive datetime in event; all timestamps must be UTC-aware")
        return value.astimezone(UTC).isoformat(timespec="seconds")
    return value


def _dt_in(value: Any, name: str) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise SchemaError(f"{name}: not an ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise SchemaError(f"{name}: timestamp without timezone: {value!r}")
    return parsed.astimezone(UTC)


def encode(event: Event) -> bytes:
    body: dict[str, Any] = {"type": event_type_name(event), "schema_version": event.schema_version}
    for name in event.__dataclass_fields__:  # dataclass field order, stable
        if name == "schema_version":
            continue
        body[name] = _dt_out(getattr(event, name))
    return json.dumps(body, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def headers_for(event: Event, produced_at: datetime | None = None) -> list[tuple[str, bytes]]:
    produced_at = produced_at or datetime.now(tz=UTC)
    return [
        (HEADER_SCHEMA_VERSION, str(event.schema_version).encode()),
        (HEADER_EVENT_TYPE, event_type_name(event).encode()),
        (HEADER_PRODUCED_AT, produced_at.isoformat(timespec="seconds").encode()),
    ]


# --------------------------------------------------------------------------- decoders


def _require(body: dict, name: str, typ: type | tuple[type, ...]) -> Any:
    if name not in body or body[name] is None:
        raise SchemaError(f"missing required field {name!r}")
    value = body[name]
    if not isinstance(value, typ) or isinstance(value, bool):
        raise SchemaError(f"field {name!r} has type {type(value).__name__}, expected {typ}")
    return value


def _decode_v1(body: dict) -> Event:
    type_name = _require(body, "type", str)
    cls = EVENT_TYPES_BY_NAME.get(type_name)
    if cls is None:
        raise SchemaError(f"unknown event type {type_name!r}")

    if cls is VehiclePosition:
        return VehiclePosition(
            vehicle_id=_require(body, "vehicle_id", str),
            trip_id=_require(body, "trip_id", str),
            route_id=_require(body, "route_id", str),
            operator=_require(body, "operator", str),
            lat=float(_require(body, "lat", (int, float))),
            lon=float(_require(body, "lon", (int, float))),
            timestamp=_dt_in(_require(body, "timestamp", str), "timestamp"),
            stop_id=body.get("stop_id"),
            stop_sequence=body.get("stop_sequence"),
            status=body.get("status"),
            bearing=body.get("bearing"),
            schema_version=1,
        )
    return ArrivalPrediction(
        trip_id=_require(body, "trip_id", str),
        route_id=_require(body, "route_id", str),
        operator=_require(body, "operator", str),
        stop_id=_require(body, "stop_id", str),
        delay_seconds=int(_require(body, "delay_seconds", int)),
        timestamp=_dt_in(_require(body, "timestamp", str), "timestamp"),
        stop_sequence=body.get("stop_sequence"),
        predicted_time=_dt_in(body.get("predicted_time"), "predicted_time"),
        schema_version=1,
    )


# One decoder per version we accept. When v2 ships: add ``2: _decode_v2`` here, keep ``1`` for
# one release so an old producer and a new processor can coexist, then drop ``1``.
DECODERS: dict[int, Callable[[dict], Event]] = {1: _decode_v1}
SUPPORTED_VERSIONS = frozenset(DECODERS)
assert SCHEMA_VERSION in SUPPORTED_VERSIONS


def decode(raw: bytes | bytearray | memoryview) -> Event:
    try:
        body = json.loads(bytes(raw).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SchemaError(f"not valid UTF-8 JSON: {exc}") from exc
    if not isinstance(body, dict):
        raise SchemaError("event body is not a JSON object")
    version = body.get("schema_version")
    if not isinstance(version, int) or isinstance(version, bool):
        raise SchemaError(f"schema_version missing or not an int: {version!r}")
    decoder = DECODERS.get(version)
    if decoder is None:
        raise SchemaError(
            f"unsupported schema_version {version}; supported: {sorted(SUPPORTED_VERSIONS)}"
        )
    return decoder(body)


__all__ = [
    "DECODERS",
    "HEADER_EVENT_TYPE",
    "HEADER_PRODUCED_AT",
    "HEADER_SCHEMA_VERSION",
    "SUPPORTED_VERSIONS",
    "SchemaError",
    "decode",
    "encode",
    "headers_for",
]
