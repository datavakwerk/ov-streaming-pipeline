#!/usr/bin/env bash
# How the fixtures in this directory were recorded (PRD OQ5: fixtures in the repo, not a 24h
# archive). OVapi is open access — no key needed. Re-run to refresh; then trim with
# `uv run python producer/tests/fixtures/gtfsrt/trim.py` so the files stay small.
set -euo pipefail
cd "$(dirname "$0")"
BASE="${OVAPI_BASE_URL:-https://gtfs.ovapi.nl/nl}"
curl -sS -o vehicle_positions.full.pb "$BASE/vehiclePositions.pb"
curl -sS -o trip_updates.full.pb     "$BASE/tripUpdates.pb"
ls -la ./*.pb
