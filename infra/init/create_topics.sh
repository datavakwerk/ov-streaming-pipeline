#!/usr/bin/env bash
# Idempotent topic bootstrap. Runs as a one-shot init container after Redpanda is healthy.
#
# Retention on the raw topics is deliberately short (24h): the broker is a buffer, not the
# archive. MinIO holds the replayable long-term copy (PRD F2). DLQ topics keep 7 days so a
# parser fix over a long weekend can still be replayed.
set -euo pipefail

BROKERS="${KAFKA_BOOTSTRAP_SERVERS:-redpanda:9092}"
RAW_PARTITIONS="${RAW_PARTITIONS:-6}"
RAW_RETENTION_MS="${RAW_RETENTION_MS:-86400000}"       # 24h
DLQ_RETENTION_MS="${DLQ_RETENTION_MS:-604800000}"      # 7d

rpk() { command rpk -X brokers="$BROKERS" "$@"; }

existing="$(rpk topic list --format json 2>/dev/null | tr -d ' \n' || true)"

create() {
  local topic="$1" partitions="$2" retention="$3"
  if [[ "$existing" == *"\"name\":\"$topic\""* ]]; then
    echo "topic $topic exists"
  else
    rpk topic create "$topic" -p "$partitions" -r 1 \
      -c retention.ms="$retention" -c cleanup.policy=delete
    echo "created $topic (partitions=$partitions retention.ms=$retention)"
  fi
}

create vehicle_positions        "$RAW_PARTITIONS" "$RAW_RETENTION_MS"
create arrival_predictions      "$RAW_PARTITIONS" "$RAW_RETENTION_MS"
create dlq.vehicle_positions    1 "$DLQ_RETENTION_MS"
create dlq.arrival_predictions  1 "$DLQ_RETENTION_MS"

rpk topic list
