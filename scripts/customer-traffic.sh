#!/usr/bin/env bash
# Send N orders (default 20) to the customer demo application. Each order creates
# JMX metric updates, JSON log lines and one distributed trace (HTTP -> Kafka -> consumer).
set -euo pipefail
cd "$(dirname "$0")/.."

requests="${CUSTOMER_DEMO_REQUESTS:-20}"
for ((request = 1; request <= requests; request++)); do
  curl --fail-with-body -sS -X POST http://localhost:8088/order >/dev/null
done
printf 'Created %s traced customer orders.\n' "$requests"