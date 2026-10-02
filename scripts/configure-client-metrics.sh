#!/usr/bin/env bash
# KIP-714: create the demo topic and a CLIENT_METRICS subscription that selects only the
# kip714-demo clients (client_id=kip714-demo-.*). Other clients of the cluster are not affected.
# Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."

broker=cp-otel-broker1
bootstrap=broker1:9092
name=kip714-demo

docker exec "$broker" kafka-topics \
  --bootstrap-server "$bootstrap" \
  --create --if-not-exists \
  --topic kip714-demo \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2

docker exec "$broker" kafka-client-metrics \
  --bootstrap-server "$bootstrap" \
  --alter \
  --name "$name" \
  --metrics '*' \
  --interval 10000 \
  --match 'client_id=kip714-demo-.*'

resources="$(docker exec "$broker" kafka-client-metrics --bootstrap-server "$bootstrap" --list)"
grep -Fxq "$name" <<<"$resources" || {
  echo "CLIENT_METRICS resource $name was not listed after configuration" >&2
  exit 1
}

docker exec "$broker" kafka-client-metrics \
  --bootstrap-server "$bootstrap" \
  --describe \
  --name "$name"