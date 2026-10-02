#!/usr/bin/env bash
# Create the demo topics, register a schema and produce/consume a first batch of records.
set -euo pipefail
cd "$(dirname "$0")/.."
# demo-orders: load-test traffic. customer-orders: the demo application. demo-file-events: Connect exercise.
for topic in demo-orders customer-orders demo-file-events; do
  docker exec cp-otel-broker1 kafka-topics --bootstrap-server broker1:9092 --create --if-not-exists --topic "$topic" --partitions 6 --replication-factor 3 --config min.insync.replicas=2
done
curl --fail-with-body -sS http://localhost:8081/subjects/demo-orders-value/versions -H 'Content-Type: application/vnd.schemaregistry.v1+json' --data '{"schema":"{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}'
# Continuous traffic is a separate action; seed provides a short initial sample.
docker exec cp-otel-broker1 kafka-producer-perf-test --topic demo-orders --num-records 20000 --record-size 256 --throughput 1000 --producer-props bootstrap.servers=broker1:9092 acks=all client.id=demo-producer
docker exec cp-otel-broker2 kafka-console-consumer --bootstrap-server broker1:9092 --topic demo-orders --group demo-reader --from-beginning --max-messages 20000 --timeout-ms 60000 >/dev/null
