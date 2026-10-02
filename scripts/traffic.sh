#!/usr/bin/env bash
# Continuous producer/consumer traffic on demo-orders, useful during a live demo. Ctrl-C to stop.
set -euo pipefail
cd "$(dirname "$0")/.."
echo 'Produces bounded batches continuously. Ctrl-C stops after the current batch.'
while true; do
  docker exec cp-otel-broker1 kafka-producer-perf-test --topic demo-orders --num-records 30000 --record-size 256 --throughput 1000 --producer-props bootstrap.servers=broker1:9092 acks=all client.id=demo-producer
  docker exec cp-otel-broker2 kafka-console-consumer --bootstrap-server broker1:9092 --topic demo-orders --group demo-reader --max-messages 30000 --timeout-ms 60000 >/dev/null
done
