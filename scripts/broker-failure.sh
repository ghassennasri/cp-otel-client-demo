#!/usr/bin/env bash
# Controlled incident: stop the Kafka service on broker3 for 45 s, then start it again.
# The OTel agent on broker3 keeps running, so up{instance="broker3"} == 0 becomes visible
# in VictoriaMetrics, as well as under-replicated partitions on the other brokers.
set -euo pipefail
cd "$(dirname "$0")/.."
# Stopping one broker leaves its OTel agent running, so up=0 can be observed.
# The EXIT trap restores the service on a normal interruption or failed command.
restore_broker() { docker exec cp-otel-broker3 systemctl start confluent-server; }
trap restore_broker EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
docker exec cp-otel-broker3 systemctl stop confluent-server
echo 'Broker 3 stopped for 45 s. Observe up=0 and under-replicated partitions.'
sleep 45
