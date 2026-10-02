#!/usr/bin/env bash
# Show the KIP-714 state: client app, CLIENT_METRICS subscription, broker plugin counters.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose ps kip714-demo
docker exec cp-otel-broker1 kafka-client-metrics \
  --bootstrap-server broker1:9092 \
  --describe \
  --name kip714-demo
docker compose logs --tail=40 kip714-demo

echo '--- Broker plugin counters (sum of all brokers) ---'
curl --fail-with-body -sS -G http://localhost:8428/api/v1/query \
  --data-urlencode 'query=sum by (__name__) ({__name__=~"kip714_reporter_payloads_.*_total"})' \
  | python3 -c 'import json,sys; [print(r["metric"]["__name__"], r["value"][1]) for r in json.load(sys.stdin)["data"]["result"]]'
echo '--- KIP-714 client series in VictoriaMetrics, per client_id ---'
curl --fail-with-body -sS -G http://localhost:8428/api/v1/query \
  --data-urlencode 'query=count by (client_id) ({telemetry_source="kip714"})' \
  | python3 -c 'import json,sys; [print(r["metric"].get("client_id"), r["value"][1], "series") for r in json.load(sys.stdin)["data"]["result"]]'
