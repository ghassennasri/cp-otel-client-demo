#!/usr/bin/env bash
# Build, deploy and test the complete demo. Safe to run again on an existing lab.
#
# Steps:
#   1. settings + host checks
#   2. build images, start the CP hosts and the backends
#   3. install Confluent Platform with cp-ansible, then one OTel agent per CP host
#   4. create topics and traffic, start the two demo applications
#   5. run the end-to-end tests and the dashboard audit (reports in artifacts/)
# On failure, diagnostics are collected in artifacts/diagnostics.
set -euo pipefail
cd "$(dirname "$0")/.."

# --- 1. Settings and host checks ---
./scripts/configure.sh
./scripts/preflight.sh
mkdir -p artifacts
trap 'rc=$?; ./scripts/diagnostics.sh || true; exit "$rc"' ERR

# Validate the gateway configuration with the exact Collector version before starting anything.
docker compose run --rm --no-deps otel-gateway validate --config=/etc/otelcol/config.yaml

# --- 2. Images, CP hosts and backends ---
docker compose build broker1 ansible customer-observability-demo kip714-demo

# When a CP host container is (re)created, its log files are new. If an old OTel state volume
# exists for it, clear the stale file offsets and only read new lines ("end").
otel_log_start_at=beginning
node_image="$(docker image inspect cp-otel-demo-node:8.3.2 --format '{{.Id}}')"
for service in controller1 controller2 controller3 broker1 broker2 broker3 schema1 connect1; do
  volume="cp-otel-demo_${service}-otel"
  container="$(docker compose ps -aq "$service")"
  if docker volume inspect "$volume" >/dev/null 2>&1 \
      && { [[ -z "$container" ]] || [[ "$(docker inspect "$container" --format '{{.Image}}')" != "$node_image" ]]; }; then
    docker run --rm -v "$volume:/state" cp-otel-demo-node:8.3.2 sh -c 'rm -rf /state/*'
    otel_log_start_at=end
  fi
done

docker compose up -d controller1 controller2 controller3 broker1 broker2 broker3 schema1 connect1 \
  victoriametrics opensearch grafana opensearch-dashboards tempo
python3 scripts/wait-http.py 'http://localhost:9200/_cluster/health?wait_for_status=yellow&timeout=3s' 240 --opensearch-health
python3 scripts/wait-http.py http://localhost:3200/ready 120
curl --fail-with-body -sS -X PUT http://localhost:9200/_index_template/cp-logs \
  -H 'Content-Type: application/json' --data-binary @config/opensearch/index-template.json
docker compose up -d data-prepper
docker compose run --rm --entrypoint python3 ansible scripts/wait-http.py http://data-prepper:4900/list 120
docker compose up -d otel-gateway
python3 scripts/wait-http.py http://localhost:13133 60

# --- 3. Confluent Platform (cp-ansible), then the OTel agents ---
docker compose run --rm ansible ansible/deploy-cp.yml 2>&1 | tee artifacts/deploy-cp.log
docker compose run --rm ansible ansible/deploy-otel.yml -e "otel_log_start_at=$otel_log_start_at" 2>&1 | tee artifacts/deploy-otel.log

# --- 4. Demo data and applications ---
./scripts/seed.sh
./scripts/configure-client-metrics.sh
docker compose up -d customer-observability-demo kip714-demo
python3 scripts/wait-http.py http://localhost:8088/health 120
python3 scripts/wait-http.py http://localhost:8089/health 120
./scripts/customer-traffic.sh
python3 scripts/wait-http.py http://localhost:8428/health 60
python3 scripts/wait-http.py http://localhost:3000/api/health 120

# --- 5. End-to-end tests and dashboard audit ---
python3 scripts/smoke.py
python3 scripts/extended-smoke.py
python3 scripts/dashboard-audit.py > /dev/null
./scripts/init-log-view.sh

printf '\n%s\n' 'Ready: Grafana http://localhost:3000 - OpenSearch Dashboards http://localhost:5601 - VictoriaMetrics http://localhost:8428/vmui/'
