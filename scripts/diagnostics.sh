#!/usr/bin/env bash
# Collect container status and recent collector/backend logs into artifacts/diagnostics.
# Review the files before sharing them.
set -uo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/diagnostics
docker compose ps --all > artifacts/diagnostics/compose-ps.txt 2>&1
docker compose logs --no-color --tail=150 otel-gateway data-prepper opensearch > artifacts/diagnostics/backends.log 2>&1
for name in controller1 controller2 controller3 broker1 broker2 broker3 schema1 connect1; do
  docker exec "cp-otel-$name" journalctl -u otelcol-contrib -n 80 --no-pager > "artifacts/diagnostics/$name-otel.log" 2>&1
done
echo 'Diagnostics written to artifacts/diagnostics; review before sharing.'
