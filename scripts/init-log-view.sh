#!/usr/bin/env bash
# Create the "cp-logs-*" index pattern in OpenSearch Dashboards (Discover).
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/wait-http.py http://localhost:5601/api/status 180
curl --fail-with-body -sS -X POST 'http://localhost:5601/api/saved_objects/index-pattern/cp-logs?overwrite=true' -H 'osd-xsrf: true' -H 'Content-Type: application/json' --data '{"attributes":{"title":"cp-logs-*","timeFieldName":"time"}}'
printf '\n%s\n' 'Discover: choose cp-logs-* and Last 15 minutes; search body for OTEL_DEMO_.'
