#!/usr/bin/env bash
# Create the "cp-logs-*" index pattern in OpenSearch Dashboards (Discover), with its field list.
# Run it after some logs are indexed: the field list is read from the existing documents.
set -euo pipefail
cd "$(dirname "$0")/.."
python3 scripts/wait-http.py http://localhost:5601/api/status 180

python3 - <<'PYCODE'
import json
import urllib.request

OSD = 'http://localhost:5601'
# Fields as OpenSearch Dashboards sees them in the cp-logs-* indices.
with urllib.request.urlopen(OSD + '/api/index_patterns/_fields_for_wildcard?pattern=cp-logs-*'
                            '&meta_fields=_source&meta_fields=_id&meta_fields=_index', timeout=30) as response:
    fields = json.load(response)['fields']
for field in fields:
    field.setdefault('count', 0)
    field.setdefault('scripted', False)

body = json.dumps({'attributes': {'title': 'cp-logs-*', 'timeFieldName': 'time', 'fields': json.dumps(fields)}}).encode()
request = urllib.request.Request(OSD + '/api/saved_objects/index-pattern/cp-logs?overwrite=true', data=body,
                                 method='POST', headers={'osd-xsrf': 'true', 'Content-Type': 'application/json'})
urllib.request.urlopen(request, timeout=30).close()

# Make it the default index pattern in Discover.
body = json.dumps({'changes': {'defaultIndex': 'cp-logs'}}).encode()
request = urllib.request.Request(OSD + '/api/opensearch-dashboards/settings', data=body, method='POST',
                                 headers={'osd-xsrf': 'true', 'Content-Type': 'application/json'})
urllib.request.urlopen(request, timeout=30).close()
print(f'Index pattern cp-logs-* created with {len(fields)} fields.')
PYCODE
printf '%s\n' 'Discover: choose cp-logs-* and "Last 15 minutes"; search body for OTEL_DEMO_.'
