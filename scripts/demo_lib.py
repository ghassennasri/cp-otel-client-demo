"""Shared helpers for the lab checks (smoke tests, dashboard audit).

Every check queries the real services: VictoriaMetrics on :8428, OpenSearch on :9200,
and the CP hosts through `docker exec`. Nothing is simulated.
"""
import json
import os
import pathlib
import re
import subprocess
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
# Inventory group -> (component name used in labels/logs, Prometheus job name).
GROUPS = {
    'kafka_broker': ('kafka-broker', 'kafka-broker'),
    'kafka_controller': ('kafka-controller', 'kafka-controller'),
    'schema_registry': ('schema-registry', 'schema-registry'),
    'kafka_connect': ('kafka-connect', 'kafka-connect'),
}


def settings():
    """DEMO_ENV and DEMO_CLUSTER_NAME from the environment or .env (same defaults as compose.yml)."""
    values = {'DEMO_ENV': 'demo', 'DEMO_CLUSTER_NAME': 'cp-demo'}
    env_file = ROOT / '.env'
    if env_file.is_file():
        for line in env_file.read_text().splitlines():
            key, sep, value = line.partition('=')
            if sep and key.strip() in values and value.strip():
                values[key.strip()] = value.strip()
    for key in values:
        values[key] = os.environ.get(key) or values[key]
    return values


def run(args, **kwargs):
    return subprocess.run(args, cwd=ROOT, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout


def request(url, payload=None, timeout=10):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def query(expression):
    result = request('http://localhost:8428/api/v1/query?' + urllib.parse.urlencode({'query': expression}))
    if result.get('status') != 'success':
        raise AssertionError(f'VictoriaMetrics query failed: {result}')
    return result['data']['result']


def eventually(check, description, seconds=180):
    """Retry check() every 2 s until it returns a truthy value, or fail after `seconds`."""
    deadline = time.monotonic() + seconds
    detail = 'condition still false'
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
            detail = 'condition still false'
        except Exception as error:
            detail = str(error)
        time.sleep(2)
    raise AssertionError(f'{description}: timed out after {seconds}s; {detail}')


def inventory_nodes():
    """One entry per CP host, read from the real Ansible inventory (no second host list)."""
    inventory = json.loads(run(['docker', 'compose', 'run', '--rm', '-T',
        '--entrypoint', 'ansible-inventory', 'ansible', '-i', 'ansible/inventory.yml', '--list']))
    nodes = []
    for group, (component, job) in GROUPS.items():
        for host in inventory.get(group, {}).get('hosts', []):
            variables = inventory['_meta']['hostvars'][host]
            nodes.append({'host': host, 'container': variables.get('ansible_host', host),
                'component': component, 'job': job, 'port': int(variables[group + '_jmxexporter_port']),
                'log_dir': variables[group + '_log_dir'], 'env': settings()['DEMO_ENV']})
    if not nodes or len({node['host'] for node in nodes}) != len(nodes):
        raise AssertionError('Inventory must contain exactly one supported CP role per host')
    return nodes


def fresh_targets(nodes):
    """True when every host has a successful JMX scrape less than 45 s old in VictoriaMetrics."""
    envs = {n['env'] for n in nodes}
    if len(envs) != 1:
        raise AssertionError('This lab expects one environment')
    selector = 'up{env=' + json.dumps(next(iter(envs))) + '}'
    # Checking just up=1 allows old samples to pass while an agent is down.
    expression = f'({selector} == 1) and (time() - timestamp({selector}) < 45)'
    rows = query(expression)
    expected = {(n['host'], n['job']) for n in nodes}
    found = {(r['metric'].get('instance'), r['metric'].get('job')) for r in rows}
    return expected <= found


def metric_names(text):
    return {match.group(1) for line in text.splitlines()
            if not line.startswith('#')
            for match in [re.match(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{|\s)', line)] if match}


def inject_log(node, marker):
    """Append a synthetic, clearly marked ERROR record with a stack trace to the host's log dir."""
    timestamp = datetime.now(timezone.utc)
    stamp = timestamp.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
    body = (f'[{stamp}] ERROR {marker} Simulated validation exception (demo.Probe)\n'
            'java.lang.IllegalStateException: synthetic test only\n'
            '\tat demo.Probe.run(Probe.java:42)\n'
            f'[{stamp}] INFO {marker}_END (demo.Probe)\n')
    path = str(pathlib.PurePosixPath(node['log_dir']) / 'otel-demo.log')
    run(['docker', 'exec', '-i', node['container'], 'tee', '-a', path], input=body)
    return timestamp


def find_log(marker):
    response = request('http://localhost:9200/cp-logs-*/_search', {
        'query': {'match_phrase': {'body': marker + ' Simulated validation exception'}},
        'track_total_hits': True, 'size': 10})
    return response['hits']


def verify_log(hits, node, expected_time):
    """Check the marker was indexed exactly once, as one multiline event, with parsed metadata."""
    total = hits['total']['value'] if isinstance(hits['total'], dict) else hits['total']
    if total != 1 or len(hits['hits']) != 1:
        raise AssertionError(f"{node['host']}: expected one indexed marker; found {total}")
    record = hits['hits'][0]['_source']
    if 'demo.Probe.run' not in record['body'] or record.get('severityText') != 'ERROR':
        raise AssertionError(f"{node['host']}: multiline/severity extraction failed")
    attrs = record['resource']['attributes']
    if attrs.get('service.name') != node['component'] or attrs.get('host.name') != node['host']:
        raise AssertionError(f"{node['host']}: incorrect resource identity: {attrs}")
    actual_time = datetime.fromisoformat(record['time'].replace('Z', '+00:00'))
    if abs((actual_time - expected_time).total_seconds()) > 1:
        raise AssertionError(f"{node['host']}: timestamp parsing failed: {record['time']}")
    return True


def wait_log(marker, node, expected_time):
    return eventually(lambda: verify_log(find_log(marker), node, expected_time),
                      f"Indexing marker for {node['host']}")


def write_report(path, report):
    target = pathlib.Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + '\n')
