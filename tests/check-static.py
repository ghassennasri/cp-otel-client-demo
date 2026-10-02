#!/usr/bin/env python3
"""Static checks that need no running lab.

- every YAML / JSON file parses, every shell script passes `bash -n`;
- vendored JMX Exporter rules match their recorded checksums;
- the OTel agent template renders for every inventory host, with the expected wiring;
- dashboards point to the provisioned VictoriaMetrics data source;
- the two demo applications stay separate (OTel agent in one, KIP-714 only in the other).

Rendered agent configs are written to artifacts/rendered/<host>.yaml, so that
scripts/validate.sh can check them with the real otelcol-contrib binary.
Requires PyYAML and Jinja2 (tests/requirements.txt). Run: python3 tests/check-static.py
"""
import hashlib
import json
import pathlib
import subprocess

import jinja2
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
EXCLUDED_DIRS = {'.venv', '.git', '.github', '__pycache__', 'artifacts', 'target'}

# Inventory group -> (component name used in labels/logs, Prometheus job name).
# Job names match what the upstream Confluent dashboards expect.
COMPONENTS = {
    'kafka_broker': ('kafka-broker', 'kafka-broker'),
    'kafka_controller': ('kafka-controller', 'kafka_controller'),
    'schema_registry': ('schema-registry', 'schema-registry'),
    'kafka_connect': ('kafka-connect', 'kafka-connect'),
}
# Values normally resolved by Ansible lookups from .env.
SAMPLE_LABELS = {'env': 'demo', 'confluent_cluster_name': 'cp-demo', 'kafka_connect_group_id': 'demo-connect'}


def source_files():
    for path in ROOT.rglob('*'):
        if path.is_file() and not EXCLUDED_DIRS.intersection(path.relative_to(ROOT).parts):
            yield path


def check_syntax():
    for path in source_files():
        if path.suffix in {'.yml', '.yaml'}:
            yaml.safe_load(path.read_text())
        elif path.suffix == '.json':
            json.loads(path.read_text())
    for script in (ROOT / 'scripts').glob('*.sh'):
        subprocess.run(['bash', '-n', str(script)], check=True)


def check_vendor_checksums():
    checksums = json.loads((ROOT / 'vendor/SHA256SUMS.json').read_text())
    for relative, expected in checksums.items():
        actual = hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
        assert actual == expected, f'Checksum mismatch for {relative}'


def render_agents():
    inventory = yaml.safe_load((ROOT / 'ansible/inventory.yml').read_text())['all']
    variables = {**inventory['vars'], **SAMPLE_LABELS}
    environment = jinja2.Environment(undefined=jinja2.StrictUndefined, keep_trailing_newline=True)
    environment.filters['to_json'] = json.dumps
    template = environment.from_string((ROOT / 'ansible/templates/agent.yaml.j2').read_text())
    output = ROOT / 'artifacts/rendered'
    output.mkdir(parents=True, exist_ok=True)
    count = 0
    for group, (component, job) in COMPONENTS.items():
        for host, host_vars in inventory['children'][group]['hosts'].items():
            context = {**variables, **host_vars, 'inventory_hostname': host, 'otel_component': {
                'name': component, 'job': job,
                'port': variables[group + '_jmxexporter_port'], 'log_dir': variables[group + '_log_dir']}}
            rendered = template.render(context)
            config = yaml.safe_load(rendered)
            scrape = config['receivers']['prometheus']['config']['scrape_configs'][0]
            assert scrape['job_name'] == job
            assert scrape['static_configs'][0]['labels']['env'] == 'demo'
            # Overwriting service.name in a resource processor would break the job label.
            assert 'resource' not in config.get('processors', {})
            assert config['exporters']['otlp/gateway']['sending_queue']['storage'] == 'file_storage'
            assert any('gc.log' in pattern for pattern in config['receivers']['filelog/cp']['exclude'])
            (output / f'{host}.yaml').write_text(rendered)
            count += 1
    return count


def check_dashboards():
    for path in (ROOT / 'assets/dashboards').glob('*.json'):
        text = path.read_text()
        assert '__inputs' not in json.loads(text), path.name
        assert '${Prometheus}' not in text, path.name
        assert 'cp-victoriametrics' in text, path.name


def check_demo_apps():
    compose = yaml.safe_load((ROOT / 'compose.yml').read_text())['services']
    gateway = yaml.safe_load((ROOT / 'config/otel/gateway.yaml').read_text())
    customer = (ROOT / 'apps/customer-observability-demo/src/main/java/com/demo/orders/CustomerObservabilityDemo.java').read_text()
    kip714 = (ROOT / 'apps/kip714-demo/src/main/java/com/demo/kip714/Kip714Demo.java').read_text()
    inventory = yaml.safe_load((ROOT / 'ansible/inventory.yml').read_text())['all']['vars']

    # Application observability: OTel Java agent for traces, JMX for metrics, KIP-714 off.
    assert 'opentelemetry-javaagent.jar' in compose['customer-observability-demo']['environment']['JAVA_TOOL_OPTIONS']
    assert 'ENABLE_METRICS_PUSH_CONFIG, false' in customer
    assert {'prometheus/customer-app', 'filelog/customer-app'} <= set(gateway['receivers'])
    assert gateway['service']['pipelines']['traces']['exporters'] == ['otlp/tempo']

    # KIP-714: no OTel agent or SDK in the client; the broker plugin is installed and registered.
    assert 'JAVA_TOOL_OPTIONS' not in compose['kip714-demo'].get('environment', {})
    assert 'io.opentelemetry' not in kip714
    assert kip714.count('ENABLE_METRICS_PUSH_CONFIG, true') == 2
    assert 'client-telemetry-reporter.jar' in (ROOT / 'docker/node.Dockerfile').read_text()
    assert 'kip714_reporter_class' in inventory['kafka_broker_custom_properties']['metric.reporters']


def main():
    check_syntax()
    check_vendor_checksums()
    hosts = render_agents()
    check_dashboards()
    check_demo_apps()
    print(f'PASS: syntax, vendor checksums, {hosts} agent configs rendered, dashboards, demo app wiring')


if __name__ == '__main__':
    main()
