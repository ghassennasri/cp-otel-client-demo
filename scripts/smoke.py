#!/usr/bin/env python3
"""End-to-end test of the Confluent Platform metrics and logs path, on every CP host.

Metrics: JMX Exporter -> OTel agent -> gateway -> VictoriaMetrics
  - every host has a fresh scrape (up == 1, sample younger than 45 s);
  - broker metric names and labels are preserved (what the Grafana dashboards need).
Logs: log file -> OTel agent -> gateway -> Data Prepper -> OpenSearch
  - a unique marker with a stack trace is written on each host and must be indexed
    once, as a single event, with timestamp, severity and host/component identity;
  - after an agent restart, new lines are delivered and old ones are not re-sent.

Writes artifacts/smoke.json (status passed/failed).
"""
import argparse
import json
import sys
import time
import uuid
from datetime import datetime, timezone
from demo_lib import (ROOT, eventually, find_log, fresh_targets, inject_log,
    inventory_nodes, metric_names, query, run, settings, verify_log, wait_log, write_report)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', default=str(ROOT / 'artifacts/smoke.json'))
    args = parser.parse_args()
    report = {'status': 'running', 'timestamp': datetime.now(timezone.utc).isoformat(), 'checks': []}
    try:
        nodes = inventory_nodes()
        eventually(lambda: fresh_targets(nodes), 'All JMX scrapes are successful and less than 45s old')
        report['scrape_targets'] = len(nodes)
        report['checks'].append('fresh JMX samples for every inventory host')
        brokers = [n for n in nodes if n['component'] == 'kafka-broker']
        for node in brokers:
            labels = '{instance=' + json.dumps(node['host']) + ',env=' + json.dumps(node['env']) + '}'
            eventually(lambda: query('kafka_server_kafkaserver_brokerstate' + labels),
                       'Broker state for ' + node['host'])
        connect_cluster_id = settings()['DEMO_ENV'] + '-connect'
        eventually(lambda: query('kafka_connect_app{job="kafka-connect",kafka_connect_cluster_id=' + json.dumps(connect_cluster_id) + '}'),
                   'Connect worker identity and dashboard label')
        node = brokers[0]
        raw = run(['docker', 'exec', node['container'], 'curl', '-fsS', f"http://localhost:{node['port']}/metrics"])
        names = metric_names(raw)
        for name in ['kafka_server_replicamanager_underreplicatedpartitions',
                     'kafka_server_brokertopicmetrics_bytesinpersec']:
            if name not in names:
                raise AssertionError(f'JMX endpoint does not expose {name}')
            selector = name + '{instance=' + json.dumps(node['host']) + ',env=' + json.dumps(node['env']) + '}'
            eventually(lambda: query(selector), 'Metric/label preservation: ' + name)
        report['checks'].append('broker metric names and Connect labels preserved')
        markers = []
        for node in nodes:
            marker = 'OTEL_DEMO_' + uuid.uuid4().hex
            emitted = inject_log(node, marker)
            markers.append((marker, node, emitted))
        for marker, node, emitted in markers:
            wait_log(marker, node, emitted)
        report['markers'] = {node['host']: marker for marker, node, _ in markers}
        report['checks'].append('multiline, severity, source timestamp and resource identity on every CP host')
        node = brokers[0]
        run(['docker', 'exec', node['container'], 'systemctl', 'restart', 'otelcol-contrib'])
        eventually(lambda: run(['docker', 'exec', node['container'], 'systemctl', 'is-active', 'otelcol-contrib']).strip() == 'active',
                   'Agent service restart')
        marker = 'OTEL_RESTART_' + uuid.uuid4().hex
        emitted = inject_log(node, marker)
        wait_log(marker, node, emitted)
        # Observe beyond the configured batch/flush periods after a fresh post-restart event.
        for _ in range(5):
            for old_marker, old_node, old_time in markers:
                verify_log(find_log(old_marker), old_node, old_time)
            time.sleep(3)
        report['checks'].append('post-restart delivery and no completed marker replay during the observation window')
        report['status'] = 'passed'
        return 0
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        print(str(error), file=sys.stderr)
        return 1
    finally:
        report['completed_at'] = datetime.now(timezone.utc).isoformat()
        write_report(args.report, report)
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    sys.exit(main())
