#!/usr/bin/env python3
"""End-to-end test of the two optional demo parts.

Application observability (customer-observability-demo):
  JMX metrics in VictoriaMetrics, JSON log in OpenSearch, trace in Tempo, log <-> trace link.
KIP-714 (kip714-demo):
  client instance ids, CLIENT_METRICS subscription, broker plugin counters,
  client metrics in VictoriaMetrics with client_id / client_instance_id labels.

Writes artifacts/extended-smoke.json (status passed/failed).
"""
import json
import re
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from demo_lib import ROOT, eventually, query, request, run, write_report


def text_request(url, method='GET'):
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=15) as response:
        return response.read().decode()


def customer_log(order_id):
    result = request('http://localhost:9200/cp-logs-*/_search', {
        'query': {'bool': {'must': [
            {'match_phrase': {'attributes.order_id': order_id}},
            {'match_phrase': {'resource.attributes.service.name': 'customer-orders-demo'}},
            {'match_phrase': {'body': 'order processed'}},
        ]}},
        'sort': [{'time': 'desc'}],
        'size': 1,
    })
    hits = result.get('hits', {}).get('hits', [])
    return hits[0]['_source'] if hits else None


def tempo_trace(trace_id):
    url = 'http://localhost:3200/api/v2/traces/' + urllib.parse.quote(trace_id)
    req = urllib.request.Request(url, headers={'Accept': 'application/json'})
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def span_names(value):
    names = set()
    if isinstance(value, dict):
        name = value.get('name')
        if isinstance(name, str):
            names.add(name)
        for child in value.values():
            names.update(span_names(child))
    elif isinstance(value, list):
        for child in value:
            names.update(span_names(child))
    return names


def kip_metrics():
    rows = query('{telemetry_source="kip714",client_id=~"kip714-demo-.*"}')
    return [row for row in rows if row.get('metric', {}).get('__name__', '').startswith('org_apache_kafka_')]


def main():
    report_path = ROOT / 'artifacts' / 'extended-smoke.json'
    report = {'status': 'running', 'timestamp': datetime.now(timezone.utc).isoformat(),
              'customer_observability': {}, 'kip714': {}}
    try:
        eventually(lambda: text_request('http://localhost:8088/health') == 'ok\n',
                   'customer-observability-demo health')
        metrics = eventually(lambda: text_request('http://localhost:9404/metrics'),
                             'customer JMX Exporter endpoint')
        required = {'customer_orders_submitted_total', 'customer_orders_processed_total',
                    'customer_orders_failed_total', 'customer_orders_processing_latency_ms',
                    'customer_jvm_heap_used_bytes', 'customer_jvm_threads',
                    'kafka_producer_producer_metrics_record_send_rate',
                    'kafka_consumer_consumer_fetch_manager_metrics_records_consumed_rate',
                    'kafka_consumer_consumer_fetch_manager_metrics_records_lag'}
        missing = {name for name in required if name not in metrics}
        if missing:
            raise AssertionError(f'Customer JMX endpoint is missing metrics: {sorted(missing)}')

        response = json.loads(text_request('http://localhost:8088/order', method='POST'))
        order_id = response['order_id']
        eventually(lambda: query('customer_orders_submitted_total{job="customer-observability-demo"}'),
                   'customer JMX metric in VictoriaMetrics')
        eventually(lambda: query('kafka_producer_producer_metrics_record_send_rate{job="customer-observability-demo"}'),
               'customer producer JMX metric in VictoriaMetrics')
        eventually(lambda: query('kafka_consumer_consumer_fetch_manager_metrics_records_consumed_rate{job="customer-observability-demo"}'),
               'customer consumer JMX metric in VictoriaMetrics')
        log_record = eventually(lambda: customer_log(order_id), 'customer application log in OpenSearch')
        trace_id = log_record.get('attributes', {}).get('trace_id')
        span_id = log_record.get('attributes', {}).get('span_id')
        if not re.fullmatch(r'[0-9a-f]{32}', trace_id or '') or not re.fullmatch(r'[0-9a-f]{16}', span_id or ''):
            raise AssertionError(f'Customer log lacks valid trace/span IDs: {trace_id}/{span_id}')
        trace = eventually(lambda: tempo_trace(trace_id), 'correlated customer trace in Tempo')
        names = span_names(trace)
        for expected in ('order.create', 'order.process'):
            if expected not in names:
                raise AssertionError(f'Tempo trace lacks {expected}; spans={sorted(names)}')
        if not any('publish' in name.lower() or 'send' in name.lower() for name in names):
            raise AssertionError(f'Tempo trace lacks a Kafka producer span: {sorted(names)}')
        report['customer_observability'] = {
            'status': 'passed', 'order_id': order_id, 'trace_id': trace_id,
            'span_id': span_id, 'span_names': sorted(names),
            'checks': ['custom, producer, and consumer JMX in VictoriaMetrics', 'JSON log in OpenSearch',
                       'trace/log correlation', 'HTTP/order and Kafka spans in Tempo'],
        }

        eventually(lambda: text_request('http://localhost:8089/health') == 'ok\n', 'kip714-demo health')
        app_logs = run(['docker', 'compose', 'logs', '--no-color', 'kip714-demo'])
        producer_id = re.search(r'KIP-714 Producer:.*?client\.instance\.id = ([\w-]+)', app_logs, re.S)
        consumer_id = re.search(r'KIP-714 Consumer:.*?client\.instance\.id = ([\w-]+)', app_logs, re.S)
        if not producer_id or not consumer_id:
            raise AssertionError('KIP-714 application did not report both client instance IDs')
        subscription = run(['docker', 'exec', 'cp-otel-broker1', 'kafka-client-metrics',
            '--bootstrap-server', 'broker1:9092', '--describe', '--name', 'kip714-demo'])
        # The describe output says interval.ms=10000 in CP 8.3 (interval=10000 in older tools).
        if 'client_id=kip714-demo-.*' not in subscription or not re.search(r'interval(\.ms)?=10000', subscription.replace(' ', '')):
            raise AssertionError(f'Unexpected CLIENT_METRICS subscription: {subscription}')
        eventually(lambda: query('kip714_reporter_payloads_received_total > 0'),
                   'broker reporter receives PushTelemetry', seconds=300)
        eventually(lambda: query('kip714_reporter_payloads_exported_total > 0'),
                   'broker reporter exports OTLP', seconds=300)
        rows = eventually(kip_metrics, 'KIP-714 client metrics in VictoriaMetrics', seconds=300)
        samples = [{'name': row['metric'].get('__name__'),
                    'client_id': row['metric'].get('client_id'),
                    'client_instance_id': row['metric'].get('client_instance_id')}
                   for row in rows[:10]]
        report['kip714'] = {
            'status': 'passed',
            'producer_client_instance_id': producer_id.group(1),
            'consumer_client_instance_id': consumer_id.group(1),
            'metric_samples': samples,
            'checks': ['targeted CLIENT_METRICS subscription', 'client instance IDs',
                       'broker PushTelemetry reception', 'reporter OTLP export',
                       'KIP-714-only series and client_id labels in VictoriaMetrics'],
        }
        report['status'] = 'passed'
        return 0
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = str(error)
        print(str(error), file=sys.stderr)
        return 1
    finally:
        report['completed_at'] = datetime.now(timezone.utc).isoformat()
        write_report(report_path, report)
        print(json.dumps(report, indent=2))


if __name__ == '__main__':
    sys.exit(main())