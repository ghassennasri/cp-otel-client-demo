#!/usr/bin/env python3
"""List, per Grafana dashboard, which referenced metric names exist in VictoriaMetrics.

Useful to explain empty panels (feature not enabled, metric renamed in a CP release, ...).
This checks metric-name presence only; it does not evaluate PromQL or render panels.
Writes artifacts/dashboard-audit.json.
"""
import json
import re
import urllib.parse
from demo_lib import ROOT, request, settings, write_report


def queries(value):
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {'expr', 'query', 'definition'} and isinstance(item, str):
                yield item
            elif isinstance(item, (dict, list)):
                yield from queries(item)
    elif isinstance(value, list):
        for item in value:
            yield from queries(item)


def main():
    data = request('http://localhost:8428/api/v1/label/__name__/values?' +
                   urllib.parse.urlencode({'match[]': '{env=' + json.dumps(settings()['DEMO_ENV']) + '}'}))
    if data.get('status') != 'success':
        raise RuntimeError(data)
    present = set(data['data'])
    result = {'scope': 'stored metric-name presence; not live health, PromQL evaluation or visual validation', 'dashboards': []}
    for path in sorted((ROOT / 'assets/dashboards').glob('*.json')):
        dashboard = json.loads(path.read_text())
        required = set()
        for expression in queries(dashboard):
            required.update(re.findall(r'\b(?:kafka|confluent|java_lang|jvm|jmx|process)_[A-Za-z0-9_:]+', expression))
        result['dashboards'].append({'file': path.name, 'title': dashboard['title'],
            'referenced_metrics': len(required), 'present': sorted(required & present),
            'missing': sorted(required - present)})
    write_report(ROOT / 'artifacts/dashboard-audit.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
