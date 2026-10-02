"""Unit tests for the smoke-test helpers: they must detect failures, not only successes.

These tests need no running lab. Run: python3 -m unittest discover -s tests -p "test_*.py"
"""
import copy
import importlib.util
import pathlib
import sys
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import demo_lib
spec = importlib.util.spec_from_file_location('wait_http', ROOT / 'scripts/wait-http.py')
wait_http = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wait_http)
extended_spec = importlib.util.spec_from_file_location('extended_smoke', ROOT / 'scripts/extended-smoke.py')
extended_smoke = importlib.util.module_from_spec(extended_spec)
extended_spec.loader.exec_module(extended_smoke)


class AcceptanceFailureDetection(unittest.TestCase):
    def setUp(self):
        self.node = {'host': 'broker1', 'component': 'kafka-broker', 'job': 'kafka-broker', 'env': 'demo'}
        self.time = datetime(2026, 9, 30, 12, 0, 0, 123000, tzinfo=timezone.utc)
        self.hits = {'total': {'value': 1}, 'hits': [{'_source': {
            'body': 'synthetic\n\tat demo.Probe.run(Probe.java:42)',
            'severityText': 'ERROR', 'time': '2026-09-30T12:00:00.123Z',
            'resource': {'attributes': {'service.name': 'kafka-broker', 'host.name': 'broker1'}}}}]}

    def test_red_or_timed_out_opensearch_is_not_ready(self):
        self.assertFalse(wait_http.healthy_opensearch({'status': 'red', 'timed_out': False}))
        self.assertFalse(wait_http.healthy_opensearch({'status': 'yellow', 'timed_out': True}))
        self.assertTrue(wait_http.healthy_opensearch({'status': 'yellow', 'timed_out': False}))

    def test_metric_name_prefix_is_not_an_exact_metric(self):
        names = demo_lib.metric_names('# HELP kafka_metric help\nkafka_metric_total 1\nkafka_other{a="x"} 2\n')
        self.assertNotIn('kafka_metric', names)
        self.assertEqual(names, {'kafka_metric_total', 'kafka_other'})

    def test_log_validation_rejects_duplicate_missing_stack_wrong_host_wrong_time(self):
        self.assertTrue(demo_lib.verify_log(self.hits, self.node, self.time))
        variants = []
        duplicate = copy.deepcopy(self.hits); duplicate['total']['value'] = 2; variants.append(duplicate)
        stack = copy.deepcopy(self.hits); stack['hits'][0]['_source']['body'] = 'only first line'; variants.append(stack)
        host = copy.deepcopy(self.hits); host['hits'][0]['_source']['resource']['attributes']['host.name'] = 'broker2'; variants.append(host)
        date = copy.deepcopy(self.hits); date['hits'][0]['_source']['time'] = '2026-09-29T12:00:00Z'; variants.append(date)
        for bad in variants:
            with self.subTest(bad=bad):
                with self.assertRaises(AssertionError):
                    demo_lib.verify_log(bad, self.node, self.time)

    def test_freshness_and_identity_are_required(self):
        with patch.object(demo_lib, 'query', return_value=[]) as query:
            self.assertFalse(demo_lib.fresh_targets([self.node]))
            self.assertIn('time() - timestamp(', query.call_args.args[0])
            self.assertIn('< 45', query.call_args.args[0])
        with patch.object(demo_lib, 'query', return_value=[{'metric': {'instance': 'broker2', 'job': 'kafka-broker'}}]):
            self.assertFalse(demo_lib.fresh_targets([self.node]))
        with patch.object(demo_lib, 'query', return_value=[{'metric': {'instance': 'broker1', 'job': 'kafka-broker'}}]):
            self.assertTrue(demo_lib.fresh_targets([self.node]))

    def test_extended_smoke_extracts_nested_span_names(self):
        trace = {'resourceSpans': [{'scopeSpans': [{'spans': [
            {'name': 'order.create'}, {'name': 'kafka.publish'}, {'name': 'order.process'}
        ]}]}]}
        self.assertEqual({'order.create', 'kafka.publish', 'order.process'}, extended_smoke.span_names(trace))


if __name__ == '__main__':
    unittest.main()
