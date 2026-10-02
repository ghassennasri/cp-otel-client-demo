# Demo runbook (about 30 minutes)

A presenter script. Commands use the scripts directly; `make` targets are given in brackets.

## Before the session

- Deploy on a dedicated VM: `./scripts/up.sh`. Check that `artifacts/smoke.json` and
  `artifacts/extended-smoke.json` both say `"status": "passed"`.
- Open SSH tunnels for Grafana (3000), OpenSearch Dashboards (5601) and VictoriaMetrics (8428).
- Five minutes before the start, run `./scripts/traffic.sh` (`make traffic`) in a terminal and leave it running.
- Open these tabs: Grafana *Kafka cluster* dashboard, VictoriaMetrics UI, OpenSearch Discover (`cp-logs-*`),
  and the files `ansible/templates/agent.yaml.j2` and `config/otel/gateway.yaml`.
- Show synthetic data only.

## 0–5 min: the goal

> "You keep your metrics and log backends and your dashboards. OpenTelemetry becomes the common
> layer that collects and routes the data. Confluent Platform here is installed with cp-ansible,
> as on real hosts."

Show the diagram in the README and the inventory: 3 controllers, 3 brokers, one per logical rack.
Say clearly that the racks are simulated on one Docker host.

## 5–12 min: phase 1, metrics

1. Show `ansible/inventory.yml`. The same variables (JMX Exporter port, log directory, labels)
   are used by cp-ansible and by the OTel agent.
2. Show the `prometheus` receiver in `agent.yaml.j2`, then the `prometheus_remote_write/vm`
   exporter in `gateway.yaml`. Explain `trim_metric_suffixes: false` and
   `UnderscoreEscapingWithoutSuffixes`: names and labels stay the same, so the dashboards work unchanged.
3. Grafana → folder *Confluent Platform* → *Kafka cluster* → `env = demo`. Show broker state,
   partitions, throughput and replication. In the *KRaft* dashboard, show the controller state.
4. VictoriaMetrics UI:

   ```promql
   up{env="demo"}
   kafka_server_kafkaserver_brokerstate{env="demo"}
   sum(rate(kafka_server_brokertopicmetrics_bytesinpersec{env="demo",topic="demo-orders"}[5m]))
   ```

## 12–18 min: phase 2, logs

1. Show the `filelog/cp` receiver in `agent.yaml.j2`: file list, multiline rule, Log4j parser,
   resource attributes. Then show `config/data-prepper/pipelines.yaml`.
2. Run `python3 scripts/smoke.py` (`make smoke`) and copy one `OTEL_DEMO_...` marker from its output.
3. OpenSearch Discover, `cp-logs-*`, search the marker. Open the ERROR document. Show that the stack trace
   is one event and that `severityText`, `time`, `resource.attributes.host.name` and `log.file.name` are set.
4. Show a real Kafka log line too, for example filter `resource.attributes.service.name: kafka-controller`.
   Make clear which events are synthetic markers and which are real CP logs.

## 18–23 min: controlled incident

1. In a second terminal: `./scripts/broker-failure.sh` (`make fault`). The Kafka service on broker3
   stops for 45 s. Its OTel agent keeps running.
2. VictoriaMetrics: `up{instance="broker3"}` goes to 0. In Grafana, under-replicated partitions appear.
3. Discover: search ISR shrink or connection messages around the same time.
4. After the restart, metrics come back and replication catches up.

Scrape interval, batching, index refresh and Grafana refresh each add a few seconds of delay.
Do not promise second-level timing.

## 23–26 min: KMinion and phases 3–4 (optional)

- **KMinion**: show `config/otel/kminion-existing.example.yaml`. The current flow
  `KMinion → VictoriaMetrics` is kept. Moving it behind the gateway is optional.
- **Application observability**: run `./scripts/customer-traffic.sh` (`make traces`). Open
  *Customer Application Observability*. Show a JSON log line in Discover (`order processed`) with
  its `trace_id`, then the same trace in Grafana → Explore → Tempo (HTTP, `order.create`, Kafka
  publish/process, `order.process`). Three signals, three standard mechanisms: JMX, log file, OTel Java agent.
- **KIP-714**: run `./scripts/kip714-status.sh` (`make kip714-status`). Show the subscription
  (`client_id=kip714-demo-.*`), the plugin counters, then the dashboard *Kafka Client Telemetry (KIP-714)*.
  Open `apps/kip714-demo` to show that there is no OTel agent or SDK: the client pushes to the
  broker, and the broker plugin sends OTLP.

## 26–30 min: next steps

- Phase 1 first: it does not change CP or the dashboards.
- Then confirm: target CP version, TLS and authentication, cardinality and retention budgets,
  backend availability targets, owners of each component, and the support scope.
- Agree on one non-production cluster for a pilot.

Do not present the lab as a certified stack, a benchmark or a validated upgrade plan.

## After the demo: empty dashboard panels

Run `python3 scripts/dashboard-audit.py` (`make audit`) and read `artifacts/dashboard-audit.json`.
Each missing metric is explained by a feature that is not used (for example connectors or tiered
storage), a metric renamed in this CP version, or a collection problem.
