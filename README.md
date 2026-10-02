# OpenTelemetry for Confluent Platform: integration demo

This demo shows how to add **OpenTelemetry (OTel)** to an existing Confluent Platform
monitoring stack **without replacing the tools already in place**.

OpenTelemetry is used as a **common collection and routing layer**. The existing pieces stay:

- JMX Exporter and its rules
- VictoriaMetrics and the Grafana dashboards
- OpenSearch and Data Prepper
- KMinion

```
          Confluent Platform / Applications
                        |
                  OpenTelemetry
                   /         \
          VictoriaMetrics   OpenSearch
                 |               |
              Grafana          Logs
```

Everything runs on one Linux VM with Docker. Confluent Platform is **installed with cp-ansible**
on systemd containers that behave like VMs. It does not use the `cp-server` application images,
so the deployment is close to a real cp-ansible installation.

---

## Contents

1. [What the demo shows](#1-what-the-demo-shows)
2. [Quick start](#2-quick-start)
3. [Walkthrough by use case](#3-walkthrough-by-use-case)
   - [3.1 Confluent Platform metrics (phase 1)](#31-confluent-platform-metrics-phase-1)
   - [3.2 Confluent Platform logs (phase 2)](#32-confluent-platform-logs-phase-2)
   - [3.3 Existing KMinion](#33-existing-kminion)
   - [3.4 Application observability (phase 3)](#34-application-observability-phase-3)
   - [3.5 Kafka client telemetry with KIP-714 (phase 4)](#35-kafka-client-telemetry-with-kip-714-phase-4)
4. [How to test](#4-how-to-test)
5. [Reuse in another environment](#5-reuse-in-another-environment)
6. [Repository layout](#6-repository-layout)
7. [Lab compared with production](#7-lab-compared-with-production)
8. [Troubleshooting](#8-troubleshooting)
9. [Versions, sources and support](#9-versions-sources-and-support)

---

## 1. What the demo shows

```
                      Telemetry sources

         Confluent Platform              Applications
            /       \                    /    |    \
          JMX       logs               JMX   logs  traces
           \         /                   \    |    /
     OTel agent (one per CP host)         \   |   /
                      \                    \  |  /
                       +----> OTel gateway <--+
                                   |
                  +----------------+----------------+
                  |                |                |
                  v                v                v
           VictoriaMetrics    Data Prepper        Tempo (optional)
                  |                |                |
               Grafana         OpenSearch        Grafana
```

- **OTel agent**: one collector per CP host. It reads the local JMX Exporter endpoint and the
  local log files, then sends both over OTLP to the gateway.
- **OTel gateway**: the single routing point. It sends metrics to VictoriaMetrics, logs to
  Data Prepper/OpenSearch and traces to Tempo. To change a backend, only the gateway changes.

| Phase | Use case | Main files | Changes on CP hosts |
|---|---|---|---|
| 1 | CP metrics: JMX → OTel → VictoriaMetrics | `ansible/templates/agent.yaml.j2`, `config/otel/gateway.yaml` | Add an OTel agent. JMX Exporter is unchanged. |
| 2 | CP logs: files → OTel → OpenSearch | `ansible/templates/agent.yaml.j2`, `config/otel/gateway.yaml`, `config/data-prepper/` | Same agent, with read access to the log directory. |
| 3 | Application traces, metrics and logs | `apps/customer-observability-demo/`, `config/otel/gateway.yaml` | None. The application gets two Java agents. |
| 4 | Kafka client metrics with KIP-714 | `plugins/client-telemetry-reporter/`, `apps/kip714-demo/` | Broker plugin + 3 broker properties. |
| – | Existing KMinion | `config/otel/kminion-existing.example.yaml` | None. KMinion is kept as it is. |

**Lab topology**: 3 KRaft controllers, 3 brokers (one per logical rack, RF=3, `min.insync.replicas=2`),
1 Schema Registry, 1 Connect worker, Confluent Platform 8.3.2. The racks are simulated on one host.

---

## 2. Quick start

### Prerequisites

- A **dedicated Linux amd64 VM**: 8 vCPU, 32 GiB RAM, 50 GB free disk (lab sizing only).
- Docker Engine (rootful) with Compose v2 and cgroup v2. Docker Desktop on macOS is not supported.
- `python3` and `curl`. `make` is optional: every `make` target below has a plain command.
- Internet access to Docker Hub, GitHub, `packages.confluent.io`, Ubuntu mirrors, Maven Central and PyPI.
- OpenSearch setting on the VM:

  ```bash
  sudo sysctl -w vm.max_map_count=262144
  ```

> The CP host containers are **privileged** (systemd needs it), and the Ansible runner uses the
> Docker socket. Use an isolated VM with no real data.

### Run

```bash
git clone <repository-url> cp-otel-client-demo
cd cp-otel-client-demo
./scripts/up.sh            # or: make up
```

The first run takes about 20 to 40 minutes, depending on the network. `up.sh`:

1. creates `.env` from `.env.example` (labels and a random Grafana password),
2. checks the host and validates the gateway configuration,
3. builds the images, then starts the CP hosts and the backends,
4. installs Confluent Platform with **cp-ansible**, then one **OTel agent** per CP host,
5. creates topics and traffic, then starts the two demo applications,
6. runs the end-to-end tests. Reports are written to `artifacts/`.

The script stops at the first error and collects diagnostics in `artifacts/diagnostics/`.

### User interfaces

All ports are bound to `127.0.0.1`. From a laptop, use an SSH tunnel, for example
`ssh -L 3000:localhost:3000 -L 5601:localhost:5601 <vm>`.

| Interface | URL | Login |
|---|---|---|
| Grafana | http://localhost:3000 | user `demo`, password in `.env` |
| OpenSearch Dashboards | http://localhost:5601 | none (lab) |
| VictoriaMetrics UI | http://localhost:8428/vmui/ | none |
| Tempo API | http://localhost:3200 | none |
| Schema Registry / Connect | http://localhost:8081 / http://localhost:8083 | none |
| Demo application | http://localhost:8088/health, `POST /order` | none |
| KIP-714 application | http://localhost:8089/health | none |

---

## 3. Walkthrough by use case

### 3.1 Confluent Platform metrics (phase 1)

```
Kafka / KRaft / Schema Registry / Connect
        | JMX
  JMX Exporter (javaagent, installed by cp-ansible, unchanged)
        | http://127.0.0.1:<port>/metrics
  OTel agent   -- prometheus receiver --> OTLP -->  OTel gateway
                                                       | Prometheus remote write
                                                 VictoriaMetrics --> Grafana (Confluent dashboards)
```

**Key configuration**

The agent scrapes the local JMX Exporter (`ansible/templates/agent.yaml.j2`):

```yaml
receivers:
  prometheus:
    trim_metric_suffixes: false          # keep the exact JMX Exporter names
    config:
      scrape_configs:
        - job_name: "kafka-broker"
          static_configs:
            - targets: ["127.0.0.1:8080"]
              labels: {env: "demo", cluster: "cp-demo", component: "kafka-broker", rack: "rack-1"}
exporters:
  otlp/gateway:
    endpoint: otel-gateway:4317
    sending_queue: {enabled: true, storage: file_storage}   # survives restarts and outages
```

The gateway writes to VictoriaMetrics (`config/otel/gateway.yaml`):

```yaml
exporters:
  prometheus_remote_write/vm:
    endpoint: http://victoriametrics:8428/api/v1/write
    translation_strategy: UnderscoreEscapingWithoutSuffixes   # no added suffixes
    resource_to_telemetry_conversion: {enabled: true}         # job, instance -> labels
```

The **Confluent dashboards** from
[jmx-monitoring-stacks](https://github.com/confluentinc/jmx-monitoring-stacks) need the same
metric names and the `env`, `job` and `instance` labels as before. The settings above keep them,
so the dashboards are used **without modification**. Only their data source is set to VictoriaMetrics.

**See it**

- Grafana → folder **Confluent Platform** → *Kafka cluster*, *KRaft*, *Topics*, *Schema Registry*,
  *Kafka Connect*. Choose `env = demo`.
- VictoriaMetrics UI:

  ```promql
  up{env="demo"}
  kafka_server_replicamanager_underreplicatedpartitions{env="demo"}
  sum(rate(kafka_server_brokertopicmetrics_bytesinpersec{env="demo",topic="demo-orders"}[5m]))
  ```

### 3.2 Confluent Platform logs (phase 2)

```
/var/log/kafka/server.log, controller.log, ...
        | filelog receiver (same OTel agent)
  OTel agent --> OTLP --> OTel gateway --> Data Prepper --> OpenSearch (index cp-logs-YYYY.MM.DD)
```

**Key configuration** (`ansible/templates/agent.yaml.j2`):

```yaml
receivers:
  filelog/cp:
    include: ["/var/log/kafka/*.log", "/var/log/kafka/*.log.*"]
    exclude: ["/var/log/kafka/*-gc.log*", "/var/log/kafka/*.gz"]
    storage: file_storage                     # remembers the read position of each file
    multiline:                                # a Java stack trace stays in one record
      line_start_pattern: '^\[?\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}'
    resource:                                 # who sent the log
      service.name: kafka-broker
      host.name: broker1
    operators:
      - type: regex_parser                    # timestamp + severity from the Log4j layout
        regex: '^\[?(?P<timestamp>...)\]?\s+(?P<level>TRACE|DEBUG|INFO|WARN|ERROR|FATAL)\b'
```

Data Prepper (`config/data-prepper/pipelines.yaml`) receives OTLP logs on port 21892 and writes them
to the daily index `cp-logs-%{yyyy.MM.dd}`.

The agent runs as a dedicated non-root user. `ansible/deploy-otel.yml` gives it read access to the
CP log directory with ACLs, and these ACLs stay valid after log rotation.

**See it**: OpenSearch Dashboards → **Discover** → index pattern `cp-logs-*`. Useful filters:
`resource.attributes.service.name`, `resource.attributes.host.name`, `severityText`.
The smoke test writes a clearly marked synthetic error on every host. Search `OTEL_DEMO_` to see a
multiline record with its stack trace.

### 3.3 Existing KMinion

**KMinion does not need to change.** If it already sends metrics to VictoriaMetrics, keep that flow:

```
KMinion -> VictoriaMetrics
```

If you later want KMinion to go through the common OTel layer, `config/otel/kminion-existing.example.yaml`
shows the receiver to add to the gateway. Scrape each endpoint only once, so the series are not stored
twice. This demo does not install KMinion.

### 3.4 Application observability (phase 3)

The same gateway works for customer applications. The demo application
`apps/customer-observability-demo` is a small order service. `POST /order` produces an order to
Kafka, and a consumer in the same process reads it.

```
Application
   |  JMX (custom MBean + Kafka client MBeans) --> JMX Exporter javaagent :9404 --scrape--+
   |  JSON log file (with trace_id / span_id)  -------------------------- filelog -------+--> OTel gateway
   |  traces (OpenTelemetry Java agent)        -------------------------- OTLP ----------+       |
                                                     VictoriaMetrics   OpenSearch   Tempo  <-------+
                                                              \           |          /
                                                                       Grafana
```

- **No code change** is needed for HTTP and Kafka spans. The OpenTelemetry Java agent creates them,
  and the trace context travels in the Kafka record headers from producer to consumer.
- Every JSON log line contains the `trace_id`, so you can go from a log in OpenSearch to its trace in Tempo.
- The configuration is in `compose.yml` (`JAVA_TOOL_OPTIONS`, `OTEL_*` variables) and in the
  `prometheus/customer-app` and `filelog/customer-app` receivers of `config/otel/gateway.yaml`.

**See it**: `./scripts/customer-traffic.sh` (or `make traces`), then open the Grafana dashboard
**Customer Application Observability**. Details: [docs/APPLICATION-OBSERVABILITY.md](docs/APPLICATION-OBSERVABILITY.md).

### 3.5 Kafka client telemetry with KIP-714 (phase 4)

KIP-714 is a **different mechanism**. It is shown with a separate application, `apps/kip714-demo`,
which has **no OpenTelemetry agent or SDK**.

```
Kafka producer / consumer  (enable.metrics.push=true)
          | PushTelemetry (Kafka protocol)
     Kafka broker
          | ClientTelemetryExporter plugin  (plugins/client-telemetry-reporter)
          | OTLP
     OTel gateway --> VictoriaMetrics --> Grafana
```

The client does not send OTLP to the collector. It pushes its metrics to the broker over the
Kafka protocol. A small broker plugin then forwards them to OpenTelemetry.

**Key configuration**

Broker properties (`ansible/inventory.yml`):

```properties
metric.reporters=com.demo.kip714.ClientOtlpMetricsReporter
confluent.telemetry.external.client.metrics.push.enabled=true
client.telemetry.otlp.endpoint=otel-gateway:4317
```

Choose which clients push which metrics. Here, only the demo clients (`scripts/configure-client-metrics.sh`):

```bash
kafka-client-metrics --bootstrap-server broker1:9092 --alter --name kip714-demo \
  --metrics '*' --interval 10000 --match 'client_id=kip714-demo-.*'
```

**See it**: `./scripts/kip714-status.sh` (or `make kip714-status`), then open the Grafana dashboard
**Kafka Client Telemetry (KIP-714)**. The series are named `org_apache_kafka_producer_*` and
`org_apache_kafka_consumer_*`, with `client_id` and `client_instance_id` labels.

> The broker plugin is **demo code**: plaintext gRPC, an in-memory queue and no retry. It is built
> against the Kafka 4.3 plugin API (CP 8.3). Details: [docs/KIP-714.md](docs/KIP-714.md).

---

## 4. How to test

All tests query the real services. Nothing is simulated. Each test writes a JSON report to `artifacts/`.

| Command | `make` target | What it checks |
|---|---|---|
| `./scripts/validate.sh` | `make validate` | No running lab needed. Compose file, Ansible syntax, gateway and 8 agent configs validated by the real `otelcol-contrib` binary, unit tests. |
| `python3 scripts/smoke.py` | `make smoke` | **Phases 1 and 2** on every CP host: fresh JMX scrape (< 45 s), metric names and labels kept, a marked multiline log indexed **once** with timestamp, severity and host, no re-sending after an agent restart. |
| `python3 scripts/extended-smoke.py` | `make extended-smoke` | **Phases 3 and 4**: application JMX metrics, JSON log, trace in Tempo, log ↔ trace link; KIP-714 subscription, plugin counters, client series. |
| `python3 scripts/dashboard-audit.py` | `make audit` | For each dashboard, which metric names exist in VictoriaMetrics. This explains empty panels. |
| `./scripts/broker-failure.sh` | `make fault` | Stops broker3 for 45 s. `up{instance="broker3"}` drops to 0, and under-replicated partitions appear, then recover. |

### Last tested

On 2 October 2026, on an Ubuntu VM with Docker Engine, starting from an empty lab
(`docker compose down --volumes`, then `./scripts/up.sh`, about 20 minutes):

| Check | Result |
|---|---|
| cp-ansible installation (8 hosts) and OTel agent installation | no failed task |
| `smoke.py`: phases 1 and 2 on all 8 CP hosts | passed |
| `extended-smoke.py`: phases 3 and 4 | passed |
| Dashboard audit (metric names found / referenced) | Kafka Topics 12/12, Customer Application 13/13, KIP-714 9/9, Schema Registry 10/11, KRaft 15/17, Kafka cluster 49/58, Kafka Connect 17/59 |

Most missing metrics are expected in this lab. Kafka Connect has no connectors running (see
[docs/CONNECT-EXERCISE.md](docs/CONNECT-EXERCISE.md)). A few upstream panels use metric names
that the JMX Exporter 1.1 rules or CP 8.3 do not produce, for example `jvm_memory_bytes_max`.
The full list is in `artifacts/dashboard-audit.json`.

Other useful commands:

```bash
./scripts/traffic.sh          # continuous Kafka traffic during a live demo (Ctrl-C to stop)
./scripts/customer-traffic.sh # 20 traced orders (CUSTOMER_DEMO_REQUESTS=100 for more)
docker compose stop           # pause the lab (everything is kept); "docker compose start" to resume
```

`docker compose down` removes the CP host containers, so Confluent Platform must be installed
again with `./scripts/up.sh`. `docker compose down --volumes` also deletes all metrics, logs and traces.

A presenter script for a 30-minute session is in [docs/DEMO-RUNBOOK.md](docs/DEMO-RUNBOOK.md).

---

## 5. Reuse in another environment

**Change the names.** Edit `.env` (created from `.env.example`):

```bash
DEMO_ENV=acme-dev            # "env" label, used by the Grafana dashboards
DEMO_CLUSTER_NAME=acme-kafka # "cluster" label and confluent.cluster.name on logs
```

They are used by cp-ansible, the OTel agents, the gateway and the tests. Then run `./scripts/up.sh` again.

**Apply the pattern to real CP hosts.** Only a few parts are needed, and they are independent of the lab:

1. Keep your cp-ansible inventory. Add the variables from the *"Shared with the OTel agent"*
   section of `ansible/inventory.yml` (JMX Exporter ports, log directories, labels, gateway endpoint).
2. Install the `otelcol-contrib` package on the hosts. Then run `ansible/deploy-otel.yml` through
   your approved access path (SSH/PAM) instead of the Docker connection.
3. Deploy `config/otel/gateway.yaml` on one or more gateway hosts and set your real backend URLs.
4. Phase 1 needs no change to CP: the JMX Exporter and its rules stay as they are.
5. For phase 4, build `plugins/client-telemetry-reporter` and copy the JAR to `/usr/share/java/kafka`
   on the brokers. This is optional and needs a broker restart.

Before production, read [docs/PRODUCTION-NOTES.md](docs/PRODUCTION-NOTES.md): TLS, sizing,
high availability, delivery guarantees and ownership.

---

## 6. Repository layout

```
.
├── README.md                     this file
├── .env.example                  demo settings (DEMO_ENV, DEMO_CLUSTER_NAME)
├── compose.yml                   CP hosts, backends and demo apps
├── Makefile                      shortcuts (make help)
├── ansible/
│   ├── inventory.yml             cp-ansible inventory + variables shared with the OTel agent
│   ├── deploy-cp.yml             installs Confluent Platform (cp-ansible)
│   ├── deploy-otel.yml           installs one OTel agent per CP host (systemd, ACLs)
│   └── templates/agent.yaml.j2   OTel AGENT config: JMX scrape + log files      <- phases 1, 2
├── config/
│   ├── otel/gateway.yaml         OTel GATEWAY config: routing to the backends   <- phases 1-4
│   ├── otel/kminion-existing.example.yaml   optional KMinion scrape
│   ├── data-prepper/             OTLP logs -> OpenSearch pipeline               <- phase 2
│   ├── opensearch/               index template for cp-logs-*
│   ├── grafana/provisioning/     VictoriaMetrics and Tempo data sources, dashboard folder
│   └── tempo/                    trace backend
├── assets/dashboards/            Grafana dashboards (5 Confluent + 2 demo)
├── vendor/jmx-exporter/          JMX Exporter rules from jmx-monitoring-stacks
├── apps/
│   ├── customer-observability-demo/   app with JMX metrics, JSON logs, traces   <- phase 3
│   └── kip714-demo/                   plain Kafka client, KIP-714 only          <- phase 4
├── plugins/client-telemetry-reporter/ broker plugin: KIP-714 -> OTLP            <- phase 4
├── docker/                       CP host image (Ubuntu + systemd) and Ansible runner image
├── scripts/                      deploy, traffic, tests, diagnostics
├── tests/                        static checks and unit tests
└── docs/                         details per topic, demo runbook, production notes, sources
```

---

## 7. Lab compared with production

| In this lab | In production |
|---|---|
| One Docker host, logical racks | Real fault domains (racks, zones) |
| Docker connection for Ansible | Approved SSH/PAM path from controlled automation |
| Plaintext Kafka, OTLP and JMX; OpenSearch security disabled | TLS or mTLS, authentication and authorization on every link and backend |
| One gateway, single-node backends | Several gateways behind a load balancer, backend high availability |
| `start_at: beginning` for log files | Decide how much history to read on the first rollout |
| All upstream JMX rules | A cardinality budget, with rules filtered to what dashboards and alerts use |
| Data Prepper in-memory buffer | Check the buffering options of your Data Prepper version if logs must not be lost |

Details, sizing method and failure scenarios: [docs/PRODUCTION-NOTES.md](docs/PRODUCTION-NOTES.md).

---

## 8. Troubleshooting

```bash
docker compose ps
docker compose logs --tail=100 otel-gateway data-prepper
docker exec cp-otel-broker1 systemctl status confluent-server otelcol-contrib
docker exec cp-otel-broker1 journalctl -u otelcol-contrib -n 100 --no-pager
docker exec cp-otel-broker1 curl -s http://localhost:8080/metrics | head   # JMX Exporter
./scripts/diagnostics.sh      # collects all of the above into artifacts/diagnostics/
```

| Symptom | What to check |
|---|---|
| Broker JMX endpoint is empty | The broker JMX Exporter waits 120 s after start (`startDelaySeconds`). Wait. |
| A dashboard panel is empty | Run `make audit`. The metric may not exist in this CP version, or the feature (e.g. connectors, tiered storage) may not be used. Also check the `env` variable and the time range. |
| No logs in Discover | Use the time range *Last 15 minutes* and index pattern `cp-logs-*`. Then check `journalctl -u otelcol-contrib` on the host and `docker compose logs data-prepper`. |
| Gateway logs "Exporting failed ... data-prepper" | Data Prepper is down or full. The gateway retries from its persistent queue. Check `docker compose logs data-prepper`. |
| Brokers write `libzstd-jni ... Unsupported OS/arch` | `/tmp` is mounted `noexec`. `compose.yml` mounts it with `exec`; keep that if you change the file. |
| No KIP-714 series | Run `./scripts/kip714-status.sh`. `received` must grow. If `failed` grows, read `grep KIP-714 /var/log/kafka/server.log` on a broker. |

---

## 9. Versions, sources and support

| Component | Version |
|---|---|
| Confluent Platform / cp-ansible | 8.3.2 / 8.3.2 |
| OpenTelemetry Collector Contrib | 0.162.0 |
| JMX Exporter | 1.1.0 |
| VictoriaMetrics | v1.153.0 |
| Grafana | 13.2.3 |
| OpenSearch / Data Prepper | 3.8.0 / 2.16.0 |
| Tempo | 2.8.2 |
| OpenTelemetry Java agent / Kafka Java client | 2.20.1 / 4.3.0 |

All versions are pinned (see also `versions.lock.json`), so the lab can be rebuilt the same way.
Phases 1 to 3 rely on JMX Exporter and log files, so they work the same way on other CP versions.
Metric names can change between CP releases: check the dashboards with `make audit` after an upgrade.

- Sources of the versions and settings: [docs/SOURCES.md](docs/SOURCES.md)
- Third-party files and licences: [NOTICE.md](NOTICE.md)

**Support**: this is an integration example, not a Confluent product. Before production, confirm
the support scope for each component of the chain (Confluent Platform, OpenTelemetry Collector,
VictoriaMetrics, OpenSearch/Data Prepper, and the KIP-714 plugin, which is demo code).
