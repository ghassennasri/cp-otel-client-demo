# Application observability (phase 3)

`apps/customer-observability-demo` is a small Java 21 order service. It shows how a customer application can use the same OpenTelemetry gateway as Confluent Platform. It is separate from the KIP-714 client and uses three standard mechanisms, one per signal.

| Signal | Mechanism | Path |
|---|---|---|
| Metrics | JMX MBeans + JMX Exporter javaagent | gateway scrapes `:9404` → VictoriaMetrics |
| Logs | JSON file, one object per line | gateway `filelog` → Data Prepper → OpenSearch |
| Traces | OpenTelemetry Java agent | OTLP → gateway → Tempo |

## Workload

`POST /order` creates an order, produces it to its own topic `customer-orders`, and a consumer in the same service processes it. The client identities are deliberately stable:

| Role | Value |
|---|---|
| Service | `customer-orders-demo` |
| Namespace | `cp-otel-demo` |
| Producer client ID | `customer-orders-producer` |
| Consumer client ID | `customer-orders-consumer` |
| Consumer group | `customer-orders-group` |

Both Kafka clients set `enable.metrics.push=false`. They are not part of the KIP-714 demonstration.

## JMX metrics

The application registers the real MBean `com.demo.orders:type=OrderMetrics`. Its counters are updated by the HTTP and Kafka code paths. Both Kafka clients also configure `JmxReporter` explicitly with `metrics.recording.level=TRACE`. The JMX Exporter javaagent exposes the custom, JVM, producer, and consumer MBeans on port 9404. The gateway Prometheus receiver scrapes that endpoint and remote-writes the resulting metrics to VictoriaMetrics.

Important series include:

- `customer_orders_submitted_total`
- `customer_orders_processed_total`
- `customer_orders_failed_total`
- `customer_orders_processing_latency_ms`
- `customer_jvm_heap_used_bytes`
- `customer_jvm_gc_collections_total`
- `customer_jvm_threads`
- `kafka_producer_*` (all numeric producer MBean attributes)
- `kafka_consumer_*` (all numeric consumer MBean attributes)

No OpenTelemetry metric SDK or OTLP metric exporter is enabled in this application.

## Logs

The application writes one JSON object per line to `/var/log/customer-demo/application.log`. The shared named volume is mounted read-only in the gateway, whose `filelog/customer-app` receiver parses the file. Logs then follow the existing gateway -> Data Prepper -> OpenSearch path.

Every parsed record has `service.name=customer-orders-demo`, `log.source=customer-application`, `order_id`, `trace_id`, and `span_id`. Use the `cp-logs-*` data view in OpenSearch Dashboards.

## Traces

Only this application uses the OpenTelemetry Java agent. The agent sends traces directly over OTLP/gRPC to the gateway, which exports them to Tempo. Kafka header propagation links the `order.create`, producer, consumer, and `order.process` spans.

Generate orders and traces with:

```bash
./scripts/customer-traffic.sh                 # 20 orders (make traces)
CUSTOMER_DEMO_REQUESTS=100 ./scripts/customer-traffic.sh
```

To find the trace of a log line: copy `attributes.trace_id` from the document in OpenSearch
Discover, then search it in Grafana → Explore → Tempo.

Open the **Customer Application Observability** dashboard in Grafana. The dashboard includes custom/JVM metrics, representative Kafka producer/consumer JMX panels, a Tempo TraceQL result table, and a link to the application logs in OpenSearch.